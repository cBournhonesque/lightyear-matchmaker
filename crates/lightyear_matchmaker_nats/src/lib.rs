//! NATS JetStream coordination backend.
//!
//! This crate stores ephemeral game-server readiness, capacity, assignments,
//! assignment acknowledgements, and active-connection reports in namespaced NATS
//! KV buckets.
//!
//! Terminology used by the coordination model:
//!
//! - A capacity report is metadata published by a game server describing how
//!   much space it currently has.
//! - An assignment is a matchmaker request for a server to accept one Lightyear
//!   client id.
//! - A reservation is a matchmaker-owned hold on capacity. Full reservation
//!   accounting is still separate from capacity reports and will use NATS
//!   compare-and-set operations rather than being published by the server.

#![allow(async_fn_in_trait)]

use async_nats::jetstream::{
    self,
    kv::{Operation, Store},
    stream::Stream as JetStreamStream,
};
use futures_util::StreamExt as _;
use lightyear_matchmaker_core::Result as CoreResult;
use lightyear_matchmaker_core::{
    ActiveConnection, AllocationId, AllocationRequest, AssignmentId, AssignmentPrepared,
    AssignmentRecord, CapacityQuery, GameServerReport, LightyearClientId, MatchmakerError,
    ProviderKind, RequestId, RoomSelection, ServerAllocation, ServerCapacity, ServerId,
    ServerProvider, ServerReadiness,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize)]
/// NATS connection and namespace configuration.
pub struct NatsConfig {
    #[serde(default = "default_nats_url")]
    /// NATS server URL.
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional username for NATS user/password authentication.
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional password for NATS user/password authentication.
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional namespace prefix for subjects and KV buckets.
    pub namespace: Option<String>,
    #[serde(default)]
    /// TTL and retention settings for NATS-backed ephemeral coordination data.
    pub ttl: NatsTtlConfig,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: "nats://127.0.0.1:4222".to_string(),
            username: None,
            password: None,
            namespace: None,
            ttl: NatsTtlConfig::default(),
        }
    }
}

fn default_nats_url() -> String {
    "nats://127.0.0.1:4222".to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// TTL and retention settings for NATS coordination buckets and streams.
pub struct NatsTtlConfig {
    #[serde(default = "default_short_ttl_secs")]
    /// Maximum age in seconds for server readiness reports.
    pub server_readiness_secs: u64,
    #[serde(default = "default_short_ttl_secs")]
    /// Maximum age in seconds for server capacity reports.
    pub server_capacity_secs: u64,
    #[serde(default = "default_assignment_ttl_secs")]
    /// Maximum age in seconds for server assignment sets and client indexes.
    pub assignments_secs: u64,
    #[serde(default = "default_assignment_ttl_secs")]
    /// Maximum age in seconds for game-server assignment-prepared acknowledgements.
    pub assignments_prepared_secs: u64,
    #[serde(default = "default_short_ttl_secs")]
    /// Maximum age in seconds for active connection reports.
    pub active_connections_secs: u64,
    #[serde(default = "default_lifecycle_work_ttl_secs")]
    /// Maximum age in seconds for release/delete lifecycle work items.
    pub lifecycle_work_secs: u64,
}

impl Default for NatsTtlConfig {
    fn default() -> Self {
        Self {
            server_readiness_secs: default_short_ttl_secs(),
            server_capacity_secs: default_short_ttl_secs(),
            assignments_secs: default_assignment_ttl_secs(),
            assignments_prepared_secs: default_assignment_ttl_secs(),
            active_connections_secs: default_short_ttl_secs(),
            lifecycle_work_secs: default_lifecycle_work_ttl_secs(),
        }
    }
}

fn default_short_ttl_secs() -> u64 {
    30
}

fn default_assignment_ttl_secs() -> u64 {
    60
}

fn default_lifecycle_work_ttl_secs() -> u64 {
    600
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Provider release request queued for asynchronous lifecycle cleanup.
pub struct ReleaseAllocationWork {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Matchmaker request id associated with the allocation, when known.
    pub request_id: Option<RequestId>,
    /// Provider allocation id to release.
    pub allocation_id: AllocationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Assignment id that triggered the release, when the release is assignment-scoped.
    pub assignment_id: Option<AssignmentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Server id associated with the allocation, when known.
    pub server_id: Option<ServerId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Provider that owns the allocation, when known.
    pub provider: Option<ProviderKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Human-readable reason for the release request.
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Assignment deletion request queued for asynchronous lifecycle cleanup.
pub struct DeleteAssignmentWork {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Matchmaker request id associated with the assignment, when known.
    pub request_id: Option<RequestId>,
    /// Assignment id to delete or invalidate.
    pub assignment_id: AssignmentId,
    /// Lightyear client id whose assignment index should be removed.
    pub client_id: LightyearClientId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Server id associated with the assignment, when known.
    pub server_id: Option<ServerId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Human-readable reason for the delete request.
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
/// Work item published to the NATS lifecycle WorkQueue stream.
pub enum LifecycleWork {
    /// Request to release provider-side capacity, session, or deployment state.
    ReleaseAllocation(ReleaseAllocationWork),
    /// Request to delete or invalidate a client assignment.
    DeleteAssignment(DeleteAssignmentWork),
}

impl LifecycleWork {
    fn subject(&self, names: &NatsNames) -> String {
        match self {
            Self::ReleaseAllocation(_) => names.lifecycle_release_allocation_subject(),
            Self::DeleteAssignment(_) => names.lifecycle_delete_assignment_subject(),
        }
    }
}

#[derive(Debug, Error)]
/// Errors returned by NATS coordination helpers.
pub enum NatsCoordinatorError {
    /// Error returned by NATS or JetStream operations.
    #[error("nats error: {0}")]
    Nats(String),
    /// Error serializing or deserializing JSON payloads.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Result type used by NATS coordination helpers.
pub type Result<T> = std::result::Result<T, NatsCoordinatorError>;

#[derive(Clone, Debug)]
/// Namespaced bucket and key builder for NATS coordination state.
pub struct NatsNames {
    namespace: Option<String>,
}

impl NatsNames {
    /// Creates a namespaced key builder.
    pub fn new(namespace: Option<String>) -> Self {
        Self {
            namespace: namespace
                .map(|namespace| sanitize_token(namespace.trim()))
                .filter(|namespace| !namespace.is_empty()),
        }
    }

    /// Returns a bucket name with the configured namespace applied.
    pub fn bucket(&self, base: &str) -> String {
        match &self.namespace {
            Some(namespace) => format!("{namespace}_{base}"),
            None => base.to_string(),
        }
    }

    /// Returns the KV key for a server id.
    pub fn key_server(&self, server_id: &ServerId) -> String {
        sanitize_token(&server_id.0)
    }

    /// Returns the KV key for a client id.
    pub fn key_client(&self, client_id: LightyearClientId) -> String {
        client_id.to_string()
    }

    /// Returns the KV key for an assignment id.
    pub fn key_assignment(&self, assignment_id: &AssignmentId) -> String {
        sanitize_token(&assignment_id.0)
    }

    /// Returns the KV key for a server/client active connection.
    pub fn key_connection(&self, server_id: &ServerId, client_id: LightyearClientId) -> String {
        format!(
            "{}.{}",
            self.key_server(server_id),
            self.key_client(client_id)
        )
    }

    /// Returns the subject used for all lifecycle work items.
    pub fn lifecycle_subject_all(&self) -> String {
        self.subject("lifecycle.>")
    }

    /// Returns the subject used for provider release lifecycle work.
    pub fn lifecycle_release_allocation_subject(&self) -> String {
        self.subject("lifecycle.release_allocation")
    }

    /// Returns the subject used for assignment deletion lifecycle work.
    pub fn lifecycle_delete_assignment_subject(&self) -> String {
        self.subject("lifecycle.delete_assignment")
    }

    fn subject(&self, base: &str) -> String {
        match &self.namespace {
            Some(namespace) => format!("{namespace}.{base}"),
            None => base.to_string(),
        }
    }
}

impl Default for NatsNames {
    fn default() -> Self {
        Self::new(None)
    }
}

#[derive(Clone)]
/// NATS-backed coordinator for ephemeral matchmaker/game-server state.
pub struct NatsCoordinator {
    names: NatsNames,
    jetstream: jetstream::Context,
    server_readiness: Store,
    server_capacity: Store,
    assignments_by_server: Store,
    assignments_by_client: Store,
    assignments_prepared: Store,
    active_connections: Store,
    lifecycle_work: JetStreamStream,
}

impl NatsCoordinator {
    /// Connects to NATS and opens or creates the required JetStream KV buckets.
    pub async fn connect(config: NatsConfig) -> Result<Self> {
        let mut options = async_nats::ConnectOptions::new();
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            options = options.user_and_password(username.clone(), password.clone());
        }
        let client = options
            .connect(config.url)
            .await
            .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))?;
        Self::new_with_ttl(client, config.namespace, config.ttl).await
    }

    /// Creates a coordinator from an existing NATS client using default TTLs.
    pub async fn new(client: async_nats::Client, namespace: Option<String>) -> Result<Self> {
        Self::new_with_ttl(client, namespace, NatsTtlConfig::default()).await
    }

    /// Creates a coordinator from an existing NATS client using explicit TTLs.
    pub async fn new_with_ttl(
        client: async_nats::Client,
        namespace: Option<String>,
        ttl: NatsTtlConfig,
    ) -> Result<Self> {
        let names = NatsNames::new(namespace);
        let js = jetstream::new(client);
        let server_readiness = get_or_create_bucket(
            &js,
            names.bucket("server_readiness"),
            "Game-server readiness and endpoint metadata",
            ttl_duration(ttl.server_readiness_secs),
            16 * 1024,
        )
        .await?;
        let server_capacity = get_or_create_bucket(
            &js,
            names.bucket("server_capacity"),
            "Game-server capacity and lobby/room metrics",
            ttl_duration(ttl.server_capacity_secs),
            32 * 1024,
        )
        .await?;
        let assignments_by_server = get_or_create_bucket(
            &js,
            names.bucket("assignments_by_server"),
            "Authoritative connection assignments keyed by game server",
            ttl_duration(ttl.assignments_secs),
            128 * 1024,
        )
        .await?;
        let assignments_by_client = get_or_create_bucket(
            &js,
            names.bucket("assignments_by_client"),
            "Secondary assignment indexes keyed by Lightyear client id",
            ttl_duration(ttl.assignments_secs),
            8 * 1024,
        )
        .await?;
        let assignments_prepared = get_or_create_bucket(
            &js,
            names.bucket("assignments_prepared"),
            "Game-server assignment preparation acknowledgements",
            ttl_duration(ttl.assignments_prepared_secs),
            16 * 1024,
        )
        .await?;
        let active_connections = get_or_create_bucket(
            &js,
            names.bucket("active_connections"),
            "Active game-server connections keyed by server and client id",
            ttl_duration(ttl.active_connections_secs),
            16 * 1024,
        )
        .await?;
        let lifecycle_work = get_or_create_lifecycle_stream(
            &js,
            names.bucket("lifecycle_work"),
            names.lifecycle_subject_all(),
            ttl_duration(ttl.lifecycle_work_secs),
        )
        .await?;
        Ok(Self {
            names,
            jetstream: js,
            server_readiness,
            server_capacity,
            assignments_by_server,
            assignments_by_client,
            assignments_prepared,
            active_connections,
            lifecycle_work,
        })
    }

    /// Returns the namespaced key builder used by this coordinator.
    pub fn names(&self) -> &NatsNames {
        &self.names
    }

    /// Publishes a server readiness report.
    pub async fn publish_readiness(&self, readiness: &ServerReadiness) -> Result<()> {
        put_json(
            &self.server_readiness,
            self.names.key_server(&readiness.server_id),
            readiness,
        )
        .await
    }

    /// Publishes a server capacity report.
    pub async fn publish_capacity(&self, capacity: &ServerCapacity) -> Result<()> {
        put_json(
            &self.server_capacity,
            self.names.key_server(&capacity.server_id),
            capacity,
        )
        .await
    }

    /// Reads the latest capacity report for a server.
    pub async fn capacity_for_server(
        &self,
        server_id: &ServerId,
    ) -> Result<Option<ServerCapacity>> {
        get_json(&self.server_capacity, self.names.key_server(server_id)).await
    }

    /// Lists capacity reports matching a game/version query.
    pub async fn list_capacity(&self, query: CapacityQuery) -> Result<Vec<ServerCapacity>> {
        let mut keys = self
            .server_capacity
            .keys()
            .await
            .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))?;
        let mut capacities = Vec::new();

        while let Some(key) = keys.next().await {
            let key = key.map_err(|error| NatsCoordinatorError::Nats(error.to_string()))?;
            let Some(capacity) = get_json::<ServerCapacity>(&self.server_capacity, key).await?
            else {
                continue;
            };
            if capacity.game == query.game && capacity.version == query.version {
                capacities.push(capacity);
            }
        }

        Ok(capacities)
    }

    /// Stores an assignment in the authoritative server-keyed assignment set.
    ///
    /// The client bucket is only a secondary index pointing to this server
    /// assignment. Game servers consume assignments by server id; client lookup
    /// remains useful for tests, debugging, and client-disconnect cleanup.
    pub async fn put_assignment(&self, assignment: &AssignmentRecord) -> Result<()> {
        self.update_server_assignments(&assignment.server_id, |assignments| {
            assignments
                .assignments
                .insert(assignment.assignment_id.clone(), assignment.clone());
        })
        .await?;
        put_json(
            &self.assignments_by_client,
            self.names.key_client(assignment.client_id),
            &ClientAssignmentIndex {
                server_id: assignment.server_id.clone(),
                assignment_id: assignment.assignment_id.clone(),
            },
        )
        .await
    }

    /// Deletes the assignment record and client index for a client id.
    pub async fn delete_assignment_for_client(&self, client_id: LightyearClientId) -> Result<()> {
        let client_key = self.names.key_client(client_id);
        let index =
            get_json::<ClientAssignmentIndex>(&self.assignments_by_client, client_key.clone())
                .await?;
        if let Some(index) = index {
            self.delete_assignment_for_server(&index.server_id, &index.assignment_id)
                .await?;
            self.delete_assignment_prepared(&index.assignment_id)
                .await?;
        }
        delete_key(&self.assignments_by_client, client_key).await
    }

    /// Reads the assignment for a client id using the secondary client index.
    pub async fn assignment_for_client(
        &self,
        client_id: LightyearClientId,
    ) -> Result<Option<AssignmentRecord>> {
        let Some(index) = get_json::<ClientAssignmentIndex>(
            &self.assignments_by_client,
            self.names.key_client(client_id),
        )
        .await?
        else {
            return Ok(None);
        };
        self.assignment_for_server(&index.server_id, &index.assignment_id)
            .await
    }

    /// Reads one assignment from the authoritative server-keyed assignment set.
    pub async fn assignment_for_server(
        &self,
        server_id: &ServerId,
        assignment_id: &AssignmentId,
    ) -> Result<Option<AssignmentRecord>> {
        Ok(self
            .server_assignments(server_id)
            .await?
            .and_then(|assignments| assignments.assignments.get(assignment_id).cloned()))
    }

    /// Lists assignments currently targeting a server.
    pub async fn assignments_for_server(
        &self,
        server_id: &ServerId,
    ) -> Result<Vec<AssignmentRecord>> {
        Ok(self
            .server_assignments(server_id)
            .await?
            .map(|assignments| assignments.assignments.into_values().collect())
            .unwrap_or_default())
    }

    /// Deletes one assignment from the authoritative server-keyed assignment set.
    pub async fn delete_assignment_for_server(
        &self,
        server_id: &ServerId,
        assignment_id: &AssignmentId,
    ) -> Result<()> {
        self.update_server_assignments(server_id, |assignments| {
            assignments.assignments.remove(assignment_id);
        })
        .await
    }

    /// Publishes a game-server assignment-prepared acknowledgement.
    pub async fn publish_assignment_prepared(&self, prepared: &AssignmentPrepared) -> Result<()> {
        put_json(
            &self.assignments_prepared,
            self.names.key_assignment(&prepared.assignment_id),
            prepared,
        )
        .await
    }

    /// Deletes the preparation acknowledgement for an assignment.
    pub async fn delete_assignment_prepared(&self, assignment_id: &AssignmentId) -> Result<()> {
        self.assignments_prepared
            .delete(self.names.key_assignment(assignment_id))
            .await
            .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))
    }

    /// Reads the preparation acknowledgement for an assignment.
    pub async fn assignment_prepared(
        &self,
        assignment_id: &AssignmentId,
    ) -> Result<Option<AssignmentPrepared>> {
        get_json(
            &self.assignments_prepared,
            self.names.key_assignment(assignment_id),
        )
        .await
    }

    /// Publishes an active-connection report.
    pub async fn publish_active_connection(&self, connection: &ActiveConnection) -> Result<()> {
        put_json(
            &self.active_connections,
            self.names
                .key_connection(&connection.server_id, connection.client_id),
            connection,
        )
        .await
    }

    /// Reads an active-connection report for a server/client pair.
    pub async fn active_connection(
        &self,
        server_id: &ServerId,
        client_id: LightyearClientId,
    ) -> Result<Option<ActiveConnection>> {
        get_json(
            &self.active_connections,
            self.names.key_connection(server_id, client_id),
        )
        .await
    }

    /// Deletes an active-connection report for a server/client pair.
    pub async fn delete_active_connection(
        &self,
        server_id: &ServerId,
        client_id: LightyearClientId,
    ) -> Result<()> {
        self.active_connections
            .delete(self.names.key_connection(server_id, client_id))
            .await
            .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))
    }

    /// Publishes a provider release lifecycle work item.
    pub async fn enqueue_release_allocation(&self, work: ReleaseAllocationWork) -> Result<()> {
        self.publish_lifecycle_work(&LifecycleWork::ReleaseAllocation(work))
            .await
    }

    /// Publishes an assignment deletion lifecycle work item.
    pub async fn enqueue_delete_assignment(&self, work: DeleteAssignmentWork) -> Result<()> {
        self.publish_lifecycle_work(&LifecycleWork::DeleteAssignment(work))
            .await
    }

    /// Publishes a lifecycle work item to the JetStream WorkQueue stream.
    pub async fn publish_lifecycle_work(&self, work: &LifecycleWork) -> Result<()> {
        let payload = serde_json::to_vec(work)?;
        let ack = self
            .jetstream
            .publish(work.subject(&self.names), payload.into())
            .await
            .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))?;
        ack.await
            .map(|_| ())
            .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))
    }

    /// Opens or creates a durable pull consumer for lifecycle work items.
    ///
    /// All worker instances that share one logical release/delete queue should
    /// use the same durable name. NATS WorkQueue streams reject overlapping
    /// durable consumers for the same subject filter.
    pub async fn lifecycle_work_consumer(
        &self,
        durable_name: impl AsRef<str>,
    ) -> Result<jetstream::consumer::PullConsumer> {
        let durable_name = sanitize_token(durable_name.as_ref());
        let durable_name = if durable_name.is_empty() {
            "lifecycle_worker".to_string()
        } else {
            durable_name
        };
        self.lifecycle_work
            .get_or_create_consumer(
                &durable_name,
                jetstream::consumer::pull::Config {
                    durable_name: Some(durable_name.clone()),
                    name: Some(durable_name.clone()),
                    description: Some(
                        "Lightyear Matchmaker release/delete lifecycle worker".to_string(),
                    ),
                    filter_subject: self.names.lifecycle_subject_all(),
                    ack_wait: Duration::from_secs(30),
                    max_deliver: 10,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))
    }

    /// Publishes any supported game-server report.
    pub async fn publish_report(&self, report: &GameServerReport) -> Result<()> {
        match report {
            GameServerReport::Readiness(readiness) => self.publish_readiness(readiness).await,
            GameServerReport::Capacity(capacity) => self.publish_capacity(capacity).await,
            GameServerReport::AssignmentPrepared(prepared) => {
                self.publish_assignment_prepared(prepared).await
            }
            GameServerReport::ActiveConnection(connection) => {
                if connection.connected {
                    self.publish_active_connection(connection).await
                } else {
                    self.delete_active_connection(&connection.server_id, connection.client_id)
                        .await?;
                    self.delete_assignment_for_client(connection.client_id)
                        .await
                }
            }
        }
    }

    async fn server_assignments(&self, server_id: &ServerId) -> Result<Option<ServerAssignments>> {
        get_json(
            &self.assignments_by_server,
            self.names.key_server(server_id),
        )
        .await
    }

    async fn update_server_assignments(
        &self,
        server_id: &ServerId,
        update: impl Fn(&mut ServerAssignments),
    ) -> Result<()> {
        let key = self.names.key_server(server_id);
        let mut last_error = None;
        for _ in 0..16 {
            let entry = self
                .assignments_by_server
                .entry(key.clone())
                .await
                .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))?;
            let (mut assignments, revision) = match entry {
                Some(entry) if entry.operation == Operation::Put => {
                    let assignments = serde_json::from_slice::<ServerAssignments>(&entry.value)?;
                    (assignments, Some(entry.revision))
                }
                Some(entry) => (
                    ServerAssignments::new(server_id.clone()),
                    Some(entry.revision),
                ),
                None => (ServerAssignments::new(server_id.clone()), None),
            };
            update(&mut assignments);
            let payload = serde_json::to_vec(&assignments)?;
            let result: std::result::Result<(), String> = match revision {
                Some(revision) => self
                    .assignments_by_server
                    .update(key.clone(), payload.into(), revision)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
                None => self
                    .assignments_by_server
                    .create(key.clone(), payload.into())
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error.to_string());
                    tokio::task::yield_now().await;
                }
            }
        }

        Err(NatsCoordinatorError::Nats(format!(
            "failed to atomically update assignments for server {} after retries: {}",
            server_id,
            last_error.unwrap_or_else(|| "unknown compare-and-set failure".to_string())
        )))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ServerAssignments {
    server_id: ServerId,
    #[serde(default)]
    assignments: std::collections::BTreeMap<AssignmentId, AssignmentRecord>,
}

impl ServerAssignments {
    fn new(server_id: ServerId) -> Self {
        Self {
            server_id,
            assignments: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClientAssignmentIndex {
    server_id: ServerId,
    assignment_id: AssignmentId,
}

#[derive(Clone)]
/// Static provider implementation backed by live NATS capacity reports.
pub struct NatsStaticServerProvider {
    coordinator: NatsCoordinator,
}

impl NatsStaticServerProvider {
    /// Creates a NATS-backed static provider.
    pub fn new(coordinator: NatsCoordinator) -> Self {
        Self { coordinator }
    }

    /// Returns the coordinator used by this provider.
    pub fn coordinator(&self) -> &NatsCoordinator {
        &self.coordinator
    }
}

impl ServerProvider for NatsStaticServerProvider {
    async fn allocate(&self, request: AllocationRequest) -> CoreResult<ServerAllocation> {
        request.validate()?;
        let capacities = self
            .coordinator
            .list_capacity(CapacityQuery {
                game: request.game.clone(),
                version: request.version.clone(),
            })
            .await
            .map_err(|error| MatchmakerError::Provider(error.to_string()))?;
        let Some(capacity) = choose_live_static_capacity(capacities, &request) else {
            return Err(MatchmakerError::NoCapacity);
        };

        Ok(ServerAllocation {
            allocation_id: AllocationId::new(format!(
                "nats-static:{}:{}",
                capacity.server_id, request.player_id
            )),
            server_id: capacity.server_id,
            provider: ProviderKind::Static,
            endpoint: capacity.endpoint,
            game: capacity.game,
            version: capacity.version,
            cert_digest: capacity.cert_digest,
            metadata: capacity.metadata,
        })
    }

    async fn release(&self, _allocation_id: AllocationId) -> CoreResult<()> {
        Ok(())
    }

    async fn list_capacity(&self, request: CapacityQuery) -> CoreResult<Vec<ServerCapacity>> {
        self.coordinator
            .list_capacity(request)
            .await
            .map_err(|error| MatchmakerError::Provider(error.to_string()))
    }
}

fn choose_live_static_capacity(
    mut capacities: Vec<ServerCapacity>,
    request: &AllocationRequest,
) -> Option<ServerCapacity> {
    // This is still a read/select policy, not a reservation. Sorting keeps the
    // choice deterministic for a single allocator: prefer lower latency, then
    // less loaded servers, then a stable server id tie-breaker.
    capacities.sort_by_key(|capacity| {
        (
            latency_rank(capacity, request),
            capacity.total_players,
            capacity.server_id.to_string(),
        )
    });
    capacities
        .into_iter()
        .find(|capacity| accepts_live_static_capacity(capacity, request))
}

fn accepts_live_static_capacity(capacity: &ServerCapacity, request: &AllocationRequest) -> bool {
    if capacity.provider != ProviderKind::Static
        || !capacity.ready
        || capacity.game != request.game
        || capacity.version != request.version
        || !capacity.has_player_capacity()
    {
        return false;
    }

    match &request.room {
        RoomSelection::Auto => true,
        RoomSelection::New => capacity.has_room_capacity(),
        RoomSelection::Code(_) | RoomSelection::Id(_) => capacity.rooms.iter().any(|room| {
            request
                .room
                .room_key()
                .is_some_and(|room_key| room.key == room_key && room.has_player_capacity())
        }),
    }
}

fn latency_rank(capacity: &ServerCapacity, request: &AllocationRequest) -> u32 {
    let Some(region) = &capacity.region else {
        return u32::MAX;
    };
    request
        .latencies
        .iter()
        .filter(|latency| &latency.region == region)
        .map(|latency| latency.rtt_ms)
        .min()
        .unwrap_or(u32::MAX)
}

async fn get_or_create_bucket(
    js: &jetstream::Context,
    bucket: String,
    description: &str,
    max_age: Duration,
    max_value_size: i32,
) -> Result<Store> {
    if let Ok(store) = js.get_key_value(bucket.clone()).await {
        return Ok(store);
    }
    js.create_key_value(jetstream::kv::Config {
        bucket,
        description: description.to_string(),
        max_age,
        max_value_size,
        ..Default::default()
    })
    .await
    .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))
}

async fn get_or_create_lifecycle_stream(
    js: &jetstream::Context,
    name: String,
    subject: String,
    max_age: Duration,
) -> Result<JetStreamStream> {
    js.get_or_create_stream(jetstream::stream::Config {
        name,
        description: Some("Release/delete lifecycle work queue for matchmaker cleanup".to_string()),
        subjects: vec![subject],
        retention: jetstream::stream::RetentionPolicy::WorkQueue,
        max_age,
        max_messages: 100_000,
        max_message_size: 16 * 1024,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await
    .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))
}

async fn put_json<T: Serialize>(store: &Store, key: String, value: &T) -> Result<()> {
    let payload = serde_json::to_vec(value)?;
    store
        .put(key, payload.into())
        .await
        .map(|_| ())
        .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))
}

async fn delete_key(store: &Store, key: String) -> Result<()> {
    store
        .delete(key)
        .await
        .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))
}

async fn get_json<T: DeserializeOwned>(store: &Store, key: String) -> Result<Option<T>> {
    let Some(payload) = store
        .get(key)
        .await
        .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))?
    else {
        return Ok(None);
    };
    serde_json::from_slice(payload.as_ref())
        .map(Some)
        .map_err(NatsCoordinatorError::from)
}

fn ttl_duration(seconds: u64) -> Duration {
    Duration::from_secs(seconds.max(1))
}

fn sanitize_token(value: &str) -> String {
    // NATS KV keys and stream subjects are easier to inspect when ids remain
    // recognizable, but arbitrary player/lobby ids may contain separators that
    // would change the key hierarchy. Preserve safe characters and flatten the
    // rest.
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightyear_matchmaker_core::{
        LatencyReport, LatencyTransport, PlayerId, RequestId, RoomSelection, ServerEndpoint,
    };
    use std::collections::BTreeMap;

    #[test]
    fn names_are_namespaced_and_sanitized() {
        let names = NatsNames::new(Some("demo/dev".to_string()));
        assert_eq!(names.bucket("server_capacity"), "demo_dev_server_capacity");
        assert_eq!(names.lifecycle_subject_all(), "demo_dev.lifecycle.>");
        assert_eq!(
            names.lifecycle_release_allocation_subject(),
            "demo_dev.lifecycle.release_allocation"
        );
        assert_eq!(names.key_server(&ServerId::new("local/dev")), "local_dev");
        assert_eq!(
            names.key_connection(&ServerId::new("local/dev"), LightyearClientId::new(7)),
            "local_dev.7"
        );
    }

    #[test]
    fn ttl_config_defaults_are_applied_when_partially_deserialized() {
        let config: NatsConfig = serde_json::from_value(serde_json::json!({
            "url": "nats://example:4222",
            "ttl": {
                "assignments_secs": 90
            }
        }))
        .unwrap();

        assert_eq!(config.ttl.server_readiness_secs, 30);
        assert_eq!(config.ttl.server_capacity_secs, 30);
        assert_eq!(config.ttl.assignments_secs, 90);
        assert_eq!(config.ttl.assignments_prepared_secs, 60);
        assert_eq!(config.ttl.active_connections_secs, 30);
        assert_eq!(config.ttl.lifecycle_work_secs, 600);
    }

    #[test]
    fn lifecycle_work_payloads_round_trip_with_kind_tags() {
        let work = LifecycleWork::ReleaseAllocation(ReleaseAllocationWork {
            request_id: Some(RequestId::new("request-1")),
            allocation_id: AllocationId::new("allocation-1"),
            assignment_id: Some(AssignmentId::new("assignment-1")),
            server_id: Some(ServerId::new("server-1")),
            provider: Some(ProviderKind::Edgegap),
            reason: Some("prepare timeout".to_string()),
        });
        let json = serde_json::to_value(&work).unwrap();

        assert_eq!(json["kind"], "release_allocation");
        assert_eq!(json["payload"]["request_id"], "request-1");
        assert_eq!(json["payload"]["allocation_id"], "allocation-1");
        assert_eq!(serde_json::from_value::<LifecycleWork>(json).unwrap(), work);
    }

    #[test]
    fn live_static_capacity_prefers_ready_capacity_with_matching_region_latency() {
        let request = AllocationRequest {
            request_id: RequestId::new("request-1"),
            game: "demo".to_string(),
            version: "dev".to_string(),
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            room: RoomSelection::Auto,
            latencies: vec![
                LatencyReport {
                    region: "local".to_string(),
                    rtt_ms: 10,
                    transport: LatencyTransport::Http,
                },
                LatencyReport {
                    region: "remote".to_string(),
                    rtt_ms: 100,
                    transport: LatencyTransport::Http,
                },
            ],
        };
        let selected = choose_live_static_capacity(
            vec![
                capacity("remote", true, 0, 64, Some("remote")),
                capacity("local", true, 5, 64, Some("local")),
            ],
            &request,
        )
        .unwrap();

        assert_eq!(selected.server_id, ServerId::new("local"));
    }

    #[test]
    fn live_static_capacity_rejects_unready_and_full_servers() {
        let request = AllocationRequest {
            request_id: RequestId::new("request-2"),
            game: "demo".to_string(),
            version: "dev".to_string(),
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            room: RoomSelection::Auto,
            latencies: Vec::new(),
        };
        let selected = choose_live_static_capacity(
            vec![
                capacity("unready", false, 0, 64, Some("local")),
                capacity("full", true, 64, 64, Some("local")),
                capacity("ready", true, 63, 64, Some("local")),
            ],
            &request,
        )
        .unwrap();

        assert_eq!(selected.server_id, ServerId::new("ready"));
    }

    fn capacity(
        id: &str,
        ready: bool,
        total_players: u32,
        max_players: u32,
        region: Option<&str>,
    ) -> ServerCapacity {
        ServerCapacity {
            server_id: ServerId::new(id),
            provider: ProviderKind::Static,
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            game: "demo".to_string(),
            version: "dev".to_string(),
            ready,
            total_players,
            max_players,
            max_rooms: 4,
            region: region.map(ToOwned::to_owned),
            cert_digest: None,
            cpu_percent: None,
            rooms: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }
}
