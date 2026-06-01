//! NATS JetStream coordination backend.
//!
//! This crate stores ephemeral coordination state shared by the matchmaker and
//! game servers. It is not a persistent player, lobby, or progression database.
//! The current buckets hold game-server readiness, server/room capacity,
//! server-keyed pending assignments, client lookup indexes, assignment-prepared
//! acknowledgements, active connection reports, and operator/server drain
//! markers.
//!
//! The runtime split is:
//!
//! - the matchmaker writes assignments and watches preparation acknowledgements,
//! - game servers publish readiness/capacity, consume their server-keyed
//!   assignments, and publish active-connection changes,
//! - providers release external capacity through durable lifecycle work when an
//!   assignment attempt fails or expires.
//!
//! Terminology used by the coordination model:
//!
//! - A capacity report is metadata published by a game server describing how
//!   much space it currently has.
//! - An assignment is a matchmaker request for a server to accept one Lightyear
//!   client id.
//! - `AssignmentPrepared` means the game server installed local admission state;
//!   the pending assignment can be consumed and active connections become the
//!   runtime state.
//! - A reservation is a matchmaker-owned hold on capacity. Full reservation
//!   accounting is still separate from capacity reports and will use NATS
//!   compare-and-set operations rather than being published by the server.

#![allow(async_fn_in_trait)]

use async_nats::jetstream::{
    self,
    kv::{Operation, Store},
    stream::Stream as JetStreamStream,
};
pub use config::{NatsConfig, NatsTtlConfig};
use futures_util::StreamExt as _;
pub use lifecycle::{DeleteAssignmentWork, LifecycleWork, ReleaseAllocationWork};
use lightyear_matchmaker_core::Result as CoreResult;
use lightyear_matchmaker_core::{
    ActiveConnection, AllocationId, AllocationRequest, AssignmentId, AssignmentPrepared,
    AssignmentRecord, CapacityQuery, GameServerReport, LightyearClientId, MatchmakerError,
    ProviderKind, RoomSelection, ServerAllocation, ServerCapacity, ServerDrain, ServerId,
    ServerProvider, ServerReadiness,
};
pub use names::NatsNames;
use names::sanitize_token;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::time::Duration;
use thiserror::Error;

mod config;
mod lifecycle;
mod names;

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
    server_drains: Store,
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
        let server_drains = get_or_create_bucket(
            &js,
            names.bucket("server_drains"),
            "Game-server drain markers that suppress new assignment placement",
            ttl_duration(ttl.server_drains_secs),
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
            server_drains,
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

    /// Deletes all coordination state for a specific assignment.
    ///
    /// This is the low-level deletion path used by lifecycle workers when they
    /// know the assignment id. It removes the server-keyed assignment, the
    /// client secondary index, and the preparation acknowledgement.
    pub async fn delete_assignment(
        &self,
        server_id: Option<&ServerId>,
        assignment_id: &AssignmentId,
        client_id: LightyearClientId,
    ) -> Result<()> {
        if let Some(server_id) = server_id {
            self.delete_assignment_for_server(server_id, assignment_id)
                .await?;
        }
        delete_key(
            &self.assignments_by_client,
            self.names.key_client(client_id),
        )
        .await?;
        self.delete_assignment_prepared(assignment_id).await
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

    /// Consumes a prepared assignment from the server/client assignment queues.
    ///
    /// A prepared acknowledgement means the game server has observed the
    /// assignment and installed local admission state. From that point the
    /// authoritative runtime signal becomes `active_connections`; the pending
    /// assignment queue should no longer advertise the work to this or any
    /// restarted server bridge. The prepared acknowledgement itself is retained
    /// until timeout or explicit cleanup so the matchmaker can observe it.
    pub async fn consume_prepared_assignment(&self, prepared: &AssignmentPrepared) -> Result<()> {
        self.delete_assignment_for_server(&prepared.server_id, &prepared.assignment_id)
            .await?;
        delete_key(
            &self.assignments_by_client,
            self.names.key_client(prepared.client_id),
        )
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

    /// Marks a game server as draining.
    ///
    /// Drained servers are still allowed to publish capacity/readiness, but
    /// allocators should treat the marker as operator intent to stop placing
    /// new assignments there.
    pub async fn publish_server_drain(&self, drain: &ServerDrain) -> Result<()> {
        put_json(
            &self.server_drains,
            self.names.key_server(&drain.server_id),
            drain,
        )
        .await
    }

    /// Clears a game-server drain marker.
    pub async fn clear_server_drain(&self, server_id: &ServerId) -> Result<()> {
        if self.server_drain(server_id).await?.is_some() {
            delete_key(&self.server_drains, self.names.key_server(server_id)).await?;
        }
        Ok(())
    }

    /// Reads a game-server drain marker.
    pub async fn server_drain(&self, server_id: &ServerId) -> Result<Option<ServerDrain>> {
        get_json(&self.server_drains, self.names.key_server(server_id)).await
    }

    /// Returns whether a game server is currently marked as draining.
    pub async fn is_server_draining(&self, server_id: &ServerId) -> Result<bool> {
        Ok(self.server_drain(server_id).await?.is_some())
    }

    /// Lists all server ids currently marked as draining.
    pub async fn drained_server_ids(&self) -> Result<BTreeSet<ServerId>> {
        let mut keys = self
            .server_drains
            .keys()
            .await
            .map_err(|error| NatsCoordinatorError::Nats(error.to_string()))?;
        let mut server_ids = BTreeSet::new();

        while let Some(key) = keys.next().await {
            let key = key.map_err(|error| NatsCoordinatorError::Nats(error.to_string()))?;
            let Some(drain) = get_json::<ServerDrain>(&self.server_drains, key).await? else {
                continue;
            };
            server_ids.insert(drain.server_id);
        }

        Ok(server_ids)
    }

    /// Cancels all pending assignments for a drained or unavailable server.
    ///
    /// This removes the authoritative server assignment set plus client
    /// secondary indexes and preparation acknowledgements for the returned
    /// records. It does not delete active connection reports; connected clients
    /// are runtime state owned by the game server.
    pub async fn cancel_assignments_for_server(
        &self,
        server_id: &ServerId,
    ) -> Result<Vec<AssignmentRecord>> {
        let assignments = self.assignments_for_server(server_id).await?;
        self.update_server_assignments(server_id, |assignments| {
            assignments.assignments.clear();
        })
        .await?;
        for assignment in &assignments {
            delete_key(
                &self.assignments_by_client,
                self.names.key_client(assignment.client_id),
            )
            .await?;
            if self
                .assignment_prepared(&assignment.assignment_id)
                .await?
                .is_some()
            {
                self.delete_assignment_prepared(&assignment.assignment_id)
                    .await?;
            }
        }
        Ok(assignments)
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
        self.lifecycle_work_consumer_with_max_deliver(durable_name, 10)
            .await
    }

    /// Opens or creates a durable pull consumer with an explicit max-deliver value.
    pub async fn lifecycle_work_consumer_with_max_deliver(
        &self,
        durable_name: impl AsRef<str>,
        max_deliver: i64,
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
                    max_deliver: max_deliver.max(1),
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
                self.publish_assignment_prepared(prepared).await?;
                self.consume_prepared_assignment(prepared).await
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
        let drained_server_ids = self
            .coordinator
            .drained_server_ids()
            .await
            .map_err(|error| MatchmakerError::Provider(error.to_string()))?;
        let Some(capacity) =
            choose_live_static_capacity_with_drains(capacities, &request, &drained_server_ids)
        else {
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

#[cfg(test)]
fn choose_live_static_capacity(
    capacities: Vec<ServerCapacity>,
    request: &AllocationRequest,
) -> Option<ServerCapacity> {
    choose_live_static_capacity_with_drains(capacities, request, &BTreeSet::new())
}

fn choose_live_static_capacity_with_drains(
    mut capacities: Vec<ServerCapacity>,
    request: &AllocationRequest,
    drained_server_ids: &BTreeSet<ServerId>,
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
    let candidates = capacities
        .into_iter()
        .filter(|capacity| {
            accepts_live_static_capacity_base(capacity, request)
                && !drained_server_ids.contains(&capacity.server_id)
        })
        .collect::<Vec<_>>();

    match &request.room {
        RoomSelection::Auto => candidates.into_iter().next(),
        RoomSelection::New => candidates
            .into_iter()
            .find(ServerCapacity::has_room_capacity),
        RoomSelection::Code(_) | RoomSelection::Id(_) => {
            choose_explicit_room_capacity(candidates, request)
        }
    }
}

fn accepts_live_static_capacity_base(
    capacity: &ServerCapacity,
    request: &AllocationRequest,
) -> bool {
    capacity.provider == ProviderKind::Static
        && capacity.ready
        && capacity.game == request.game
        && capacity.version == request.version
        && capacity.has_player_capacity()
        && !request.avoids_server(&capacity.server_id)
}

fn choose_explicit_room_capacity(
    candidates: Vec<ServerCapacity>,
    request: &AllocationRequest,
) -> Option<ServerCapacity> {
    let room_key = request.room.room_key()?;
    let room_exists = candidates
        .iter()
        .any(|capacity| capacity.rooms.iter().any(|room| room.key == room_key));

    if room_exists {
        // Keep all players for one explicit room on the server that already
        // hosts it. If the room exists but is full, do not create a second room
        // with the same key on another server.
        candidates.into_iter().find(|capacity| {
            capacity
                .rooms
                .iter()
                .any(|room| room.key == room_key && room.has_player_capacity())
        })
    } else {
        // The requested room does not exist yet, so select a server that can
        // create it during assignment preparation or on first client join.
        candidates
            .into_iter()
            .find(ServerCapacity::has_room_capacity)
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

#[cfg(test)]
mod tests {
    use super::*;
    use lightyear_matchmaker_core::{
        LatencyReport, LatencyTransport, PlayerId, RequestId, RoomSelection, ServerEndpoint,
        ServerRoomMetrics,
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
        assert_eq!(config.ttl.server_drains_secs, 86_400);
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
            avoid_server_ids: Vec::new(),
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
    fn live_static_capacity_skips_avoided_servers() {
        let request = AllocationRequest {
            request_id: RequestId::new("request-avoid"),
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
            avoid_server_ids: vec![ServerId::new("local")],
        };
        let selected = choose_live_static_capacity(
            vec![
                capacity("remote", true, 0, 64, Some("remote")),
                capacity("local", true, 0, 64, Some("local")),
            ],
            &request,
        )
        .unwrap();

        assert_eq!(selected.server_id, ServerId::new("remote"));
    }

    #[test]
    fn live_static_capacity_skips_drained_servers() {
        let request = AllocationRequest {
            request_id: RequestId::new("request-drain"),
            game: "demo".to_string(),
            version: "dev".to_string(),
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            room: RoomSelection::Auto,
            latencies: Vec::new(),
            avoid_server_ids: Vec::new(),
        };
        let selected = choose_live_static_capacity_with_drains(
            vec![
                capacity("drained", true, 0, 64, Some("local")),
                capacity("available", true, 1, 64, Some("local")),
            ],
            &request,
            &BTreeSet::from([ServerId::new("drained")]),
        )
        .unwrap();

        assert_eq!(selected.server_id, ServerId::new("available"));
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
            avoid_server_ids: Vec::new(),
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

    #[test]
    fn live_static_capacity_creates_missing_explicit_room_on_available_server() {
        let request = room_request("request-3", RoomSelection::Code("ABCD".to_string()));
        let selected = choose_live_static_capacity(
            vec![
                capacity_with_rooms(
                    "full-rooms",
                    true,
                    0,
                    64,
                    1,
                    vec![room("id:1", 0, 8)],
                    Some("local"),
                ),
                capacity("can-create", true, 1, 64, Some("local")),
            ],
            &request,
        )
        .unwrap();

        assert_eq!(selected.server_id, ServerId::new("can-create"));
    }

    #[test]
    fn live_static_capacity_prefers_existing_explicit_room() {
        let request = room_request("request-4", RoomSelection::Code("ABCD".to_string()));
        let selected = choose_live_static_capacity(
            vec![
                capacity("lower-latency-empty", true, 0, 64, Some("local")),
                capacity_with_rooms(
                    "existing-room",
                    true,
                    2,
                    64,
                    4,
                    vec![room("code:ABCD", 2, 8)],
                    Some("remote"),
                ),
            ],
            &request,
        )
        .unwrap();

        assert_eq!(selected.server_id, ServerId::new("existing-room"));
    }

    #[test]
    fn live_static_capacity_does_not_duplicate_full_explicit_room() {
        let request = room_request("request-5", RoomSelection::Id("42".to_string()));
        let selected = choose_live_static_capacity(
            vec![
                capacity_with_rooms(
                    "existing-full-room",
                    true,
                    8,
                    64,
                    4,
                    vec![room("id:42", 8, 8)],
                    Some("local"),
                ),
                capacity("can-create", true, 0, 64, Some("local")),
            ],
            &request,
        );

        assert!(selected.is_none());
    }

    fn room_request(id: &str, room: RoomSelection) -> AllocationRequest {
        AllocationRequest {
            request_id: RequestId::new(id),
            game: "demo".to_string(),
            version: "dev".to_string(),
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            room,
            latencies: Vec::new(),
            avoid_server_ids: Vec::new(),
        }
    }

    fn capacity(
        id: &str,
        ready: bool,
        total_players: u32,
        max_players: u32,
        region: Option<&str>,
    ) -> ServerCapacity {
        capacity_with_rooms(id, ready, total_players, max_players, 4, Vec::new(), region)
    }

    fn capacity_with_rooms(
        id: &str,
        ready: bool,
        total_players: u32,
        max_players: u32,
        max_rooms: u32,
        rooms: Vec<ServerRoomMetrics>,
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
            max_rooms,
            region: region.map(ToOwned::to_owned),
            cert_digest: None,
            cpu_percent: None,
            rooms,
            metadata: BTreeMap::new(),
        }
    }

    fn room(key: &str, players: u32, max_players: u32) -> ServerRoomMetrics {
        ServerRoomMetrics {
            key: key.to_string(),
            private: key.starts_with("code:"),
            players,
            max_players,
        }
    }
}
