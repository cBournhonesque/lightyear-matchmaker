//! Deployable Axum matchmaker server.
//!
//! This crate is the client-facing matchmaker process. It accepts websocket
//! play/lobby requests, resolves lightweight player identity, keeps the current
//! in-process lobby state, asks the configured provider for server capacity,
//! persists assignments through the optional NATS coordination backend, waits
//! for game-server preparation when configured, and issues Lightyear connection
//! grants.
//!
//! The matchmaker is intentionally not the game server and not the provider. The
//! game server runs the authoritative game, publishes readiness/capacity, polls
//! assignments, prepares local admission state, and reports connections. The
//! provider owns capacity outside the matchmaker, such as configured static
//! servers or Edgegap sessions/deployments.

use anyhow::Context as _;
use axum::Json;
use axum::Router;
use axum::extract::Path as AxumPath;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use futures_util::StreamExt as _;
use lightyear_matchmaker_core::{
    AllocationRequest, AssignmentId, AssignmentRecord, AssignmentRosterMember, AssignmentState,
    CapacityQuery, ClientMessage, ErrorCode, IdentityRequest, IdentityResolver, IpIdentityResolver,
    LatencyReport, LifecycleWorkState, LightyearClientId, Lobby, LobbyId, MatchmakerError,
    PlayerId, PlayerSummary, RequestId, ResolvedIdentity, Result, RoomSelection, ServerAllocation,
    ServerDrain, ServerId, ServerMessage, ServerProvider, TokenIssuer, TokenRequest,
    is_supported_websocket_protocol_version,
};
use lightyear_matchmaker_lightyear::NetcodeTokenIssuer;
use lightyear_matchmaker_nats::{
    DeleteAssignmentWork, LifecycleWork, NatsCoordinator, NatsStaticServerProvider,
    ReleaseAllocationWork,
};
use lightyear_matchmaker_provider_edgegap::{EdgegapProvider, MockEdgegapProvider};
use lightyear_matchmaker_provider_static::StaticServerProvider;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc as tokio_mpsc;
use tracing::{debug, error, info, warn};

mod config;
mod lobby;
mod metrics;

pub use config::{
    AllocationConfig, AllocationSource, GameConfig, IdentityConfig, MatchmakerConfig, ServerConfig,
};
use lobby::{LobbyRuntime, OutboundMessages};
use metrics::{
    DrainRequest, GlobalDrainStatus, MetricsSnapshot, ReadinessStatus, RuntimeMetrics,
    ServerDrainResponse,
};

const OPENAPI_YAML: &str = include_str!("../../../docs/openapi.yaml");
const ASYNCAPI_YAML: &str = include_str!("../../../docs/asyncapi.yaml");

#[derive(Clone)]
/// Fully wired application state used by the Axum router.
pub struct AppState {
    game: GameConfig,
    allocation: AllocationConfig,
    identity: IpIdentityResolver,
    lobbies: LobbyRuntime,
    provider: ProviderRouter,
    token_issuer: NetcodeTokenIssuer,
    coordination: Option<NatsCoordinator>,
    metrics: RuntimeMetrics,
    draining: Arc<AtomicBool>,
    drain_reason: Arc<Mutex<Option<String>>>,
    drained_servers: Arc<Mutex<BTreeMap<ServerId, ServerDrain>>>,
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn new_request_id() -> RequestId {
    RequestId::new(format!("req-{:032x}", rand::random::<u128>()))
}

fn assignment_id(request_id: &RequestId, client_id: LightyearClientId) -> AssignmentId {
    AssignmentId::new(format!("assignment:{}:{}", request_id, client_id))
}

fn avoid_failed_server(request: &mut AllocationRequest, server_id: &ServerId) {
    if !request.avoids_server(server_id) {
        request.avoid_server_ids.push(server_id.clone());
    }
}

fn log_assignment_state_transition(
    assignment: &AssignmentRecord,
    from: AssignmentState,
    to: AssignmentState,
    reason: &'static str,
) {
    let valid = from.transition_to(to).is_some();
    if valid {
        info!(
            assignment_id = %assignment.assignment_id,
            request_id = %assignment.request_id,
            allocation_id = %assignment.allocation_id,
            server_id = %assignment.server_id,
            client_id = %assignment.client_id,
            player_id = %assignment.player_id,
            lobby_id = assignment.lobby_id.as_ref().map(ToString::to_string),
            state_from = from.as_str(),
            state_to = to.as_str(),
            reason = reason,
            "assignment.state_transition"
        );
    } else {
        warn!(
            assignment_id = %assignment.assignment_id,
            request_id = %assignment.request_id,
            allocation_id = %assignment.allocation_id,
            server_id = %assignment.server_id,
            client_id = %assignment.client_id,
            player_id = %assignment.player_id,
            lobby_id = assignment.lobby_id.as_ref().map(ToString::to_string),
            state_from = from.as_str(),
            state_to = to.as_str(),
            reason = reason,
            "assignment.invalid_state_transition"
        );
    }
}

fn log_lifecycle_work_state_transition(
    work: &LifecycleWork,
    from: LifecycleWorkState,
    to: LifecycleWorkState,
    delivery_attempt: i64,
    max_deliver: i64,
    reason: &'static str,
) {
    let valid = from.transition_to(to).is_some();
    let request_id = lifecycle_work_request_id(work);
    let assignment_id = lifecycle_work_assignment_id(work);
    let allocation_id = lifecycle_work_allocation_id(work);
    let server_id = lifecycle_work_server_id(work);

    if valid {
        info!(
            work_kind = work.kind(),
            request_id = request_id.as_deref(),
            assignment_id = assignment_id.as_deref(),
            allocation_id = allocation_id.as_deref(),
            server_id = server_id.as_deref(),
            state_from = from.as_str(),
            state_to = to.as_str(),
            delivery_attempt,
            max_deliver,
            reason = reason,
            "lifecycle_work.state_transition"
        );
    } else {
        warn!(
            work_kind = work.kind(),
            request_id = request_id.as_deref(),
            assignment_id = assignment_id.as_deref(),
            allocation_id = allocation_id.as_deref(),
            server_id = server_id.as_deref(),
            state_from = from.as_str(),
            state_to = to.as_str(),
            delivery_attempt,
            max_deliver,
            reason = reason,
            "lifecycle_work.invalid_state_transition"
        );
    }
}

fn lifecycle_work_request_id(work: &LifecycleWork) -> Option<String> {
    match work {
        LifecycleWork::ReleaseAllocation(work) => work.request_id.as_ref(),
        LifecycleWork::DeleteAssignment(work) => work.request_id.as_ref(),
    }
    .map(ToString::to_string)
}

fn lifecycle_work_assignment_id(work: &LifecycleWork) -> Option<String> {
    match work {
        LifecycleWork::ReleaseAllocation(work) => work.assignment_id.as_ref(),
        LifecycleWork::DeleteAssignment(work) => Some(&work.assignment_id),
    }
    .map(ToString::to_string)
}

fn lifecycle_work_allocation_id(work: &LifecycleWork) -> Option<String> {
    match work {
        LifecycleWork::ReleaseAllocation(work) => Some(work.allocation_id.to_string()),
        LifecycleWork::DeleteAssignment(_) => None,
    }
}

fn lifecycle_work_server_id(work: &LifecycleWork) -> Option<String> {
    match work {
        LifecycleWork::ReleaseAllocation(work) => work.server_id.as_ref(),
        LifecycleWork::DeleteAssignment(work) => work.server_id.as_ref(),
    }
    .map(ToString::to_string)
}

#[derive(Default)]
struct ClientSession {
    identity: Option<ResolvedIdentity>,
    lobby_id: Option<LobbyId>,
}

#[derive(Clone)]
enum ProviderRouter {
    ConfiguredStatic(StaticServerProvider),
    NatsStatic(Box<NatsStaticServerProvider>),
    Edgegap(EdgegapProvider),
    EdgegapMock(MockEdgegapProvider),
}

impl ProviderRouter {
    async fn allocate(&self, request: AllocationRequest) -> Result<ServerAllocation> {
        match self {
            Self::ConfiguredStatic(provider) => provider.allocate(request).await,
            Self::NatsStatic(provider) => provider.allocate(request).await,
            Self::Edgegap(provider) => provider.allocate(request).await,
            Self::EdgegapMock(provider) => provider.allocate(request).await,
        }
    }

    async fn release(&self, allocation_id: lightyear_matchmaker_core::AllocationId) -> Result<()> {
        match self {
            Self::ConfiguredStatic(provider) => provider.release(allocation_id).await,
            Self::NatsStatic(provider) => provider.release(allocation_id).await,
            Self::Edgegap(provider) => provider.release(allocation_id).await,
            Self::EdgegapMock(provider) => provider.release(allocation_id).await,
        }
    }
}

impl AppState {
    /// Builds application state from config without creating NATS coordination.
    pub fn from_config(config: MatchmakerConfig) -> Result<Self> {
        if config.nats.is_some() {
            return Err(MatchmakerError::Config(
                "NATS coordination requires AppState::from_config_with_coordination".to_string(),
            ));
        }
        Self::from_config_parts(config, None)
    }

    fn from_config_parts(
        config: MatchmakerConfig,
        coordination: Option<NatsCoordinator>,
    ) -> Result<Self> {
        let provider = match config.allocation.source {
            AllocationSource::ConfiguredStatic => {
                ProviderRouter::ConfiguredStatic(StaticServerProvider::new(config.static_provider))
            }
            AllocationSource::NatsStatic => {
                let Some(coordination) = coordination.clone() else {
                    return Err(MatchmakerError::Config(
                        "allocation.source = \"nats_static\" requires [nats] configuration"
                            .to_string(),
                    ));
                };
                ProviderRouter::NatsStatic(Box::new(NatsStaticServerProvider::new(coordination)))
            }
            AllocationSource::Edgegap => {
                ProviderRouter::Edgegap(EdgegapProvider::new(config.edgegap_provider)?)
            }
            AllocationSource::EdgegapMock => {
                ProviderRouter::EdgegapMock(MockEdgegapProvider::new(config.edgegap_provider))
            }
        };
        Ok(Self {
            game: config.game,
            allocation: config.allocation,
            identity: IpIdentityResolver::new(config.identity.trust_forwarded_for),
            lobbies: LobbyRuntime::default(),
            provider,
            token_issuer: NetcodeTokenIssuer::from_config(config.lightyear)?,
            coordination,
            metrics: RuntimeMetrics::default(),
            draining: Arc::new(AtomicBool::new(false)),
            drain_reason: Arc::new(Mutex::new(None)),
            drained_servers: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Builds application state from config and connects to NATS when configured.
    pub async fn from_config_with_coordination(config: MatchmakerConfig) -> Result<Self> {
        let nats = config.nats.clone();
        let coordination = if let Some(config) = nats {
            Some(
                NatsCoordinator::connect(config)
                    .await
                    .map_err(coordination_error)?,
            )
        } else {
            None
        };
        Self::from_config_parts(config, coordination)
    }

    /// Handles a request-play message without using a live WebSocket.
    ///
    /// This is mainly useful for unit tests and embedders that want the same
    /// assignment flow as the WebSocket path.
    pub async fn request_play(
        &self,
        identity_request: IdentityRequest,
        message: ClientMessage,
    ) -> Result<Vec<ServerMessage>> {
        if self.is_draining() {
            return Err(MatchmakerError::Draining(
                self.drain_reason()
                    .unwrap_or_else(|| "matchmaker is draining".to_string()),
            ));
        }

        let identity = self.identity.resolve(identity_request).await?;
        let mut responses = vec![ServerMessage::IdentityResolved {
            player: PlayerSummary {
                id: identity.player_id.clone(),
                display_name: identity.display_name,
            },
        }];

        let ClientMessage::RequestPlay {
            game,
            version,
            room,
            latencies,
        } = message
        else {
            return Ok(responses);
        };
        RuntimeMetrics::inc(&self.metrics.inner.request_play_started);

        let allocation_request = self.allocation_request(
            new_request_id(),
            identity.player_id.clone(),
            game,
            version,
            room,
            latencies,
        );
        let (assignment, allocation) = self
            .create_prepared_assignment(
                identity.player_id.clone(),
                allocation_request,
                |assignment| {
                    responses.push(ServerMessage::AssignmentPreparing {
                        assignment_id: assignment.assignment_id.to_string(),
                    });
                    Ok(())
                },
            )
            .await?;
        let grant = self
            .issue_connection_grant(allocation, identity.player_id.clone(), assignment.client_id)
            .await?;
        responses.push(ServerMessage::AssignmentReady { connect: grant });
        RuntimeMetrics::inc(&self.metrics.inner.assignments_ready);
        log_assignment_state_transition(
            &assignment,
            AssignmentState::Prepared,
            AssignmentState::Ready,
            "connection grant returned to request-play caller",
        );
        Ok(responses)
    }

    fn allocation_request(
        &self,
        request_id: RequestId,
        player_id: PlayerId,
        game: String,
        version: String,
        room: RoomSelection,
        latencies: Vec<lightyear_matchmaker_core::LatencyReport>,
    ) -> AllocationRequest {
        let game = if game.is_empty() {
            self.game.name.clone()
        } else {
            game
        };
        let version = if version.is_empty() {
            self.game.version.clone()
        } else {
            version
        };
        AllocationRequest {
            request_id,
            game,
            version,
            player_id,
            lobby_id: None,
            room,
            latencies,
            avoid_server_ids: self.local_drained_server_ids(),
        }
    }

    async fn create_assignment(
        &self,
        player_id: PlayerId,
        request: AllocationRequest,
    ) -> Result<(AssignmentRecord, ServerAllocation)> {
        let allocation = self.provider.allocate(request.clone()).await?;
        let client_id = LightyearClientId::new(rand::random::<u64>());
        let assignment = AssignmentRecord {
            assignment_id: assignment_id(&request.request_id, client_id),
            request_id: request.request_id.clone(),
            allocation_id: allocation.allocation_id.clone(),
            server_id: allocation.server_id.clone(),
            client_id,
            player_id: player_id.clone(),
            lobby_id: request.lobby_id.clone(),
            team: Some("solo".to_string()),
            roster: vec![AssignmentRosterMember {
                player_id: player_id.clone(),
                client_id: Some(client_id),
                team: Some("solo".to_string()),
                metadata: BTreeMap::new(),
            }],
            match_metadata: assignment_match_metadata(&request),
            metadata: allocation.metadata.clone(),
        };
        self.put_assignment(&assignment).await?;
        RuntimeMetrics::inc(&self.metrics.inner.assignments_created);
        info!(
            assignment_id = %assignment.assignment_id,
            request_id = %assignment.request_id,
            server_id = %assignment.server_id,
            client_id = %assignment.client_id,
            player_id = %assignment.player_id,
            lobby_id = assignment.lobby_id.as_ref().map(ToString::to_string),
            "assignment.created"
        );
        log_assignment_state_transition(
            &assignment,
            AssignmentState::Created,
            AssignmentState::Persisted,
            "assignment persisted for game-server preparation",
        );
        Ok((assignment, allocation))
    }

    async fn create_prepared_assignment(
        &self,
        player_id: PlayerId,
        request: AllocationRequest,
        on_preparing: impl FnMut(&AssignmentRecord) -> Result<()>,
    ) -> Result<(AssignmentRecord, ServerAllocation)> {
        self.create_prepared_assignment_with_retries(
            player_id,
            request,
            self.assignment_prepare_max_retries(),
            on_preparing,
        )
        .await
    }

    async fn create_prepared_assignment_with_retries(
        &self,
        player_id: PlayerId,
        mut request: AllocationRequest,
        max_retries: u32,
        mut on_preparing: impl FnMut(&AssignmentRecord) -> Result<()>,
    ) -> Result<(AssignmentRecord, ServerAllocation)> {
        for attempt in 0..=max_retries {
            if attempt > 0 {
                request.request_id = new_request_id();
            }

            let (assignment, allocation) = self
                .create_assignment(player_id.clone(), request.clone())
                .await?;
            if let Err(error) = on_preparing(&assignment) {
                log_assignment_state_transition(
                    &assignment,
                    AssignmentState::Persisted,
                    AssignmentState::Failed,
                    "failed to notify client that assignment preparation started",
                );
                self.queue_failed_assignment_cleanup(&assignment, &allocation, error.to_string())
                    .await;
                return Err(error);
            }
            log_assignment_state_transition(
                &assignment,
                AssignmentState::Persisted,
                AssignmentState::Preparing,
                "client notified that assignment preparation started",
            );

            match self.wait_for_assignment_prepared(&assignment).await {
                Ok(()) => {
                    if attempt > 0 {
                        info!(
                            assignment_id = %assignment.assignment_id,
                            request_id = %assignment.request_id,
                            attempt = attempt + 1,
                            max_retries,
                            "assignment.prepare_retry_succeeded"
                        );
                    }
                    return Ok((assignment, allocation));
                }
                Err(error) if attempt < max_retries => {
                    let reason = error.to_string();
                    self.queue_failed_assignment_cleanup(&assignment, &allocation, reason.clone())
                        .await;
                    avoid_failed_server(&mut request, &allocation.server_id);
                    RuntimeMetrics::inc(&self.metrics.inner.assignment_prepare_retries);
                    warn!(
                        assignment_id = %assignment.assignment_id,
                        request_id = %assignment.request_id,
                        server_id = %assignment.server_id,
                        client_id = %assignment.client_id,
                        attempt = attempt + 1,
                        next_attempt = attempt + 2,
                        max_retries,
                        error = %reason,
                        "assignment.prepare_retrying"
                    );
                    self.sleep_assignment_retry_backoff().await;
                }
                Err(error) => {
                    self.queue_failed_assignment_cleanup(
                        &assignment,
                        &allocation,
                        error.to_string(),
                    )
                    .await;
                    return Err(error);
                }
            }
        }

        unreachable!("assignment retry loop always returns from the final attempt")
    }

    async fn assign_lobby(&self, lobby: Lobby) -> Result<()> {
        let request = AllocationRequest {
            request_id: new_request_id(),
            game: lobby.game.clone(),
            version: lobby.version.clone(),
            player_id: lobby.owner.clone(),
            lobby_id: Some(lobby.id.clone()),
            room: RoomSelection::New,
            latencies: aggregate_lobby_latencies(&lobby),
            avoid_server_ids: self.local_drained_server_ids(),
        };
        let monitor_request = request.clone();
        let monitor_lobby = lobby.clone();
        let (assignments, allocation) = self
            .create_prepared_lobby_assignments(&lobby, request)
            .await?;
        let monitor_assignments = assignments.clone();
        let monitor_allocation = allocation.clone();

        for (player_id, assignment) in assignments {
            let grant = self
                .issue_connection_grant(allocation.clone(), player_id.clone(), assignment.client_id)
                .await?;
            self.lobbies.send_to_player(
                &player_id,
                ServerMessage::AssignmentReady { connect: grant },
            );
            RuntimeMetrics::inc(&self.metrics.inner.assignments_ready);
            log_assignment_state_transition(
                &assignment,
                AssignmentState::Prepared,
                AssignmentState::Ready,
                "connection grant sent to lobby member",
            );
            info!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                client_id = %assignment.client_id,
                player_id = %player_id,
                "assignment.ready"
            );
        }
        if self.allocation.require_assignment_prepare && self.coordination.is_some() {
            let monitor_state = self.clone();
            tokio::spawn(async move {
                monitor_state
                    .monitor_lobby_assignment_connections(
                        monitor_lobby,
                        monitor_request,
                        monitor_assignments,
                        monitor_allocation,
                    )
                    .await;
            });
        }

        Ok(())
    }

    async fn create_prepared_lobby_assignments(
        &self,
        lobby: &Lobby,
        request: AllocationRequest,
    ) -> Result<(Vec<(PlayerId, AssignmentRecord)>, ServerAllocation)> {
        self.create_prepared_lobby_assignments_with_retries(
            lobby,
            request,
            self.assignment_prepare_max_retries(),
        )
        .await
    }

    async fn create_prepared_lobby_assignments_with_retries(
        &self,
        lobby: &Lobby,
        mut request: AllocationRequest,
        max_retries: u32,
    ) -> Result<(Vec<(PlayerId, AssignmentRecord)>, ServerAllocation)> {
        for attempt in 0..=max_retries {
            if attempt > 0 {
                request.request_id = new_request_id();
            }

            let allocation = self.provider.allocate(request.clone()).await?;
            let assignments = self
                .create_lobby_assignment_records(lobby, &request, &allocation)
                .await?;
            match self
                .wait_for_lobby_assignments_prepared(&assignments, &allocation)
                .await
            {
                Ok(()) => {
                    if attempt > 0 {
                        info!(
                            request_id = %request.request_id,
                            lobby_id = %lobby.id,
                            attempt = attempt + 1,
                            max_retries,
                            "lobby_assignment.prepare_retry_succeeded"
                        );
                    }
                    return Ok((assignments, allocation));
                }
                Err(error) if attempt < max_retries => {
                    avoid_failed_server(&mut request, &allocation.server_id);
                    RuntimeMetrics::inc(&self.metrics.inner.assignment_prepare_retries);
                    warn!(
                        request_id = %request.request_id,
                        lobby_id = %lobby.id,
                        attempt = attempt + 1,
                        next_attempt = attempt + 2,
                        max_retries,
                        error = %error,
                        "lobby_assignment.prepare_retrying"
                    );
                    self.sleep_assignment_retry_backoff().await;
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("lobby assignment retry loop always returns from the final attempt")
    }

    async fn create_lobby_assignment_records(
        &self,
        lobby: &Lobby,
        request: &AllocationRequest,
        allocation: &ServerAllocation,
    ) -> Result<Vec<(PlayerId, AssignmentRecord)>> {
        let member_client_ids = lobby
            .members
            .iter()
            .map(|member| {
                (
                    member.player_id.clone(),
                    LightyearClientId::new(rand::random()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let roster = lobby
            .members
            .iter()
            .map(|member| AssignmentRosterMember {
                player_id: member.player_id.clone(),
                client_id: member_client_ids.get(&member.player_id).copied(),
                team: member.team.clone(),
                metadata: member.metadata.clone(),
            })
            .collect::<Vec<_>>();

        let mut assignments = Vec::new();
        for member in &lobby.members {
            let Some(client_id) = member_client_ids.get(&member.player_id).copied() else {
                continue;
            };
            let assignment = AssignmentRecord {
                assignment_id: assignment_id(&request.request_id, client_id),
                request_id: request.request_id.clone(),
                allocation_id: allocation.allocation_id.clone(),
                server_id: allocation.server_id.clone(),
                client_id,
                player_id: member.player_id.clone(),
                lobby_id: Some(lobby.id.clone()),
                team: member.team.clone(),
                roster: roster.clone(),
                match_metadata: lobby_match_metadata(lobby),
                metadata: allocation.metadata.clone(),
            };
            self.put_assignment(&assignment).await?;
            RuntimeMetrics::inc(&self.metrics.inner.assignments_created);
            info!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                server_id = %assignment.server_id,
                client_id = %assignment.client_id,
                player_id = %assignment.player_id,
                lobby_id = %lobby.id,
                "assignment.created"
            );
            log_assignment_state_transition(
                &assignment,
                AssignmentState::Created,
                AssignmentState::Persisted,
                "lobby assignment persisted for game-server preparation",
            );
            self.lobbies.send_to_player(
                &member.player_id,
                ServerMessage::AssignmentPreparing {
                    assignment_id: assignment.assignment_id.to_string(),
                },
            );
            log_assignment_state_transition(
                &assignment,
                AssignmentState::Persisted,
                AssignmentState::Preparing,
                "lobby member notified that assignment preparation started",
            );
            assignments.push((member.player_id.clone(), assignment));
        }
        Ok(assignments)
    }

    async fn wait_for_lobby_assignments_prepared(
        &self,
        assignments: &[(PlayerId, AssignmentRecord)],
        allocation: &ServerAllocation,
    ) -> Result<()> {
        for (_, assignment) in assignments {
            if let Err(error) = self.wait_for_assignment_prepared(assignment).await {
                self.queue_failed_lobby_attempt_cleanup(assignments, allocation, error.to_string())
                    .await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn put_assignment(&self, assignment: &AssignmentRecord) -> Result<()> {
        let Some(coordination) = &self.coordination else {
            return Ok(());
        };
        coordination
            .put_assignment(assignment)
            .await
            .map_err(coordination_error)
    }

    fn assignment_prepare_max_retries(&self) -> u32 {
        if self.allocation.require_assignment_prepare {
            self.allocation.assignment_prepare_max_retries
        } else {
            0
        }
    }

    fn assignment_timeout(&self) -> Duration {
        Duration::from_secs(self.allocation.assignment_timeout_secs.max(1))
    }

    fn assignment_retry_backoff(&self) -> Duration {
        Duration::from_millis(self.allocation.assignment_retry_backoff_ms)
    }

    async fn sleep_assignment_retry_backoff(&self) {
        let backoff = self.assignment_retry_backoff();
        if !backoff.is_zero() {
            tokio::time::sleep(backoff).await;
        }
    }

    fn lifecycle_job_max_deliver(&self) -> i64 {
        self.allocation.lifecycle_job_max_deliver.max(1)
    }

    fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    fn start_draining(&self, reason: Option<String>) -> GlobalDrainStatus {
        self.draining.store(true, Ordering::Relaxed);
        {
            let mut stored_reason = lock_or_recover(&self.drain_reason);
            *stored_reason = reason.clone();
        }
        info!(reason = reason.as_deref(), "matchmaker.draining_started");
        GlobalDrainStatus {
            draining: true,
            reason,
        }
    }

    fn stop_draining(&self) -> GlobalDrainStatus {
        self.draining.store(false, Ordering::Relaxed);
        {
            let mut stored_reason = lock_or_recover(&self.drain_reason);
            *stored_reason = None;
        }
        info!("matchmaker.draining_stopped");
        GlobalDrainStatus {
            draining: false,
            reason: None,
        }
    }

    fn drain_reason(&self) -> Option<String> {
        lock_or_recover(&self.drain_reason).clone()
    }

    fn local_drained_server_ids(&self) -> Vec<ServerId> {
        lock_or_recover(&self.drained_servers)
            .keys()
            .cloned()
            .collect()
    }

    fn mark_server_drained_locally(&self, drain: ServerDrain) {
        lock_or_recover(&self.drained_servers).insert(drain.server_id.clone(), drain);
    }

    fn clear_server_drained_locally(&self, server_id: &ServerId) {
        lock_or_recover(&self.drained_servers).remove(server_id);
    }

    fn is_server_drained_locally(&self, server_id: &ServerId) -> bool {
        lock_or_recover(&self.drained_servers).contains_key(server_id)
    }

    async fn is_server_draining(&self, server_id: &ServerId) -> Result<bool> {
        if self.is_server_drained_locally(server_id) {
            return Ok(true);
        }
        let Some(coordination) = &self.coordination else {
            return Ok(false);
        };
        coordination
            .is_server_draining(server_id)
            .await
            .map_err(coordination_error)
    }

    async fn drain_game_server(
        &self,
        server_id: ServerId,
        reason: Option<String>,
    ) -> Result<ServerDrainResponse> {
        let drain = ServerDrain {
            server_id: server_id.clone(),
            reason: reason.clone(),
            metadata: BTreeMap::new(),
        };
        self.mark_server_drained_locally(drain.clone());

        let mut canceled_assignments = Vec::new();
        if let Some(coordination) = &self.coordination {
            coordination
                .publish_server_drain(&drain)
                .await
                .map_err(coordination_error)?;
            canceled_assignments = coordination
                .cancel_assignments_for_server(&server_id)
                .await
                .map_err(coordination_error)?;
            self.queue_server_drain_cleanup(&server_id, &canceled_assignments, reason.clone())
                .await;
        }

        let release_jobs_queued = canceled_assignments
            .iter()
            .map(|assignment| assignment.allocation_id.clone())
            .collect::<BTreeSet<_>>()
            .len();
        info!(
            server_id = %server_id,
            canceled_assignments = canceled_assignments.len(),
            release_jobs_queued,
            reason = reason.as_deref(),
            "game_server.drained"
        );
        Ok(ServerDrainResponse {
            server_id,
            draining: true,
            reason,
            nats_configured: self.coordination.is_some(),
            canceled_assignments: canceled_assignments.len(),
            release_jobs_queued,
        })
    }

    async fn undrain_game_server(&self, server_id: ServerId) -> Result<ServerDrainResponse> {
        self.clear_server_drained_locally(&server_id);
        if let Some(coordination) = &self.coordination {
            coordination
                .clear_server_drain(&server_id)
                .await
                .map_err(coordination_error)?;
        }
        info!(server_id = %server_id, "game_server.drain_cleared");
        Ok(ServerDrainResponse {
            server_id,
            draining: false,
            reason: None,
            nats_configured: self.coordination.is_some(),
            canceled_assignments: 0,
            release_jobs_queued: 0,
        })
    }

    fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot(
            self.lobbies.metrics(),
            self.is_draining(),
            self.coordination.is_some(),
        )
    }

    async fn readiness_status(&self) -> ReadinessStatus {
        let draining = self.is_draining();
        let drain_message = self
            .drain_reason()
            .unwrap_or_else(|| "matchmaker is draining".to_string());
        let Some(coordination) = &self.coordination else {
            return ReadinessStatus {
                ready: !draining,
                draining,
                nats_configured: false,
                nats_ok: true,
                message: if draining {
                    drain_message
                } else {
                    "ready".to_string()
                },
            };
        };

        match coordination
            .list_capacity(CapacityQuery {
                game: self.game.name.clone(),
                version: self.game.version.clone(),
            })
            .await
        {
            Ok(_) if !draining => ReadinessStatus {
                ready: true,
                draining,
                nats_configured: true,
                nats_ok: true,
                message: "ready".to_string(),
            },
            Ok(_) => ReadinessStatus {
                ready: false,
                draining,
                nats_configured: true,
                nats_ok: true,
                message: drain_message,
            },
            Err(error) => ReadinessStatus {
                ready: false,
                draining,
                nats_configured: true,
                nats_ok: false,
                message: format!("NATS coordination failed: {error}"),
            },
        }
    }

    async fn wait_for_assignment_prepared(&self, assignment: &AssignmentRecord) -> Result<()> {
        if !self.allocation.require_assignment_prepare {
            RuntimeMetrics::inc(&self.metrics.inner.assignments_prepared);
            log_assignment_state_transition(
                assignment,
                AssignmentState::Preparing,
                AssignmentState::Prepared,
                "assignment preparation not required by config",
            );
            return Ok(());
        }
        let Some(coordination) = &self.coordination else {
            return Err(MatchmakerError::Config(
                "assignment preparation acknowledgement requires [nats] configuration".to_string(),
            ));
        };

        let deadline = Duration::from_millis(self.allocation.assignment_prepare_timeout_ms);
        let poll_interval =
            Duration::from_millis(self.allocation.assignment_prepare_poll_ms.max(1));
        // The game server publishes preparation acks after it has observed the
        // assignment and installed any local validation state. Matching both
        // server id and client id prevents a stale ack for the same assignment
        // key from another server generation from unblocking the request.
        let result = tokio::time::timeout(deadline, async {
            loop {
                if self.is_server_draining(&assignment.server_id).await? {
                    return Err(MatchmakerError::Draining(format!(
                        "game server {} is draining",
                        assignment.server_id
                    )));
                }
                if let Some(prepared) = coordination
                    .assignment_prepared(&assignment.assignment_id)
                    .await
                    .map_err(coordination_error)?
                    && prepared.server_id == assignment.server_id
                    && prepared.client_id == assignment.client_id
                {
                    if prepared.prepared {
                        RuntimeMetrics::inc(&self.metrics.inner.assignments_prepared);
                        log_assignment_state_transition(
                            assignment,
                            AssignmentState::Preparing,
                            AssignmentState::Prepared,
                            "game server acknowledged assignment preparation",
                        );
                        info!(
                            assignment_id = %assignment.assignment_id,
                            request_id = %assignment.request_id,
                            server_id = %assignment.server_id,
                            client_id = %assignment.client_id,
                            "assignment.prepared"
                        );
                        return Ok(());
                    }
                    log_assignment_state_transition(
                        assignment,
                        AssignmentState::Preparing,
                        AssignmentState::Rejected,
                        "game server rejected assignment preparation",
                    );
                    return Err(MatchmakerError::Provider(prepared.reason.unwrap_or_else(
                        || "assignment was rejected by game server".to_string(),
                    )));
                }
                tokio::time::sleep(poll_interval).await;
            }
        })
        .await;

        match result {
            Ok(result) => result,
            Err(_) => {
                log_assignment_state_transition(
                    assignment,
                    AssignmentState::Preparing,
                    AssignmentState::TimedOut,
                    "timed out waiting for game server preparation acknowledgement",
                );
                Err(MatchmakerError::Transport(format!(
                    "timed out waiting for game server to prepare assignment {}",
                    assignment.assignment_id
                )))
            }
        }
    }

    async fn assignment_has_active_connection(
        &self,
        assignment: &AssignmentRecord,
    ) -> Result<bool> {
        let Some(coordination) = &self.coordination else {
            return Ok(false);
        };
        Ok(coordination
            .active_connection(&assignment.server_id, assignment.client_id)
            .await
            .map_err(coordination_error)?
            .is_some_and(|connection| connection.connected))
    }

    async fn assignment_server_unavailable(&self, assignment: &AssignmentRecord) -> Result<bool> {
        if self.is_server_draining(&assignment.server_id).await? {
            return Ok(true);
        }
        let Some(coordination) = &self.coordination else {
            return Ok(false);
        };
        Ok(coordination
            .capacity_for_server(&assignment.server_id)
            .await
            .map_err(coordination_error)?
            .is_none_or(|capacity| !capacity.ready))
    }

    async fn monitor_assignment_connection(
        &self,
        outbound: OutboundMessages,
        player_id: PlayerId,
        mut request: AllocationRequest,
        mut assignment: AssignmentRecord,
        mut allocation: ServerAllocation,
    ) {
        if !self.allocation.require_assignment_prepare || self.coordination.is_none() {
            return;
        }

        let timeout = self.assignment_timeout();
        let mut retries_remaining = self.assignment_prepare_max_retries();
        loop {
            tokio::time::sleep(timeout).await;

            match self.assignment_has_active_connection(&assignment).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        assignment_id = %assignment.assignment_id,
                        request_id = %assignment.request_id,
                        server_id = %assignment.server_id,
                        error = %error,
                        "assignment.connection_monitor_failed"
                    );
                    return;
                }
            }

            let server_unavailable = match self.assignment_server_unavailable(&assignment).await {
                Ok(server_unavailable) => server_unavailable,
                Err(error) => {
                    warn!(
                        assignment_id = %assignment.assignment_id,
                        request_id = %assignment.request_id,
                        server_id = %assignment.server_id,
                        error = %error,
                        "assignment.connection_monitor_capacity_check_failed"
                    );
                    false
                }
            };
            let reason = if server_unavailable {
                "assignment server became unavailable before client connected"
            } else {
                "assignment timed out before client connected"
            }
            .to_string();
            self.queue_failed_assignment_cleanup(&assignment, &allocation, reason.clone())
                .await;

            if !server_unavailable || retries_remaining == 0 {
                RuntimeMetrics::inc(&self.metrics.inner.assignments_timed_out);
                log_assignment_state_transition(
                    &assignment,
                    AssignmentState::Ready,
                    AssignmentState::TimedOut,
                    "client did not connect before assignment timeout",
                );
                warn!(
                    assignment_id = %assignment.assignment_id,
                    request_id = %assignment.request_id,
                    server_id = %assignment.server_id,
                    client_id = %assignment.client_id,
                    retries_remaining,
                    reason = %reason,
                    "assignment.connection_timed_out"
                );
                return;
            }

            retries_remaining -= 1;
            avoid_failed_server(&mut request, &assignment.server_id);
            request.request_id = new_request_id();
            RuntimeMetrics::inc(&self.metrics.inner.assignment_connection_retries);
            warn!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                server_id = %assignment.server_id,
                client_id = %assignment.client_id,
                retries_remaining,
                "assignment.connection_retrying"
            );
            self.sleep_assignment_retry_backoff().await;

            let next = self
                .create_prepared_assignment_with_retries(
                    player_id.clone(),
                    request.clone(),
                    0,
                    |assignment| {
                        send_outbound(
                            &outbound,
                            ServerMessage::AssignmentPreparing {
                                assignment_id: assignment.assignment_id.to_string(),
                            },
                        )
                    },
                )
                .await;
            let (next_assignment, next_allocation) = match next {
                Ok(next) => next,
                Err(error) => {
                    let _ = send_outbound(&outbound, error_message(error));
                    return;
                }
            };
            let grant = match self
                .issue_connection_grant(
                    next_allocation.clone(),
                    player_id.clone(),
                    next_assignment.client_id,
                )
                .await
            {
                Ok(grant) => grant,
                Err(error) => {
                    self.queue_failed_assignment_cleanup(
                        &next_assignment,
                        &next_allocation,
                        error.to_string(),
                    )
                    .await;
                    let _ = send_outbound(&outbound, error_message(error));
                    return;
                }
            };
            if send_outbound(&outbound, ServerMessage::AssignmentReady { connect: grant }).is_err()
            {
                self.queue_failed_assignment_cleanup(
                    &next_assignment,
                    &next_allocation,
                    "client websocket closed before replacement assignment could be sent"
                        .to_string(),
                )
                .await;
                return;
            }
            RuntimeMetrics::inc(&self.metrics.inner.assignments_ready);
            log_assignment_state_transition(
                &next_assignment,
                AssignmentState::Prepared,
                AssignmentState::Ready,
                "replacement connection grant sent to client",
            );
            info!(
                assignment_id = %next_assignment.assignment_id,
                request_id = %next_assignment.request_id,
                client_id = %next_assignment.client_id,
                retries_remaining,
                "assignment.replacement_ready"
            );
            assignment = next_assignment;
            allocation = next_allocation;
        }
    }

    async fn monitor_lobby_assignment_connections(
        &self,
        lobby: Lobby,
        mut request: AllocationRequest,
        mut assignments: Vec<(PlayerId, AssignmentRecord)>,
        mut allocation: ServerAllocation,
    ) {
        if !self.allocation.require_assignment_prepare || self.coordination.is_none() {
            return;
        }

        let timeout = self.assignment_timeout();
        let mut retries_remaining = self.assignment_prepare_max_retries();
        loop {
            tokio::time::sleep(timeout).await;

            let mut all_connected = true;
            let mut timed_out_assignments = Vec::new();
            for (_, assignment) in &assignments {
                match self.assignment_has_active_connection(assignment).await {
                    Ok(true) => {}
                    Ok(false) => {
                        all_connected = false;
                        timed_out_assignments.push(assignment.clone());
                    }
                    Err(error) => {
                        warn!(
                            assignment_id = %assignment.assignment_id,
                            request_id = %assignment.request_id,
                            server_id = %assignment.server_id,
                            error = %error,
                            "lobby_assignment.connection_monitor_failed"
                        );
                        return;
                    }
                }
            }
            if all_connected {
                return;
            }

            let server_unavailable = match assignments
                .first()
                .map(|(_, assignment)| assignment)
                .map(|assignment| self.assignment_server_unavailable(assignment))
            {
                Some(check) => match check.await {
                    Ok(server_unavailable) => server_unavailable,
                    Err(error) => {
                        warn!(
                            request_id = %request.request_id,
                            server_id = %allocation.server_id,
                            error = %error,
                            "lobby_assignment.connection_monitor_capacity_check_failed"
                        );
                        false
                    }
                },
                None => return,
            };
            let reason = if server_unavailable {
                "lobby assignment server became unavailable before all clients connected"
            } else {
                "lobby assignment timed out before all clients connected"
            }
            .to_string();
            self.queue_failed_lobby_attempt_cleanup(&assignments, &allocation, reason.clone())
                .await;

            if !server_unavailable || retries_remaining == 0 {
                RuntimeMetrics::inc(&self.metrics.inner.assignments_timed_out);
                for assignment in &timed_out_assignments {
                    log_assignment_state_transition(
                        assignment,
                        AssignmentState::Ready,
                        AssignmentState::TimedOut,
                        "lobby member did not connect before assignment timeout",
                    );
                }
                warn!(
                    request_id = %request.request_id,
                    lobby_id = %lobby.id,
                    server_id = %allocation.server_id,
                    retries_remaining,
                    reason = %reason,
                    "lobby_assignment.connection_timed_out"
                );
                return;
            }

            retries_remaining -= 1;
            avoid_failed_server(&mut request, &allocation.server_id);
            request.request_id = new_request_id();
            RuntimeMetrics::inc(&self.metrics.inner.assignment_connection_retries);
            warn!(
                request_id = %request.request_id,
                lobby_id = %lobby.id,
                server_id = %allocation.server_id,
                retries_remaining,
                "lobby_assignment.connection_retrying"
            );
            self.sleep_assignment_retry_backoff().await;

            let next = self
                .create_prepared_lobby_assignments_with_retries(&lobby, request.clone(), 0)
                .await;
            let (next_assignments, next_allocation) = match next {
                Ok(next) => next,
                Err(error) => {
                    let message = error_message(error);
                    for member in &lobby.members {
                        self.lobbies
                            .send_to_player(&member.player_id, message.clone());
                    }
                    return;
                }
            };
            for (player_id, assignment) in &next_assignments {
                let grant = match self
                    .issue_connection_grant(
                        next_allocation.clone(),
                        player_id.clone(),
                        assignment.client_id,
                    )
                    .await
                {
                    Ok(grant) => grant,
                    Err(error) => {
                        self.queue_failed_lobby_attempt_cleanup(
                            &next_assignments,
                            &next_allocation,
                            error.to_string(),
                        )
                        .await;
                        self.lobbies.send_to_player(player_id, error_message(error));
                        return;
                    }
                };
                self.lobbies
                    .send_to_player(player_id, ServerMessage::AssignmentReady { connect: grant });
                RuntimeMetrics::inc(&self.metrics.inner.assignments_ready);
                log_assignment_state_transition(
                    assignment,
                    AssignmentState::Prepared,
                    AssignmentState::Ready,
                    "replacement connection grant sent to lobby member",
                );
                info!(
                    assignment_id = %assignment.assignment_id,
                    request_id = %assignment.request_id,
                    client_id = %assignment.client_id,
                    player_id = %player_id,
                    retries_remaining,
                    "lobby_assignment.replacement_ready"
                );
            }
            assignments = next_assignments;
            allocation = next_allocation;
        }
    }

    async fn queue_failed_assignment_cleanup(
        &self,
        assignment: &AssignmentRecord,
        allocation: &ServerAllocation,
        reason: String,
    ) {
        self.queue_failed_assignment_delete(assignment, reason.clone())
            .await;
        self.queue_failed_allocation_release(
            Some(assignment.request_id.clone()),
            allocation,
            Some(assignment.assignment_id.clone()),
            reason,
        )
        .await;
    }

    async fn queue_failed_lobby_attempt_cleanup(
        &self,
        assignments: &[(PlayerId, AssignmentRecord)],
        allocation: &ServerAllocation,
        reason: String,
    ) {
        for (_, assignment) in assignments {
            self.queue_failed_assignment_delete(assignment, reason.clone())
                .await;
        }

        // Lobby members share one provider allocation, so queue one release for
        // the whole failed lobby attempt instead of one release per member.
        let request_id = assignments
            .first()
            .map(|(_, assignment)| assignment.request_id.clone());
        self.queue_failed_allocation_release(request_id, allocation, None, reason)
            .await;
    }

    async fn queue_server_drain_cleanup(
        &self,
        server_id: &ServerId,
        assignments: &[AssignmentRecord],
        reason: Option<String>,
    ) {
        let Some(coordination) = &self.coordination else {
            return;
        };
        let reason = reason.unwrap_or_else(|| "game server drained".to_string());

        for assignment in assignments {
            if let Err(error) = coordination
                .enqueue_delete_assignment(DeleteAssignmentWork {
                    request_id: Some(assignment.request_id.clone()),
                    assignment_id: assignment.assignment_id.clone(),
                    client_id: assignment.client_id,
                    server_id: Some(server_id.clone()),
                    reason: Some(reason.clone()),
                })
                .await
            {
                warn!(
                    assignment_id = %assignment.assignment_id,
                    request_id = %assignment.request_id,
                    client_id = %assignment.client_id,
                    server_id = %server_id,
                    error = %error,
                    "game_server.drain.delete_queue_failed"
                );
            }
        }

        let mut released_allocations = BTreeSet::new();
        for assignment in assignments {
            if !released_allocations.insert(assignment.allocation_id.clone()) {
                continue;
            }
            if let Err(error) = coordination
                .enqueue_release_allocation(ReleaseAllocationWork {
                    request_id: Some(assignment.request_id.clone()),
                    allocation_id: assignment.allocation_id.clone(),
                    assignment_id: None,
                    server_id: Some(server_id.clone()),
                    provider: None,
                    reason: Some(reason.clone()),
                })
                .await
            {
                warn!(
                    allocation_id = %assignment.allocation_id,
                    request_id = %assignment.request_id,
                    server_id = %server_id,
                    error = %error,
                    "game_server.drain.release_queue_failed"
                );
            }
        }
    }

    async fn queue_failed_assignment_delete(&self, assignment: &AssignmentRecord, reason: String) {
        let Some(coordination) = &self.coordination else {
            return;
        };

        // The immediate delete makes the assignment disappear from client
        // lookups now; the queued work is for later background consumers that
        // need to tell providers or game servers about the failed assignment.
        if let Err(error) = coordination
            .delete_assignment_for_client(assignment.client_id)
            .await
        {
            warn!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                client_id = %assignment.client_id,
                error = %error,
                "assignment.cleanup.delete_failed"
            );
        }
        // Prepared assignments are consumed from the client/server assignment
        // indexes, so client-id deletion may not know which prepared ack should
        // be removed. Delete the ack explicitly when cleanup knows the
        // assignment id.
        if let Err(error) = coordination
            .delete_assignment_prepared(&assignment.assignment_id)
            .await
        {
            warn!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                error = %error,
                "assignment.cleanup.prepared_delete_failed"
            );
        }

        if let Err(error) = coordination
            .enqueue_delete_assignment(DeleteAssignmentWork {
                request_id: Some(assignment.request_id.clone()),
                assignment_id: assignment.assignment_id.clone(),
                client_id: assignment.client_id,
                server_id: Some(assignment.server_id.clone()),
                reason: Some(reason.clone()),
            })
            .await
        {
            warn!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                client_id = %assignment.client_id,
                error = %error,
                "assignment.cleanup.delete_queue_failed"
            );
        }
    }

    async fn queue_failed_allocation_release(
        &self,
        request_id: Option<RequestId>,
        allocation: &ServerAllocation,
        assignment_id: Option<AssignmentId>,
        reason: String,
    ) {
        let Some(coordination) = &self.coordination else {
            return;
        };

        let request_id_for_log = request_id.as_ref().map(ToString::to_string);
        let assignment_id_for_log = assignment_id.as_ref().map(ToString::to_string);

        if let Err(error) = coordination
            .enqueue_release_allocation(ReleaseAllocationWork {
                request_id,
                allocation_id: allocation.allocation_id.clone(),
                assignment_id,
                server_id: Some(allocation.server_id.clone()),
                provider: Some(allocation.provider.clone()),
                reason: Some(reason),
            })
            .await
        {
            warn!(
                assignment_id = ?assignment_id_for_log,
                request_id = ?request_id_for_log,
                allocation_id = %allocation.allocation_id,
                error = %error,
                "assignment.cleanup.release_queue_failed"
            );
        }
    }

    /// Runs the durable NATS lifecycle worker until the lifecycle stream ends.
    ///
    /// The worker consumes queued provider release and assignment deletion jobs.
    /// Failed jobs are left unacked so JetStream can redeliver them according to
    /// the consumer policy.
    pub async fn run_lifecycle_worker(&self, durable_name: impl AsRef<str>) -> Result<()> {
        let Some(coordination) = &self.coordination else {
            return Err(MatchmakerError::Config(
                "lifecycle worker requires [nats] configuration".to_string(),
            ));
        };
        let consumer = coordination
            .lifecycle_work_consumer_with_max_deliver(
                durable_name,
                self.lifecycle_job_max_deliver(),
            )
            .await
            .map_err(coordination_error)?;
        let mut messages = consumer.messages().await.map_err(coordination_error)?;
        while let Some(message) = messages.next().await {
            let message = message.map_err(coordination_error)?;
            RuntimeMetrics::inc(&self.metrics.inner.lifecycle_jobs_received);
            let delivery_attempt = message
                .info()
                .map(|info| info.delivered)
                .unwrap_or(1)
                .max(1);
            let work = match serde_json::from_slice::<LifecycleWork>(&message.payload) {
                Ok(work) => work,
                Err(error) => {
                    RuntimeMetrics::inc(&self.metrics.inner.lifecycle_invalid_payloads);
                    warn!(error = %error, "lifecycle_worker.invalid_payload");
                    message.ack().await.map_err(coordination_error)?;
                    continue;
                }
            };
            log_lifecycle_work_state_transition(
                &work,
                LifecycleWorkState::Queued,
                LifecycleWorkState::Processing,
                delivery_attempt,
                self.lifecycle_job_max_deliver(),
                "lifecycle worker received queued work",
            );
            match self.process_lifecycle_work(&work).await {
                Ok(()) => {
                    RuntimeMetrics::inc(&self.metrics.inner.lifecycle_jobs_succeeded);
                    log_lifecycle_work_state_transition(
                        &work,
                        LifecycleWorkState::Processing,
                        LifecycleWorkState::Succeeded,
                        delivery_attempt,
                        self.lifecycle_job_max_deliver(),
                        "lifecycle work completed and will be acknowledged",
                    );
                    message.ack().await.map_err(coordination_error)?;
                }
                Err(error) => {
                    RuntimeMetrics::inc(&self.metrics.inner.lifecycle_jobs_failed);
                    warn!(error = %error, work = ?work, "lifecycle_worker.job_failed");
                    if delivery_attempt >= self.lifecycle_job_max_deliver() {
                        RuntimeMetrics::inc(&self.metrics.inner.lifecycle_jobs_dead_lettered);
                        log_lifecycle_work_state_transition(
                            &work,
                            LifecycleWorkState::Processing,
                            LifecycleWorkState::DeadLettered,
                            delivery_attempt,
                            self.lifecycle_job_max_deliver(),
                            "lifecycle work exhausted delivery attempts",
                        );
                        error!(
                            error = %error,
                            work = ?work,
                            delivery_attempt,
                            max_deliver = self.lifecycle_job_max_deliver(),
                            "lifecycle_worker.job_dead_lettered"
                        );
                        message.ack().await.map_err(coordination_error)?;
                    } else {
                        log_lifecycle_work_state_transition(
                            &work,
                            LifecycleWorkState::Processing,
                            LifecycleWorkState::Retrying,
                            delivery_attempt,
                            self.lifecycle_job_max_deliver(),
                            "lifecycle work failed and will be redelivered",
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_lifecycle_work(&self, work: &LifecycleWork) -> Result<()> {
        match work {
            LifecycleWork::ReleaseAllocation(work) => {
                self.provider.release(work.allocation_id.clone()).await?;
                RuntimeMetrics::inc(&self.metrics.inner.lifecycle_release_succeeded);
                info!(
                    allocation_id = %work.allocation_id,
                    request_id = work.request_id.as_ref().map(ToString::to_string),
                    assignment_id = work.assignment_id.as_ref().map(ToString::to_string),
                    server_id = work.server_id.as_ref().map(ToString::to_string),
                    provider = ?work.provider,
                    "lifecycle.release_allocation_completed"
                );
                Ok(())
            }
            LifecycleWork::DeleteAssignment(work) => {
                let Some(coordination) = &self.coordination else {
                    return Err(MatchmakerError::Config(
                        "assignment deletion work requires [nats] configuration".to_string(),
                    ));
                };
                coordination
                    .delete_assignment(work.server_id.as_ref(), &work.assignment_id, work.client_id)
                    .await
                    .map_err(coordination_error)?;
                RuntimeMetrics::inc(&self.metrics.inner.lifecycle_delete_succeeded);
                info!(
                    assignment_id = %work.assignment_id,
                    client_id = %work.client_id,
                    request_id = work.request_id.as_ref().map(ToString::to_string),
                    server_id = work.server_id.as_ref().map(ToString::to_string),
                    "lifecycle.delete_assignment_completed"
                );
                Ok(())
            }
        }
    }

    async fn issue_connection_grant(
        &self,
        allocation: ServerAllocation,
        player_id: PlayerId,
        client_id: LightyearClientId,
    ) -> Result<lightyear_matchmaker_core::ConnectionGrant> {
        self.token_issuer
            .issue(TokenRequest {
                allocation,
                player_id,
                client_id: Some(client_id),
            })
            .await
    }
}

fn assignment_match_metadata(request: &AllocationRequest) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert("request_id".to_string(), request.request_id.to_string());
    metadata.insert("game".to_string(), request.game.clone());
    metadata.insert("version".to_string(), request.version.clone());
    metadata.insert("room".to_string(), room_selection_label(&request.room));
    if let Some(lobby_id) = &request.lobby_id {
        metadata.insert("lobby_id".to_string(), lobby_id.to_string());
    }
    metadata
}

fn lobby_match_metadata(lobby: &Lobby) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert("game".to_string(), lobby.game.clone());
    metadata.insert("version".to_string(), lobby.version.clone());
    metadata.insert("lobby_id".to_string(), lobby.id.to_string());
    metadata.insert("join_code".to_string(), lobby.join_code.clone());
    metadata
}

fn aggregate_lobby_latencies(lobby: &Lobby) -> Vec<LatencyReport> {
    let mut best_by_region = BTreeMap::<String, LatencyReport>::new();
    // Providers consume one latency report per region. For a lobby, use the
    // best player-observed RTT in each region so a single high-latency member
    // does not hide a viable placement option for the whole group.
    for latency in lobby
        .members
        .iter()
        .flat_map(|member| member.latencies.iter().cloned())
    {
        best_by_region
            .entry(latency.region.clone())
            .and_modify(|current| {
                if latency.rtt_ms < current.rtt_ms {
                    *current = latency.clone();
                }
            })
            .or_insert(latency);
    }
    best_by_region.into_values().collect()
}

fn room_selection_label(room: &RoomSelection) -> String {
    match room {
        RoomSelection::Auto => "auto".to_string(),
        RoomSelection::New => "new".to_string(),
        RoomSelection::Code(code) => format!("code:{code}"),
        RoomSelection::Id(id) => format!("id:{id}"),
    }
}

/// Builds the Axum router for the matchmaker HTTP/WebSocket API.
pub fn router(state: AppState) -> Router {
    spawn_lifecycle_worker(state.clone());
    let state = Arc::new(state);
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/openapi.yaml", get(openapi_yaml))
        .route("/asyncapi.yaml", get(asyncapi_yaml))
        .route(
            "/admin/drain",
            post(drain_matchmaker).delete(undrain_matchmaker),
        )
        .route(
            "/admin/servers/{server_id}/drain",
            post(drain_game_server).delete(undrain_game_server),
        )
        .route("/ws", get(ws_handler))
        .with_state(state)
}

/// Loads configuration from a path and runs the matchmaker server.
pub async fn run_from_config_path(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let config = MatchmakerConfig::from_path(path)?;
    let bind = config.server.bind;
    let state = AppState::from_config_with_coordination(config)
        .await
        .map_err(anyhow::Error::from)?;
    run(bind, state).await
}

/// Runs the matchmaker server on an already constructed application state.
pub async fn run(bind: SocketAddr, state: AppState) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind {bind}"))?;
    info!(
        "lightyear matchmaker listening on {}",
        listener.local_addr()?
    );
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("matchmaker server failed")
}

fn spawn_lifecycle_worker(state: AppState) {
    if state.coordination.is_none() {
        return;
    }
    tokio::spawn(async move {
        loop {
            match state.run_lifecycle_worker("matchmaker_lifecycle").await {
                Ok(()) => warn!("lifecycle worker stream ended"),
                Err(error) => error!(error = %error, "lifecycle worker stopped"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

async fn health() -> &'static str {
    "ok"
}

async fn ready(State(state): State<Arc<AppState>>) -> (StatusCode, Json<ReadinessStatus>) {
    let status = state.readiness_status().await;
    let code = if status.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(status))
}

async fn metrics(State(state): State<Arc<AppState>>) -> Json<MetricsSnapshot> {
    Json(state.metrics_snapshot())
}

async fn drain_matchmaker(
    State(state): State<Arc<AppState>>,
    Json(request): Json<DrainRequest>,
) -> Json<GlobalDrainStatus> {
    Json(state.start_draining(request.reason))
}

async fn undrain_matchmaker(State(state): State<Arc<AppState>>) -> Json<GlobalDrainStatus> {
    Json(state.stop_draining())
}

async fn drain_game_server(
    State(state): State<Arc<AppState>>,
    AxumPath(server_id): AxumPath<String>,
    Json(request): Json<DrainRequest>,
) -> (StatusCode, Json<ServerDrainResponse>) {
    let server_id = ServerId::new(server_id);
    match state
        .drain_game_server(server_id.clone(), request.reason)
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ServerDrainResponse {
                server_id,
                draining: false,
                reason: Some(error.to_string()),
                nats_configured: state.coordination.is_some(),
                canceled_assignments: 0,
                release_jobs_queued: 0,
            }),
        ),
    }
}

async fn undrain_game_server(
    State(state): State<Arc<AppState>>,
    AxumPath(server_id): AxumPath<String>,
) -> (StatusCode, Json<ServerDrainResponse>) {
    let server_id = ServerId::new(server_id);
    match state.undrain_game_server(server_id.clone()).await {
        Ok(response) => (StatusCode::OK, Json(response)),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ServerDrainResponse {
                server_id,
                draining: true,
                reason: Some(error.to_string()),
                nats_configured: state.coordination.is_some(),
                canceled_assignments: 0,
                release_jobs_queued: 0,
            }),
        ),
    }
}

async fn openapi_yaml() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/yaml")], OPENAPI_YAML)
}

async fn asyncapi_yaml() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/yaml")], ASYNCAPI_YAML)
}

async fn ws_handler(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    if state.is_draining() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(GlobalDrainStatus {
                draining: true,
                reason: state.drain_reason(),
            }),
        )
            .into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, addr, headers, state))
        .into_response()
}

async fn handle_socket(
    mut socket: WebSocket,
    addr: SocketAddr,
    headers: HeaderMap,
    state: Arc<AppState>,
) {
    let identity_request = identity_request_from_headers(addr, &headers);
    let (outbound_tx, mut outbound_rx) = tokio_mpsc::unbounded_channel();
    let mut session = ClientSession::default();
    RuntimeMetrics::inc(&state.metrics.inner.websocket_sessions_opened);
    RuntimeMetrics::inc(&state.metrics.inner.websocket_sessions_active);

    loop {
        tokio::select! {
            response = outbound_rx.recv() => {
                let Some(response) = response else {
                    break;
                };
                if !send_server_message(&mut socket, response).await {
                    break;
                }
            }
            message = socket.recv() => {
                let Some(message) = message else {
                    break;
                };
                let message = match message {
                    Ok(Message::Text(text)) => text,
                    Ok(Message::Close(_)) => break,
                    Ok(other) => {
                        debug!("ignoring unsupported websocket message: {other:?}");
                        continue;
                    }
                    Err(error) => {
                        warn!("websocket receive error: {error}");
                        break;
                    }
                };
                let client_message = match serde_json::from_str::<ClientMessage>(&message) {
                    Ok(client_message) => client_message,
                    Err(error) => {
                        let _ = outbound_tx.send(ServerMessage::error(
                            ErrorCode::InvalidJson,
                            error.to_string(),
                        ));
                        continue;
                    }
                };

                if let Err(error) = handle_client_message(
                    &outbound_tx,
                    state.clone(),
                    identity_request.clone(),
                    &mut session,
                    client_message,
                )
                .await
                {
                    let _ = outbound_tx.send(error_message(error));
                }
            }
        }
    }

    if let Some(identity) = session.identity {
        state.lobbies.unregister_session(&identity.player_id);
    }
    RuntimeMetrics::dec(&state.metrics.inner.websocket_sessions_active);
    RuntimeMetrics::inc(&state.metrics.inner.websocket_sessions_closed);
}

async fn handle_client_message(
    outbound: &OutboundMessages,
    state: Arc<AppState>,
    identity_request: IdentityRequest,
    session: &mut ClientSession,
    client_message: ClientMessage,
) -> Result<()> {
    if let ClientMessage::Hello {
        protocol_version, ..
    } = &client_message
    {
        if is_supported_websocket_protocol_version(*protocol_version) {
            send_outbound(outbound, ServerMessage::hello())?;
        } else {
            send_outbound(
                outbound,
                ServerMessage::error(
                    ErrorCode::UnsupportedProtocolVersion,
                    format!("unsupported websocket protocol version {protocol_version}"),
                ),
            )?;
        }
        return Ok(());
    }

    if state.is_draining() {
        return Err(MatchmakerError::Draining(
            state
                .drain_reason()
                .unwrap_or_else(|| "matchmaker is draining".to_string()),
        ));
    }

    let identity = ensure_identity(&state, identity_request, outbound, session).await?;

    match client_message {
        ClientMessage::RequestPlay {
            game,
            version,
            room,
            latencies,
        } => {
            RuntimeMetrics::inc(&state.metrics.inner.request_play_started);
            let result = stream_assignment(
                outbound,
                state.clone(),
                identity,
                game,
                version,
                room,
                latencies,
            )
            .await;
            if result.is_err() {
                RuntimeMetrics::inc(&state.metrics.inner.request_play_failed);
            }
            result
        }
        ClientMessage::LobbyCreate {
            game,
            version,
            max_players,
            latencies,
        } => {
            let game = if game.is_empty() {
                state.game.name.clone()
            } else {
                game
            };
            let version = if version.is_empty() {
                state.game.version.clone()
            } else {
                version
            };
            let lobby =
                state
                    .lobbies
                    .create_lobby(&identity, game, version, max_players, latencies);
            session.lobby_id = Some(lobby.id.clone());
            state.lobbies.notify_lobby(&lobby);
            Ok(())
        }
        ClientMessage::LobbyJoinCode { code, latencies } => {
            let lobby = state
                .lobbies
                .join_lobby_by_code(&code, &identity, latencies)?;
            session.lobby_id = Some(lobby.id.clone());
            state.lobbies.notify_lobby(&lobby);
            Ok(())
        }
        ClientMessage::LobbySetReady { ready } => {
            let Some(lobby_id) = session.lobby_id.clone() else {
                return Err(MatchmakerError::InvalidRequest(
                    "cannot set ready before joining a lobby".to_string(),
                ));
            };
            let (lobby, should_assign) =
                state
                    .lobbies
                    .set_ready(&lobby_id, &identity.player_id, ready)?;
            state.lobbies.notify_lobby(&lobby);
            if should_assign {
                state.assign_lobby(lobby).await?;
            }
            Ok(())
        }
        ClientMessage::Hello { .. } => unreachable!("hello is handled before identity resolution"),
    }
}

async fn ensure_identity(
    state: &AppState,
    identity_request: IdentityRequest,
    outbound: &OutboundMessages,
    session: &mut ClientSession,
) -> Result<ResolvedIdentity> {
    if let Some(identity) = &session.identity {
        return Ok(identity.clone());
    }

    let identity = state.identity.resolve(identity_request).await?;
    state
        .lobbies
        .register_session(identity.player_id.clone(), outbound.clone());
    send_outbound(
        outbound,
        ServerMessage::IdentityResolved {
            player: PlayerSummary {
                id: identity.player_id.clone(),
                display_name: identity.display_name.clone(),
            },
        },
    )?;
    session.identity = Some(identity.clone());
    Ok(identity)
}

async fn stream_assignment(
    outbound: &OutboundMessages,
    state: Arc<AppState>,
    identity: ResolvedIdentity,
    game: String,
    version: String,
    room: RoomSelection,
    latencies: Vec<lightyear_matchmaker_core::LatencyReport>,
) -> Result<()> {
    let allocation_request = state.allocation_request(
        new_request_id(),
        identity.player_id.clone(),
        game,
        version,
        room,
        latencies,
    );
    let monitor_request = allocation_request.clone();
    let (assignment, allocation) = state
        .create_prepared_assignment(
            identity.player_id.clone(),
            allocation_request,
            |assignment| {
                send_outbound(
                    outbound,
                    ServerMessage::AssignmentPreparing {
                        assignment_id: assignment.assignment_id.to_string(),
                    },
                )
            },
        )
        .await?;
    let grant = state
        .issue_connection_grant(
            allocation.clone(),
            identity.player_id.clone(),
            assignment.client_id,
        )
        .await?;
    send_outbound(outbound, ServerMessage::AssignmentReady { connect: grant })?;
    RuntimeMetrics::inc(&state.metrics.inner.assignments_ready);
    log_assignment_state_transition(
        &assignment,
        AssignmentState::Prepared,
        AssignmentState::Ready,
        "connection grant sent to websocket client",
    );
    if state.allocation.require_assignment_prepare && state.coordination.is_some() {
        let monitor_state = state.clone();
        let outbound = outbound.clone();
        let player_id = identity.player_id.clone();
        let assignment = assignment.clone();
        let allocation = allocation.clone();
        tokio::spawn(async move {
            monitor_state
                .monitor_assignment_connection(
                    outbound,
                    player_id,
                    monitor_request,
                    assignment,
                    allocation,
                )
                .await;
        });
    }
    info!(
        assignment_id = %assignment.assignment_id,
        request_id = %assignment.request_id,
        client_id = %assignment.client_id,
        "assignment.ready"
    );
    Ok(())
}

fn send_outbound(outbound: &OutboundMessages, response: ServerMessage) -> Result<()> {
    outbound.send(response).map_err(|_| socket_closed())
}

async fn send_server_message(socket: &mut WebSocket, response: ServerMessage) -> bool {
    match serde_json::to_string(&response) {
        Ok(payload) => socket.send(Message::Text(payload.into())).await.is_ok(),
        Err(error) => {
            error!("failed to serialize response: {error}");
            false
        }
    }
}

fn socket_closed() -> MatchmakerError {
    MatchmakerError::Transport("websocket closed while sending response".to_string())
}

fn identity_request_from_headers(addr: SocketAddr, headers: &HeaderMap) -> IdentityRequest {
    IdentityRequest {
        remote_addr: addr,
        forwarded_for: header_to_string(headers, "x-forwarded-for"),
        user_agent: header_to_string(headers, "user-agent"),
        metadata: BTreeMap::new(),
    }
}

fn header_to_string(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn error_message(error: MatchmakerError) -> ServerMessage {
    ServerMessage::Error {
        code: error.code(),
        message: error.to_string(),
        retryable: error.retryable(),
    }
}

fn coordination_error(error: impl std::fmt::Display) -> MatchmakerError {
    MatchmakerError::Transport(format!("nats coordination failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightyear_matchmaker_core::{ClientMessage, PlayerId, RoomSelection, ServerEndpoint};
    use lightyear_matchmaker_lightyear::NetcodeTokenConfig;
    use lightyear_matchmaker_provider_edgegap::EdgegapProviderConfig;
    use lightyear_matchmaker_provider_static::{StaticProviderConfig, StaticServerConfig};

    #[test]
    fn example_matchmaker_configs_parse() {
        for config in [
            include_str!("../../../examples/bevy_local_static/config/matchmaker.local.toml"),
            include_str!("../../../examples/bevy_local_static/config/matchmaker.nats.local.toml"),
            include_str!(
                "../../../examples/bevy_local_static/config/matchmaker.edgegap-mock.local.toml"
            ),
            include_str!(
                "../../../examples/bevy_local_static/config/matchmaker.edgegap.local.example.toml"
            ),
            include_str!("../../../examples/bevy_local_static/config/matchmaker.compose.toml"),
        ] {
            MatchmakerConfig::from_toml_str(config).unwrap();
        }
    }

    #[test]
    fn allocation_config_defaults_to_one_prepare_retry() {
        assert_eq!(
            AllocationConfig::default().assignment_prepare_max_retries,
            1
        );
        assert_eq!(AllocationConfig::default().assignment_retry_backoff_ms, 0);
        assert_eq!(AllocationConfig::default().assignment_timeout_secs, 60);
        assert_eq!(AllocationConfig::default().lifecycle_job_max_deliver, 10);

        let config: AllocationConfig = toml::from_str("require_assignment_prepare = true").unwrap();
        assert_eq!(config.assignment_prepare_max_retries, 1);
        assert_eq!(config.assignment_retry_backoff_ms, 0);
        assert_eq!(config.assignment_timeout_secs, 60);
        assert_eq!(config.lifecycle_job_max_deliver, 10);
    }

    #[tokio::test]
    async fn request_play_returns_assignment() {
        let state = AppState::from_config(MatchmakerConfig {
            server: ServerConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
            },
            game: GameConfig {
                name: "demo".to_string(),
                version: "dev".to_string(),
            },
            lightyear: NetcodeTokenConfig {
                protocol_id: 0,
                private_key: String::new(),
                client_timeout_secs: 15,
                token_expire_secs: 30,
            },
            identity: IdentityConfig::default(),
            allocation: AllocationConfig::default(),
            nats: None,
            static_provider: StaticProviderConfig {
                servers: vec![StaticServerConfig {
                    id: "local".to_string(),
                    game: "demo".to_string(),
                    version: "dev".to_string(),
                    endpoint: ServerEndpoint {
                        public_ip: "127.0.0.1".parse().unwrap(),
                        port: 7777,
                    },
                    ready: true,
                    total_players: 0,
                    max_players: 64,
                    max_rooms: 1,
                    region: Some("local".to_string()),
                    cert_digest: None,
                    metadata: BTreeMap::new(),
                }],
            },
            edgegap_provider: EdgegapProviderConfig::default(),
        })
        .unwrap();
        let responses = state
            .request_play(
                IdentityRequest {
                    remote_addr: "127.0.0.1:12345".parse().unwrap(),
                    forwarded_for: None,
                    user_agent: None,
                    metadata: BTreeMap::new(),
                },
                ClientMessage::RequestPlay {
                    game: "demo".to_string(),
                    version: "dev".to_string(),
                    room: RoomSelection::Auto,
                    latencies: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            responses.last(),
            Some(ServerMessage::AssignmentReady { .. })
        ));
        let metrics = state.metrics_snapshot();
        assert_eq!(metrics.request_play_started, 1);
        assert_eq!(metrics.assignments_created, 1);
        assert_eq!(metrics.assignments_prepared, 1);
        assert_eq!(metrics.assignments_ready, 1);
        assert_eq!(
            metrics.assignment_state_transitions.get("persisted"),
            Some(&1)
        );
        assert_eq!(
            metrics.assignment_state_transitions.get("prepared"),
            Some(&1)
        );
        assert_eq!(metrics.assignment_state_transitions.get("ready"), Some(&1));
        assert!(state.readiness_status().await.ready);
    }

    #[tokio::test]
    async fn repeated_request_play_uses_unique_assignment_ids() {
        let state = AppState::from_config(MatchmakerConfig {
            server: ServerConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
            },
            game: GameConfig {
                name: "demo".to_string(),
                version: "dev".to_string(),
            },
            lightyear: NetcodeTokenConfig {
                protocol_id: 0,
                private_key: String::new(),
                client_timeout_secs: 15,
                token_expire_secs: 30,
            },
            identity: IdentityConfig::default(),
            allocation: AllocationConfig::default(),
            nats: None,
            static_provider: StaticProviderConfig {
                servers: vec![StaticServerConfig {
                    id: "local".to_string(),
                    game: "demo".to_string(),
                    version: "dev".to_string(),
                    endpoint: ServerEndpoint {
                        public_ip: "127.0.0.1".parse().unwrap(),
                        port: 7777,
                    },
                    ready: true,
                    total_players: 0,
                    max_players: 64,
                    max_rooms: 1,
                    region: Some("local".to_string()),
                    cert_digest: None,
                    metadata: BTreeMap::new(),
                }],
            },
            edgegap_provider: EdgegapProviderConfig::default(),
        })
        .unwrap();
        let first = request_play_preparing_id(&state).await;
        let second = request_play_preparing_id(&state).await;

        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn prepare_failure_retries_once_by_default() {
        let state = AppState::from_config(MatchmakerConfig {
            server: ServerConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
            },
            game: GameConfig {
                name: "demo".to_string(),
                version: "dev".to_string(),
            },
            lightyear: NetcodeTokenConfig {
                protocol_id: 0,
                private_key: String::new(),
                client_timeout_secs: 15,
                token_expire_secs: 30,
            },
            identity: IdentityConfig::default(),
            allocation: AllocationConfig {
                require_assignment_prepare: true,
                assignment_prepare_timeout_ms: 1,
                assignment_prepare_poll_ms: 1,
                ..Default::default()
            },
            nats: None,
            static_provider: StaticProviderConfig {
                servers: vec![
                    StaticServerConfig {
                        id: "a".to_string(),
                        game: "demo".to_string(),
                        version: "dev".to_string(),
                        endpoint: ServerEndpoint {
                            public_ip: "127.0.0.1".parse().unwrap(),
                            port: 7777,
                        },
                        ready: true,
                        total_players: 0,
                        max_players: 64,
                        max_rooms: 1,
                        region: Some("local".to_string()),
                        cert_digest: None,
                        metadata: BTreeMap::new(),
                    },
                    StaticServerConfig {
                        id: "b".to_string(),
                        game: "demo".to_string(),
                        version: "dev".to_string(),
                        endpoint: ServerEndpoint {
                            public_ip: "127.0.0.1".parse().unwrap(),
                            port: 7778,
                        },
                        ready: true,
                        total_players: 0,
                        max_players: 64,
                        max_rooms: 1,
                        region: Some("local".to_string()),
                        cert_digest: None,
                        metadata: BTreeMap::new(),
                    },
                ],
            },
            edgegap_provider: EdgegapProviderConfig::default(),
        })
        .unwrap();
        let player_id = PlayerId::new("ip:127.0.0.1");
        let request = state.allocation_request(
            new_request_id(),
            player_id.clone(),
            "demo".to_string(),
            "dev".to_string(),
            RoomSelection::Auto,
            Vec::new(),
        );
        let mut preparing_server_ids = Vec::new();
        let result = state
            .create_prepared_assignment(player_id, request, |assignment| {
                preparing_server_ids.push(assignment.server_id.clone());
                Ok(())
            })
            .await;

        assert!(matches!(result, Err(MatchmakerError::Config(_))));
        assert_eq!(
            preparing_server_ids,
            vec![ServerId::new("a"), ServerId::new("b")]
        );
    }

    #[tokio::test]
    async fn matchmaker_drain_rejects_new_request_play() {
        let state = AppState::from_config(test_config(vec![static_server("local", 7777)]))
            .expect("test config should build");
        state.start_draining(Some("maintenance".to_string()));

        let result = state
            .request_play(
                IdentityRequest {
                    remote_addr: "127.0.0.1:12345".parse().unwrap(),
                    forwarded_for: None,
                    user_agent: None,
                    metadata: BTreeMap::new(),
                },
                ClientMessage::RequestPlay {
                    game: "demo".to_string(),
                    version: "dev".to_string(),
                    room: RoomSelection::Auto,
                    latencies: Vec::new(),
                },
            )
            .await;

        assert!(
            matches!(result, Err(MatchmakerError::Draining(reason)) if reason == "maintenance")
        );
        assert!(!state.readiness_status().await.ready);
    }

    #[tokio::test]
    async fn drained_static_server_is_avoided_for_new_assignments() {
        let state = AppState::from_config(test_config(vec![
            static_server("a", 7777),
            static_server("b", 7778),
        ]))
        .expect("test config should build");
        state
            .drain_game_server(ServerId::new("a"), Some("rotate".to_string()))
            .await
            .unwrap();

        let player_id = PlayerId::new("ip:127.0.0.1");
        let request = state.allocation_request(
            new_request_id(),
            player_id.clone(),
            "demo".to_string(),
            "dev".to_string(),
            RoomSelection::Auto,
            Vec::new(),
        );
        let (assignment, _) = state
            .create_prepared_assignment(player_id, request, |_| Ok(()))
            .await
            .unwrap();

        assert_eq!(assignment.server_id, ServerId::new("b"));
    }

    async fn request_play_preparing_id(state: &AppState) -> String {
        let responses = state
            .request_play(
                IdentityRequest {
                    remote_addr: "127.0.0.1:12345".parse().unwrap(),
                    forwarded_for: None,
                    user_agent: None,
                    metadata: BTreeMap::new(),
                },
                ClientMessage::RequestPlay {
                    game: "demo".to_string(),
                    version: "dev".to_string(),
                    room: RoomSelection::Auto,
                    latencies: Vec::new(),
                },
            )
            .await
            .unwrap();
        responses
            .into_iter()
            .find_map(|response| match response {
                ServerMessage::AssignmentPreparing { assignment_id } => Some(assignment_id),
                _ => None,
            })
            .expect("request_play should emit AssignmentPreparing")
    }

    fn test_config(servers: Vec<StaticServerConfig>) -> MatchmakerConfig {
        MatchmakerConfig {
            server: ServerConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
            },
            game: GameConfig {
                name: "demo".to_string(),
                version: "dev".to_string(),
            },
            lightyear: NetcodeTokenConfig {
                protocol_id: 0,
                private_key: String::new(),
                client_timeout_secs: 15,
                token_expire_secs: 30,
            },
            identity: IdentityConfig::default(),
            allocation: AllocationConfig::default(),
            nats: None,
            static_provider: StaticProviderConfig { servers },
            edgegap_provider: EdgegapProviderConfig::default(),
        }
    }

    fn static_server(id: &str, port: u16) -> StaticServerConfig {
        StaticServerConfig {
            id: id.to_string(),
            game: "demo".to_string(),
            version: "dev".to_string(),
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port,
            },
            ready: true,
            total_players: 0,
            max_players: 64,
            max_rooms: 1,
            region: Some("local".to_string()),
            cert_digest: None,
            metadata: BTreeMap::new(),
        }
    }
}
