//! Deployable Axum matchmaker server.
//!
//! The server wires identity resolution, lobby state, provider allocation, NATS
//! coordination, and Lightyear token issuing behind a WebSocket API.

use anyhow::Context as _;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use lightyear_matchmaker_core::{
    AllocationRequest, AssignmentId, AssignmentRecord, AssignmentRosterMember, ClientMessage,
    IdentityRequest, IdentityResolver, IpIdentityResolver, LatencyReport, LightyearClientId, Lobby,
    LobbyId, LobbyMember, MatchmakerError, PlayerId, PlayerSummary, RequestId, ResolvedIdentity,
    Result, RoomSelection, ServerAllocation, ServerMessage, ServerProvider, TokenIssuer,
    TokenRequest,
};
use lightyear_matchmaker_lightyear::{NetcodeTokenConfig, NetcodeTokenIssuer};
use lightyear_matchmaker_nats::{
    DeleteAssignmentWork, NatsConfig, NatsCoordinator, NatsStaticServerProvider,
    ReleaseAllocationWork,
};
use lightyear_matchmaker_provider_edgegap::{
    EdgegapProvider, EdgegapProviderConfig, MockEdgegapProvider,
};
use lightyear_matchmaker_provider_static::{StaticProviderConfig, StaticServerProvider};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc as tokio_mpsc;
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Top-level configuration for the deployable matchmaker server.
pub struct MatchmakerConfig {
    /// HTTP/WebSocket listener configuration.
    pub server: ServerConfig,
    /// Default game identity used when client messages omit game/version.
    pub game: GameConfig,
    /// Lightyear Netcode token issuing configuration.
    pub lightyear: NetcodeTokenConfig,
    #[serde(default)]
    /// Identity resolver configuration.
    pub identity: IdentityConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional NATS coordination backend configuration.
    pub nats: Option<NatsConfig>,
    #[serde(default)]
    /// Allocation and assignment lifecycle configuration.
    pub allocation: AllocationConfig,
    #[serde(default)]
    /// Configured static provider settings.
    pub static_provider: StaticProviderConfig,
    #[serde(default)]
    /// Edgegap provider settings used by both real and mock Edgegap sources.
    pub edgegap_provider: EdgegapProviderConfig,
}

impl MatchmakerConfig {
    /// Parses matchmaker configuration from TOML text.
    pub fn from_toml_str(value: &str) -> anyhow::Result<Self> {
        toml::from_str(value).context("failed to parse matchmaker config")
    }

    /// Loads and parses matchmaker configuration from a TOML file path.
    pub fn from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let value = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Self::from_toml_str(&value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// HTTP server configuration.
pub struct ServerConfig {
    /// Socket address the HTTP/WebSocket server binds to.
    pub bind: SocketAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Default game identity for allocation requests.
pub struct GameConfig {
    /// Game name accepted by providers and game servers.
    pub name: String,
    /// Game version accepted by providers and game servers.
    pub version: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
/// Identity resolver configuration.
pub struct IdentityConfig {
    #[serde(default)]
    /// Whether `x-forwarded-for` should be trusted for IP-derived identity.
    pub trust_forwarded_for: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Allocation and assignment lifecycle configuration.
pub struct AllocationConfig {
    #[serde(default)]
    /// Provider source used by the matchmaker.
    pub source: AllocationSource,
    #[serde(default)]
    /// Whether the client waits for game-server assignment preparation.
    pub require_assignment_prepare: bool,
    #[serde(default = "default_assignment_prepare_timeout_ms")]
    /// Maximum milliseconds to wait for game-server assignment preparation.
    pub assignment_prepare_timeout_ms: u64,
    #[serde(default = "default_assignment_prepare_poll_ms")]
    /// Poll interval in milliseconds while waiting for assignment preparation.
    pub assignment_prepare_poll_ms: u64,
}

impl Default for AllocationConfig {
    fn default() -> Self {
        Self {
            source: AllocationSource::ConfiguredStatic,
            require_assignment_prepare: false,
            assignment_prepare_timeout_ms: default_assignment_prepare_timeout_ms(),
            assignment_prepare_poll_ms: default_assignment_prepare_poll_ms(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Runtime provider source selected by configuration.
pub enum AllocationSource {
    #[default]
    /// Select from the configured static server list.
    ConfiguredStatic,
    /// Select from NATS-published static server capacity.
    NatsStatic,
    /// Create and poll Edgegap sessions through the real Edgegap API.
    Edgegap,
    /// Select from the mock Edgegap provider bridge.
    EdgegapMock,
}

fn default_assignment_prepare_timeout_ms() -> u64 {
    3_000
}

fn default_assignment_prepare_poll_ms() -> u64 {
    25
}

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
}

type OutboundMessages = tokio_mpsc::UnboundedSender<ServerMessage>;

#[derive(Clone, Default)]
struct LobbyRuntime {
    inner: Arc<Mutex<LobbyRuntimeState>>,
}

#[derive(Default)]
struct LobbyRuntimeState {
    next_lobby_id: u64,
    next_join_code: u64,
    lobbies: BTreeMap<LobbyId, Lobby>,
    code_to_lobby: BTreeMap<String, LobbyId>,
    sessions: BTreeMap<PlayerId, OutboundMessages>,
    assigning_lobbies: BTreeSet<LobbyId>,
}

impl LobbyRuntime {
    fn register_session(&self, player_id: PlayerId, sender: OutboundMessages) {
        self.with_state(|state| {
            state.sessions.insert(player_id, sender);
        });
    }

    fn unregister_session(&self, player_id: &PlayerId) {
        self.with_state(|state| {
            state.sessions.remove(player_id);
        });
    }

    fn create_lobby(
        &self,
        owner: &ResolvedIdentity,
        game: String,
        version: String,
        max_players: u32,
        latencies: Vec<lightyear_matchmaker_core::LatencyReport>,
    ) -> Lobby {
        self.with_state(|state| {
            state.next_lobby_id = state.next_lobby_id.saturating_add(1);
            state.next_join_code = state.next_join_code.saturating_add(1);
            let id = LobbyId::new(format!("lobby-{}", state.next_lobby_id));
            let join_code = format!("{:04X}", state.next_join_code);
            let lobby = Lobby {
                id: id.clone(),
                join_code: join_code.clone(),
                owner: owner.player_id.clone(),
                members: vec![LobbyMember {
                    player_id: owner.player_id.clone(),
                    display_name: owner.display_name.clone(),
                    team: Some("team-1".to_string()),
                    ready: false,
                    latencies,
                    metadata: BTreeMap::new(),
                }],
                game,
                version,
                max_players: max_players.max(1),
                ready: false,
                metadata: BTreeMap::new(),
            };
            state.code_to_lobby.insert(join_code, id.clone());
            state.lobbies.insert(id, lobby.clone());
            lobby
        })
    }

    fn join_lobby_by_code(
        &self,
        code: &str,
        player: &ResolvedIdentity,
        latencies: Vec<lightyear_matchmaker_core::LatencyReport>,
    ) -> Result<Lobby> {
        self.with_state(|state| {
            let code = normalize_join_code(code);
            let Some(lobby_id) = state.code_to_lobby.get(&code).cloned() else {
                return Err(MatchmakerError::InvalidRequest(format!(
                    "unknown lobby join code {code}"
                )));
            };
            let Some(lobby) = state.lobbies.get_mut(&lobby_id) else {
                return Err(MatchmakerError::InvalidRequest(format!(
                    "lobby {lobby_id} no longer exists"
                )));
            };
            if !lobby
                .members
                .iter()
                .any(|member| member.player_id == player.player_id)
            {
                if lobby.members.len() >= lobby.max_players as usize {
                    return Err(MatchmakerError::InvalidRequest(format!(
                        "lobby {lobby_id} is full"
                    )));
                }
                let team = format!("team-{}", (lobby.members.len() % 2) + 1);
                lobby.members.push(LobbyMember {
                    player_id: player.player_id.clone(),
                    display_name: player.display_name.clone(),
                    team: Some(team),
                    ready: false,
                    latencies,
                    metadata: BTreeMap::new(),
                });
            }
            Ok(lobby.clone())
        })
    }

    fn set_ready(
        &self,
        lobby_id: &LobbyId,
        player_id: &PlayerId,
        ready: bool,
    ) -> Result<(Lobby, bool)> {
        self.with_state(|state| {
            let Some(lobby) = state.lobbies.get_mut(lobby_id) else {
                return Err(MatchmakerError::InvalidRequest(format!(
                    "lobby {lobby_id} no longer exists"
                )));
            };
            let Some(member) = lobby
                .members
                .iter_mut()
                .find(|member| &member.player_id == player_id)
            else {
                return Err(MatchmakerError::InvalidRequest(format!(
                    "player {player_id} is not in lobby {lobby_id}"
                )));
            };
            member.ready = ready;
            lobby.ready = lobby.members.len() >= lobby.max_players as usize
                && lobby.members.iter().all(|member| member.ready);
            let should_assign = lobby.ready && state.assigning_lobbies.insert(lobby_id.clone());
            Ok((lobby.clone(), should_assign))
        })
    }

    fn send_to_player(&self, player_id: &PlayerId, message: ServerMessage) {
        let sender = self.with_state(|state| state.sessions.get(player_id).cloned());
        if let Some(sender) = sender {
            let _ = sender.send(message);
        }
    }

    fn notify_lobby(&self, lobby: &Lobby) {
        for member in &lobby.members {
            self.send_to_player(
                &member.player_id,
                ServerMessage::LobbyUpdated {
                    lobby: lobby.clone(),
                },
            );
        }
    }

    fn with_state<T>(&self, f: impl FnOnce(&mut LobbyRuntimeState) -> T) -> T {
        let mut state = match self.inner.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        f(&mut state)
    }
}

fn normalize_join_code(code: &str) -> String {
    code.trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase()
}

fn new_request_id() -> RequestId {
    RequestId::new(format!("req-{:032x}", rand::random::<u128>()))
}

fn assignment_id(request_id: &RequestId, client_id: LightyearClientId) -> AssignmentId {
    AssignmentId::new(format!("assignment:{}:{}", request_id, client_id))
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

        let allocation_request = self.allocation_request(
            new_request_id(),
            identity.player_id.clone(),
            game,
            version,
            room,
            latencies,
        );
        let (assignment, allocation) = self
            .create_assignment(identity.player_id.clone(), allocation_request)
            .await?;
        responses.push(ServerMessage::AssignmentPreparing {
            assignment_id: assignment.assignment_id.to_string(),
        });
        self.wait_for_assignment_prepared_or_cleanup(&assignment, &allocation)
            .await?;
        let grant = self
            .issue_connection_grant(allocation, identity.player_id.clone(), assignment.client_id)
            .await?;
        responses.push(ServerMessage::AssignmentReady { connect: grant });
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
        info!(
            assignment_id = %assignment.assignment_id,
            request_id = %assignment.request_id,
            server_id = %assignment.server_id,
            client_id = %assignment.client_id,
            player_id = %assignment.player_id,
            lobby_id = assignment.lobby_id.as_ref().map(ToString::to_string),
            "assignment.created"
        );
        Ok((assignment, allocation))
    }

    async fn assign_lobby(&self, lobby: Lobby) -> Result<()> {
        let request_id = new_request_id();
        let request = AllocationRequest {
            request_id: request_id.clone(),
            game: lobby.game.clone(),
            version: lobby.version.clone(),
            player_id: lobby.owner.clone(),
            lobby_id: Some(lobby.id.clone()),
            room: RoomSelection::New,
            latencies: aggregate_lobby_latencies(&lobby),
        };
        let allocation = self.provider.allocate(request.clone()).await?;
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
                assignment_id: assignment_id(&request_id, client_id),
                request_id: request_id.clone(),
                allocation_id: allocation.allocation_id.clone(),
                server_id: allocation.server_id.clone(),
                client_id,
                player_id: member.player_id.clone(),
                lobby_id: Some(lobby.id.clone()),
                team: member.team.clone(),
                roster: roster.clone(),
                match_metadata: lobby_match_metadata(&lobby),
                metadata: allocation.metadata.clone(),
            };
            self.put_assignment(&assignment).await?;
            info!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                server_id = %assignment.server_id,
                client_id = %assignment.client_id,
                player_id = %assignment.player_id,
                lobby_id = %lobby.id,
                "assignment.created"
            );
            self.lobbies.send_to_player(
                &member.player_id,
                ServerMessage::AssignmentPreparing {
                    assignment_id: assignment.assignment_id.to_string(),
                },
            );
            assignments.push((member.player_id.clone(), assignment));
        }

        for (player_id, assignment) in assignments {
            self.wait_for_assignment_prepared_or_cleanup(&assignment, &allocation)
                .await?;
            let grant = self
                .issue_connection_grant(allocation.clone(), player_id.clone(), assignment.client_id)
                .await?;
            self.lobbies.send_to_player(
                &player_id,
                ServerMessage::AssignmentReady { connect: grant },
            );
            info!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                client_id = %assignment.client_id,
                player_id = %player_id,
                "assignment.ready"
            );
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

    async fn wait_for_assignment_prepared(&self, assignment: &AssignmentRecord) -> Result<()> {
        if !self.allocation.require_assignment_prepare {
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
        tokio::time::timeout(deadline, async {
            loop {
                if let Some(prepared) = coordination
                    .assignment_prepared(&assignment.assignment_id)
                    .await
                    .map_err(coordination_error)?
                    && prepared.server_id == assignment.server_id
                    && prepared.client_id == assignment.client_id
                {
                    if prepared.prepared {
                        info!(
                            assignment_id = %assignment.assignment_id,
                            request_id = %assignment.request_id,
                            server_id = %assignment.server_id,
                            client_id = %assignment.client_id,
                            "assignment.prepared"
                        );
                        return Ok(());
                    }
                    return Err(MatchmakerError::Provider(prepared.reason.unwrap_or_else(
                        || "assignment was rejected by game server".to_string(),
                    )));
                }
                tokio::time::sleep(poll_interval).await;
            }
        })
        .await
        .map_err(|_| {
            MatchmakerError::Transport(format!(
                "timed out waiting for game server to prepare assignment {}",
                assignment.assignment_id
            ))
        })?
    }

    async fn wait_for_assignment_prepared_or_cleanup(
        &self,
        assignment: &AssignmentRecord,
        allocation: &ServerAllocation,
    ) -> Result<()> {
        match self.wait_for_assignment_prepared(assignment).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.queue_failed_assignment_cleanup(assignment, allocation, error.to_string())
                    .await;
                Err(error)
            }
        }
    }

    async fn queue_failed_assignment_cleanup(
        &self,
        assignment: &AssignmentRecord,
        allocation: &ServerAllocation,
        reason: String,
    ) {
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

        if let Err(error) = coordination
            .enqueue_release_allocation(ReleaseAllocationWork {
                request_id: Some(assignment.request_id.clone()),
                allocation_id: allocation.allocation_id.clone(),
                assignment_id: Some(assignment.assignment_id.clone()),
                server_id: Some(allocation.server_id.clone()),
                provider: Some(allocation.provider.clone()),
                reason: Some(reason),
            })
            .await
        {
            warn!(
                assignment_id = %assignment.assignment_id,
                request_id = %assignment.request_id,
                allocation_id = %allocation.allocation_id,
                error = %error,
                "assignment.cleanup.release_queue_failed"
            );
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
    let state = Arc::new(state);
    Router::new()
        .route("/health", get(health))
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

async fn health() -> &'static str {
    "ok"
}

async fn ws_handler(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, addr, headers, state))
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
                        let _ = outbound_tx.send(ServerMessage::Error {
                            code: "invalid_json".to_string(),
                            message: error.to_string(),
                        });
                        continue;
                    }
                };

                if let Err(error) = handle_client_message(
                    &outbound_tx,
                    &state,
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
}

async fn handle_client_message(
    outbound: &OutboundMessages,
    state: &AppState,
    identity_request: IdentityRequest,
    session: &mut ClientSession,
    client_message: ClientMessage,
) -> Result<()> {
    let identity = ensure_identity(state, identity_request, outbound, session).await?;

    match client_message {
        ClientMessage::Hello { .. } => Ok(()),
        ClientMessage::RequestPlay {
            game,
            version,
            room,
            latencies,
        } => stream_assignment(outbound, state, identity, game, version, room, latencies).await,
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
    state: &AppState,
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
    let (assignment, allocation) = state
        .create_assignment(identity.player_id.clone(), allocation_request)
        .await?;
    send_outbound(
        outbound,
        ServerMessage::AssignmentPreparing {
            assignment_id: assignment.assignment_id.to_string(),
        },
    )?;

    state
        .wait_for_assignment_prepared_or_cleanup(&assignment, &allocation)
        .await?;
    let grant = state
        .issue_connection_grant(allocation, identity.player_id, assignment.client_id)
        .await?;
    send_outbound(outbound, ServerMessage::AssignmentReady { connect: grant })?;
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
    let code = match &error {
        MatchmakerError::InvalidRequest(_) => "invalid_request",
        MatchmakerError::NoCapacity => "no_capacity",
        MatchmakerError::Provider(_) => "provider_error",
        MatchmakerError::Token(_) => "token_error",
        MatchmakerError::Config(_) => "config_error",
        MatchmakerError::Transport(_) => "transport_error",
    };
    ServerMessage::Error {
        code: code.to_string(),
        message: error.to_string(),
    }
}

fn coordination_error(error: impl std::fmt::Display) -> MatchmakerError {
    MatchmakerError::Transport(format!("nats coordination failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightyear_matchmaker_core::{ClientMessage, RoomSelection, ServerEndpoint};
    use lightyear_matchmaker_provider_static::StaticServerConfig;

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
}
