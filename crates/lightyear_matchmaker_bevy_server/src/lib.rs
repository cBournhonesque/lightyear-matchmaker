//! Bevy game-server integration for matchmaker coordination.
//!
//! This crate is used by the authoritative game process, not by the
//! client-facing matchmaker. The plugin publishes game-server readiness and
//! capacity, polls assignments addressed to the server, prepares local
//! admission state, exposes assignment context to game code, and reports active
//! Lightyear connections back through the coordination backend.
//!
//! The matchmaker decides which clients should be allowed to connect and issues
//! Lightyear connection grants. The game server still performs the final
//! validation step by checking the connecting Lightyear client id against its
//! prepared assignments.
//!
//! With the optional `agones` feature, the plugin can also call the local
//! Agones SDK HTTP API used by providers such as Gameflow: `Ready()` once the
//! server starts, periodic `Health()`, and explicit `Shutdown()`.

use bevy_app::{App, Plugin, Update};
#[cfg(feature = "lightyear-netcode")]
use bevy_ecs::prelude::{Added, Commands, Entity, Query, With};
use bevy_ecs::prelude::{Message, MessageReader, Res, ResMut, Resource};
#[cfg(feature = "lightyear-netcode")]
use lightyear_connection::{
    client::{Connected, Disconnected},
    client_of::ClientOf,
    server::Start,
    shared::{ConnectionRequestHandler, DeniedReason},
};
#[cfg(feature = "lightyear-netcode")]
use lightyear_core::id::{PeerId, RemoteId};
use lightyear_matchmaker_core::{
    ActiveConnection, AssignmentPrepared, AssignmentRecord, AssignmentRosterMember,
    GameServerReport, LightyearClientId, LobbyId, PlayerId, RegisteredGameServer, ServerCapacity,
    ServerId, ServerReadiness,
};
use lightyear_matchmaker_nats::{NatsConfig, NatsCoordinator};
#[cfg(feature = "lightyear-netcode")]
use lightyear_netcode::NetcodeServer;
#[cfg(feature = "lightyear-netcode")]
use std::collections::HashSet;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock, mpsc as std_mpsc};
use std::time::{Duration, Instant};
use tokio::sync::mpsc as tokio_mpsc;
use tracing::{info, warn};

#[derive(Clone, Debug)]
/// Bevy plugin for game-server matchmaker coordination.
pub struct LightyearMatchmakerServerPlugin {
    server: RegisteredGameServer,
    max_players: u32,
    max_rooms: u32,
    assignment_timeout: Duration,
    nats_bridge: Option<NatsBridgeConfig>,
    #[cfg(feature = "agones")]
    agones_sdk: Option<AgonesSdkConfig>,
    #[cfg(feature = "lightyear-netcode")]
    lightyear_netcode: Option<LightyearNetcodeIntegrationConfig>,
}

impl LightyearMatchmakerServerPlugin {
    /// Creates a plugin for a registered game server.
    pub fn new(server: RegisteredGameServer) -> Self {
        Self {
            server,
            max_players: 64,
            max_rooms: 1,
            assignment_timeout: Duration::from_secs(60),
            nats_bridge: None,
            #[cfg(feature = "agones")]
            agones_sdk: None,
            #[cfg(feature = "lightyear-netcode")]
            lightyear_netcode: None,
        }
    }

    /// Sets the maximum player and room counts used for capacity snapshots.
    pub fn with_capacity_limits(mut self, max_players: u32, max_rooms: u32) -> Self {
        self.max_players = max_players;
        self.max_rooms = max_rooms;
        self
    }

    /// Sets how long a prepared assignment may wait for a client connection.
    ///
    /// When this timeout elapses, the local admission gate revokes the client
    /// id and publishes an inactive connection report. This keeps old
    /// `ConnectToken`s from remaining valid in a long-running game server.
    pub fn with_assignment_timeout(mut self, timeout: Duration) -> Self {
        self.assignment_timeout = timeout;
        self
    }

    /// Enables the NATS bridge used to publish reports and poll assignments.
    pub fn with_nats_bridge(mut self, config: NatsBridgeConfig) -> Self {
        self.nats_bridge = Some(config);
        self
    }

    /// Enables calls to the local Agones SDK HTTP API.
    #[cfg(feature = "agones")]
    pub fn with_agones_sdk(self) -> Self {
        self.with_agones_sdk_config(AgonesSdkConfig::default())
    }

    /// Enables calls to the local Agones SDK HTTP API.
    #[cfg(feature = "agones")]
    pub fn with_agones_sdk_config(mut self, config: AgonesSdkConfig) -> Self {
        self.agones_sdk = Some(config);
        self
    }

    /// Enables integration with Lightyear's Netcode server connection hook.
    ///
    /// This installs a Lightyear `ConnectionRequestHandler` that accepts only
    /// assigned Netcode client ids and wires Lightyear connect/disconnect events
    /// into matchmaker active-connection reports.
    #[cfg(feature = "lightyear-netcode")]
    pub fn with_lightyear_netcode(self) -> Self {
        self.with_lightyear_netcode_config(LightyearNetcodeIntegrationConfig::default())
    }

    /// Enables integration with Lightyear's Netcode server connection hook.
    #[cfg(feature = "lightyear-netcode")]
    pub fn with_lightyear_netcode_config(
        mut self,
        config: LightyearNetcodeIntegrationConfig,
    ) -> Self {
        self.lightyear_netcode = Some(config);
        self
    }
}

impl Plugin for LightyearMatchmakerServerPlugin {
    fn build(&self, app: &mut App) {
        app.insert_resource(MatchmakerServerState::new(
            self.server.clone(),
            self.max_players,
            self.max_rooms,
        ))
        .insert_resource(AssignmentTimeout(self.assignment_timeout))
        .add_message::<PublishReadiness>()
        .add_message::<PublishCapacity>()
        .add_message::<AssignmentReceived>()
        .add_message::<ClientConnected>()
        .add_message::<ClientDisconnected>()
        .add_systems(
            Update,
            (
                handle_readiness_messages,
                handle_capacity_messages,
                handle_assignment_messages,
                handle_client_connected_messages,
                handle_client_disconnected_messages,
                expire_unconnected_assignment_messages,
            ),
        );

        if let Some(config) = &self.nats_bridge {
            app.insert_resource(spawn_nats_bridge(
                config.clone(),
                self.server.server_id.clone(),
            ))
            .add_systems(Update, sync_nats_bridge);
        }

        #[cfg(feature = "agones")]
        if let Some(config) = &self.agones_sdk {
            app.insert_resource(spawn_agones_sdk(config.clone()))
                .add_systems(Update, sync_agones_sdk);
        }

        #[cfg(feature = "lightyear-netcode")]
        if let Some(config) = self.lightyear_netcode {
            app.insert_resource(config)
                .init_resource::<LightyearNetcodeIntegrationState>()
                .add_systems(
                    Update,
                    (
                        install_lightyear_netcode_request_handler,
                        handle_lightyear_netcode_client_connected,
                        handle_lightyear_netcode_client_disconnected,
                    ),
                );
        }
    }
}

#[derive(Clone, Debug)]
/// Configuration for the optional Agones SDK HTTP integration.
#[cfg(feature = "agones")]
pub struct AgonesSdkConfig {
    /// Optional explicit Agones SDK HTTP base URL.
    ///
    /// When absent, the plugin reads `http_port_env` and builds
    /// `http://127.0.0.1:{port}`.
    pub base_url: Option<String>,
    /// Environment variable that contains the local Agones SDK HTTP port.
    pub http_port_env: String,
    /// Whether the background worker should call `/ready` immediately.
    pub ready_on_start: bool,
    /// Interval between `/health` calls. `Duration::ZERO` disables health.
    pub health_interval: Duration,
}

#[cfg(feature = "agones")]
impl Default for AgonesSdkConfig {
    fn default() -> Self {
        Self {
            base_url: None,
            http_port_env: "AGONES_SDK_HTTP_PORT".to_string(),
            ready_on_start: true,
            health_interval: Duration::from_secs(2),
        }
    }
}

#[derive(Clone, Copy, Debug, Resource)]
/// Configuration for the optional Lightyear Netcode server integration.
#[cfg(feature = "lightyear-netcode")]
pub struct LightyearNetcodeIntegrationConfig {
    /// Whether to trigger Lightyear's `Start` event after installing the handler.
    pub start_server: bool,
}

#[cfg(feature = "lightyear-netcode")]
impl Default for LightyearNetcodeIntegrationConfig {
    fn default() -> Self {
        Self { start_server: true }
    }
}

#[derive(Debug, Default, Resource)]
#[cfg(feature = "lightyear-netcode")]
struct LightyearNetcodeIntegrationState {
    installed_servers: HashSet<Entity>,
}

#[derive(Clone, Debug)]
/// Configuration for the Bevy server NATS bridge.
pub struct NatsBridgeConfig {
    /// NATS connection configuration.
    pub nats: NatsConfig,
    /// How often the bridge polls NATS for assignments targeting this server.
    pub assignment_poll_interval: Duration,
}

impl Default for NatsBridgeConfig {
    fn default() -> Self {
        Self {
            nats: NatsConfig::default(),
            assignment_poll_interval: Duration::from_millis(250),
        }
    }
}

#[derive(Clone, Copy, Debug, Resource)]
struct AssignmentTimeout(Duration);

#[derive(Resource)]
/// Bevy resource that owns the background NATS bridge channels.
pub struct NatsBridgeResource {
    reports: tokio_mpsc::UnboundedSender<GameServerReport>,
    assignments: Mutex<std_mpsc::Receiver<AssignmentRecord>>,
    errors: Mutex<std_mpsc::Receiver<String>>,
}

#[derive(Resource)]
/// Background Agones SDK HTTP integration.
#[cfg(feature = "agones")]
pub struct AgonesSdkResource {
    commands: tokio_mpsc::UnboundedSender<AgonesCommand>,
    errors: Mutex<std_mpsc::Receiver<String>>,
}

#[cfg(feature = "agones")]
impl AgonesSdkResource {
    /// Queues a `/ready` call.
    pub fn ready(&self) -> bool {
        self.commands.send(AgonesCommand::Ready).is_ok()
    }

    /// Queues a `/health` call.
    pub fn health(&self) -> bool {
        self.commands.send(AgonesCommand::Health).is_ok()
    }

    /// Queues a `/shutdown` call and asks the background worker to stop.
    pub fn shutdown(&self) -> bool {
        self.commands.send(AgonesCommand::Shutdown).is_ok()
    }

    /// Drains Agones SDK errors emitted by the background worker.
    pub fn drain_errors(&self) -> Vec<String> {
        drain_receiver(&self.errors)
    }
}

#[derive(Clone, Copy, Debug)]
#[cfg(feature = "agones")]
enum AgonesCommand {
    Ready,
    Health,
    Shutdown,
}

impl NatsBridgeResource {
    /// Queues a game-server report for publication through NATS.
    pub fn send_report(&self, report: GameServerReport) -> bool {
        self.reports.send(report).is_ok()
    }

    /// Drains assignments received by the background bridge.
    pub fn drain_assignments(&self) -> Vec<AssignmentRecord> {
        drain_receiver(&self.assignments)
    }

    /// Drains bridge errors emitted by the background bridge.
    pub fn drain_errors(&self) -> Vec<String> {
        drain_receiver(&self.errors)
    }
}

#[derive(Clone, Debug, Default, Resource)]
/// Shared allow-list used by synchronous connection request handlers.
///
/// Lightyear's connection hook is synchronous, so it cannot await NATS or query
/// Bevy systems directly. The matchmaker server state updates this gate whenever
/// assignments arrive, and the Lightyear handler reads it without blocking on
/// async work.
pub struct AssignmentConnectionGate {
    allowed_client_ids: Arc<RwLock<BTreeSet<LightyearClientId>>>,
}

impl AssignmentConnectionGate {
    /// Adds a client id to the allow-list.
    pub fn allow_client(&self, client_id: LightyearClientId) {
        if let Ok(mut allowed_client_ids) = self.allowed_client_ids.write() {
            allowed_client_ids.insert(client_id);
        }
    }

    /// Removes a client id from the allow-list.
    pub fn revoke_client(&self, client_id: LightyearClientId) {
        if let Ok(mut allowed_client_ids) = self.allowed_client_ids.write() {
            allowed_client_ids.remove(&client_id);
        }
    }

    /// Returns whether a client id is currently allowed.
    pub fn is_client_allowed(&self, client_id: LightyearClientId) -> bool {
        self.allowed_client_ids
            .read()
            .is_ok_and(|allowed_client_ids| allowed_client_ids.contains(&client_id))
    }

    /// Returns the currently allowed client ids.
    pub fn allowed_clients(&self) -> Vec<LightyearClientId> {
        self.allowed_client_ids
            .read()
            .map(|allowed_client_ids| allowed_client_ids.iter().copied().collect())
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
/// Lightyear Netcode connection request handler backed by assignment state.
#[cfg(feature = "lightyear-netcode")]
pub struct LightyearNetcodeConnectionRequestHandler {
    gate: AssignmentConnectionGate,
}

#[cfg(feature = "lightyear-netcode")]
impl LightyearNetcodeConnectionRequestHandler {
    /// Creates a handler from a shared assignment gate.
    pub fn new(gate: AssignmentConnectionGate) -> Self {
        Self { gate }
    }
}

#[cfg(feature = "lightyear-netcode")]
impl ConnectionRequestHandler for LightyearNetcodeConnectionRequestHandler {
    fn handle_request(&self, peer_id: PeerId) -> Option<DeniedReason> {
        let Some(client_id) = lightyear_netcode_client_id(peer_id) else {
            warn!("rejecting non-netcode Lightyear peer id: {peer_id:?}");
            return Some(DeniedReason::InvalidToken);
        };
        if self.gate.is_client_allowed(client_id) {
            None
        } else {
            warn!("rejecting Lightyear client id {client_id}: no matchmaker assignment");
            Some(DeniedReason::InvalidToken)
        }
    }
}

/// Converts a Lightyear peer id into a matchmaker Lightyear client id.
#[cfg(feature = "lightyear-netcode")]
pub fn lightyear_netcode_client_id(peer_id: PeerId) -> Option<LightyearClientId> {
    match peer_id {
        PeerId::Netcode(client_id) => Some(LightyearClientId::new(client_id)),
        _ => None,
    }
}

/// Spawns the background NATS bridge used by the Bevy plugin.
pub fn spawn_nats_bridge(config: NatsBridgeConfig, server_id: ServerId) -> NatsBridgeResource {
    let (reports_tx, reports_rx) = tokio_mpsc::unbounded_channel();
    let (assignments_tx, assignments_rx) = std_mpsc::channel();
    let (errors_tx, errors_rx) = std_mpsc::channel();

    std::thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = errors_tx.send(format!("failed to start NATS bridge runtime: {error}"));
                return;
            }
        };
        runtime.block_on(run_nats_bridge_loop(
            config,
            server_id,
            reports_rx,
            assignments_tx,
            errors_tx,
        ));
    });

    NatsBridgeResource {
        reports: reports_tx,
        assignments: Mutex::new(assignments_rx),
        errors: Mutex::new(errors_rx),
    }
}

/// Spawns the background worker used by the optional Agones SDK integration.
#[cfg(feature = "agones")]
pub fn spawn_agones_sdk(config: AgonesSdkConfig) -> AgonesSdkResource {
    let (commands_tx, commands_rx) = tokio_mpsc::unbounded_channel();
    let (errors_tx, errors_rx) = std_mpsc::channel();

    std::thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = errors_tx.send(format!("failed to start Agones SDK runtime: {error}"));
                return;
            }
        };
        runtime.block_on(run_agones_sdk_loop(config, commands_rx, errors_tx));
    });

    AgonesSdkResource {
        commands: commands_tx,
        errors: Mutex::new(errors_rx),
    }
}

#[cfg(feature = "agones")]
async fn run_agones_sdk_loop(
    config: AgonesSdkConfig,
    mut commands: tokio_mpsc::UnboundedReceiver<AgonesCommand>,
    errors: std_mpsc::Sender<String>,
) {
    let Some(base_url) = agones_base_url(&config) else {
        let _ = errors.send(format!(
            "failed to configure Agones SDK: set {} or AgonesSdkConfig::base_url",
            config.http_port_env
        ));
        return;
    };
    let client = reqwest::Client::new();
    if config.ready_on_start
        && let Err(error) = call_agones(&client, &base_url, "ready").await
    {
        let _ = errors.send(error);
    }

    let mut health = (!config.health_interval.is_zero()).then(|| {
        let mut interval = tokio::time::interval(config.health_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    });

    loop {
        if let Some(health) = health.as_mut() {
            tokio::select! {
                command = commands.recv() => {
                    if !handle_agones_command(&client, &base_url, command, &errors).await {
                        return;
                    }
                }
                _ = health.tick() => {
                    if let Err(error) = call_agones(&client, &base_url, "health").await {
                        let _ = errors.send(error);
                    }
                }
            }
        } else if !handle_agones_command(&client, &base_url, commands.recv().await, &errors).await {
            return;
        }
    }
}

#[cfg(feature = "agones")]
async fn handle_agones_command(
    client: &reqwest::Client,
    base_url: &str,
    command: Option<AgonesCommand>,
    errors: &std_mpsc::Sender<String>,
) -> bool {
    let Some(command) = command else {
        return false;
    };
    let endpoint = match command {
        AgonesCommand::Ready => "ready",
        AgonesCommand::Health => "health",
        AgonesCommand::Shutdown => "shutdown",
    };
    if let Err(error) = call_agones(client, base_url, endpoint).await {
        let _ = errors.send(error);
    }
    !matches!(command, AgonesCommand::Shutdown)
}

#[cfg(feature = "agones")]
async fn call_agones(
    client: &reqwest::Client,
    base_url: &str,
    endpoint: &str,
) -> Result<(), String> {
    let response = client
        .post(format!("{}/{endpoint}", base_url.trim_end_matches('/')))
        .send()
        .await
        .map_err(|error| format!("Agones SDK /{endpoint} transport error: {error}"))?;
    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        Err(format!(
            "Agones SDK /{endpoint} failed with status {status}: {body}"
        ))
    }
}

#[cfg(feature = "agones")]
fn agones_base_url(config: &AgonesSdkConfig) -> Option<String> {
    config.base_url.clone().or_else(|| {
        std::env::var(&config.http_port_env)
            .ok()
            .filter(|port| !port.trim().is_empty())
            .map(|port| format!("http://127.0.0.1:{}", port.trim()))
    })
}

async fn run_nats_bridge_loop(
    config: NatsBridgeConfig,
    server_id: ServerId,
    mut reports: tokio_mpsc::UnboundedReceiver<GameServerReport>,
    assignments: std_mpsc::Sender<AssignmentRecord>,
    errors: std_mpsc::Sender<String>,
) {
    let coordinator = match NatsCoordinator::connect(config.nats).await {
        Ok(coordinator) => coordinator,
        Err(error) => {
            let _ = errors.send(format!("failed to connect NATS bridge: {error}"));
            return;
        }
    };
    let mut seen_assignments = BTreeSet::new();
    let mut ticker = tokio::time::interval(config.assignment_poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            report = reports.recv() => {
                let Some(report) = report else {
                    return;
                };
                if let Err(error) = coordinator.publish_report(&report).await {
                    let _ = errors.send(format!("failed to publish matchmaker report: {error}"));
                }
            }
            _ = ticker.tick() => {
                match coordinator.assignments_for_server(&server_id).await {
                    Ok(records) => {
                        for assignment in records {
                            if seen_assignments.insert(assignment.client_id) {
                                let _ = assignments.send(assignment);
                            }
                        }
                    }
                    Err(error) => {
                        let _ = errors.send(format!("failed to poll matchmaker assignments: {error}"));
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Message)]
/// Bevy message requesting a readiness publication.
pub struct PublishReadiness {
    /// Whether the server is ready for assignment.
    pub ready: bool,
    /// Optional transport certificate digest.
    pub cert_digest: Option<String>,
}

#[derive(Clone, Debug, Message)]
/// Bevy message requesting a capacity publication.
pub struct PublishCapacity {
    /// Capacity snapshot to publish.
    pub capacity: ServerCapacity,
}

#[derive(Clone, Debug, Message)]
/// Bevy message indicating that an assignment was received.
pub struct AssignmentReceived {
    /// Assignment received from the matchmaker.
    pub assignment: AssignmentRecord,
}

#[derive(Clone, Copy, Debug, Message)]
/// Bevy message indicating that a client connected.
pub struct ClientConnected {
    /// Connected Lightyear client id.
    pub client_id: LightyearClientId,
}

#[derive(Clone, Copy, Debug, Message)]
/// Bevy message indicating that a client disconnected.
pub struct ClientDisconnected {
    /// Disconnected Lightyear client id.
    pub client_id: LightyearClientId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Request to validate a connecting client id.
pub struct ConnectionValidationRequest {
    /// Client id attempting to connect.
    pub client_id: LightyearClientId,
}

impl From<LightyearClientId> for ConnectionValidationRequest {
    fn from(client_id: LightyearClientId) -> Self {
        Self { client_id }
    }
}

#[derive(Clone, Debug)]
/// Result of validating a connection attempt.
pub enum ConnectionValidation {
    /// The client id is assigned and may connect.
    Accepted(Box<ValidatedConnection>),
    /// The client id is not currently allowed.
    Rejected(RejectedConnection),
}

impl ConnectionValidation {
    /// Returns whether validation accepted the connection.
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted(_))
    }

    /// Returns the accepted assignment, if validation succeeded.
    pub fn assignment(&self) -> Option<&AssignmentRecord> {
        match self {
            Self::Accepted(connection) => Some(&connection.assignment),
            Self::Rejected(_) => None,
        }
    }

    /// Returns the rejection reason, if validation failed.
    pub fn rejection_reason(&self) -> Option<ConnectionRejectionReason> {
        match self {
            Self::Accepted(_) => None,
            Self::Rejected(connection) => Some(connection.reason),
        }
    }
}

#[derive(Clone, Debug)]
/// Accepted connection and its assignment context.
pub struct ValidatedConnection {
    /// Accepted client id.
    pub client_id: LightyearClientId,
    /// Assignment that authorized the connection.
    pub assignment: AssignmentRecord,
    /// Game-facing assignment context.
    pub context: PlayerAssignmentContext,
}

#[derive(Clone, Debug)]
/// Rejected connection and reason.
pub struct RejectedConnection {
    /// Rejected client id.
    pub client_id: LightyearClientId,
    /// Reason the connection was rejected.
    pub reason: ConnectionRejectionReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Reason a connection validation failed.
pub enum ConnectionRejectionReason {
    /// No assignment exists for the client id.
    NoAssignment,
}

#[derive(Clone, Debug)]
/// Game-facing context resolved from an assignment.
pub struct PlayerAssignmentContext {
    /// Client id being validated.
    pub client_id: LightyearClientId,
    /// Player associated with the client id.
    pub player_id: PlayerId,
    /// Optional lobby associated with the assignment.
    pub lobby_id: Option<LobbyId>,
    /// Optional team assigned to the player.
    pub team: Option<String>,
    /// Roster entry for this client, when present.
    pub roster_member: Option<AssignmentRosterMember>,
    /// Full assigned roster.
    pub roster: Vec<AssignmentRosterMember>,
    /// Match metadata from the assignment.
    pub match_metadata: BTreeMap<String, String>,
    /// Provider or assignment metadata.
    pub assignment_metadata: BTreeMap<String, String>,
}

/// Validates whether a Lightyear client id is allowed to connect.
pub trait ConnectionValidator {
    /// Validates a connection attempt.
    fn validate_connection(
        &self,
        request: impl Into<ConnectionValidationRequest>,
    ) -> ConnectionValidation;
}

#[derive(Clone, Debug, Resource)]
/// Bevy resource tracking matchmaker-visible game-server state.
pub struct MatchmakerServerState {
    server: RegisteredGameServer,
    capacity: ServerCapacity,
    connection_gate: AssignmentConnectionGate,
    assignment_ids: BTreeSet<lightyear_matchmaker_core::AssignmentId>,
    assignments: BTreeMap<LightyearClientId, TrackedAssignment>,
    outbox: Vec<GameServerReport>,
}

#[derive(Clone, Debug)]
struct TrackedAssignment {
    record: AssignmentRecord,
    received_at: Instant,
    connected: bool,
}

impl TrackedAssignment {
    fn new(record: AssignmentRecord) -> Self {
        Self {
            record,
            received_at: Instant::now(),
            connected: false,
        }
    }
}

impl MatchmakerServerState {
    /// Creates game-server state for a registered server.
    pub fn new(server: RegisteredGameServer, max_players: u32, max_rooms: u32) -> Self {
        let capacity = server.capacity_snapshot(max_players, max_rooms);
        Self {
            server,
            capacity,
            connection_gate: AssignmentConnectionGate::default(),
            assignment_ids: BTreeSet::new(),
            assignments: BTreeMap::new(),
            outbox: Vec::new(),
        }
    }

    /// Returns the registered server metadata.
    pub fn server(&self) -> &RegisteredGameServer {
        &self.server
    }

    /// Returns the latest capacity snapshot.
    pub fn capacity(&self) -> &ServerCapacity {
        &self.capacity
    }

    /// Returns the shared gate used by synchronous connection hooks.
    pub fn connection_gate(&self) -> AssignmentConnectionGate {
        self.connection_gate.clone()
    }

    /// Returns a Lightyear Netcode connection handler backed by this state.
    #[cfg(feature = "lightyear-netcode")]
    pub fn lightyear_netcode_connection_request_handler(
        &self,
    ) -> LightyearNetcodeConnectionRequestHandler {
        LightyearNetcodeConnectionRequestHandler::new(self.connection_gate())
    }

    /// Updates readiness and queues a readiness report.
    pub fn set_ready(&mut self, ready: bool, cert_digest: Option<String>) {
        self.capacity.ready = ready;
        self.capacity.cert_digest.clone_from(&cert_digest);
        let mut readiness = ServerReadiness::from_registered(&self.server, ready);
        readiness.cert_digest = cert_digest;
        self.outbox.push(GameServerReport::Readiness(readiness));
    }

    /// Updates capacity and queues a capacity report.
    pub fn publish_capacity(&mut self, capacity: ServerCapacity) {
        self.capacity = capacity.clone();
        self.outbox.push(GameServerReport::Capacity(capacity));
    }

    /// Registers an assignment and queues a preparation acknowledgement.
    pub fn register_assignment(&mut self, assignment: AssignmentRecord) {
        if !self.assignment_ids.insert(assignment.assignment_id.clone()) {
            return;
        }
        let client_id = assignment.client_id;
        self.connection_gate.allow_client(client_id);
        self.assignments
            .insert(client_id, TrackedAssignment::new(assignment));
        if let Some(assignment) = self.assignments.get(&client_id) {
            self.outbox.push(GameServerReport::AssignmentPrepared(
                AssignmentPrepared::accepted(&assignment.record),
            ));
        }
    }

    /// Iterates over current assignments.
    pub fn assignments(&self) -> impl Iterator<Item = &AssignmentRecord> {
        self.assignments
            .values()
            .map(|assignment| &assignment.record)
    }

    /// Returns the number of current assignments.
    pub fn assignment_count(&self) -> usize {
        self.assignments.len()
    }

    /// Returns the assignment for a client id.
    pub fn assignment_for_client(&self, client_id: LightyearClientId) -> Option<&AssignmentRecord> {
        self.assignments
            .get(&client_id)
            .map(|assignment| &assignment.record)
    }

    /// Returns game-facing assignment context for a client id.
    pub fn assignment_context_for_client(
        &self,
        client_id: LightyearClientId,
    ) -> Option<PlayerAssignmentContext> {
        self.assignments
            .get(&client_id)
            .map(|assignment| context_for_assignment(&assignment.record, client_id))
    }

    /// Returns whether the client id has an assignment.
    pub fn is_client_allowed(&self, client_id: LightyearClientId) -> bool {
        self.assignments.contains_key(&client_id)
    }

    /// Marks an assigned client as connected and queues an active report.
    pub fn client_connected(&mut self, client_id: LightyearClientId) -> bool {
        let Some(assignment) = self.assignments.get_mut(&client_id) else {
            return false;
        };
        assignment.connected = true;
        let assignment = assignment.record.clone();
        self.outbox
            .push(GameServerReport::ActiveConnection(ActiveConnection {
                server_id: self.server.server_id.clone(),
                client_id,
                player_id: assignment.player_id.clone(),
                connected: true,
            }));
        info!(%client_id, player_id = %assignment.player_id, "matchmaker client connected");
        true
    }

    /// Marks an assigned client as disconnected, revokes the local admission
    /// gate, and queues an inactive report.
    pub fn client_disconnected(&mut self, client_id: LightyearClientId) -> bool {
        let Some(assignment) = self.assignments.remove(&client_id) else {
            return false;
        };
        let assignment = assignment.record;
        self.assignment_ids.remove(&assignment.assignment_id);
        self.connection_gate.revoke_client(client_id);
        self.outbox
            .push(GameServerReport::ActiveConnection(ActiveConnection {
                server_id: self.server.server_id.clone(),
                client_id,
                player_id: assignment.player_id.clone(),
                connected: false,
            }));
        info!(%client_id, player_id = %assignment.player_id, "matchmaker client disconnected");
        true
    }

    /// Expires assignments that were prepared but never became active.
    ///
    /// The matchmaker consumes NATS assignments once preparation is reported,
    /// so the game server must also forget local allow-list entries that never
    /// turn into real client connections. Connected clients are not expired by
    /// this method; they are removed only on disconnect.
    pub fn expire_unconnected_assignments(&mut self, timeout: Duration) -> Vec<AssignmentRecord> {
        let now = Instant::now();
        let expired_client_ids = self
            .assignments
            .iter()
            .filter_map(|(client_id, assignment)| {
                if !assignment.connected && now.duration_since(assignment.received_at) >= timeout {
                    Some(*client_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut expired = Vec::new();
        for client_id in expired_client_ids {
            let Some(assignment) = self.assignments.remove(&client_id) else {
                continue;
            };
            let assignment = assignment.record;
            self.assignment_ids.remove(&assignment.assignment_id);
            self.connection_gate.revoke_client(client_id);
            self.outbox
                .push(GameServerReport::ActiveConnection(ActiveConnection {
                    server_id: self.server.server_id.clone(),
                    client_id,
                    player_id: assignment.player_id.clone(),
                    connected: false,
                }));
            warn!(
                %client_id,
                player_id = %assignment.player_id,
                assignment_id = %assignment.assignment_id,
                "matchmaker assignment expired before client connected"
            );
            expired.push(assignment);
        }
        expired
    }

    /// Drains queued reports for bridge publication.
    pub fn drain_reports(&mut self) -> Vec<GameServerReport> {
        self.outbox.drain(..).collect()
    }
}

impl ConnectionValidator for MatchmakerServerState {
    fn validate_connection(
        &self,
        request: impl Into<ConnectionValidationRequest>,
    ) -> ConnectionValidation {
        let request = request.into();
        match self.assignments.get(&request.client_id) {
            Some(assignment) => ConnectionValidation::Accepted(Box::new(ValidatedConnection {
                client_id: request.client_id,
                context: context_for_assignment(&assignment.record, request.client_id),
                assignment: assignment.record.clone(),
            })),
            None => ConnectionValidation::Rejected(RejectedConnection {
                client_id: request.client_id,
                reason: ConnectionRejectionReason::NoAssignment,
            }),
        }
    }
}

fn context_for_assignment(
    assignment: &AssignmentRecord,
    client_id: LightyearClientId,
) -> PlayerAssignmentContext {
    let roster_member = assignment.roster_member_for_client(client_id).cloned();
    PlayerAssignmentContext {
        client_id,
        player_id: roster_member
            .as_ref()
            .map(|member| member.player_id.clone())
            .unwrap_or_else(|| assignment.player_id.clone()),
        lobby_id: assignment.lobby_id.clone(),
        team: roster_member
            .as_ref()
            .and_then(|member| member.team.clone())
            .or_else(|| assignment.team.clone()),
        roster_member,
        roster: assignment.roster.clone(),
        match_metadata: assignment.match_metadata.clone(),
        assignment_metadata: assignment.metadata.clone(),
    }
}

fn handle_readiness_messages(
    mut messages: MessageReader<PublishReadiness>,
    mut state: ResMut<MatchmakerServerState>,
) {
    for message in messages.read() {
        state.set_ready(message.ready, message.cert_digest.clone());
    }
}

fn handle_capacity_messages(
    mut messages: MessageReader<PublishCapacity>,
    mut state: ResMut<MatchmakerServerState>,
) {
    for message in messages.read() {
        state.publish_capacity(message.capacity.clone());
    }
}

fn handle_assignment_messages(
    mut messages: MessageReader<AssignmentReceived>,
    mut state: ResMut<MatchmakerServerState>,
) {
    for message in messages.read() {
        state.register_assignment(message.assignment.clone());
    }
}

fn handle_client_connected_messages(
    mut messages: MessageReader<ClientConnected>,
    mut state: ResMut<MatchmakerServerState>,
) {
    for message in messages.read() {
        state.client_connected(message.client_id);
    }
}

fn handle_client_disconnected_messages(
    mut messages: MessageReader<ClientDisconnected>,
    mut state: ResMut<MatchmakerServerState>,
) {
    for message in messages.read() {
        state.client_disconnected(message.client_id);
    }
}

fn expire_unconnected_assignment_messages(
    mut state: ResMut<MatchmakerServerState>,
    timeout: Res<AssignmentTimeout>,
) {
    state.expire_unconnected_assignments(timeout.0);
}

#[cfg(feature = "lightyear-netcode")]
fn install_lightyear_netcode_request_handler(
    mut commands: Commands,
    config: Res<LightyearNetcodeIntegrationConfig>,
    state: Res<MatchmakerServerState>,
    mut integration: ResMut<LightyearNetcodeIntegrationState>,
    mut servers: Query<(Entity, &mut NetcodeServer)>,
) {
    for (entity, mut server) in &mut servers {
        if !integration.installed_servers.insert(entity) {
            continue;
        }
        server.set_connection_request_handler(Arc::new(
            state.lightyear_netcode_connection_request_handler(),
        ));
        if config.start_server {
            commands.trigger(Start { entity });
        }
        info!(
            "installed matchmaker connection-request handler on Lightyear Netcode server {entity}"
        );
    }
}

#[cfg(feature = "lightyear-netcode")]
fn handle_lightyear_netcode_client_connected(
    clients: Query<&RemoteId, (With<ClientOf>, Added<Connected>)>,
    mut state: ResMut<MatchmakerServerState>,
) {
    for remote_id in &clients {
        let Some(client_id) = lightyear_netcode_client_id(remote_id.0) else {
            continue;
        };
        state.client_connected(client_id);
    }
}

#[cfg(feature = "lightyear-netcode")]
fn handle_lightyear_netcode_client_disconnected(
    clients: Query<&RemoteId, (With<ClientOf>, Added<Disconnected>)>,
    mut state: ResMut<MatchmakerServerState>,
) {
    for remote_id in &clients {
        let Some(client_id) = lightyear_netcode_client_id(remote_id.0) else {
            continue;
        };
        state.client_disconnected(client_id);
    }
}

fn sync_nats_bridge(mut state: ResMut<MatchmakerServerState>, bridge: Res<NatsBridgeResource>) {
    for assignment in bridge.drain_assignments() {
        state.register_assignment(assignment);
    }

    for report in state.drain_reports() {
        if !bridge.send_report(report) {
            warn!("failed to queue matchmaker report for NATS bridge");
        }
    }

    for error in bridge.drain_errors() {
        warn!("{error}");
    }
}

#[cfg(feature = "agones")]
fn sync_agones_sdk(agones: Res<AgonesSdkResource>) {
    for error in agones.drain_errors() {
        warn!("{error}");
    }
}

fn drain_receiver<T>(receiver: &Mutex<std_mpsc::Receiver<T>>) -> Vec<T> {
    let receiver = match receiver.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut items = Vec::new();
    while let Ok(item) = receiver.try_recv() {
        items.push(item);
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_app::App;
    use lightyear_matchmaker_core::{
        AllocationId, AssignmentId, AssignmentRosterMember, PlayerId, ProviderKind, RequestId,
        ServerEndpoint, ServerId,
    };
    use std::collections::BTreeMap;

    #[test]
    fn plugin_tracks_assignments_and_connection_reports() {
        let server = RegisteredGameServer {
            server_id: ServerId::new("local-dev"),
            provider: ProviderKind::Static,
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            game: "demo".to_string(),
            version: "dev".to_string(),
            region: Some("local".to_string()),
            metadata: BTreeMap::new(),
        };
        let mut app = App::new();
        app.add_plugins(LightyearMatchmakerServerPlugin::new(server));

        let client_id = LightyearClientId::new(42);
        app.world_mut().write_message(AssignmentReceived {
            assignment: AssignmentRecord {
                assignment_id: AssignmentId::new("assignment-42"),
                request_id: RequestId::new("request-42"),
                allocation_id: AllocationId::new("static:local-dev:ip:127.0.0.1"),
                server_id: ServerId::new("local-dev"),
                client_id,
                player_id: PlayerId::new("ip:127.0.0.1"),
                lobby_id: Some(LobbyId::new("lobby-1")),
                team: Some("blue".to_string()),
                roster: vec![AssignmentRosterMember {
                    player_id: PlayerId::new("ip:127.0.0.1"),
                    client_id: Some(client_id),
                    team: Some("blue".to_string()),
                    metadata: BTreeMap::new(),
                }],
                match_metadata: BTreeMap::from([("map".to_string(), "arena".to_string())]),
                metadata: BTreeMap::new(),
            },
        });
        app.world_mut().write_message(ClientConnected { client_id });
        app.update();

        let mut state = app.world_mut().resource_mut::<MatchmakerServerState>();
        assert!(state.is_client_allowed(client_id));
        let reports = state.drain_reports();
        assert!(reports.iter().any(|report| matches!(
            report,
            GameServerReport::AssignmentPrepared(prepared)
                if prepared.client_id == client_id && prepared.prepared
        )));
        assert!(reports.iter().any(|report| matches!(
            report,
            GameServerReport::ActiveConnection(connection)
                if connection.client_id == client_id && connection.connected
        )));
    }

    #[test]
    fn validation_accepts_assigned_clients_and_rejects_unknown_clients() {
        let server = RegisteredGameServer {
            server_id: ServerId::new("local-dev"),
            provider: ProviderKind::Static,
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            game: "demo".to_string(),
            version: "dev".to_string(),
            region: Some("local".to_string()),
            metadata: BTreeMap::new(),
        };
        let mut state = MatchmakerServerState::new(server, 64, 4);
        let client_id = LightyearClientId::new(42);
        state.register_assignment(AssignmentRecord {
            assignment_id: AssignmentId::new("assignment-42"),
            request_id: RequestId::new("request-42"),
            allocation_id: AllocationId::new("static:local-dev:ip:127.0.0.1"),
            server_id: ServerId::new("local-dev"),
            client_id,
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: Some(LobbyId::new("lobby-1")),
            team: Some("blue".to_string()),
            roster: vec![AssignmentRosterMember {
                player_id: PlayerId::new("ip:127.0.0.1"),
                client_id: Some(client_id),
                team: Some("blue".to_string()),
                metadata: BTreeMap::new(),
            }],
            match_metadata: BTreeMap::from([("mode".to_string(), "duel".to_string())]),
            metadata: BTreeMap::new(),
        });

        let accepted = state.validate_connection(client_id);
        assert!(accepted.is_accepted());
        assert_eq!(accepted.assignment().unwrap().client_id, client_id);
        let context = state.assignment_context_for_client(client_id).unwrap();
        assert_eq!(context.lobby_id, Some(LobbyId::new("lobby-1")));
        assert_eq!(context.team.as_deref(), Some("blue"));
        assert_eq!(context.roster.len(), 1);
        assert_eq!(
            context.match_metadata.get("mode").map(String::as_str),
            Some("duel")
        );

        let rejected = state.validate_connection(LightyearClientId::new(99));
        assert!(!rejected.is_accepted());
        assert_eq!(
            rejected.rejection_reason(),
            Some(ConnectionRejectionReason::NoAssignment)
        );

        assert!(state.client_disconnected(client_id));
        assert!(!state.is_client_allowed(client_id));
        assert!(!state.connection_gate().is_client_allowed(client_id));
        assert!(!state.validate_connection(client_id).is_accepted());
    }

    #[test]
    fn unconnected_assignments_expire_and_revoke_gate() {
        let server = RegisteredGameServer {
            server_id: ServerId::new("local-dev"),
            provider: ProviderKind::Static,
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            game: "demo".to_string(),
            version: "dev".to_string(),
            region: Some("local".to_string()),
            metadata: BTreeMap::new(),
        };
        let mut state = MatchmakerServerState::new(server, 64, 4);
        let client_id = LightyearClientId::new(42);
        state.register_assignment(AssignmentRecord {
            assignment_id: AssignmentId::new("assignment-42"),
            request_id: RequestId::new("request-42"),
            allocation_id: AllocationId::new("static:local-dev:ip:127.0.0.1"),
            server_id: ServerId::new("local-dev"),
            client_id,
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            team: Some("solo".to_string()),
            roster: Vec::new(),
            match_metadata: BTreeMap::new(),
            metadata: BTreeMap::new(),
        });
        let _ = state.drain_reports();

        let expired = state.expire_unconnected_assignments(Duration::ZERO);

        assert_eq!(expired.len(), 1);
        assert!(!state.is_client_allowed(client_id));
        assert!(!state.connection_gate().is_client_allowed(client_id));
        assert!(state.assignment_for_client(client_id).is_none());
        let reports = state.drain_reports();
        assert!(reports.iter().any(|report| matches!(
            report,
            GameServerReport::ActiveConnection(connection)
                if connection.client_id == client_id && !connection.connected
        )));
    }

    #[test]
    fn connected_assignments_do_not_expire() {
        let server = RegisteredGameServer {
            server_id: ServerId::new("local-dev"),
            provider: ProviderKind::Static,
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            game: "demo".to_string(),
            version: "dev".to_string(),
            region: Some("local".to_string()),
            metadata: BTreeMap::new(),
        };
        let mut state = MatchmakerServerState::new(server, 64, 4);
        let client_id = LightyearClientId::new(42);
        state.register_assignment(AssignmentRecord {
            assignment_id: AssignmentId::new("assignment-42"),
            request_id: RequestId::new("request-42"),
            allocation_id: AllocationId::new("static:local-dev:ip:127.0.0.1"),
            server_id: ServerId::new("local-dev"),
            client_id,
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            team: Some("solo".to_string()),
            roster: Vec::new(),
            match_metadata: BTreeMap::new(),
            metadata: BTreeMap::new(),
        });
        assert!(state.client_connected(client_id));
        let _ = state.drain_reports();

        let expired = state.expire_unconnected_assignments(Duration::ZERO);

        assert!(expired.is_empty());
        assert!(state.is_client_allowed(client_id));
        assert!(state.connection_gate().is_client_allowed(client_id));
    }

    #[cfg(feature = "lightyear-netcode")]
    #[test]
    fn lightyear_netcode_handler_accepts_only_assigned_netcode_clients() {
        use lightyear_connection::shared::{ConnectionRequestHandler, DeniedReason};
        use lightyear_core::id::PeerId;

        let server = RegisteredGameServer {
            server_id: ServerId::new("local-dev"),
            provider: ProviderKind::Static,
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            game: "demo".to_string(),
            version: "dev".to_string(),
            region: Some("local".to_string()),
            metadata: BTreeMap::new(),
        };
        let mut state = MatchmakerServerState::new(server, 64, 4);
        let client_id = LightyearClientId::new(42);
        state.register_assignment(AssignmentRecord {
            assignment_id: AssignmentId::new("assignment-42"),
            request_id: RequestId::new("request-42"),
            allocation_id: AllocationId::new("static:local-dev:ip:127.0.0.1"),
            server_id: ServerId::new("local-dev"),
            client_id,
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            team: Some("solo".to_string()),
            roster: Vec::new(),
            match_metadata: BTreeMap::new(),
            metadata: BTreeMap::new(),
        });

        let handler = state.lightyear_netcode_connection_request_handler();
        assert_eq!(handler.handle_request(PeerId::Netcode(42)), None);
        assert_eq!(
            handler.handle_request(PeerId::Netcode(99)),
            Some(DeniedReason::InvalidToken)
        );
        assert_eq!(
            handler.handle_request(PeerId::Server),
            Some(DeniedReason::InvalidToken)
        );
    }

    #[test]
    fn duplicate_assignment_id_is_idempotent() {
        let server = RegisteredGameServer {
            server_id: ServerId::new("local-dev"),
            provider: ProviderKind::Static,
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            game: "demo".to_string(),
            version: "dev".to_string(),
            region: Some("local".to_string()),
            metadata: BTreeMap::new(),
        };
        let mut state = MatchmakerServerState::new(server, 64, 4);
        let assignment = AssignmentRecord {
            assignment_id: AssignmentId::new("assignment-42"),
            request_id: RequestId::new("request-42"),
            allocation_id: AllocationId::new("static:local-dev:ip:127.0.0.1"),
            server_id: ServerId::new("local-dev"),
            client_id: LightyearClientId::new(42),
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            team: Some("solo".to_string()),
            roster: Vec::new(),
            match_metadata: BTreeMap::new(),
            metadata: BTreeMap::new(),
        };

        state.register_assignment(assignment.clone());
        state.register_assignment(assignment);
        let reports = state.drain_reports();
        assert_eq!(
            reports
                .iter()
                .filter(|report| matches!(report, GameServerReport::AssignmentPrepared(_)))
                .count(),
            1
        );
    }
}
