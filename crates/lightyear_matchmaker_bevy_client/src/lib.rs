//! Bevy-friendly client helpers for matchmaker websocket workflows.
//!
//! `lightyear_matchmaker_bevy_client` provides a Bevy-facing wrapper around the
//! matchmaker websocket protocol.
//!
//! There are two client surfaces:
//!
//! - [`request_play_once`]: one-shot helper that opens a websocket, sends
//!   `request_play`, waits for `assignment_ready`, and returns the connection
//!   grant.
//! - [`LightyearMatchmakerClientPlugin`]: persistent websocket session for app
//!   UI and lobby flows.
//!
//! # Plugin State
//!
//! The plugin inserts [`MatchmakerClientState`].
//!
//! Important fields:
//!
//! - `connected`: whether the websocket transport is connected.
//! - `protocol`: server protocol details after `hello`.
//! - `player`: latest resolved player identity.
//! - `lobby`: latest lobby snapshot.
//! - `assignment_id`: assignment currently being prepared.
//! - `grant`: latest connection grant.
//! - `grant_state`: `idle`, `preparing`, `ready`, or `failed`.
//! - `last_error`: last structured server or transport error.
//!
//! # Outbound Messages
//!
//! Game/UI code sends Bevy messages:
//!
//! - [`RequestPlay`]
//! - [`CreateLobby`]
//! - [`JoinLobbyCode`]
//! - [`SetLobbyReady`]
//! - [`SendMatchmakerMessage`] for raw protocol messages
//!
//! The plugin forwards those messages over the persistent websocket session.
//!
//! # Inbound Messages
//!
//! The plugin emits Bevy messages:
//!
//! - [`MatchmakerClientConnected`]
//! - [`MatchmakerProtocolReady`]
//! - [`MatchmakerClientDisconnected`]
//! - [`MatchmakerIdentityResolved`]
//! - [`MatchmakerLobbyUpdated`]
//! - [`MatchmakerAssignmentPreparing`]
//! - [`MatchmakerQueueProgress`]
//! - [`ConnectionGrantReady`]
//! - [`MatchmakerClientFailed`]
//!
//! [`MatchmakerClientFailed`] keeps a plain `message` string for simple UI
//! handling and also carries structured [`MatchmakerClientErrorInfo`] with
//! `code` and `retryable`.
//!
//! # Reconnect Behavior
//!
//! Native builds start one background websocket session task. It sends `hello`,
//! forwards outbound Bevy messages, emits inbound Bevy messages, and reconnects
//! after transport failures when [`MatchmakerClientConfig::reconnect`] is true.
//!
//! Commands queued while the session is disconnected remain in the local command
//! channel until the session task can send them. Commands already sent to a
//! connection that later fails may need to be retried by game/UI code based on
//! [`MatchmakerClientFailed`] and the server's `retryable` flag.
//!
//! Persistent websocket sessions are not implemented for wasm yet. The one-shot
//! [`request_play_once`] helper remains available for wasm.
//!
//! # Minimal Setup
//!
//! ```rust,ignore
//! use lightyear_matchmaker_bevy_client::{
//!     LightyearMatchmakerClientPlugin, MatchmakerClientConfig,
//! };
//!
//! app.add_plugins(LightyearMatchmakerClientPlugin::new(
//!     MatchmakerClientConfig::new("ws://127.0.0.1:3000/ws"),
//! ));
//! ```
//!
//! To request play:
//!
//! ```rust,ignore
//! use bevy_ecs::prelude::MessageWriter;
//! use lightyear_matchmaker_bevy_client::RequestPlay;
//!
//! fn request_play(mut requests: MessageWriter<RequestPlay>) {
//!     requests.write(RequestPlay::new("demo", "dev"));
//! }
//! ```
//!
//! To create a lobby and ready up:
//!
//! ```rust,ignore
//! use bevy_ecs::prelude::MessageWriter;
//! use lightyear_matchmaker_bevy_client::{CreateLobby, SetLobbyReady};
//!
//! fn create_lobby(mut create: MessageWriter<CreateLobby>) {
//!     create.write(CreateLobby::new("demo", "dev", 2));
//! }
//!
//! fn ready(mut ready: MessageWriter<SetLobbyReady>) {
//!     ready.write(SetLobbyReady { ready: true });
//! }
//! ```

use bevy_app::{App, Plugin, Update};
use bevy_ecs::prelude::{Message, MessageReader, MessageWriter, Res, ResMut, Resource};
use futures_util::{SinkExt as _, StreamExt as _};
use lightyear_matchmaker_core::{
    ClientMessage, ConnectionGrant, ErrorCode, LatencyReport, Lobby, PlayerSummary, RoomSelection,
    ServerMessage, WEBSOCKET_PROTOCOL_VERSION,
};
use std::sync::{Mutex, mpsc as std_mpsc};
#[cfg(not(target_arch = "wasm32"))]
use std::thread;
use std::time::Duration;
use thiserror::Error;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
#[cfg(target_arch = "wasm32")]
use tokio_tungstenite_wasm::Message as WebSocketMessage;

#[derive(Clone, Debug)]
/// Bevy plugin that exposes a persistent matchmaker websocket workflow.
pub struct LightyearMatchmakerClientPlugin {
    config: MatchmakerClientConfig,
}

impl LightyearMatchmakerClientPlugin {
    /// Creates the client plugin for a matchmaker WebSocket endpoint.
    pub fn new(config: MatchmakerClientConfig) -> Self {
        Self { config }
    }
}

impl Plugin for LightyearMatchmakerClientPlugin {
    fn build(&self, app: &mut App) {
        app.insert_resource(self.config.clone())
            .insert_resource(MatchmakerClientRuntime::new())
            .insert_resource(MatchmakerClientState::default())
            .add_message::<RequestPlay>()
            .add_message::<CreateLobby>()
            .add_message::<JoinLobbyCode>()
            .add_message::<SetLobbyReady>()
            .add_message::<SendMatchmakerMessage>()
            .add_message::<MatchmakerClientConnected>()
            .add_message::<MatchmakerProtocolReady>()
            .add_message::<MatchmakerClientDisconnected>()
            .add_message::<MatchmakerIdentityResolved>()
            .add_message::<MatchmakerLobbyUpdated>()
            .add_message::<MatchmakerAssignmentPreparing>()
            .add_message::<MatchmakerQueueProgress>()
            .add_message::<ConnectionGrantReady>()
            .add_message::<MatchmakerClientFailed>()
            .add_systems(
                Update,
                (
                    ensure_matchmaker_session,
                    forward_client_commands,
                    drain_client_runtime_events,
                ),
            );
    }
}

#[derive(Clone, Debug, Resource)]
/// Bevy client plugin configuration.
pub struct MatchmakerClientConfig {
    /// Matchmaker WebSocket URL.
    pub websocket_url: String,
    /// Websocket protocol version sent in the initial hello.
    pub protocol_version: u16,
    /// Optional client implementation label sent in the initial hello.
    pub client_label: Option<String>,
    /// Whether the session task should reconnect after transport failures.
    pub reconnect: bool,
    /// Delay between reconnect attempts.
    pub reconnect_delay: Duration,
}

impl MatchmakerClientConfig {
    /// Creates a config with default websocket protocol and reconnect settings.
    pub fn new(websocket_url: impl Into<String>) -> Self {
        Self {
            websocket_url: websocket_url.into(),
            protocol_version: WEBSOCKET_PROTOCOL_VERSION,
            client_label: Some("bevy-client".to_string()),
            reconnect: true,
            reconnect_delay: Duration::from_secs(1),
        }
    }
}

#[derive(Resource)]
struct MatchmakerClientRuntime {
    command_tx: std_mpsc::Sender<ClientMessage>,
    command_rx: Mutex<Option<std_mpsc::Receiver<ClientMessage>>>,
    events_tx: std_mpsc::Sender<ClientRuntimeEvent>,
    events_rx: Mutex<std_mpsc::Receiver<ClientRuntimeEvent>>,
    session_started: Mutex<bool>,
}

impl MatchmakerClientRuntime {
    fn new() -> Self {
        let (command_tx, command_rx) = std_mpsc::channel();
        let (events_tx, events_rx) = std_mpsc::channel();
        Self {
            command_tx,
            command_rx: Mutex::new(Some(command_rx)),
            events_tx,
            events_rx: Mutex::new(events_rx),
            session_started: Mutex::new(false),
        }
    }

    fn take_command_rx(&self) -> Option<std_mpsc::Receiver<ClientMessage>> {
        match self.command_rx.lock() {
            Ok(mut receiver) => receiver.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        }
    }

    fn mark_session_started(&self) -> bool {
        let mut started = match self.session_started.lock() {
            Ok(started) => started,
            Err(poisoned) => poisoned.into_inner(),
        };
        if *started {
            false
        } else {
            *started = true;
            true
        }
    }
}

#[derive(Clone, Debug, Default, Resource)]
/// Current client-side view of the matchmaker websocket session.
pub struct MatchmakerClientState {
    /// Whether the websocket transport is currently connected.
    pub connected: bool,
    /// Negotiated websocket protocol, when the server hello has been received.
    pub protocol: Option<MatchmakerProtocol>,
    /// Last resolved player identity.
    pub player: Option<PlayerSummary>,
    /// Last lobby snapshot received from the server.
    pub lobby: Option<Lobby>,
    /// Current assignment id being prepared.
    pub assignment_id: Option<String>,
    /// Latest connection grant.
    pub grant: Option<ConnectionGrant>,
    /// Client-side connection grant state.
    pub grant_state: ClientConnectionGrantState,
    /// Last error reported by the transport or server.
    pub last_error: Option<MatchmakerClientErrorInfo>,
}

#[derive(Clone, Debug)]
/// Negotiated websocket protocol details.
pub struct MatchmakerProtocol {
    /// Protocol version selected by the server.
    pub protocol_version: u16,
    /// Minimum protocol version supported by the server.
    pub min_protocol_version: u16,
    /// Maximum protocol version supported by the server.
    pub max_protocol_version: u16,
    /// Optional server implementation label.
    pub server: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Client-side connection grant state.
pub enum ClientConnectionGrantState {
    #[default]
    /// No assignment has been requested or observed.
    Idle,
    /// The matchmaker is preparing an assignment with a game server.
    Preparing,
    /// A connection grant has been received.
    Ready,
    /// The current request failed.
    Failed,
}

#[derive(Clone, Debug)]
/// Structured client-side error information.
pub struct MatchmakerClientErrorInfo {
    /// Stable server error code, when the error came from the server.
    pub code: Option<ErrorCode>,
    /// Human-readable error message.
    pub message: String,
    /// Whether the operation may be retried with backoff.
    pub retryable: bool,
}

#[derive(Clone, Debug, Message)]
/// Bevy message requesting a playable assignment.
pub struct RequestPlay {
    /// Requested game name.
    pub game: String,
    /// Requested game version.
    pub version: String,
    /// Requested room selection.
    pub room: RoomSelection,
    /// Optional region latency hints.
    pub latencies: Vec<LatencyReport>,
}

impl RequestPlay {
    /// Creates a request-play message with default room selection.
    pub fn new(game: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            game: game.into(),
            version: version.into(),
            room: RoomSelection::Auto,
            latencies: Vec::new(),
        }
    }
}

impl From<RequestPlay> for ClientMessage {
    fn from(request: RequestPlay) -> Self {
        Self::RequestPlay {
            game: request.game,
            version: request.version,
            room: request.room,
            latencies: request.latencies,
        }
    }
}

#[derive(Clone, Debug, Message)]
/// Bevy message requesting lobby creation.
pub struct CreateLobby {
    /// Lobby game name.
    pub game: String,
    /// Lobby game version.
    pub version: String,
    /// Required lobby size before assignment.
    pub max_players: u32,
    /// Optional region latency hints.
    pub latencies: Vec<LatencyReport>,
}

impl CreateLobby {
    /// Creates a lobby-create message with no latency hints.
    pub fn new(game: impl Into<String>, version: impl Into<String>, max_players: u32) -> Self {
        Self {
            game: game.into(),
            version: version.into(),
            max_players,
            latencies: Vec::new(),
        }
    }
}

impl From<CreateLobby> for ClientMessage {
    fn from(request: CreateLobby) -> Self {
        Self::LobbyCreate {
            game: request.game,
            version: request.version,
            max_players: request.max_players,
            latencies: request.latencies,
        }
    }
}

#[derive(Clone, Debug, Message)]
/// Bevy message requesting a lobby join by short code.
pub struct JoinLobbyCode {
    /// Join code displayed by the lobby owner.
    pub code: String,
    /// Optional region latency hints.
    pub latencies: Vec<LatencyReport>,
}

impl JoinLobbyCode {
    /// Creates a lobby join-code message with no latency hints.
    pub fn new(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            latencies: Vec::new(),
        }
    }
}

impl From<JoinLobbyCode> for ClientMessage {
    fn from(request: JoinLobbyCode) -> Self {
        Self::LobbyJoinCode {
            code: request.code,
            latencies: request.latencies,
        }
    }
}

#[derive(Clone, Debug, Message)]
/// Bevy message updating the current player's lobby ready state.
pub struct SetLobbyReady {
    /// New ready state.
    pub ready: bool,
}

impl From<SetLobbyReady> for ClientMessage {
    fn from(request: SetLobbyReady) -> Self {
        Self::LobbySetReady {
            ready: request.ready,
        }
    }
}

#[derive(Clone, Debug, Message)]
/// Escape hatch for sending a raw matchmaker client message.
pub struct SendMatchmakerMessage {
    /// Message to send.
    pub message: ClientMessage,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when the websocket transport connects.
pub struct MatchmakerClientConnected;

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when the server acknowledges websocket protocol.
pub struct MatchmakerProtocolReady {
    /// Negotiated protocol details.
    pub protocol: MatchmakerProtocol,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when the websocket transport disconnects.
pub struct MatchmakerClientDisconnected {
    /// Disconnect reason.
    pub reason: String,
    /// Whether the session task is reconnecting.
    pub reconnecting: bool,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when identity is resolved.
pub struct MatchmakerIdentityResolved {
    /// Resolved player summary.
    pub player: PlayerSummary,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when lobby state changes.
pub struct MatchmakerLobbyUpdated {
    /// Updated lobby state.
    pub lobby: Lobby,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when assignment preparation starts.
pub struct MatchmakerAssignmentPreparing {
    /// Assignment id being prepared.
    pub assignment_id: String,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted for queue/progress updates.
pub struct MatchmakerQueueProgress {
    /// Human-readable progress message.
    pub message: String,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when a connection grant is ready.
pub struct ConnectionGrantReady {
    /// Result returned by the matchmaker.
    pub result: MatchmakerClientResult,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when a matchmaker request or session fails.
pub struct MatchmakerClientFailed {
    /// Human-readable error message.
    pub message: String,
    /// Structured error information.
    pub error: MatchmakerClientErrorInfo,
}

#[derive(Clone, Debug)]
/// Result of a successful matchmaker request-play workflow.
pub struct MatchmakerClientResult {
    /// Resolved player identity, when received from the server.
    pub player: Option<PlayerSummary>,
    /// Assignment id seen before readiness, when received.
    pub assignment_id: Option<String>,
    /// Connection grant returned by the matchmaker.
    pub grant: ConnectionGrant,
    /// Server messages observed before completion.
    pub messages: Vec<ServerMessage>,
}

#[derive(Debug, Error)]
/// Errors returned by the client helper.
pub enum MatchmakerClientError {
    /// WebSocket connection or send/receive failure.
    #[error("websocket error: {0}")]
    WebSocket(String),
    /// Server protocol failure or error response.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// JSON serialization or deserialization failure.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Result type used by the Bevy client helper.
pub type Result<T> = std::result::Result<T, MatchmakerClientError>;

/// Opens a WebSocket, sends one request-play message, and waits for a grant.
pub async fn request_play_once(
    websocket_url: impl AsRef<str>,
    request: RequestPlay,
) -> Result<MatchmakerClientResult> {
    #[cfg(not(target_arch = "wasm32"))]
    let (mut socket, _) = tokio_tungstenite::connect_async(websocket_url.as_ref())
        .await
        .map_err(|error| MatchmakerClientError::WebSocket(error.to_string()))?;
    #[cfg(target_arch = "wasm32")]
    let mut socket = tokio_tungstenite_wasm::connect(websocket_url.as_ref().to_string())
        .await
        .map_err(|error| MatchmakerClientError::WebSocket(format!("{error:?}")))?;

    send_client_message_once(&mut socket, ClientMessage::from(request)).await?;

    let mut player = None;
    let mut assignment_id = None;
    let mut messages = Vec::new();
    while let Some(message) = socket.next().await {
        let message =
            message.map_err(|error| MatchmakerClientError::WebSocket(error.to_string()))?;
        let WebSocketMessage::Text(payload) = message else {
            continue;
        };
        let message = serde_json::from_str::<ServerMessage>(payload.as_ref())?;
        match &message {
            ServerMessage::IdentityResolved { player: summary } => {
                player = Some(summary.clone());
            }
            ServerMessage::AssignmentPreparing { assignment_id: id } => {
                assignment_id = Some(id.clone());
            }
            ServerMessage::AssignmentReady { connect } => {
                let grant = connect.clone();
                messages.push(message);
                return Ok(MatchmakerClientResult {
                    player,
                    assignment_id,
                    grant,
                    messages,
                });
            }
            ServerMessage::Error {
                code,
                message,
                retryable,
            } => {
                return Err(MatchmakerClientError::Protocol(format!(
                    "{code}: {message} (retryable={retryable})"
                )));
            }
            ServerMessage::Hello { .. } => {}
            ServerMessage::LobbyUpdated { .. } => {}
            ServerMessage::QueueProgress { .. } => {}
        }
        messages.push(message);
    }

    Err(MatchmakerClientError::Protocol(
        "websocket closed before assignment.ready".to_string(),
    ))
}

async fn send_client_message_once<S>(socket: &mut S, message: ClientMessage) -> Result<()>
where
    S: futures_util::Sink<WebSocketMessage> + Unpin,
    S::Error: std::fmt::Display,
{
    let payload = serde_json::to_string(&message)?;
    socket
        .send(text_message(payload))
        .await
        .map_err(|error| MatchmakerClientError::WebSocket(error.to_string()))
}

fn ensure_matchmaker_session(
    config: Res<MatchmakerClientConfig>,
    runtime: Res<MatchmakerClientRuntime>,
) {
    #[cfg(not(target_arch = "wasm32"))]
    {
        if !runtime.mark_session_started() {
            return;
        }
        let Some(command_rx) = runtime.take_command_rx() else {
            return;
        };
        let events_tx = runtime.events_tx.clone();
        let config = config.clone();
        thread::spawn(move || {
            let tokio_runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = events_tx.send(ClientRuntimeEvent::TransportError {
                        message: error.to_string(),
                        retryable: false,
                    });
                    return;
                }
            };
            tokio_runtime.block_on(run_session_loop(config, command_rx, events_tx));
        });
    }

    #[cfg(target_arch = "wasm32")]
    {
        let _ = config;
        if runtime.mark_session_started() {
            let _ = runtime.events_tx.send(ClientRuntimeEvent::TransportError {
                message: "persistent matchmaker websocket sessions are not implemented for wasm yet; use request_play_once".to_string(),
                retryable: false,
            });
        }
    }
}

fn forward_client_commands(
    runtime: Res<MatchmakerClientRuntime>,
    mut request_play: MessageReader<RequestPlay>,
    mut create_lobby: MessageReader<CreateLobby>,
    mut join_lobby: MessageReader<JoinLobbyCode>,
    mut set_ready: MessageReader<SetLobbyReady>,
    mut raw_messages: MessageReader<SendMatchmakerMessage>,
    mut failed: MessageWriter<MatchmakerClientFailed>,
) {
    for request in request_play.read() {
        send_command(&runtime, ClientMessage::from(request.clone()), &mut failed);
    }
    for request in create_lobby.read() {
        send_command(&runtime, ClientMessage::from(request.clone()), &mut failed);
    }
    for request in join_lobby.read() {
        send_command(&runtime, ClientMessage::from(request.clone()), &mut failed);
    }
    for request in set_ready.read() {
        send_command(&runtime, ClientMessage::from(request.clone()), &mut failed);
    }
    for message in raw_messages.read() {
        send_command(&runtime, message.message.clone(), &mut failed);
    }
}

fn send_command(
    runtime: &MatchmakerClientRuntime,
    message: ClientMessage,
    failed: &mut MessageWriter<MatchmakerClientFailed>,
) {
    if let Err(error) = runtime.command_tx.send(message) {
        let message = format!("failed to queue matchmaker command: {error}");
        failed.write(MatchmakerClientFailed {
            message: message.clone(),
            error: MatchmakerClientErrorInfo {
                code: None,
                message,
                retryable: true,
            },
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_client_runtime_events(
    runtime: Res<MatchmakerClientRuntime>,
    mut state: ResMut<MatchmakerClientState>,
    mut connected: MessageWriter<MatchmakerClientConnected>,
    mut protocol_ready: MessageWriter<MatchmakerProtocolReady>,
    mut disconnected: MessageWriter<MatchmakerClientDisconnected>,
    mut identity_resolved: MessageWriter<MatchmakerIdentityResolved>,
    mut lobby_updated: MessageWriter<MatchmakerLobbyUpdated>,
    mut assignment_preparing: MessageWriter<MatchmakerAssignmentPreparing>,
    mut queue_progress: MessageWriter<MatchmakerQueueProgress>,
    mut grant_ready: MessageWriter<ConnectionGrantReady>,
    mut failed: MessageWriter<MatchmakerClientFailed>,
) {
    let receiver = match runtime.events_rx.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    while let Ok(event) = receiver.try_recv() {
        match event {
            ClientRuntimeEvent::SessionConnected => {
                state.connected = true;
                state.last_error = None;
                connected.write(MatchmakerClientConnected);
            }
            ClientRuntimeEvent::SessionDisconnected {
                reason,
                reconnecting,
            } => {
                state.connected = false;
                disconnected.write(MatchmakerClientDisconnected {
                    reason,
                    reconnecting,
                });
            }
            ClientRuntimeEvent::TransportError { message, retryable } => {
                let error = MatchmakerClientErrorInfo {
                    code: None,
                    message,
                    retryable,
                };
                state.last_error = Some(error.clone());
                state.grant_state = ClientConnectionGrantState::Failed;
                failed.write(MatchmakerClientFailed {
                    message: error.message.clone(),
                    error,
                });
            }
            ClientRuntimeEvent::ServerMessage(message) => {
                handle_server_message(
                    &mut state,
                    message,
                    &mut protocol_ready,
                    &mut identity_resolved,
                    &mut lobby_updated,
                    &mut assignment_preparing,
                    &mut queue_progress,
                    &mut grant_ready,
                    &mut failed,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_server_message(
    state: &mut MatchmakerClientState,
    message: ServerMessage,
    protocol_ready: &mut MessageWriter<MatchmakerProtocolReady>,
    identity_resolved: &mut MessageWriter<MatchmakerIdentityResolved>,
    lobby_updated: &mut MessageWriter<MatchmakerLobbyUpdated>,
    assignment_preparing: &mut MessageWriter<MatchmakerAssignmentPreparing>,
    queue_progress: &mut MessageWriter<MatchmakerQueueProgress>,
    grant_ready: &mut MessageWriter<ConnectionGrantReady>,
    failed: &mut MessageWriter<MatchmakerClientFailed>,
) {
    match message {
        ServerMessage::Hello {
            protocol_version,
            min_protocol_version,
            max_protocol_version,
            server,
        } => {
            let protocol = MatchmakerProtocol {
                protocol_version,
                min_protocol_version,
                max_protocol_version,
                server,
            };
            state.protocol = Some(protocol.clone());
            protocol_ready.write(MatchmakerProtocolReady { protocol });
        }
        ServerMessage::IdentityResolved { player } => {
            state.player = Some(player.clone());
            identity_resolved.write(MatchmakerIdentityResolved { player });
        }
        ServerMessage::LobbyUpdated { lobby } => {
            state.lobby = Some(lobby.clone());
            lobby_updated.write(MatchmakerLobbyUpdated { lobby });
        }
        ServerMessage::QueueProgress { message } => {
            queue_progress.write(MatchmakerQueueProgress { message });
        }
        ServerMessage::AssignmentPreparing { assignment_id } => {
            state.assignment_id = Some(assignment_id.clone());
            state.grant_state = ClientConnectionGrantState::Preparing;
            assignment_preparing.write(MatchmakerAssignmentPreparing { assignment_id });
        }
        ServerMessage::AssignmentReady { connect } => {
            state.grant = Some(connect.clone());
            state.grant_state = ClientConnectionGrantState::Ready;
            grant_ready.write(ConnectionGrantReady {
                result: MatchmakerClientResult {
                    player: state.player.clone(),
                    assignment_id: state.assignment_id.clone(),
                    grant: connect,
                    messages: Vec::new(),
                },
            });
        }
        ServerMessage::Error {
            code,
            message,
            retryable,
        } => {
            let error = MatchmakerClientErrorInfo {
                code: Some(code),
                message,
                retryable,
            };
            state.last_error = Some(error.clone());
            state.grant_state = ClientConnectionGrantState::Failed;
            failed.write(MatchmakerClientFailed {
                message: error.message.clone(),
                error,
            });
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_session_loop(
    config: MatchmakerClientConfig,
    command_rx: std_mpsc::Receiver<ClientMessage>,
    events_tx: std_mpsc::Sender<ClientRuntimeEvent>,
) {
    loop {
        match run_one_session(&config, &command_rx, &events_tx).await {
            Ok(()) => {}
            Err(error) => {
                let _ = events_tx.send(ClientRuntimeEvent::TransportError {
                    message: error.to_string(),
                    retryable: config.reconnect,
                });
            }
        }

        let reconnecting = config.reconnect;
        let _ = events_tx.send(ClientRuntimeEvent::SessionDisconnected {
            reason: "websocket session ended".to_string(),
            reconnecting,
        });
        if !reconnecting {
            break;
        }
        tokio::time::sleep(config.reconnect_delay).await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_one_session(
    config: &MatchmakerClientConfig,
    command_rx: &std_mpsc::Receiver<ClientMessage>,
    events_tx: &std_mpsc::Sender<ClientRuntimeEvent>,
) -> Result<()> {
    let (mut socket, _) = tokio_tungstenite::connect_async(&config.websocket_url)
        .await
        .map_err(|error| MatchmakerClientError::WebSocket(error.to_string()))?;
    let _ = events_tx.send(ClientRuntimeEvent::SessionConnected);
    send_client_message_once(
        &mut socket,
        ClientMessage::Hello {
            protocol_version: config.protocol_version,
            client: config.client_label.clone(),
        },
    )
    .await?;

    loop {
        while let Ok(command) = command_rx.try_recv() {
            send_client_message_once(&mut socket, command).await?;
        }

        match tokio::time::timeout(Duration::from_millis(20), socket.next()).await {
            Ok(Some(Ok(WebSocketMessage::Text(payload)))) => {
                let message = serde_json::from_str::<ServerMessage>(payload.as_ref())?;
                let _ = events_tx.send(ClientRuntimeEvent::ServerMessage(message));
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => {
                return Err(MatchmakerClientError::WebSocket(error.to_string()));
            }
            Ok(None) => return Ok(()),
            Err(_) => {}
        }
    }
}

#[derive(Debug)]
enum ClientRuntimeEvent {
    SessionConnected,
    SessionDisconnected { reason: String, reconnecting: bool },
    ServerMessage(ServerMessage),
    TransportError { message: String, retryable: bool },
}

#[cfg(not(target_arch = "wasm32"))]
fn text_message(payload: String) -> WebSocketMessage {
    WebSocketMessage::Text(payload.into())
}

#[cfg(target_arch = "wasm32")]
fn text_message(payload: String) -> WebSocketMessage {
    WebSocketMessage::Text(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_app::App;

    #[test]
    fn request_play_converts_to_wire_message() {
        let message = ClientMessage::from(RequestPlay::new("demo", "dev"));
        assert!(matches!(
            message,
            ClientMessage::RequestPlay {
                game,
                version,
                room: RoomSelection::Auto,
                ..
            } if game == "demo" && version == "dev"
        ));
    }

    #[test]
    fn lobby_commands_convert_to_wire_messages() {
        let create = ClientMessage::from(CreateLobby::new("demo", "dev", 2));
        assert!(matches!(
            create,
            ClientMessage::LobbyCreate {
                game,
                version,
                max_players: 2,
                ..
            } if game == "demo" && version == "dev"
        ));

        let join = ClientMessage::from(JoinLobbyCode::new("ABCD"));
        assert!(matches!(
            join,
            ClientMessage::LobbyJoinCode { code, .. } if code == "ABCD"
        ));

        let ready = ClientMessage::from(SetLobbyReady { ready: true });
        assert!(matches!(
            ready,
            ClientMessage::LobbySetReady { ready: true }
        ));
    }

    #[test]
    fn plugin_registers_client_resources() {
        let mut app = App::new();
        app.add_plugins(LightyearMatchmakerClientPlugin::new(
            MatchmakerClientConfig::new("ws://127.0.0.1:3000/ws"),
        ));
        assert!(
            app.world()
                .get_resource::<MatchmakerClientConfig>()
                .is_some()
        );
        assert!(
            app.world()
                .get_resource::<MatchmakerClientState>()
                .is_some()
        );
    }
}
