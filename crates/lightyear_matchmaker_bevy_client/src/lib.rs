//! Bevy-friendly client helpers for requesting matchmaker assignments.
//!
//! The current surface is intentionally minimal: it opens the matchmaker
//! WebSocket, sends `request_play`, waits for `assignment.ready`, and exposes
//! the resulting connection grant to game code.

use bevy_app::{App, Plugin, Update};
use bevy_ecs::prelude::{Message, MessageReader, MessageWriter, Res, Resource};
use futures_util::{SinkExt as _, StreamExt as _};
use lightyear_matchmaker_core::{
    ClientMessage, ConnectionGrant, LatencyReport, PlayerSummary, RoomSelection, ServerMessage,
};
use std::sync::{Mutex, mpsc as std_mpsc};
#[cfg(not(target_arch = "wasm32"))]
use std::thread;
use thiserror::Error;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
#[cfg(target_arch = "wasm32")]
use tokio_tungstenite_wasm::Message as WebSocketMessage;

#[derive(Clone, Debug)]
/// Bevy plugin that exposes a request-play message workflow.
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
        let (results_tx, results_rx) = std_mpsc::channel();
        app.insert_resource(self.config.clone())
            .insert_resource(MatchmakerClientRuntime {
                results_tx,
                results_rx: Mutex::new(results_rx),
            })
            .add_message::<RequestPlay>()
            .add_message::<ConnectionGrantReady>()
            .add_message::<MatchmakerClientFailed>()
            .add_systems(Update, (spawn_request_play_tasks, drain_client_results));
    }
}

#[derive(Clone, Debug, Resource)]
/// Bevy client plugin configuration.
pub struct MatchmakerClientConfig {
    /// Matchmaker WebSocket URL.
    pub websocket_url: String,
}

#[derive(Resource)]
struct MatchmakerClientRuntime {
    results_tx: std_mpsc::Sender<ClientTaskResult>,
    results_rx: Mutex<std_mpsc::Receiver<ClientTaskResult>>,
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
/// Bevy message emitted when a connection grant is ready.
pub struct ConnectionGrantReady {
    /// Result returned by the matchmaker.
    pub result: MatchmakerClientResult,
}

#[derive(Clone, Debug, Message)]
/// Bevy message emitted when a matchmaker request fails.
pub struct MatchmakerClientFailed {
    /// Human-readable error message.
    pub message: String,
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

    let request = serde_json::to_string(&ClientMessage::from(request))?;
    #[cfg(not(target_arch = "wasm32"))]
    socket
        .send(WebSocketMessage::Text(request.into()))
        .await
        .map_err(|error| MatchmakerClientError::WebSocket(error.to_string()))?;
    #[cfg(target_arch = "wasm32")]
    socket
        .send(WebSocketMessage::Text(request))
        .await
        .map_err(|error| MatchmakerClientError::WebSocket(format!("{error:?}")))?;

    let mut player = None;
    let mut assignment_id = None;
    let mut messages = Vec::new();
    while let Some(message) = socket.next().await {
        let message =
            message.map_err(|error| MatchmakerClientError::WebSocket(error.to_string()))?;
        let WebSocketMessage::Text(payload) = message else {
            continue;
        };
        let message = serde_json::from_str::<ServerMessage>(&payload)?;
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
            ServerMessage::Error { code, message } => {
                return Err(MatchmakerClientError::Protocol(format!(
                    "{code}: {message}"
                )));
            }
            ServerMessage::LobbyUpdated { .. } => {}
            ServerMessage::QueueProgress { .. } => {}
        }
        messages.push(message);
    }

    Err(MatchmakerClientError::Protocol(
        "websocket closed before assignment.ready".to_string(),
    ))
}

fn spawn_request_play_tasks(
    mut requests: MessageReader<RequestPlay>,
    config: Res<MatchmakerClientConfig>,
    runtime: Res<MatchmakerClientRuntime>,
) {
    for request in requests.read() {
        let request = request.clone();
        let websocket_url = config.websocket_url.clone();
        let results_tx = runtime.results_tx.clone();
        #[cfg(not(target_arch = "wasm32"))]
        thread::spawn(move || {
            let result = run_request_play_blocking(websocket_url, request);
            let _ = results_tx.send(result);
        });
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(async move {
            let result = request_play_once(websocket_url, request).await;
            let _ = results_tx.send(result);
        });
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn run_request_play_blocking(websocket_url: String, request: RequestPlay) -> ClientTaskResult {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return Err(MatchmakerClientError::WebSocket(error.to_string())),
    };
    runtime.block_on(request_play_once(websocket_url, request))
}

fn drain_client_results(
    runtime: Res<MatchmakerClientRuntime>,
    mut ready: MessageWriter<ConnectionGrantReady>,
    mut failed: MessageWriter<MatchmakerClientFailed>,
) {
    let receiver = match runtime.results_rx.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    while let Ok(result) = receiver.try_recv() {
        match result {
            Ok(result) => {
                ready.write(ConnectionGrantReady { result });
            }
            Err(error) => {
                failed.write(MatchmakerClientFailed {
                    message: error.to_string(),
                });
            }
        }
    }
}

type ClientTaskResult = Result<MatchmakerClientResult>;

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
    fn plugin_registers_client_resources() {
        let mut app = App::new();
        app.add_plugins(LightyearMatchmakerClientPlugin::new(
            MatchmakerClientConfig {
                websocket_url: "ws://127.0.0.1:3000/ws".to_string(),
            },
        ));
        assert!(
            app.world()
                .get_resource::<MatchmakerClientConfig>()
                .is_some()
        );
    }
}
