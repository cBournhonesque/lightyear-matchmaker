//! Versioned WebSocket protocol messages shared by clients and the server.
//!
//! These DTOs are intentionally small and transport-friendly; the deployable
//! server maps them onto identity, lobby, allocation, and token issuing logic.

use crate::{ConnectionGrant, LatencyReport, Lobby, PlayerId, RoomSelection, error::ErrorCode};
use serde::{Deserialize, Serialize};

/// Current websocket protocol version.
pub const WEBSOCKET_PROTOCOL_VERSION: u16 = 1;

/// Lowest websocket protocol version this crate can speak.
pub const MIN_WEBSOCKET_PROTOCOL_VERSION: u16 = 1;

/// Highest websocket protocol version this crate can speak.
pub const MAX_WEBSOCKET_PROTOCOL_VERSION: u16 = WEBSOCKET_PROTOCOL_VERSION;

/// Returns whether a client websocket protocol version is supported.
pub fn is_supported_websocket_protocol_version(version: u16) -> bool {
    (MIN_WEBSOCKET_PROTOCOL_VERSION..=MAX_WEBSOCKET_PROTOCOL_VERSION).contains(&version)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Client-to-matchmaker WebSocket messages.
pub enum ClientMessage {
    /// Optional protocol greeting from a client.
    Hello {
        /// Client protocol version.
        #[serde(alias = "protocol")]
        protocol_version: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Optional client implementation label.
        client: Option<String>,
    },
    /// Request a playable server assignment immediately.
    RequestPlay {
        /// Requested game name.
        game: String,
        /// Requested game version.
        version: String,
        #[serde(default)]
        /// Requested room selection behavior.
        room: RoomSelection,
        #[serde(default)]
        /// Optional region latency hints.
        latencies: Vec<LatencyReport>,
    },
    /// Create a lobby.
    LobbyCreate {
        /// Lobby game name.
        game: String,
        /// Lobby game version.
        version: String,
        #[serde(default = "default_lobby_max_players")]
        /// Required lobby size before assignment.
        max_players: u32,
        #[serde(default)]
        /// Optional region latency hints from the creating player.
        latencies: Vec<LatencyReport>,
    },
    /// Join a lobby by its short code.
    LobbyJoinCode {
        /// Join code displayed by the lobby owner.
        code: String,
        #[serde(default)]
        /// Optional region latency hints from the joining player.
        latencies: Vec<LatencyReport>,
    },
    /// Update the current player's ready state in their lobby.
    LobbySetReady {
        /// New ready state.
        ready: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Matchmaker-to-client WebSocket messages.
pub enum ServerMessage {
    /// Server protocol greeting.
    Hello {
        /// Protocol version the server will speak on this connection.
        protocol_version: u16,
        /// Minimum protocol version supported by this server.
        min_protocol_version: u16,
        /// Maximum protocol version supported by this server.
        max_protocol_version: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Optional server implementation label.
        server: Option<String>,
    },
    /// Player identity has been resolved.
    IdentityResolved {
        /// Resolved player summary.
        player: PlayerSummary,
    },
    /// Lobby state changed.
    LobbyUpdated {
        /// Updated lobby state.
        lobby: Lobby,
    },
    /// Queue or matchmaking progress update.
    QueueProgress {
        /// Human-readable progress message.
        message: String,
    },
    /// Assignment has been created and is waiting for game-server preparation.
    AssignmentPreparing {
        /// Assignment id being prepared.
        assignment_id: String,
    },
    /// Assignment is ready and includes connection material.
    AssignmentReady {
        /// Connection grant for the assigned server.
        connect: ConnectionGrant,
    },
    /// Request failed.
    Error {
        /// Stable error code.
        code: ErrorCode,
        /// Human-readable error message.
        message: String,
        #[serde(default)]
        /// Whether the client may retry the operation with backoff.
        retryable: bool,
    },
}

impl ServerMessage {
    /// Builds a server hello response for the current protocol.
    pub fn hello() -> Self {
        Self::Hello {
            protocol_version: WEBSOCKET_PROTOCOL_VERSION,
            min_protocol_version: MIN_WEBSOCKET_PROTOCOL_VERSION,
            max_protocol_version: MAX_WEBSOCKET_PROTOCOL_VERSION,
            server: Some("lightyear-matchmaker".to_string()),
        }
    }

    /// Builds an error response using default retryability for the code.
    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::Error {
            code,
            message: message.into(),
            retryable: code.retryable(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Lightweight player summary sent to clients.
pub struct PlayerSummary {
    /// Player id.
    pub id: PlayerId,
    /// Display label.
    pub display_name: String,
}

fn default_lobby_max_players() -> u32 {
    2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_accepts_legacy_protocol_field() {
        let message = serde_json::from_str::<ClientMessage>(
            r#"{"type":"hello","protocol":1,"client":"web"}"#,
        )
        .unwrap();

        assert!(matches!(
            message,
            ClientMessage::Hello {
                protocol_version: WEBSOCKET_PROTOCOL_VERSION,
                client: Some(_)
            }
        ));
    }

    #[test]
    fn server_error_serializes_retryability() {
        let message = ServerMessage::error(ErrorCode::NoCapacity, "no capacity");
        let value = serde_json::to_value(message).unwrap();

        assert_eq!(value["type"], "error");
        assert_eq!(value["code"], "no_capacity");
        assert_eq!(value["retryable"], true);
    }
}
