//! Versionable WebSocket protocol messages shared by clients and the server.
//!
//! These DTOs are intentionally small and transport-friendly; the deployable
//! server maps them onto identity, lobby, allocation, and token issuing logic.

use crate::{ConnectionGrant, LatencyReport, Lobby, PlayerId, RoomSelection};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Client-to-matchmaker WebSocket messages.
pub enum ClientMessage {
    /// Optional protocol greeting from a client.
    Hello {
        /// Client protocol version.
        protocol: u16,
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
        code: String,
        /// Human-readable error message.
        message: String,
    },
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
