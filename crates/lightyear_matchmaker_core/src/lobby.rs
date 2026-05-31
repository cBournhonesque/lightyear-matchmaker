//! Lobby data model and lobby-store trait.
//!
//! The current server uses an in-process lobby runtime, but these DTOs are kept
//! in core so client, server, and future storage backends share the same shape.

use crate::{LatencyReport, PlayerId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Unique lobby identifier.
pub struct LobbyId(pub String);

impl LobbyId {
    /// Creates a lobby id from a string-like value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for LobbyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Lobby state shared with clients and assignment logic.
pub struct Lobby {
    /// Unique lobby id.
    pub id: LobbyId,
    /// Short code that other clients can use to join.
    pub join_code: String,
    /// Player that owns the lobby.
    pub owner: PlayerId,
    /// Players currently in the lobby.
    pub members: Vec<LobbyMember>,
    /// Game name selected for the lobby.
    pub game: String,
    /// Game version selected for the lobby.
    pub version: String,
    /// Number of players required before the lobby can auto-assign.
    pub max_players: u32,
    /// Whether all required members are ready.
    pub ready: bool,
    #[serde(default)]
    /// Additional lobby metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Player membership inside a lobby.
pub struct LobbyMember {
    /// Member player id.
    pub player_id: PlayerId,
    /// Display label for this member.
    pub display_name: String,
    /// Optional team assignment.
    pub team: Option<String>,
    /// Whether this member has marked ready.
    pub ready: bool,
    #[serde(default)]
    /// Latency reports submitted by this member.
    pub latencies: Vec<LatencyReport>,
    #[serde(default)]
    /// Additional member metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Request to create a lobby.
pub struct CreateLobby {
    /// Player that will own the lobby.
    pub owner: PlayerId,
    /// Game name for the lobby.
    pub game: String,
    /// Game version for the lobby.
    pub version: String,
    /// Maximum or required players for assignment.
    pub max_players: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Request to join an existing lobby.
pub struct JoinLobby {
    /// Lobby to join.
    pub lobby_id: LobbyId,
    /// Player joining the lobby.
    pub player_id: PlayerId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Request to update a lobby member.
pub struct UpdateLobbyMember {
    /// Lobby containing the member.
    pub lobby_id: LobbyId,
    /// Member to update.
    pub player_id: PlayerId,
    /// Optional ready-state update.
    pub ready: Option<bool>,
    /// Optional team update.
    pub team: Option<String>,
}

/// Storage abstraction for lobby state.
pub trait LobbyStore: Send + Sync + 'static {
    /// Creates a lobby.
    async fn create_lobby(&self, request: CreateLobby) -> crate::Result<Lobby>;
    /// Adds a player to a lobby.
    async fn join_lobby(&self, request: JoinLobby) -> crate::Result<Lobby>;
    /// Updates a lobby member.
    async fn update_member(&self, request: UpdateLobbyMember) -> crate::Result<Lobby>;
}
