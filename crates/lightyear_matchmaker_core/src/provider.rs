//! Server provider contract and allocation-related data types.
//!
//! A provider owns server capacity outside the matchmaker. It can be as simple
//! as a configured list of already-running static servers, or as dynamic as an
//! Edgegap bridge that creates a session, waits for deployment readiness, and
//! later releases provider-side state.
//!
//! Providers map a game/version/player or lobby request onto a reachable server
//! allocation without leaking provider-specific APIs into the core model. The
//! returned `allocation_id` is provider-owned capacity/session identity; the
//! matchmaker later creates one or more game-server assignments from that
//! allocation.
//!
//! Capacity returned by a provider or reported by a game server is placement
//! metadata, not a reservation. The current model assumes one matchmaker. A
//! future reservation layer can add matchmaker-owned capacity holds before
//! assignment persistence if multi-matchmaker placement races become a target.

use crate::{LobbyId, MatchmakerError, PlayerId, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, SocketAddr};

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Matchmaker-generated id for one client or lobby allocation request.
///
/// A request id is a correlation/idempotency boundary for the matchmaker's own
/// workflow. One request can produce one provider allocation and one or more
/// player assignments.
pub struct RequestId(pub String);

impl RequestId {
    /// Creates a request id from a string-like value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Stable server instance identifier.
pub struct ServerId(pub String);

impl ServerId {
    /// Creates a server id from a string-like value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Provider-owned allocation identifier.
///
/// An allocation id identifies the provider-side capacity/session/deployment
/// selected for a request. Multiple player assignments can share the same
/// allocation id, for example when a lobby roster is placed on one game server.
/// Static providers may use stable ids because release is a no-op, while dynamic
/// providers should use the provider's session/deployment id.
pub struct AllocationId(pub String);

impl AllocationId {
    /// Creates an allocation id from a string-like value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for AllocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Provider implementation kind.
pub enum ProviderKind {
    /// Static server provider.
    Static,
    /// Edgegap provider.
    Edgegap,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Public endpoint for a game server.
pub struct ServerEndpoint {
    /// Public IP address clients should connect to.
    pub public_ip: IpAddr,
    /// Public port clients should connect to.
    pub port: u16,
}

impl ServerEndpoint {
    /// Returns the endpoint as a socket address.
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.public_ip, self.port)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Request passed to a server provider to allocate capacity.
pub struct AllocationRequest {
    #[serde(default)]
    /// Matchmaker request id that correlates provider allocation and assignments.
    pub request_id: RequestId,
    /// Requested game name.
    pub game: String,
    /// Requested game version.
    pub version: String,
    /// Player that initiated the allocation.
    pub player_id: PlayerId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional lobby being allocated.
    pub lobby_id: Option<LobbyId>,
    #[serde(default)]
    /// Room selection preference.
    pub room: RoomSelection,
    #[serde(default)]
    /// Optional region latency hints.
    pub latencies: Vec<LatencyReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Server ids the provider should avoid for this allocation attempt.
    pub avoid_server_ids: Vec<ServerId>,
}

impl AllocationRequest {
    /// Validates request fields that are shared across providers.
    pub fn validate(&self) -> Result<()> {
        validate_token("game", &self.game, 64)?;
        validate_token("version", &self.version, 64)?;
        Ok(())
    }

    /// Returns whether this request asks providers to avoid a server.
    pub fn avoids_server(&self, server_id: &ServerId) -> bool {
        self.avoid_server_ids
            .iter()
            .any(|avoided| avoided == server_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Server allocation returned by a provider.
pub struct ServerAllocation {
    /// Provider allocation id.
    pub allocation_id: AllocationId,
    /// Allocated server id.
    pub server_id: ServerId,
    /// Provider that produced the allocation.
    pub provider: ProviderKind,
    /// Public endpoint for the allocated server.
    pub endpoint: ServerEndpoint,
    /// Game name served by the allocation.
    pub game: String,
    /// Game version served by the allocation.
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional transport certificate digest.
    pub cert_digest: Option<String>,
    #[serde(default)]
    /// Provider-specific allocation metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Capacity report published by a game server.
///
/// Capacity is server-owned metadata describing how much space the server
/// currently believes it has. A reservation is a separate matchmaker-owned hold
/// on this capacity and should be tracked independently so multiple matchmaker
/// instances cannot over-assign the same server.
pub struct ServerCapacity {
    /// Reporting server id.
    pub server_id: ServerId,
    /// Provider that owns the server.
    pub provider: ProviderKind,
    /// Public endpoint for this server.
    pub endpoint: ServerEndpoint,
    /// Game name served by this server.
    pub game: String,
    /// Game version served by this server.
    pub version: String,
    /// Whether this server is ready for assignments.
    pub ready: bool,
    /// Current player count.
    pub total_players: u32,
    /// Maximum supported players.
    pub max_players: u32,
    /// Maximum supported rooms.
    pub max_rooms: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional provider-neutral region label.
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional transport certificate digest.
    pub cert_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional CPU utilization estimate.
    pub cpu_percent: Option<f32>,
    #[serde(default)]
    /// Per-room capacity metrics.
    pub rooms: Vec<ServerRoomMetrics>,
    #[serde(default)]
    /// Additional capacity metadata.
    pub metadata: BTreeMap<String, String>,
}

impl ServerCapacity {
    /// Returns whether this server has remaining player capacity.
    pub fn has_player_capacity(&self) -> bool {
        self.total_players < self.max_players.max(1)
    }

    /// Returns whether this server has remaining room capacity.
    pub fn has_room_capacity(&self) -> bool {
        (self.rooms.len() as u32) < self.max_rooms.max(1)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Capacity metrics for one room on a server.
pub struct ServerRoomMetrics {
    /// Room key used for matching join requests.
    pub key: String,
    /// Whether this room should be hidden from public browsing.
    pub private: bool,
    /// Current players in the room.
    pub players: u32,
    /// Maximum players in the room.
    pub max_players: u32,
}

impl ServerRoomMetrics {
    /// Returns whether this room has remaining player capacity.
    pub fn has_player_capacity(&self) -> bool {
        self.players < self.max_players.max(1)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", content = "value", rename_all = "snake_case")]
/// Client room selection preference.
pub enum RoomSelection {
    #[default]
    /// Let the provider choose whether to reuse or create a room.
    Auto,
    /// Prefer a new room.
    New,
    /// Join a room by short code.
    Code(String),
    /// Join a room by internal id.
    Id(String),
}

impl RoomSelection {
    /// Returns a normalized room key for explicit room selections.
    pub fn room_key(&self) -> Option<String> {
        match self {
            Self::Auto | Self::New => None,
            Self::Code(code) => Some(format!("code:{}", normalize_room_token(code))),
            Self::Id(id) => Some(format!("id:{}", normalize_room_token(id))),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Client-measured latency to a provider-neutral region.
pub struct LatencyReport {
    /// Region identifier.
    pub region: String,
    /// Round-trip time in milliseconds.
    pub rtt_ms: u32,
    /// Transport used to measure latency.
    pub transport: LatencyTransport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Transport used for a latency measurement.
pub enum LatencyTransport {
    /// HTTP latency measurement.
    Http,
    /// UDP latency measurement.
    Udp,
    /// Other measurement transport.
    Other(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Query for provider capacity.
pub struct CapacityQuery {
    /// Game name to query.
    pub game: String,
    /// Game version to query.
    pub version: String,
}

/// Provider contract for server allocation and capacity listing.
pub trait ServerProvider: Send + Sync + 'static {
    /// Allocates server capacity for a request.
    async fn allocate(&self, request: AllocationRequest) -> Result<ServerAllocation>;
    /// Releases a provider allocation.
    async fn release(&self, allocation_id: AllocationId) -> Result<()>;
    /// Lists capacity matching a query.
    async fn list_capacity(&self, request: CapacityQuery) -> Result<Vec<ServerCapacity>>;
}

fn validate_token(field: &str, value: &str, max_len: usize) -> Result<()> {
    if value.is_empty() {
        return Err(MatchmakerError::InvalidRequest(format!("{field} is empty")));
    }
    if value.len() > max_len {
        return Err(MatchmakerError::InvalidRequest(format!(
            "{field} is too long; max {max_len} bytes"
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '-' | '.'))
    {
        return Err(MatchmakerError::InvalidRequest(format!(
            "{field} contains unsupported characters"
        )));
    }
    Ok(())
}

fn normalize_room_token(value: &str) -> String {
    let normalized = value
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect::<String>();
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized.to_ascii_uppercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_selection_keys_are_stable() {
        assert_eq!(
            RoomSelection::Code("ab-c".to_string())
                .room_key()
                .as_deref(),
            Some("code:AB-C")
        );
        assert_eq!(
            RoomSelection::Id("42".to_string()).room_key().as_deref(),
            Some("id:42")
        );
        assert_eq!(RoomSelection::Auto.room_key(), None);
    }

    #[test]
    fn allocation_request_matches_avoided_servers() {
        let request = AllocationRequest {
            request_id: RequestId::new("request-1"),
            game: "demo".to_string(),
            version: "dev".to_string(),
            player_id: PlayerId::new("player-1"),
            lobby_id: None,
            room: RoomSelection::Auto,
            latencies: Vec::new(),
            avoid_server_ids: vec![ServerId::new("failed-server")],
        };

        assert!(request.avoids_server(&ServerId::new("failed-server")));
        assert!(!request.avoids_server(&ServerId::new("other-server")));
    }
}
