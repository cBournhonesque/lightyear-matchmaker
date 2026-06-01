//! Game-server registration, assignment, readiness, and connection-report types.
//!
//! A game server is the running authoritative game process selected by the
//! matchmaker. It is responsible for publishing readiness and capacity, polling
//! assignments addressed to its `ServerId`, preparing local admission state,
//! validating connecting Lightyear client ids, and reporting active connection
//! changes.
//!
//! The matchmaker creates assignments after a provider returns capacity. An
//! assignment is a request for one game server to accept one Lightyear client id
//! for a player/lobby/team context. When the game server publishes
//! `AssignmentPrepared`, the pending assignment can be consumed from the
//! server's queue and runtime truth moves to active-connection reports.
//!
//! These types describe that provider-independent contract between the
//! matchmaker and a running game server.

use crate::{
    AllocationId, LobbyId, PlayerId, ProviderKind, RequestId, ServerCapacity, ServerEndpoint,
    ServerId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Client identifier used by Lightyear Netcode assignments.
pub struct LightyearClientId(pub u64);

impl LightyearClientId {
    /// Creates a Lightyear client id from its raw numeric value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for LightyearClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Matchmaker-owned identifier for one player assignment.
///
/// Assignment ids are unique per assigned client and are used to coordinate
/// game-server preparation acknowledgements. They must not be derived solely
/// from provider allocation ids, because one provider allocation can produce
/// multiple assignments and repeated requests can reuse the same provider-side
/// capacity.
pub struct AssignmentId(pub String);

impl AssignmentId {
    /// Creates an assignment id from a string-like value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for AssignmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Game server registered with the matchmaker.
pub struct RegisteredGameServer {
    /// Stable server instance id.
    pub server_id: ServerId,
    /// Provider that owns this server.
    pub provider: ProviderKind,
    /// Public endpoint clients should use to connect.
    pub endpoint: ServerEndpoint,
    /// Game name served by this server.
    pub game: String,
    /// Game version served by this server.
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional provider-neutral region label.
    pub region: Option<String>,
    #[serde(default)]
    /// Additional server metadata.
    pub metadata: BTreeMap<String, String>,
}

impl RegisteredGameServer {
    /// Builds an initial capacity snapshot for this server.
    pub fn capacity_snapshot(&self, max_players: u32, max_rooms: u32) -> ServerCapacity {
        ServerCapacity {
            server_id: self.server_id.clone(),
            provider: self.provider.clone(),
            endpoint: self.endpoint.clone(),
            game: self.game.clone(),
            version: self.version.clone(),
            ready: false,
            total_players: 0,
            max_players,
            max_rooms,
            region: self.region.clone(),
            cert_digest: None,
            cpu_percent: None,
            rooms: Vec::new(),
            metadata: self.metadata.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Readiness report published by a game server.
pub struct ServerReadiness {
    /// Reporting server id.
    pub server_id: ServerId,
    /// Provider that owns the server.
    pub provider: ProviderKind,
    /// Public endpoint clients should use to connect.
    pub endpoint: ServerEndpoint,
    /// Game name served by this server.
    pub game: String,
    /// Game version served by this server.
    pub version: String,
    /// Whether the server is ready for assignments.
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional WebTransport certificate digest or other transport certificate material.
    pub cert_digest: Option<String>,
    #[serde(default)]
    /// Additional readiness metadata.
    pub metadata: BTreeMap<String, String>,
}

impl ServerReadiness {
    /// Builds a readiness report from a registered server.
    pub fn from_registered(server: &RegisteredGameServer, ready: bool) -> Self {
        Self {
            server_id: server.server_id.clone(),
            provider: server.provider.clone(),
            endpoint: server.endpoint.clone(),
            game: server.game.clone(),
            version: server.version.clone(),
            ready,
            cert_digest: None,
            metadata: server.metadata.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Operator/matchmaker intent to drain one game server.
///
/// A drain marker is not a capacity report. It means the matchmaker should stop
/// placing new assignments on this server and should cancel pending assignments
/// that have not become active game connections yet. Connected players may keep
/// playing until the game or operator decides to remove them.
pub struct ServerDrain {
    /// Server id that should stop receiving new assignments.
    pub server_id: ServerId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Human-readable reason for the drain.
    pub reason: Option<String>,
    #[serde(default)]
    /// Additional operator or orchestration metadata.
    pub metadata: BTreeMap<String, String>,
}

impl ServerDrain {
    /// Creates a drain marker for a server.
    pub fn new(server_id: ServerId, reason: Option<String>) -> Self {
        Self {
            server_id,
            reason,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Request for a game server to accept one Lightyear client id.
///
/// An assignment is not a capacity report and not a reservation. It is the
/// server-facing admission request created after the matchmaker has selected
/// capacity for a player or lobby.
pub struct AssignmentRecord {
    /// Unique assignment id.
    pub assignment_id: AssignmentId,
    #[serde(default)]
    /// Request id that produced this assignment.
    pub request_id: RequestId,
    /// Provider allocation id this assignment uses.
    ///
    /// This identifies the provider-side capacity/session/deployment and can be
    /// shared by multiple assignment records.
    pub allocation_id: AllocationId,
    /// Server selected for this assignment.
    pub server_id: ServerId,
    /// Lightyear client id authorized by this assignment.
    pub client_id: LightyearClientId,
    /// Player assigned to this client id.
    pub player_id: PlayerId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional lobby associated with the assignment.
    pub lobby_id: Option<LobbyId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional team assigned to this player.
    pub team: Option<String>,
    #[serde(default)]
    /// Full roster assigned to the same match/server.
    pub roster: Vec<AssignmentRosterMember>,
    #[serde(default)]
    /// Match-level metadata shared with the game server.
    pub match_metadata: BTreeMap<String, String>,
    #[serde(default)]
    /// Provider or assignment-specific metadata.
    pub metadata: BTreeMap<String, String>,
}

impl AssignmentRecord {
    /// Finds the roster entry associated with a Lightyear client id.
    pub fn roster_member_for_client(
        &self,
        client_id: LightyearClientId,
    ) -> Option<&AssignmentRosterMember> {
        self.roster
            .iter()
            .find(|member| member.client_id == Some(client_id))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Roster entry for a player assigned to a match.
pub struct AssignmentRosterMember {
    /// Player represented by this roster entry.
    pub player_id: PlayerId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Client id assigned to this player, when known.
    pub client_id: Option<LightyearClientId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Team assigned to this player.
    pub team: Option<String>,
    #[serde(default)]
    /// Player-specific assignment metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Game-server acknowledgement that an assignment has been prepared or rejected.
pub struct AssignmentPrepared {
    /// Assignment being acknowledged.
    pub assignment_id: AssignmentId,
    /// Server that observed the assignment.
    pub server_id: ServerId,
    /// Client id associated with the assignment.
    pub client_id: LightyearClientId,
    /// Whether the assignment was accepted by the game server.
    pub prepared: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional rejection reason.
    pub reason: Option<String>,
    #[serde(default)]
    /// Additional acknowledgement metadata.
    pub metadata: BTreeMap<String, String>,
}

impl AssignmentPrepared {
    /// Builds a successful preparation acknowledgement for an assignment.
    pub fn accepted(assignment: &AssignmentRecord) -> Self {
        Self {
            assignment_id: assignment.assignment_id.clone(),
            server_id: assignment.server_id.clone(),
            client_id: assignment.client_id,
            prepared: true,
            reason: None,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Active connection report from a game server.
pub struct ActiveConnection {
    /// Reporting server id.
    pub server_id: ServerId,
    /// Client id whose connection state changed.
    pub client_id: LightyearClientId,
    /// Player associated with the client id.
    pub player_id: PlayerId,
    /// Whether the client is currently connected.
    pub connected: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
/// Reports published by game servers to the matchmaker coordination backend.
pub enum GameServerReport {
    /// Server readiness report.
    Readiness(ServerReadiness),
    /// Server capacity report.
    Capacity(ServerCapacity),
    /// Assignment preparation acknowledgement.
    AssignmentPrepared(AssignmentPrepared),
    /// Client connection state report.
    ActiveConnection(ActiveConnection),
}
