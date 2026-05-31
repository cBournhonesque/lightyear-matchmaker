//! Transport connection grant and token issuing contract.
//!
//! Core describes the token request/result shape while transport-specific token
//! generation lives in integration crates such as `lightyear_matchmaker_lightyear`.

use crate::{LightyearClientId, PlayerId, Result, ServerAllocation, ServerEndpoint};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Request to issue a connection grant for an allocation.
pub struct TokenRequest {
    /// Allocation the token should connect to.
    pub allocation: ServerAllocation,
    /// Player receiving the token.
    pub player_id: PlayerId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional preselected Lightyear client id.
    pub client_id: Option<LightyearClientId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Connection material returned to a client.
pub struct ConnectionGrant {
    /// Kind of connection material in this grant.
    pub kind: ConnectionGrantKind,
    /// Lightyear client id assigned to the player.
    pub client_id: LightyearClientId,
    /// Server endpoint the client should connect to.
    pub endpoint: ServerEndpoint,
    /// Serialized token or transport-specific connection payload.
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional transport certificate digest.
    pub cert_digest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Supported connection grant kinds.
pub enum ConnectionGrantKind {
    /// Lightyear Netcode connect token.
    LightyearNetcode,
}

/// Issues connection grants for provider allocations.
pub trait TokenIssuer: Send + Sync + 'static {
    /// Issues a connection grant.
    async fn issue(&self, request: TokenRequest) -> Result<ConnectionGrant>;
}
