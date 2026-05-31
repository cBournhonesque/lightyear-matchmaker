//! Lightweight identity resolution types.
//!
//! The current implementation derives a player identity from connection
//! metadata, primarily the remote or trusted forwarded IP address.

use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, SocketAddr};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Provider-neutral player identifier.
pub struct PlayerId(pub String);

impl PlayerId {
    /// Creates a player id from a string-like value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for PlayerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Input used by identity resolvers.
pub struct IdentityRequest {
    /// Remote socket address observed by the matchmaker.
    pub remote_addr: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional `x-forwarded-for` header value.
    pub forwarded_for: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional user-agent header value.
    pub user_agent: Option<String>,
    #[serde(default)]
    /// Additional resolver-specific metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Identity resolved for a connected player.
pub struct ResolvedIdentity {
    /// Stable-enough player id for this session.
    pub player_id: PlayerId,
    /// Display label for logs and lobby surfaces.
    pub display_name: String,
    /// Source that produced this identity.
    pub source: IdentitySource,
    #[serde(default)]
    /// Additional identity metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Source used to resolve a player identity.
pub enum IdentitySource {
    /// Identity derived from an IP address.
    Ip,
}

/// Resolves a connection request into a player identity.
pub trait IdentityResolver: Send + Sync + 'static {
    /// Resolves identity from connection metadata.
    async fn resolve(&self, request: IdentityRequest) -> Result<ResolvedIdentity>;
}

#[derive(Clone, Debug)]
/// Identity resolver that derives player id from the client IP.
pub struct IpIdentityResolver {
    trust_forwarded_for: bool,
}

impl IpIdentityResolver {
    /// Creates an IP identity resolver.
    pub fn new(trust_forwarded_for: bool) -> Self {
        Self {
            trust_forwarded_for,
        }
    }

    fn client_ip(&self, request: &IdentityRequest) -> IpAddr {
        if self.trust_forwarded_for
            && let Some(ip) = request
                .forwarded_for
                .as_deref()
                .and_then(first_forwarded_ip)
        {
            return ip;
        }
        request.remote_addr.ip()
    }
}

impl Default for IpIdentityResolver {
    fn default() -> Self {
        Self::new(false)
    }
}

impl IdentityResolver for IpIdentityResolver {
    async fn resolve(&self, request: IdentityRequest) -> Result<ResolvedIdentity> {
        let ip = self.client_ip(&request);
        let player_id = PlayerId::new(format!("ip:{ip}"));
        let mut metadata = request.metadata;
        metadata.insert("ip".to_string(), ip.to_string());
        if let Some(user_agent) = request.user_agent {
            metadata.insert("user_agent".to_string(), user_agent);
        }
        Ok(ResolvedIdentity {
            display_name: player_id.to_string(),
            player_id,
            source: IdentitySource::Ip,
            metadata,
        })
    }
}

fn first_forwarded_ip(value: &str) -> Option<IpAddr> {
    value
        .split(',')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ip_identity_uses_remote_addr_by_default() {
        let resolver = IpIdentityResolver::default();
        let identity = resolver
            .resolve(IdentityRequest {
                remote_addr: "192.0.2.10:5000".parse().unwrap(),
                forwarded_for: Some("203.0.113.5".to_string()),
                user_agent: None,
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(identity.player_id, PlayerId::new("ip:192.0.2.10"));
    }

    #[tokio::test]
    async fn ip_identity_can_use_trusted_forwarded_for() {
        let resolver = IpIdentityResolver::new(true);
        let identity = resolver
            .resolve(IdentityRequest {
                remote_addr: "192.0.2.10:5000".parse().unwrap(),
                forwarded_for: Some("203.0.113.5, 192.0.2.10".to_string()),
                user_agent: None,
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(identity.player_id, PlayerId::new("ip:203.0.113.5"));
    }
}
