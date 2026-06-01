//! NATS connection and TTL configuration.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// NATS connection and namespace configuration.
pub struct NatsConfig {
    #[serde(default = "default_nats_url")]
    /// NATS server URL.
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional username for NATS user/password authentication.
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional password for NATS user/password authentication.
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional namespace prefix for subjects and KV buckets.
    pub namespace: Option<String>,
    #[serde(default)]
    /// TTL and retention settings for NATS-backed ephemeral coordination data.
    pub ttl: NatsTtlConfig,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: "nats://127.0.0.1:4222".to_string(),
            username: None,
            password: None,
            namespace: None,
            ttl: NatsTtlConfig::default(),
        }
    }
}

fn default_nats_url() -> String {
    "nats://127.0.0.1:4222".to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// TTL and retention settings for NATS coordination buckets and streams.
pub struct NatsTtlConfig {
    #[serde(default = "default_short_ttl_secs")]
    /// Maximum age in seconds for server readiness reports.
    pub server_readiness_secs: u64,
    #[serde(default = "default_short_ttl_secs")]
    /// Maximum age in seconds for server capacity reports.
    pub server_capacity_secs: u64,
    #[serde(default = "default_assignment_ttl_secs")]
    /// Maximum age in seconds for server assignment sets and client indexes.
    pub assignments_secs: u64,
    #[serde(default = "default_assignment_ttl_secs")]
    /// Maximum age in seconds for game-server assignment-prepared acknowledgements.
    pub assignments_prepared_secs: u64,
    #[serde(default = "default_short_ttl_secs")]
    /// Maximum age in seconds for active connection reports.
    pub active_connections_secs: u64,
    #[serde(default = "default_drain_ttl_secs")]
    /// Maximum age in seconds for game-server drain markers.
    pub server_drains_secs: u64,
    #[serde(default = "default_lifecycle_work_ttl_secs")]
    /// Maximum age in seconds for release/delete lifecycle work items.
    pub lifecycle_work_secs: u64,
}

impl Default for NatsTtlConfig {
    fn default() -> Self {
        Self {
            server_readiness_secs: default_short_ttl_secs(),
            server_capacity_secs: default_short_ttl_secs(),
            assignments_secs: default_assignment_ttl_secs(),
            assignments_prepared_secs: default_assignment_ttl_secs(),
            active_connections_secs: default_short_ttl_secs(),
            server_drains_secs: default_drain_ttl_secs(),
            lifecycle_work_secs: default_lifecycle_work_ttl_secs(),
        }
    }
}

fn default_short_ttl_secs() -> u64 {
    30
}

fn default_assignment_ttl_secs() -> u64 {
    60
}

fn default_drain_ttl_secs() -> u64 {
    86_400
}

fn default_lifecycle_work_ttl_secs() -> u64 {
    600
}
