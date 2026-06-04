//! Configuration types for the deployable matchmaker server.

use anyhow::Context as _;
use lightyear_matchmaker_lightyear::NetcodeTokenConfig;
use lightyear_matchmaker_nats::NatsConfig;
use lightyear_matchmaker_provider_edgegap::EdgegapProviderConfig;
use lightyear_matchmaker_provider_gameflow::GameflowProviderConfig;
use lightyear_matchmaker_provider_static::StaticProviderConfig;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Top-level configuration for the deployable matchmaker server.
pub struct MatchmakerConfig {
    /// HTTP/WebSocket listener configuration.
    pub server: ServerConfig,
    /// Default game identity used when client messages omit game/version.
    pub game: GameConfig,
    /// Lightyear Netcode token issuing configuration.
    pub lightyear: NetcodeTokenConfig,
    #[serde(default)]
    /// Identity resolver configuration.
    pub identity: IdentityConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional NATS coordination backend configuration.
    pub nats: Option<NatsConfig>,
    #[serde(default)]
    /// Allocation and assignment lifecycle configuration.
    pub allocation: AllocationConfig,
    #[serde(default)]
    /// Configured static provider settings.
    pub static_provider: StaticProviderConfig,
    #[serde(default)]
    /// Edgegap provider settings used by both real and mock Edgegap sources.
    pub edgegap_provider: EdgegapProviderConfig,
    #[serde(default)]
    /// Gameflow provider settings.
    pub gameflow_provider: GameflowProviderConfig,
}

impl MatchmakerConfig {
    /// Parses matchmaker configuration from TOML text.
    pub fn from_toml_str(value: &str) -> anyhow::Result<Self> {
        toml::from_str(value).context("failed to parse matchmaker config")
    }

    /// Loads and parses matchmaker configuration from a TOML file path.
    pub fn from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let value = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Self::from_toml_str(&value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// HTTP server configuration.
pub struct ServerConfig {
    /// Socket address the HTTP/WebSocket server binds to.
    pub bind: SocketAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Default game identity for allocation requests.
pub struct GameConfig {
    /// Game name accepted by providers and game servers.
    pub name: String,
    /// Game version accepted by providers and game servers.
    pub version: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
/// Identity resolver configuration.
pub struct IdentityConfig {
    #[serde(default)]
    /// Whether `x-forwarded-for` should be trusted for IP-derived identity.
    pub trust_forwarded_for: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Allocation and assignment lifecycle configuration.
pub struct AllocationConfig {
    #[serde(default)]
    /// Provider source used by the matchmaker.
    pub source: AllocationSource,
    #[serde(default)]
    /// Whether the client waits for game-server assignment preparation.
    pub require_assignment_prepare: bool,
    #[serde(default = "default_assignment_prepare_timeout_ms")]
    /// Maximum milliseconds to wait for game-server assignment preparation.
    pub assignment_prepare_timeout_ms: u64,
    #[serde(default = "default_assignment_prepare_poll_ms")]
    /// Poll interval in milliseconds while waiting for assignment preparation.
    pub assignment_prepare_poll_ms: u64,
    #[serde(default = "default_assignment_prepare_max_retries")]
    /// Additional full allocation/assignment attempts after preparation failure.
    pub assignment_prepare_max_retries: u32,
    #[serde(default)]
    /// Milliseconds to wait before retrying a failed assignment attempt.
    pub assignment_retry_backoff_ms: u64,
    #[serde(default = "default_assignment_timeout_secs")]
    /// Seconds a prepared assignment may wait for an active game connection.
    pub assignment_timeout_secs: u64,
    #[serde(default = "default_lifecycle_job_max_deliver")]
    /// Maximum lifecycle job delivery attempts before the worker drops the job.
    pub lifecycle_job_max_deliver: i64,
}

impl Default for AllocationConfig {
    fn default() -> Self {
        Self {
            source: AllocationSource::ConfiguredStatic,
            require_assignment_prepare: false,
            assignment_prepare_timeout_ms: default_assignment_prepare_timeout_ms(),
            assignment_prepare_poll_ms: default_assignment_prepare_poll_ms(),
            assignment_prepare_max_retries: default_assignment_prepare_max_retries(),
            assignment_retry_backoff_ms: 0,
            assignment_timeout_secs: default_assignment_timeout_secs(),
            lifecycle_job_max_deliver: default_lifecycle_job_max_deliver(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Runtime provider source selected by configuration.
pub enum AllocationSource {
    #[default]
    /// Select from the configured static server list.
    ConfiguredStatic,
    /// Select from NATS-published static server capacity.
    NatsStatic,
    /// Create and poll Edgegap sessions through the real Edgegap API.
    Edgegap,
    /// Select from the mock Edgegap provider bridge.
    EdgegapMock,
    /// Allocate through the Gameflow API.
    Gameflow,
}

fn default_assignment_prepare_timeout_ms() -> u64 {
    3_000
}

fn default_assignment_prepare_poll_ms() -> u64 {
    25
}

fn default_assignment_prepare_max_retries() -> u32 {
    1
}

fn default_assignment_timeout_secs() -> u64 {
    60
}

fn default_lifecycle_job_max_deliver() -> i64 {
    10
}
