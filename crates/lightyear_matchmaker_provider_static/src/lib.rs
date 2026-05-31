//! Static server provider.
//!
//! This provider selects from a configured list of always-known servers. It is
//! useful for local development, bare-metal deployments, and tests that do not
//! need dynamic orchestration.

#![allow(async_fn_in_trait)]

use lightyear_matchmaker_core::{
    AllocationId, AllocationRequest, CapacityQuery, MatchmakerError, ProviderKind, Result,
    ServerAllocation, ServerCapacity, ServerEndpoint, ServerId, ServerProvider,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
/// Configuration for the static server provider.
pub struct StaticProviderConfig {
    #[serde(default)]
    /// Static servers available for allocation.
    pub servers: Vec<StaticServerConfig>,
}

impl StaticProviderConfig {
    /// Parses static provider configuration from TOML text.
    pub fn from_toml_str(value: &str) -> Result<Self> {
        toml::from_str(value).map_err(|error| MatchmakerError::Config(error.to_string()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// One configured static server.
pub struct StaticServerConfig {
    /// Static server id.
    pub id: String,
    /// Game name served by this server.
    pub game: String,
    /// Game version served by this server.
    pub version: String,
    /// Public endpoint for client connections.
    pub endpoint: ServerEndpoint,
    #[serde(default = "default_ready")]
    /// Whether this server should be considered ready.
    pub ready: bool,
    #[serde(default)]
    /// Current player count.
    pub total_players: u32,
    #[serde(default = "default_max_players")]
    /// Maximum supported players.
    pub max_players: u32,
    #[serde(default = "default_max_rooms")]
    /// Maximum supported rooms.
    pub max_rooms: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional region label.
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional transport certificate digest.
    pub cert_digest: Option<String>,
    #[serde(default)]
    /// Additional provider metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
/// Provider that allocates from a configured static server list.
pub struct StaticServerProvider {
    servers: Vec<StaticServerConfig>,
}

impl StaticServerProvider {
    /// Creates a static server provider from configuration.
    pub fn new(config: StaticProviderConfig) -> Self {
        Self {
            servers: config.servers,
        }
    }

    fn capacities(&self) -> Vec<ServerCapacity> {
        self.servers
            .iter()
            .map(|server| ServerCapacity {
                server_id: ServerId::new(server.id.clone()),
                provider: ProviderKind::Static,
                endpoint: server.endpoint.clone(),
                game: server.game.clone(),
                version: server.version.clone(),
                ready: server.ready,
                total_players: server.total_players,
                max_players: server.max_players,
                max_rooms: server.max_rooms,
                region: server.region.clone(),
                cert_digest: server.cert_digest.clone(),
                cpu_percent: None,
                rooms: Vec::new(),
                metadata: server.metadata.clone(),
            })
            .collect()
    }
}

impl ServerProvider for StaticServerProvider {
    async fn allocate(&self, request: AllocationRequest) -> Result<ServerAllocation> {
        request.validate()?;
        let Some(server) = self
            .servers
            .iter()
            .filter(|server| accepts_request(server, &request))
            .min_by_key(|server| {
                (
                    latency_rank(server, &request),
                    server.total_players,
                    server.id.as_str(),
                )
            })
        else {
            return Err(MatchmakerError::NoCapacity);
        };
        Ok(ServerAllocation {
            allocation_id: AllocationId::new(format!("static:{}:{}", server.id, request.player_id)),
            server_id: ServerId::new(server.id.clone()),
            provider: ProviderKind::Static,
            endpoint: server.endpoint.clone(),
            game: server.game.clone(),
            version: server.version.clone(),
            cert_digest: server.cert_digest.clone(),
            metadata: server.metadata.clone(),
        })
    }

    async fn release(&self, _allocation_id: AllocationId) -> Result<()> {
        Ok(())
    }

    async fn list_capacity(&self, request: CapacityQuery) -> Result<Vec<ServerCapacity>> {
        Ok(self
            .capacities()
            .into_iter()
            .filter(|server| server.game == request.game && server.version == request.version)
            .collect())
    }
}

fn accepts_request(server: &StaticServerConfig, request: &AllocationRequest) -> bool {
    server.ready
        && server.game == request.game
        && server.version == request.version
        && server.total_players < server.max_players.max(1)
}

fn latency_rank(server: &StaticServerConfig, request: &AllocationRequest) -> u32 {
    let Some(region) = &server.region else {
        return u32::MAX;
    };
    request
        .latencies
        .iter()
        .filter(|latency| &latency.region == region)
        .map(|latency| latency.rtt_ms)
        .min()
        .unwrap_or(u32::MAX)
}

fn default_ready() -> bool {
    true
}

fn default_max_players() -> u32 {
    64
}

fn default_max_rooms() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightyear_matchmaker_core::{
        LatencyReport, LatencyTransport, PlayerId, RequestId, RoomSelection,
    };

    fn provider() -> StaticServerProvider {
        StaticServerProvider::new(StaticProviderConfig {
            servers: vec![StaticServerConfig {
                id: "local".to_string(),
                game: "demo".to_string(),
                version: "dev".to_string(),
                endpoint: ServerEndpoint {
                    public_ip: "127.0.0.1".parse().unwrap(),
                    port: 7777,
                },
                ready: true,
                total_players: 0,
                max_players: 2,
                max_rooms: 1,
                region: Some("local".to_string()),
                cert_digest: None,
                metadata: BTreeMap::new(),
            }],
        })
    }

    #[tokio::test]
    async fn allocates_matching_ready_server() {
        let allocation = provider()
            .allocate(AllocationRequest {
                request_id: RequestId::new("request-1"),
                game: "demo".to_string(),
                version: "dev".to_string(),
                player_id: PlayerId::new("ip:127.0.0.1"),
                lobby_id: None,
                room: RoomSelection::Auto,
                latencies: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(allocation.server_id, ServerId::new("local"));
        assert_eq!(allocation.endpoint.port, 7777);
    }

    #[tokio::test]
    async fn allocates_lowest_latency_matching_region() {
        let provider = StaticServerProvider::new(StaticProviderConfig {
            servers: vec![
                server("local", "local", 0),
                server("remote", "remote", 0),
                server("unknown", "unknown", 0),
            ],
        });
        let allocation = provider
            .allocate(AllocationRequest {
                request_id: RequestId::new("request-2"),
                game: "demo".to_string(),
                version: "dev".to_string(),
                player_id: PlayerId::new("ip:127.0.0.1"),
                lobby_id: None,
                room: RoomSelection::Auto,
                latencies: vec![
                    LatencyReport {
                        region: "local".to_string(),
                        rtt_ms: 90,
                        transport: LatencyTransport::Http,
                    },
                    LatencyReport {
                        region: "remote".to_string(),
                        rtt_ms: 20,
                        transport: LatencyTransport::Http,
                    },
                ],
            })
            .await
            .unwrap();

        assert_eq!(allocation.server_id, ServerId::new("remote"));
    }

    fn server(id: &str, region: &str, total_players: u32) -> StaticServerConfig {
        StaticServerConfig {
            id: id.to_string(),
            game: "demo".to_string(),
            version: "dev".to_string(),
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            ready: true,
            total_players,
            max_players: 2,
            max_rooms: 1,
            region: Some(region.to_string()),
            cert_digest: None,
            metadata: BTreeMap::new(),
        }
    }
}
