//! Gameflow provider bridge.
//!
//! A provider is the matchmaker's capacity backend. The Gameflow provider maps a
//! matchmaker allocation request to either a fleet allocation or a standalone
//! Gameflow server start request, then returns the endpoint and server identity
//! that should receive matchmaker assignments.
//!
//! Gameflow server lifecycle is expected to be driven by the game server through
//! the Agones SDK HTTP API inside the server container. For that reason provider
//! `release` is currently a no-op; the Bevy game-server crate has an optional
//! `agones` feature for Ready/Health/Shutdown calls.

#![allow(async_fn_in_trait)]

use lightyear_matchmaker_core::{
    AllocationId, AllocationRequest, CapacityQuery, MatchmakerError, ProviderKind, Result,
    ServerAllocation, ServerCapacity, ServerEndpoint, ServerId, ServerProvider,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Configuration for the Gameflow provider.
pub struct GameflowProviderConfig {
    /// Gameflow game id used in allocation URLs.
    pub game_id: String,
    #[serde(default)]
    /// Allocation mode: fleet allocation or standalone server creation.
    pub mode: GameflowAllocationMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Build id used for standalone server creation.
    pub build_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Direct Gameflow API key. If absent, `api_key_env` then `api_key_path` are read.
    pub api_key: Option<String>,
    #[serde(default = "default_api_key_env")]
    /// Environment variable used to load the Gameflow API key.
    pub api_key_env: String,
    #[serde(default = "default_api_key_path")]
    /// Local file used to load the Gameflow API key when no direct/env key exists.
    pub api_key_path: String,
    #[serde(default = "default_base_url")]
    /// Gameflow API base URL.
    pub base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Region override. When absent, the lowest-latency request region is used.
    pub region: Option<String>,
}

impl GameflowProviderConfig {
    /// Parses Gameflow provider configuration from TOML text.
    pub fn from_toml_str(value: &str) -> Result<Self> {
        toml::from_str(value).map_err(|error| MatchmakerError::Config(error.to_string()))
    }
}

impl Default for GameflowProviderConfig {
    fn default() -> Self {
        Self {
            game_id: String::new(),
            mode: GameflowAllocationMode::default(),
            build_id: None,
            api_key: None,
            api_key_env: default_api_key_env(),
            api_key_path: default_api_key_path(),
            base_url: default_base_url(),
            region: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Gameflow allocation API mode.
pub enum GameflowAllocationMode {
    #[default]
    /// Allocate an already-running server from a Gameflow fleet.
    Fleet,
    /// Start a standalone Gameflow server.
    Standalone,
}

#[derive(Clone, Debug)]
/// Provider backed by the Gameflow API.
pub struct GameflowProvider {
    config: GameflowProviderConfig,
    client: reqwest::Client,
    api_key: String,
}

impl GameflowProvider {
    /// Creates a Gameflow provider from configuration.
    pub fn new(config: GameflowProviderConfig) -> Result<Self> {
        let api_key = load_api_key(&config)?.ok_or_else(|| {
            MatchmakerError::Config(format!(
                "Gameflow API key is required; set gameflow_provider.api_key, {}, or {}",
                config.api_key_env, config.api_key_path
            ))
        })?;
        if config.game_id.trim().is_empty() {
            return Err(MatchmakerError::Config(
                "gameflow_provider.game_id is required".to_string(),
            ));
        }
        if config.mode == GameflowAllocationMode::Standalone
            && config.build_id.as_deref().unwrap_or_default().is_empty()
        {
            return Err(MatchmakerError::Config(
                "gameflow_provider.build_id is required for standalone mode".to_string(),
            ));
        }
        Ok(Self {
            config,
            client: reqwest::Client::new(),
            api_key,
        })
    }

    /// Returns the provider configuration.
    pub fn config(&self) -> &GameflowProviderConfig {
        &self.config
    }

    fn allocation_payload(&self, request: &AllocationRequest) -> GameflowAllocationRequest {
        GameflowAllocationRequest {
            region: self
                .config
                .region
                .clone()
                .or_else(|| best_latency_region(request)),
            build_id: self.config.build_id.clone(),
            payload: Some(gameflow_payload(request)),
        }
    }

    async fn send_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        payload: &GameflowAllocationRequest,
    ) -> Result<T> {
        let response = self
            .client
            .post(self.url(path))
            .header("X-Api-Key", &self.api_key)
            .json(payload)
            .send()
            .await
            .map_err(gameflow_transport_error)?;
        let status = response.status();
        let body = response.text().await.map_err(gameflow_transport_error)?;
        if !status.is_success() {
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                return Err(MatchmakerError::NoCapacity);
            }
            return Err(MatchmakerError::Provider(format!(
                "Gameflow API request failed with status {status}: {body}"
            )));
        }
        serde_json::from_str(&body).map_err(|error| {
            MatchmakerError::Provider(format!(
                "failed to decode Gameflow response: {error}; {body}"
            ))
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.config.base_url.trim_end_matches('/'), path)
    }
}

impl ServerProvider for GameflowProvider {
    async fn allocate(&self, request: AllocationRequest) -> Result<ServerAllocation> {
        request.validate()?;
        let payload = self.allocation_payload(&request);
        let server = match self.config.mode {
            GameflowAllocationMode::Fleet => {
                let response = self
                    .send_json::<GameflowFleetAllocateResponse>(
                        &format!(
                            "/v1/fleets/{}/allocate",
                            percent_encode_path_segment(&self.config.game_id)
                        ),
                        &payload,
                    )
                    .await?;
                response.allocation
            }
            GameflowAllocationMode::Standalone => {
                let response = self
                    .send_json::<GameflowStandaloneServerResponse>(
                        &format!(
                            "/v1/games/{}/servers",
                            percent_encode_path_segment(&self.config.game_id)
                        ),
                        &payload,
                    )
                    .await?;
                response.server
            }
        };
        allocation_from_gameflow_server(&self.config, &request, server).await
    }

    async fn release(&self, _allocation_id: AllocationId) -> Result<()> {
        Ok(())
    }

    async fn list_capacity(&self, _request: CapacityQuery) -> Result<Vec<ServerCapacity>> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug, Serialize)]
struct GameflowAllocationRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    #[serde(rename = "buildId", skip_serializing_if = "Option::is_none")]
    build_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct GameflowFleetAllocateResponse {
    allocation: GameflowServer,
}

#[derive(Clone, Debug, Deserialize)]
struct GameflowStandaloneServerResponse {
    server: GameflowServer,
}

#[derive(Clone, Debug, Deserialize)]
struct GameflowServer {
    #[serde(default, alias = "id", alias = "serverId", alias = "server_id")]
    id: Option<String>,
    #[serde(alias = "address", alias = "ip", alias = "host")]
    address: String,
    port: u16,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

async fn allocation_from_gameflow_server(
    config: &GameflowProviderConfig,
    request: &AllocationRequest,
    server: GameflowServer,
) -> Result<ServerAllocation> {
    let public_ip = resolve_gameflow_address(&server.address, server.port).await?;
    let server_id_value = server
        .id
        .clone()
        .unwrap_or_else(|| format!("{}:{}", server.address, server.port));
    let server_id = ServerId::new(server_id_value.clone());
    if request.avoids_server(&server_id) {
        return Err(MatchmakerError::NoCapacity);
    }

    let allocation_id = AllocationId::new(format!("gameflow:{server_id_value}"));
    let mut metadata = BTreeMap::new();
    metadata.insert("gameflow.game_id".to_string(), config.game_id.clone());
    metadata.insert("gameflow.address".to_string(), server.address.clone());
    metadata.insert(
        "gameflow.mode".to_string(),
        mode_label(config.mode).to_string(),
    );
    if let Some(build_id) = &config.build_id {
        metadata.insert("gameflow.build_id".to_string(), build_id.clone());
    }
    if let Some(region) = &server.region {
        metadata.insert("gameflow.region".to_string(), region.clone());
    }
    if let Some(status) = &server.status {
        metadata.insert("gameflow.status".to_string(), status.clone());
    }

    Ok(ServerAllocation {
        allocation_id,
        server_id,
        provider: ProviderKind::Gameflow,
        endpoint: ServerEndpoint {
            public_ip,
            port: server.port,
        },
        game: request.game.clone(),
        version: request.version.clone(),
        cert_digest: None,
        metadata,
    })
}

async fn resolve_gameflow_address(address: &str, port: u16) -> Result<IpAddr> {
    if let Ok(public_ip) = address.parse::<IpAddr>() {
        return Ok(public_ip);
    }
    tokio::net::lookup_host((address, port))
        .await
        .map_err(|error| {
            MatchmakerError::Provider(format!(
                "failed to resolve Gameflow address {address}:{port}: {error}"
            ))
        })?
        .next()
        .map(|addr| addr.ip())
        .ok_or_else(|| {
            MatchmakerError::Provider(format!(
                "Gameflow address {address}:{port} did not resolve to an IP"
            ))
        })
}

fn load_api_key(config: &GameflowProviderConfig) -> Result<Option<String>> {
    if let Some(api_key) = config
        .api_key
        .as_ref()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty())
    {
        return Ok(Some(api_key.to_string()));
    }
    if let Ok(api_key) = std::env::var(&config.api_key_env)
        && !api_key.trim().is_empty()
    {
        return Ok(Some(api_key.trim().to_string()));
    }
    let path = expand_home(&config.api_key_path);
    if !path.exists() {
        return Ok(None);
    }
    let value = std::fs::read_to_string(&path).map_err(|error| {
        MatchmakerError::Config(format!(
            "failed to read Gameflow API key from {}: {error}",
            path.display()
        ))
    })?;
    Ok(extract_api_key(&value))
}

fn extract_api_key(value: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|json| {
            json.get("api_key")
                .or_else(|| json.get("apiKey"))
                .or_else(|| json.get("key"))
                .and_then(|key| key.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        })
}

fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return Path::new(&home).join(rest);
    }
    PathBuf::from(path)
}

fn gameflow_payload(request: &AllocationRequest) -> String {
    serde_json::json!({
        "request_id": request.request_id,
        "game": request.game,
        "version": request.version,
        "player_id": request.player_id,
        "lobby_id": request.lobby_id,
        "room": request.room,
        "latencies": request.latencies,
    })
    .to_string()
}

fn best_latency_region(request: &AllocationRequest) -> Option<String> {
    request
        .latencies
        .iter()
        .min_by_key(|latency| latency.rtt_ms)
        .map(|latency| latency.region.clone())
}

fn mode_label(mode: GameflowAllocationMode) -> &'static str {
    match mode {
        GameflowAllocationMode::Fleet => "fleet",
        GameflowAllocationMode::Standalone => "standalone",
    }
}

fn default_api_key_env() -> String {
    "GAMEFLOW_API_KEY".to_string()
}

fn default_api_key_path() -> String {
    "~/.config/gameflow/key.json".to_string()
}

fn default_base_url() -> String {
    "https://api.gameflow.gg".to_string()
}

fn gameflow_transport_error(error: reqwest::Error) -> MatchmakerError {
    MatchmakerError::Provider(format!("Gameflow API transport error: {error}"))
}

fn percent_encode_path_segment(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightyear_matchmaker_core::{
        LatencyReport, LatencyTransport, PlayerId, RequestId, RoomSelection,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn key_file_accepts_plaintext_or_json() {
        assert_eq!(
            extract_api_key("  secret-key \n").as_deref(),
            Some("secret-key")
        );
        assert_eq!(
            extract_api_key(r#"{"api_key":"secret-json"}"#).as_deref(),
            Some("secret-json")
        );
    }

    #[tokio::test]
    async fn fleet_allocation_posts_to_gameflow() {
        let (base_url, requests) = fake_gameflow(
            200,
            r#"{"allocation":{"id":"server-1","address":"127.0.0.1","port":7777,"region":"us-east"}}"#,
        );
        let provider = GameflowProvider::new(GameflowProviderConfig {
            game_id: "game-1".to_string(),
            api_key: Some("secret".to_string()),
            base_url,
            ..Default::default()
        })
        .unwrap();

        let allocation = provider.allocate(allocation_request()).await.unwrap();

        assert_eq!(allocation.provider, ProviderKind::Gameflow);
        assert_eq!(allocation.server_id, ServerId::new("server-1"));
        assert_eq!(allocation.endpoint.port, 7777);
        let request = requests.recv().unwrap();
        assert!(request.contains("POST /v1/fleets/game-1/allocate HTTP/1.1"));
        assert!(request.contains("x-api-key: secret") || request.contains("X-Api-Key: secret"));
        assert!(request.contains("\"region\":\"us-east\""));
    }

    #[tokio::test]
    async fn standalone_allocation_sends_build_id() {
        let (base_url, requests) = fake_gameflow(
            200,
            r#"{"server":{"id":"standalone-1","address":"localhost","port":7778}}"#,
        );
        let provider = GameflowProvider::new(GameflowProviderConfig {
            game_id: "game-1".to_string(),
            mode: GameflowAllocationMode::Standalone,
            build_id: Some("build-1".to_string()),
            api_key: Some("secret".to_string()),
            base_url,
            ..Default::default()
        })
        .unwrap();

        let allocation = provider.allocate(allocation_request()).await.unwrap();

        assert_eq!(allocation.server_id, ServerId::new("standalone-1"));
        assert!(allocation.endpoint.public_ip.is_loopback());
        let request = requests.recv().unwrap();
        assert!(request.contains("POST /v1/games/game-1/servers HTTP/1.1"));
        assert!(request.contains("\"buildId\":\"build-1\""));
    }

    fn allocation_request() -> AllocationRequest {
        AllocationRequest {
            request_id: RequestId::new("request-1"),
            game: "demo".to_string(),
            version: "dev".to_string(),
            player_id: PlayerId::new("ip:127.0.0.1"),
            lobby_id: None,
            room: RoomSelection::Auto,
            latencies: vec![LatencyReport {
                region: "us-east".to_string(),
                rtt_ms: 10,
                transport: LatencyTransport::Http,
            }],
            avoid_server_ids: Vec::new(),
        }
    }

    fn fake_gameflow(status: u16, body: &'static str) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            tx.send(request).unwrap();
            let response = format!(
                "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (format!("http://{addr}"), rx)
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        let mut buffer = [0u8; 8192];
        let mut data = Vec::new();
        loop {
            let n = stream.read(&mut buffer).unwrap();
            if n == 0 {
                break;
            }
            data.extend_from_slice(&buffer[..n]);
            if let Some(headers_end) = find_headers_end(&data) {
                let headers = String::from_utf8_lossy(&data[..headers_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                    })
                    .unwrap_or(0);
                if data.len() >= headers_end + content_length {
                    break;
                }
            }
        }
        String::from_utf8_lossy(&data).to_string()
    }

    fn find_headers_end(data: &[u8]) -> Option<usize> {
        data.windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
    }
}
