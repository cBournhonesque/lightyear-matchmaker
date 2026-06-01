//! Edgegap provider bridge.
//!
//! A provider is the matchmaker's capacity backend. The Edgegap provider maps a
//! matchmaker allocation request to an Edgegap session/deployment and returns
//! the endpoint/server identity that should receive assignments.
//!
//! This crate contains both a real Edgegap session provider and a mock/static
//! Edgegap-shaped provider used by local tests. The real provider follows the
//! Bevygap flow: create a session, poll it until it links to a ready deployment,
//! extract the deployment endpoint, preserve Edgegap ids in allocation metadata,
//! and release by deleting the Edgegap session.

#![allow(async_fn_in_trait)]

use lightyear_matchmaker_core::{
    AllocationId, AllocationRequest, CapacityQuery, LatencyReport, MatchmakerError, PlayerId,
    ProviderKind, Result, ServerAllocation, ServerCapacity, ServerEndpoint, ServerId,
    ServerProvider,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Configuration for Edgegap providers.
pub struct EdgegapProviderConfig {
    #[serde(default)]
    /// Edgegap app name. When empty, the allocation request game is used.
    pub app: String,
    #[serde(default)]
    /// Edgegap app version. When empty, the allocation request version is used.
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Direct Edgegap API key. If absent, `api_key_env` is read.
    pub api_key: Option<String>,
    #[serde(default = "default_api_key_env")]
    /// Environment variable used to load the Edgegap API key.
    pub api_key_env: String,
    #[serde(default = "default_base_url")]
    /// Edgegap API base URL.
    pub base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional webhook URL passed to Edgegap session creation.
    pub webhook_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional deployment request id to force session placement onto a deployment.
    pub deployment_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional Edgegap region override for new sessions.
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional Edgegap country override for new sessions.
    pub country: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional Edgegap city override for new sessions.
    pub city: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional deployment port mapping name to use for the game endpoint.
    pub port_name: Option<String>,
    #[serde(default = "default_session_ready_timeout_secs")]
    /// Maximum seconds to wait for a created Edgegap session to become ready.
    pub session_ready_timeout_secs: u64,
    #[serde(default = "default_session_poll_ms")]
    /// Poll interval in milliseconds while waiting for Edgegap session readiness.
    pub session_poll_ms: u64,
    #[serde(default = "default_release_missing_ok")]
    /// Whether release treats Edgegap 404/410 responses as successful cleanup.
    pub release_missing_ok: bool,
    #[serde(default)]
    /// Mock deployments available for local allocation tests.
    pub deployments: Vec<EdgegapDeploymentConfig>,
}

impl EdgegapProviderConfig {
    /// Parses Edgegap provider configuration from TOML text.
    pub fn from_toml_str(value: &str) -> Result<Self> {
        toml::from_str(value).map_err(|error| MatchmakerError::Config(error.to_string()))
    }
}

impl Default for EdgegapProviderConfig {
    fn default() -> Self {
        Self {
            app: String::new(),
            version: String::new(),
            api_key: None,
            api_key_env: default_api_key_env(),
            base_url: default_base_url(),
            webhook_url: None,
            deployment_request_id: None,
            region: None,
            country: None,
            city: None,
            port_name: None,
            session_ready_timeout_secs: default_session_ready_timeout_secs(),
            session_poll_ms: default_session_poll_ms(),
            release_missing_ok: default_release_missing_ok(),
            deployments: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Mock Edgegap deployment entry.
pub struct EdgegapDeploymentConfig {
    /// Edgegap deployment id.
    pub deployment_id: String,
    /// Game-server id represented by this deployment.
    pub server_id: String,
    /// Game name served by this deployment.
    pub game: String,
    /// Game version served by this deployment.
    pub version: String,
    /// Public endpoint for client connections.
    pub endpoint: ServerEndpoint,
    #[serde(default = "default_ready")]
    /// Whether this deployment is ready.
    pub ready: bool,
    #[serde(default)]
    /// Current session count.
    pub current_sessions: u32,
    #[serde(default = "default_max_sessions")]
    /// Maximum session count.
    pub max_sessions: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional Edgegap session id.
    pub session_id: Option<String>,
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
/// Real Edgegap provider backed by Edgegap's session API.
pub struct EdgegapProvider {
    config: EdgegapProviderConfig,
    client: reqwest::Client,
    api_key: String,
}

impl EdgegapProvider {
    /// Creates a real Edgegap provider from configuration.
    pub fn new(config: EdgegapProviderConfig) -> Result<Self> {
        let api_key = config
            .api_key
            .clone()
            .or_else(|| std::env::var(&config.api_key_env).ok())
            .filter(|key| !key.trim().is_empty())
            .ok_or_else(|| {
                MatchmakerError::Config(format!(
                    "Edgegap API key is required; set edgegap_provider.api_key or {}",
                    config.api_key_env
                ))
            })?;
        Ok(Self {
            config,
            client: reqwest::Client::new(),
            api_key,
        })
    }

    /// Returns the provider configuration.
    pub fn config(&self) -> &EdgegapProviderConfig {
        &self.config
    }

    async fn create_session(
        &self,
        request: &AllocationRequest,
    ) -> Result<EdgegapSessionCreateResponse> {
        let payload = self.session_create_payload(request);
        let url = self.url("/v1/session");
        self.send_json(
            self.client.post(url).header("authorization", &self.api_key),
            &payload,
        )
        .await
    }

    async fn get_session(&self, session_id: &str) -> Result<EdgegapSessionGetResponse> {
        let url = self.url(&format!(
            "/v1/session/{}",
            percent_encode_path_segment(session_id)
        ));
        self.send(self.client.get(url).header("authorization", &self.api_key))
            .await
    }

    async fn delete_session(&self, session_id: &str) -> Result<()> {
        let url = self.url(&format!(
            "/v1/session/{}",
            percent_encode_path_segment(session_id)
        ));
        let response = self
            .client
            .delete(url)
            .header("authorization", &self.api_key)
            .send()
            .await
            .map_err(edgegap_transport_error)?;
        let status = response.status();
        if status.is_success()
            || (self.config.release_missing_ok
                && matches!(status, StatusCode::NOT_FOUND | StatusCode::GONE))
        {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        Err(edgegap_status_error("delete session", status, body))
    }

    async fn wait_for_ready_session(&self, session_id: &str) -> Result<EdgegapSessionGetResponse> {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(
                self.config.session_ready_timeout_secs.max(1),
            ))
            .expect("deadline should be representable");
        let poll_interval = Duration::from_millis(self.config.session_poll_ms.max(1));

        loop {
            let session = self.get_session(session_id).await?;
            if session.ready {
                return Ok(session);
            }
            if Instant::now() >= deadline {
                // A created session may already have linked capacity on Edgegap's
                // side. Delete it on timeout so a failed allocation attempt does
                // not leak a seat or deployment reservation.
                let _ = self.delete_session(session_id).await;
                return Err(MatchmakerError::Provider(format!(
                    "Edgegap session {session_id} did not become ready within {}s; last status: {}",
                    self.config.session_ready_timeout_secs, session.status
                )));
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    fn session_create_payload(&self, request: &AllocationRequest) -> EdgegapSessionCreateRequest {
        let best_region = best_latency_region(&request.latencies);
        // Edgegap can choose placement from either the caller IP list or an
        // explicit location hint. Configured location wins over client-reported
        // latency so operators can pin a test or regional deployment.
        EdgegapSessionCreateRequest {
            app_name: edgegap_app_name(&self.config, request),
            version_name: Some(edgegap_version_name(&self.config, request)),
            ip_list: player_ip(&request.player_id).map(|ip| vec![ip.to_string()]),
            deployment_request_id: self.config.deployment_request_id.clone(),
            region: self.config.region.clone().or(best_region),
            country: self.config.country.clone(),
            city: self.config.city.clone(),
            webhook_url: self.config.webhook_url.clone(),
        }
    }

    async fn send_json<T: Serialize, U: serde::de::DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        payload: &T,
    ) -> Result<U> {
        self.send(request.json(payload)).await
    }

    async fn send<T: serde::de::DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T> {
        let response = request.send().await.map_err(edgegap_transport_error)?;
        let status = response.status();
        let body = response.text().await.map_err(edgegap_transport_error)?;
        if !status.is_success() {
            return Err(edgegap_status_error("Edgegap API request", status, body));
        }
        serde_json::from_str(&body).map_err(|error| {
            MatchmakerError::Provider(format!(
                "failed to decode Edgegap response: {error}; {body}"
            ))
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.config.base_url.trim_end_matches('/'), path)
    }
}

impl ServerProvider for EdgegapProvider {
    async fn allocate(&self, request: AllocationRequest) -> Result<ServerAllocation> {
        request.validate()?;
        let session = self.create_session(&request).await?;
        let session_id = session.session_id.clone();
        let ready_session = self.wait_for_ready_session(&session_id).await?;
        match self.allocation_from_session(&request, &ready_session) {
            Ok(allocation) => Ok(allocation),
            Err(error) => {
                let _ = self.delete_session(&session_id).await;
                Err(error)
            }
        }
    }

    async fn release(&self, allocation_id: AllocationId) -> Result<()> {
        let session_id = session_id_from_allocation_id(&allocation_id)?;
        self.delete_session(&session_id).await
    }

    async fn list_capacity(&self, _request: CapacityQuery) -> Result<Vec<ServerCapacity>> {
        Ok(Vec::new())
    }
}

impl EdgegapProvider {
    fn allocation_from_session(
        &self,
        request: &AllocationRequest,
        session: &EdgegapSessionGetResponse,
    ) -> Result<ServerAllocation> {
        // In Edgegap's model the session is the releasable allocation, while the
        // linked deployment is the concrete game-server endpoint clients connect
        // to. Keep both ids in the returned allocation metadata.
        let deployment = session.deployment.as_ref().ok_or_else(|| {
            MatchmakerError::Provider(format!(
                "Edgegap session {} is ready but has no deployment",
                session.session_id
            ))
        })?;
        let server_id = ServerId::new(deployment.request_id.clone());
        if request.avoids_server(&server_id) {
            return Err(MatchmakerError::NoCapacity);
        }
        let port = select_edgegap_port(deployment, self.config.port_name.as_deref())?;
        let public_ip = deployment.public_ip.parse::<IpAddr>().map_err(|error| {
            MatchmakerError::Provider(format!(
                "Edgegap deployment {} returned invalid public_ip {}: {error}",
                deployment.request_id, deployment.public_ip
            ))
        })?;
        let endpoint = ServerEndpoint {
            public_ip,
            port: port.external_port()?,
        };
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "edgegap.app".to_string(),
            edgegap_app_name(&self.config, request),
        );
        metadata.insert(
            "edgegap.version".to_string(),
            edgegap_version_name(&self.config, request),
        );
        metadata.insert("edgegap.session_id".to_string(), session.session_id.clone());
        metadata.insert("edgegap.session_status".to_string(), session.status.clone());
        metadata.insert(
            "edgegap.deployment_id".to_string(),
            deployment.request_id.clone(),
        );
        if let Some(location) = &deployment.location
            && let Some(region) = location
                .region
                .as_ref()
                .or(location.city.as_ref())
                .or(location.country.as_ref())
        {
            metadata.insert("edgegap.region".to_string(), region.clone());
        }
        if let Some(port_name) = &port.name {
            metadata.insert("edgegap.port_name".to_string(), port_name.clone());
        }

        Ok(ServerAllocation {
            allocation_id: AllocationId::new(edgegap_session_allocation_id(&session.session_id)),
            server_id,
            provider: ProviderKind::Edgegap,
            endpoint,
            game: request.game.clone(),
            version: request.version.clone(),
            cert_digest: None,
            metadata,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
struct EdgegapSessionCreateRequest {
    app_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip_list: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deployment_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    webhook_url: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct EdgegapSessionCreateResponse {
    session_id: String,
}

#[derive(Clone, Debug, Deserialize)]
struct EdgegapSessionGetResponse {
    session_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    ready: bool,
    #[serde(default)]
    deployment: Option<EdgegapDeployment>,
}

#[derive(Clone, Debug, Deserialize)]
struct EdgegapDeployment {
    request_id: String,
    public_ip: String,
    #[serde(default)]
    ports: Option<BTreeMap<String, EdgegapPortMapping>>,
    #[serde(default)]
    location: Option<EdgegapDeploymentLocation>,
}

#[derive(Clone, Debug, Deserialize)]
struct EdgegapDeploymentLocation {
    #[serde(default)]
    city: Option<String>,
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    region: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct EdgegapPortMapping {
    #[serde(default)]
    external: Option<i32>,
    #[serde(default)]
    name: Option<String>,
}

impl EdgegapPortMapping {
    fn external_port(&self) -> Result<u16> {
        let external = self.external.ok_or_else(|| {
            MatchmakerError::Provider("Edgegap deployment port has no external port".to_string())
        })?;
        u16::try_from(external).map_err(|_| {
            MatchmakerError::Provider(format!("Edgegap external port {external} is not a u16"))
        })
    }
}

fn select_edgegap_port<'a>(
    deployment: &'a EdgegapDeployment,
    port_name: Option<&str>,
) -> Result<&'a EdgegapPortMapping> {
    let ports = deployment
        .ports
        .as_ref()
        .filter(|ports| !ports.is_empty())
        .ok_or_else(|| {
            MatchmakerError::Provider(format!(
                "Edgegap deployment {} has no ports",
                deployment.request_id
            ))
        })?;
    if let Some(port_name) = port_name {
        // Edgegap deployments expose ports in a keyed map, and the map key is
        // not always the same as the optional port `name`. Accept either so the
        // config can match both API shapes.
        return ports
            .iter()
            .find(|(key, port)| {
                key.as_str() == port_name || port.name.as_deref() == Some(port_name)
            })
            .map(|(_, port)| port)
            .ok_or_else(|| {
                MatchmakerError::Provider(format!(
                    "Edgegap deployment {} has no port named {port_name}",
                    deployment.request_id
                ))
            });
    }
    ports.values().next().ok_or_else(|| {
        MatchmakerError::Provider(format!(
            "Edgegap deployment {} has no usable ports",
            deployment.request_id
        ))
    })
}

fn edgegap_transport_error(error: reqwest::Error) -> MatchmakerError {
    MatchmakerError::Provider(format!("Edgegap API transport error: {error}"))
}

fn edgegap_status_error(operation: &str, status: StatusCode, body: String) -> MatchmakerError {
    MatchmakerError::Provider(format!("{operation} failed with status {status}: {body}"))
}

fn edgegap_app_name(config: &EdgegapProviderConfig, request: &AllocationRequest) -> String {
    if config.app.is_empty() {
        request.game.clone()
    } else {
        config.app.clone()
    }
}

fn edgegap_version_name(config: &EdgegapProviderConfig, request: &AllocationRequest) -> String {
    if config.version.is_empty() {
        request.version.clone()
    } else {
        config.version.clone()
    }
}

fn player_ip(player_id: &PlayerId) -> Option<IpAddr> {
    // The default identity resolver emits `ip:<addr>`, while tests and custom
    // resolvers may pass the raw IP. Non-IP identities simply skip Edgegap's
    // `ip_list` placement hint.
    player_id
        .0
        .strip_prefix("ip:")
        .unwrap_or(player_id.0.as_str())
        .parse()
        .ok()
}

fn best_latency_region(latencies: &[LatencyReport]) -> Option<String> {
    latencies
        .iter()
        .min_by_key(|latency| latency.rtt_ms)
        .map(|latency| latency.region.clone())
}

const EDGEGAP_SESSION_ALLOCATION_PREFIX: &str = "edgegap-session:";

fn edgegap_session_allocation_id(session_id: &str) -> String {
    format!("{EDGEGAP_SESSION_ALLOCATION_PREFIX}{session_id}")
}

fn session_id_from_allocation_id(allocation_id: &AllocationId) -> Result<String> {
    // Release is intentionally strict: only allocations produced by the real
    // Edgegap provider encode a session id that can be deleted through the
    // Edgegap session API.
    allocation_id
        .0
        .strip_prefix(EDGEGAP_SESSION_ALLOCATION_PREFIX)
        .map(ToOwned::to_owned)
        .filter(|session_id| !session_id.is_empty())
        .ok_or_else(|| {
            MatchmakerError::Provider(format!(
                "allocation id {} is not an Edgegap session allocation",
                allocation_id
            ))
        })
}

fn percent_encode_path_segment(value: &str) -> String {
    // Session ids normally look URL-safe, but keep path construction defensive
    // so release cannot be confused by unexpected provider ids.
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

#[derive(Clone, Debug)]
/// Mock Edgegap-shaped provider used to test the provider boundary.
pub struct MockEdgegapProvider {
    config: EdgegapProviderConfig,
}

impl MockEdgegapProvider {
    /// Creates a mock Edgegap provider from configuration.
    pub fn new(config: EdgegapProviderConfig) -> Self {
        Self { config }
    }

    fn capacities(&self) -> Vec<ServerCapacity> {
        self.config
            .deployments
            .iter()
            .map(|deployment| ServerCapacity {
                server_id: ServerId::new(deployment.server_id.clone()),
                provider: ProviderKind::Edgegap,
                endpoint: deployment.endpoint.clone(),
                game: deployment.game.clone(),
                version: deployment.version.clone(),
                ready: deployment.ready,
                total_players: deployment.current_sessions,
                max_players: deployment.max_sessions,
                max_rooms: 1,
                region: deployment.region.clone(),
                cert_digest: deployment.cert_digest.clone(),
                cpu_percent: None,
                rooms: Vec::new(),
                metadata: edgegap_metadata(&self.config, deployment),
            })
            .collect()
    }
}

impl ServerProvider for MockEdgegapProvider {
    async fn allocate(&self, request: AllocationRequest) -> Result<ServerAllocation> {
        request.validate()?;
        let Some(deployment) = self
            .config
            .deployments
            .iter()
            .filter(|deployment| accepts_request(deployment, &request))
            .min_by_key(|deployment| {
                (
                    latency_rank(deployment, &request),
                    deployment.current_sessions,
                    deployment.deployment_id.as_str(),
                )
            })
        else {
            return Err(MatchmakerError::NoCapacity);
        };

        Ok(ServerAllocation {
            allocation_id: AllocationId::new(format!(
                "edgegap:{}:{}",
                deployment.deployment_id, request.player_id
            )),
            server_id: ServerId::new(deployment.server_id.clone()),
            provider: ProviderKind::Edgegap,
            endpoint: deployment.endpoint.clone(),
            game: deployment.game.clone(),
            version: deployment.version.clone(),
            cert_digest: deployment.cert_digest.clone(),
            metadata: edgegap_metadata(&self.config, deployment),
        })
    }

    async fn release(&self, _allocation_id: AllocationId) -> Result<()> {
        Ok(())
    }

    async fn list_capacity(&self, request: CapacityQuery) -> Result<Vec<ServerCapacity>> {
        Ok(self
            .capacities()
            .into_iter()
            .filter(|capacity| capacity.game == request.game && capacity.version == request.version)
            .collect())
    }
}

fn accepts_request(deployment: &EdgegapDeploymentConfig, request: &AllocationRequest) -> bool {
    deployment.ready
        && deployment.game == request.game
        && deployment.version == request.version
        && deployment.current_sessions < deployment.max_sessions.max(1)
        && !request
            .avoid_server_ids
            .iter()
            .any(|server_id| server_id.0 == deployment.server_id)
}

fn latency_rank(deployment: &EdgegapDeploymentConfig, request: &AllocationRequest) -> u32 {
    let Some(region) = &deployment.region else {
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

fn edgegap_metadata(
    config: &EdgegapProviderConfig,
    deployment: &EdgegapDeploymentConfig,
) -> BTreeMap<String, String> {
    let mut metadata = deployment.metadata.clone();
    if !config.app.is_empty() {
        metadata.insert("edgegap.app".to_string(), config.app.clone());
    }
    if !config.version.is_empty() {
        metadata.insert("edgegap.version".to_string(), config.version.clone());
    }
    metadata.insert(
        "edgegap.deployment_id".to_string(),
        deployment.deployment_id.clone(),
    );
    if let Some(session_id) = &deployment.session_id {
        metadata.insert("edgegap.session_id".to_string(), session_id.clone());
    }
    metadata
}

fn default_ready() -> bool {
    true
}

fn default_max_sessions() -> u32 {
    64
}

fn default_api_key_env() -> String {
    "EDGEGAP_API_KEY".to_string()
}

fn default_base_url() -> String {
    "https://api.edgegap.com".to_string()
}

fn default_session_ready_timeout_secs() -> u64 {
    60
}

fn default_session_poll_ms() -> u64 {
    200
}

fn default_release_missing_ok() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightyear_matchmaker_core::{
        LatencyReport, LatencyTransport, PlayerId, RequestId, RoomSelection, ServerEndpoint,
    };
    use std::collections::VecDeque;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[tokio::test]
    async fn real_provider_creates_polls_and_releases_edgegap_session() {
        let server = MockHttpServer::new(vec![
            (
                200,
                serde_json::json!({ "session_id": "session-1" }).to_string(),
            ),
            (200, ready_session_json()),
            (200, serde_json::json!({ "deleted": true }).to_string()),
        ]);
        let provider = EdgegapProvider::new(EdgegapProviderConfig {
            app: "demo-app".to_string(),
            version: "dev".to_string(),
            api_key: Some("test-key".to_string()),
            base_url: server.base_url(),
            session_poll_ms: 1,
            port_name: Some("game".to_string()),
            ..Default::default()
        })
        .unwrap();

        let allocation = provider
            .allocate(AllocationRequest {
                request_id: RequestId::new("request-1"),
                game: "demo".to_string(),
                version: "dev".to_string(),
                player_id: PlayerId::new("ip:127.0.0.1"),
                lobby_id: None,
                room: RoomSelection::Auto,
                latencies: vec![LatencyReport {
                    region: "new-york".to_string(),
                    rtt_ms: 12,
                    transport: LatencyTransport::Http,
                }],
                avoid_server_ids: Vec::new(),
            })
            .await
            .unwrap();

        assert_eq!(allocation.provider, ProviderKind::Edgegap);
        assert_eq!(
            allocation.allocation_id,
            AllocationId::new("edgegap-session:session-1")
        );
        assert_eq!(allocation.server_id, ServerId::new("deployment-1"));
        assert_eq!(allocation.endpoint.port, 7777);
        assert_eq!(
            allocation
                .metadata
                .get("edgegap.session_id")
                .map(String::as_str),
            Some("session-1")
        );
        assert_eq!(
            allocation
                .metadata
                .get("edgegap.deployment_id")
                .map(String::as_str),
            Some("deployment-1")
        );

        provider.release(allocation.allocation_id).await.unwrap();
        let requests = server.finish();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /v1/session "));
        assert!(requests[0].contains("\"app_name\":\"demo-app\""));
        assert!(requests[0].contains("\"version_name\":\"dev\""));
        assert!(requests[0].contains("\"ip_list\":[\"127.0.0.1\"]"));
        assert!(requests[0].contains("\"region\":\"new-york\""));
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("authorization: test-key")
        );
        assert!(requests[1].starts_with("GET /v1/session/session-1 "));
        assert!(requests[2].starts_with("DELETE /v1/session/session-1 "));
    }

    #[tokio::test]
    async fn real_provider_release_treats_missing_session_as_success_by_default() {
        let server = MockHttpServer::new(vec![(
            404,
            serde_json::json!({ "message": "not found" }).to_string(),
        )]);
        let provider = EdgegapProvider::new(EdgegapProviderConfig {
            api_key: Some("test-key".to_string()),
            base_url: server.base_url(),
            ..Default::default()
        })
        .unwrap();

        provider
            .release(AllocationId::new("edgegap-session:already-gone"))
            .await
            .unwrap();

        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("DELETE /v1/session/already-gone "));
    }

    #[tokio::test]
    async fn real_provider_releases_session_when_ready_deployment_is_avoided() {
        let server = MockHttpServer::new(vec![
            (
                200,
                serde_json::json!({ "session_id": "session-avoided" }).to_string(),
            ),
            (200, ready_session_json()),
            (200, serde_json::json!({ "deleted": true }).to_string()),
        ]);
        let provider = EdgegapProvider::new(EdgegapProviderConfig {
            api_key: Some("test-key".to_string()),
            base_url: server.base_url(),
            session_poll_ms: 1,
            port_name: Some("game".to_string()),
            ..Default::default()
        })
        .unwrap();

        let result = provider
            .allocate(AllocationRequest {
                request_id: RequestId::new("request-avoided"),
                game: "demo".to_string(),
                version: "dev".to_string(),
                player_id: PlayerId::new("ip:127.0.0.1"),
                lobby_id: None,
                room: RoomSelection::Auto,
                latencies: Vec::new(),
                avoid_server_ids: vec![ServerId::new("deployment-1")],
            })
            .await;

        assert!(matches!(result, Err(MatchmakerError::NoCapacity)));
        let requests = server.finish();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /v1/session "));
        assert!(requests[1].starts_with("GET /v1/session/session-avoided "));
        assert!(requests[2].starts_with("DELETE /v1/session/session-avoided "));
    }

    #[tokio::test]
    async fn mock_provider_allocates_edgegap_metadata() {
        let allocation = provider()
            .allocate(AllocationRequest {
                request_id: RequestId::new("request-1"),
                game: "demo".to_string(),
                version: "dev".to_string(),
                player_id: PlayerId::new("ip:127.0.0.1"),
                lobby_id: None,
                room: RoomSelection::Auto,
                latencies: Vec::new(),
                avoid_server_ids: Vec::new(),
            })
            .await
            .unwrap();

        assert_eq!(allocation.provider, ProviderKind::Edgegap);
        assert_eq!(allocation.server_id, ServerId::new("local-dev"));
        assert_eq!(
            allocation
                .metadata
                .get("edgegap.deployment_id")
                .map(String::as_str),
            Some("deployment-local")
        );
    }

    #[tokio::test]
    async fn mock_provider_prefers_low_latency_region() {
        let allocation = provider()
            .allocate(AllocationRequest {
                request_id: RequestId::new("request-2"),
                game: "demo".to_string(),
                version: "dev".to_string(),
                player_id: PlayerId::new("ip:127.0.0.1"),
                lobby_id: None,
                room: RoomSelection::Auto,
                latencies: vec![
                    LatencyReport {
                        region: "remote".to_string(),
                        rtt_ms: 20,
                        transport: LatencyTransport::Http,
                    },
                    LatencyReport {
                        region: "local".to_string(),
                        rtt_ms: 40,
                        transport: LatencyTransport::Http,
                    },
                ],
                avoid_server_ids: Vec::new(),
            })
            .await
            .unwrap();

        assert_eq!(allocation.server_id, ServerId::new("remote-dev"));
    }

    #[tokio::test]
    async fn mock_provider_skips_avoided_servers() {
        let allocation = provider()
            .allocate(AllocationRequest {
                request_id: RequestId::new("request-3"),
                game: "demo".to_string(),
                version: "dev".to_string(),
                player_id: PlayerId::new("ip:127.0.0.1"),
                lobby_id: None,
                room: RoomSelection::Auto,
                latencies: vec![
                    LatencyReport {
                        region: "local".to_string(),
                        rtt_ms: 10,
                        transport: LatencyTransport::Http,
                    },
                    LatencyReport {
                        region: "remote".to_string(),
                        rtt_ms: 100,
                        transport: LatencyTransport::Http,
                    },
                ],
                avoid_server_ids: vec![ServerId::new("local-dev")],
            })
            .await
            .unwrap();

        assert_eq!(allocation.server_id, ServerId::new("remote-dev"));
    }

    fn provider() -> MockEdgegapProvider {
        MockEdgegapProvider::new(EdgegapProviderConfig {
            app: "demo-app".to_string(),
            version: "dev".to_string(),
            deployments: vec![
                deployment("deployment-local", "local-dev", "local", 0),
                deployment("deployment-remote", "remote-dev", "remote", 0),
            ],
            ..Default::default()
        })
    }

    fn deployment(
        deployment_id: &str,
        server_id: &str,
        region: &str,
        current_sessions: u32,
    ) -> EdgegapDeploymentConfig {
        EdgegapDeploymentConfig {
            deployment_id: deployment_id.to_string(),
            server_id: server_id.to_string(),
            game: "demo".to_string(),
            version: "dev".to_string(),
            endpoint: ServerEndpoint {
                public_ip: "127.0.0.1".parse().unwrap(),
                port: 7777,
            },
            ready: true,
            current_sessions,
            max_sessions: 64,
            session_id: Some(format!("session-{deployment_id}")),
            region: Some(region.to_string()),
            cert_digest: None,
            metadata: BTreeMap::new(),
        }
    }

    fn ready_session_json() -> String {
        serde_json::json!({
            "session_id": "session-1",
            "status": "Ready",
            "ready": true,
            "linked": true,
            "kind": "default",
            "user_count": 1,
            "app_version": 1,
            "create_time": "2026-01-01T00:00:00Z",
            "elapsed": 1,
            "deployment": {
                "request_id": "deployment-1",
                "public_ip": "127.0.0.1",
                "status": "Ready",
                "ready": true,
                "fqdn": "deployment-1.example",
                "ports": {
                    "game": {
                        "external": 7777,
                        "internal": 7777,
                        "protocol": "UDP",
                        "name": "game"
                    }
                },
                "location": {
                    "region": "new-york",
                    "country": "US",
                    "city": "New York"
                }
            }
        })
        .to_string()
    }

    struct MockHttpServer {
        base_url: String,
        requests: Arc<Mutex<Vec<String>>>,
        handle: thread::JoinHandle<()>,
    }

    impl MockHttpServer {
        fn new(responses: Vec<(u16, String)>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_for_thread = Arc::clone(&requests);
            let handle = thread::spawn(move || {
                let mut responses = VecDeque::from(responses);
                while let Some((status, body)) = responses.pop_front() {
                    let (mut stream, _) = listener.accept().unwrap();
                    let mut buffer = [0_u8; 8192];
                    let read = stream.read(&mut buffer).unwrap();
                    let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                    requests_for_thread.lock().unwrap().push(request);
                    let reason = match status {
                        200 => "OK",
                        404 => "Not Found",
                        _ => "Response",
                    };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).unwrap();
                }
            });
            Self {
                base_url,
                requests,
                handle,
            }
        }

        fn base_url(&self) -> String {
            self.base_url.clone()
        }

        fn finish(self) -> Vec<String> {
            self.handle.join().unwrap();
            Arc::try_unwrap(self.requests)
                .unwrap()
                .into_inner()
                .unwrap()
        }
    }
}
