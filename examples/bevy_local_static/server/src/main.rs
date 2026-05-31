//! Headless Bevy game-server example for the local static/NATS smoke path.
//!
//! The example publishes readiness/capacity, polls NATS assignments through the
//! Bevy server plugin, and auto-simulates validated client connections.

use anyhow::{Context as _, bail};
use bevy_app::{App, Startup, Update};
use bevy_ecs::prelude::{Res, ResMut, Resource};
use clap::Parser;
use lightyear_matchmaker_bevy_server::{
    ConnectionValidator, LightyearMatchmakerServerPlugin, MatchmakerServerState, NatsBridgeConfig,
};
use lightyear_matchmaker_core::{
    LightyearClientId, ProviderKind, RegisteredGameServer, ServerEndpoint, ServerId,
};
use lightyear_matchmaker_nats::NatsConfig;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct Args {
    #[arg(
        long,
        default_value = "examples/bevy_local_static/config/game-server.local.toml"
    )]
    config: PathBuf,
    #[arg(long)]
    nats_url: Option<String>,
    #[arg(long)]
    nats_namespace: Option<String>,
    #[arg(long)]
    run_seconds: Option<u64>,
}

fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let mut config = GameServerExampleConfig::from_path(&args.config)?;
    if let Some(url) = args.nats_url {
        config.nats.url = url;
    }
    if let Some(namespace) = args.nats_namespace {
        config.nats.namespace = Some(namespace);
    }
    let socket = UdpSocket::bind(config.server.bind_addr())
        .with_context(|| format!("failed to bind UDP socket {}", config.server.bind_addr()))?;
    socket
        .set_nonblocking(true)
        .context("failed to make UDP socket non-blocking")?;

    let tick_duration = config.demo.tick_duration()?;
    let registered = config.registered_server();
    let plugin = LightyearMatchmakerServerPlugin::new(registered)
        .with_capacity_limits(config.server.max_players, config.server.max_rooms)
        .with_nats_bridge(NatsBridgeConfig {
            nats: config.nats.clone(),
            assignment_poll_interval: Duration::from_millis(config.demo.assignment_poll_ms),
        });

    let mut app = App::new();
    app.insert_resource(config)
        .insert_resource(DemoRuntime::default())
        .insert_resource(BoundTransportSocket(socket))
        .add_plugins(plugin)
        .add_systems(Startup, publish_initial_server_state)
        .add_systems(Update, update_demo_server_state);

    let started = Instant::now();
    loop {
        app.update();
        if args
            .run_seconds
            .is_some_and(|seconds| started.elapsed() >= Duration::from_secs(seconds))
        {
            break;
        }
        thread::sleep(tick_duration);
    }

    info!("bevy local static server stopped");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("bevy_local_static_server=info,lightyear_matchmaker_bevy_server=info")
    });
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[derive(Clone, Debug, Resource, Serialize, Deserialize)]
struct GameServerExampleConfig {
    pub server: GameServerConfig,
    pub nats: NatsConfig,
    #[serde(default)]
    pub demo: DemoConfig,
}

impl GameServerExampleConfig {
    fn from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let value = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        toml::from_str(&value).with_context(|| format!("failed to parse {}", path.display()))
    }

    fn registered_server(&self) -> RegisteredGameServer {
        RegisteredGameServer {
            server_id: ServerId::new(self.server.id.clone()),
            provider: ProviderKind::Static,
            endpoint: self.server.public_endpoint(),
            game: self.server.game.clone(),
            version: self.server.version.clone(),
            region: self.server.region.clone(),
            metadata: self.server.metadata.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GameServerConfig {
    pub id: String,
    pub game: String,
    pub version: String,
    pub bind_ip: IpAddr,
    pub public_ip: IpAddr,
    pub port: u16,
    #[serde(default = "default_max_players")]
    pub max_players: u32,
    #[serde(default = "default_max_rooms")]
    pub max_rooms: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_digest: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl GameServerConfig {
    fn bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_ip, self.port)
    }

    fn public_endpoint(&self) -> ServerEndpoint {
        ServerEndpoint {
            public_ip: self.public_ip,
            port: self.port,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DemoConfig {
    #[serde(default = "default_tick_hz")]
    pub tick_hz: f64,
    #[serde(default = "default_assignment_poll_ms")]
    pub assignment_poll_ms: u64,
    #[serde(default = "default_capacity_publish_ticks")]
    pub capacity_publish_ticks: u64,
    #[serde(default = "default_auto_connect_assigned_clients")]
    pub auto_connect_assigned_clients: bool,
}

impl Default for DemoConfig {
    fn default() -> Self {
        Self {
            tick_hz: default_tick_hz(),
            assignment_poll_ms: default_assignment_poll_ms(),
            capacity_publish_ticks: default_capacity_publish_ticks(),
            auto_connect_assigned_clients: default_auto_connect_assigned_clients(),
        }
    }
}

impl DemoConfig {
    fn tick_duration(&self) -> anyhow::Result<Duration> {
        if !self.tick_hz.is_finite() || self.tick_hz <= 0.0 {
            bail!("demo.tick_hz must be finite and positive");
        }
        Ok(Duration::from_secs_f64(1.0 / self.tick_hz))
    }
}

#[derive(Resource)]
struct BoundTransportSocket(UdpSocket);

#[derive(Default, Resource)]
struct DemoRuntime {
    ticks: u64,
    connected_clients: BTreeSet<LightyearClientId>,
}

fn publish_initial_server_state(
    config: Res<GameServerExampleConfig>,
    socket: Res<BoundTransportSocket>,
    mut state: ResMut<MatchmakerServerState>,
) {
    info!(
        server_id = %config.server.id,
        bind_addr = %socket.0.local_addr().expect("UDP socket has local address"),
        public_endpoint = %config.server.public_endpoint().socket_addr(),
        "bevy local static server started"
    );
    state.set_ready(true, config.server.cert_digest.clone());
    publish_capacity(&config, &mut state, 0);
}

fn update_demo_server_state(
    config: Res<GameServerExampleConfig>,
    mut runtime: ResMut<DemoRuntime>,
    mut state: ResMut<MatchmakerServerState>,
) {
    if config.demo.auto_connect_assigned_clients {
        let newly_assigned = state
            .assignments()
            .filter(|assignment| !runtime.connected_clients.contains(&assignment.client_id))
            .map(|assignment| assignment.client_id)
            .collect::<Vec<_>>();

        for client_id in newly_assigned {
            let validation = state.validate_connection(client_id);
            if let Some(context) = state.assignment_context_for_client(client_id) {
                let player_id = context.player_id.clone();
                let lobby_id = context.lobby_id.as_ref().map(ToString::to_string);
                let team = context.team.clone();
                runtime.connected_clients.insert(client_id);
                if state.client_connected(client_id) {
                    info!(
                        %client_id,
                        %player_id,
                        ?lobby_id,
                        ?team,
                        "validated and accepted assigned client"
                    );
                } else {
                    warn!(%client_id, "assignment disappeared before simulated connection");
                }
                continue;
            }

            warn!(
                %client_id,
                reason = ?validation.rejection_reason(),
                "rejected unassigned client"
            );
        }
    }

    runtime.ticks = runtime.ticks.saturating_add(1);
    if runtime
        .ticks
        .is_multiple_of(config.demo.capacity_publish_ticks.max(1))
    {
        publish_capacity(&config, &mut state, runtime.connected_clients.len() as u32);
    }
}

fn publish_capacity(
    config: &GameServerExampleConfig,
    state: &mut MatchmakerServerState,
    total_players: u32,
) {
    let mut capacity = state.capacity().clone();
    capacity.ready = true;
    capacity.total_players = total_players;
    capacity.max_players = config.server.max_players;
    capacity.max_rooms = config.server.max_rooms;
    capacity.cert_digest.clone_from(&config.server.cert_digest);
    state.publish_capacity(capacity);
}

fn default_tick_hz() -> f64 {
    20.0
}

fn default_assignment_poll_ms() -> u64 {
    250
}

fn default_capacity_publish_ticks() -> u64 {
    20
}

fn default_auto_connect_assigned_clients() -> bool {
    true
}

fn default_max_players() -> u32 {
    64
}

fn default_max_rooms() -> u32 {
    4
}
