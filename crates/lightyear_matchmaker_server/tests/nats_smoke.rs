//! NATS-backed integration smoke tests for the deployable matchmaker server.
//!
//! These tests are ignored by default because they require a local NATS server
//! with JetStream enabled and, for the full path, the headless Bevy game-server
//! example.

use futures_util::{SinkExt as _, StreamExt as _};
use lightyear_matchmaker_bevy_client::{RequestPlay, request_play_once};
use lightyear_matchmaker_core::{
    ActiveConnection, AssignmentRecord, ClientMessage, ConnectionGrant, GameServerReport,
    LightyearClientId, Lobby, RoomSelection, ServerEndpoint, ServerId, ServerMessage,
};
use lightyear_matchmaker_lightyear::NetcodeTokenConfig;
use lightyear_matchmaker_nats::{NatsConfig, NatsCoordinator};
use lightyear_matchmaker_provider_edgegap::{EdgegapDeploymentConfig, EdgegapProviderConfig};
use lightyear_matchmaker_provider_static::{StaticProviderConfig, StaticServerConfig};
use lightyear_matchmaker_server::{
    AllocationConfig, AllocationSource, AppState, GameConfig, IdentityConfig, MatchmakerConfig,
    ServerConfig, router,
};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[tokio::test]
#[ignore = "requires a local NATS server with JetStream enabled"]
async fn websocket_request_play_publishes_assignment_to_nats() {
    let nats_url = std::env::var("LIGHTYEAR_MATCHMAKER_NATS_SMOKE_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let namespace = std::env::var("LIGHTYEAR_MATCHMAKER_NATS_SMOKE_NAMESPACE")
        .unwrap_or_else(|_| unique_namespace());
    let nats = NatsConfig {
        url: nats_url,
        namespace: Some(namespace),
        ..Default::default()
    };
    let state = AppState::from_config_with_coordination(config(nats.clone()))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client_result = tokio::time::timeout(
        Duration::from_secs(5),
        request_play_once(
            format!("ws://{addr}/ws"),
            RequestPlay {
                game: "demo".to_string(),
                version: "dev".to_string(),
                room: RoomSelection::Auto,
                latencies: Vec::new(),
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(client_result.player.is_some());
    let saw_preparing = client_result
        .messages
        .iter()
        .position(|message| matches!(message, ServerMessage::AssignmentPreparing { .. }));
    let saw_ready = client_result
        .messages
        .iter()
        .position(|message| matches!(message, ServerMessage::AssignmentReady { .. }));
    assert!(matches!(
        (saw_preparing, saw_ready),
        (Some(preparing), Some(ready)) if preparing < ready
    ));
    let assignment_ready = client_result.grant;

    let coordinator = NatsCoordinator::connect(nats).await.unwrap();
    let assignment = assignment_for_server_client(
        &coordinator,
        &ServerId::new("local-dev"),
        assignment_ready.client_id,
    )
    .await;
    let client_indexed_assignment =
        assignment_for_client(&coordinator, assignment_ready.client_id).await;
    assert_eq!(
        client_indexed_assignment.assignment_id,
        assignment.assignment_id
    );

    assert_eq!(assignment.server_id, ServerId::new("local-dev"));
    assert_eq!(assignment.client_id, assignment_ready.client_id);
    assert_eq!(assignment.player_id.0, "ip:127.0.0.1");
    assert!(!assignment.request_id.0.is_empty());
    assert_ne!(assignment.assignment_id.0, assignment.allocation_id.0);
    assert_eq!(assignment.team.as_deref(), Some("solo"));
    assert_eq!(assignment.roster.len(), 1);
    assert_eq!(
        assignment.roster[0].client_id,
        Some(assignment_ready.client_id)
    );
    assert_eq!(assignment.roster[0].team.as_deref(), Some("solo"));
    assert_eq!(
        assignment.match_metadata.get("game").map(String::as_str),
        Some("demo")
    );
    assert_eq!(
        assignment.match_metadata.get("version").map(String::as_str),
        Some("dev")
    );
    if matches!(allocation_source(), AllocationSource::EdgegapMock) {
        assert_eq!(
            assignment
                .metadata
                .get("edgegap.deployment_id")
                .map(String::as_str),
            Some("deployment-local-dev")
        );
    }

    if require_assignment_prepare() {
        let prepared = coordinator
            .assignment_prepared(&assignment.assignment_id)
            .await
            .unwrap()
            .expect("assignment prepared acknowledgement should be present");
        assert!(prepared.prepared);
        assert_eq!(prepared.server_id, ServerId::new("local-dev"));
        assert_eq!(prepared.client_id, assignment_ready.client_id);
    }

    if std::env::var("LIGHTYEAR_MATCHMAKER_NATS_SMOKE_EXPECT_ACTIVE").as_deref() == Ok("true") {
        let active_connection = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(connection) = coordinator
                    .active_connection(&ServerId::new("local-dev"), assignment_ready.client_id)
                    .await
                    .unwrap()
                {
                    return connection;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        assert!(active_connection.connected);
        assert_eq!(active_connection.client_id, assignment_ready.client_id);
        assert_eq!(active_connection.player_id.0, "ip:127.0.0.1");
    }

    coordinator
        .publish_report(&GameServerReport::ActiveConnection(ActiveConnection {
            server_id: ServerId::new("local-dev"),
            client_id: assignment_ready.client_id,
            player_id: assignment.player_id.clone(),
            connected: false,
        }))
        .await
        .unwrap();
    wait_assignment_absent_for_client(&coordinator, assignment_ready.client_id).await;
    wait_assignment_absent_for_server_client(
        &coordinator,
        &ServerId::new("local-dev"),
        assignment_ready.client_id,
    )
    .await;

    server.abort();
}

#[tokio::test]
#[ignore = "requires a local NATS server with JetStream enabled and the headless Bevy server"]
async fn websocket_lobby_ready_assigns_roster_to_two_clients() {
    let nats_url = std::env::var("LIGHTYEAR_MATCHMAKER_NATS_SMOKE_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let namespace = std::env::var("LIGHTYEAR_MATCHMAKER_NATS_SMOKE_NAMESPACE")
        .unwrap_or_else(|_| unique_namespace());
    let nats = NatsConfig {
        url: nats_url,
        namespace: Some(namespace),
        ..Default::default()
    };
    let state = AppState::from_config_with_coordination(config(nats.clone()))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let mut owner = connect_with_forwarded_ip(addr, "203.0.113.10").await;
    let mut joiner = connect_with_forwarded_ip(addr, "203.0.113.11").await;

    send_client_message(
        &mut owner,
        ClientMessage::LobbyCreate {
            game: "demo".to_string(),
            version: "dev".to_string(),
            max_players: 2,
            latencies: Vec::new(),
        },
    )
    .await;
    let lobby = recv_lobby_update(&mut owner).await;

    send_client_message(
        &mut joiner,
        ClientMessage::LobbyJoinCode {
            code: lobby.join_code.clone(),
            latencies: Vec::new(),
        },
    )
    .await;
    let _ = recv_lobby_update(&mut owner).await;
    let joined_lobby = recv_lobby_update(&mut joiner).await;
    assert_eq!(joined_lobby.members.len(), 2);

    send_client_message(&mut owner, ClientMessage::LobbySetReady { ready: true }).await;
    let _ = recv_lobby_update(&mut owner).await;
    send_client_message(&mut joiner, ClientMessage::LobbySetReady { ready: true }).await;

    let (owner_grant, joiner_grant) = tokio::join!(
        recv_assignment_ready(&mut owner),
        recv_assignment_ready(&mut joiner)
    );
    assert_ne!(owner_grant.client_id, joiner_grant.client_id);

    let coordinator = NatsCoordinator::connect(nats).await.unwrap();
    let lobby_assignments = assignments_for_server(&coordinator, &ServerId::new("local-dev"))
        .await
        .into_iter()
        .filter(|assignment| assignment.lobby_id == Some(joined_lobby.id.clone()))
        .collect::<Vec<_>>();
    assert_eq!(lobby_assignments.len(), 2);
    let owner_assignment = lobby_assignments
        .iter()
        .find(|assignment| assignment.client_id == owner_grant.client_id)
        .cloned()
        .expect("owner assignment should be server-keyed");
    let joiner_assignment = lobby_assignments
        .iter()
        .find(|assignment| assignment.client_id == joiner_grant.client_id)
        .cloned()
        .expect("joiner assignment should be server-keyed");
    assert_eq!(
        assignment_for_client(&coordinator, owner_grant.client_id)
            .await
            .assignment_id,
        owner_assignment.assignment_id
    );
    assert_eq!(
        assignment_for_client(&coordinator, joiner_grant.client_id)
            .await
            .assignment_id,
        joiner_assignment.assignment_id
    );
    assert_eq!(owner_assignment.lobby_id, Some(joined_lobby.id.clone()));
    assert_eq!(joiner_assignment.lobby_id, Some(joined_lobby.id.clone()));
    assert_eq!(owner_assignment.request_id, joiner_assignment.request_id);
    assert_eq!(
        owner_assignment.allocation_id,
        joiner_assignment.allocation_id
    );
    assert_ne!(
        owner_assignment.assignment_id,
        joiner_assignment.assignment_id
    );
    assert_eq!(owner_assignment.roster.len(), 2);
    assert_eq!(joiner_assignment.roster.len(), 2);
    assert!(owner_assignment.roster.iter().any(|member| {
        member.player_id.0 == "ip:203.0.113.10" && member.team.as_deref() == Some("team-1")
    }));
    assert!(joiner_assignment.roster.iter().any(|member| {
        member.player_id.0 == "ip:203.0.113.11" && member.team.as_deref() == Some("team-2")
    }));

    server.abort();
}

fn unique_namespace() -> String {
    format!(
        "smoke_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

type TestSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_with_forwarded_ip(addr: SocketAddr, ip: &str) -> TestSocket {
    let mut request = format!("ws://{addr}/ws").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-forwarded-for", ip.parse().unwrap());
    tokio_tungstenite::connect_async(request).await.unwrap().0
}

async fn send_client_message(socket: &mut TestSocket, message: ClientMessage) {
    socket
        .send(Message::Text(
            serde_json::to_string(&message).unwrap().into(),
        ))
        .await
        .unwrap();
}

async fn recv_lobby_update(socket: &mut TestSocket) -> Lobby {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let ServerMessage::LobbyUpdated { lobby } = recv_server_message(socket).await {
                return lobby;
            }
        }
    })
    .await
    .unwrap()
}

async fn recv_assignment_ready(socket: &mut TestSocket) -> ConnectionGrant {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut saw_preparing = false;
        loop {
            match recv_server_message(socket).await {
                ServerMessage::AssignmentPreparing { .. } => {
                    saw_preparing = true;
                }
                ServerMessage::AssignmentReady { connect } => {
                    assert!(saw_preparing);
                    return connect;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap()
}

async fn recv_server_message(socket: &mut TestSocket) -> ServerMessage {
    loop {
        let message = socket.next().await.unwrap().unwrap();
        let Message::Text(payload) = message else {
            continue;
        };
        return serde_json::from_str(&payload).unwrap();
    }
}

async fn assignment_for_client(
    coordinator: &NatsCoordinator,
    client_id: LightyearClientId,
) -> AssignmentRecord {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(assignment) = coordinator.assignment_for_client(client_id).await.unwrap() {
                return assignment;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

async fn assignment_for_server_client(
    coordinator: &NatsCoordinator,
    server_id: &ServerId,
    client_id: LightyearClientId,
) -> AssignmentRecord {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(assignment) = coordinator
                .assignments_for_server(server_id)
                .await
                .unwrap()
                .into_iter()
                .find(|assignment| assignment.client_id == client_id)
            {
                return assignment;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

async fn assignments_for_server(
    coordinator: &NatsCoordinator,
    server_id: &ServerId,
) -> Vec<AssignmentRecord> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let assignments = coordinator.assignments_for_server(server_id).await.unwrap();
            if !assignments.is_empty() {
                return assignments;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

async fn wait_assignment_absent_for_client(
    coordinator: &NatsCoordinator,
    client_id: LightyearClientId,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if coordinator
                .assignment_for_client(client_id)
                .await
                .unwrap()
                .is_none()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

async fn wait_assignment_absent_for_server_client(
    coordinator: &NatsCoordinator,
    server_id: &ServerId,
    client_id: LightyearClientId,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let exists = coordinator
                .assignments_for_server(server_id)
                .await
                .unwrap()
                .into_iter()
                .any(|assignment| assignment.client_id == client_id);
            if !exists {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

fn config(nats: NatsConfig) -> MatchmakerConfig {
    MatchmakerConfig {
        server: ServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
        },
        game: GameConfig {
            name: "demo".to_string(),
            version: "dev".to_string(),
        },
        lightyear: NetcodeTokenConfig {
            protocol_id: 0,
            private_key: String::new(),
            client_timeout_secs: 15,
            token_expire_secs: 30,
        },
        identity: IdentityConfig {
            trust_forwarded_for: true,
        },
        nats: Some(nats),
        allocation: AllocationConfig {
            source: allocation_source(),
            require_assignment_prepare: require_assignment_prepare(),
            ..Default::default()
        },
        static_provider: StaticProviderConfig {
            servers: vec![StaticServerConfig {
                id: "local-dev".to_string(),
                game: "demo".to_string(),
                version: "dev".to_string(),
                endpoint: ServerEndpoint {
                    public_ip: "127.0.0.1".parse().unwrap(),
                    port: 7777,
                },
                ready: true,
                total_players: 0,
                max_players: 64,
                max_rooms: 4,
                region: Some("local".to_string()),
                cert_digest: None,
                metadata: BTreeMap::new(),
            }],
        },
        edgegap_provider: EdgegapProviderConfig {
            app: "demo-app".to_string(),
            version: "dev".to_string(),
            deployments: vec![EdgegapDeploymentConfig {
                deployment_id: "deployment-local-dev".to_string(),
                server_id: "local-dev".to_string(),
                game: "demo".to_string(),
                version: "dev".to_string(),
                endpoint: ServerEndpoint {
                    public_ip: "127.0.0.1".parse().unwrap(),
                    port: 7777,
                },
                ready: true,
                current_sessions: 0,
                max_sessions: 64,
                session_id: Some("session-local-dev".to_string()),
                region: Some("local".to_string()),
                cert_digest: None,
                metadata: BTreeMap::new(),
            }],
            ..Default::default()
        },
    }
}

fn allocation_source() -> AllocationSource {
    match std::env::var("LIGHTYEAR_MATCHMAKER_NATS_SMOKE_ALLOCATION_SOURCE").as_deref() {
        Ok("nats_static") => AllocationSource::NatsStatic,
        Ok("edgegap_mock") => AllocationSource::EdgegapMock,
        _ => AllocationSource::ConfiguredStatic,
    }
}

fn require_assignment_prepare() -> bool {
    std::env::var("LIGHTYEAR_MATCHMAKER_NATS_SMOKE_REQUIRE_PREPARE").as_deref() == Ok("true")
}
