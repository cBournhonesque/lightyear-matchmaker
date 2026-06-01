//! In-process lobby runtime used by the MVP matchmaker server.

use lightyear_matchmaker_core::{
    LatencyReport, Lobby, LobbyId, LobbyMember, MatchmakerError, PlayerId, ResolvedIdentity,
    Result, ServerMessage,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc as tokio_mpsc;

pub(super) type OutboundMessages = tokio_mpsc::UnboundedSender<ServerMessage>;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(super) struct LobbyMetrics {
    pub(super) lobbies_total: u64,
    pub(super) websocket_sessions_registered: u64,
    pub(super) assigning_lobbies: u64,
}

#[derive(Clone, Default)]
pub(super) struct LobbyRuntime {
    inner: Arc<Mutex<LobbyRuntimeState>>,
}

#[derive(Default)]
struct LobbyRuntimeState {
    next_lobby_id: u64,
    next_join_code: u64,
    lobbies: BTreeMap<LobbyId, Lobby>,
    code_to_lobby: BTreeMap<String, LobbyId>,
    sessions: BTreeMap<PlayerId, OutboundMessages>,
    assigning_lobbies: BTreeSet<LobbyId>,
}

impl LobbyRuntime {
    pub(super) fn register_session(&self, player_id: PlayerId, sender: OutboundMessages) {
        self.with_state(|state| {
            state.sessions.insert(player_id, sender);
        });
    }

    pub(super) fn unregister_session(&self, player_id: &PlayerId) {
        self.with_state(|state| {
            state.sessions.remove(player_id);
        });
    }

    pub(super) fn create_lobby(
        &self,
        owner: &ResolvedIdentity,
        game: String,
        version: String,
        max_players: u32,
        latencies: Vec<LatencyReport>,
    ) -> Lobby {
        self.with_state(|state| {
            state.next_lobby_id = state.next_lobby_id.saturating_add(1);
            state.next_join_code = state.next_join_code.saturating_add(1);
            let id = LobbyId::new(format!("lobby-{}", state.next_lobby_id));
            let join_code = format!("{:04X}", state.next_join_code);
            let lobby = Lobby {
                id: id.clone(),
                join_code: join_code.clone(),
                owner: owner.player_id.clone(),
                members: vec![LobbyMember {
                    player_id: owner.player_id.clone(),
                    display_name: owner.display_name.clone(),
                    team: Some("team-1".to_string()),
                    ready: false,
                    latencies,
                    metadata: BTreeMap::new(),
                }],
                game,
                version,
                max_players: max_players.max(1),
                ready: false,
                metadata: BTreeMap::new(),
            };
            state.code_to_lobby.insert(join_code, id.clone());
            state.lobbies.insert(id, lobby.clone());
            lobby
        })
    }

    pub(super) fn join_lobby_by_code(
        &self,
        code: &str,
        player: &ResolvedIdentity,
        latencies: Vec<LatencyReport>,
    ) -> Result<Lobby> {
        self.with_state(|state| {
            let code = normalize_join_code(code);
            let Some(lobby_id) = state.code_to_lobby.get(&code).cloned() else {
                return Err(MatchmakerError::InvalidRequest(format!(
                    "unknown lobby join code {code}"
                )));
            };
            let Some(lobby) = state.lobbies.get_mut(&lobby_id) else {
                return Err(MatchmakerError::InvalidRequest(format!(
                    "lobby {lobby_id} no longer exists"
                )));
            };
            if !lobby
                .members
                .iter()
                .any(|member| member.player_id == player.player_id)
            {
                if lobby.members.len() >= lobby.max_players as usize {
                    return Err(MatchmakerError::InvalidRequest(format!(
                        "lobby {lobby_id} is full"
                    )));
                }
                let team = format!("team-{}", (lobby.members.len() % 2) + 1);
                lobby.members.push(LobbyMember {
                    player_id: player.player_id.clone(),
                    display_name: player.display_name.clone(),
                    team: Some(team),
                    ready: false,
                    latencies,
                    metadata: BTreeMap::new(),
                });
            }
            Ok(lobby.clone())
        })
    }

    pub(super) fn set_ready(
        &self,
        lobby_id: &LobbyId,
        player_id: &PlayerId,
        ready: bool,
    ) -> Result<(Lobby, bool)> {
        self.with_state(|state| {
            let Some(lobby) = state.lobbies.get_mut(lobby_id) else {
                return Err(MatchmakerError::InvalidRequest(format!(
                    "lobby {lobby_id} no longer exists"
                )));
            };
            let Some(member) = lobby
                .members
                .iter_mut()
                .find(|member| &member.player_id == player_id)
            else {
                return Err(MatchmakerError::InvalidRequest(format!(
                    "player {player_id} is not in lobby {lobby_id}"
                )));
            };
            member.ready = ready;
            lobby.ready = lobby.members.len() >= lobby.max_players as usize
                && lobby.members.iter().all(|member| member.ready);
            let should_assign = lobby.ready && state.assigning_lobbies.insert(lobby_id.clone());
            Ok((lobby.clone(), should_assign))
        })
    }

    pub(super) fn send_to_player(&self, player_id: &PlayerId, message: ServerMessage) {
        let sender = self.with_state(|state| state.sessions.get(player_id).cloned());
        if let Some(sender) = sender {
            let _ = sender.send(message);
        }
    }

    pub(super) fn notify_lobby(&self, lobby: &Lobby) {
        for member in &lobby.members {
            self.send_to_player(
                &member.player_id,
                ServerMessage::LobbyUpdated {
                    lobby: lobby.clone(),
                },
            );
        }
    }

    pub(super) fn metrics(&self) -> LobbyMetrics {
        self.with_state(|state| LobbyMetrics {
            lobbies_total: state.lobbies.len() as u64,
            websocket_sessions_registered: state.sessions.len() as u64,
            assigning_lobbies: state.assigning_lobbies.len() as u64,
        })
    }

    fn with_state<T>(&self, f: impl FnOnce(&mut LobbyRuntimeState) -> T) -> T {
        let mut state = match self.inner.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        f(&mut state)
    }
}

fn normalize_join_code(code: &str) -> String {
    code.trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase()
}
