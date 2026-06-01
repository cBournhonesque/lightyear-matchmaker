# Bevy Client Plugin

`lightyear_matchmaker_bevy_client` provides a Bevy-facing wrapper around the
matchmaker websocket protocol.

There are two client surfaces:

- `request_play_once`: one-shot helper that opens a websocket, sends
  `request_play`, waits for `assignment_ready`, and returns the connection
  grant.
- `LightyearMatchmakerClientPlugin`: persistent websocket session for app UI and
  lobby flows.

## Plugin State

The plugin inserts `MatchmakerClientState`.

Important fields:

- `connected`: whether the websocket transport is connected.
- `protocol`: server protocol details after `hello`.
- `player`: latest resolved player identity.
- `lobby`: latest lobby snapshot.
- `assignment_id`: assignment currently being prepared.
- `grant`: latest connection grant.
- `grant_state`: `idle`, `preparing`, `ready`, or `failed`.
- `last_error`: last structured server or transport error.

## Outbound Messages

Game/UI code sends Bevy messages:

- `RequestPlay`
- `CreateLobby`
- `JoinLobbyCode`
- `SetLobbyReady`
- `SendMatchmakerMessage` for raw protocol messages

The plugin forwards those messages over the persistent websocket session.

## Inbound Messages

The plugin emits Bevy messages:

- `MatchmakerClientConnected`
- `MatchmakerProtocolReady`
- `MatchmakerClientDisconnected`
- `MatchmakerIdentityResolved`
- `MatchmakerLobbyUpdated`
- `MatchmakerAssignmentPreparing`
- `MatchmakerQueueProgress`
- `ConnectionGrantReady`
- `MatchmakerClientFailed`

`MatchmakerClientFailed` keeps a plain `message` string for simple UI handling
and also carries structured `MatchmakerClientErrorInfo` with `code` and
`retryable`.

## Reconnect Behavior

Native builds start one background websocket session task. It sends `hello`,
forwards outbound Bevy messages, emits inbound Bevy messages, and reconnects
after transport failures when `MatchmakerClientConfig::reconnect` is true.

Commands queued while the session is disconnected remain in the local command
channel until the session task can send them. Commands already sent to a
connection that later fails may need to be retried by game/UI code based on
`MatchmakerClientFailed` and the server's `retryable` flag.

Persistent websocket sessions are not implemented for wasm yet. The one-shot
`request_play_once` helper remains available for wasm.

## Minimal Setup

```rust
use lightyear_matchmaker_bevy_client::{
    LightyearMatchmakerClientPlugin, MatchmakerClientConfig,
};

app.add_plugins(LightyearMatchmakerClientPlugin::new(
    MatchmakerClientConfig::new("ws://127.0.0.1:3000/ws"),
));
```

To request play:

```rust
use bevy_ecs::prelude::MessageWriter;
use lightyear_matchmaker_bevy_client::RequestPlay;

fn request_play(mut requests: MessageWriter<RequestPlay>) {
    requests.write(RequestPlay::new("demo", "dev"));
}
```

To create a lobby and ready up:

```rust
use bevy_ecs::prelude::MessageWriter;
use lightyear_matchmaker_bevy_client::{CreateLobby, SetLobbyReady};

fn create_lobby(mut create: MessageWriter<CreateLobby>) {
    create.write(CreateLobby::new("demo", "dev", 2));
}

fn ready(mut ready: MessageWriter<SetLobbyReady>) {
    ready.write(SetLobbyReady { ready: true });
}
```
