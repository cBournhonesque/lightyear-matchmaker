# WebSocket Protocol

This document describes the current JSON WebSocket protocol exposed at `/ws`.
It is an implementation reference for the Phase 1 API, not a final stability
promise.

The current websocket protocol version is `1`.

Messages use an internally tagged JSON shape:

```json
{ "type": "request_play", "game": "lightrider", "version": "dev" }
```

Unknown binary frames are ignored. Invalid JSON produces an `error` response
with code `invalid_json`.

## Machine-Readable Schemas

The repo provides two API description files:

- `docs/asyncapi.yaml`: websocket message contract for `/ws`. This is the right
  artifact for generated websocket clients.
- `docs/openapi.yaml`: HTTP operational endpoints and a pointer to the
  websocket contract.

The deployable server serves these files at `/asyncapi.yaml` and
`/openapi.yaml`.

OpenAPI/Swagger is primarily for request/response HTTP APIs. It can mention a
websocket upgrade route, but it is not a good format for bidirectional websocket
message flows. AsyncAPI is designed for asynchronous/event-driven protocols and
is the better fit for `/ws`.

## Connection Startup

The server currently resolves identity lazily when the first valid client
message is handled. The only identity implementation is IP-derived identity,
optionally using `x-forwarded-for` when configured.

`hello` is optional for v1 clients. If omitted, the server treats the connection
as protocol v1. Clients that want explicit version negotiation should send:

```json
{ "type": "hello", "protocol_version": 1, "client": "web" }
```

The legacy field name `protocol` is still accepted as an alias for
`protocol_version`.

If the version is supported, the server responds:

```json
{
  "type": "hello",
  "protocol_version": 1,
  "min_protocol_version": 1,
  "max_protocol_version": 1,
  "server": "lightyear-matchmaker"
}
```

If the version is unsupported, the server responds with `error` code
`unsupported_protocol_version` and `retryable: false`.

The first non-hello message normally causes an `identity_resolved` response:

```json
{
  "type": "identity_resolved",
  "player": {
    "id": "ip:127.0.0.1",
    "display_name": "127.0.0.1"
  }
}
```

## Request Play

Use `request_play` when a single client wants an immediate assignment.

```json
{
  "type": "request_play",
  "game": "lightrider",
  "version": "dev",
  "room": { "mode": "auto" },
  "latencies": [
    { "region": "us-east", "rtt_ms": 42, "transport": "http" }
  ]
}
```

`room` is optional and defaults to `auto`.

Room selection values:

- `{ "mode": "auto" }`: provider may reuse or create capacity.
- `{ "mode": "new" }`: provider should select capacity that can create a new
  room.
- `{ "mode": "code", "value": "ABCD" }`: join or create a room associated with
  a short code.
- `{ "mode": "id", "value": "room-42" }`: join or create a room associated with
  an internal id.

When assignment preparation is enabled, a successful request first emits:

```json
{ "type": "assignment_preparing", "assignment_id": "request-123:client-456" }
```

Once the game server acknowledges preparation, the server emits:

```json
{
  "type": "assignment_ready",
  "connect": {
    "kind": "lightyear_netcode",
    "client_id": 123456,
    "endpoint": {
      "public_ip": "127.0.0.1",
      "port": 5000
    },
    "token": "...",
    "cert_digest": null
  }
}
```

The returned `client_id` must match the Lightyear Netcode client id accepted by
the game server. The `token` is transport-specific connection material produced
by `lightyear_matchmaker_lightyear`.

## Lobby Messages

The lobby API is intentionally small and in-process today. It is enough for the
local two-client ready-check flow, but not a complete lobby service.

Create a lobby:

```json
{
  "type": "lobby_create",
  "game": "lightrider",
  "version": "dev",
  "max_players": 2,
  "latencies": []
}
```

Join by code:

```json
{ "type": "lobby_join_code", "code": "ABCD", "latencies": [] }
```

Set ready:

```json
{ "type": "lobby_set_ready", "ready": true }
```

Lobby changes are pushed to every connected lobby member:

```json
{
  "type": "lobby_updated",
  "lobby": {
    "id": "lobby-1",
    "join_code": "0001",
    "owner": "ip:127.0.0.1",
    "members": [],
    "game": "lightrider",
    "version": "dev",
    "max_players": 2,
    "ready": false,
    "metadata": {}
  }
}
```

When all required members are ready, the matchmaker assigns the whole lobby as
one roster. Each player receives its own `assignment_ready` message. The game
server receives assignment records with shared lobby/match metadata and distinct
Lightyear client ids.

## Error Messages

Errors use:

```json
{
  "type": "error",
  "code": "no_capacity",
  "message": "no server capacity is available",
  "retryable": true
}
```

Current error codes:

| Code | Meaning |
| --- | --- |
| `invalid_json` | Incoming text frame could not be decoded as a client message. |
| `invalid_request` | The request is structurally valid JSON but invalid for current state. |
| `unsupported_protocol_version` | Client requested a websocket protocol version this server does not support. |
| `no_capacity` | No provider/server capacity is available. |
| `draining` | The matchmaker or selected game server is draining and rejecting new work. |
| `provider_error` | Provider-specific allocation or release failed. |
| `token_error` | Lightyear connection grant generation failed. |
| `config_error` | Runtime configuration is invalid or incomplete. |
| `transport_error` | WebSocket or NATS coordination failed. |

Retry guidance:

- Clients should primarily use the `retryable` boolean on each error frame.
- `invalid_json`, `invalid_request`, `unsupported_protocol_version`,
  `token_error`, and `config_error` are not retryable by default.
- `no_capacity` and `draining` can be retried by the client after a delay,
  preferably against a healthy endpoint if one is available.
- `provider_error` and `transport_error` are retryable by default, but clients
  should use backoff and surface repeated failures to the user.

## Compatibility Notes

Protocol v1 compatibility rules:

- optional fields may be added to existing messages,
- new server message variants may be added,
- existing field meanings should not change within v1,
- clients should ignore unknown server message types they do not understand,
- clients should use `hello` if they need explicit version negotiation.

Before treating v1 as stable, still define:

- client behavior for unknown message types,
- whether `RoomSelection::Id` should create missing rooms or require an
  existing room.
