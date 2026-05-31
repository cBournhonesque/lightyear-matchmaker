# Lightyear Matchmaker

Planning repo for a Lightyear-oriented matchmaking, lobby, identity, and dedicated
server assignment layer.

The initial architecture draft lives in
[docs/architecture-plan.md](docs/architecture-plan.md).

## Phase 1 Slice

The current workspace contains the first compileable Phase 1 slice:

- `lightyear_matchmaker_core`: provider-agnostic types and traits.
- `lightyear_matchmaker_nats`: NATS JetStream KV coordination helpers.
- `lightyear_matchmaker_provider_static`: static server allocation.
- `lightyear_matchmaker_provider_edgegap`: real Edgegap session provider plus a
  mock Edgegap-shaped provider for testing the provider boundary.
- `lightyear_matchmaker_lightyear`: Lightyear Netcode `ConnectToken` issuing.
- `lightyear_matchmaker_server`: minimal Axum WebSocket server with optional
  NATS assignment publishing.
- `lightyear_matchmaker_bevy_client`: minimal Bevy-friendly websocket
  request-play helper/plugin surface.
- `lightyear_matchmaker_bevy_server`: Bevy game-server contract for readiness,
  capacity, assignments, client connection reports, and an optional NATS bridge.
- `bevy_local_static_server`: headless local Bevy game-server example under
  `examples/bevy_local_static/server`.

Run locally with:

```sh
cargo run -p lightyear_matchmaker_server -- --config examples/bevy_local_static/config/matchmaker.local.toml
```

The WebSocket endpoint is `/ws`, and the health endpoint is `/health`.

To exercise the NATS path locally, start NATS with JetStream enabled and use:

```sh
just run-matchmaker-nats
just run-game-server
```

The local Bevy server is headless. It binds the configured UDP game port,
publishes readiness/capacity through the matchmaker Bevy plugin, receives NATS
assignments, and auto-simulates assigned clients connecting so the full local
coordination path can be tested before real Lightyear server validation is
wired in.

The NATS local and compose matchmaker configs use `allocation.source =
"nats_static"`, so static allocation comes from the game server's live
`server_capacity` reports. The no-NATS config still uses the configured static
server list directly. Configured static, NATS static, and the Edgegap providers
honor client `LatencyReport` region hints.

Those NATS configs also require assignment preparation. The matchmaker writes
the assignment to NATS, sends `AssignmentPreparing`, waits for the headless Bevy
server to publish an `AssignmentPrepared` acknowledgement, and only then sends
`AssignmentReady` with the Lightyear connect token.

The compose examples live under `examples/bevy_local_static/docker-compose/`
and `examples/bevy_local_static/podman/`. They run NATS plus the matchmaker
using `examples/bevy_local_static/config/matchmaker.compose.toml`, plus the
local static game server using
`examples/bevy_local_static/config/game-server.compose.toml`.

The websocket-to-NATS smoke test is ignored by default because it needs NATS:

```sh
NATS_SMOKE_URL=nats://127.0.0.1:4222 just smoke-nats
```

With `just run-game-server` running in another terminal, this also waits for the
game server to publish an active-connection report:

```sh
NATS_SMOKE_NAMESPACE=lightyear_local just smoke-nats-active
```

For the full local regression path in one command:

```sh
just smoke-full-local
```

That command starts temporary NATS with JetStream, runs the headless Bevy game
server, sends a websocket play request, checks NATS assignment storage, checks
the game server's active-connection report, and runs a two-client lobby
join-code/ready-check assignment smoke.

For the mock Edgegap provider boundary, run:

```sh
just smoke-full-edgegap-mock
```

For real Edgegap session creation, start from
`examples/bevy_local_static/config/matchmaker.edgegap.local.example.toml`, set
`EDGEGAP_API_KEY`, and run the server with `allocation.source = "edgegap"`.
That path uses Edgegap's session API and is covered by local HTTP tests, but it
is not yet covered by a live Edgegap smoke test.
