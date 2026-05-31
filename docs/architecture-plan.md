# Lightyear Matchmaker Architecture Plan

Status: draft.

This repository should become a companion workspace to Lightyear, not a part of
the core Lightyear netcode repo. Its job is to coordinate everything around a
Lightyear game session: lightweight player identity, lobbies, matchmaking
tickets, server selection, server allocation, game-server readiness, and issuing
the Lightyear connection material that lets clients actually connect.

The first version should stay focused: static servers, Edgegap, NATS-backed
ephemeral coordination, a matchmaker WebSocket server, and Bevy client/server
plugins. Other identity systems and providers can be designed for later without
being implemented now.

The core should stay provider-agnostic. Edgegap and static servers are the first
provider implementations. NATS is the first coordination backend. Real account
systems, persistent progression, and additional providers are out of scope for
the first implementation.

## Literature Review

### Open Match

Open Match is the closest architectural reference for the matchmaking side. Its
classic architecture separates the game-facing frontend, ticket storage/querying,
match functions, evaluator, and the director that assigns matched players to
dedicated game servers. The useful lesson is the separation between:

- player-facing request handling,
- matchmaking tickets and rules,
- match proposal/evaluation,
- dedicated game server allocation,
- final assignment delivery to the client.

Open Match's docs explicitly place authentication, player data, lobbies/groups,
dedicated server allocation, and assignment delivery in external game services,
not in the generic matching core. That maps well to this repo: Lightyear
Matchmaker should own those game-service edges while keeping each concern behind
interfaces.

Open Match 2 is also relevant because it makes Open Match smaller rather than
larger. OM2 describes itself as a player data cache with a data-retrieving gRPC
proxy, and its core responsibilities are receiving match requests, managing
tickets, invoking matchmaking functions, and returning matches. It removes the
OM1 evaluator, combines frontend/backend/query into one `om-core` binary, and
communicates over HTTP instead of the older gRPC split.

Lessons to apply here:

- keep one deployable matchmaker server for the MVP,
- keep ticket storage/querying separate from provider allocation,
- make collision handling explicit in the director/matchmaker logic,
- do not bake auth, progression, or server orchestration into matching rules,
- make assignment delivery a first-class state transition,
- start with simple tickets/lobbies before introducing distributed matching
  functions.

Open Match itself should remain an architectural reference for now, not a
dependency.

Sources:

- https://open-match.dev/site/docs/guides/matchmaker/
- https://open-match.dev/site/docs/guides/matchmaker/frontend/
- https://open-match.dev/site/docs/guides/evaluator/
- https://open-match.dev/site/v2/overview/

### Agones

Agones is a server orchestration reference, not a complete game backend. Its
key model is a warm `Fleet` of game servers, allocated by an external
matchmaker through `GameServerAllocation`. This supports the provider boundary:
the matchmaker asks for capacity, but the provider owns the operational details.

Agones also provides useful capacity and autoscaling concepts: ready buffers,
counter/list-based capacity, packed versus distributed scheduling, rolling fleet
updates, and webhook-driven autoscaling. The Lightyear provider trait should be
able to express "reuse an existing ready server" and "allocate or launch new
capacity" without assuming the provider is Kubernetes.

Sources:

- https://agones.dev/site/docs/reference/fleet/
- https://performance.agones.dev/site/docs/integration-patterns/allocation-from-fleet/
- https://agones.dev/site/docs/reference/fleetautoscaler/

Agones is not an implementation target for the first version. The useful lesson
is the allocation contract shape: the matchmaker asks for capacity, the provider
owns orchestration.

### Nakama

Nakama is useful as a product-shape reference: it combines identity, social
systems, matchmaking, parties, lobbies/match listing, leaderboards, storage, and
realtime multiplayer. For this repo, the lesson is not to rebuild all of Nakama
up front. In fact, the first version should deliberately avoid Nakama-like
persistent identity, social, progression, and achievement scope.

Nakama's matchmaker distinguishes active matchmaking tickets from match listing:
matchmaking forms new groups; match listing lets players browse and join
existing matches/lobbies. That distinction should exist in the public API. Its
party matchmaking model also maps to our lobby/party abstraction: one leader can
queue a group while preserving the group assignment.

If Nakama integration is ever added, treat it as a bridge or delegation backend,
not as a default dependency.

Sources:

- https://heroiclabs.com/docs/nakama/concepts/multiplayer/matchmaker/
- https://heroiclabs.com/docs/nakama/concepts/multiplayer/
- https://heroiclabs.com/docs/nakama/concepts/multiplayer/authoritative/
- https://heroiclabs.com/nakama/

### Bevygap

Bevygap already proves the minimum viable Lightyear-specific connection flow:

- a web client connects to an Axum WebSocket endpoint,
- the HTTP service forwards the request over NATS,
- the matchmaker creates or chooses a server/session,
- the matchmaker creates a Lightyear `ConnectToken`,
- the matchmaker stores client/session mappings in NATS KV,
- the game server publishes readiness, cert digest, capacity, and active
  connection state,
- cleanup happens through session TTLs, active connection state, and a delete
  queue.

The main design issue is that Edgegap details are mixed into the core matchmaker.
The new repo should lift the session protocol, NATS state model, server
heartbeats, and Lightyear token issuance from Bevygap, then move Edgegap API
calls behind a provider crate.

Local references:

- `/spare/ssd/cbournhonesque/src/other/bevygap/bevygap_matchmaker`
- `/spare/ssd/cbournhonesque/src/other/bevygap/bevygap_matchmaker_httpd`
- `/spare/ssd/cbournhonesque/src/other/bevygap/bevygap_shared`
- `/spare/ssd/cbournhonesque/src/other/bevygap/bevygap_client_plugin`
- `/spare/ssd/cbournhonesque/src/other/bevygap/bevygap_server_plugin`

### Bevygap Compared To Open Match 2

What Bevygap does well:

- proves the whole Lightyear-specific path end to end,
- keeps the deployable shape simple instead of adopting Open Match 1's larger
  microservice split,
- combines allocation, readiness waiting, token issuance, and assignment return
  in one understandable flow,
- uses NATS for both request/reply and shared game-server coordination,
- has Bevy client/server plugins, which is the right integration surface for
  Lightyear users,
- handles practical Lightyear details like `ConnectToken`, `ClientId` mappings,
  WebTransport certificate digest publication, and connection validation.

What Bevygap does poorly relative to the Open Match 2 lessons:

- Edgegap is part of the core model rather than a provider implementation,
- "session request" is doing the work that should be split into ticket, match
  proposal, allocation, assignment, and connection-grant states,
- collision handling is implicit; there is no explicit "consume this ticket once"
  director step,
- lobby/room intent exists, but the lobby lifecycle is not a first-class model,
- NATS bucket and subject names leak Edgegap/Bevygap terminology,
- server allocation and Lightyear token issuance are too tightly coupled,
- the matchmaker is hard to reuse for static servers without special-case code.

The new repo should keep Bevygap's pragmatic end-to-end path, but reshape it
around Open Match 2's simpler lesson: a small core that manages tickets and
invokes matching/allocation logic, with assignment delivery kept explicit.

### Edgegap

Edgegap's session model is provider-specific but maps naturally to this repo's
allocation model. A session is tied to a deployment, supports dynamic player or
match placement into a running deployment, and can autodeploy when no deployment
has capacity. That should become `lightyear_matchmaker_provider_edgegap`.

Edgegap docs also distinguish deployment readiness from game-server readiness.
The provider can say a deployment is reachable, but the Lightyear server plugin
still needs to publish game-specific readiness, capacity, and WebTransport
certificate material before clients receive a token.

Sources:

- https://docs.edgegap.com/docs/session
- https://docs.edgegap.com/docs/session/how-they-works
- https://docs.edgegap.com/docs/session/session-how-to-manage-request
- https://docs.edgegap.com/docs/deployment/automated-deployment

### GameFlow

GameFlow's best-region docs are useful even though GameFlow is not an initial
provider.
They expose HTTP and UDP regional ping endpoints and recommend client-side
latency measurements, preferring UDP when available and HTTP when UDP is blocked.
For browser clients, HTTP measurements are the practical path.

This should not be hardcoded as "GameFlow region selection" in core. Core should
accept a normalized latency report from the client:

```text
[
  { region: "us-east", rtt_ms: 42, transport: "http" },
  { region: "us-west", rtt_ms: 80, transport: "http" }
]
```

Provider crates can translate canonical region preferences into provider-native
regions. For now, keep the data model compatible with this style of latency
report, but do not implement a GameFlow provider.

Sources:

- https://docs.gameflow.gg/best-region
- https://www.gameflow.gg/

### Identity

Do not focus on account identity yet. The MVP identity implementation should be
a passthrough resolver that derives a stable-enough `PlayerId` from the
connection metadata, initially the client IP address. This is not secure identity
and must not be used for ownership, bans, purchases, or competitive ranking. Its
only purpose is giving the lobby/matchmaking flow a player handle while the
system shape is still being built.

Design the trait narrowly:

- input: remote socket address, trusted forwarded IP headers, optional client
  metadata,
- output: `PlayerId`, display label, and debug metadata,
- first implementation: IP-derived identity,
- future implementations: Discord, Steam, EOS, platform tickets, or game-specific
  account services.

## Product Scope

This repo should eventually provide:

- an HTTP/WebSocket server for clients,
- lightweight session handling,
- a minimal pluggable identity resolver, initially IP passthrough only,
- lobbies, parties, invites, ready checks, team assignment, and lobby metadata,
- matchmaking queues and ticket-based matching,
- server browsing and joining existing rooms,
- region selection from client latency reports,
- provider-agnostic server allocation and release,
- game-server registration, readiness, capacity, cert digest, and connection
  lifecycle reporting,
- Lightyear `ConnectToken` issuance for Netcode transports,
- Bevy client and game-server plugins for the Lightyear-specific integration.

It should not become a general-purpose full game backend on day one. The first
version should focus on the "player wants to play, matchmaker returns a valid
Lightyear connection assignment" path.

Explicitly out of scope for the first version:

- Discord, Steam, EOS, or account linking,
- achievements, inventory, ownership, bans, ranking, progression, and social
  graphs,
- persistent player/lobby/progression storage,
- GameFlow, Agones, Nakama, or other provider implementations beyond static and
  Edgegap.

## Workspace Layout

Proposed initial workspace:

```text
lightyear-matchmaker/
  Cargo.toml
  README.md
  docs/
    architecture-plan.md
    protocol.md
    provider-contract.md
    lobby-model.md
  crates/
    lightyear_matchmaker/
    lightyear_matchmaker_core/
    lightyear_matchmaker_server/
    lightyear_matchmaker_lightyear/
    lightyear_matchmaker_nats/
    lightyear_matchmaker_provider_static/
    lightyear_matchmaker_provider_edgegap/
    lightyear_matchmaker_bevy_client/
    lightyear_matchmaker_bevy_server/
  examples/
    bevy_local_static/
      docker-compose/
      podman/
    bevy_edgegap/
  xtask/
  justfile
```

The crate list is the target shape. Build it incrementally.

### `lightyear_matchmaker`

Facade crate for common users. It should re-export core types and selected
integration crates behind features.

Example features:

- `server`
- `lightyear-netcode`
- `nats`
- `provider-static`
- `provider-edgegap`
- `bevy-client`
- `bevy-server`

### `lightyear_matchmaker_core`

Provider-agnostic domain model and traits. No Axum, no NATS, no Edgegap, no
GameFlow API client, no Bevy plugin code.

Core types:

- `PlayerId`
- `ResolvedIdentity`
- `PlayerSession`
- `LobbyId`
- `Lobby`
- `LobbyMember`
- `Party`
- `Team`
- `ReadyState`
- `MatchTicket`
- `MatchProfile`
- `MatchProposal`
- `Assignment`
- `ServerId`
- `DeploymentId`
- `ServerEndpoint`
- `ServerCapacity`
- `RegionId`
- `LatencyReport`
- `ConnectPayload`
- `ConnectionGrant`

Core traits:

```rust
pub trait IdentityResolver {
    async fn resolve(&self, request: IdentityRequest) -> Result<ResolvedIdentity>;
}

pub trait LobbyStore {
    async fn create_lobby(&self, request: CreateLobby) -> Result<Lobby>;
    async fn join_lobby(&self, request: JoinLobby) -> Result<Lobby>;
    async fn update_member(&self, request: UpdateLobbyMember) -> Result<Lobby>;
}

pub trait Matchmaker {
    async fn enqueue(&self, ticket: MatchTicket) -> Result<TicketId>;
    async fn cancel(&self, ticket: TicketId) -> Result<()>;
    async fn next_matches(&self, profile: MatchProfile) -> Result<Vec<MatchProposal>>;
}

pub trait ServerProvider {
    async fn allocate(&self, request: AllocationRequest) -> Result<ServerAllocation>;
    async fn release(&self, allocation: AllocationId) -> Result<()>;
    async fn list_capacity(&self, request: CapacityQuery) -> Result<Vec<ServerCapacity>>;
}

pub trait TokenIssuer {
    async fn issue(&self, request: TokenRequest) -> Result<ConnectionGrant>;
}
```

### Rust Async Trait Guidance

Rust supports `async fn` in traits on current stable compilers, so core traits
should prefer native async trait methods first. Do not add `async_trait` by
default.

The caveat is dynamic dispatch. Native async trait methods are not dyn-compatible
in the same way ordinary object-safe methods are. If the server needs a runtime
registry like `Vec<Box<dyn ServerProvider>>`, then there are three options:

- use an enum over known providers for the MVP, such as `Provider::Static` and
  `Provider::Edgegap`,
- make the server generic over provider types where possible,
- introduce boxed futures or `async_trait` only at the type-erased boundary.

Preferred initial approach: use native async traits in `core`, and use an enum
or thin adapter in `lightyear_matchmaker_server` for runtime-selected static and
Edgegap providers. That keeps public APIs modern and avoids hidden heap
allocation on every trait call unless we deliberately choose it.

### `lightyear_matchmaker_server`

The deployable service. It wires core services together and exposes:

- WebSocket client API,
- optional REST API for lobbies and server browser,
- provider webhook endpoints,
- game-server registration endpoints if not using NATS,
- health/readiness/metrics endpoints.

This crate should own the high-level state machines:

- client WebSocket session,
- lobby lifecycle,
- queue lifecycle,
- assignment lifecycle,
- cleanup lifecycle.

### `lightyear_matchmaker_lightyear`

Lightyear-specific connection code:

- `ConnectToken` creation for Netcode,
- private key and protocol id configuration,
- `ClientId` assignment strategy,
- token serialization format,
- optional WebTransport certificate digest requirements,
- mapping between `PlayerId`, `ClientId`, `LobbyId`, and server assignment.

Keep this separate from core so the core model can still describe assignments
for non-netcode Lightyear transports or future connection methods.

### `lightyear_matchmaker_nats`

NATS-backed ephemeral state, event bus, and server coordination.

Initial lift from Bevygap:

- namespaced subjects and buckets,
- client-id to assignment/session mappings,
- server readiness and cert digest KV,
- active connection tracking,
- unclaimed allocation/session TTL cleanup,
- delete/release work queue,
- deployment/server capacity heartbeats.

Use neutral names instead of Edgegap-specific names:

```text
assignment_client_to_server
assignment_server_to_clients
server_readiness
server_cert_digests
server_capacity
active_connections
unclaimed_allocations
release_allocation_queue
```

### Provider Crates

Each provider crate implements `ServerProvider`, and optionally provider webhook
or capacity-watch helpers.

Only static and Edgegap providers are implementation targets for the first
version.

#### `lightyear_matchmaker_provider_static`

For local dev, bare metal, LAN servers, and permanently running community
servers.

Responsibilities:

- configured server list,
- health checks,
- capacity selection from server heartbeats,
- no-op release or configurable drain policy.

This should be the first provider because it makes local development and tests
cheap.

#### `lightyear_matchmaker_provider_edgegap`

Edgegap bridge extracted from Bevygap.

Responsibilities:

- create session,
- route to existing deployment when capacity exists,
- use autodeploy when needed,
- poll or consume webhook status,
- map Edgegap deployment/session IDs to core `AllocationId`,
- release sessions/deployments,
- preserve Edgegap-specific metadata in an extension field.

### Deferred Integrations

Do not create crates for these until the static and Edgegap path is solid:

- GameFlow provider,
- Agones provider,
- Nakama bridge,
- Discord/Steam/EOS auth,
- achievements, stats, ownership, entitlements, or progression.

## Public Client API Shape

The WebSocket API should be message-oriented and versioned:

```json
{ "type": "hello", "protocol": 1, "client": "web" }
{ "type": "lobby.create", "visibility": "private", "mode": "duo" }
{ "type": "lobby.join_code", "code": "ABCD" }
{ "type": "lobby.set_ready", "ready": true }
{ "type": "queue.join", "profile": "casual_2v2", "latencies": [] }
{ "type": "assignment.accepted", "assignment_id": "..." }
```

Server responses should be event-like:

```json
{ "type": "identity.resolved", "player": { "id": "ip:203.0.113.10" } }
{ "type": "lobby.updated", "lobby": { "...": "..." } }
{ "type": "queue.progress", "ticket": "...", "estimate_ms": 12000 }
{ "type": "assignment.preparing", "assignment_id": "..." }
{ "type": "assignment.ready", "connect": { "kind": "lightyear_netcode", "...": "..." } }
{ "type": "error", "code": "not_authorized", "message": "..." }
```

Keep transport protocol DTOs in a small crate or in `core::protocol` with strict
semver discipline. The Bevy client plugin and web client will depend on these.

## Game-Server API Shape

Yes: the primary game-server integration should be a Bevy plugin, probably
`lightyear_matchmaker_bevy_server`. The provider-independent contract should
also be available as plain Rust traits/types so non-Bevy servers can implement
it, but the first-class path is a plugin that a Lightyear Bevy server can add.

The Bevy server plugin should:

- register server instance,
- publish readiness,
- publish endpoint and optional cert digest,
- publish capacity and lobby/room metrics,
- receive assignment/roster,
- acknowledge assignment prepared,
- publish player connected/disconnected,
- publish match complete,
- request drain/shutdown.

The plugin should also integrate with Lightyear connection validation. The
matchmaker can decide who should connect, but the game server must validate
connection attempts. For Lightyear Netcode this means the server plugin should
check the `ClientId` against current assignment state and resolve that to
`PlayerId`, lobby, team, and match metadata.

For NATS-backed deployments this can be subjects/KV. For simpler deployments it
can also be exposed as HTTP endpoints in `lightyear_matchmaker_server`.

## Core Flow

### Identity Resolution

1. Client opens a WebSocket.
2. Server derives the remote player identity from connection metadata.
3. The initial implementation uses the client IP as the player identity.
4. The resolved identity is attached to lobby, ticket, and assignment state.
5. No account linking or persistent identity store is required.

### Lobby

1. Player creates or joins a lobby.
2. Lobby owner or rules engine sets mode, teams, map, privacy, and constraints.
3. Players ready up.
4. Lobby transitions to queued.
5. Lobby members become one matchmaking ticket or a group of linked tickets.

### Matchmaking And Assignment

1. Matchmaker creates tickets with player, lobby, party, MMR, region latency,
   mode, version, and rule metadata.
2. Matching logic selects tickets and creates a `MatchProposal`.
3. Director verifies no ticket has already been consumed.
4. Director asks a `ServerProvider` for capacity.
5. Provider returns an allocation endpoint or a pending allocation.
6. Game server publishes readiness and accepts the roster.
7. `TokenIssuer` creates Lightyear connection grants.
8. Client receives `assignment.ready`.
9. Game server validates connecting client ids against assignment state.

### Cleanup

1. If the client never connects, assignment TTL expires.
2. If the game server reports disconnect, active connection state is updated.
3. If all assigned clients leave or match completes, provider release policy runs.
4. Static providers may keep servers alive; dynamic providers may delete sessions
   or deployments.

## Storage And Runtime Split

For the first version, avoid persistent player/lobby/progression storage.
Everything required for the matchmaker flow should be ephemeral:

- active WebSocket sessions,
- lobby state,
- matchmaking tickets,
- assignment state,
- server readiness and capacity,
- active connection state,
- unclaimed allocation/session cleanup.

Lobby state should start in-process or in NATS, depending on what is easiest for
the first local example. Do not add SQL until there is a concrete persistence
requirement.

### NATS vs Redis

Default recommendation: keep NATS as the first coordination backend.

NATS JetStream KV fits the Bevygap-derived shape well because it gives us:

- KV buckets with TTLs,
- watch/watch-all semantics for game-server and matchmaker coordination,
- streams and work-queue semantics for release/delete jobs,
- request/reply for matchmaker service calls,
- one system for messaging plus ephemeral state.

Redis is also viable, especially if an application already runs Redis. It has
excellent basic data structures and Redis Streams can support consumer groups.
However, Redis Pub/Sub is at-most-once and keyspace notifications are fire and
forget, so a Redis backend would need to lean on Streams and explicit polling or
careful keyspace-notification handling for the same coordination semantics.

Keep the core independent enough that a Redis backend can be added later, but do
not implement it before the NATS path is stable.

Sources:

- https://docs.nats.io/nats-concepts/jetstream/key-value-store
- https://docs.nats.io/nats-concepts/jetstream
- https://redis.io/docs/latest/develop/pubsub/
- https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
- https://redis.io/docs/latest/develop/data-types/streams/

## Configuration

Follow the Open Match 2 lesson: one deployable server should be easy to run
locally. Prefer:

- environment variables for production deployment,
- one TOML/YAML file for local examples,
- explicit provider blocks,
- no provider dependency unless enabled by feature/config.

Example:

```toml
[server]
bind = "0.0.0.0:3000"
public_url = "https://matchmaker.example.com"

[lightyear]
protocol_id = 123
private_key_env = "LIGHTYEAR_PRIVATE_KEY"

[state.nats]
url_env = "NATS_URL"
namespace = "mygame-prod"

[[providers]]
name = "static-us-east"
kind = "static"
regions = ["us-east"]

[[providers]]
name = "edgegap-prod"
kind = "edgegap"
app = "mygame-server"
version = "prod"
```

## Examples And Local Deployment

Yes, the repo should contain local examples and config. The first version should
make the full system runnable without an external platform account:

```text
examples/
  bevy_local_static/
    client/
    server/
    config/
      matchmaker.local.toml
      static-servers.toml
    docker-compose/
      compose.local.yml
      .env.example
    podman/
      compose.local.yml
  bevy_edgegap/
    client/
    server/
    config/
      matchmaker.edgegap.example.toml
      edgegap.env.example
    docker-compose/
      compose.edgegap.yml
    podman/
      compose.edgegap.yml
```

Minimum local stack:

- NATS with JetStream enabled,
- matchmaker server,
- one static Bevy game server,
- optional web client/dev server,
- optional NATS monitoring port.

The local compose file should expose and document the useful ports:

- matchmaker HTTP/WebSocket,
- game server Lightyear transport port,
- NATS client port,
- NATS monitoring port,
- any web client/dev-server port.

For Docker and Podman, prefer standard Compose YAML where possible. If Podman
needs different networking or host gateway behavior, keep a small separate file
instead of complicating the common one.

The Edgegap example should mirror the Lightrider/Bevygap workflow:

- local matchmaker and NATS,
- Edgegap provider config through `.env`,
- webhook endpoint route exposed by the matchmaker,
- documented tunnel/port-forward path for webhooks during development,
- static fallback provider for local smoke tests.

Do not require Edgegap credentials to run the default example.

## Developer Commands

Yes, add a `justfile`. It should be the friendly entry point for common workflows,
while `xtask` can hold longer Rust-based automation if needed.

Suggested commands:

```just
check
fmt
clippy
test
doc
run-matchmaker CONFIG="examples/bevy_local_static/config/matchmaker.local.toml"
run-static-server
run-client
compose-up
compose-down
compose-logs
nats-shell
smoke-local
```

Guidance:

- `just` commands should be short wrappers around understandable commands,
- `xtask` is useful later for codegen, release checks, or multi-step migrations,
- Docker/Podman commands should accept an override, for example
  `CONTAINER_TOOL=podman just compose-up`,
- examples should work from a fresh checkout with copied `.env.example` files,
- document any required port-forwarding or tunnel command next to the compose
  file that needs it.

## Implementation Phases

### Phase 0: Planning And Extraction Boundary

- Keep this architecture doc current.
- Inventory Bevygap modules and classify them as core, NATS, Lightyear, Edgegap,
  Bevy client, Bevy server, or deprecated.
- Define the first public DTOs and trait names.

### Phase 1: Minimal Static Provider MVP

- Create workspace and core crates.
- Port Bevygap protocol types with neutral naming.
- Implement NATS state using neutral bucket/subject names.
- Implement IP-derived `IdentityResolver`.
- Implement static provider.
- Implement Lightyear Netcode token issuer.
- Implement minimal Axum WebSocket server:
  resolve player from IP,
  request play,
  choose static server,
  return token.
- Port Bevy client/server plugin enough for a local example.
- Add local config examples, `compose.local.yml`, and basic `just` commands.

This phase should prove the repo without Edgegap.

## Execution Roadmap

The architecture phases above are broad. Implementation should proceed in
small, testable slices. Every slice that changes client assignment, game-server
coordination, provider behavior, or Lightyear integration must be validated
against the headless Bevy local static server before it is considered done.

The current baseline proves:

- websocket `request_play`,
- IP-derived identity,
- static server selection from config,
- Lightyear Netcode token generation,
- assignment persistence in NATS,
- headless Bevy server readiness/capacity publishing,
- headless Bevy server assignment polling,
- provider-independent connection validation that accepts assigned
  `LightyearClientId`s and rejects unknown ones,
- NATS-backed static allocation from live `server_capacity` reports,
- assignment preparation acknowledgements where the matchmaker writes an
  assignment, the game server observes/prepares it, and the client receives the
  ready token only after preparation,
- active-connection reporting after assignment receipt,
- roster/team/match metadata on assignments and Bevy validation context,
- a minimal Bevy client helper/plugin surface for websocket request-play,
- a mock Edgegap provider bridge with Edgegap IDs preserved in provider
  metadata,
- in-process lobby create/join-code/ready-check flow with shared roster
  assignment,
- region latency hints for configured static, NATS static, and mock Edgegap
  allocation,
- token timeout/expiry configuration and basic assignment lifecycle logs.

Next implementation slices:

1. Phase 1 completion harness

   - Keep `just smoke-full-local` green.
   - The command must start NATS, run the headless Bevy game server, send a
     websocket play request, verify assignment storage, and verify the game
     server publishes an active connection.
   - Use this as the regression test for every new feature until a broader
     integration test suite exists.

2. Lightyear connection-validation contract

   - Status: provider-independent validation API implemented in the Bevy server
     crate; real Lightyear adapter still pending.
   - Add a provider-independent validation API to the Bevy server crate that
     answers whether a `LightyearClientId` is allowed, and returns the
     assignment metadata needed by the game.
   - Keep it independent from real Lightyear server internals first, then add
     the adapter where Lightyear exposes the connection-validation hook.
   - Headless Bevy test: assignment arrives from NATS, validation accepts that
     client id and rejects an unknown client id.

3. Static provider from live capacity

   - Status: implemented for static servers using `allocation.source =
     "nats_static"`; deeper reservation/idempotency policy remains pending.
   - Add a NATS-backed static provider mode that chooses among live
     `server_capacity` reports instead of only configured static entries.
   - Keep the current configured static provider for no-NATS local development.
   - Headless Bevy test: the matchmaker allocates only after the game server has
     published readiness/capacity, and stops allocating when capacity is full.

4. Assignment acknowledgement and prepared state

   - Status: initial prepared acknowledgement implemented for NATS-backed
     assignments; richer rejection/retry semantics remain pending.
   - Add an assignment lifecycle: created by matchmaker, observed by game
     server, acknowledged/prepared by game server, then returned to the client.
   - Avoid returning a token for a server that has not observed the assignment
     unless the provider explicitly supports fire-and-forget assignments.
   - Headless Bevy test: websocket receives `assignment.preparing`, game server
     acknowledges, websocket receives `assignment.ready`.

5. Roster and match metadata delivery

   - Status: implemented for single-player request-play and lobby assignments.
   - Extend assignments to include lobby id, roster, team, and match metadata.
   - Expose this through the Bevy server state so game code can map
     `LightyearClientId` to player/lobby/team context.
   - Headless Bevy test: assigned clients can be validated with roster/team
     metadata available.

6. Bevy client plugin

   - Status: minimal helper/plugin crate implemented as
     `lightyear_matchmaker_bevy_client`.
   - Add a minimal client plugin or helper that opens the matchmaker WebSocket,
     sends `request_play`, receives `ConnectionGrant`, and exposes the token to
     game code.
   - Headless/local test: use the client helper against the local matchmaker
     and headless server path.

7. Edgegap provider bridge

   - Status: mock provider bridge implemented; real Edgegap API extraction is
     still Phase 2.
   - Extract the Bevygap Edgegap integration into
     `lightyear_matchmaker_provider_edgegap`.
   - Keep Edgegap IDs in provider metadata, not core assignment fields.
   - Headless/local test: use a mock Edgegap provider first, then document the
     real Edgegap smoke path separately because it requires external services.

8. Lobby and ready-check MVP

   - Status: in-process create, join-code, ready checks, and basic team
     assignment implemented; leave and owner transfer remain pending.
   - Add in-process lobby state first, then NATS-backed lobby state if multiple
     matchmaker instances need to share lobbies.
   - Implement create, join, leave, ready, owner transfer, and basic team
     assignment.
   - Headless Bevy test: two websocket clients join a lobby, both ready, a
     single assignment roster reaches the game server.

9. Region hints

   - Status: implemented for request-play and lobby allocation requests using
     existing `LatencyReport` DTOs.
   - Use existing `LatencyReport` DTOs to choose a static or Edgegap region.
   - Document a web-client path for collecting GameFlow-style best-region
     measurements without coupling core to GameFlow.
   - Headless Bevy test: multiple published static servers with different
     regions result in the expected capacity choice.

10. Hardening

   - Status: initial structured assignment lifecycle logs, duplicate
     assignment-id idempotency in the Bevy server, and Netcode token
     timeout/expiry config implemented. Metrics, retries, drain controls, and
     compatibility matrix remain pending.
   - Add metrics, structured assignment lifecycle logs, retry/idempotency
     policy, token expiry policy, drain controls, and compatibility tests
     against supported Lightyear versions.
   - Headless Bevy tests remain the first regression layer; add load and
     provider-specific tests where they are meaningful.

### Phase 2: Edgegap Bridge

- Move Bevygap Edgegap session creation/polling/deletion into
  `lightyear_matchmaker_provider_edgegap`.
- Preserve current mock Edgegap behavior as tests or examples.
- Keep Edgegap-specific IDs in provider metadata, not core model fields.
- Support deployment/session reuse based on server capacity heartbeats.
- Add Edgegap example config, compose workflow, and documented webhook
  tunnel/port-forward path.

### Phase 3: Lobby And Ready Checks

- Add lobby model and state transitions.
- Add join codes, private/public lobbies, owner transfer, ready checks, and team
  assignment.
- Make matchmaking consume lobby tickets rather than only individual players.
- Add assignment roster delivery to game server.

### Phase 4: Region Hints

- Add latency report DTOs.
- Add a generic client latency-report path.
- Optionally document how a web client could collect HTTP best-region samples.
- Keep provider implementations limited to static and Edgegap.

### Phase 5: Production Hardening

- Metrics, tracing, structured logs.
- Load tests for WebSocket sessions and ticket queues.
- Provider failure policy and retries.
- Idempotent assignment creation.
- Token expiry and replay protections.
- Admin/drain controls.
- Compatibility test matrix against Lightyear versions.

## Naming Guidance

Keep the repo and crates explicit:

- repo: `lightyear-matchmaker`
- facade crate: `lightyear_matchmaker`
- core crate: `lightyear_matchmaker_core`
- provider crates: `lightyear_matchmaker_provider_*`

Avoid making Edgegap or any other provider part of the project name. Providers
are bridges, not the system identity.

## Open Questions

- Should the first lobby service be in-process only, or should it use NATS from
  day one so multiple matchmaker instances can share lobby state?
- Should `lightyear_matchmaker_core` depend on `lightyear` types, or should
  `lightyear_matchmaker_lightyear` be the only crate that imports `lightyear`?
  Current recommendation: keep `lightyear` out of core.
- Should the public client API be WebSocket-only initially, or WebSocket plus
  REST for lobbies?
- How much should the first Bevy server plugin own versus exposing lower-level
  systems/events for games to wire themselves?
- Should Open Match itself be an optional integration later, or only an
  architectural reference?
- What exact NATS bucket/subject names should replace the Bevygap names while
  preserving easy migration?
- Should runtime provider selection use an enum over built-in providers, or a
  boxed `dyn ServerProvider` adapter once third-party providers become important?
