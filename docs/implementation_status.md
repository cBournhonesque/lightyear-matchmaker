# Implementation Status

Status as of the 2026-06-01 Phase 1/MVP implementation.

This repo has a solid local vertical slice, but it does not implement the full
architecture plan yet. The current state is best described as a working Phase 1
foundation plus initial versions of the later roadmap slices.

For the current runtime behavior and terminology, see
[runtime-model.md](runtime-model.md). That document is the best starting point
for reviewing assignment ids, server-keyed NATS state, lifecycle jobs, retry
behavior, and room selection.

## Implemented

- WebSocket `request_play` flow.
- Versioned websocket protocol with optional `hello` negotiation.
- Structured websocket error codes with client-facing `retryable` metadata.
- Static OpenAPI and AsyncAPI descriptions, served at `/openapi.yaml` and
  `/asyncapi.yaml`.
- IP-derived identity resolver.
- Configured static provider.
- NATS-backed static provider using live `server_capacity` reports.
- Lightyear Netcode `ConnectToken` generation.
- Configurable Netcode client timeout and token expiry.
- NATS assignment persistence.
- Server-keyed NATS assignment storage, with a lightweight client-to-assignment
  index for lookup and disconnect cleanup.
- NATS KV TTLs configured under `[nats.ttl]`.
- NATS lifecycle work queue messages for `release_allocation` and
  `delete_assignment`.
- Runtime lifecycle worker that consumes NATS release/delete work and calls
  provider release APIs.
- Operational `/ready`, `/metrics`, and drain admin endpoints.
- Matchmaker-wide drain rejects new websocket upgrades and new non-hello
  websocket work on existing sessions.
- Per-game-server drain markers suppress future allocation, cancel pending
  NATS assignments for that server, and queue lifecycle cleanup for canceled
  assignments/provider allocations.
- Runtime metrics for websocket sessions, assignments, lobbies, retries, and
  lifecycle worker outcomes.
- Headless Bevy game-server example.
- Bevy game-server readiness and capacity publishing.
- Bevy game-server assignment polling.
- Assignment preparation acknowledgement before returning `assignment.ready`.
- `AssignmentPrepared` consumes the pending server/client assignment queues;
  active connections become the runtime connected-client state.
- Prepared-but-unconnected assignments expire after
  `[allocation].assignment_timeout_secs` on the matchmaker side and
  `demo.assignment_timeout_secs` in the Bevy example, both defaulting to 60
  seconds.
- If a prepared assignment times out because the selected server disappeared
  from NATS capacity before the client connected, the matchmaker can issue a
  replacement assignment to a still-open WebSocket.
- Configurable assignment preparation retries with a default of one retry after
  the initial allocation/assignment attempt.
- Optional assignment retry backoff via `[allocation].assignment_retry_backoff_ms`.
- Retry attempts carry failed server ids so providers can avoid assigning the
  same server again.
- Active-connection reporting after assignment receipt.
- Assignment roster, team, lobby id, and match metadata.
- Provider-independent Bevy connection validation state.
- Bevy validation context that maps `LightyearClientId` to player/lobby/team
  metadata.
- Optional real Lightyear Netcode connection request handler in
  `lightyear_matchmaker_bevy_server` behind the `lightyear-netcode` feature.
- Lightyear Netcode `Connected`/`Disconnected` observation for active-connection
  reports when that feature is enabled.
- Bevy client one-shot `request_play` helper plus persistent native plugin
  session for request-play, lobby create/join, ready checks, inbound websocket
  events, connection-grant state, structured errors, and reconnect reporting.
- Mock Edgegap provider bridge with Edgegap IDs stored in provider metadata.
- Real Edgegap session provider with create, poll, endpoint extraction, and
  release.
- Real Edgegap example config template:
  `examples/bevy_local_static/config/matchmaker.edgegap.local.example.toml`.
- In-process lobby create, join-code, ready-check, and basic team assignment.
- Two-client lobby smoke path that delivers one shared roster to the game
  server.
- Region latency hints for configured static, NATS static, mock Edgegap, real
  Edgegap, and lobby allocation.
- Explicit requested-room routing for NATS static capacity: if the requested
  room already exists, route to that room; if it does not exist yet, route to a
  server with room capacity so the game server can create it.
- Basic assignment lifecycle logs.
- Duplicate assignment-id idempotency in the Bevy server.
- Local example layout scoped under `examples/bevy_local_static/`, including
  config, smoke scripts, Docker Compose, and Podman Compose files.
- Example config parse coverage for local, NATS, mock Edgegap, real Edgegap
  template, and compose matchmaker configs.
- Public module/type/function docs plus targeted private comments around
  non-obvious lifecycle and provider behavior.
- Initial module split for readability: server config, lobby runtime,
  metrics/status DTOs, NATS config, NATS names, and NATS lifecycle work
  payloads now live outside the largest orchestration files.
- Formal core lifecycle state enums for lobbies, allocations, assignments, and
  lifecycle work.
- Assignment and lifecycle cleanup state labels are now used in runtime
  transition logs and JSON metrics snapshots.
- State-machine and Open Match 2 integration docs:
  `docs/state-machines.md` and `docs/open-match2-integration.md`.
- Downstream static-provider integration validated with Lightrider on a Linode
  host: browser client, Caddy HTTPS, matchmaker, NATS coordination, and a static
  Bevy/Lightyear game server all reached the connection flow successfully.
- Local smoke harnesses:
  - `just smoke-full-local`
  - `just smoke-full-edgegap-mock`

## Not Fully Implemented

- End-to-end real Lightyear transport example. The connection request handler
  exists, but the local headless Bevy example still simulates accepted clients
  instead of running a real Lightyear server/client transport loop.
- Complete Edgegap production behavior. The real provider exists, but webhook
  readiness, capacity/reuse policy, and live API smoke coverage are still
  missing.
- Full lobby lifecycle. Create, join-code, ready checks, and basic team
  assignment exist, but leave, owner transfer, reconnect handling, privacy,
  invites, cleanup, and distributed lobby state are missing.
- NATS-backed lobby state for multiple matchmaker instances.
- Allocation reservation semantics. Static/NATS allocation currently selects
  capacity, but there is no durable reservation step. This is intentionally
  deferred while the target deployment has one client-facing matchmaker.
- Matchmaker-owned reservation accounting. Capacity reports exist, but there is
  still no separate CAS-protected reservation ledger that accounts for pending
  assignments across multiple matchmaker instances. This is not part of the
  current MVP unless multi-matchmaker deployment becomes a requirement.
- Full metrics export format for Prometheus/OpenTelemetry. The current endpoint
  is JSON.
- Open Match 2 integration. The integration model is documented, but no adapter
  crate or queue matchmaking path exists yet.
- Graceful process-signal shutdown orchestration. Manual matchmaker/server drain
  controls exist, but shutdown signals do not yet drive a full drain/wait/exit
  sequence.
- Load tests.
- Live Edgegap smoke tests guarded by credentials. This is intentionally treated
  as a deployment/integration check, not the main development loop.
- Compatibility matrix against supported Lightyear versions.
- Full Bevy client workflow plugin for browser/wasm. The native persistent
  session exists, but wasm still only has the one-shot helper.
- Deployed smoke coverage for create-on-demand requested rooms. Unit coverage
  exists for the NATS static selector, but the `ABCD` flow should still be
  rebuilt, deployed, and verified when we next run Lightrider.

## Production Readiness Blockers

This repo is not production ready yet. Treat it as an MVP/alpha foundation until
these are addressed:

- Admin endpoints, including drain controls, have no authentication or
  authorization.
- Identity is still IP-derived passthrough only; Discord/Steam/EOS or another
  trusted identity layer is not implemented.
- Graceful process shutdown is not wired to OS signals. The matchmaker can be
  put into drain manually, but it does not yet automatically drain, cancel, wait
  for in-flight work, and exit.
- The runtime still assumes one matchmaker for allocation correctness. There is
  no CAS-backed reservation ledger for active/pending capacity across multiple
  matchmaker instances.
- Metrics are JSON only. There is no Prometheus/OpenTelemetry export, alerting
  policy, or operational dashboard.
- Lifecycle work has dead-letter-style counters/logs, but no separate
  inspectable dead-letter stream or operator endpoint.
- Real Edgegap behavior needs live credential-gated smoke coverage, capacity
  reuse policy review, and webhook/readiness hardening.
- The Bevy/browser client path lacks a persistent wasm websocket lifecycle.
- Real Lightyear server/client end-to-end coverage is still incomplete in the
  local headless example.
- There is no documented compatibility matrix for supported Lightyear versions.

Primary cleanup blocker before stabilizing public APIs: split large modules and
keep module boundaries aligned with runtime concepts. The first split extracted
server config, lobby runtime, metrics/status DTOs, NATS config, NATS names, and
NATS lifecycle work payloads from the largest files; assignment orchestration
and HTTP/WebSocket handlers should be split next.

## Priority Review Items

### 1. Assignment ID And Idempotency Semantics

Status: addressed in the current implementation, but still worth reviewing for
API clarity.

The model now distinguishes three ids:

- `request_id`: matchmaker-generated id for one client or lobby request.
- `allocation_id`: provider-owned id for the selected capacity/session.
- `assignment_id`: matchmaker-owned id for one assigned client.

The original issue was that solo `request_play` derived `assignment_id` from the
provider `allocation_id`. Some provider allocation ids are deterministic by
server/player, which meant repeated play requests from the same player could
collide.

This interacts with Bevy-side duplicate assignment-id idempotency: duplicate
assignment ids are ignored by the game-server state.

The server now creates a fresh `request_id` per request and derives assignment
ids from `request_id + client_id`, while keeping allocation ids provider-specific.
For lobby assignment, all players share the same request id and allocation id,
but each player has a distinct assignment id.

Relevant files:

- `crates/lightyear_matchmaker_core/src/provider.rs`
- `crates/lightyear_matchmaker_core/src/game_server.rs`
- `crates/lightyear_matchmaker_server/src/lib.rs`
- `crates/lightyear_matchmaker_bevy_server/src/lib.rs`

### 2. Server And Lobby Runtime Shape

The in-process lobby runtime is intentionally small. It is good enough for the
smoke path, but it is not a complete lobby service.

Current limitations:

- predictable local join codes,
- no leave,
- no owner transfer,
- no lobby cleanup,
- no reconnect/session recovery,
- no privacy or invite model,
- no distributed state across matchmaker instances.

Relevant file:

- `crates/lightyear_matchmaker_server/src/lib.rs`

### 3. Room Selection Semantics

Status: implemented for NATS static capacity selection; deployed smoke pending.

The current NATS static live-capacity selector has these semantics:

- `RoomSelection::Auto`: any ready server with player capacity can be selected.
- `RoomSelection::New`: a ready server must still have room capacity.
- `RoomSelection::Code` or `RoomSelection::Id`: if any eligible server already
  advertises the requested room key, the selected server must be the server that
  hosts that room and the room must have player capacity. If no eligible server
  advertises the requested room key yet, the selector may choose any eligible
  server with room capacity so the game server can create that room.

This supports Lightrider-style private codes where the first player creates the
private room, while still avoiding duplicate requested rooms once a server has
advertised that room key. The game-specific server remains responsible for
creating the room during assignment preparation or on first client join.

Review note: `Code` and `Id` currently share the same create-if-missing behavior.
If internal ids should mean "existing only", split those semantics before
publishing this API as stable. The configured static provider only has
server-level capacity and therefore already treats requested rooms as work for
the selected game server to create.

Relevant files:

- `crates/lightyear_matchmaker_nats/src/lib.rs`
- `crates/lightyear_matchmaker_core/src/provider.rs`
- `crates/lightyear_matchmaker_server/src/lib.rs`

### 4. Assignment Lifecycle And NATS Model

The NATS coordination model works for local smoke tests, but it is not yet a
production assignment store.

Implemented:

- NATS KV TTLs are configurable under `[nats.ttl]`, with defaults matching the
  original hard-coded values.
- Assignment records and prepared acknowledgements can be deleted explicitly.
- Assignment records are now stored in an authoritative server-keyed bucket.
- Client assignment lookup is a secondary index, not the primary game-server
  polling path.
- `AssignmentPrepared` now consumes the server-keyed assignment and client
  secondary index while leaving the prepared acknowledgement available for the
  matchmaker wait path and TTL cleanup.
- Disconnected active-connection reports delete the corresponding client
  assignment index and server assignment record.
- Failed assignment preparation now deletes the stale assignment record and
  enqueues typed lifecycle work.
- A JetStream WorkQueue stream now carries `release_allocation` and
  `delete_assignment` lifecycle jobs.
- The deployable matchmaker starts a durable lifecycle worker when NATS is
  configured. It consumes release/delete jobs, calls provider `release`, and
  removes stale assignment/prepared state.
- Lifecycle jobs have configurable max-deliver handling via
  `[allocation].lifecycle_job_max_deliver`; jobs that still fail on their final
  delivery are acked, counted, and logged as dead-lettered.
- Prepared-but-unconnected assignments have a configurable timeout:
  `[allocation].assignment_timeout_secs` for matchmaker retry/cleanup and
  `demo.assignment_timeout_secs` for the Bevy example's local admission gate.

Current limitations:

- no explicit assignment reservation step,
- lifecycle work has retry via JetStream redelivery and final-attempt
  dead-letter-style accounting, but no separate dead-letter stream or
  operator-facing retry dashboard yet,
- replacement assignment after server shutdown requires the client WebSocket to
  remain open after the first `AssignmentReady`.

What these limitations mean:

- Reservation: allocation currently reads capacity and then writes an
  assignment. There is no atomic capacity reservation, so multiple matchmaker
  instances could still race and choose the same last available slot. This is
  not a priority while we intentionally run one matchmaker.
- Lifecycle worker: release/delete jobs are durable work items in NATS and are
  now consumed by the matchmaker. Failed jobs rely on JetStream redelivery and
  are counted/logged when final delivery is reached, but there is not yet a
  separate dead-letter queue or UI for inspecting stuck lifecycle work.

Relevant file:

- `crates/lightyear_matchmaker_nats/src/lib.rs`

### 5. Prepared Ack Failure Behavior

The matchmaker waits for game-server `AssignmentPrepared` before returning
`assignment.ready`, and rejected prepares are represented.

Implemented:

- `[allocation].assignment_prepare_max_retries` controls how many additional
  allocation/assignment attempts can run after a preparation rejection or
  timeout,
- the default is `1`, meaning one initial attempt plus one retry,
- each retry creates a fresh request id, allocation, assignment id, and
  Lightyear client id,
- retries add failed server ids to the next allocation request's
  `avoid_server_ids`,
- configured static, NATS static, and mock Edgegap providers skip avoided
  servers before selecting capacity,
- real Edgegap rejects and releases an avoided deployment after session
  readiness reveals the deployment id,
- failed attempts are cleaned up before the next retry,
- lobby assignment retries clean up all member assignments for the failed
  attempt and queue one provider allocation release.

Current limitations:

- real Edgegap may still spend one create/poll cycle on an avoided deployment
  because the concrete deployment id is only known after session readiness,
- retry backoff exists, but the default is still `0` and deployed defaults still
  need review.

Relevant file:

- `crates/lightyear_matchmaker_server/src/lib.rs`

### 6. Real Lightyear Integration

The Bevy server crate can validate assigned `LightyearClientId`s and expose
assignment context. With the `lightyear-netcode` feature enabled on
`lightyear_matchmaker_bevy_server`, it also installs a real Lightyear Netcode
`ConnectionRequestHandler` that accepts only assigned Netcode client ids and
rejects unknown or non-Netcode ids.

Implemented:

- shared synchronous assignment allow-list for Lightyear's connection hook,
- Lightyear Netcode request handler backed by assignment state,
- optional plugin wiring via `with_lightyear_netcode()`,
- Lightyear `Connected`/`Disconnected` component observation to publish
  matchmaker active-connection reports.

Current limitations:

- The local headless Bevy example still simulates accepted clients instead of
  running a real Lightyear transport loop.
- The integration currently targets Lightyear Netcode. Steam/raw/local peer-id
  variants are intentionally rejected by this handler until we define their
  matchmaker identity mapping.

Relevant file:

- `crates/lightyear_matchmaker_bevy_server/src/lib.rs`

### 7. Real Edgegap Provider Boundary

The mock Edgegap provider remains available for local tests, and the real
provider now implements the core Bevygap Edgegap session flow.

Implemented:

- `allocation.source = "edgegap"` constructs the real provider,
- `POST /v1/session` session creation,
- readiness polling with `GET /v1/session/{session_id}`,
- deployment endpoint extraction from the ready session response,
- `DELETE /v1/session/{session_id}` release,
- Edgegap session/deployment ids preserved in allocation metadata,
- local HTTP tests for create/poll/release behavior.

Current limitations:

- no webhook-driven readiness path yet; readiness is poll-only,
- no live deployment capacity/reuse policy inside the real provider yet,
- lifecycle release calls are wired through the generic worker, but this has not
  been proven against a live Edgegap session yet,
- provider-specific failure mapping is still mostly status/body strings.

Relevant file:

- `crates/lightyear_matchmaker_provider_edgegap/src/lib.rs`

### 8. Client API And Bevy Client Plugin Scope

The wire protocol now includes request-play and basic lobby messages. The Bevy
client crate currently only wraps request-play and returns a `ConnectionGrant`.

If the goal is to support actual lobby UI/client workflows, the client plugin
needs a larger state model and message/event API.

Relevant files:

- `crates/lightyear_matchmaker_core/src/protocol.rs`
- `crates/lightyear_matchmaker_bevy_client/src/lib.rs`

## Current Verification

The following checks passed after the latest updates:

- `cargo fmt --all --check`
- `cargo test --workspace -j 4`
- `cargo clippy --workspace --all-targets -j 4 -- -D warnings`
- `RUSTDOCFLAGS='-D missing-docs' cargo doc --workspace --no-deps --all-features -j 4`
- `cargo test -p lightyear_matchmaker_nats -j 4`
- `just smoke-full-local`
- `podman compose -f examples/bevy_local_static/podman/compose.local.yml config`
- Lightrider static-provider deployment on Linode with the release-built
  matchmaker and static Bevy game server images.

Docker was not installed in the local environment, so the Docker Compose file was
not parsed through Docker itself.

The Linode deployment exposed the previous room-code limitation: the default
play flow worked, but joining private room `ABCD` failed with `no_capacity`
because no server had advertised that room key yet. The NATS static selector has
now been updated for this case. Re-run the deployed Lightrider check when we
next want deployment validation.

## Review/Fix/Improve Plan

This plan is ordered around non-smoke work first. Live Lightrider and live
Edgegap checks are still useful, but they are deployment validation, not the next
best way to improve the matchmaker implementation.

### P0: Contracts And Runtime Semantics

1. Decide whether allocation reservation semantics are needed for the MVP.

   Status: deferred for the current single-matchmaker target.

   Static and NATS static allocation currently read server-published capacity
   and then write an assignment. Capacity is not a reservation; it is only the
   server's latest self-report. There is not yet an atomic matchmaker-owned hold
   on that capacity. This is acceptable while one matchmaker is active, but it
   remains the main missing piece for multi-matchmaker deployment.

   Revisit this only if we need multiple active matchmaker instances or stronger
   capacity accounting than server-published capacity plus assignment
   preparation acknowledgements.

2. Wire explicit state machines into server/runtime observability.

   Status: partially implemented.

   Core now defines the lifecycle states. Assignment and lifecycle cleanup
   states have stable labels, transition helpers, transition logs, and
   state-labeled JSON metric snapshots. This is useful operationally because it
   makes retry, timeout, rejection, and dead-letter behavior visible without
   requiring a separate persisted state ledger for every assignment.

   Review target:

   - extend the same state labels to lobby metrics,
   - expose current assignment/lobby states through debug endpoints,
   - keep terminal states from re-entering active flows in more cleanup paths,
   - decide whether assignment state should become persisted state if we later
     add multi-matchmaker support.

3. Review and stabilize the public protocol shape.

   The websocket protocol now has version constants, optional `hello`
   negotiation, structured retryable errors, a protocol reference, and an
   AsyncAPI schema. It is still not a final stability contract.

   Review target:

   - review every client message and server message before declaring v1 stable,
   - decide whether clients must send `hello` or may keep implicit v1,
   - decide whether AsyncAPI is enough for client generation or whether we also
     want generated TypeScript/Rust client crates in the repo,
   - document the exact meaning of `request_id`, `allocation_id`,
     `assignment_id`, and `LightyearClientId`.

4. Review server-keyed assignment consumption semantics.

   Game servers now poll authoritative assignments by server id, while client
   lookup is a secondary index. `AssignmentPrepared` consumes the assignment
   queues and leaves active connections as the runtime state.

   Review target:

   - verify local NATS state no longer accumulates prepared assignments,
   - verify active connections remain present until disconnect or TTL,
   - verify assignment timeout cleanup does not revoke clients that connected.

5. Harden lifecycle worker observability.

   NATS lifecycle work is now consumed by the matchmaker process.

   Review target:

   - add a real dead-letter stream or inspection endpoint for final-failed jobs,
   - decide whether `/metrics` should stay JSON or gain Prometheus output,
   - document the operator flow for replaying or dismissing failed lifecycle
     work.

### P1: Local Runtime Confidence

6. Build a real Lightyear local transport smoke.

   The Bevy server crate has a real Netcode request handler, but the local smoke
   path still simulates accepted clients.

   Review target:

   - run a headless Lightyear server loop in the example,
   - connect a minimal headless Lightyear client with a returned `ConnectToken`,
   - verify accepted and rejected client ids through the real handler.

7. Add provider contract coverage.

   Static, NATS static, mock Edgegap, and real Edgegap all implement the same
   provider trait, but the expected cross-provider behavior is still mostly
   implicit.

   Review target:

   - shared tests for room selection inputs providers can support,
   - shared tests for `avoid_server_ids` behavior where providers can support
     it,
   - documented release semantics for static, NATS static, mock Edgegap, and
     real Edgegap.

8. Tune retry backoff and observability.

   Assignment preparation rejection or timeout now retries according to
   `[allocation].assignment_prepare_max_retries`, defaulting to one retry, and
   avoids failed server ids on later attempts. Optional retry backoff is
   implemented through `[allocation].assignment_retry_backoff_ms`.

   Review target:

   - decide whether the default should remain `0` for local dev or be non-zero
     for deployed configs,
   - surface avoided-server counts in metrics,
   - improve real Edgegap placement avoidance before session creation if the API
     exposes a suitable control.

### P2: Provider And Deployment Shape

9. Decide whether to implement the optional Open Match 2 adapter.

   The design is documented in [open-match2-integration.md](open-match2-integration.md).
   OM2 should be treated as an optional ticket pool and match-function runner,
   while Lightyear Matchmaker remains the director/session coordinator.

   Review target:

   - add provider-agnostic ticket and match DTOs,
   - define a `TicketMatcher` trait,
   - implement a local matcher first,
   - add `lightyear_matchmaker_open_match2` only after the local trait shape is
     stable,
   - keep direct room joins and Lightyear assignment delivery outside OM2.

10. Define Edgegap readiness strategy.

   The real provider is poll-only. Edgegap supports webhooks, and Bevygap also
   has cleanup concepts around unclaimed sessions.

   Review target:

   - decide whether readiness should be poll-only, webhook-first, or hybrid,
   - add webhook endpoint and verification if needed,
   - add unclaimed-session cleanup using the generic lifecycle model.

11. Decide Edgegap capacity/reuse policy.

   The current real Edgegap path creates a session and links to a deployment,
   but does not expose provider capacity or decide when to reuse deployments.

   Review target:

   - clarify whether Edgegap allocation is always session-per-request,
   - model deployment reuse/capacity if needed,
   - preserve Edgegap-specific ids only in provider metadata.

12. Improve deployment examples.

   The Bevy local static compose files now live under the example, but the real
   Edgegap deployment path still needs a complete operator-facing example.

   Review target:

   - add Edgegap-specific compose/env docs if needed,
   - document port forwarding or public NATS requirements,
   - keep credentials out of committed configs.

### P3: Lobby And Client Workflow

13. Decide whether lobbies stay in-process for the MVP.

    The current lobby service is intentionally small and not distributed.

    Review target:

    - either explicitly keep in-process lobbies as MVP-only,
    - or move lobby state to NATS for multiple matchmaker instances,
    - add leave, owner transfer, cleanup, reconnect, privacy, and invite rules
      only once the target runtime model is chosen.

14. Expand the Bevy client plugin.

    The native client plugin now has a persistent websocket session, lobby
    commands, inbound message events, connection grant state, structured errors,
    and reconnect reporting. The remaining work is higher-level UX state and
    wasm parity.

    Review target:

    - add a higher-level lobby UI state resource if game code needs one,
    - decide whether the plugin should automatically retry retryable commands,
    - implement persistent websocket session support for wasm,
    - add integration tests against a local matchmaker websocket.

### P4: Operations, Compatibility, And Deployment Validation

15. Improve operational endpoints and metrics.

    `/health`, `/ready`, and JSON `/metrics` exist. The remaining question is
    how operator-facing this should become before broader use.

    Review target:

    - decide whether JSON metrics are enough for now,
    - add Prometheus/OpenTelemetry output if needed,
    - add provider-specific health summaries if needed.

16. Add drain and shutdown controls.

    Matchmaker-wide drain and per-game-server drain controls now exist. New
    websocket upgrades are rejected while the matchmaker is draining; existing
    websocket sessions reject new non-hello work. Per-server drain markers make
    new allocations avoid that server, and the NATS path cancels pending
    assignments plus queues assignment-delete and allocation-release lifecycle
    work.

    Review target:

    - decide whether admin endpoints need auth before public deployment,
    - add graceful process signal handling if shutdown should actively drain
      before exiting,
    - add operator-facing visibility for active server drains and cleanup
      status,
    - decide whether connected clients should ever be forcibly disconnected by
      the matchmaker, or remain game-server-owned.

17. Build compatibility and load coverage.

    Review target:

    - supported Lightyear version matrix,
    - multi-client lobby load test,
    - multi-matchmaker NATS race test once reservation semantics exist.

18. Validate deployed requested-room and live Edgegap flows when credentials and
    deployment time are available.

    These checks should stay outside the normal inner loop.

    Review target:

    - verify first player can create private room `ABCD`,
    - verify second player joins the existing `ABCD` room,
    - verify a full `ABCD` room does not create a duplicate on another server,
    - run a credential-gated Edgegap create/poll/release check against a real
      provider app/version.
