# Implementation Status

Status as of the current Phase 1/MVP implementation.

This repo has a solid local vertical slice, but it does not implement the full
architecture plan yet. The current state is best described as a working Phase 1
foundation plus initial versions of the later roadmap slices.

## Implemented

- WebSocket `request_play` flow.
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
- Headless Bevy game-server example.
- Bevy game-server readiness and capacity publishing.
- Bevy game-server assignment polling.
- Assignment preparation acknowledgement before returning `assignment.ready`.
- Active-connection reporting after assignment receipt.
- Assignment roster, team, lobby id, and match metadata.
- Provider-independent Bevy connection validation state.
- Bevy validation context that maps `LightyearClientId` to player/lobby/team
  metadata.
- Optional real Lightyear Netcode connection request handler in
  `lightyear_matchmaker_bevy_server` behind the `lightyear-netcode` feature.
- Lightyear Netcode `Connected`/`Disconnected` observation for active-connection
  reports when that feature is enabled.
- Minimal Bevy client helper/plugin surface for websocket `request_play`.
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
- Basic assignment lifecycle logs.
- Duplicate assignment-id idempotency in the Bevy server.
- Local example layout scoped under `examples/bevy_local_static/`, including
  config, smoke scripts, Docker Compose, and Podman Compose files.
- Example config parse coverage for local, NATS, mock Edgegap, real Edgegap
  template, and compose matchmaker configs.
- Public module/type/function docs plus targeted private comments around
  non-obvious lifecycle and provider behavior.
- Local smoke harnesses:
  - `just smoke-full-local`
  - `just smoke-full-edgegap-mock`

## Not Fully Implemented

- End-to-end real Lightyear transport example. The connection request handler
  exists, but the local headless Bevy example still simulates accepted clients
  instead of running a real Lightyear server/client transport loop.
- Complete Edgegap production behavior. The real provider exists, but webhook
  readiness, capacity/reuse policy, live API smoke coverage, and lifecycle
  worker integration are still missing.
- Full lobby lifecycle. Create, join-code, ready checks, and basic team
  assignment exist, but leave, owner transfer, reconnect handling, privacy,
  invites, cleanup, and distributed lobby state are missing.
- NATS-backed lobby state for multiple matchmaker instances.
- Allocation reservation and consume semantics. Static/NATS allocation currently
  selects capacity, but there is no durable reservation step.
- Matchmaker-owned reservation accounting. Capacity reports exist, but there is
  still no separate CAS-protected reservation ledger that accounts for pending
  assignments across multiple matchmaker instances.
- Retry/reallocation policy when assignment preparation fails or times out.
- Runtime lifecycle worker that consumes NATS release/delete work and calls
  provider release APIs.
- Metrics and operational endpoints beyond `/health`.
- Drain/shutdown controls.
- Load tests.
- Live Edgegap smoke tests guarded by credentials.
- Compatibility matrix against supported Lightyear versions.
- Full Bevy client workflow plugin for lobby UI/client state. The current client
  crate is a minimal request-play helper.

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

### 3. Assignment Lifecycle And NATS Model

The NATS coordination model works for local smoke tests, but it is not yet a
production assignment store.

Implemented:

- NATS KV TTLs are configurable under `[nats.ttl]`, with defaults matching the
  original hard-coded values.
- Assignment records and prepared acknowledgements can be deleted explicitly.
- Assignment records are now stored in an authoritative server-keyed bucket.
- Client assignment lookup is a secondary index, not the primary game-server
  polling path.
- Disconnected active-connection reports delete the corresponding client
  assignment index and server assignment record.
- Failed assignment preparation now deletes the stale assignment record and
  enqueues typed lifecycle work.
- A JetStream WorkQueue stream now carries `release_allocation` and
  `delete_assignment` lifecycle jobs.

Current limitations:

- no explicit assignment reservation/consume step,
- lifecycle workers still need to be implemented for real provider/session
  release and game-server assignment invalidation.

What these limitations mean:

- Reservation/consume: allocation currently reads capacity and then writes an
  assignment. There is no atomic capacity reservation, so two matchmaker
  instances can still race and both choose the same last available slot.
- Lifecycle workers: release/delete jobs are now durable work items in NATS, but
  no worker has been written yet to call provider release APIs or tell a game
  server to evict an already-observed assignment.

Relevant file:

- `crates/lightyear_matchmaker_nats/src/lib.rs`

### 4. Prepared Ack Failure Behavior

The matchmaker waits for game-server `AssignmentPrepared` before returning
`assignment.ready`, and rejected prepares are represented. However, rejection or
timeout currently fails the client request instead of retrying another server.

Decision needed: should prepare failure be client-visible, or should the
matchmaker retry allocation/preparation before failing?

Relevant file:

- `crates/lightyear_matchmaker_server/src/lib.rs`

### 5. Real Lightyear Integration

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

### 6. Real Edgegap Provider Boundary

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
- no lifecycle worker consumes NATS release jobs and calls this provider release
  path yet,
- provider-specific failure mapping is still mostly status/body strings.

Relevant file:

- `crates/lightyear_matchmaker_provider_edgegap/src/lib.rs`

### 7. Client API And Bevy Client Plugin Scope

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
- `just smoke-full-local`
- `podman compose -f examples/bevy_local_static/podman/compose.local.yml config`

Docker was not installed in the local environment, so the Docker Compose file was
not parsed through Docker itself.

## Review/Fix/Improve Plan

### P0: Correctness And Cleanup Semantics

1. Add allocation reservation and consume semantics.

   Static and NATS static allocation currently read server-published capacity
   and then write an assignment. Capacity is not a reservation; it is only the
   server's latest self-report. There is not yet an atomic matchmaker-owned hold
   on that capacity, so two matchmaker instances can still select the same last
   available slot.

   Review target:

   - define a provider-agnostic reservation model,
   - make static/NATS capacity selection reserve before assignment persistence,
   - decide how reservations expire and how failed assignment preparation
     releases them.

2. Review server-keyed assignment consumption semantics.

   Game servers now poll authoritative assignments by server id, while client
   lookup is a secondary index. The remaining design question is whether a game
   server should delete/consume an assignment immediately after preparing it, or
   keep it until the client disconnects.

   Review target:

   - define whether `AssignmentPrepared` consumes an assignment,
   - keep active connections as the runtime connected-client state,
   - ensure assignment and client-index deletes stay consistent.

3. Implement lifecycle workers.

   NATS already carries typed `release_allocation` and `delete_assignment` work,
   but there is no worker that consumes those messages.

   Review target:

   - provider release worker for Edgegap sessions and future providers,
   - game-server invalidation path for assignments already observed by a server,
   - retry and dead-letter behavior for failed lifecycle jobs.

### P1: End-To-End Runtime Confidence

4. Build a real Lightyear local transport smoke.

   The Bevy server crate has a real Netcode request handler, but the local smoke
   path still simulates accepted clients.

   Review target:

   - run a headless Lightyear server loop in the example,
   - connect a minimal headless Lightyear client with a returned `ConnectToken`,
   - verify accepted and rejected client ids through the real handler.

5. Add credential-gated live Edgegap smoke coverage.

   The real Edgegap provider is tested with a local HTTP server, but not against
   Edgegap itself.

   Review target:

   - `EDGEGAP_API_KEY` guarded test or `just smoke-edgegap-live`,
   - create/poll/release a real session,
   - document expected app/version/port setup and cleanup behavior.

6. Decide retry/reallocation behavior for prepare failure.

   Assignment preparation rejection or timeout currently fails the client
   request after queueing cleanup.

   Review target:

   - decide whether the matchmaker retries another allocation before failing,
   - cap retries and surface useful client errors,
   - release every failed allocation attempt.

### P2: Provider And Deployment Shape

7. Define Edgegap readiness strategy.

   The real provider is poll-only. Edgegap supports webhooks, and Bevygap also
   has cleanup concepts around unclaimed sessions.

   Review target:

   - decide whether readiness should be poll-only, webhook-first, or hybrid,
   - add webhook endpoint and verification if needed,
   - add unclaimed-session cleanup using the generic lifecycle model.

8. Decide Edgegap capacity/reuse policy.

   The current real Edgegap path creates a session and links to a deployment,
   but does not expose provider capacity or decide when to reuse deployments.

   Review target:

   - clarify whether Edgegap allocation is always session-per-request,
   - model deployment reuse/capacity if needed,
   - preserve Edgegap-specific ids only in provider metadata.

9. Improve deployment examples.

   The Bevy local static compose files now live under the example, but the real
   Edgegap deployment path still needs a complete operator-facing example.

   Review target:

   - add Edgegap-specific compose/env docs if needed,
   - document port forwarding or public NATS requirements,
   - keep credentials out of committed configs.

### P3: Lobby And Client Workflow

10. Decide whether lobbies stay in-process for the MVP.

    The current lobby service is intentionally small and not distributed.

    Review target:

    - either explicitly keep in-process lobbies as MVP-only,
    - or move lobby state to NATS for multiple matchmaker instances,
    - add leave, owner transfer, cleanup, reconnect, privacy, and invite rules
      only once the target runtime model is chosen.

11. Expand the Bevy client plugin.

    The client crate currently wraps request-play. It does not yet model lobby
    UI state or the full websocket message lifecycle.

    Review target:

    - client-side lobby create/join/ready events,
    - connection grant state transitions,
    - reconnect/error handling.

### P4: Operations And Compatibility

12. Add operational endpoints and metrics.

    Current HTTP surface is mostly `/health` and websocket traffic.

    Review target:

    - readiness/liveness distinction,
    - assignment/provider/lobby metrics,
    - NATS and provider health summaries.

13. Add drain and shutdown controls.

    Review target:

    - stop accepting new websocket requests,
    - finish or cancel in-flight assignments,
    - release pending provider allocations on shutdown.

14. Build compatibility and load coverage.

    Review target:

    - supported Lightyear version matrix,
    - multi-client lobby load test,
    - multi-matchmaker NATS race test once reservation semantics exist.
