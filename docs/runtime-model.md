# Runtime Model

This document describes the runtime model that exists today. The broader target
architecture is still tracked in [architecture-plan.md](architecture-plan.md),
but this file is the shorter reference for reviewing the current
implementation.

The formal lifecycle states are tracked separately in
[state-machines.md](state-machines.md). The Open Match 2 adapter design is
tracked in [open-match2-integration.md](open-match2-integration.md).

## Edgegap Smoke Strategy

A live Edgegap smoke test is useful, but it should not be the default way to
validate most matchmaker changes.

The live Edgegap path needs real provider credentials, a published game-server
image, provider app/version setup, public networking, and cleanup of real
provider sessions. That makes it an integration/deployment test, not a cheap
development loop.

Lightrider is a reasonable place to run that smoke only when the goal is to
prove the full deployed game path:

- browser client,
- public HTTPS/WebSocket endpoint,
- matchmaker,
- NATS coordination,
- Edgegap-created game deployment,
- game-server assignment preparation,
- Lightyear connection grant returned to the client.

For normal development, prefer these cheaper checks:

- unit tests for provider selection and NATS state transitions,
- local NATS static smoke for the game-server contract,
- mock Edgegap smoke for provider-boundary behavior,
- local HTTP tests for Edgegap request/response mapping.

## Concepts

`request_id`

One matchmaker-owned id for a client or lobby request. Retries get fresh request
ids so retried assignments do not collide with earlier attempts.

`allocation_id`

Provider-owned id for the selected capacity or session. Static providers may use
a deterministic id derived from the server. Edgegap uses the provider session or
deployment identity. This id is used when releasing provider-side state.

`assignment_id`

Matchmaker-owned id for one client that should be accepted by one game server.
An assignment means "ask this server to prepare to accept this Lightyear client
id for this player/lobby/team context."

`reservation`

Not implemented yet. A reservation would be a matchmaker-owned capacity hold
created before writing assignments. Today, capacity reports are read and then
assignments are written. That is sufficient for the current one-matchmaker
deployment model, but it is not a complete multi-matchmaker capacity ledger.

`server capacity`

Game-server-published metadata describing whether a server is ready, how many
players it currently hosts, room capacity, region, endpoint, optional cert
digest, and per-room occupancy.

`assignment prepared`

Game-server acknowledgement that it observed an assignment and installed local
admission state. After this acknowledgement, the pending assignment is consumed
from the server queue. Runtime truth moves to `active_connections`.

`active connection`

Game-server report that a Lightyear client id is connected or disconnected.
Connected reports keep the runtime state alive. Disconnected reports remove the
client index and server assignment state for that client.

`server drain`

Operator or matchmaker intent to stop placing new assignments on one game
server. A drain marker is separate from the server's own capacity report:
servers may keep reporting ready/capacity while operators suppress new
placement and let existing connected players finish.

## Assignment Flow

1. A client opens `/ws` and sends `request_play`, or a lobby becomes ready.
2. The matchmaker resolves a lightweight identity from the connection metadata.
3. The configured provider selects capacity:
   - `configured_static` uses the configured server list,
   - `nats_static` uses live game-server capacity reports,
   - `edgegap_mock` uses the mock provider bridge,
   - `edgegap` creates and polls a real Edgegap session.
4. The matchmaker creates an assignment and a Lightyear Netcode connection
   grant.
5. If assignment preparation is not required, the client receives
   `assignment.ready` immediately.
6. If assignment preparation is required, the assignment is written to NATS under
   the target server id.
7. The game server polls its server-keyed assignments, prepares local admission
   state, and publishes `AssignmentPrepared`.
8. The matchmaker observes the prepared acknowledgement and sends
   `assignment.ready` to the client.
9. The game server reports `active_connections` when the Lightyear client
   connects or disconnects.

## NATS State

NATS is used for ephemeral coordination, not persistent player or progression
state.

| Bucket or Stream | Purpose | Default TTL |
| --- | --- | --- |
| `server_readiness` | Latest server endpoint/readiness metadata | 30 seconds |
| `server_capacity` | Latest server and room capacity reports | 30 seconds |
| `assignments_by_server` | Authoritative pending assignments keyed by server | 60 seconds |
| `assignments_by_client` | Secondary lookup index keyed by Lightyear client id | 60 seconds |
| `assignments_prepared` | Game-server preparation acknowledgements | 60 seconds |
| `active_connections` | Connected-client reports keyed by server/client | 30 seconds |
| `server_drains` | Per-server drain markers that suppress new placement | 86400 seconds |
| `lifecycle_work` | Durable release/delete work queue | 600 seconds |

All names are namespace-prefixed when `[nats].namespace` is configured.

The authoritative game-server polling path is `assignments_by_server`.
`assignments_by_client` is intentionally secondary. It is useful for debugging,
tests, and disconnect cleanup, but it is not the data structure game servers
poll.

## Assignment Cleanup

Prepared-but-unconnected assignments time out after
`[allocation].assignment_timeout_secs`, defaulting to 60 seconds. The Bevy local
example has a matching `demo.assignment_timeout_secs` setting so the game-server
side admission gate expires stale prepared assignments too.

If a prepared assignment times out and the selected server has disappeared from
capacity reports, the matchmaker can try a replacement assignment while the
client WebSocket remains open. Replacement attempts avoid the failed server id.

When the game server reports a disconnect, the coordinator removes:

- the active connection report,
- the client secondary assignment index,
- the server-keyed assignment record when one still exists.

When a server is explicitly drained through the matchmaker admin API, the
matchmaker:

- marks the server drained locally and in NATS when NATS is configured,
- excludes it from new allocation requests,
- cancels pending server-keyed assignments for that server,
- removes client indexes and preparation acknowledgements for those assignments,
- queues lifecycle work to delete assignments and release unique provider
  allocations associated with the canceled assignments.

Active connection reports are not deleted by server drain. A drain stops new
placement and pending preparation work; connected clients remain owned by the
game server's normal disconnect/shutdown policy.

## Lifecycle Work

Lifecycle work is used for cleanup that should survive a failed request handler.
The current work types are:

- `release_allocation`: call the provider release path for an allocation,
- `delete_assignment`: delete stale assignment/prepared state.

The deployable matchmaker starts a durable lifecycle worker when NATS is
configured. Failed jobs rely on JetStream redelivery. The maximum delivery count
is configured by `[allocation].lifecycle_job_max_deliver`, defaulting to 10.
On the final failed delivery, the worker logs and counts the job as
dead-lettered, then acknowledges it so it stops cycling.

There is not yet a separate dead-letter stream or operator UI. `/metrics`
exposes counters that make this visible enough for the current MVP.

## Retry Behavior

`[allocation].assignment_prepare_max_retries` controls additional full
allocation/assignment attempts after preparation failure or timeout. The default
is `1`, meaning one initial attempt plus one retry.

Retries create fresh ids:

- new `request_id`,
- new `allocation_id` when the provider returns one,
- new `assignment_id`,
- new Lightyear client id.

Retries carry `avoid_server_ids` so static, NATS static, and mock Edgegap avoid
servers that already failed this request. The real Edgegap provider can only
avoid a concrete deployment after readiness reveals the deployment id, so it may
still spend one create/poll cycle before releasing an avoided deployment.

`[allocation].assignment_retry_backoff_ms` adds an optional delay before retrying
after a failed assignment attempt. It defaults to `0`.

## Room Selection

`RoomSelection::Auto`

Select any ready server with player capacity.

`RoomSelection::New`

Select a ready server with room capacity.

`RoomSelection::Code` and `RoomSelection::Id`

If a matching room is already advertised, route to the server that hosts it. If
the room is full, do not create a duplicate elsewhere. If no matching room is
advertised yet, choose a server with room capacity so the game server can create
the room during assignment preparation or first client join.

`Code` and `Id` currently share this create-if-missing behavior. If an internal
room id should mean "existing only", split those semantics before stabilizing
the public API.

## Operational Endpoints

`GET /health`

Liveness check. Returns a static success response when the process can serve
HTTP.

`GET /ready`

Readiness check. Returns JSON with:

- `ready`,
- `draining`,
- `nats_configured`,
- `nats_ok`,
- `message`.

When NATS is configured, readiness also checks that NATS coordination can list
capacity for the configured game/version.

`GET /metrics`

JSON runtime counters for:

- websocket sessions,
- request-play attempts,
- assignment creation/preparation/readiness/retries/timeouts,
- lifecycle worker outcomes,
- lobby counts.

This is intentionally simple JSON for the MVP. Prometheus/OpenTelemetry export
is still a separate operations task.

`POST /admin/drain`

Marks the matchmaker as draining. New websocket upgrades return HTTP 503, and
existing websocket sessions reject new non-hello matchmaker messages with a
`draining` protocol error. In-flight handlers are not interrupted by this flag.
Body:

```json
{ "reason": "maintenance" }
```

`DELETE /admin/drain`

Clears the matchmaker-wide drain flag.

`POST /admin/servers/{server_id}/drain`

Marks one game server as drained. New placements avoid the server. With NATS
configured, pending assignments for that server are canceled and cleanup work is
queued for provider allocation release.

`DELETE /admin/servers/{server_id}/drain`

Clears the per-server drain marker.

## Config Fields To Review

These are the main runtime knobs for the current implementation:

```toml
[allocation]
source = "nats_static"
require_assignment_prepare = true
assignment_prepare_timeout_ms = 3000
assignment_prepare_poll_ms = 25
assignment_prepare_max_retries = 1
assignment_retry_backoff_ms = 0
assignment_timeout_secs = 60
lifecycle_job_max_deliver = 10

[nats.ttl]
server_readiness_secs = 30
server_capacity_secs = 30
assignments_secs = 60
assignments_prepared_secs = 60
active_connections_secs = 30
server_drains_secs = 86400
lifecycle_work_secs = 600
```

The key review point is whether the defaults match the expected game behavior:
short enough to clean up crashed local processes, but long enough that slow
assignment preparation and client connection setup are not accidentally
invalidated.
