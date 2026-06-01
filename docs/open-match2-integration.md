# Open Match 2 Integration

Open Match 2 can be a good optional ticket-matching backend for Lightyear
Matchmaker, but it should not replace the Lightyear-specific session
coordination path.

## Recommendation

Keep Lightyear Matchmaker as the **director and session coordinator**:

- client websocket ownership,
- lobby/party ownership,
- Lightyear `ConnectToken` issuing,
- provider allocation,
- game-server assignment preparation,
- assignment delivery to connected clients,
- lifecycle cleanup.

Use Open Match 2 optionally as the **ticket pool and match-function runner**:

- ticket creation and activation,
- ticket storage/expiry,
- match profile evaluation,
- invoking external matchmaking functions,
- streaming matches back to the director.

Do not use Open Match 2 deprecated assignment APIs as the primary client path.
OM2's own docs recommend that the matchmaker/director owns assignment delivery
to clients.

Sources:

- https://openmatch.dev/site/v2/overview/
- https://development-dot-open-match-site.appspot.com/site/v2/api/
- https://openmatch.dev/site/v2/installation/

## Why This Fits

Open Match 2 is deliberately smaller than Open Match 1. It has one main
deployable binary, `om-core`, which stores tickets, invokes matchmaking
functions, and streams matches. It no longer has the Open Match 1 evaluator.
Ticket collision handling is explicitly the director's responsibility.

That maps naturally to this repo:

- OM2 owns ticket pools and match-function scaling.
- Lightyear Matchmaker owns collision resolution, provider allocation,
  Lightyear token issuing, and websocket notification.

This avoids reimplementing complicated ticket querying/matching infrastructure
while keeping the Lightyear-specific parts in Rust where they belong.

## Deployment Shape

Do not embed `om-core` inside the Rust matchmaker process. Treat it as a
separate service managed by local compose files, Kubernetes, Cloud Run, or a
sidecar only when that is operationally useful.

Suggested optional workspace shape:

```text
crates/
  lightyear_matchmaker_open_match2/
examples/
  bevy_open_match2/
    config/
    docker-compose/
    podman/
    mmf/
```

The adapter crate would talk to OM2 over its HTTP/gRPC API. The example would
run:

- NATS with JetStream,
- Lightyear Matchmaker,
- OM2 `om-core`,
- Redis or OM2's local in-memory mode for development,
- one or more MMF services,
- one or more static Bevy game servers.

## Runtime Flow

1. Client opens `/ws`.
2. Client creates or joins a lobby, or sends a future queue-join request.
3. Lightyear Matchmaker creates one local `request_id`.
4. Lightyear Matchmaker creates one or more OM2 tickets:
   - one ticket per solo player, or
   - one ticket per lobby/party for the MVP if the group must stay together.
5. Lightyear Matchmaker activates tickets when they should enter matchmaking.
6. A director loop calls OM2 `InvokeMatchmakingFunctions` with a match profile
   and configured MMF endpoints.
7. OM2 streams matches returned by the MMFs.
8. Lightyear Matchmaker checks ticket collisions by consuming local ticket state.
9. Winning matches move to provider allocation.
10. The provider returns a server allocation.
11. Lightyear Matchmaker writes game-server assignments and waits for
    `AssignmentPrepared`.
12. Lightyear Matchmaker sends `assignment.ready` to each connected client.
13. Cancelled/disconnected clients cause tickets to be deactivated/cancelled.

## Ticket Mapping

For a solo player ticket, suggested OM2 attributes:

```text
game = "lightrider"
version = "dev"
mode = "casual"
region = "us-east"
player_id = "ip:..."
request_id = "request-..."
party_size = 1
```

For a lobby/party ticket, MVP mapping should prefer one group ticket:

```text
game = "lightrider"
version = "dev"
mode = "duo"
lobby_id = "lobby-..."
party_size = 2
regions = "us-east,us-west"
request_id = "request-..."
```

One group ticket keeps parties together without requiring MMFs to understand
multi-ticket group locking. Later, if match functions need individual player
attributes, we can use one ticket per player with a shared `lobby_id` or
`party_id`, but then the director must reject partial-party matches.

## Director Responsibilities

Even with OM2, Lightyear Matchmaker must own:

- ticket collision resolution,
- local state transitions from `matched` to `consumed`,
- provider allocation,
- assignment id generation,
- assignment preparation timeout/retry,
- client notification,
- provider release on failure,
- ticket deactivation on cancellation or disconnect.

This is not optional. OM2 intentionally does not solve final assignment delivery
or ticket collision resolution for the director.

## What Not To Use OM2 For

Do not route simple room joins through OM2.

These flows should remain native Lightyear Matchmaker behavior:

- join existing room by id/code,
- create room if missing by code,
- browse/list rooms,
- direct static server request for local development,
- provider allocation after a match is accepted,
- Lightyear `ConnectToken` generation.

OM2 is most useful when we need queued matching across many active tickets with
rules, pools, profiles, skill/latency matching, and scalable match functions.

## Implementation Plan

1. Keep the current direct `request_play` path unchanged.
2. Add provider-agnostic ticket and match DTOs to core.
3. Add a `TicketMatcher` trait that can be implemented by a local matcher first.
4. Add `lightyear_matchmaker_open_match2` as an optional adapter.
5. Add a director loop in `lightyear_matchmaker_server` that consumes matches
   from the trait and drives allocation/assignment.
6. Add local compose files for OM2 and a simple example MMF.
7. Add collision tests before any live deployment test.

## Open Questions

- Should lobbies become one OM2 ticket or multiple linked tickets by default?
- Which attributes are stable enough for a public match profile schema?
- Should ticket state live in NATS, OM2 only, or both?
- Do we need a Rust MMF SDK/example, or is a small Go example enough for OM2?
- Should queue matchmaking be exposed as new websocket messages or reuse
  `request_play` with a queue profile?
