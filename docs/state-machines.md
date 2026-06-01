# State Machines

This document formalizes the runtime states used by Lightyear Matchmaker. The
core crate re-exports matching Rust enums from `lightyear_matchmaker_core`.

The current server does not persist every state as a separate record yet. Some
states are still implicit in websocket sessions, NATS buckets, and game-server
reports. The purpose of this document is to make the model explicit before we
wire these states into metrics, debug endpoints, and future ticket-matching
backends.

## Principles

- State transitions should be monotonic unless a state explicitly supports
  returning to a previous phase.
- Terminal states should not return to active states.
- Capacity reports are not reservations.
- Assignment state is per client.
- Allocation and reservation state are per provider capacity/session.
- Lobby and ticket state are matchmaker-owned.
- Game-server connection state is authoritative after assignment preparation.

## Lobby State

`LobbyState` describes the lifecycle of a lobby managed by the matchmaker.

| State | Meaning |
| --- | --- |
| `open` | Lobby exists and can accept members or readiness changes. |
| `full` | Lobby reached its configured member limit but is not fully ready. |
| `ready` | Required members are ready and the lobby can be assigned. |
| `assigning` | The matchmaker is assigning the lobby roster to server capacity. |
| `assigned` | The lobby roster has received connection grants. |
| `closed` | Lobby is closed and should not accept further mutations. |

Expected transitions:

```text
open -> full
open -> ready
open -> closed
full -> open
full -> ready
full -> closed
ready -> open
ready -> assigning
ready -> closed
assigning -> open
assigning -> assigned
assigning -> closed
assigned -> closed
```

The current implementation has a small in-process lobby runtime. It effectively
uses `open`, `ready`, and `assigning`, while `assigned` and `closed` are target
states for clearer cleanup and reconnect behavior.

## Ticket State

`TicketState` describes a matchmaking ticket. This is the state machine that
maps most directly to Open Match 2.

| State | Meaning |
| --- | --- |
| `created` | Ticket exists but is not eligible for matching yet. |
| `active` | Ticket is eligible to be returned by a matching function. |
| `matched` | Ticket was included in a match returned by matching logic. |
| `consumed` | Ticket was accepted by the director and will not be reused. |
| `cancelled` | Ticket was cancelled by the client or owning lobby. |
| `expired` | Ticket expired before it was consumed. |
| `failed` | Ticket failed because of a backend or validation error. |

Expected transitions:

```text
created -> active
created -> cancelled | expired | failed
active -> matched
active -> cancelled | expired | failed
matched -> consumed
matched -> cancelled | expired | failed
```

If we integrate Open Match 2, `created` corresponds to OM2 ticket creation,
`active` corresponds to activating tickets into matchmaking pools, `matched`
corresponds to a streamed MMF result, and `consumed` is our director-side
collision-resolution decision.

## Match State

`MatchState` describes the director-owned lifecycle of a match returned by
matching logic.

| State | Meaning |
| --- | --- |
| `proposed` | A match candidate was returned by matching logic. |
| `accepted` | The director accepted the match after collision checks. |
| `rejected` | The director rejected the match, usually because a ticket was consumed. |
| `assigning` | The director is allocating server capacity for this match. |
| `assigned` | Every accepted player in the match has received an assignment. |
| `failed` | The match failed before assignment completed. |

Expected transitions:

```text
proposed -> accepted | rejected | failed
accepted -> assigning | rejected | failed
assigning -> assigned | failed
```

Open Match 2 does not resolve ticket collisions for us. If two match functions
return the same ticket in different matches, Lightyear Matchmaker must reject
the losing match before provider allocation.

## Allocation State

`AllocationState` describes provider allocation.

| State | Meaning |
| --- | --- |
| `requested` | Allocation has been requested from a provider. |
| `allocated` | Provider returned capacity, endpoint, and server identity. |
| `released` | Provider-side capacity/session was released. |
| `expired` | Allocation expired before it was used or released. |
| `failed` | Allocation failed. |

Expected transitions:

```text
requested -> allocated | expired | failed
allocated -> released | expired | failed
```

For static providers, release is usually a no-op. For Edgegap, release maps to
deleting or releasing provider-side session/deployment state.

## Reservation State

`ReservationState` describes a future matchmaker-owned capacity hold.
Reservations are not implemented yet.

| State | Meaning |
| --- | --- |
| `requested` | The matchmaker is trying to create a capacity hold. |
| `held` | Capacity is held for a pending assignment attempt. |
| `committed` | The held capacity was consumed by assignment/connection state. |
| `released` | The hold was released without being committed. |
| `expired` | The hold expired before assignment completed. |
| `failed` | The hold failed. |

Expected transitions:

```text
requested -> held | expired | failed
held -> committed | released | expired | failed
```

This state machine is the clearest way to improve our capacity model without
jumping immediately to multiple matchmakers. We can first implement a simple
single-matchmaker seat hold, then back it with NATS compare-and-set if we later
need multi-matchmaker correctness.

## Assignment State

`AssignmentState` is per assigned client.

| State | Meaning |
| --- | --- |
| `created` | Assignment object was created in matchmaker memory. |
| `persisted` | Assignment was persisted for game-server consumption. |
| `preparing` | Game server has been asked to prepare local admission state. |
| `prepared` | Game server acknowledged that local admission state is ready. |
| `ready` | Client has received connection material for the prepared assignment. |
| `connected` | Game server reported the Lightyear client id as connected. |
| `disconnected` | Game server reported the Lightyear client id as disconnected. |
| `rejected` | Game server rejected the assignment during preparation. |
| `timed_out` | Assignment timed out before the client connected. |
| `failed` | Assignment failed because of a matchmaker/provider/coordination error. |
| `deleted` | Assignment coordination state was explicitly deleted. |

Expected transitions:

```text
created -> persisted | preparing | failed | deleted
persisted -> preparing | prepared | rejected | timed_out | failed | deleted
preparing -> prepared | rejected | timed_out | failed | deleted
prepared -> ready | connected | timed_out | failed | deleted
ready -> connected | timed_out | failed | deleted
connected -> disconnected
```

`AssignmentPrepared` consumes pending server/client assignment queues. After
that, `active_connections` becomes the authoritative runtime signal.

## Connection State

`ConnectionState` describes the client connection attempt after a grant exists.

| State | Meaning |
| --- | --- |
| `pending` | Client has connection material but has not connected yet. |
| `accepted` | Game server validation accepted the connection attempt. |
| `connected` | Game server reported the client as connected. |
| `disconnected` | Game server reported the client as disconnected. |
| `rejected` | Game server validation rejected the connection attempt. |
| `expired` | Prepared assignment expired before a connection was observed. |

Expected transitions:

```text
pending -> accepted | connected | rejected | expired
accepted -> connected | rejected | expired
connected -> disconnected
```

## Lifecycle Work State

`LifecycleWorkState` describes durable cleanup jobs.

| State | Meaning |
| --- | --- |
| `queued` | Work was queued durably. |
| `processing` | A worker is currently processing the item. |
| `succeeded` | Work succeeded and was acknowledged. |
| `retrying` | Work failed and is waiting for redelivery. |
| `dead_lettered` | Work exhausted delivery attempts and needs operator attention. |
| `dropped` | Work was invalid or intentionally discarded. |

Expected transitions:

```text
queued -> processing | dropped
processing -> succeeded | retrying | dead_lettered | dropped
retrying -> processing | dead_lettered | dropped
```

The current NATS worker implements retry through JetStream redelivery and
dead-letter-style accounting. A real dead-letter stream or inspection endpoint
is still future work.
