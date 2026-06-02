# State Machines

This document formalizes the runtime states currently exposed by
`lightyear_matchmaker_core`.

The goal is to keep the state model small enough to match the implementation.
The server still stores some facts implicitly through websocket sessions, NATS
buckets, game-server reports, and counters. States should be added to the core
model only when they describe a real runtime object or a stable integration
contract.

## Principles

- State transitions should be monotonic unless a state explicitly supports
  returning to a previous phase.
- Terminal states should not return to active states.
- Capacity reports are not reservations.
- Assignment state is per assigned Lightyear client id.
- Lobby membership state is separate from assignment progress.
- Game-server connection reports become authoritative after assignment
  preparation.

## Lobby State

`LobbyState` describes only the lifecycle of lobby membership.

| State | Meaning |
| --- | --- |
| `open` | Lobby exists and can accept membership or readiness changes. |
| `closed` | Lobby is closed and should not accept further mutations. |

Expected transitions:

```text
open -> closed
```

The lobby state intentionally does not include `full`, `ready`, `assigning`, or
`assigned`.

- `full` is derived from `members.len() >= max_players`.
- `ready` is derived from member readiness.
- `assigning` and `assigned` belong to assignment or match attempts, not to the
  lobby itself.

This matters because a lobby or room can receive multiple assignment attempts
over time, and potentially in parallel. The lobby can remain `open` while one
roster subset is preparing, another has received connection grants, and other
members are still changing readiness.

## Allocation State

`AllocationState` describes provider allocation.

| State | Meaning |
| --- | --- |
| `requested` | Allocation has been requested from a provider. |
| `allocated` | Provider returned capacity, endpoint, and server identity. |
| `released` | Provider-side capacity/session was released. |
| `failed` | Allocation failed. |

Expected transitions:

```text
requested -> allocated | failed
allocated -> released | failed
```

For static providers, release is usually a no-op. For Edgegap, release maps to
deleting or releasing provider-side session/deployment state. Expiry is treated
as a failure or release reason unless the implementation later needs a distinct
provider-visible expiry state.

## Assignment State

`AssignmentState` is per assigned Lightyear client id.

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

## Not Modeled As Core States Yet

`TicketState` and `MatchState`

The optional Open Match 2 adapter may eventually need ticket and match attempt
states. Those states should be introduced with the adapter or local ticket
matcher implementation, not as speculative core API.

`ReservationState`

Capacity reservations are intentionally deferred while the target runtime uses
one matchmaker. If we add a real reservation ledger later, it should get its own
state model at that point.

`ConnectionState`

Connection progress is already represented by `AssignmentState` plus
game-server `active_connections` reports. A separate connection state would
duplicate the assignment lifecycle unless the game-server plugin starts storing
connection attempts as first-class records.
