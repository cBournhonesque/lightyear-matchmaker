//! Provider-agnostic domain model for Lightyear Matchmaker.
//!
//! Lightyear Matchmaker coordinates the work around a Lightyear game session:
//! accepting client requests, choosing or creating server capacity, asking the
//! selected game server to prepare an assignment, and returning connection
//! material such as a Lightyear Netcode `ConnectToken`.
//!
//! The model separates three runtime roles:
//!
//! - The **matchmaker** is the client-facing coordinator. It owns the websocket
//!   protocol, lightweight identity resolution, lobby state, allocation
//!   requests, assignment ids, retry policy, cleanup lifecycle, and token
//!   issuing.
//! - A **game server** is the running authoritative game process. It publishes
//!   readiness and capacity, receives assignments keyed by its server id,
//!   prepares local admission state, validates connecting Lightyear client ids,
//!   and reports active/disconnected clients.
//! - A **provider** owns capacity outside the matchmaker. Static providers pick
//!   from already-running servers. Dynamic providers such as Edgegap can create,
//!   poll, reuse, or release provider-side sessions/deployments.
//!
//! The normal assignment flow is:
//!
//! 1. A client sends a play request, or a lobby becomes ready.
//! 2. The matchmaker creates a `request_id` for that request.
//! 3. A provider returns an `allocation_id`, endpoint, and selected server id.
//! 4. The matchmaker writes one `assignment_id` per assigned player/client.
//! 5. The game server prepares the assignment and acknowledges it.
//! 6. The matchmaker returns a `ConnectionGrant` to the client.
//! 7. The game server reports active connection changes after the client
//!    connects or disconnects.
//!
//! `request_id`, `allocation_id`, and `assignment_id` are intentionally
//! different concepts. A request id is the matchmaker's correlation boundary for
//! one client or lobby request. An allocation id is provider-owned capacity or
//! session identity and may be shared by several players in one lobby. An
//! assignment id is matchmaker-owned and authorizes one Lightyear client id on
//! one selected game server.
//!
//! Capacity reports are not reservations. A future reservation layer may add a
//! matchmaker-owned capacity hold before assignment persistence; the current
//! model is designed to run with one matchmaker and treat server-published
//! capacity as the source of placement information.
//! Server drains are separate operator/matchmaker intent: a drained game server
//! can keep reporting capacity, but providers and coordinators should stop
//! placing new assignments there and cancel pending assignment work.
//!
//! The crate also exposes formal lifecycle enums such as `TicketState`,
//! `MatchState`, `AllocationState`, `ReservationState`, and `AssignmentState`.
//! These make the state machines explicit for docs, metrics, debug endpoints,
//! and future ticket-matching integrations.
//!
//! This crate intentionally avoids Axum, NATS, Bevy, Edgegap, and concrete
//! Lightyear transport dependencies. It defines the portable IDs, protocol
//! messages, provider traits, lobby data, assignment records, and token issuing
//! contracts used by the integration crates.

#![allow(async_fn_in_trait)]

mod error;
mod game_server;
mod identity;
mod lobby;
mod protocol;
mod provider;
mod state;
mod token;

pub use error::{ErrorCode, MatchmakerError, Result};
pub use game_server::*;
pub use identity::*;
pub use lobby::*;
pub use protocol::*;
pub use provider::*;
pub use state::*;
pub use token::*;
