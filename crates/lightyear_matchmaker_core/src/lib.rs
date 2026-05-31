//! Provider-agnostic domain model for Lightyear Matchmaker.
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
mod token;

pub use error::{MatchmakerError, Result};
pub use game_server::*;
pub use identity::*;
pub use lobby::*;
pub use protocol::*;
pub use provider::*;
pub use token::*;
