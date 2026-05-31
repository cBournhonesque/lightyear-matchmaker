//! Facade crate for the common Lightyear Matchmaker APIs and optional integrations.
//!
//! Most domain types are re-exported from `lightyear_matchmaker_core`. Optional
//! integration crates are exposed behind Cargo features so applications can opt
//! into only the runtime surfaces they need.

pub use lightyear_matchmaker_core::*;

#[cfg(feature = "bevy-client")]
/// Bevy client helpers for requesting a matchmaker assignment.
pub mod bevy_client {
    pub use lightyear_matchmaker_bevy_client::*;
}

#[cfg(feature = "bevy-server")]
/// Bevy game-server plugin and validation state for matchmaker assignments.
pub mod bevy_server {
    pub use lightyear_matchmaker_bevy_server::*;
}

#[cfg(feature = "lightyear-netcode")]
/// Lightyear Netcode token issuing integration.
pub mod lightyear {
    pub use lightyear_matchmaker_lightyear::*;
}

#[cfg(feature = "nats")]
/// NATS JetStream coordination backend.
pub mod nats {
    pub use lightyear_matchmaker_nats::*;
}

#[cfg(feature = "static-provider")]
/// Static server provider implementation.
pub mod provider_static {
    pub use lightyear_matchmaker_provider_static::*;
}

#[cfg(feature = "provider-edgegap")]
/// Edgegap provider bridge implementation.
pub mod provider_edgegap {
    pub use lightyear_matchmaker_provider_edgegap::*;
}

#[cfg(feature = "server")]
/// Deployable Axum matchmaker server.
pub mod server {
    pub use lightyear_matchmaker_server::*;
}
