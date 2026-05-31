//! Shared error and result types for provider-agnostic matchmaker code.

use thiserror::Error;

/// Result type used by provider-agnostic matchmaker APIs.
pub type Result<T> = std::result::Result<T, MatchmakerError>;

#[derive(Debug, Error)]
/// Errors returned by matchmaker core and integration contracts.
pub enum MatchmakerError {
    /// The caller supplied an invalid request.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// No provider capacity is available for the request.
    #[error("no server capacity is available")]
    NoCapacity,
    /// A provider-specific operation failed.
    #[error("provider error: {0}")]
    Provider(String),
    /// Token generation or serialization failed.
    #[error("token error: {0}")]
    Token(String),
    /// Configuration is invalid or incomplete.
    #[error("configuration error: {0}")]
    Config(String),
    /// Transport or coordination failed.
    #[error("transport error: {0}")]
    Transport(String),
}
