//! Shared error and result types for provider-agnostic matchmaker code.

use serde::{Deserialize, Serialize};
use std::fmt;
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
    /// The matchmaker or selected game server is draining and rejecting new work.
    #[error("draining: {0}")]
    Draining(String),
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

impl MatchmakerError {
    /// Returns the wire-protocol error code for this error.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::InvalidRequest(_) => ErrorCode::InvalidRequest,
            Self::NoCapacity => ErrorCode::NoCapacity,
            Self::Draining(_) => ErrorCode::Draining,
            Self::Provider(_) => ErrorCode::ProviderError,
            Self::Token(_) => ErrorCode::TokenError,
            Self::Config(_) => ErrorCode::ConfigError,
            Self::Transport(_) => ErrorCode::TransportError,
        }
    }

    /// Returns whether clients should consider retrying this error.
    pub fn retryable(&self) -> bool {
        self.code().retryable()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Stable websocket protocol error code.
pub enum ErrorCode {
    /// Incoming text frame could not be decoded as a client message.
    InvalidJson,
    /// Request is structurally valid JSON but invalid for the current state.
    InvalidRequest,
    /// Client requested a websocket protocol version this server does not support.
    UnsupportedProtocolVersion,
    /// No provider/server capacity is available.
    NoCapacity,
    /// The matchmaker or selected game server is draining.
    Draining,
    /// Provider allocation, readiness, or release failed.
    ProviderError,
    /// Lightyear connection grant generation failed.
    TokenError,
    /// Runtime configuration is invalid or incomplete.
    ConfigError,
    /// WebSocket, NATS, or other coordination transport failed.
    TransportError,
}

impl ErrorCode {
    /// Returns the stable snake-case wire value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid_json",
            Self::InvalidRequest => "invalid_request",
            Self::UnsupportedProtocolVersion => "unsupported_protocol_version",
            Self::NoCapacity => "no_capacity",
            Self::Draining => "draining",
            Self::ProviderError => "provider_error",
            Self::TokenError => "token_error",
            Self::ConfigError => "config_error",
            Self::TransportError => "transport_error",
        }
    }

    /// Returns whether clients should consider retrying this class of error.
    ///
    /// Retryability is intentionally conservative. `provider_error` and
    /// `transport_error` can be transient; clients should still use backoff and
    /// surface repeated failures to the user.
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::NoCapacity | Self::Draining | Self::ProviderError | Self::TransportError
        )
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
