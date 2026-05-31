//! Lightyear Netcode token issuing integration.
//!
//! This crate converts provider allocations into serialized Lightyear Netcode
//! connect tokens while keeping the core matchmaker model free of direct
//! Lightyear dependencies.

#![allow(async_fn_in_trait)]

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use lightyear_matchmaker_core::{
    ConnectionGrant, ConnectionGrantKind, LightyearClientId, MatchmakerError, Result, TokenIssuer,
    TokenRequest,
};
use lightyear_netcode::{ConnectToken, PRIVATE_KEY_BYTES};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Configuration for Lightyear Netcode token issuing.
pub struct NetcodeTokenConfig {
    /// Lightyear protocol id embedded in generated tokens.
    pub protocol_id: u64,
    #[serde(default)]
    /// Private key encoded as hex or comma-separated bytes.
    pub private_key: String,
    #[serde(default = "default_client_timeout_secs")]
    /// Client timeout seconds embedded in generated tokens.
    pub client_timeout_secs: i32,
    #[serde(default = "default_token_expire_secs")]
    /// Token expiry seconds for generated tokens.
    pub token_expire_secs: i32,
}

#[derive(Clone, Debug)]
/// Token issuer for Lightyear Netcode connect tokens.
pub struct NetcodeTokenIssuer {
    protocol_id: u64,
    private_key: [u8; PRIVATE_KEY_BYTES],
    client_timeout_secs: i32,
    token_expire_secs: i32,
}

impl NetcodeTokenIssuer {
    /// Creates a token issuer with default token timeout and expiry settings.
    pub fn new(protocol_id: u64, private_key: [u8; PRIVATE_KEY_BYTES]) -> Self {
        Self {
            protocol_id,
            private_key,
            client_timeout_secs: default_client_timeout_secs(),
            token_expire_secs: default_token_expire_secs(),
        }
    }

    /// Creates a token issuer with explicit token timeout and expiry settings.
    pub fn with_timeouts(
        protocol_id: u64,
        private_key: [u8; PRIVATE_KEY_BYTES],
        client_timeout_secs: i32,
        token_expire_secs: i32,
    ) -> Self {
        Self {
            protocol_id,
            private_key,
            client_timeout_secs,
            token_expire_secs,
        }
    }

    /// Builds a token issuer from serialized configuration.
    pub fn from_config(config: NetcodeTokenConfig) -> Result<Self> {
        Ok(Self::with_timeouts(
            config.protocol_id,
            parse_private_key(&config.private_key)?,
            config.client_timeout_secs,
            config.token_expire_secs,
        ))
    }

    /// Returns the configured Lightyear protocol id.
    pub fn protocol_id(&self) -> u64 {
        self.protocol_id
    }

    /// Returns the configured client timeout in seconds.
    pub fn client_timeout_secs(&self) -> i32 {
        self.client_timeout_secs
    }

    /// Returns the configured token expiry in seconds.
    pub fn token_expire_secs(&self) -> i32 {
        self.token_expire_secs
    }
}

impl TokenIssuer for NetcodeTokenIssuer {
    async fn issue(&self, request: TokenRequest) -> Result<ConnectionGrant> {
        let client_id = request
            .client_id
            .unwrap_or_else(|| LightyearClientId::new(rand::random::<u64>()));
        let token = ConnectToken::build(
            request.allocation.endpoint.socket_addr(),
            self.protocol_id,
            client_id.0,
            self.private_key,
        )
        .timeout_seconds(self.client_timeout_secs)
        .expire_seconds(self.token_expire_secs)
        .generate()
        .map_err(|error| MatchmakerError::Token(format!("failed to generate token: {error:?}")))?;
        let token_bytes = token.try_into_bytes().map_err(|error| {
            MatchmakerError::Token(format!("failed to serialize token: {error}"))
        })?;
        Ok(ConnectionGrant {
            kind: ConnectionGrantKind::LightyearNetcode,
            client_id,
            endpoint: request.allocation.endpoint,
            token: BASE64_STANDARD.encode(token_bytes),
            cert_digest: request.allocation.cert_digest,
        })
    }
}

fn default_client_timeout_secs() -> i32 {
    15
}

fn default_token_expire_secs() -> i32 {
    30
}

/// Parses a Lightyear private key from empty, comma-separated, or hex input.
pub fn parse_private_key(value: &str) -> Result<[u8; PRIVATE_KEY_BYTES]> {
    let value = value.trim();
    if value.is_empty() {
        return Ok([0; PRIVATE_KEY_BYTES]);
    }
    if value.contains(',') {
        return parse_comma_private_key(value);
    }
    parse_hex_private_key(value)
}

fn parse_comma_private_key(value: &str) -> Result<[u8; PRIVATE_KEY_BYTES]> {
    let bytes = value
        .split(',')
        .map(|part| {
            part.trim().parse::<u8>().map_err(|error| {
                MatchmakerError::Config(format!("invalid private key byte: {error}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if bytes.len() != PRIVATE_KEY_BYTES {
        return Err(MatchmakerError::Config(format!(
            "private key must contain {PRIVATE_KEY_BYTES} bytes"
        )));
    }
    let mut key = [0; PRIVATE_KEY_BYTES];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn parse_hex_private_key(value: &str) -> Result<[u8; PRIVATE_KEY_BYTES]> {
    let normalized = value.trim().trim_start_matches("0x");
    if normalized.len() != PRIVATE_KEY_BYTES * 2 {
        return Err(MatchmakerError::Config(format!(
            "hex private key must contain {} characters",
            PRIVATE_KEY_BYTES * 2
        )));
    }
    let mut key = [0; PRIVATE_KEY_BYTES];
    for (index, chunk) in normalized.as_bytes().chunks_exact(2).enumerate() {
        let hex = std::str::from_utf8(chunk).map_err(|error| {
            MatchmakerError::Config(format!("invalid private key hex: {error}"))
        })?;
        key[index] = u8::from_str_radix(hex, 16).map_err(|error| {
            MatchmakerError::Config(format!("invalid private key hex: {error}"))
        })?;
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_key_as_dev_zero_key() {
        assert_eq!(parse_private_key("").unwrap(), [0; PRIVATE_KEY_BYTES]);
    }

    #[test]
    fn parses_comma_private_key() {
        let value = (0..PRIVATE_KEY_BYTES)
            .map(|_| "7")
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(parse_private_key(&value).unwrap(), [7; PRIVATE_KEY_BYTES]);
    }

    #[test]
    fn parses_hex_private_key() {
        let value = "2a".repeat(PRIVATE_KEY_BYTES);
        assert_eq!(parse_private_key(&value).unwrap(), [42; PRIVATE_KEY_BYTES]);
    }

    #[test]
    fn token_config_sets_timeouts() {
        let issuer = NetcodeTokenIssuer::from_config(NetcodeTokenConfig {
            protocol_id: 7,
            private_key: String::new(),
            client_timeout_secs: 5,
            token_expire_secs: 9,
        })
        .unwrap();

        assert_eq!(issuer.protocol_id(), 7);
        assert_eq!(issuer.client_timeout_secs(), 5);
        assert_eq!(issuer.token_expire_secs(), 9);
    }
}
