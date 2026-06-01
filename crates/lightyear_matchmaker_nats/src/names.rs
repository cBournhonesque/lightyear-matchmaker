//! Namespaced NATS bucket, key, and subject builders.

use lightyear_matchmaker_core::{AssignmentId, LightyearClientId, ServerId};

#[derive(Clone, Debug)]
/// Namespaced bucket and key builder for NATS coordination state.
pub struct NatsNames {
    namespace: Option<String>,
}

impl NatsNames {
    /// Creates a namespaced key builder.
    pub fn new(namespace: Option<String>) -> Self {
        Self {
            namespace: namespace
                .map(|namespace| sanitize_token(namespace.trim()))
                .filter(|namespace| !namespace.is_empty()),
        }
    }

    /// Returns a bucket name with the configured namespace applied.
    pub fn bucket(&self, base: &str) -> String {
        match &self.namespace {
            Some(namespace) => format!("{namespace}_{base}"),
            None => base.to_string(),
        }
    }

    /// Returns the KV key for a server id.
    pub fn key_server(&self, server_id: &ServerId) -> String {
        sanitize_token(&server_id.0)
    }

    /// Returns the KV key for a client id.
    pub fn key_client(&self, client_id: LightyearClientId) -> String {
        client_id.to_string()
    }

    /// Returns the KV key for an assignment id.
    pub fn key_assignment(&self, assignment_id: &AssignmentId) -> String {
        sanitize_token(&assignment_id.0)
    }

    /// Returns the KV key for a server/client active connection.
    pub fn key_connection(&self, server_id: &ServerId, client_id: LightyearClientId) -> String {
        format!(
            "{}.{}",
            self.key_server(server_id),
            self.key_client(client_id)
        )
    }

    /// Returns the subject used for all lifecycle work items.
    pub fn lifecycle_subject_all(&self) -> String {
        self.subject("lifecycle.>")
    }

    /// Returns the subject used for provider release lifecycle work.
    pub fn lifecycle_release_allocation_subject(&self) -> String {
        self.subject("lifecycle.release_allocation")
    }

    /// Returns the subject used for assignment deletion lifecycle work.
    pub fn lifecycle_delete_assignment_subject(&self) -> String {
        self.subject("lifecycle.delete_assignment")
    }

    fn subject(&self, base: &str) -> String {
        match &self.namespace {
            Some(namespace) => format!("{namespace}.{base}"),
            None => base.to_string(),
        }
    }
}

impl Default for NatsNames {
    fn default() -> Self {
        Self::new(None)
    }
}

pub(super) fn sanitize_token(value: &str) -> String {
    // NATS KV keys and stream subjects are easier to inspect when ids remain
    // recognizable, but arbitrary player/lobby ids may contain separators that
    // would change the key hierarchy. Preserve safe characters and flatten the
    // rest.
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
}
