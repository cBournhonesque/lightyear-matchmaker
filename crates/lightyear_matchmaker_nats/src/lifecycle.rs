//! Durable lifecycle work payloads for cleanup workers.

use crate::NatsNames;
use lightyear_matchmaker_core::{
    AllocationId, AssignmentId, LightyearClientId, ProviderKind, RequestId, ServerId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Provider release request queued for asynchronous lifecycle cleanup.
pub struct ReleaseAllocationWork {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Matchmaker request id associated with the allocation, when known.
    pub request_id: Option<RequestId>,
    /// Provider allocation id to release.
    pub allocation_id: AllocationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Assignment id that triggered the release, when the release is assignment-scoped.
    pub assignment_id: Option<AssignmentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Server id associated with the allocation, when known.
    pub server_id: Option<ServerId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Provider that owns the allocation, when known.
    pub provider: Option<ProviderKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Human-readable reason for the release request.
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Assignment deletion request queued for asynchronous lifecycle cleanup.
pub struct DeleteAssignmentWork {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Matchmaker request id associated with the assignment, when known.
    pub request_id: Option<RequestId>,
    /// Assignment id to delete or invalidate.
    pub assignment_id: AssignmentId,
    /// Lightyear client id whose assignment index should be removed.
    pub client_id: LightyearClientId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Server id associated with the assignment, when known.
    pub server_id: Option<ServerId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Human-readable reason for the delete request.
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
/// Work item published to the NATS lifecycle WorkQueue stream.
pub enum LifecycleWork {
    /// Request to release provider-side capacity, session, or deployment state.
    ReleaseAllocation(ReleaseAllocationWork),
    /// Request to delete or invalidate a client assignment.
    DeleteAssignment(DeleteAssignmentWork),
}

impl LifecycleWork {
    /// Returns the stable work kind label used in logs and metrics.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ReleaseAllocation(_) => "release_allocation",
            Self::DeleteAssignment(_) => "delete_assignment",
        }
    }

    pub(super) fn subject(&self, names: &NatsNames) -> String {
        match self {
            Self::ReleaseAllocation(_) => names.lifecycle_release_allocation_subject(),
            Self::DeleteAssignment(_) => names.lifecycle_delete_assignment_subject(),
        }
    }
}
