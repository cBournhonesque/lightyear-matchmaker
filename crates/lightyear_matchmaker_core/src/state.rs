//! Formal lifecycle states for current matchmaker runtime objects.
//!
//! These states make the implicit runtime model explicit without forcing a
//! particular storage backend. The deployable server can keep using in-memory
//! and NATS state today, while protocol docs, metrics, and debug endpoints can
//! use the same vocabulary.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state for lobby membership.
///
/// This state intentionally does not describe readiness or assignment progress.
/// A lobby can remain open while one or more roster subsets are being assigned
/// elsewhere. Fullness and readiness are derived from the lobby members, while
/// per-client assignment progress is represented by [`AssignmentState`].
pub enum LobbyState {
    /// Lobby exists and can accept members or readiness changes.
    Open,
    /// Lobby is closed and should not accept further mutations.
    Closed,
}

impl LobbyState {
    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed)
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next || matches!((self, next), (Self::Open, Self::Closed))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state for provider allocation.
pub enum AllocationState {
    /// Allocation has been requested from a provider.
    Requested,
    /// Provider returned capacity, endpoint, and server identity.
    Allocated,
    /// Provider-side capacity/session was released.
    Released,
    /// Allocation failed.
    Failed,
}

impl AllocationState {
    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Released | Self::Failed)
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Requested, Self::Allocated | Self::Failed)
                    | (Self::Allocated, Self::Released | Self::Failed)
            )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state for one game-server assignment.
pub enum AssignmentState {
    /// Assignment object was created in matchmaker memory.
    Created,
    /// Assignment was persisted for game-server consumption.
    Persisted,
    /// Game server has been asked to prepare local admission state.
    Preparing,
    /// Game server acknowledged that local admission state is ready.
    Prepared,
    /// Client has received connection material for the prepared assignment.
    Ready,
    /// Game server reported the Lightyear client id as connected.
    Connected,
    /// Game server reported the Lightyear client id as disconnected.
    Disconnected,
    /// Game server rejected the assignment during preparation.
    Rejected,
    /// Assignment timed out before the client connected.
    TimedOut,
    /// Assignment failed because of a matchmaker/provider/coordination error.
    Failed,
    /// Assignment coordination state was explicitly deleted.
    Deleted,
}

impl AssignmentState {
    /// Returns the stable snake_case label used in logs, metrics, and docs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Persisted => "persisted",
            Self::Preparing => "preparing",
            Self::Prepared => "prepared",
            Self::Ready => "ready",
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
            Self::Rejected => "rejected",
            Self::TimedOut => "timed_out",
            Self::Failed => "failed",
            Self::Deleted => "deleted",
        }
    }

    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Disconnected | Self::Rejected | Self::TimedOut | Self::Failed | Self::Deleted
        )
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        if next == Self::Deleted && !self.is_terminal() {
            return true;
        }
        matches!(
            (self, next),
            (
                Self::Created,
                Self::Persisted | Self::Preparing | Self::Failed
            ) | (
                Self::Persisted,
                Self::Preparing | Self::Prepared | Self::Rejected | Self::TimedOut | Self::Failed
            ) | (
                Self::Preparing,
                Self::Prepared | Self::Rejected | Self::TimedOut | Self::Failed
            ) | (
                Self::Prepared,
                Self::Ready | Self::Connected | Self::TimedOut | Self::Failed
            ) | (Self::Ready, Self::Connected | Self::TimedOut | Self::Failed)
                | (Self::Connected, Self::Disconnected)
        )
    }

    /// Returns `Some(next)` when moving to `next` is legal, otherwise `None`.
    #[must_use]
    pub fn transition_to(self, next: Self) -> Option<Self> {
        self.can_transition_to(next).then_some(next)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state for durable cleanup work.
pub enum LifecycleWorkState {
    /// Work was queued durably.
    Queued,
    /// A worker is currently processing the item.
    Processing,
    /// Work succeeded and was acknowledged.
    Succeeded,
    /// Work failed and is waiting for redelivery.
    Retrying,
    /// Work exhausted delivery attempts and needs operator attention.
    DeadLettered,
    /// Work was invalid or intentionally discarded.
    Dropped,
}

impl LifecycleWorkState {
    /// Returns the stable snake_case label used in logs, metrics, and docs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Processing => "processing",
            Self::Succeeded => "succeeded",
            Self::Retrying => "retrying",
            Self::DeadLettered => "dead_lettered",
            Self::Dropped => "dropped",
        }
    }

    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::DeadLettered | Self::Dropped)
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Queued, Self::Processing | Self::Dropped)
                    | (
                        Self::Processing,
                        Self::Succeeded | Self::Retrying | Self::DeadLettered | Self::Dropped
                    )
                    | (
                        Self::Retrying,
                        Self::Processing | Self::DeadLettered | Self::Dropped
                    )
            )
    }

    /// Returns `Some(next)` when moving to `next` is legal, otherwise `None`.
    #[must_use]
    pub fn transition_to(self, next: Self) -> Option<Self> {
        self.can_transition_to(next).then_some(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lobby_state_only_tracks_membership_lifecycle() {
        assert!(LobbyState::Open.can_transition_to(LobbyState::Closed));
        assert!(!LobbyState::Closed.can_transition_to(LobbyState::Open));
        assert!(LobbyState::Closed.is_terminal());
    }

    #[test]
    fn allocation_state_has_one_active_provider_phase() {
        assert!(AllocationState::Requested.can_transition_to(AllocationState::Allocated));
        assert!(AllocationState::Allocated.can_transition_to(AllocationState::Released));
        assert!(AllocationState::Allocated.can_transition_to(AllocationState::Failed));
        assert!(!AllocationState::Released.can_transition_to(AllocationState::Allocated));
    }

    #[test]
    fn assignment_state_rejects_reuse_after_terminal_state() {
        assert!(AssignmentState::Created.can_transition_to(AssignmentState::Persisted));
        assert!(AssignmentState::Ready.can_transition_to(AssignmentState::Connected));
        assert!(AssignmentState::Connected.can_transition_to(AssignmentState::Disconnected));
        assert!(!AssignmentState::TimedOut.can_transition_to(AssignmentState::Ready));
        assert!(!AssignmentState::Disconnected.can_transition_to(AssignmentState::Connected));
        assert_eq!(
            AssignmentState::Created.transition_to(AssignmentState::Persisted),
            Some(AssignmentState::Persisted)
        );
        assert_eq!(
            AssignmentState::TimedOut.transition_to(AssignmentState::Ready),
            None
        );
        assert_eq!(AssignmentState::TimedOut.as_str(), "timed_out");
    }

    #[test]
    fn lifecycle_work_state_labels_and_transitions_are_stable() {
        assert_eq!(LifecycleWorkState::Queued.as_str(), "queued");
        assert_eq!(
            LifecycleWorkState::Queued.transition_to(LifecycleWorkState::Processing),
            Some(LifecycleWorkState::Processing)
        );
        assert_eq!(
            LifecycleWorkState::Succeeded.transition_to(LifecycleWorkState::Processing),
            None
        );
    }
}
