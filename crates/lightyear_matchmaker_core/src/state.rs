//! Formal lifecycle states for matchmaker runtime objects.
//!
//! These states make the implicit runtime model explicit without forcing a
//! particular storage backend. The deployable server can keep using in-memory
//! and NATS state today, while protocol docs, metrics, debug endpoints, and
//! future Open Match 2 integration can use the same vocabulary.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state for an in-matchmaker lobby.
pub enum LobbyState {
    /// Lobby exists and can accept members or readiness changes.
    Open,
    /// Lobby reached its configured member limit but is not fully ready.
    Full,
    /// Required members are ready and the lobby can be assigned.
    Ready,
    /// The matchmaker is assigning the lobby roster to server capacity.
    Assigning,
    /// The lobby roster has received connection grants.
    Assigned,
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
        self == next
            || matches!(
                (self, next),
                (Self::Open, Self::Full | Self::Ready | Self::Closed)
                    | (Self::Full, Self::Open | Self::Ready | Self::Closed)
                    | (Self::Ready, Self::Open | Self::Assigning | Self::Closed)
                    | (Self::Assigning, Self::Open | Self::Assigned | Self::Closed)
                    | (Self::Assigned, Self::Closed)
            )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state for a matchmaking ticket.
///
/// This state model is intentionally compatible with Open Match 2's ticket
/// shape: tickets can be created inactive, activated into matchmaking pools,
/// returned by match functions, and then consumed by the matchmaker/director
/// after collision checks and server assignment.
pub enum TicketState {
    /// Ticket exists but is not eligible for matching yet.
    Created,
    /// Ticket is eligible to be returned by a matching function.
    Active,
    /// Ticket was included in a match returned by matching logic.
    Matched,
    /// Ticket was accepted by the director and will not be reused.
    Consumed,
    /// Ticket was cancelled by the client or owning lobby.
    Cancelled,
    /// Ticket expired before it was consumed.
    Expired,
    /// Ticket failed because of a backend or validation error.
    Failed,
}

impl TicketState {
    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Consumed | Self::Cancelled | Self::Expired | Self::Failed
        )
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Created,
                    Self::Active | Self::Cancelled | Self::Expired | Self::Failed
                ) | (
                    Self::Active,
                    Self::Matched | Self::Cancelled | Self::Expired | Self::Failed
                ) | (
                    Self::Matched,
                    Self::Consumed | Self::Cancelled | Self::Expired | Self::Failed
                )
            )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Director-side state for a match returned by matching logic.
///
/// Open Match 2 returns final matches from match functions, but it deliberately
/// leaves ticket collision handling and game-server assignment to the
/// matchmaker/director. This state tracks that director-owned phase.
pub enum MatchState {
    /// A match candidate was returned by matching logic.
    Proposed,
    /// The director accepted the match after collision checks.
    Accepted,
    /// The director rejected the match, usually because a ticket was consumed.
    Rejected,
    /// The director is allocating server capacity for this match.
    Assigning,
    /// Every accepted player in the match has received an assignment.
    Assigned,
    /// The match failed before assignment completed.
    Failed,
}

impl MatchState {
    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Rejected | Self::Assigned | Self::Failed)
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Proposed,
                    Self::Accepted | Self::Rejected | Self::Failed
                ) | (
                    Self::Accepted,
                    Self::Assigning | Self::Rejected | Self::Failed
                ) | (Self::Assigning, Self::Assigned | Self::Failed)
            )
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
    /// Allocation expired before it was used or released.
    Expired,
    /// Allocation failed.
    Failed,
}

impl AllocationState {
    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Released | Self::Expired | Self::Failed)
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Requested,
                    Self::Allocated | Self::Expired | Self::Failed
                ) | (
                    Self::Allocated,
                    Self::Released | Self::Expired | Self::Failed
                )
            )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state for a future matchmaker-owned capacity reservation.
///
/// Reservations are not implemented yet. This state exists to make the intended
/// model explicit: capacity reports are server-owned metadata, while
/// reservations are matchmaker-owned holds that prevent over-assignment.
pub enum ReservationState {
    /// The matchmaker is trying to create a capacity hold.
    Requested,
    /// Capacity is held for a pending assignment attempt.
    Held,
    /// The held capacity was consumed by assignment/connection state.
    Committed,
    /// The hold was released without being committed.
    Released,
    /// The hold expired before assignment completed.
    Expired,
    /// The hold failed.
    Failed,
}

impl ReservationState {
    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Committed | Self::Released | Self::Expired | Self::Failed
        )
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Requested, Self::Held | Self::Expired | Self::Failed)
                    | (
                        Self::Held,
                        Self::Committed | Self::Released | Self::Expired | Self::Failed
                    )
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
/// Lifecycle state for a Lightyear client connection attempt.
pub enum ConnectionState {
    /// Client has connection material but has not connected yet.
    Pending,
    /// Game server validation accepted the connection attempt.
    Accepted,
    /// Game server reported the client as connected.
    Connected,
    /// Game server reported the client as disconnected.
    Disconnected,
    /// Game server validation rejected the connection attempt.
    Rejected,
    /// The prepared assignment expired before a connection was observed.
    Expired,
}

impl ConnectionState {
    /// Returns whether the state is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Disconnected | Self::Rejected | Self::Expired)
    }

    /// Returns whether moving from this state to `next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Pending,
                    Self::Accepted | Self::Connected | Self::Rejected | Self::Expired
                ) | (
                    Self::Accepted,
                    Self::Connected | Self::Rejected | Self::Expired
                ) | (Self::Connected, Self::Disconnected)
            )
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

    #[test]
    fn ticket_state_matches_director_collision_flow() {
        assert!(TicketState::Created.can_transition_to(TicketState::Active));
        assert!(TicketState::Active.can_transition_to(TicketState::Matched));
        assert!(TicketState::Matched.can_transition_to(TicketState::Consumed));
        assert!(TicketState::Matched.can_transition_to(TicketState::Cancelled));
        assert!(!TicketState::Consumed.can_transition_to(TicketState::Active));
    }

    #[test]
    fn reservation_state_is_separate_from_allocation_state() {
        assert!(AllocationState::Requested.can_transition_to(AllocationState::Allocated));
        assert!(ReservationState::Requested.can_transition_to(ReservationState::Held));
        assert!(ReservationState::Held.can_transition_to(ReservationState::Committed));
        assert!(!ReservationState::Committed.can_transition_to(ReservationState::Held));
    }
}
