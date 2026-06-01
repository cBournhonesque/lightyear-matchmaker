//! Runtime metrics and small admin/status DTOs for the matchmaker server.

use crate::lobby::LobbyMetrics;
use lightyear_matchmaker_core::{AssignmentState, LifecycleWorkState, ServerId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone)]
pub(super) struct RuntimeMetrics {
    pub(super) inner: Arc<RuntimeMetricsInner>,
}

pub(super) struct RuntimeMetricsInner {
    pub(super) started_at: Instant,
    pub(super) websocket_sessions_opened: AtomicU64,
    pub(super) websocket_sessions_active: AtomicU64,
    pub(super) websocket_sessions_closed: AtomicU64,
    pub(super) request_play_started: AtomicU64,
    pub(super) request_play_failed: AtomicU64,
    pub(super) assignments_created: AtomicU64,
    pub(super) assignments_prepared: AtomicU64,
    pub(super) assignments_ready: AtomicU64,
    pub(super) assignment_prepare_retries: AtomicU64,
    pub(super) assignment_connection_retries: AtomicU64,
    pub(super) assignments_timed_out: AtomicU64,
    pub(super) lifecycle_jobs_received: AtomicU64,
    pub(super) lifecycle_jobs_succeeded: AtomicU64,
    pub(super) lifecycle_jobs_failed: AtomicU64,
    pub(super) lifecycle_jobs_dead_lettered: AtomicU64,
    pub(super) lifecycle_invalid_payloads: AtomicU64,
    pub(super) lifecycle_release_succeeded: AtomicU64,
    pub(super) lifecycle_delete_succeeded: AtomicU64,
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self {
            inner: Arc::new(RuntimeMetricsInner {
                started_at: Instant::now(),
                websocket_sessions_opened: AtomicU64::new(0),
                websocket_sessions_active: AtomicU64::new(0),
                websocket_sessions_closed: AtomicU64::new(0),
                request_play_started: AtomicU64::new(0),
                request_play_failed: AtomicU64::new(0),
                assignments_created: AtomicU64::new(0),
                assignments_prepared: AtomicU64::new(0),
                assignments_ready: AtomicU64::new(0),
                assignment_prepare_retries: AtomicU64::new(0),
                assignment_connection_retries: AtomicU64::new(0),
                assignments_timed_out: AtomicU64::new(0),
                lifecycle_jobs_received: AtomicU64::new(0),
                lifecycle_jobs_succeeded: AtomicU64::new(0),
                lifecycle_jobs_failed: AtomicU64::new(0),
                lifecycle_jobs_dead_lettered: AtomicU64::new(0),
                lifecycle_invalid_payloads: AtomicU64::new(0),
                lifecycle_release_succeeded: AtomicU64::new(0),
                lifecycle_delete_succeeded: AtomicU64::new(0),
            }),
        }
    }
}

impl RuntimeMetrics {
    pub(super) fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn dec(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_sub(1))
        });
    }

    fn read(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    pub(super) fn snapshot(
        &self,
        lobbies: LobbyMetrics,
        draining: bool,
        nats_configured: bool,
    ) -> MetricsSnapshot {
        let inner = &self.inner;
        let assignments_created = Self::read(&inner.assignments_created);
        let assignments_prepared = Self::read(&inner.assignments_prepared);
        let assignments_ready = Self::read(&inner.assignments_ready);
        let assignments_timed_out = Self::read(&inner.assignments_timed_out);
        let lifecycle_jobs_received = Self::read(&inner.lifecycle_jobs_received);
        let lifecycle_jobs_succeeded = Self::read(&inner.lifecycle_jobs_succeeded);
        let lifecycle_jobs_failed = Self::read(&inner.lifecycle_jobs_failed);
        let lifecycle_jobs_dead_lettered = Self::read(&inner.lifecycle_jobs_dead_lettered);

        let mut assignment_state_transitions = BTreeMap::new();
        assignment_state_transitions.insert(
            AssignmentState::Persisted.as_str().to_string(),
            assignments_created,
        );
        assignment_state_transitions.insert(
            AssignmentState::Prepared.as_str().to_string(),
            assignments_prepared,
        );
        assignment_state_transitions.insert(
            AssignmentState::Ready.as_str().to_string(),
            assignments_ready,
        );
        assignment_state_transitions.insert(
            AssignmentState::TimedOut.as_str().to_string(),
            assignments_timed_out,
        );

        let mut lifecycle_work_state_transitions = BTreeMap::new();
        lifecycle_work_state_transitions.insert(
            LifecycleWorkState::Processing.as_str().to_string(),
            lifecycle_jobs_received,
        );
        lifecycle_work_state_transitions.insert(
            LifecycleWorkState::Succeeded.as_str().to_string(),
            lifecycle_jobs_succeeded,
        );
        lifecycle_work_state_transitions.insert(
            LifecycleWorkState::Retrying.as_str().to_string(),
            lifecycle_jobs_failed.saturating_sub(lifecycle_jobs_dead_lettered),
        );
        lifecycle_work_state_transitions.insert(
            LifecycleWorkState::DeadLettered.as_str().to_string(),
            lifecycle_jobs_dead_lettered,
        );

        MetricsSnapshot {
            uptime_secs: inner.started_at.elapsed().as_secs(),
            draining,
            nats_configured,
            websocket_sessions_opened: Self::read(&inner.websocket_sessions_opened),
            websocket_sessions_active: Self::read(&inner.websocket_sessions_active),
            websocket_sessions_closed: Self::read(&inner.websocket_sessions_closed),
            request_play_started: Self::read(&inner.request_play_started),
            request_play_failed: Self::read(&inner.request_play_failed),
            assignments_created,
            assignments_prepared,
            assignments_ready,
            assignment_prepare_retries: Self::read(&inner.assignment_prepare_retries),
            assignment_connection_retries: Self::read(&inner.assignment_connection_retries),
            assignments_timed_out,
            assignment_state_transitions,
            lifecycle_jobs_received,
            lifecycle_jobs_succeeded,
            lifecycle_jobs_failed,
            lifecycle_jobs_dead_lettered,
            lifecycle_work_state_transitions,
            lifecycle_invalid_payloads: Self::read(&inner.lifecycle_invalid_payloads),
            lifecycle_release_succeeded: Self::read(&inner.lifecycle_release_succeeded),
            lifecycle_delete_succeeded: Self::read(&inner.lifecycle_delete_succeeded),
            lobbies,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct MetricsSnapshot {
    pub(super) uptime_secs: u64,
    pub(super) draining: bool,
    pub(super) nats_configured: bool,
    pub(super) websocket_sessions_opened: u64,
    pub(super) websocket_sessions_active: u64,
    pub(super) websocket_sessions_closed: u64,
    pub(super) request_play_started: u64,
    pub(super) request_play_failed: u64,
    pub(super) assignments_created: u64,
    pub(super) assignments_prepared: u64,
    pub(super) assignments_ready: u64,
    pub(super) assignment_prepare_retries: u64,
    pub(super) assignment_connection_retries: u64,
    pub(super) assignments_timed_out: u64,
    pub(super) assignment_state_transitions: BTreeMap<String, u64>,
    pub(super) lifecycle_jobs_received: u64,
    pub(super) lifecycle_jobs_succeeded: u64,
    pub(super) lifecycle_jobs_failed: u64,
    pub(super) lifecycle_jobs_dead_lettered: u64,
    pub(super) lifecycle_work_state_transitions: BTreeMap<String, u64>,
    pub(super) lifecycle_invalid_payloads: u64,
    pub(super) lifecycle_release_succeeded: u64,
    pub(super) lifecycle_delete_succeeded: u64,
    pub(super) lobbies: LobbyMetrics,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ReadinessStatus {
    pub(super) ready: bool,
    pub(super) draining: bool,
    pub(super) nats_configured: bool,
    pub(super) nats_ok: bool,
    pub(super) message: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(super) struct DrainRequest {
    pub(super) reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct GlobalDrainStatus {
    pub(super) draining: bool,
    pub(super) reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ServerDrainResponse {
    pub(super) server_id: ServerId,
    pub(super) draining: bool,
    pub(super) reason: Option<String>,
    pub(super) nats_configured: bool,
    pub(super) canceled_assignments: usize,
    pub(super) release_jobs_queued: usize,
}
