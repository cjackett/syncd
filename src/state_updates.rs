use std::time::SystemTime;

use crate::state::ProjectState;

#[derive(Debug, Clone)]
pub enum StateUpdate {
    SetState {
        project: String,
        state: ProjectState,
    },
    SetPendingCount {
        project: String,
        pending_count: u64,
    },
    SetLastChange {
        project: String,
        at: SystemTime,
    },
    SetLastError {
        project: String,
        error: Option<String>,
    },
    SetLastSyncStart {
        project: String,
        at: SystemTime,
    },
    SetLastSyncEnd {
        project: String,
        at: SystemTime,
    },
    SetLastSuccess {
        project: String,
        at: SystemTime,
    },
    SetBackoffUntil {
        project: String,
        until: Option<SystemTime>,
    },
}
