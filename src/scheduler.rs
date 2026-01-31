use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime};

use crate::aggregation::PendingSet;
use crate::config::ProjectConfig;
use crate::errors::{ErrorClass, SyncError};
use crate::paths::NormalizedPath;
use crate::state::ProjectState;
use crate::state_updates::StateUpdate;
use crate::sync::{SyncMode, SyncRequest, SyncRunResult, SyncRunner};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Copy)]
pub struct BackoffPolicy {
    pub base_ms: u64,
    pub max_ms: u64,
}

impl BackoffPolicy {
    pub fn next_delay_ms(&self, attempt: u32) -> u64 {
        let exp = 1u64 << attempt.min(31);
        let delay = self.base_ms.saturating_mul(exp);
        delay.min(self.max_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleOutcome {
    Ran,
    SkippedBackoff,
}

#[derive(Debug, Clone)]
pub struct ScheduleResult {
    pub outcome: ScheduleOutcome,
    pub run: Option<SyncRunResult>,
    pub backoff_until: Option<SystemTime>,
}

pub trait StateSink: Send + Sync {
    fn set_state(&self, project: &str, state: ProjectState);
    fn set_pending_count(&self, project: &str, pending_count: u64);
    fn set_last_change(&self, project: &str, at: SystemTime);
    fn set_last_error(&self, project: &str, error: Option<SyncError>);
    fn set_last_sync_start(&self, project: &str, at: SystemTime);
    fn set_last_sync_end(&self, project: &str, at: SystemTime);
    fn set_last_success(&self, project: &str, at: SystemTime);
    fn set_backoff_until(&self, project: &str, until: Option<SystemTime>);
}

pub struct ChannelStateSink {
    sender: UnboundedSender<StateUpdate>,
}

impl ChannelStateSink {
    pub fn new(sender: UnboundedSender<StateUpdate>) -> Self {
        Self { sender }
    }
}

impl StateSink for ChannelStateSink {
    fn set_state(&self, project: &str, state: ProjectState) {
        let _ = self.sender.send(StateUpdate::SetState {
            project: project.to_string(),
            state,
        });
    }

    fn set_pending_count(&self, project: &str, pending_count: u64) {
        let _ = self.sender.send(StateUpdate::SetPendingCount {
            project: project.to_string(),
            pending_count,
        });
    }

    fn set_last_change(&self, project: &str, at: SystemTime) {
        let _ = self.sender.send(StateUpdate::SetLastChange {
            project: project.to_string(),
            at,
        });
    }

    fn set_last_error(&self, project: &str, error: Option<SyncError>) {
        let _ = self.sender.send(StateUpdate::SetLastError {
            project: project.to_string(),
            error: error.map(|value| value.to_string()),
        });
    }

    fn set_last_sync_start(&self, project: &str, at: SystemTime) {
        let _ = self.sender.send(StateUpdate::SetLastSyncStart {
            project: project.to_string(),
            at,
        });
    }

    fn set_last_sync_end(&self, project: &str, at: SystemTime) {
        let _ = self.sender.send(StateUpdate::SetLastSyncEnd {
            project: project.to_string(),
            at,
        });
    }

    fn set_last_success(&self, project: &str, at: SystemTime) {
        let _ = self.sender.send(StateUpdate::SetLastSuccess {
            project: project.to_string(),
            at,
        });
    }

    fn set_backoff_until(&self, project: &str, until: Option<SystemTime>) {
        let _ = self.sender.send(StateUpdate::SetBackoffUntil {
            project: project.to_string(),
            until,
        });
    }
}

pub trait Runner: Send + Sync {
    fn run(&self, request: &SyncRequest) -> std::io::Result<SyncRunResult>;
}

impl Runner for SyncRunner {
    fn run(&self, request: &SyncRequest) -> std::io::Result<SyncRunResult> {
        SyncRunner::run(self, request)
    }
}

#[derive(Default)]
pub struct NoopStateSink;

impl StateSink for NoopStateSink {
    fn set_state(&self, _project: &str, _state: ProjectState) {}
    fn set_pending_count(&self, _project: &str, _pending_count: u64) {}
    fn set_last_change(&self, _project: &str, _at: SystemTime) {}
    fn set_last_error(&self, _project: &str, _error: Option<SyncError>) {}
    fn set_last_sync_start(&self, _project: &str, _at: SystemTime) {}
    fn set_last_sync_end(&self, _project: &str, _at: SystemTime) {}
    fn set_last_success(&self, _project: &str, _at: SystemTime) {}
    fn set_backoff_until(&self, _project: &str, _until: Option<SystemTime>) {}
}

struct BackoffState {
    attempt: u32,
    until: Option<SystemTime>,
}

pub struct SyncGate {
    inner: Mutex<GateState>,
    cvar: Condvar,
}

struct GateState {
    max_parallel: usize,
    active_total: usize,
    active_projects: HashSet<String>,
}

impl SyncGate {
    pub fn new(max_parallel: usize) -> Self {
        Self {
            inner: Mutex::new(GateState {
                max_parallel,
                active_total: 0,
                active_projects: HashSet::new(),
            }),
            cvar: Condvar::new(),
        }
    }

    pub fn acquire(&self, project: &str) -> SyncPermit<'_> {
        let mut guard = self.inner.lock().expect("sync gate poisoned");
        while guard.active_total >= guard.max_parallel || guard.active_projects.contains(project) {
            guard = self.cvar.wait(guard).expect("sync gate poisoned");
        }

        guard.active_total += 1;
        guard.active_projects.insert(project.to_string());
        SyncPermit {
            gate: self,
            project: project.to_string(),
        }
    }
}

pub struct SyncPermit<'a> {
    gate: &'a SyncGate,
    project: String,
}

impl<'a> Drop for SyncPermit<'a> {
    fn drop(&mut self) {
        let mut guard = self.gate.inner.lock().expect("sync gate poisoned");
        guard.active_total = guard.active_total.saturating_sub(1);
        guard.active_projects.remove(&self.project);
        self.gate.cvar.notify_all();
    }
}

pub struct Scheduler<R: Runner> {
    gate: Arc<SyncGate>,
    runner: R,
    backoff: BackoffPolicy,
    state: Mutex<HashMap<String, BackoffState>>,
}

impl<R: Runner> Scheduler<R> {
    pub fn new(max_parallel: usize, runner: R, backoff: BackoffPolicy) -> Self {
        Self {
            gate: Arc::new(SyncGate::new(max_parallel)),
            runner,
            backoff,
            state: Mutex::new(HashMap::new()),
        }
    }

    pub fn run_once<S: StateSink>(
        &self,
        request: &SyncRequest,
        pending_after: bool,
        sink: &S,
    ) -> std::io::Result<ScheduleResult> {
        let now = SystemTime::now();
        let backoff_until = self.current_backoff_until(&request.project);
        if let Some(until) = backoff_until {
            if now < until {
                sink.set_state(&request.project, ProjectState::Backoff);
                sink.set_backoff_until(&request.project, Some(until));
                return Ok(ScheduleResult {
                    outcome: ScheduleOutcome::SkippedBackoff,
                    run: None,
                    backoff_until: Some(until),
                });
            }
            self.clear_backoff(&request.project);
        }

        let _permit = self.gate.acquire(&request.project);
        let start = SystemTime::now();
        sink.set_state(&request.project, ProjectState::Syncing);
        sink.set_last_sync_start(&request.project, start);

        let run = self.runner.run(request)?;
        let end = SystemTime::now();
        sink.set_last_sync_end(&request.project, end);

        match &run.outcome {
            crate::sync::SyncOutcome::Success => {
                self.clear_backoff(&request.project);
                sink.set_last_error(&request.project, None);
                sink.set_backoff_until(&request.project, None);
                sink.set_last_success(&request.project, end);
                let next = if pending_after {
                    ProjectState::Pending
                } else {
                    ProjectState::Idle
                };
                sink.set_state(&request.project, next);
            }
            crate::sync::SyncOutcome::Noop => {
                self.clear_backoff(&request.project);
                sink.set_last_error(&request.project, None);
                sink.set_backoff_until(&request.project, None);
                let next = if pending_after {
                    ProjectState::Pending
                } else {
                    ProjectState::Idle
                };
                sink.set_state(&request.project, next);
            }
            crate::sync::SyncOutcome::Failed { error, .. } => match error.class {
                ErrorClass::Transient => {
                    let delay = self.bump_backoff(&request.project);
                    let until = SystemTime::now() + Duration::from_millis(delay);
                    sink.set_last_error(&request.project, Some(error.clone()));
                    sink.set_backoff_until(&request.project, Some(until));
                    sink.set_state(&request.project, ProjectState::Backoff);
                }
                ErrorClass::Hard => {
                    self.clear_backoff(&request.project);
                    sink.set_last_error(&request.project, Some(error.clone()));
                    sink.set_backoff_until(&request.project, None);
                    sink.set_state(&request.project, ProjectState::Error);
                }
            },
        }

        let backoff_until = self.current_backoff_until(&request.project);

        Ok(ScheduleResult {
            outcome: ScheduleOutcome::Ran,
            run: Some(run),
            backoff_until,
        })
    }

    pub fn run_pending<S: StateSink>(
        &self,
        config: &ProjectConfig,
        pending: &mut PendingSync,
        pending_after: bool,
        sink: &S,
    ) -> std::io::Result<Option<ScheduleResult>> {
        let now = SystemTime::now();
        if let Some(until) = self.current_backoff_until(&config.name)
            && now < until
        {
            sink.set_state(&config.name, ProjectState::Backoff);
            sink.set_backoff_until(&config.name, Some(until));
            return Ok(Some(ScheduleResult {
                outcome: ScheduleOutcome::SkippedBackoff,
                run: None,
                backoff_until: Some(until),
            }));
        }

        let mode = match pending.take_next_mode() {
            Some(mode) => mode,
            None => return Ok(None),
        };

        let request = build_request_from_config(config, mode);
        self.run_once(&request, pending_after, sink).map(Some)
    }

    fn current_backoff_until(&self, project: &str) -> Option<SystemTime> {
        let guard = self.state.lock().expect("scheduler state poisoned");
        guard.get(project).and_then(|state| state.until)
    }

    fn bump_backoff(&self, project: &str) -> u64 {
        let mut guard = self.state.lock().expect("scheduler state poisoned");
        let entry = guard.entry(project.to_string()).or_insert(BackoffState {
            attempt: 0,
            until: None,
        });
        let delay = self.backoff.next_delay_ms(entry.attempt);
        entry.attempt = entry.attempt.saturating_add(1);
        entry.until = Some(SystemTime::now() + Duration::from_millis(delay));
        delay
    }

    fn clear_backoff(&self, project: &str) {
        let mut guard = self.state.lock().expect("scheduler state poisoned");
        guard.remove(project);
    }
}

#[derive(Debug)]
pub struct PendingSync {
    pending: PendingSet,
    initial_full_sync: bool,
}

impl PendingSync {
    pub fn new() -> Self {
        Self {
            pending: PendingSet::default(),
            initial_full_sync: true,
        }
    }

    pub fn queue_initial_full(&mut self) {
        self.initial_full_sync = true;
    }

    pub fn enqueue(&mut self, path: NormalizedPath) {
        self.pending.insert(path);
    }

    pub fn enqueue_all<I>(&mut self, paths: I)
    where
        I: IntoIterator<Item = NormalizedPath>,
    {
        for path in paths {
            self.pending.insert(path);
        }
    }

    pub fn has_work(&self) -> bool {
        self.initial_full_sync || !self.pending.is_empty()
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn take_next_mode(&mut self) -> Option<SyncMode> {
        if self.initial_full_sync {
            self.initial_full_sync = false;
            self.pending.take_all();
            return Some(SyncMode::Full);
        }

        if self.pending.is_empty() {
            return None;
        }

        let relative_paths = self
            .pending
            .take_all()
            .into_iter()
            .map(|path| PathBuf::from(path.as_str()))
            .collect();
        Some(SyncMode::Files { relative_paths })
    }
}

impl Default for PendingSync {
    fn default() -> Self {
        Self::new()
    }
}

fn build_request_from_config(config: &ProjectConfig, mode: SyncMode) -> SyncRequest {
    SyncRequest {
        project: config.name.clone(),
        local_root: PathBuf::from(&config.local_root),
        remote_user: config.remote_user.clone(),
        remote_host: config.remote_host.clone(),
        remote_port: config.remote_port,
        remote_root: PathBuf::from(&config.remote_root),
        ssh_identity: config.ssh_identity.as_ref().map(PathBuf::from),
        mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{SyncOutcome, SyncRunResult};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestSink {
        last_state: Mutex<Option<ProjectState>>,
        last_error: Mutex<Option<SyncError>>,
        last_backoff: Mutex<Option<SystemTime>>,
    }

    impl TestSink {
        fn new() -> Self {
            Self {
                last_state: Mutex::new(None),
                last_error: Mutex::new(None),
                last_backoff: Mutex::new(None),
            }
        }
    }

    impl StateSink for TestSink {
        fn set_state(&self, _project: &str, state: ProjectState) {
            *self.last_state.lock().unwrap() = Some(state);
        }

        fn set_pending_count(&self, _project: &str, _pending_count: u64) {}
        fn set_last_change(&self, _project: &str, _at: SystemTime) {}
        fn set_last_error(&self, _project: &str, error: Option<SyncError>) {
            *self.last_error.lock().unwrap() = error;
        }

        fn set_last_sync_start(&self, _project: &str, _at: SystemTime) {}
        fn set_last_sync_end(&self, _project: &str, _at: SystemTime) {}
        fn set_last_success(&self, _project: &str, _at: SystemTime) {}

        fn set_backoff_until(&self, _project: &str, until: Option<SystemTime>) {
            *self.last_backoff.lock().unwrap() = until;
        }
    }

    struct FakeRunner {
        calls: AtomicUsize,
        outcomes: Mutex<Vec<SyncOutcome>>,
    }

    impl FakeRunner {
        fn new(outcomes: Vec<SyncOutcome>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcomes: Mutex::new(outcomes),
            }
        }

        fn run(&self, _request: &SyncRequest) -> std::io::Result<SyncRunResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut guard = self.outcomes.lock().unwrap();
            let outcome = if guard.is_empty() {
                SyncOutcome::Success
            } else {
                guard.remove(0)
            };

            Ok(SyncRunResult {
                outcome,
                command: None,
                stderr: None,
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl Runner for FakeRunner {
        fn run(&self, request: &SyncRequest) -> std::io::Result<SyncRunResult> {
            FakeRunner::run(self, request)
        }
    }

    #[test]
    fn backoff_policy_caps() {
        let policy = BackoffPolicy {
            base_ms: 1000,
            max_ms: 3000,
        };
        assert_eq!(policy.next_delay_ms(0), 1000);
        assert_eq!(policy.next_delay_ms(1), 2000);
        assert_eq!(policy.next_delay_ms(2), 3000);
        assert_eq!(policy.next_delay_ms(3), 3000);
    }

    #[test]
    fn scheduler_sets_backoff_on_transient() {
        let runner = FakeRunner::new(vec![SyncOutcome::Failed {
            error: SyncError::new(ErrorClass::Transient, "timeout"),
            exit_code: Some(255),
        }]);
        let scheduler = Scheduler::new(
            1,
            runner,
            BackoffPolicy {
                base_ms: 1000,
                max_ms: 5000,
            },
        );
        let request = SyncRequest {
            project: "proj".to_string(),
            local_root: "/tmp/local".into(),
            remote_user: "user".to_string(),
            remote_host: "host".to_string(),
            remote_port: None,
            remote_root: "/remote".into(),
            ssh_identity: None,
            mode: crate::sync::SyncMode::Full,
        };
        let sink = TestSink::new();

        let result = scheduler.run_once(&request, false, &sink).unwrap();
        assert_eq!(result.outcome, ScheduleOutcome::Ran);
        assert_eq!(
            *sink.last_state.lock().unwrap(),
            Some(ProjectState::Backoff)
        );
        assert!(sink.last_backoff.lock().unwrap().is_some());
        assert!(sink.last_error.lock().unwrap().is_some());
    }

    #[test]
    fn scheduler_clears_backoff_on_success() {
        let runner = FakeRunner::new(vec![
            SyncOutcome::Failed {
                error: SyncError::new(ErrorClass::Transient, "timeout"),
                exit_code: Some(255),
            },
            SyncOutcome::Success,
        ]);
        let scheduler = Scheduler::new(
            1,
            runner,
            BackoffPolicy {
                base_ms: 0,
                max_ms: 0,
            },
        );
        let request = SyncRequest {
            project: "proj".to_string(),
            local_root: "/tmp/local".into(),
            remote_user: "user".to_string(),
            remote_host: "host".to_string(),
            remote_port: None,
            remote_root: "/remote".into(),
            ssh_identity: None,
            mode: crate::sync::SyncMode::Full,
        };
        let sink = TestSink::new();

        let _ = scheduler.run_once(&request, false, &sink).unwrap();
        let _ = scheduler.run_once(&request, false, &sink).unwrap();

        assert_eq!(*sink.last_state.lock().unwrap(), Some(ProjectState::Idle));
        assert!(sink.last_backoff.lock().unwrap().is_none());
        assert!(sink.last_error.lock().unwrap().is_none());
    }

    #[test]
    fn scheduler_skips_while_in_backoff() {
        let runner = FakeRunner::new(vec![SyncOutcome::Failed {
            error: SyncError::new(ErrorClass::Transient, "timeout"),
            exit_code: Some(255),
        }]);
        let scheduler = Scheduler::new(
            1,
            runner,
            BackoffPolicy {
                base_ms: 5_000,
                max_ms: 5_000,
            },
        );
        let request = SyncRequest {
            project: "proj".to_string(),
            local_root: "/tmp/local".into(),
            remote_user: "user".to_string(),
            remote_host: "host".to_string(),
            remote_port: None,
            remote_root: "/remote".into(),
            ssh_identity: None,
            mode: crate::sync::SyncMode::Full,
        };
        let sink = TestSink::new();

        let _ = scheduler.run_once(&request, false, &sink).unwrap();
        let result = scheduler.run_once(&request, false, &sink).unwrap();

        assert_eq!(result.outcome, ScheduleOutcome::SkippedBackoff);
        assert_eq!(scheduler.runner.calls(), 1);
    }

    #[test]
    fn pending_sync_runs_initial_full_then_files() {
        let mut pending = PendingSync::new();
        let initial = pending.take_next_mode();
        assert!(matches!(initial, Some(SyncMode::Full)));

        pending.enqueue(NormalizedPath::new("src/lib.rs".to_string()));
        let next = pending.take_next_mode();
        match next {
            Some(SyncMode::Files { relative_paths }) => {
                assert_eq!(relative_paths, vec![PathBuf::from("src/lib.rs")]);
            }
            other => panic!("expected files mode, got {:?}", other),
        }
    }
}
