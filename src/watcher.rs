use crate::aggregation::ChangeAggregator;
use crate::config::ProjectConfig;
use crate::ignore::IgnoreMatcher;
use crate::paths::{NormalizedPath, normalize_relative_path};
use crate::scheduler::{PendingSync, Runner, ScheduleOutcome, Scheduler, StateSink};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::{Duration, Instant, SystemTime};

#[derive(Debug, Clone)]
pub struct WatcherEvent {
    pub path: NormalizedPath,
    pub kind: EventKind,
}

#[derive(Debug)]
pub struct ProjectWatcher {
    root: PathBuf,
    _watcher: RecommendedWatcher,
}

pub struct ProjectWatchLoop<R: Runner, S: StateSink> {
    project: ProjectConfig,
    _watcher: ProjectWatcher,
    receiver: Receiver<WatcherEvent>,
    aggregator: ChangeAggregator,
    pending_sync: PendingSync,
    scheduler: Arc<Scheduler<R>>,
    sink: S,
    backoff_until: Option<SystemTime>,
    hard_error: bool,
}

impl<R: Runner, S: StateSink> ProjectWatchLoop<R, S> {
    pub fn new(
        project: ProjectConfig,
        ignore: IgnoreMatcher,
        debounce: Duration,
        scheduler: Arc<Scheduler<R>>,
        sink: S,
    ) -> notify::Result<Self> {
        let (sender, receiver) = channel();
        let watcher = ProjectWatcher::new(Path::new(&project.local_root), ignore.clone(), sender)?;
        let aggregator = ChangeAggregator::new(ignore, debounce);
        Ok(Self {
            project,
            _watcher: watcher,
            receiver,
            aggregator,
            pending_sync: PendingSync::new(),
            scheduler,
            sink,
            backoff_until: None,
            hard_error: false,
        })
    }

    pub fn run(&mut self) -> std::io::Result<()> {
        self.flush_if_ready()?;
        loop {
            self.tick()?;
        }
    }

    fn tick(&mut self) -> std::io::Result<()> {
        let now = Instant::now();
        let timeout = self.next_timeout(now);
        match timeout {
            Some(timeout) => match self.receiver.recv_timeout(timeout) {
                Ok(event) => self.handle_event(event),
                Err(RecvTimeoutError::Timeout) => self.flush_if_ready()?,
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "watcher channel disconnected",
                    ));
                }
            },
            None => match self.receiver.recv() {
                Ok(event) => self.handle_event(event),
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "watcher channel disconnected",
                    ));
                }
            },
        }

        Ok(())
    }

    fn handle_event(&mut self, event: WatcherEvent) {
        let now = Instant::now();
        if !self.aggregator.ingest(event.path, now) {
            return;
        }
        let change_time = SystemTime::now();
        self.sink
            .set_pending_count(&self.project.name, self.aggregator.pending_len() as u64);
        self.sink.set_last_change(&self.project.name, change_time);
        if !self.hard_error {
            let in_backoff = self
                .backoff_until
                .map(|until| SystemTime::now() < until)
                .unwrap_or(false);
            if !in_backoff {
                self.sink
                    .set_state(&self.project.name, crate::state::ProjectState::Pending);
            }
        }
    }

    fn flush_if_ready(&mut self) -> std::io::Result<()> {
        let now = Instant::now();
        if self.hard_error {
            return Ok(());
        }

        if let Some(until) = self.backoff_until
            && SystemTime::now() < until
        {
            return Ok(());
        }

        if !self.pending_sync.has_work() {
            if !self.aggregator.has_pending() || !self.aggregator.debounce_ready(now) {
                return Ok(());
            }

            let pending = self.aggregator.take_pending();
            self.sink.set_pending_count(&self.project.name, 0);
            self.pending_sync.enqueue_all(pending);
        }

        let result = match self.scheduler.run_pending(
            &self.project,
            &mut self.pending_sync,
            false,
            &self.sink,
        )? {
            Some(result) => result,
            None => return Ok(()),
        };
        self.backoff_until = result.backoff_until;
        if matches!(result.outcome, ScheduleOutcome::SkippedBackoff) {
            self.sink
                .set_pending_count(&self.project.name, self.pending_sync.pending_len() as u64);
            return Ok(());
        }
        if let Some(run) = result.run {
            match run.outcome {
                crate::sync::SyncOutcome::Success | crate::sync::SyncOutcome::Noop => {
                    self.hard_error = false;
                }
                crate::sync::SyncOutcome::Failed { error, .. } => {
                    if error.class == crate::errors::ErrorClass::Hard {
                        self.hard_error = true;
                    }
                }
            }
        }

        self.drain_events();

        if self.aggregator.has_pending() {
            self.sink
                .set_pending_count(&self.project.name, self.aggregator.pending_len() as u64);
            if matches!(result.outcome, ScheduleOutcome::Ran) && !self.hard_error {
                self.sink
                    .set_state(&self.project.name, crate::state::ProjectState::Pending);
            }
        }

        Ok(())
    }

    fn next_timeout(&self, now: Instant) -> Option<Duration> {
        let debounce = self.aggregator.debounce_remaining(now);
        let backoff = self.backoff_remaining();
        let has_work = self.pending_sync.has_work();

        match (debounce, backoff) {
            (Some(debounce), Some(backoff)) => Some(debounce.min(backoff)),
            (Some(debounce), None) => Some(debounce),
            (None, Some(backoff)) => Some(backoff),
            (None, None) => {
                if has_work {
                    Some(Duration::from_millis(0))
                } else {
                    None
                }
            }
        }
    }

    fn backoff_remaining(&self) -> Option<Duration> {
        let until = self.backoff_until?;
        until.duration_since(SystemTime::now()).ok()
    }

    fn drain_events(&mut self) {
        while let Ok(event) = self.receiver.try_recv() {
            self.handle_event(event);
        }
    }
}

impl ProjectWatcher {
    pub fn new(
        root: &Path,
        ignore: IgnoreMatcher,
        sender: Sender<WatcherEvent>,
    ) -> notify::Result<Self> {
        let root = root.to_path_buf();
        let root_clone = root.clone();
        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                for path in event.paths {
                    if let Some(rel) = normalize_relative_path(&root_clone, &path) {
                        if ignore.is_ignored(&rel) {
                            continue;
                        }
                        let _ = sender.send(WatcherEvent {
                            path: rel,
                            kind: event.kind,
                        });
                    }
                }
            }
        })?;
        watcher.watch(&root, RecursiveMode::Recursive)?;

        Ok(Self {
            root,
            _watcher: watcher,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProjectConfig;
    use crate::scheduler::{BackoffPolicy, Scheduler};
    use crate::state::ProjectState;
    use crate::sync::{SyncOutcome, SyncRunResult};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestSink {
        pending_count: Arc<Mutex<u64>>,
        last_state: Arc<Mutex<Option<ProjectState>>>,
        last_change: Arc<Mutex<Option<SystemTime>>>,
    }

    struct TestSinkHandles {
        pending_count: Arc<Mutex<u64>>,
        last_state: Arc<Mutex<Option<ProjectState>>>,
        last_change: Arc<Mutex<Option<SystemTime>>>,
    }

    impl TestSink {
        fn new() -> (Self, TestSinkHandles) {
            let pending_count = Arc::new(Mutex::new(0));
            let last_state = Arc::new(Mutex::new(None));
            let last_change = Arc::new(Mutex::new(None));
            (
                Self {
                    pending_count: Arc::clone(&pending_count),
                    last_state: Arc::clone(&last_state),
                    last_change: Arc::clone(&last_change),
                },
                TestSinkHandles {
                    pending_count,
                    last_state,
                    last_change,
                },
            )
        }
    }

    impl StateSink for TestSink {
        fn set_state(&self, _project: &str, state: ProjectState) {
            *self.last_state.lock().unwrap() = Some(state);
        }

        fn set_pending_count(&self, _project: &str, pending_count: u64) {
            *self.pending_count.lock().unwrap() = pending_count;
        }

        fn set_last_change(&self, _project: &str, at: SystemTime) {
            *self.last_change.lock().unwrap() = Some(at);
        }

        fn set_last_error(&self, _project: &str, _error: Option<crate::errors::SyncError>) {}
        fn set_last_sync_start(&self, _project: &str, _at: SystemTime) {}
        fn set_last_sync_end(&self, _project: &str, _at: SystemTime) {}
        fn set_last_success(&self, _project: &str, _at: SystemTime) {}
        fn set_backoff_until(&self, _project: &str, _until: Option<SystemTime>) {}
    }

    struct FakeRunner {
        calls: AtomicUsize,
    }

    impl FakeRunner {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl Runner for FakeRunner {
        fn run(&self, _request: &crate::sync::SyncRequest) -> std::io::Result<SyncRunResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(SyncRunResult {
                outcome: SyncOutcome::Success,
                command: None,
                stderr: None,
            })
        }
    }

    fn sample_project(path: &Path) -> ProjectConfig {
        ProjectConfig {
            name: "alpha".to_string(),
            local_root: path.to_string_lossy().to_string(),
            remote_host: "example.com".to_string(),
            remote_user: "user".to_string(),
            remote_port: None,
            remote_root: "/remote/alpha".to_string(),
            ssh_identity: None,
            enabled: true,
            watch_debounce_ms: None,
            max_backoff_ms: None,
            ignore: Vec::new(),
        }
    }

    fn scheduler() -> Arc<Scheduler<FakeRunner>> {
        Arc::new(Scheduler::new(
            1,
            FakeRunner::new(),
            BackoffPolicy {
                base_ms: 1,
                max_ms: 10,
            },
        ))
    }

    #[test]
    fn watcher_ignores_paths() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let project = sample_project(temp_dir.path());
        let ignore = IgnoreMatcher::new(["target/"]);
        let (sink, handles) = TestSink::new();
        let mut watch_loop = ProjectWatchLoop::new(
            project,
            ignore,
            Duration::from_millis(10),
            scheduler(),
            sink,
        )
        .expect("watch loop");

        watch_loop.handle_event(WatcherEvent {
            path: NormalizedPath::new("target/bin/app".to_string()),
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
        });

        assert_eq!(*handles.pending_count.lock().unwrap(), 0);
        assert!(handles.last_state.lock().unwrap().is_none());
        assert!(handles.last_change.lock().unwrap().is_none());
    }

    #[test]
    fn watcher_updates_pending_and_state() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let project = sample_project(temp_dir.path());
        let ignore = IgnoreMatcher::new([] as [&str; 0]);
        let (sink, handles) = TestSink::new();
        let mut watch_loop = ProjectWatchLoop::new(
            project,
            ignore,
            Duration::from_millis(10),
            scheduler(),
            sink,
        )
        .expect("watch loop");

        watch_loop.handle_event(WatcherEvent {
            path: NormalizedPath::new("src/lib.rs".to_string()),
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
        });

        assert_eq!(*handles.pending_count.lock().unwrap(), 1);
        assert_eq!(
            *handles.last_state.lock().unwrap(),
            Some(ProjectState::Pending)
        );
        assert!(handles.last_change.lock().unwrap().is_some());
    }
}
