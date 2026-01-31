use crate::ignore::IgnoreMatcher;
use crate::paths::NormalizedPath;
use std::collections::HashSet;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct PendingSet {
    paths: HashSet<NormalizedPath>,
}

impl PendingSet {
    pub fn insert(&mut self, path: NormalizedPath) {
        self.paths.insert(path);
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn take_all(&mut self) -> Vec<NormalizedPath> {
        self.paths.drain().collect()
    }
}

#[derive(Debug)]
pub struct DebounceTracker {
    debounce: Duration,
    last_event: Option<Instant>,
}

impl DebounceTracker {
    pub fn new(debounce: Duration) -> Self {
        Self {
            debounce,
            last_event: None,
        }
    }

    pub fn record_event(&mut self, now: Instant) {
        self.last_event = Some(now);
    }

    pub fn ready(&self, now: Instant) -> bool {
        self.last_event
            .map(|last| now.duration_since(last) >= self.debounce)
            .unwrap_or(false)
    }

    pub fn remaining(&self, now: Instant) -> Option<Duration> {
        self.last_event.map(|last| {
            let elapsed = now.duration_since(last);
            if elapsed >= self.debounce {
                Duration::from_millis(0)
            } else {
                self.debounce - elapsed
            }
        })
    }
}

#[derive(Debug)]
pub struct ChangeAggregator {
    ignore: IgnoreMatcher,
    pending: PendingSet,
    debounce: DebounceTracker,
}

impl ChangeAggregator {
    pub fn new(ignore: IgnoreMatcher, debounce: Duration) -> Self {
        Self {
            ignore,
            pending: PendingSet::default(),
            debounce: DebounceTracker::new(debounce),
        }
    }

    pub fn ingest(&mut self, path: NormalizedPath, now: Instant) -> bool {
        if self.ignore.is_ignored(&path) {
            return false;
        }
        self.pending.insert(path);
        self.debounce.record_event(now);
        true
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub fn debounce_ready(&self, now: Instant) -> bool {
        self.debounce.ready(now)
    }

    pub fn debounce_remaining(&self, now: Instant) -> Option<Duration> {
        if self.pending.is_empty() {
            None
        } else {
            self.debounce.remaining(now)
        }
    }

    pub fn take_pending(&mut self) -> Vec<NormalizedPath> {
        self.pending.take_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::NormalizedPath;

    #[test]
    fn debounce_waits_until_elapsed() {
        let ignore = IgnoreMatcher::new([] as [&str; 0]);
        let mut agg = ChangeAggregator::new(ignore, Duration::from_millis(100));
        let start = Instant::now();
        assert!(agg.ingest(NormalizedPath::new("src/main.rs".to_string()), start));
        assert!(!agg.debounce_ready(start));
        assert!(!agg.debounce_ready(start + Duration::from_millis(50)));
        assert!(agg.debounce_ready(start + Duration::from_millis(100)));
    }

    #[test]
    fn pending_set_dedupes_paths() {
        let ignore = IgnoreMatcher::new([] as [&str; 0]);
        let mut agg = ChangeAggregator::new(ignore, Duration::from_millis(10));
        let now = Instant::now();
        let path = NormalizedPath::new("src/lib.rs".to_string());
        assert!(agg.ingest(path.clone(), now));
        assert!(agg.ingest(path, now + Duration::from_millis(1)));
        assert_eq!(agg.pending_len(), 1);
    }

    #[test]
    fn ignore_rules_skip_pending() {
        let ignore = IgnoreMatcher::new(["target/"]);
        let mut agg = ChangeAggregator::new(ignore, Duration::from_millis(10));
        let now = Instant::now();
        assert!(!agg.ingest(NormalizedPath::new("target/bin/app".to_string()), now));
        assert_eq!(agg.pending_len(), 0);
    }

    #[test]
    fn take_pending_clears_set() {
        let ignore = IgnoreMatcher::new([] as [&str; 0]);
        let mut agg = ChangeAggregator::new(ignore, Duration::from_millis(10));
        let now = Instant::now();
        assert!(agg.ingest(NormalizedPath::new("src/lib.rs".to_string()), now));
        assert_eq!(agg.pending_len(), 1);
        let drained = agg.take_pending();
        assert_eq!(drained.len(), 1);
        assert_eq!(agg.pending_len(), 0);
    }
}
