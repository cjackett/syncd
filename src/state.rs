use crate::config::{Config, ProjectConfig};
use crate::state_updates::StateUpdate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProjectState {
    Idle,
    Pending,
    Syncing,
    Backoff,
    Error,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectStatus {
    pub project: String,
    pub local_root: String,
    pub remote: String,
    pub state: ProjectState,
    pub pending_count: u64,
    pub last_change_ts: Option<String>,
    pub last_sync_start_ts: Option<String>,
    pub last_sync_end_ts: Option<String>,
    pub last_success_ts: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
struct ProjectRuntime {
    config: ProjectConfig,
    state: ProjectState,
    pending_count: u64,
    last_change_ts: Option<String>,
    last_sync_start_ts: Option<String>,
    last_sync_end_ts: Option<String>,
    last_success_ts: Option<String>,
    last_error: Option<String>,
    backoff_until_ts: Option<String>,
}

impl ProjectRuntime {
    fn status(&self) -> ProjectStatus {
        ProjectStatus {
            project: self.config.name.clone(),
            local_root: self.config.local_root.clone(),
            remote: format!(
                "{}@{}:{}",
                self.config.remote_user, self.config.remote_host, self.config.remote_root
            ),
            state: self.state,
            pending_count: self.pending_count,
            last_change_ts: self.last_change_ts.clone(),
            last_sync_start_ts: self.last_sync_start_ts.clone(),
            last_sync_end_ts: self.last_sync_end_ts.clone(),
            last_success_ts: self.last_success_ts.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub struct StateRegistry {
    inner: RwLock<HashMap<String, ProjectRuntime>>,
}

impl StateRegistry {
    pub fn new(config: &Config) -> Self {
        let mut map = HashMap::new();
        for project in &config.projects {
            let state = if project.enabled {
                ProjectState::Idle
            } else {
                ProjectState::Disabled
            };
            map.insert(
                project.name.clone(),
                ProjectRuntime {
                    config: project.clone(),
                    state,
                    pending_count: 0,
                    last_change_ts: None,
                    last_sync_start_ts: None,
                    last_sync_end_ts: None,
                    last_success_ts: None,
                    last_error: None,
                    backoff_until_ts: None,
                },
            );
        }
        Self {
            inner: RwLock::new(map),
        }
    }

    pub async fn list_status(&self) -> Vec<ProjectStatus> {
        let guard = self.inner.read().await;
        let mut statuses: Vec<ProjectStatus> = guard.values().map(ProjectRuntime::status).collect();
        statuses.sort_by(|a, b| a.project.cmp(&b.project));
        statuses
    }

    pub async fn status(&self, name: &str) -> Option<ProjectStatus> {
        let guard = self.inner.read().await;
        guard.get(name).map(ProjectRuntime::status)
    }

    pub async fn start_project(&self, name: &str) -> Result<(), String> {
        let mut guard = self.inner.write().await;
        let project = guard
            .get_mut(name)
            .ok_or_else(|| "project not found".to_string())?;
        project.state = ProjectState::Idle;
        project.last_error = None;
        Ok(())
    }

    pub async fn stop_project(&self, name: &str) -> Result<(), String> {
        let mut guard = self.inner.write().await;
        let project = guard
            .get_mut(name)
            .ok_or_else(|| "project not found".to_string())?;
        project.state = ProjectState::Disabled;
        Ok(())
    }

    pub async fn restart_project(&self, name: &str) -> Result<(), String> {
        let mut guard = self.inner.write().await;
        let project = guard
            .get_mut(name)
            .ok_or_else(|| "project not found".to_string())?;
        project.state = ProjectState::Idle;
        project.last_error = None;
        Ok(())
    }

    pub async fn apply_update(&self, update: StateUpdate) {
        let mut guard = self.inner.write().await;
        match update {
            StateUpdate::SetState { project, state } => {
                if let Some(entry) = guard.get_mut(&project) {
                    entry.state = state;
                }
            }
            StateUpdate::SetPendingCount {
                project,
                pending_count,
            } => {
                if let Some(entry) = guard.get_mut(&project) {
                    entry.pending_count = pending_count;
                }
            }
            StateUpdate::SetLastChange { project, at } => {
                if let Some(entry) = guard.get_mut(&project) {
                    entry.last_change_ts = Some(format_timestamp(at));
                }
            }
            StateUpdate::SetLastError { project, error } => {
                if let Some(entry) = guard.get_mut(&project) {
                    entry.last_error = error;
                }
            }
            StateUpdate::SetLastSyncStart { project, at } => {
                if let Some(entry) = guard.get_mut(&project) {
                    entry.last_sync_start_ts = Some(format_timestamp(at));
                }
            }
            StateUpdate::SetLastSyncEnd { project, at } => {
                if let Some(entry) = guard.get_mut(&project) {
                    entry.last_sync_end_ts = Some(format_timestamp(at));
                }
            }
            StateUpdate::SetLastSuccess { project, at } => {
                if let Some(entry) = guard.get_mut(&project) {
                    entry.last_success_ts = Some(format_timestamp(at));
                }
            }
            StateUpdate::SetBackoffUntil { project, until } => {
                if let Some(entry) = guard.get_mut(&project) {
                    entry.backoff_until_ts = until.map(format_timestamp);
                }
            }
        }
    }

    pub async fn run_update_loop(&self, mut receiver: UnboundedReceiver<StateUpdate>) {
        while let Some(update) = receiver.recv().await {
            self.apply_update(update).await;
        }
    }
}

fn format_timestamp(at: SystemTime) -> String {
    match at.duration_since(UNIX_EPOCH) {
        Ok(delta) => format!("{}.{:03}Z", delta.as_secs(), delta.subsec_millis()),
        Err(_) => "0.000Z".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Defaults;

    fn sample_config() -> Config {
        Config {
            version: 1,
            defaults: Defaults::default(),
            projects: vec![ProjectConfig {
                name: "alpha".to_string(),
                local_root: "/tmp/alpha".to_string(),
                remote_host: "example.com".to_string(),
                remote_user: "user".to_string(),
                remote_port: None,
                remote_root: "/remote/alpha".to_string(),
                ssh_identity: None,
                enabled: true,
                watch_debounce_ms: None,
                max_backoff_ms: None,
                ignore: Vec::new(),
            }],
        }
    }

    #[tokio::test]
    async fn start_stop_updates_state() {
        let registry = StateRegistry::new(&sample_config());
        registry.stop_project("alpha").await.unwrap();
        let status = registry.status("alpha").await.unwrap();
        assert_eq!(status.state, ProjectState::Disabled);
        registry.start_project("alpha").await.unwrap();
        let status = registry.status("alpha").await.unwrap();
        assert_eq!(status.state, ProjectState::Idle);
    }

    #[tokio::test]
    async fn apply_update_sets_state_and_error() {
        let registry = StateRegistry::new(&sample_config());
        registry
            .apply_update(StateUpdate::SetState {
                project: "alpha".to_string(),
                state: ProjectState::Error,
            })
            .await;
        registry
            .apply_update(StateUpdate::SetLastError {
                project: "alpha".to_string(),
                error: Some("hard: auth failed".to_string()),
            })
            .await;
        let status = registry.status("alpha").await.unwrap();
        assert_eq!(status.state, ProjectState::Error);
        assert_eq!(status.last_error, Some("hard: auth failed".to_string()));
    }

    #[tokio::test]
    async fn apply_update_sets_pending_and_last_change() {
        let registry = StateRegistry::new(&sample_config());
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
        registry
            .apply_update(StateUpdate::SetPendingCount {
                project: "alpha".to_string(),
                pending_count: 7,
            })
            .await;
        registry
            .apply_update(StateUpdate::SetLastChange {
                project: "alpha".to_string(),
                at: now,
            })
            .await;

        let status = registry.status("alpha").await.unwrap();
        assert_eq!(status.pending_count, 7);
        assert_eq!(status.last_change_ts, Some("10.000Z".to_string()));
    }
}
