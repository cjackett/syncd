use crate::config::{
    self, DEFAULT_MAX_BACKOFF_MS, DEFAULT_MAX_PARALLEL_PROJECTS, DEFAULT_WATCH_DEBOUNCE_MS,
};
use crate::ignore::IgnoreMatcher;
use crate::ipc;
use crate::scheduler::{BackoffPolicy, ChannelStateSink, Scheduler};
use crate::state::StateRegistry;
use crate::sync::SyncRunner;
use crate::watcher::ProjectWatchLoop;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub async fn run() -> anyhow::Result<()> {
    let config = config::load_or_default()?;
    let registry = Arc::new(StateRegistry::new(&config));
    let socket_path = runtime_socket_path()?;
    let (update_tx, update_rx) = mpsc::unbounded_channel();
    let registry_for_updates = Arc::clone(&registry);
    let update_handle = tokio::spawn(async move {
        registry_for_updates.run_update_loop(update_rx).await;
    });

    let max_parallel = config
        .defaults
        .max_parallel_projects
        .unwrap_or(DEFAULT_MAX_PARALLEL_PROJECTS) as usize;
    let max_backoff_ms = config
        .defaults
        .max_backoff_ms
        .unwrap_or(DEFAULT_MAX_BACKOFF_MS);
    let scheduler = Arc::new(Scheduler::new(
        max_parallel,
        SyncRunner::new(),
        BackoffPolicy {
            base_ms: 1_000,
            max_ms: max_backoff_ms,
        },
    ));

    for project in config.projects.iter().cloned() {
        if !project.enabled {
            continue;
        }
        let debounce_ms = project
            .watch_debounce_ms
            .or(config.defaults.watch_debounce_ms)
            .unwrap_or(DEFAULT_WATCH_DEBOUNCE_MS);
        let ignore_rules = if project.ignore.is_empty() {
            if config.defaults.ignore.is_empty() {
                config::DEFAULT_IGNORE
                    .iter()
                    .map(|rule| (*rule).to_string())
                    .collect::<Vec<String>>()
            } else {
                config.defaults.ignore.clone()
            }
        } else {
            project.ignore.clone()
        };
        let ignore = IgnoreMatcher::new(ignore_rules);
        let sink = ChannelStateSink::new(update_tx.clone());
        match ProjectWatchLoop::new(
            project.clone(),
            ignore,
            Duration::from_millis(debounce_ms),
            Arc::clone(&scheduler),
            sink,
        ) {
            Ok(mut watch_loop) => {
                std::thread::Builder::new()
                    .name(format!("watch-{}", project.name))
                    .spawn(move || {
                        if let Err(err) = watch_loop.run() {
                            eprintln!("watch loop {} error: {err}", project.name);
                        }
                    })?;
            }
            Err(err) => {
                let _ = update_tx.send(crate::state_updates::StateUpdate::SetState {
                    project: project.name.clone(),
                    state: crate::state::ProjectState::Error,
                });
                let _ = update_tx.send(crate::state_updates::StateUpdate::SetLastError {
                    project: project.name.clone(),
                    error: Some(err.to_string()),
                });
            }
        }
    }

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let registry_for_ipc = Arc::clone(&registry);
    let mut ipc_handle =
        tokio::spawn(async move { ipc::serve(socket_path, registry_for_ipc, shutdown_rx).await });

    tokio::select! {
        result = &mut ipc_handle => {
            drop(update_tx);
            update_handle.abort();
            result??;
        }
        _ = tokio::signal::ctrl_c() => {
            let _ = shutdown_tx.send(());
            drop(update_tx);
            ipc_handle.await??;
            update_handle.abort();
        }
    }
    Ok(())
}

pub fn runtime_socket_path() -> anyhow::Result<PathBuf> {
    let runtime_dir = env::var("XDG_RUNTIME_DIR")?;
    Ok(PathBuf::from(runtime_dir).join("syncd.sock"))
}
