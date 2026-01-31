use std::env;
use std::sync::Arc;
use std::time::Duration;

use syncd::config::{Config, Defaults, ProjectConfig};
use syncd::ipc::{send_request, serve};
use syncd::state::StateRegistry;
use tokio::sync::oneshot;

#[tokio::test]
async fn ipc_list_and_status_roundtrip() {
    if env::var("SYNCD_SOCKET_TESTS").ok().as_deref() != Some("1") {
        eprintln!("skipping socket IPC test; set SYNCD_SOCKET_TESTS=1 to enable");
        return;
    }

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let project_root = temp_dir.path().join("project");
    std::fs::create_dir_all(&project_root).expect("create project root");

    let config = Config {
        version: 1,
        defaults: Defaults::default(),
        projects: vec![ProjectConfig {
            name: "alpha".to_string(),
            local_root: project_root.to_string_lossy().to_string(),
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
    };

    unsafe {
        env::set_var("XDG_RUNTIME_DIR", temp_dir.path());
    }
    let socket_path = temp_dir.path().join("syncd.sock");
    let registry = Arc::new(StateRegistry::new(&config));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve(socket_path, registry, shutdown_rx));

    for _ in 0..100 {
        if temp_dir.path().join("syncd.sock").exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let list_response = tokio::task::spawn_blocking(|| send_request("list", None))
        .await
        .expect("spawn list")
        .expect("list response");
    assert!(list_response.ok);
    let list = list_response.data.expect("list data");
    assert_eq!(list.as_array().map(|value| value.len()), Some(1));

    let status_response = tokio::task::spawn_blocking(|| send_request("status", Some("alpha")))
        .await
        .expect("spawn status")
        .expect("status response");
    assert!(status_response.ok);

    let _ = shutdown_tx.send(());
    server_handle
        .await
        .expect("server join")
        .expect("server result");
}
