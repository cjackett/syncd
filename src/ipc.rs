use crate::state::StateRegistry;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::env;
use std::io::{BufRead, BufReader as StdBufReader, Write};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

#[derive(Debug, Deserialize, Serialize)]
pub struct IpcRequest {
    pub id: String,
    pub cmd: String,
    pub project: Option<String>,
    pub args: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IpcResponse {
    pub id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl IpcResponse {
    fn ok(id: String, data: serde_json::Value) -> Self {
        Self {
            id,
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    fn err(id: String, message: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            data: None,
            error: Some(message.into()),
        }
    }
}

pub async fn serve(
    path: PathBuf,
    registry: Arc<StateRegistry>,
    mut shutdown: oneshot::Receiver<()>,
) -> std::io::Result<()> {
    prepare_socket_path(&path)?;
    let listener = UnixListener::bind(&path)?;
    let result = loop {
        tokio::select! {
            _ = &mut shutdown => {
                break Ok(());
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let registry = Arc::clone(&registry);
                        tokio::spawn(async move {
                            if let Err(err) = handle_client(stream, registry).await {
                                eprintln!("ipc client error: {err}");
                            }
                        });
                    }
                    Err(err) => {
                        break Err(err);
                    }
                }
            }
        }
    };
    cleanup_socket_path(&path)?;
    result
}

async fn handle_client(stream: UnixStream, registry: Arc<StateRegistry>) -> std::io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = TokioBufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<IpcRequest>(trimmed) {
            Ok(request) => route_request(request, &registry).await,
            Err(_) => IpcResponse::err("".to_string(), "invalid json"),
        };
        let payload = serde_json::to_string(&response)
            .unwrap_or_else(|_| r#"{"id":"","ok":false,"error":"serialize failed"}"#.to_string());
        writer.write_all(payload.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    Ok(())
}

pub async fn serve_stream(stream: UnixStream, registry: Arc<StateRegistry>) -> std::io::Result<()> {
    handle_client(stream, registry).await
}

pub fn send_request(cmd: &str, project: Option<&str>) -> Result<IpcResponse> {
    let id = format!(
        "cli-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let request = IpcRequest {
        id: id.clone(),
        cmd: cmd.to_string(),
        project: project.map(|value| value.to_string()),
        args: None,
    };

    let socket_path = runtime_socket_path()?;
    let mut stream = StdUnixStream::connect(&socket_path)
        .with_context(|| format!("connecting to {}", socket_path.display()))?;

    let payload = serde_json::to_string(&request).context("serializing IPC request")?;
    stream
        .write_all(format!("{}\n", payload).as_bytes())
        .context("writing IPC request")?;
    stream.flush().context("flushing IPC request")?;

    let mut reader = StdBufReader::new(stream);
    let mut line = String::new();
    let bytes = reader
        .read_line(&mut line)
        .context("reading IPC response")?;
    if bytes == 0 {
        bail!("daemon closed IPC connection without response");
    }

    let response: IpcResponse = serde_json::from_str(&line).context("parsing IPC response")?;
    if response.id != id {
        bail!("IPC response id mismatch");
    }
    Ok(response)
}

fn runtime_socket_path() -> Result<PathBuf> {
    let runtime = env::var_os("XDG_RUNTIME_DIR")
        .ok_or_else(|| anyhow::anyhow!("XDG_RUNTIME_DIR is not set"))?;
    Ok(PathBuf::from(runtime).join("syncd.sock"))
}

fn prepare_socket_path(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        let metadata = std::fs::metadata(path)?;
        if metadata.file_type().is_socket() {
            std::fs::remove_file(path)?;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "refusing to remove non-socket IPC path",
            ));
        }
    }
    Ok(())
}

fn cleanup_socket_path(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = std::fs::metadata(path)?;
    if metadata.file_type().is_socket() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

pub async fn route_request(request: IpcRequest, registry: &StateRegistry) -> IpcResponse {
    match request.cmd.as_str() {
        "list" => {
            let statuses = registry.list_status().await;
            IpcResponse::ok(request.id, serde_json::to_value(statuses).unwrap())
        }
        "status" => match request.project {
            Some(name) => match registry.status(&name).await {
                Some(status) => IpcResponse::ok(request.id, serde_json::to_value(status).unwrap()),
                None => IpcResponse::err(request.id, "project not found"),
            },
            None => IpcResponse::err(request.id, "project required"),
        },
        "start" => match request.project {
            Some(name) => match registry.start_project(&name).await {
                Ok(()) => IpcResponse::ok(request.id, serde_json::json!({})),
                Err(err) => IpcResponse::err(request.id, err),
            },
            None => IpcResponse::err(request.id, "project required"),
        },
        "stop" => match request.project {
            Some(name) => match registry.stop_project(&name).await {
                Ok(()) => IpcResponse::ok(request.id, serde_json::json!({})),
                Err(err) => IpcResponse::err(request.id, err),
            },
            None => IpcResponse::err(request.id, "project required"),
        },
        "restart" => match request.project {
            Some(name) => match registry.restart_project(&name).await {
                Ok(()) => IpcResponse::ok(request.id, serde_json::json!({})),
                Err(err) => IpcResponse::err(request.id, err),
            },
            None => IpcResponse::err(request.id, "project required"),
        },
        _ => IpcResponse::err(request.id, "unknown command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, Defaults, ProjectConfig};

    fn registry() -> StateRegistry {
        let config = Config {
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
        };
        StateRegistry::new(&config)
    }

    #[tokio::test]
    async fn list_returns_projects() {
        let registry = registry();
        let request = IpcRequest {
            id: "1".to_string(),
            cmd: "list".to_string(),
            project: None,
            args: None,
        };
        let response = route_request(request, &registry).await;
        assert!(response.ok);
        let data = response.data.unwrap();
        let list = data.as_array().unwrap();
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn status_requires_project() {
        let registry = registry();
        let request = IpcRequest {
            id: "1".to_string(),
            cmd: "status".to_string(),
            project: None,
            args: None,
        };
        let response = route_request(request, &registry).await;
        assert!(!response.ok);
    }
}
