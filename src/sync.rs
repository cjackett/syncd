use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use tempfile::NamedTempFile;

use crate::errors::{ErrorClass, SyncError};

const DEFAULT_FILES_FROM_MAX: usize = 4096;

#[derive(Debug, Clone)]
pub struct SyncRequest {
    pub project: String,
    pub local_root: PathBuf,
    pub remote_user: String,
    pub remote_host: String,
    pub remote_port: Option<u16>,
    pub remote_root: PathBuf,
    pub ssh_identity: Option<PathBuf>,
    pub mode: SyncMode,
}

#[derive(Debug, Clone)]
pub enum SyncMode {
    Full,
    Files { relative_paths: Vec<PathBuf> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    Success,
    Noop,
    Failed {
        error: SyncError,
        exit_code: Option<i32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRunResult {
    pub outcome: SyncOutcome,
    pub command: Option<CommandSpec>,
    pub stderr: Option<String>,
}

pub struct SyncRunner {
    rsync_path: PathBuf,
    files_from_max: usize,
}

impl SyncRunner {
    pub fn new() -> Self {
        Self {
            rsync_path: PathBuf::from("rsync"),
            files_from_max: DEFAULT_FILES_FROM_MAX,
        }
    }

    pub fn with_rsync_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.rsync_path = path.into();
        self
    }

    pub fn with_files_from_max(mut self, max: usize) -> Self {
        self.files_from_max = max;
        self
    }

    pub fn run(&self, request: &SyncRequest) -> io::Result<SyncRunResult> {
        let mut files_from_file: Option<NamedTempFile> = None;
        let (mode, files_from_path) = match &request.mode {
            SyncMode::Full => (SyncMode::Full, None),
            SyncMode::Files { relative_paths } => {
                if relative_paths.is_empty() {
                    return Ok(SyncRunResult {
                        outcome: SyncOutcome::Noop,
                        command: None,
                        stderr: None,
                    });
                }

                if relative_paths.len() > self.files_from_max {
                    (SyncMode::Full, None)
                } else {
                    let normalized = normalize_relative_paths(relative_paths)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
                    let file = write_files_from(&normalized)?;
                    let path = file.path().to_path_buf();
                    files_from_file = Some(file);
                    (
                        SyncMode::Files {
                            relative_paths: normalized,
                        },
                        Some(path),
                    )
                }
            }
        };

        let spec = build_command_spec(&self.rsync_path, request, mode, files_from_path.as_deref());
        let mut cmd = Command::new(&spec.program);
        cmd.args(&spec.args);

        let output = cmd.output()?;
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stderr = if stderr.is_empty() {
            None
        } else {
            Some(stderr)
        };
        let exit_code = output.status.code();

        let outcome = if output.status.success() {
            SyncOutcome::Success
        } else {
            let class = classify_error(exit_code, stderr.as_deref().unwrap_or(""));
            let message = stderr.clone().unwrap_or_else(|| "rsync failed".to_string());
            SyncOutcome::Failed {
                error: SyncError::new(class, message),
                exit_code,
            }
        };

        drop(files_from_file);

        Ok(SyncRunResult {
            outcome,
            command: Some(spec),
            stderr,
        })
    }
}

impl Default for SyncRunner {
    fn default() -> Self {
        Self::new()
    }
}

pub fn build_command_spec(
    rsync_path: &Path,
    request: &SyncRequest,
    mode: SyncMode,
    files_from_path: Option<&Path>,
) -> CommandSpec {
    let mut args: Vec<String> = Vec::new();

    args.push("-az".to_string());

    if let Some(path) = files_from_path {
        args.push("--files-from".to_string());
        args.push(path.to_string_lossy().to_string());
    }

    let ssh_cmd = build_ssh_command(request.remote_port, request.ssh_identity.as_deref());
    args.push("-e".to_string());
    args.push(ssh_cmd);

    let source = ensure_trailing_slash(&request.local_root);
    let dest = format!(
        "{}@{}:{}/",
        request.remote_user,
        request.remote_host,
        strip_trailing_slash(&request.remote_root)
    );

    let _ = mode;

    args.push(source);
    args.push(dest);

    CommandSpec {
        program: rsync_path.to_string_lossy().to_string(),
        args,
    }
}

fn build_ssh_command(port: Option<u16>, identity: Option<&Path>) -> String {
    let mut parts = vec!["ssh".to_string()];
    if let Some(port) = port {
        parts.push("-p".to_string());
        parts.push(port.to_string());
    }
    if let Some(identity) = identity {
        parts.push("-i".to_string());
        parts.push(identity.to_string_lossy().to_string());
    }
    parts
        .into_iter()
        .map(|part| shell_escape(&part))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_escape(text: &str) -> String {
    if text.is_empty() {
        return "''".to_string();
    }

    if text
        .chars()
        .all(|ch| matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' | '/' | ':' ))
    {
        return text.to_string();
    }

    let mut escaped = String::from("'");
    for ch in text.chars() {
        if ch == '\'' {
            escaped.push_str("'\\''");
        } else {
            escaped.push(ch);
        }
    }
    escaped.push('\'');
    escaped
}

fn ensure_trailing_slash(path: &Path) -> String {
    let mut text = path.to_string_lossy().to_string();
    if !text.ends_with('/') {
        text.push('/');
    }
    text
}

fn strip_trailing_slash(path: &Path) -> String {
    let mut text = path.to_string_lossy().to_string();
    while text.ends_with('/') {
        text.pop();
    }
    text
}

fn normalize_relative_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    let mut normalized = Vec::with_capacity(paths.len());
    for path in paths {
        if path.is_absolute() {
            return Err("absolute path in pending set".to_string());
        }
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err("parent directory component in pending set".to_string());
        }
        normalized.push(path.clone());
    }
    Ok(normalized)
}

fn write_files_from(paths: &[PathBuf]) -> io::Result<NamedTempFile> {
    let mut file = NamedTempFile::new()?;
    for path in paths {
        writeln!(file, "{}", path.to_string_lossy())?;
    }
    file.flush()?;
    Ok(file)
}

pub fn classify_error(exit_code: Option<i32>, stderr: &str) -> ErrorClass {
    let text = stderr.to_lowercase();

    let hard_markers = [
        "permission denied",
        "authentication failed",
        "auth failed",
        "no such file or directory",
        "host key verification failed",
        "invalid argument",
    ];

    if hard_markers.iter().any(|m| text.contains(m)) {
        return ErrorClass::Hard;
    }

    let transient_markers = [
        "connection timed out",
        "operation timed out",
        "connection reset",
        "connection refused",
        "temporary failure in name resolution",
        "could not resolve hostname",
        "network is unreachable",
        "connection closed by remote host",
    ];

    if transient_markers.iter().any(|m| text.contains(m)) {
        return ErrorClass::Transient;
    }

    if exit_code == Some(255) {
        return ErrorClass::Transient;
    }

    ErrorClass::Hard
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_error_marks_hard_auth() {
        assert_eq!(
            classify_error(Some(255), "Permission denied (publickey)."),
            ErrorClass::Hard
        );
    }

    #[test]
    fn classify_error_marks_transient_timeout() {
        assert_eq!(
            classify_error(Some(255), "Connection timed out"),
            ErrorClass::Transient
        );
    }

    #[test]
    fn classify_error_marks_transient_exit_code() {
        assert_eq!(
            classify_error(Some(255), "unrecognized"),
            ErrorClass::Transient
        );
    }

    #[test]
    fn build_command_uses_files_from_and_ssh_opts() {
        let request = SyncRequest {
            project: "proj".to_string(),
            local_root: PathBuf::from("/tmp/local"),
            remote_user: "user".to_string(),
            remote_host: "example.com".to_string(),
            remote_port: Some(2222),
            remote_root: PathBuf::from("/remote/root"),
            ssh_identity: Some(PathBuf::from("/home/user/.ssh/id with space")),
            mode: SyncMode::Files {
                relative_paths: vec![PathBuf::from("src/lib.rs")],
            },
        };

        let spec = build_command_spec(
            Path::new("rsync"),
            &request,
            request.mode.clone(),
            Some(Path::new("/tmp/files.txt")),
        );

        assert_eq!(spec.program, "rsync");
        assert!(spec.args.contains(&"--files-from".to_string()));
        assert!(spec.args.contains(&"/tmp/files.txt".to_string()));
        assert!(
            spec.args
                .contains(&"ssh -p 2222 -i '/home/user/.ssh/id with space'".to_string())
        );
        assert!(spec.args.contains(&"/tmp/local/".to_string()));
        assert!(
            spec.args
                .contains(&"user@example.com:/remote/root/".to_string())
        );
    }

    #[test]
    fn normalize_relative_paths_rejects_absolute() {
        let paths = vec![PathBuf::from("/etc/passwd")];
        let err = normalize_relative_paths(&paths).unwrap_err();
        assert!(err.contains("absolute path"));
    }

    #[test]
    fn normalize_relative_paths_rejects_parent() {
        let paths = vec![PathBuf::from("../secret.txt")];
        let err = normalize_relative_paths(&paths).unwrap_err();
        assert!(err.contains("parent directory"));
    }
}
