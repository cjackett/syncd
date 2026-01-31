use std::collections::HashSet;
use std::env;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::config::{
    Config, DEFAULT_IGNORE, DEFAULT_MAX_BACKOFF_MS, DEFAULT_MAX_PARALLEL_PROJECTS,
    DEFAULT_WATCH_DEBOUNCE_MS, Defaults, ProjectConfig, config_path, expand_tilde,
};
use crate::ipc;
use crate::selection::select_project_by_cwd;

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusPayload {
    pub project: String,
    pub local_root: String,
    pub remote: String,
    pub state: String,
    pub pending_count: u64,
    pub last_change_ts: Option<String>,
    pub last_sync_start_ts: Option<String>,
    pub last_sync_end_ts: Option<String>,
    pub last_success_ts: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub name: String,
    pub local_root: String,
    pub remote: String,
    pub enabled: bool,
    pub state: String,
}

pub fn cmd_add() -> Result<()> {
    let cwd = env::current_dir().context("reading current directory")?;
    let mut config = Config::load()?.unwrap_or_else(|| Config {
        version: 1,
        defaults: Defaults {
            watch_debounce_ms: Some(DEFAULT_WATCH_DEBOUNCE_MS),
            max_backoff_ms: Some(DEFAULT_MAX_BACKOFF_MS),
            max_parallel_projects: Some(DEFAULT_MAX_PARALLEL_PROJECTS),
            ignore: DEFAULT_IGNORE
                .iter()
                .map(|item| (*item).to_string())
                .collect(),
        },
        projects: Vec::new(),
    });

    println!("syncd add - create a new project");
    let local_root_input = prompt_with_default("Local root", &cwd.to_string_lossy())?;
    let local_root = normalize_local_root(&local_root_input, &cwd)?;
    if !local_root.exists() {
        bail!("local root does not exist: {}", local_root.display());
    }

    let remote_host = prompt_required("Remote host", None)?;
    let remote_user = prompt_required("Remote user", Some(&default_username()))?;
    let remote_root_input = prompt_required("Remote root", None)?;
    let remote_root = normalize_remote_root(&remote_root_input)?;

    let ssh_identity_input = prompt_optional("SSH identity (optional)")?;
    let ssh_identity = ssh_identity_input
        .as_deref()
        .map(expand_tilde)
        .filter(|value| !value.trim().is_empty());

    let ignore_input = prompt_optional("Ignore overrides (comma-separated, optional)")?;
    let ignore = ignore_input
        .as_deref()
        .map(parse_ignore_list)
        .unwrap_or_default();

    let default_name = local_root
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| "project".to_string());

    let name = loop {
        let candidate = prompt_with_default("Project name", &default_name)?;
        if config.project_by_name(&candidate).is_some() {
            println!("Project name '{}' already exists.", candidate);
            continue;
        }
        if candidate.trim().is_empty() {
            println!("Project name cannot be empty.");
            continue;
        }
        break candidate;
    };

    let project = ProjectConfig {
        name,
        local_root: local_root.to_string_lossy().to_string(),
        remote_host,
        remote_user,
        remote_port: None,
        remote_root: remote_root.to_string_lossy().to_string(),
        ssh_identity,
        enabled: true,
        watch_debounce_ms: None,
        max_backoff_ms: None,
        ignore,
    };

    config.projects.push(project);
    config.save()?;

    println!("Project added to {}", config_path().display());
    Ok(())
}

pub fn cmd_list(json: bool) -> Result<()> {
    if let Ok(response) = ipc::send_request("list", None)
        && response.ok
        && let Some(data) = response.data
    {
        if json {
            let output = serde_json::to_string_pretty(&data).context("formatting JSON output")?;
            println!("{}", output);
            return Ok(());
        }
        if let Ok(statuses) = serde_json::from_value::<Vec<StatusPayload>>(data) {
            print_list(&summaries_from_statuses(&statuses));
            return Ok(());
        }
    }

    let config = Config::load()?.context("no config found")?;
    let statuses = status_payloads_from_projects(&config.projects);
    if json {
        let output = serde_json::to_string_pretty(&statuses).context("formatting JSON output")?;
        println!("{}", output);
    } else {
        print_list(&summaries_from_statuses(&statuses));
    }
    Ok(())
}

pub fn cmd_status(project_name: Option<&str>, json: bool) -> Result<()> {
    let config = Config::load()?;
    let resolved_name = if let Some(name) = project_name {
        Some(name.to_string())
    } else if let Some(config) = &config {
        Some(resolve_project(config, None)?.name.clone())
    } else {
        None
    };

    if let Some(name) = resolved_name.as_deref()
        && let Ok(response) = ipc::send_request("status", Some(name))
        && response.ok
        && let Some(data) = response.data
    {
        if json {
            let output = serde_json::to_string_pretty(&data).context("formatting JSON output")?;
            println!("{}", output);
            return Ok(());
        }
        if let Ok(status) = serde_json::from_value::<StatusPayload>(data) {
            print_status(&status);
            return Ok(());
        }
    }

    let config = config.context("no config found")?;
    let project = resolve_project(&config, project_name)?;
    let status = StatusPayload::from_project(project);

    if json {
        let output = serde_json::to_string_pretty(&status).context("formatting JSON output")?;
        println!("{}", output);
    } else {
        print_status(&status);
    }
    Ok(())
}

pub fn cmd_watch() -> Result<()> {
    println!("syncd watch - press Ctrl+C to exit");
    loop {
        match ipc::send_request("list", None) {
            Ok(response) if response.ok => {
                let data = response.data.unwrap_or_else(|| serde_json::json!([]));
                let statuses: Vec<StatusPayload> =
                    serde_json::from_value(data).context("parsing daemon status list")?;
                render_watch_frame(&statuses);
            }
            Ok(response) => {
                let message = response.error.unwrap_or_else(|| "daemon error".to_string());
                bail!("{}", message);
            }
            Err(err) => {
                if let Some(config) = Config::load()? {
                    eprintln!("Daemon unavailable ({}). Showing config snapshot.", err);
                    let statuses = status_payloads_from_projects(&config.projects);
                    render_watch_frame(&statuses);
                    return Ok(());
                }
                return Err(err);
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

pub fn cmd_edit(project_name: Option<&str>) -> Result<()> {
    let mut config = Config::load()?.context("no config found")?;
    let cwd = env::current_dir().context("reading current directory")?;
    let index = resolve_project_index(&config, project_name, &cwd)?;
    let current = config.projects[index].clone();

    println!("syncd edit - update '{}'", current.name);

    let name = loop {
        let candidate = prompt_with_default("Project name", &current.name)?;
        if candidate.trim().is_empty() {
            println!("Project name cannot be empty.");
            continue;
        }
        if candidate != current.name && config.project_by_name(&candidate).is_some() {
            println!("Project name '{}' already exists.", candidate);
            continue;
        }
        break candidate;
    };

    let local_root = loop {
        let input = prompt_with_default("Local root", &current.local_root)?;
        let root = normalize_local_root(&input, &cwd)?;
        if !root.exists() {
            println!("local root does not exist: {}", root.display());
            continue;
        }
        break root.to_string_lossy().to_string();
    };

    let remote_host = prompt_required("Remote host", Some(&current.remote_host))?;
    let remote_user = prompt_required("Remote user", Some(&current.remote_user))?;

    let remote_root = loop {
        let input = prompt_with_default("Remote root", &current.remote_root)?;
        match normalize_remote_root(&input) {
            Ok(path) => break path.to_string_lossy().to_string(),
            Err(err) => {
                println!("{err}");
                continue;
            }
        }
    };

    let remote_port = prompt_optional_u16("Remote port", current.remote_port)?;
    let ssh_identity = prompt_optional_string("SSH identity", current.ssh_identity.as_deref())?
        .map(|value| expand_tilde(&value));

    let ignore = prompt_optional_list("Ignore overrides (comma-separated)", &current.ignore)?;

    let watch_debounce_ms =
        prompt_optional_u64("Watch debounce ms override", current.watch_debounce_ms)?;
    let max_backoff_ms = prompt_optional_u64("Max backoff ms override", current.max_backoff_ms)?;
    let enabled = prompt_bool("Enabled", current.enabled)?;

    let updated = ProjectConfig {
        name,
        local_root,
        remote_host,
        remote_user,
        remote_port,
        remote_root,
        ssh_identity,
        enabled,
        watch_debounce_ms,
        max_backoff_ms,
        ignore,
    };

    config.projects[index] = updated;
    config.save()?;

    println!("Project updated in {}", config_path().display());
    Ok(())
}

pub fn cmd_remove(project_name: Option<&str>) -> Result<()> {
    let mut config = Config::load()?.context("no config found")?;
    let cwd = env::current_dir().context("reading current directory")?;
    let index = resolve_project_index(&config, project_name, &cwd)?;
    let name = config.projects[index].name.clone();

    if !prompt_confirm(&format!("Remove project '{}'?", name), false)? {
        println!("Aborted.");
        return Ok(());
    }

    config.projects.remove(index);
    config.save()?;

    println!("Project removed from {}", config_path().display());
    let _ = ipc::send_request("stop", Some(&name));
    Ok(())
}

pub enum ControlCommand {
    Start,
    Stop,
    Restart,
}

pub fn cmd_control(command: ControlCommand, project_name: Option<&str>, all: bool) -> Result<()> {
    let mut config = Config::load()?.context("no config found")?;
    if config.projects.is_empty() {
        println!("No projects configured.");
        return Ok(());
    }
    let cwd = env::current_dir().context("reading current directory")?;
    let targets = resolve_project_names(&config, project_name, all, &cwd)?;

    let desired_enabled = match command {
        ControlCommand::Start => Some(true),
        ControlCommand::Stop => Some(false),
        ControlCommand::Restart => None,
    };
    if let Some(enabled) = desired_enabled {
        let target_set: HashSet<&str> = targets.iter().map(|name| name.as_str()).collect();
        for project in &mut config.projects {
            if target_set.contains(project.name.as_str()) {
                project.enabled = enabled;
            }
        }
        config.save()?;
    }

    let cmd = match command {
        ControlCommand::Start => "start",
        ControlCommand::Stop => "stop",
        ControlCommand::Restart => "restart",
    };

    let mut ipc_errors = Vec::new();
    for name in &targets {
        match ipc::send_request(cmd, Some(name)) {
            Ok(response) => {
                if response.ok {
                    println!("{} {}", cmd, name);
                } else if let Some(error) = response.error {
                    ipc_errors.push(format!("{}: {}", name, error));
                }
            }
            Err(err) => {
                ipc_errors.push(format!("{}: {}", name, err));
                break;
            }
        }
    }

    if !ipc_errors.is_empty() {
        bail!(
            "daemon unavailable or returned errors; config updated but IPC failed: {}",
            ipc_errors.join("; ")
        );
    }

    Ok(())
}

pub fn resolve_project<'a>(
    config: &'a Config,
    project_name: Option<&str>,
) -> Result<&'a ProjectConfig> {
    if let Some(name) = project_name {
        return config
            .project_by_name(name)
            .with_context(|| format!("project '{}' not found", name));
    }

    let cwd = env::current_dir().context("reading current directory")?;
    select_project_by_cwd(&config.projects, &cwd)
        .context("no project selected (not in a project directory and none specified)")
}

fn resolve_project_index(config: &Config, project_name: Option<&str>, cwd: &Path) -> Result<usize> {
    if let Some(name) = project_name {
        return config
            .projects
            .iter()
            .position(|project| project.name == name)
            .with_context(|| format!("project '{}' not found", name));
    }

    let selected = select_project_by_cwd(&config.projects, cwd)
        .context("no project selected (not in a project directory and none specified)")?;
    config
        .projects
        .iter()
        .position(|project| project.name == selected.name)
        .context("selected project missing from config")
}

fn resolve_project_names(
    config: &Config,
    project_name: Option<&str>,
    all: bool,
    cwd: &Path,
) -> Result<Vec<String>> {
    if all {
        return Ok(config.projects.iter().map(|p| p.name.clone()).collect());
    }

    let index = resolve_project_index(config, project_name, cwd)?;
    Ok(vec![config.projects[index].name.clone()])
}

fn summaries_from_statuses(statuses: &[StatusPayload]) -> Vec<ProjectSummary> {
    statuses
        .iter()
        .map(|status| ProjectSummary {
            name: status.project.clone(),
            local_root: status.local_root.clone(),
            remote: status.remote.clone(),
            enabled: status.state != "disabled",
            state: status.state.clone(),
        })
        .collect()
}

fn status_payloads_from_projects(projects: &[ProjectConfig]) -> Vec<StatusPayload> {
    projects.iter().map(StatusPayload::from_project).collect()
}

fn print_list(items: &[ProjectSummary]) {
    if items.is_empty() {
        println!("No projects configured.");
        return;
    }

    for summary in items {
        println!(
            "{}\t{} -> {}\t{}\t{}",
            summary.name,
            summary.local_root,
            summary.remote,
            if summary.enabled {
                "enabled"
            } else {
                "disabled"
            },
            summary.state
        );
    }
}

fn render_watch_frame(statuses: &[StatusPayload]) {
    print!("\x1b[2J\x1b[H");
    println!("syncd watch - press Ctrl+C to exit");
    print_list(&summaries_from_statuses(statuses));
    let _ = io::stdout().flush();
}

fn print_status(status: &StatusPayload) {
    println!("Project: {}", status.project);
    println!("Local root: {}", status.local_root);
    println!("Remote: {}", status.remote);
    println!("State: {}", status.state);
    println!("Pending: {}", status.pending_count);
    if let Some(value) = &status.last_change_ts {
        println!("Last change: {}", value);
    }
    if let Some(value) = &status.last_sync_start_ts {
        println!("Last sync start: {}", value);
    }
    if let Some(value) = &status.last_sync_end_ts {
        println!("Last sync end: {}", value);
    }
    if let Some(value) = &status.last_success_ts {
        println!("Last success: {}", value);
    }
    if let Some(value) = &status.last_error {
        println!("Last error: {}", value);
    }
}

fn normalize_local_root(input: &str, cwd: &Path) -> Result<PathBuf> {
    let expanded = expand_tilde(input);
    let path = PathBuf::from(expanded);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(cwd.join(path))
    }
}

fn normalize_remote_root(input: &str) -> Result<PathBuf> {
    let expanded = expand_tilde(input);
    let path = PathBuf::from(expanded);
    if !path.is_absolute() {
        bail!("remote root must be absolute");
    }
    Ok(path)
}

fn prompt_with_default(label: &str, default: &str) -> Result<String> {
    let input = prompt_optional(&format!("{} [{}]", label, default))?;
    Ok(input.unwrap_or_else(|| default.to_string()))
}

fn prompt_required(label: &str, default: Option<&str>) -> Result<String> {
    loop {
        let prompt = match default {
            Some(value) => format!("{} [{}]", label, value),
            None => label.to_string(),
        };
        let input = prompt_optional(&prompt)?;
        if let Some(value) = input {
            if !value.trim().is_empty() {
                return Ok(value);
            }
        } else if let Some(value) = default
            && !value.trim().is_empty()
        {
            return Ok(value.to_string());
        }
        println!("{} is required.", label);
    }
}

fn prompt_optional(label: &str) -> Result<Option<String>> {
    print!("{}: ", label);
    io::stdout().flush().context("flushing prompt")?;
    let mut input = String::new();
    io::stdin().read_line(&mut input).context("reading input")?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

fn prompt_optional_string(label: &str, current: Option<&str>) -> Result<Option<String>> {
    let current_label = current.unwrap_or("none");
    let input = prompt_optional(&format!(
        "{} [{}] (enter '-' to clear)",
        label, current_label
    ))?;
    match input {
        None => Ok(current.map(|value| value.to_string())),
        Some(value) => {
            if value.trim() == "-" {
                Ok(None)
            } else {
                Ok(Some(value))
            }
        }
    }
}

fn prompt_optional_u64(label: &str, current: Option<u64>) -> Result<Option<u64>> {
    let current_label = current
        .map(|value| value.to_string())
        .unwrap_or_else(|| "default".to_string());
    loop {
        let input = prompt_optional(&format!(
            "{} [{}] (enter '-' to clear)",
            label, current_label
        ))?;
        match input {
            None => return Ok(current),
            Some(value) => {
                let trimmed = value.trim();
                if trimmed == "-" {
                    return Ok(None);
                }
                match trimmed.parse::<u64>() {
                    Ok(parsed) if parsed > 0 => return Ok(Some(parsed)),
                    Ok(_) => println!("{} must be > 0.", label),
                    Err(_) => println!("{} must be a number.", label),
                }
            }
        }
    }
}

fn prompt_optional_u16(label: &str, current: Option<u16>) -> Result<Option<u16>> {
    let current_label = current
        .map(|value| value.to_string())
        .unwrap_or_else(|| "default".to_string());
    loop {
        let input = prompt_optional(&format!(
            "{} [{}] (enter '-' to clear)",
            label, current_label
        ))?;
        match input {
            None => return Ok(current),
            Some(value) => {
                let trimmed = value.trim();
                if trimmed == "-" {
                    return Ok(None);
                }
                match trimmed.parse::<u16>() {
                    Ok(parsed) if parsed > 0 => return Ok(Some(parsed)),
                    Ok(_) => println!("{} must be > 0.", label),
                    Err(_) => println!("{} must be a number.", label),
                }
            }
        }
    }
}

fn prompt_optional_list(label: &str, current: &[String]) -> Result<Vec<String>> {
    let current_label = if current.is_empty() {
        "none".to_string()
    } else {
        current.join(", ")
    };
    let input = prompt_optional(&format!(
        "{} [{}] (enter '-' to clear)",
        label, current_label
    ))?;
    match input {
        None => Ok(current.to_vec()),
        Some(value) => {
            if value.trim() == "-" {
                Ok(Vec::new())
            } else {
                Ok(parse_ignore_list(&value))
            }
        }
    }
}

fn prompt_bool(label: &str, current: bool) -> Result<bool> {
    let default = if current { "y" } else { "n" };
    loop {
        let input = prompt_optional(&format!("{} [{}]", label, default))?;
        match input {
            None => return Ok(current),
            Some(value) => match value.trim().to_lowercase().as_str() {
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => println!("{} must be 'y' or 'n'.", label),
            },
        }
    }
}

fn prompt_confirm(label: &str, default: bool) -> Result<bool> {
    let default_label = if default { "y" } else { "n" };
    loop {
        let input = prompt_optional(&format!("{} [{}]", label, default_label))?;
        match input {
            None => return Ok(default),
            Some(value) => match value.trim().to_lowercase().as_str() {
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => println!("Please enter y or n."),
            },
        }
    }
}

fn parse_ignore_list(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .collect()
}

fn default_username() -> String {
    env::var("USER").unwrap_or_else(|_| "user".to_string())
}

impl StatusPayload {
    fn from_project(project: &ProjectConfig) -> Self {
        Self {
            project: project.name.clone(),
            local_root: project.local_root.clone(),
            remote: project.remote_string(),
            state: if project.enabled { "idle" } else { "disabled" }.to_string(),
            pending_count: 0,
            last_change_ts: None,
            last_sync_start_ts: None,
            last_sync_end_ts: None,
            last_success_ts: None,
            last_error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn sample_config(root: &Path, nested: &Path) -> Config {
        Config {
            version: 1,
            defaults: Defaults::default(),
            projects: vec![
                ProjectConfig {
                    name: "alpha".to_string(),
                    local_root: root.to_string_lossy().to_string(),
                    remote_host: "example.com".to_string(),
                    remote_user: "user".to_string(),
                    remote_port: None,
                    remote_root: "/remote/alpha".to_string(),
                    ssh_identity: None,
                    enabled: true,
                    watch_debounce_ms: None,
                    max_backoff_ms: None,
                    ignore: Vec::new(),
                },
                ProjectConfig {
                    name: "beta".to_string(),
                    local_root: nested.to_string_lossy().to_string(),
                    remote_host: "example.com".to_string(),
                    remote_user: "user".to_string(),
                    remote_port: None,
                    remote_root: "/remote/beta".to_string(),
                    ssh_identity: None,
                    enabled: false,
                    watch_debounce_ms: None,
                    max_backoff_ms: None,
                    ignore: Vec::new(),
                },
            ],
        }
    }

    #[test]
    fn resolve_project_index_prefers_cwd_match() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root = temp_dir.path().join("alpha");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("create nested dir");
        let config = sample_config(&root, &nested);

        let cwd = nested.join("subdir");
        fs::create_dir_all(&cwd).expect("create cwd");

        let index = resolve_project_index(&config, None, &cwd).expect("selection");
        assert_eq!(config.projects[index].name, "beta");
    }

    #[test]
    fn status_payloads_keep_enabled_state() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root = temp_dir.path().join("alpha");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("create nested dir");
        let config = sample_config(&root, &nested);

        let statuses = status_payloads_from_projects(&config.projects);
        let alpha = statuses
            .iter()
            .find(|status| status.project == "alpha")
            .unwrap();
        let beta = statuses
            .iter()
            .find(|status| status.project == "beta")
            .unwrap();
        assert_eq!(alpha.state, "idle");
        assert_eq!(beta.state, "disabled");
    }
}
