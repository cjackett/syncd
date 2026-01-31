use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const DEFAULT_WATCH_DEBOUNCE_MS: u64 = 500;
pub const DEFAULT_MAX_BACKOFF_MS: u64 = 300_000;
pub const DEFAULT_MAX_PARALLEL_PROJECTS: u32 = 4;
pub const DEFAULT_IGNORE: &[&str] = &[
    ".git/",
    ".idea/",
    "target/",
    "node_modules/",
    "env/",
    "venv/",
    ".env",
    ".venv/",
    "__pycache__/",
];

pub fn load_or_default() -> Result<Config> {
    match Config::load()? {
        Some(config) => Ok(config),
        None => Ok(Config::new_with_defaults()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Defaults {
    pub watch_debounce_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
    pub max_parallel_projects: Option<u32>,
    #[serde(default)]
    pub ignore: Vec<String>,
}

impl Defaults {
    pub fn with_default_values() -> Self {
        Self {
            watch_debounce_ms: Some(DEFAULT_WATCH_DEBOUNCE_MS),
            max_backoff_ms: Some(DEFAULT_MAX_BACKOFF_MS),
            max_parallel_projects: Some(DEFAULT_MAX_PARALLEL_PROJECTS),
            ignore: DEFAULT_IGNORE
                .iter()
                .map(|item| (*item).to_string())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub version: u32,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub projects: Vec<ProjectConfig>,
}

impl Config {
    pub fn new_with_defaults() -> Self {
        Self {
            version: 1,
            defaults: Defaults::with_default_values(),
            projects: Vec::new(),
        }
    }

    pub fn load() -> Result<Option<Self>> {
        let path = config_path();
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("reading config from {}", path.display()))?;
        let config: Self = toml::from_str(&contents)
            .with_context(|| format!("parsing config from {}", path.display()))?;
        config.validate()?;
        Ok(Some(config))
    }

    pub fn save(&self) -> Result<()> {
        self.validate()?;
        let path = config_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating config dir {}", parent.display()))?;
        }
        let contents = toml::to_string_pretty(self).context("serializing config")?;
        fs::write(&path, contents).with_context(|| format!("writing config {}", path.display()))?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            bail!("config version must be 1");
        }

        if self.defaults.watch_debounce_ms == Some(0) {
            bail!("defaults.watch_debounce_ms must be > 0");
        }
        if self.defaults.max_backoff_ms == Some(0) {
            bail!("defaults.max_backoff_ms must be > 0");
        }

        let mut names = HashSet::new();
        for project in &self.projects {
            project.validate()?;
            if !names.insert(project.name.as_str()) {
                bail!("project name '{}' is not unique", project.name);
            }
        }
        Ok(())
    }

    pub fn project_by_name(&self, name: &str) -> Option<&ProjectConfig> {
        self.projects.iter().find(|project| project.name == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    pub local_root: String,
    pub remote_host: String,
    pub remote_user: String,
    pub remote_port: Option<u16>,
    pub remote_root: String,
    pub ssh_identity: Option<String>,
    pub enabled: bool,
    pub watch_debounce_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
    #[serde(default)]
    pub ignore: Vec<String>,
}

impl ProjectConfig {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("project name is required");
        }
        if self.local_root.trim().is_empty() {
            bail!("project local_root is required");
        }
        if self.remote_host.trim().is_empty() {
            bail!("project remote_host is required");
        }
        if self.remote_user.trim().is_empty() {
            bail!("project remote_user is required");
        }
        if self.remote_root.trim().is_empty() {
            bail!("project remote_root is required");
        }

        let local_root = Path::new(&self.local_root);
        if !local_root.is_absolute() {
            bail!("project local_root must be absolute: {}", self.local_root);
        }
        if !local_root.exists() {
            bail!("project local_root must exist: {}", self.local_root);
        }

        let remote_root = Path::new(&self.remote_root);
        if !remote_root.is_absolute() {
            bail!("project remote_root must be absolute: {}", self.remote_root);
        }

        if self.watch_debounce_ms == Some(0) {
            bail!("project watch_debounce_ms must be > 0");
        }
        if self.max_backoff_ms == Some(0) {
            bail!("project max_backoff_ms must be > 0");
        }

        Ok(())
    }

    pub fn remote_string(&self) -> String {
        format!(
            "{}@{}:{}",
            self.remote_user, self.remote_host, self.remote_root
        )
    }
}

pub fn config_path() -> PathBuf {
    if let Some(base) = env::var_os("XDG_CONFIG_HOME") {
        return PathBuf::from(base).join("syncd").join("config.toml");
    }

    let home = env::var_os("HOME").unwrap_or_default();
    if home.is_empty() {
        return PathBuf::from(".config/syncd/config.toml");
    }
    PathBuf::from(home)
        .join(".config")
        .join("syncd")
        .join("config.toml")
}

pub fn expand_tilde(input: &str) -> String {
    if let Some(stripped) = input.strip_prefix("~/")
        && let Some(home) = env::var_os("HOME")
    {
        return PathBuf::from(home)
            .join(stripped)
            .to_string_lossy()
            .to_string();
    }
    input.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_unique_names() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let project_path = temp_dir.path().join("project");
        fs::create_dir_all(&project_path).expect("create project dir");

        let project = ProjectConfig {
            name: "alpha".to_string(),
            local_root: project_path.to_string_lossy().to_string(),
            remote_host: "example.com".to_string(),
            remote_user: "user".to_string(),
            remote_port: None,
            remote_root: "/remote/path".to_string(),
            ssh_identity: None,
            enabled: true,
            watch_debounce_ms: None,
            max_backoff_ms: None,
            ignore: Vec::new(),
        };

        let config = Config {
            version: 1,
            defaults: Defaults::default(),
            projects: vec![project.clone(), project],
        };

        let err = config
            .validate()
            .expect_err("expected duplicate name error");
        assert!(err.to_string().contains("not unique"));
    }

    #[test]
    fn validates_paths() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let project_path = temp_dir.path().join("project");
        fs::create_dir_all(&project_path).expect("create project dir");

        let project = ProjectConfig {
            name: "alpha".to_string(),
            local_root: project_path.to_string_lossy().to_string(),
            remote_host: "example.com".to_string(),
            remote_user: "user".to_string(),
            remote_port: None,
            remote_root: "/remote/path".to_string(),
            ssh_identity: None,
            enabled: true,
            watch_debounce_ms: Some(1),
            max_backoff_ms: Some(1),
            ignore: Vec::new(),
        };

        let config = Config {
            version: 1,
            defaults: Defaults::default(),
            projects: vec![project],
        };

        config.validate().expect("config should be valid");
    }
}
