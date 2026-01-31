use std::path::{Path, PathBuf};

use crate::config::ProjectConfig;

pub fn select_project_by_cwd<'a>(
    projects: &'a [ProjectConfig],
    cwd: &Path,
) -> Option<&'a ProjectConfig> {
    let cwd = normalize_path(cwd);
    let mut best_match: Option<(&ProjectConfig, usize)> = None;

    for project in projects {
        let root = normalize_path(Path::new(&project.local_root));
        if cwd.starts_with(&root) {
            let depth = root.components().count();
            if best_match
                .as_ref()
                .is_none_or(|(_, best_depth)| depth > *best_depth)
            {
                best_match = Some((project, depth));
            }
        }
    }

    best_match.map(|(project, _)| project)
}

fn normalize_path(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProjectConfig;
    use std::fs;

    #[test]
    fn selects_longest_prefix_match() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root_a = temp_dir.path().join("alpha");
        let root_b = root_a.join("nested");
        fs::create_dir_all(&root_b).expect("create nested dir");

        let project_a = ProjectConfig {
            name: "alpha".to_string(),
            local_root: root_a.to_string_lossy().to_string(),
            remote_host: "example.com".to_string(),
            remote_user: "user".to_string(),
            remote_port: None,
            remote_root: "/remote/alpha".to_string(),
            ssh_identity: None,
            enabled: true,
            watch_debounce_ms: None,
            max_backoff_ms: None,
            ignore: Vec::new(),
        };

        let project_b = ProjectConfig {
            name: "beta".to_string(),
            local_root: root_b.to_string_lossy().to_string(),
            remote_host: "example.com".to_string(),
            remote_user: "user".to_string(),
            remote_port: None,
            remote_root: "/remote/beta".to_string(),
            ssh_identity: None,
            enabled: true,
            watch_debounce_ms: None,
            max_backoff_ms: None,
            ignore: Vec::new(),
        };

        let cwd = root_b.join("subdir");
        fs::create_dir_all(&cwd).expect("create cwd");

        let projects = [project_a, project_b];
        let selected = select_project_by_cwd(&projects, &cwd).expect("expected a selection");
        assert_eq!(selected.name, "beta");
    }
}
