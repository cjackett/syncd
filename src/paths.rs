use std::fmt;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NormalizedPath(String);

impl NormalizedPath {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NormalizedPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub fn normalize_relative_path(root: &Path, path: &Path) -> Option<NormalizedPath> {
    let root_norm = normalize_absolute(root);
    let path_norm = normalize_absolute(path);
    let rel = path_norm.strip_prefix(&root_norm).ok()?;
    let rel_str = components_to_forward_slash(rel);
    if rel_str.is_empty() {
        return None;
    }
    Some(NormalizedPath::new(rel_str))
}

fn normalize_absolute(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => out.push(Component::RootDir.as_os_str()),
            Component::Prefix(prefix) => out.push(prefix.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(part) => out.push(part),
        }
    }
    out
}

fn components_to_forward_slash(path: &Path) -> String {
    let mut parts = Vec::new();
    for component in path.components() {
        if let Component::Normal(part) = component {
            parts.push(part.to_string_lossy());
        }
    }
    parts.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_relative_path_strips_root() {
        let root = Path::new("/tmp/project");
        let path = Path::new("/tmp/project/src/lib.rs");
        let rel = normalize_relative_path(root, path).unwrap();
        assert_eq!(rel.as_str(), "src/lib.rs");
    }

    #[test]
    fn normalize_relative_path_rejects_root_itself() {
        let root = Path::new("/tmp/project");
        let path = Path::new("/tmp/project");
        assert!(normalize_relative_path(root, path).is_none());
    }

    #[test]
    fn normalize_relative_path_handles_dot_segments() {
        let root = Path::new("/tmp/project/./");
        let path = Path::new("/tmp/project/./src/../src/main.rs");
        let rel = normalize_relative_path(root, path).unwrap();
        assert_eq!(rel.as_str(), "src/main.rs");
    }
}
