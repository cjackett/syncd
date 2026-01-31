use crate::paths::NormalizedPath;

#[derive(Clone, Debug)]
struct IgnoreRule {
    rule: String,
    has_slash: bool,
    is_dir: bool,
}

impl IgnoreRule {
    fn parse(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        let mut rule = trimmed.replace("\\", "/");
        while rule.starts_with("./") {
            rule = rule.trim_start_matches("./").to_string();
        }
        if rule.starts_with('/') {
            rule = rule.trim_start_matches('/').to_string();
        }
        if rule.is_empty() {
            return None;
        }
        let has_slash = rule.contains('/');
        let is_dir = rule.ends_with('/');
        if is_dir {
            rule = rule.trim_end_matches('/').to_string() + "/";
        }
        Some(Self {
            rule,
            has_slash,
            is_dir,
        })
    }

    fn matches(&self, path: &NormalizedPath) -> bool {
        let candidate = path.as_str();
        if self.has_slash {
            if self.is_dir {
                return candidate.starts_with(&self.rule);
            }
            return candidate == self.rule;
        }

        let target = self.rule.trim_end_matches('/');
        candidate.split('/').any(|part| part == target)
    }
}

#[derive(Clone, Debug)]
pub struct IgnoreMatcher {
    rules: Vec<IgnoreRule>,
}

impl IgnoreMatcher {
    pub fn new<I, S>(rules: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let parsed = rules
            .into_iter()
            .filter_map(|rule| IgnoreRule::parse(rule.as_ref()))
            .collect();
        Self { rules: parsed }
    }

    pub fn is_ignored(&self, path: &NormalizedPath) -> bool {
        self.rules.iter().any(|rule| rule.matches(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::NormalizedPath;

    #[test]
    fn matches_directory_prefix() {
        let matcher = IgnoreMatcher::new(["target/"]);
        let path = NormalizedPath::new("target/debug/app".to_string());
        assert!(matcher.is_ignored(&path));
    }

    #[test]
    fn matches_basename_anywhere() {
        let matcher = IgnoreMatcher::new([".env"]);
        let path = NormalizedPath::new("config/.env".to_string());
        assert!(matcher.is_ignored(&path));
    }

    #[test]
    fn matches_explicit_relative_path() {
        let matcher = IgnoreMatcher::new(["data/tmp/"]);
        let path = NormalizedPath::new("data/tmp/cache.bin".to_string());
        assert!(matcher.is_ignored(&path));
    }

    #[test]
    fn ignores_normalizes_leading_dot_slash() {
        let matcher = IgnoreMatcher::new(["./node_modules/"]);
        let path = NormalizedPath::new("node_modules/pkg/index.js".to_string());
        assert!(matcher.is_ignored(&path));
    }
}
