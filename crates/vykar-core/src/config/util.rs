use std::path::Path;

use crate::platform::paths;

/// Expand a leading `~` or `~/` to the user's home directory.
pub fn expand_tilde(path: &str) -> String {
    if path == "~" {
        if let Some(home) = paths::home_dir() {
            return home.to_string_lossy().to_string();
        }
    }
    if let Some(suffix) = path.strip_prefix("~/") {
        if let Some(home) = paths::home_dir() {
            return home.join(suffix).to_string_lossy().to_string();
        }
    }
    path.to_string()
}

/// Derive a label from a path by taking the last component (basename).
pub fn label_from_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_tilde_home_only() {
        let home = paths::home_dir().unwrap();
        assert_eq!(expand_tilde("~"), home.to_string_lossy().to_string());
    }
}
