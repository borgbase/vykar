use std::collections::HashMap;
use std::path::Path;

use vykar_core::snapshot::item::{Item, ItemType};

pub(super) struct TransformedItem {
    pub(super) display_path: String,
    pub(super) item_path: String,
    pub(super) entry_type: ItemType,
    pub(super) mode: u32,
    pub(super) size: u64,
}

/// Compute the longest common directory-component prefix of all paths.
/// Returns empty string if paths is empty. Preserves the leading `/` for
/// absolute Unix paths; omits it for Windows-style paths (e.g. `C:/...`).
fn common_directory_prefix(paths: &[String]) -> String {
    fn split_components(p: &str) -> Vec<&str> {
        p.split('/').filter(|c| !c.is_empty()).collect()
    }

    let (first, rest) = match paths.split_first() {
        Some(parts) => parts,
        None => return String::new(),
    };

    let has_leading_slash = first.starts_with('/');
    let first_components = split_components(first);
    let mut common_len = first_components.len();

    for path in rest {
        let components = split_components(path);
        let limit = common_len.min(components.len());
        common_len = first_components
            .iter()
            .zip(components.iter())
            .take(limit)
            .take_while(|(a, b)| a == b)
            .count();
    }

    if common_len == 0 {
        return if has_leading_slash {
            "/".to_string()
        } else {
            String::new()
        };
    }

    let joined = first_components
        .get(..common_len)
        .expect("common_len <= first_components.len()")
        .join("/");
    if has_leading_slash {
        format!("/{joined}")
    } else {
        joined
    }
}

/// Transform items using source_paths to reconstruct absolute-ish display paths
/// with the common prefix stripped.
///
/// Returns `(transformed_items, common_prefix_for_display)`.
pub(super) fn transform_items(
    items: &[Item],
    source_paths: &[String],
) -> (Vec<TransformedItem>, String) {
    let source_paths: Vec<String> = source_paths.iter().map(|p| p.replace('\\', "/")).collect();
    let source_paths = source_paths.as_slice();

    if source_paths.is_empty() {
        let transformed = items
            .iter()
            .map(|item| TransformedItem {
                display_path: item.path.clone(),
                item_path: item.path.clone(),
                entry_type: item.entry_type,
                mode: item.mode,
                size: item.size,
            })
            .collect();
        return (transformed, String::new());
    }

    let mut common_prefix = common_directory_prefix(source_paths);

    if source_paths.iter().any(|sp| sp == &common_prefix) {
        common_prefix = Path::new(&common_prefix)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "/".to_string());
    }

    let strip_prefix = if common_prefix == "/" {
        ""
    } else {
        common_prefix.as_str()
    };

    if let [source] = source_paths {
        let display_prefix = source
            .strip_prefix(strip_prefix)
            .unwrap_or(source)
            .trim_start_matches('/');

        let transformed = items
            .iter()
            .map(|item| {
                let display_path = if display_prefix.is_empty() {
                    item.path.clone()
                } else if item.path.is_empty() {
                    display_prefix.to_string()
                } else {
                    format!("{display_prefix}/{}", item.path)
                };
                TransformedItem {
                    display_path,
                    item_path: item.path.clone(),
                    entry_type: item.entry_type,
                    mode: item.mode,
                    size: item.size,
                }
            })
            .collect();
        return (transformed, common_prefix);
    }

    let mut basename_to_display: HashMap<String, String> = HashMap::new();
    for source in source_paths {
        let basename = Path::new(source)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| source.clone());
        let display_prefix = source
            .strip_prefix(strip_prefix)
            .unwrap_or(source)
            .trim_start_matches('/')
            .to_string();
        basename_to_display.insert(basename, display_prefix);
    }

    let transformed = items
        .iter()
        .map(|item| {
            let first_component = item.path.split('/').next().unwrap_or("");
            let display_path =
                if let Some(display_prefix) = basename_to_display.get(first_component) {
                    let rest = item
                        .path
                        .strip_prefix(first_component)
                        .unwrap_or("")
                        .trim_start_matches('/');
                    if rest.is_empty() {
                        display_prefix.clone()
                    } else {
                        format!("{display_prefix}/{rest}")
                    }
                } else {
                    item.path.clone()
                };
            TransformedItem {
                display_path,
                item_path: item.path.clone(),
                entry_type: item.entry_type,
                mode: item.mode,
                size: item.size,
            }
        })
        .collect();

    (transformed, common_prefix)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::dir;
    use super::super::FileTree;

    #[test]
    fn common_prefix_return_values() {
        let items = vec![dir("Documents")];

        let (_, prefix) = FileTree::build_from_items(&items, &["/home/adam".to_string()]);
        assert_eq!(prefix, "/home");

        let items2 = vec![dir("user"), dir("data")];
        let (_, prefix) = FileTree::build_from_items(
            &items2,
            &["/home/user".to_string(), "/var/data".to_string()],
        );
        assert_eq!(prefix, "/");

        let (_, prefix) = FileTree::build_from_items(&items, &[]);
        assert_eq!(prefix, "");
    }
}
