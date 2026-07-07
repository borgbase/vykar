use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GuiState {
    /// Last config file path (used when no config found via standard search).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
    /// Window width in logical pixels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_width: Option<f32>,
    /// Window height in logical pixels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_height: Option<f32>,
    /// Whether to start with the window hidden (tray only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_in_background: Option<bool>,
    /// Last active page as ordinal. Maps to `crate::Page` via [`page_from_i32`] /
    /// [`page_to_i32`]. Stored as i32 so old state files remain readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page: Option<i32>,
    /// Last selected repository name. Resolved to an index after the repo
    /// model arrives; survives reordering/renaming better than an index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_repo_name: Option<String>,
}

/// Convert the persisted ordinal to a `Page`, falling back to `Overview` for
/// unrecognized values (e.g. state written by a newer version).
pub fn page_from_i32(i: i32) -> crate::Page {
    match i {
        0 => crate::Page::Overview,
        1 => crate::Page::Snapshots,
        2 => crate::Page::Find,
        3 => crate::Page::Sources,
        4 => crate::Page::Advanced,
        5 => crate::Page::Log,
        6 => crate::Page::Settings,
        _ => crate::Page::Overview,
    }
}

pub fn page_to_i32(p: crate::Page) -> i32 {
    match p {
        crate::Page::Overview => 0,
        crate::Page::Snapshots => 1,
        crate::Page::Find => 2,
        crate::Page::Sources => 3,
        crate::Page::Advanced => 4,
        crate::Page::Log => 5,
        crate::Page::Settings => 6,
    }
}

fn state_file_path() -> Option<PathBuf> {
    vykar_common::paths::config_dir().map(|d| d.join("vykar").join("gui_state.json"))
}

pub fn load() -> GuiState {
    let path = match state_file_path() {
        Some(p) => p,
        None => return GuiState::default(),
    };
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save(state: &GuiState) {
    let path = match state_file_path() {
        Some(p) => p,
        None => return,
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = serde_json::to_string_pretty(state)
        .ok()
        .map(|json| std::fs::write(&path, json));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_with_start_in_background() {
        let state = GuiState {
            config_path: Some("/tmp/vykar.yaml".into()),
            window_width: Some(1100.0),
            window_height: Some(760.0),
            start_in_background: Some(true),
            last_page: Some(2),
            last_repo_name: Some("local-backup".into()),
        };
        let json = serde_json::to_string(&state).unwrap();
        let restored: GuiState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.start_in_background, Some(true));
        assert_eq!(restored.last_repo_name.as_deref(), Some("local-backup"));
    }

    /// Every `Page` variant, in declaration order. Keep in sync with the enum
    /// in `ui/components/types.slint` and with [`page_from_i32`]/[`page_to_i32`].
    /// A missing/extra variant here (or a reordered one in the Slint enum) is
    /// caught by the round-trip below and by the exhaustive `page_to_i32` match.
    const ALL_PAGES: [crate::Page; 7] = [
        crate::Page::Overview,
        crate::Page::Snapshots,
        crate::Page::Find,
        crate::Page::Sources,
        crate::Page::Advanced,
        crate::Page::Log,
        crate::Page::Settings,
    ];

    #[test]
    fn page_ordinals_round_trip_and_are_contiguous() {
        for (expected, page) in ALL_PAGES.iter().enumerate() {
            let ord = page_to_i32(*page);
            assert_eq!(
                ord, expected as i32,
                "page_to_i32 ordinal drifted from declaration order for {page:?}"
            );
            assert_eq!(
                page_from_i32(ord),
                *page,
                "page_from_i32/page_to_i32 disagree for {page:?}"
            );
        }
        // Ordinals form a contiguous 0..N with no gaps or duplicates.
        let ordinals: Vec<i32> = ALL_PAGES.iter().map(|p| page_to_i32(*p)).collect();
        assert_eq!(ordinals, (0..ALL_PAGES.len() as i32).collect::<Vec<_>>());
    }

    #[test]
    fn page_from_i32_out_of_range_falls_back_to_overview() {
        assert_eq!(page_from_i32(-1), crate::Page::Overview);
        assert_eq!(page_from_i32(999), crate::Page::Overview);
    }

    #[test]
    fn backwards_compat_missing_field() {
        // Old gui_state.json without start_in_background.
        let json = r#"{"config_path":"/tmp/vykar.yaml","window_width":1100.0}"#;
        let state: GuiState = serde_json::from_str(json).unwrap();
        assert_eq!(state.start_in_background, None);
    }
}
