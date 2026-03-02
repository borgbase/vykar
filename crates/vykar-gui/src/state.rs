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
}

fn state_file_path() -> Option<PathBuf> {
    vykar_core::platform::paths::config_dir().map(|d| d.join("vykar").join("gui_state.json"))
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
