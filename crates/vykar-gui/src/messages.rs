use chrono::Local;
use vykar_core::snapshot::item::Item;

/// Build a `UiEvent::LogEntry` capturing the current local time for both date and timestamp.
pub(crate) fn log_entry_now(message: impl Into<String>) -> UiEvent {
    let now = Local::now();
    UiEvent::LogEntry {
        date: now.format("%b %d").to_string(),
        timestamp: now.format("%H:%M:%S").to_string(),
        message: message.into(),
    }
}

// ── Commands (UI → worker) ──

#[derive(Debug)]
pub(crate) enum AppCommand {
    RunBackupAll {
        scheduled: bool,
    },
    RunBackupRepo {
        repo_name: String,
    },
    RunBackupSource {
        source_label: String,
    },
    FetchAllRepoInfo,
    RefreshSnapshots {
        repo_selector: String,
    },
    FetchSnapshotContents {
        repo_name: String,
        snapshot_name: String,
    },
    RestoreSelected {
        repo_name: String,
        snapshot: String,
        dest: String,
        paths: Vec<String>,
    },
    DeleteSnapshot {
        repo_name: String,
        snapshot_name: String,
    },
    PruneRepo {
        repo_name: String,
    },
    FindFiles {
        repo_name: String,
        name_pattern: String,
    },
    OpenConfigFile,
    ReloadConfig,
    SwitchConfig,
    SaveAndApplyConfig {
        yaml_text: String,
    },
}

// ── Data transfer structs ──

#[derive(Debug, Clone)]
pub(crate) struct RepoInfoData {
    pub name: String,
    pub url: String,
    pub snapshots: String,
    pub last_snapshot: String,
    pub size: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotRowData {
    pub id: String,
    pub hostname: String,
    pub time_str: String,
    pub label: String,
    pub files: String,
    pub size: String,
    pub nfiles: Option<u64>,
    pub size_bytes: Option<u64>,
    pub time_epoch: i64,
    pub repo_name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceInfoData {
    pub label: String,
    pub paths: String,
    pub excludes: String,
    pub target_repos: String,
    /// Resolved list of repo names the source targets. Empty means "all repos".
    pub target_repo_names: Vec<String>,
    pub detail_paths: String,
    pub detail_excludes: String,
    pub detail_exclude_if_present: String,
    pub detail_flags: String,
    pub detail_hooks: String,
    pub detail_retention: String,
    pub detail_command_dumps: String,
}

#[derive(Debug, Clone)]
pub(crate) struct FindResultRow {
    pub path: String,
    pub snapshot: String,
    pub date: String,
    pub size: String,
    pub status: String,
}

// ── Events (worker → UI) ──

#[derive(Debug, Clone)]
pub(crate) enum UiEvent {
    Status(String),
    LogEntry {
        date: String,
        timestamp: String,
        message: String,
    },
    ConfigInfo {
        path: String,
        schedule_brief: String,
    },
    RepoNames(Vec<String>),
    RepoModelData {
        items: Vec<RepoInfoData>,
        labels: Vec<String>,
    },
    SourceModelData {
        items: Vec<SourceInfoData>,
        labels: Vec<String>,
    },
    SnapshotTableData {
        data: Vec<SnapshotRowData>,
    },
    SnapshotContentsData {
        repo_name: String,
        snapshot_name: String,
        items: Vec<Item>,
        source_paths: Vec<String>,
    },
    RestoreFinished {
        success: bool,
        message: String,
    },
    FindResultsData {
        rows: Vec<FindResultRow>,
    },
    ConfigText(String),
    ConfigSaveError(String),
    OperationStarted,
    OperationFinished,
    Quit,
    ShowWindow,
    TriggerSnapshotRefresh,
}
