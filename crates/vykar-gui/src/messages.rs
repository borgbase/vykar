use chrono::Local;
use vykar_core::commands::diff::DiffChangeKind;
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
    DiffSnapshots {
        repo_name: String,
        snapshot_a: String,
        snapshot_b: String,
    },
    DeleteSnapshots {
        repo_name: String,
        snapshot_names: Vec<String>,
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
    ClearRepoLocks {
        repo_name: String,
    },
    ClearRepoSessions {
        repo_name: String,
    },
    StartMount {
        repo_name: String,
        snapshot_name: Option<String>,
    },
    StopMount,
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

/// Multi-row selection state for the Snapshots table. Indices align with
/// `snapshot_data` (and thus the rows model). Reset whenever the rows model
/// is repopulated.
#[derive(Debug, Default)]
pub(crate) struct SnapshotSelection {
    pub selected: Vec<bool>,
    pub anchor: Option<usize>,
}

impl SnapshotSelection {
    pub fn reset(&mut self, len: usize) {
        self.selected = vec![false; len];
        self.anchor = None;
    }

    pub fn count(&self) -> i32 {
        self.selected.iter().filter(|s| **s).count() as i32
    }
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
    pub mtime: String,
    pub size: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub(crate) struct FindSnapshotGroup {
    pub snapshot_id: String,
    pub snapshot_time: String,
    pub rows: Vec<FindResultRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct DiffResultRow {
    pub change: DiffChangeKind,
    pub path: String,
    pub old_size_bytes: Option<u64>,
    pub new_size_bytes: Option<u64>,
    pub delta_bytes: i64,
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
    DiffResultsData {
        repo_name: String,
        snapshot_a: String,
        snapshot_b: String,
        base_snapshot: String,
        target_snapshot: String,
        rows: Vec<DiffResultRow>,
        error: Option<String>,
    },
    FindResultsData {
        groups: Vec<FindSnapshotGroup>,
    },
    ConfigText(String),
    ConfigSaveError(String),
    OperationStarted,
    OperationFinished,
    Quit,
    ShowWindow,
    TriggerSnapshotRefresh,
    MountStarted {
        url: String,
    },
    MountStopped,
    MountFailed {
        message: String,
    },
}
