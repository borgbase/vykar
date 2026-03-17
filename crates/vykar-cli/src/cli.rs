use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "vykar",
    version,
    about = "Fast, encrypted, deduplicated backups",
    after_help = "\
Configuration file lookup order:
  1. --config <path>             (explicit flag)
  2. $VYKAR_CONFIG                (environment variable)
  3. ./vykar.yaml                (project)
  4. Platform user config dir + /vykar/config.yaml (e.g. ~/.config or %APPDATA%)
  5. Platform system config path (Unix: /etc/vykar/config.yaml, Windows: %PROGRAMDATA%/vykar/config.yaml)

Environment variables:
  VYKAR_CONFIG       Path to configuration file (overrides default search)
  VYKAR_PASSPHRASE   Repository passphrase (skips interactive prompt)"
)]
pub(crate) struct Cli {
    /// Path to configuration file (overrides VYKAR_CONFIG and default search)
    #[arg(short, long)]
    pub config: Option<String>,

    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Accept a changed repository identity and re-pin.
    /// Requires -R/--repo when multiple repositories are configured.
    #[arg(long, global = true)]
    pub trust_repo: bool,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Initialize a new repository
    Init {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,
    },

    /// Back up files to a new snapshot
    Backup {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Label for the snapshot (sets source_label for ad-hoc backups)
        #[arg(short = 'l', long)]
        label: Option<String>,

        /// Compression algorithm override (lz4, zstd, none)
        #[arg(long)]
        compression: Option<String>,

        /// Filter which configured sources to back up (by label)
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Parallel backend connections (1-16, overrides config)
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=16))]
        connections: Option<u16>,

        /// Ad-hoc paths to back up (grouped into a single snapshot)
        paths: Vec<String>,
    },

    /// List snapshots
    List {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Filter displayed snapshots by source label
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Show only the N most recent snapshots
        #[arg(long)]
        last: Option<usize>,
    },

    /// Inspect snapshot contents and metadata
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },

    /// Restore files from a snapshot
    Restore {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Snapshot to restore from (name or "latest")
        snapshot: String,

        /// Destination directory to restore into
        dest: String,

        /// Only restore paths matching this glob pattern
        #[arg(long)]
        pattern: Option<String>,
    },

    /// Delete an entire repository permanently
    Delete {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Skip interactive confirmation (for scripting)
        #[arg(long)]
        yes_delete_this_repo: bool,
    },

    /// Prune snapshots according to retention policy
    Prune {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Only show what would be pruned, don't actually prune
        #[arg(short = 'n', long)]
        dry_run: bool,

        /// Show detailed list of kept/pruned snapshots with reasons
        #[arg(long)]
        list: bool,

        /// Apply retention only to snapshots matching these source labels
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Run compact after pruning to reclaim space from orphaned blobs
        #[arg(long)]
        compact: bool,
    },

    /// Verify repository integrity
    Check {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Read and verify all data chunks (slow but thorough)
        #[arg(long)]
        verify_data: bool,

        /// Force client-side download + crypto verification even when the
        /// server supports server-side pack verification. Use this if you
        /// don't trust the server to report honestly.
        /// Ignored in --repair mode (repair always verifies client-side).
        #[arg(long)]
        distrust_server: bool,

        /// Detect and fix integrity issues
        #[arg(long)]
        repair: bool,

        /// Dry-run: show repair plan without applying
        #[arg(short = 'n', long, requires = "repair")]
        dry_run: bool,

        /// Skip interactive confirmation (for scripting)
        #[arg(long, requires = "repair")]
        yes: bool,
    },

    /// Show repository statistics and snapshot totals
    Info {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,
    },

    /// Generate a minimal configuration file
    Config {
        /// Destination path (skips interactive prompt)
        #[arg(short, long)]
        dest: Option<String>,
    },

    /// Browse snapshots via a local WebDAV server
    Mount {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Serve a single snapshot (omit for all snapshots)
        #[arg(long)]
        snapshot: Option<String>,

        /// Expose only snapshots matching these source labels
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Listen address (default: 127.0.0.1:8080)
        #[arg(long, default_value = "127.0.0.1:8080")]
        address: String,

        /// LRU chunk cache size in entries (default: 256)
        #[arg(long, default_value = "256")]
        cache_size: usize,
    },

    /// Remove stale repository locks left by killed processes
    BreakLock {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Also remove all session markers (orphaned by crashed backups)
        #[arg(long)]
        sessions: bool,
    },

    /// Run scheduled backups as a foreground daemon
    Daemon,

    /// Free repository space by compacting pack files
    Compact {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Minimum percentage of unused space to trigger repack
        #[arg(long)]
        threshold: Option<f64>,

        /// Maximum total bytes to repack (e.g. 500M, 2G)
        #[arg(long)]
        max_repack_size: Option<String>,

        /// Only show what would be compacted, don't actually do it
        #[arg(short = 'n', long)]
        dry_run: bool,
    },
}

#[derive(Clone, clap::ValueEnum)]
pub(crate) enum SortField {
    Name,
    Size,
    Mtime,
}

#[derive(Subcommand)]
pub(crate) enum SnapshotCommand {
    /// Show contents of a snapshot
    List {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,
        /// Snapshot to inspect (name or "latest")
        snapshot: String,
        /// Show only files under this subtree
        #[arg(long)]
        path: Option<String>,
        /// Show permissions, size, mtime
        #[arg(long)]
        long: bool,
        /// Sort output (default: name)
        #[arg(long, value_enum, default_value_t = SortField::Name)]
        sort: SortField,
    },
    /// Show metadata of a snapshot
    Info {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,
        /// Snapshot to inspect (name or "latest")
        snapshot: String,
    },
    /// Delete a specific snapshot
    Delete {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,
        /// Snapshot name to delete
        snapshot: String,
        /// Only show what would be deleted, don't actually delete
        #[arg(short = 'n', long)]
        dry_run: bool,
    },
    /// Find files across snapshots
    Find {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,
        /// Starting directory (default: root)
        path: Option<String>,
        /// Filter by source label
        #[arg(short = 'S', long = "source", help_heading = "Scope Options")]
        source: Option<String>,
        /// Search only the last N snapshots (must be >= 1)
        #[arg(
            long,
            value_parser = clap::value_parser!(u64).range(1..),
            help_heading = "Scope Options"
        )]
        last: Option<u64>,
        /// Match filename by glob pattern (case-sensitive)
        #[arg(long = "name", help_heading = "Filter Options")]
        name: Option<String>,
        /// Match filename by glob pattern (case-insensitive)
        #[arg(long = "iname", help_heading = "Filter Options")]
        iname: Option<String>,
        /// Filter by entry type: f (file), d (directory), l (symlink)
        #[arg(long = "type", value_name = "TYPE", help_heading = "Filter Options")]
        entry_type: Option<String>,
        /// Only include items modified within this time span (e.g. 24h, 7d, 2w, 6m, 1y)
        #[arg(long, help_heading = "Filter Options")]
        since: Option<String>,
        /// Only include items at least this size (e.g. 1M, 500K)
        #[arg(long, help_heading = "Filter Options")]
        larger: Option<String>,
        /// Only include items at most this size (e.g. 10M, 1G)
        #[arg(long, help_heading = "Filter Options")]
        smaller: Option<String>,
    },
}

impl SnapshotCommand {
    pub(crate) fn repo(&self) -> Option<&str> {
        match self {
            Self::List { repo, .. }
            | Self::Info { repo, .. }
            | Self::Delete { repo, .. }
            | Self::Find { repo, .. } => repo.as_deref(),
        }
    }
}

impl Commands {
    pub(crate) fn repo(&self) -> Option<&str> {
        match self {
            Self::Init { repo, .. }
            | Self::Backup { repo, .. }
            | Self::List { repo, .. }
            | Self::Restore { repo, .. }
            | Self::Delete { repo, .. }
            | Self::Prune { repo, .. }
            | Self::Check { repo, .. }
            | Self::Info { repo, .. }
            | Self::Mount { repo, .. }
            | Self::BreakLock { repo, .. }
            | Self::Compact { repo, .. } => repo.as_deref(),
            Self::Snapshot { command, .. } => command.repo(),
            Self::Config { .. } | Self::Daemon => None,
        }
    }

    /// Returns the targeted snapshot name, if the command references one.
    pub(crate) fn snapshot_name(&self) -> Option<&str> {
        match self {
            Self::Restore { snapshot, .. } => Some(snapshot),
            Self::Snapshot {
                command:
                    SnapshotCommand::List { snapshot, .. }
                    | SnapshotCommand::Info { snapshot, .. }
                    | SnapshotCommand::Delete { snapshot, .. },
            } => Some(snapshot),
            Self::Mount { snapshot, .. } => snapshot.as_deref(),
            _ => None,
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Init { .. } => "init",
            Self::Backup { .. } => "backup",
            Self::List { .. } => "list",
            Self::Restore { .. } => "restore",
            Self::Delete { .. } => "delete",
            Self::Prune { .. } => "prune",
            Self::Check { .. } => "check",
            Self::Info { .. } => "info",
            Self::Mount { .. } => "mount",
            Self::BreakLock { .. } => "break-lock",
            Self::Compact { .. } => "compact",
            Self::Snapshot { .. } => "snapshot",
            Self::Config { .. } => "config",
            Self::Daemon => "daemon",
        }
    }
}
