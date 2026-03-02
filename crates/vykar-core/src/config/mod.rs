mod defaults;
mod deserialize;
mod hooks;
mod limits;
mod resolve;
mod sources;
mod types;
mod util;

// Re-export config schema types
pub use self::hooks::HOOK_COMMANDS;
pub use self::hooks::{HooksConfig, SourceHooksConfig};
pub use self::limits::{
    CpuLimitsConfig, IoLimitsConfig, NetworkLimitsConfig, ResourceLimitsConfig,
    DEFAULT_UPLOAD_CONCURRENCY,
};

// Re-export from submodules
pub use self::defaults::parse_human_duration;
pub(crate) use self::defaults::{default_max_pack_size, default_min_pack_size};
#[allow(deprecated)]
pub use self::resolve::load_config;
pub use self::resolve::{
    default_config_search_paths, load_and_resolve, minimal_config_template, resolve_config_path,
    select_repo, select_sources, ConfigSource, RepositoryEntry, ResolvedRepo,
};
pub use self::sources::{CommandDump, SourceEntry, SourceInput};
pub use self::types::*;
pub use self::util::{expand_tilde, label_from_path};
