mod discovery;
mod document;
mod env;
mod env_de;
mod resolution;
mod template;

pub use discovery::{ConfigSource, default_config_search_paths, resolve_config_path};
pub use document::RepositoryEntry;
#[allow(deprecated)]
pub use resolution::load_config;
pub use resolution::{ResolvedRepo, load_and_resolve, select_repo, select_sources};
pub use template::minimal_config_template;
