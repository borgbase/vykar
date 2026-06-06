mod discovery;
mod document;
mod env;
mod resolution;
mod template;
#[cfg(test)]
mod test_support;

pub use discovery::{default_config_search_paths, resolve_config_path, ConfigSource};
pub use document::RepositoryEntry;
#[allow(deprecated)]
pub use resolution::load_config;
pub use resolution::{load_and_resolve, select_repo, select_sources, ResolvedRepo};
pub use template::minimal_config_template;
