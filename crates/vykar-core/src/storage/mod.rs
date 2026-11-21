//! Storage backends. A backend is built from a `StorageConfig`;
//! `RepositoryConfig::storage_config` makes one from the client's settings.

pub use vykar_storage::{
    ParsedUrl, StorageBackend, StorageConfig, backend_from_config, parse_repo_url,
};
