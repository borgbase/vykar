use crate::config::RepositoryConfig;
use vykar_storage::StorageConfig;
use vykar_types::error::Result;

/// Convert a [`RepositoryConfig`] into a [`StorageConfig`] for backend construction.
fn storage_config_from_repo(cfg: &RepositoryConfig) -> StorageConfig {
    StorageConfig {
        url: cfg.url.clone(),
        region: cfg.region.clone(),
        access_key_id: cfg.access_key_id.clone(),
        secret_access_key: cfg.secret_access_key.clone(),
        sftp_key: cfg.sftp_key.clone(),
        sftp_known_hosts: cfg.sftp_known_hosts.clone(),
        max_connections: None,
        sftp_timeout: cfg.sftp_timeout,
        access_token: cfg.access_token.clone(),
        allow_insecure_http: cfg.allow_insecure_http,
        retry: cfg.retry.clone(),
    }
}

/// Build a storage backend from the repository configuration.
///
/// Convenience wrapper around [`vykar_storage::backend_from_config`] that
/// accepts a [`RepositoryConfig`] directly.
pub fn backend_from_config(
    cfg: &RepositoryConfig,
) -> Result<Box<dyn vykar_storage::StorageBackend>> {
    vykar_storage::backend_from_config(&storage_config_from_repo(cfg))
}

/// Build a storage backend with a specific connection pool size.
///
/// Used by backup to size the SFTP pool to match `upload_concurrency`,
/// avoiding wasted connections.
pub fn backend_from_config_with_pool(
    cfg: &RepositoryConfig,
    pool_size: usize,
) -> Result<Box<dyn vykar_storage::StorageBackend>> {
    let mut sc = storage_config_from_repo(cfg);
    sc.max_connections = Some(pool_size);
    vykar_storage::backend_from_config(&sc)
}
