use crate::config::RepositoryConfig;
use vykar_storage::StorageConfig;
use vykar_types::error::Result;

pub use vykar_storage::StorageBackend;

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
        s3_soft_delete: cfg.s3_soft_delete,
    }
}

/// Build a storage backend with a specific connection pool size.
///
/// The `connections` parameter sizes the SFTP pool and controls parallelism
/// for all storage operations.
pub fn backend_from_config(
    cfg: &RepositoryConfig,
    connections: usize,
) -> Result<Box<dyn vykar_storage::StorageBackend>> {
    let mut sc = storage_config_from_repo(cfg);
    sc.max_connections = Some(connections);
    vykar_storage::backend_from_config(&sc)
}
