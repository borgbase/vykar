use crate::config::RepositoryConfig;
use vykar_storage::StorageConfig;
use vykar_types::error::Result;

pub use vykar_storage::StorageBackend;

/// Convert a [`RepositoryConfig`] into a [`StorageConfig`] for backend construction.
///
/// `cfg` is destructured exhaustively (no `..` rest pattern) so that adding a
/// field to [`RepositoryConfig`] fails to compile here rather than being
/// silently dropped on the way to the backend.
fn storage_config_from_repo(cfg: &RepositoryConfig) -> StorageConfig {
    let RepositoryConfig {
        url,
        region,
        access_key_id,
        secret_access_key,
        sftp_key,
        sftp_known_hosts,
        sftp_timeout,
        access_token,
        allow_insecure_http,
        // Pack sizing is a repository-format concern, not a transport one.
        min_pack_size: _,
        max_pack_size: _,
        retry,
        s3_soft_delete,
    } = cfg;

    StorageConfig {
        url: url.clone(),
        region: region.clone(),
        access_key_id: access_key_id.clone(),
        secret_access_key: secret_access_key.clone(),
        sftp_key: sftp_key.clone(),
        sftp_known_hosts: sftp_known_hosts.clone(),
        max_connections: None,
        sftp_timeout: *sftp_timeout,
        access_token: access_token.clone(),
        allow_insecure_http: *allow_insecure_http,
        retry: *retry,
        s3_soft_delete: *s3_soft_delete,
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
