pub mod local_backend;
pub mod rest_backend;
pub mod s3_backend;
#[cfg(feature = "backend-sftp")]
pub mod sftp_backend;

mod http_util;
mod retry;

#[cfg(feature = "backend-sftp")]
pub(crate) mod runtime;

#[cfg(feature = "backend-sftp")]
mod paths;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;

use vykar_types::error::{Result, VykarError};

/// Metadata sent to backends that support native advisory lock APIs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendLockInfo {
    pub hostname: String,
    pub pid: u64,
}

// Re-export wire-format types from vykar-protocol (shared with vykar-server).
pub use vykar_protocol::{
    RepackBlobRef, RepackOperationRequest, RepackOperationResult, RepackPlanRequest,
    RepackResultResponse, VerifyBlobRef, VerifyPackRequest, VerifyPackResult,
    VerifyPacksPlanRequest, VerifyPacksResponse, PROTOCOL_VERSION,
};

/// Abstract key-value storage for repository objects.
/// Keys are `/`-separated string paths (e.g. "packs/ab/ab01cd02...").
pub trait StorageBackend: Send + Sync {
    /// Read an object by key. Returns `None` if not found.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Write an object. Overwrites if it already exists.
    fn put(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Delete an object.
    fn delete(&self, key: &str) -> Result<()>;

    /// Check if an object exists.
    fn exists(&self, key: &str) -> Result<bool>;

    /// List all keys under a prefix. Returns full key paths.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Read a byte range from an object. Returns `None` if not found.
    ///
    /// When the key exists, the returned `Vec<u8>` **must** contain exactly
    /// `length` bytes. A short read is an error, not a silent truncation.
    /// `length` must be > 0 (callers must not request zero-length reads).
    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>>;

    /// Read a byte range from an object into a caller-provided buffer.
    ///
    /// Returns `Ok(true)` when the key exists (buf filled), `Ok(false)` when
    /// not found (buf cleared). LocalBackend overrides this to read directly
    /// into `buf`, achieving true buffer reuse. Other backends fall through to
    /// `get_range()` + move.
    fn get_range_into(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        match self.get_range(key, offset, length)? {
            Some(data) => {
                *buf = data;
                Ok(true)
            }
            None => {
                buf.clear();
                Ok(false)
            }
        }
    }

    /// Create a directory marker (no-op for flat object stores).
    fn create_dir(&self, key: &str) -> Result<()>;

    /// Write an object from an owned buffer. Backends can override to avoid
    /// an extra copy when the caller already owns the data.
    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.put(key, &data)
    }

    /// Return the size in bytes of an object. Returns `None` if not found.
    ///
    /// Backends should override this with a metadata-only operation (e.g.
    /// HTTP HEAD, `stat()`, `fs::metadata`) to avoid downloading the object.
    fn size(&self, key: &str) -> Result<Option<u64>> {
        Ok(self.get(key)?.map(|v| v.len() as u64))
    }

    /// Acquire an advisory lock using a backend-native API.
    ///
    /// Backends that don't support a lock API should return
    /// `VykarError::UnsupportedBackend`, so the caller can fall back to
    /// object-based lock files.
    fn acquire_advisory_lock(&self, _lock_id: &str, _info: &BackendLockInfo) -> Result<()> {
        Err(VykarError::UnsupportedBackend("advisory lock API".into()))
    }

    /// Release an advisory lock using a backend-native API.
    fn release_advisory_lock(&self, _lock_id: &str) -> Result<()> {
        Err(VykarError::UnsupportedBackend("advisory lock API".into()))
    }

    /// Execute a server-side repack plan when supported by the backend.
    fn server_repack(&self, _plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        Err(VykarError::UnsupportedBackend(
            "server-side repack API".into(),
        ))
    }

    /// Batch-delete keys using a backend-native API.
    fn batch_delete_keys(&self, _keys: &[String]) -> Result<()> {
        Err(VykarError::UnsupportedBackend("batch delete API".into()))
    }

    /// Server-side pack verification: hash check + header + blob boundary scan.
    fn server_verify_packs(&self, _plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        Err(VykarError::UnsupportedBackend(
            "server-side verify-packs API".into(),
        ))
    }

    /// Server-side repository directory scaffolding (keys/, snapshots/, locks/, packs/*).
    fn server_init(&self) -> Result<()> {
        Err(VykarError::UnsupportedBackend(
            "server-side init API".into(),
        ))
    }
}

impl StorageBackend for Arc<dyn StorageBackend> {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        (**self).get(key)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        (**self).put(key, data)
    }
    fn delete(&self, key: &str) -> Result<()> {
        (**self).delete(key)
    }
    fn exists(&self, key: &str) -> Result<bool> {
        (**self).exists(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        (**self).list(prefix)
    }
    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        (**self).get_range(key, offset, length)
    }
    fn get_range_into(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        (**self).get_range_into(key, offset, length, buf)
    }
    fn create_dir(&self, key: &str) -> Result<()> {
        (**self).create_dir(key)
    }
    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        (**self).put_owned(key, data)
    }
    fn size(&self, key: &str) -> Result<Option<u64>> {
        (**self).size(key)
    }
    fn acquire_advisory_lock(&self, lock_id: &str, info: &BackendLockInfo) -> Result<()> {
        (**self).acquire_advisory_lock(lock_id, info)
    }
    fn release_advisory_lock(&self, lock_id: &str) -> Result<()> {
        (**self).release_advisory_lock(lock_id)
    }
    fn server_repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        (**self).server_repack(plan)
    }
    fn batch_delete_keys(&self, keys: &[String]) -> Result<()> {
        (**self).batch_delete_keys(keys)
    }
    fn server_verify_packs(&self, plan: &VerifyPacksPlanRequest) -> Result<VerifyPacksResponse> {
        (**self).server_verify_packs(plan)
    }
    fn server_init(&self) -> Result<()> {
        (**self).server_init()
    }
}

/// Parsed repository URL.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedUrl {
    /// Local filesystem path.
    Local { path: String },
    /// S3 or S3-compatible storage.
    S3 {
        bucket: String,
        root: String,
        /// Explicit endpoint URL derived from the repository URL.
        endpoint: String,
    },
    /// SFTP remote storage.
    Sftp {
        user: Option<String>,
        host: String,
        port: Option<u16>,
        path: String,
    },
    /// REST backend (HTTP/HTTPS).
    Rest { url: String },
}

/// Parse a repository URL into its components.
///
/// Supported formats:
/// - Bare path (`/backups/repo`, `./relative`, `relative`) -> `Local`
/// - `file:///backups/repo` -> `Local`
/// - `s3://endpoint[:port]/bucket/prefix` -> `S3` over HTTPS
/// - `s3+http://endpoint[:port]/bucket/prefix` -> `S3` over HTTP (unsafe; blocked by default)
/// - `sftp://[user@]host[:port]/path` -> `Sftp`
/// - `http(s)://...` -> `Rest`
pub fn parse_repo_url(raw: &str) -> Result<ParsedUrl> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(VykarError::Config(
            "repository URL must not be empty".into(),
        ));
    }

    // Bare path: starts with `/`, `./`, or `../`
    if trimmed.starts_with('/')
        || trimmed.starts_with("./")
        || trimmed.starts_with("../")
        || trimmed == "."
        || trimmed == ".."
    {
        return Ok(ParsedUrl::Local {
            path: trimmed.to_string(),
        });
    }

    // Bare relative paths (without ./) are local too.
    if !trimmed.contains("://") {
        return Ok(ParsedUrl::Local {
            path: trimmed.to_string(),
        });
    }

    // Try to parse as URL
    let url = Url::parse(trimmed)
        .map_err(|e| VykarError::Config(format!("invalid repository URL '{trimmed}': {e}")))?;

    match url.scheme() {
        "file" => {
            let path = url.path().to_string();
            if path.is_empty() {
                return Err(VykarError::Config("file:// URL has empty path".into()));
            }
            Ok(ParsedUrl::Local { path })
        }
        "s3" | "s3+https" => parse_s3_url(&url, "https"),
        "s3+http" => parse_s3_url(&url, "http"),
        "sftp" => parse_sftp_url(&url),
        "http" | "https" => Ok(ParsedUrl::Rest {
            url: trimmed.to_string(),
        }),
        other => Err(VykarError::UnsupportedBackend(format!(
            "unsupported URL scheme: '{other}'"
        ))),
    }
}

/// Parse an S3 URL (`s3://` or `s3+http://`).
///
/// S3 URLs must always include an endpoint host and bucket path:
/// `s3://endpoint[:port]/bucket[/prefix]`.
fn parse_s3_url(url: &Url, endpoint_scheme: &str) -> Result<ParsedUrl> {
    let host = url
        .host_str()
        .ok_or_else(|| VykarError::Config("s3 URL is missing an endpoint host".into()))?;

    let port_suffix = url.port().map(|p| format!(":{p}")).unwrap_or_default();
    let endpoint = format!("{endpoint_scheme}://{host}{port_suffix}");

    let path = url.path().trim_start_matches('/');
    let (bucket, root) = path.split_once('/').unwrap_or((path, ""));
    if bucket.is_empty() {
        return Err(VykarError::Config(
            "s3 URL must include a bucket in the path (expected s3://endpoint/bucket[/prefix])"
                .into(),
        ));
    }
    Ok(ParsedUrl::S3 {
        bucket: bucket.to_string(),
        root: root.to_string(),
        endpoint,
    })
}

/// Parse an `sftp://` URL.
fn parse_sftp_url(url: &Url) -> Result<ParsedUrl> {
    let host = url
        .host_str()
        .ok_or_else(|| VykarError::Config("sftp:// URL is missing a host".into()))?;

    let user = if url.username().is_empty() {
        None
    } else {
        Some(url.username().to_string())
    };

    let path = url.path().to_string();

    Ok(ParsedUrl::Sftp {
        user,
        host: host.to_string(),
        port: url.port(),
        path,
    })
}

fn enforce_secure_http(url: &str, allow_insecure_http: bool, field_name: &str) -> Result<()> {
    let lowered = url.to_ascii_lowercase();
    if lowered.starts_with("http://") {
        if allow_insecure_http {
            tracing::warn!(
                "{field_name} uses plaintext HTTP; repository.allow_insecure_http=true enables this unsafe mode"
            );
            return Ok(());
        }
        return Err(VykarError::Config(format!(
            "{field_name} uses insecure HTTP and is blocked by default. \
Use HTTPS, or set repository.allow_insecure_http: true to permit plaintext HTTP (unsafe)."
        )));
    }
    Ok(())
}

/// Configuration for constructing a storage backend.
///
/// This is a decoupled subset of the full repository config, containing
/// only the fields needed by storage backends.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub url: String,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub sftp_key: Option<String>,
    pub sftp_known_hosts: Option<String>,
    pub sftp_max_connections: Option<usize>,
    pub access_token: Option<String>,
    pub allow_insecure_http: bool,
    pub retry: RetryConfig,
}

/// Retry configuration for remote storage backends (S3, SFTP, REST).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries).
    #[serde(default = "RetryConfig::default_max_retries")]
    pub max_retries: usize,
    /// Initial delay between retries in milliseconds.
    #[serde(default = "RetryConfig::default_retry_delay_ms")]
    pub retry_delay_ms: u64,
    /// Maximum delay between retries in milliseconds.
    #[serde(default = "RetryConfig::default_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
}

impl RetryConfig {
    fn default_max_retries() -> usize {
        3
    }
    fn default_retry_delay_ms() -> u64 {
        1000
    }
    fn default_retry_max_delay_ms() -> u64 {
        60_000
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: Self::default_max_retries(),
            retry_delay_ms: Self::default_retry_delay_ms(),
            retry_max_delay_ms: Self::default_retry_max_delay_ms(),
        }
    }
}

/// Build a storage backend from a `StorageConfig`.
pub fn backend_from_config(cfg: &StorageConfig) -> Result<Box<dyn StorageBackend>> {
    let parsed = parse_repo_url(&cfg.url)?;

    if let ParsedUrl::Rest { url } = &parsed {
        enforce_secure_http(url, cfg.allow_insecure_http, "repository.url")?;
    }

    if let ParsedUrl::S3 { endpoint, .. } = &parsed {
        enforce_secure_http(endpoint, cfg.allow_insecure_http, "repository.url")?;
    }

    match parsed {
        ParsedUrl::Local { path } => Ok(Box::new(local_backend::LocalBackend::new(&path)?)),
        ParsedUrl::S3 {
            bucket,
            root,
            endpoint,
        } => {
            let region = cfg.region.as_deref().unwrap_or("us-east-1");

            let access_key_id = cfg.access_key_id.as_deref().ok_or_else(|| {
                VykarError::Config("S3 requires access_key_id in repository config".into())
            })?;
            let secret_access_key = cfg.secret_access_key.as_deref().ok_or_else(|| {
                VykarError::Config("S3 requires secret_access_key in repository config".into())
            })?;

            Ok(Box::new(s3_backend::S3Backend::new(
                &bucket,
                region,
                &root,
                &endpoint,
                access_key_id,
                secret_access_key,
                cfg.retry.clone(),
            )?))
        }
        #[cfg(feature = "backend-sftp")]
        ParsedUrl::Sftp {
            user,
            host,
            port,
            path,
        } => Ok(Box::new(sftp_backend::SftpBackend::new(
            &host,
            user.as_deref(),
            port,
            &path,
            cfg.sftp_key.as_deref(),
            cfg.sftp_known_hosts.as_deref(),
            cfg.sftp_max_connections,
            cfg.retry.clone(),
        )?)),
        #[cfg(not(feature = "backend-sftp"))]
        ParsedUrl::Sftp { .. } => Err(VykarError::UnsupportedBackend(
            "sftp (compile with feature 'backend-sftp')".into(),
        )),
        ParsedUrl::Rest { url } => {
            let token = cfg.access_token.as_deref();
            Ok(Box::new(rest_backend::RestBackend::new(
                &url,
                token,
                cfg.retry.clone(),
            )?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bare_absolute_path() {
        let parsed = parse_repo_url("/backups/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "/backups/repo".into()
            }
        );
    }

    #[test]
    fn test_bare_relative_path() {
        let parsed = parse_repo_url("./my-repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "./my-repo".into()
            }
        );
    }

    #[test]
    fn test_bare_relative_path_without_dot_prefix() {
        let parsed = parse_repo_url("my-repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "my-repo".into()
            }
        );
    }

    #[test]
    fn test_file_url() {
        let parsed = parse_repo_url("file:///backups/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "/backups/repo".into()
            }
        );
    }

    #[test]
    fn test_s3_bucket_only() {
        let err = parse_repo_url("s3://my-bucket").unwrap_err();
        assert!(err.to_string().contains("must include a bucket"));
    }

    #[test]
    fn test_s3_bucket_with_prefix() {
        let parsed = parse_repo_url("s3://s3.us-east-1.amazonaws.com/my-bucket/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vykar".into(),
                endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            }
        );
    }

    #[test]
    fn test_s3_bucket_with_nested_prefix() {
        let parsed =
            parse_repo_url("s3://s3.us-east-1.amazonaws.com/my-bucket/backups/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "backups/vykar".into(),
                endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_with_port() {
        let parsed = parse_repo_url("s3://minio.local:9000/my-bucket/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vykar".into(),
                endpoint: "https://minio.local:9000".into(),
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_with_dot() {
        let parsed = parse_repo_url("s3://s3.example.com/my-bucket/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vykar".into(),
                endpoint: "https://s3.example.com".into(),
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_missing_bucket() {
        let err = parse_repo_url("s3://minio.local:9000").unwrap_err();
        assert!(err.to_string().contains("must include a bucket"));
    }

    #[test]
    fn test_s3_http_endpoint_scheme() {
        let parsed = parse_repo_url("s3+http://minio.local:9000/my-bucket/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vykar".into(),
                endpoint: "http://minio.local:9000".into(),
            }
        );
    }

    #[test]
    fn test_sftp_basic() {
        let parsed = parse_repo_url("sftp://nas.local/backups/vykar").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: None,
                host: "nas.local".into(),
                port: None,
                path: "/backups/vykar".into(),
            }
        );
    }

    #[test]
    fn test_sftp_with_user() {
        let parsed = parse_repo_url("sftp://backup@nas.local/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: Some("backup".into()),
                host: "nas.local".into(),
                port: None,
                path: "/repo".into(),
            }
        );
    }

    #[test]
    fn test_sftp_with_user_and_port() {
        let parsed = parse_repo_url("sftp://backup@nas.local:2222/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: Some("backup".into()),
                host: "nas.local".into(),
                port: Some(2222),
                path: "/repo".into(),
            }
        );
    }

    #[test]
    fn test_https_rest() {
        let parsed = parse_repo_url("https://backup.example.com/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Rest {
                url: "https://backup.example.com/repo".into(),
            }
        );
    }

    #[test]
    fn test_http_rest() {
        let parsed = parse_repo_url("http://localhost:8080/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Rest {
                url: "http://localhost:8080/repo".into(),
            }
        );
    }

    #[test]
    fn test_unsupported_scheme() {
        let err = parse_repo_url("ftp://host/path").unwrap_err();
        assert!(err.to_string().contains("unsupported URL scheme"));
    }

    #[test]
    fn test_invalid_url() {
        let err = parse_repo_url("http://[::1").unwrap_err();
        assert!(err.to_string().contains("invalid repository URL"));
    }

    #[test]
    fn test_empty_url_rejected() {
        let err = parse_repo_url("   ").unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    fn test_config(url: &str) -> StorageConfig {
        StorageConfig {
            url: url.to_string(),
            region: None,
            access_key_id: None,
            secret_access_key: None,
            sftp_key: None,
            sftp_known_hosts: None,
            sftp_max_connections: None,
            access_token: None,
            allow_insecure_http: false,
            retry: RetryConfig::default(),
        }
    }

    #[test]
    fn test_backend_rejects_http_rest_by_default() {
        let cfg = test_config("http://localhost:8080/repo");
        let err = match backend_from_config(&cfg) {
            Ok(_) => panic!("expected HTTP REST URL to be rejected"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("repository.url"));
        assert!(msg.contains("repository.allow_insecure_http: true"));
    }

    #[test]
    fn test_backend_rejects_http_s3_endpoint_by_default() {
        let mut cfg = test_config("s3+http://minio.local:9000/my-bucket/vykar");
        cfg.access_key_id = Some("test-key".into());
        cfg.secret_access_key = Some("test-secret".into());
        let err = match backend_from_config(&cfg) {
            Ok(_) => panic!("expected HTTP S3 endpoint to be rejected"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("repository.url"));
        assert!(msg.contains("repository.allow_insecure_http: true"));
    }

    #[test]
    fn test_backend_allows_http_rest_when_opted_in() {
        let mut cfg = test_config("http://localhost:8080/repo");
        cfg.allow_insecure_http = true;
        let backend = backend_from_config(&cfg);
        assert!(
            backend.is_ok(),
            "expected HTTP REST URL to be allowed when opted in"
        );
    }

    #[test]
    fn test_backend_allows_http_s3_endpoint_when_opted_in() {
        let mut cfg = test_config("s3+http://minio.local:9000/my-bucket/vykar");
        cfg.allow_insecure_http = true;
        cfg.access_key_id = Some("test-key".into());
        cfg.secret_access_key = Some("test-secret".into());
        let backend = backend_from_config(&cfg);
        assert!(
            backend.is_ok(),
            "expected HTTP S3 endpoint to be allowed when opted in"
        );
    }
}
