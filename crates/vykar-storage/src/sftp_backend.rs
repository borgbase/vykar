use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use russh::client;
use russh::keys::known_hosts::{known_host_keys_path, learn_known_hosts_path};
use russh::keys::ssh_key;
use russh::keys::{load_secret_key, PrivateKey, PrivateKeyWithHashAlg};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{OpenFlags, StatusCode};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::paths;
use crate::RetryConfig;
use crate::StorageBackend;
use vykar_types::error::{Result, VykarError};

use super::runtime::ASYNC_RUNTIME;

/// Connection timeout for SSH handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Inactivity timeout for established SSH sessions.
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(300);

/// Default number of pooled SFTP connections.
const DEFAULT_SFTP_MAX_CONNECTIONS: usize = 4;

/// Hard upper bound for pooled SFTP connections.
const MAX_SFTP_MAX_CONNECTIONS: usize = 32;

/// Safety cap for one `get_range` request.
const MAX_GET_RANGE_BYTES: u64 = 1024 * 1024 * 1024;

/// Default per-request SFTP timeout in seconds.
/// Generous enough for slow/congested links, but still detects dead connections.
const DEFAULT_SFTP_TIMEOUT_SECS: u64 = 30;

/// Minimum per-request SFTP timeout in seconds.
/// Prevents accidental zero/near-zero values that would cause immediate timeouts.
const MIN_SFTP_TIMEOUT_SECS: u64 = 5;

/// Maximum per-request SFTP timeout in seconds (5 minutes).
/// Prevents a single stuck request from blocking retries for an unreasonable time.
const MAX_SFTP_TIMEOUT_SECS: u64 = 300;

/// SSH keepalive interval. Detects dead connections within ~90s (30s × max 3)
/// and keeps NAT mappings alive.
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Parameters needed to (re-)establish an SFTP connection.
#[derive(Clone)]
struct SftpConnectParams {
    host: String,
    port: u16,
    user: String,
    key_path: Option<PathBuf>,
    root: String,
    known_hosts_path: PathBuf,
    sftp_timeout_secs: u64,
}

/// SSH client handler that enforces known-host checks (TOFU).
struct SshHandler {
    host: String,
    port: u16,
    known_hosts_path: PathBuf,
}

impl client::Handler for SshHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &ssh_key::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        match verify_or_learn_host_key(
            &self.host,
            self.port,
            &self.known_hosts_path,
            server_public_key,
        ) {
            Ok(HostKeyState::Matched) => Ok(true),
            Ok(HostKeyState::Learned) => {
                tracing::warn!(
                    host = %self.host,
                    port = self.port,
                    known_hosts = %self.known_hosts_path.display(),
                    "learned new SSH host key via TOFU"
                );
                Ok(true)
            }
            Err(e) => {
                tracing::error!(
                    host = %self.host,
                    port = self.port,
                    known_hosts = %self.known_hosts_path.display(),
                    "SSH host key verification failed: {e}"
                );
                Err(e)
            }
        }
    }
}

/// Outcome of host key verification.
enum HostKeyState {
    Matched,
    Learned,
}

/// An active SSH + SFTP connection.
struct SftpConn {
    sftp: SftpSession,
    // Keep handle alive so the session isn't dropped.
    _session: client::Handle<SshHandler>,
}

#[derive(Default)]
struct ConnPoolState {
    idle: Vec<SftpConn>,
    total: usize,
}

#[derive(Debug)]
struct RetryError {
    err: VykarError,
    retryable: bool,
}

type RetryResult<T> = std::result::Result<T, RetryError>;

impl RetryError {
    fn transient(err: VykarError) -> Self {
        Self {
            err,
            retryable: true,
        }
    }

    fn permanent(err: VykarError) -> Self {
        Self {
            err,
            retryable: false,
        }
    }
}

/// Timeout for acquiring a connection from the pool.
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);

/// RAII guard that returns a connection slot to the pool on drop.
struct ConnGuard<'a> {
    conn: Option<SftpConn>,
    backend: &'a SftpBackend,
}

impl<'a> ConnGuard<'a> {
    fn sftp(&self) -> &SftpSession {
        &self
            .conn
            .as_ref()
            .expect("invariant: ConnGuard holds connection until release/discard")
            .sftp
    }

    /// Return connection to the idle pool (it's still healthy).
    fn release(mut self) {
        if let Some(conn) = self.conn.take() {
            self.backend.release_conn(conn);
        }
    }

    /// Discard connection (it's broken), free the slot.
    fn discard(mut self) {
        if let Some(conn) = self.conn.take() {
            drop(conn);
            self.backend.release_slot();
        }
    }
}

impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        // If neither release() nor discard() was called, treat as broken.
        if let Some(conn) = self.conn.take() {
            drop(conn);
            self.backend.release_slot();
        }
    }
}

/// SFTP storage backend using `russh` + `russh-sftp`.
///
/// Connections are established lazily and managed by a bounded pool to allow
/// concurrent transfers.
pub struct SftpBackend {
    params: SftpConnectParams,
    pool: Mutex<ConnPoolState>,
    pool_ready: Condvar,
    max_connections: usize,
    retry: RetryConfig,
    /// Cache of directories known to exist, skipping redundant `mkdir_p` calls.
    known_dirs: Mutex<HashSet<String>>,
}

impl SftpBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: &str,
        user: Option<&str>,
        port: Option<u16>,
        root: &str,
        key_path: Option<&str>,
        known_hosts_path: Option<&str>,
        max_connections: Option<usize>,
        sftp_timeout_secs: Option<u64>,
        retry: RetryConfig,
    ) -> Result<Self> {
        let user = user
            .map(|u| u.to_string())
            .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "unknown".into()));
        let known_hosts_path = resolve_known_hosts_path(known_hosts_path)?;
        let requested_max_connections = max_connections;
        let max_connections = normalize_max_connections(requested_max_connections);

        if let Some(requested) = requested_max_connections {
            if requested != max_connections {
                tracing::warn!(
                    requested,
                    effective = max_connections,
                    "adjusted max_connections to supported range"
                );
            }
        }

        let effective_timeout = normalize_sftp_timeout(sftp_timeout_secs);
        if let Some(requested) = sftp_timeout_secs {
            if requested != effective_timeout {
                tracing::warn!(
                    requested,
                    effective = effective_timeout,
                    "adjusted sftp_timeout to supported range"
                );
            }
        }
        tracing::debug!(
            sftp_timeout_secs = effective_timeout,
            "SFTP session timeout"
        );

        Ok(Self {
            params: SftpConnectParams {
                host: host.to_string(),
                port: port.unwrap_or(22),
                user,
                key_path: key_path.map(expand_tilde_path),
                root: normalize_root(root),
                known_hosts_path,
                sftp_timeout_secs: effective_timeout,
            },
            pool: Mutex::new(ConnPoolState::default()),
            pool_ready: Condvar::new(),
            max_connections,
            retry,
            known_dirs: Mutex::new(HashSet::new()),
        })
    }

    /// Full remote path for a given key.
    fn full_path(&self, key: &str) -> String {
        join_root_key(&self.params.root, key)
    }

    /// Check if a directory is in the known-dirs cache.
    fn is_known_dir(&self, dir: &str) -> bool {
        self.known_dirs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(dir)
    }

    /// Mark a directory (and all its ancestors) as known.
    fn mark_dir_known(&self, dir: &str) {
        let mut known = self.known_dirs.lock().unwrap_or_else(|e| e.into_inner());
        let mut current = String::new();
        for component in dir.split('/') {
            if component.is_empty() {
                current.push('/');
                continue;
            }
            if current.is_empty() || current == "/" {
                current = format!("{current}{component}");
            } else {
                current = format!("{current}/{component}");
            }
            known.insert(current.clone());
        }
    }

    fn lock_pool(&self) -> std::sync::MutexGuard<'_, ConnPoolState> {
        self.pool.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn acquire_conn(&self) -> RetryResult<ConnGuard<'_>> {
        let deadline = Instant::now() + POOL_ACQUIRE_TIMEOUT;
        loop {
            let mut state = self.lock_pool();

            if let Some(conn) = state.idle.pop() {
                return Ok(ConnGuard {
                    conn: Some(conn),
                    backend: self,
                });
            }

            if state.total < self.max_connections {
                state.total += 1;
                drop(state);

                match ASYNC_RUNTIME.block_on(Self::connect(&self.params)) {
                    Ok(conn) => {
                        return Ok(ConnGuard {
                            conn: Some(conn),
                            backend: self,
                        })
                    }
                    Err(e) => {
                        self.release_slot();
                        return Err(e);
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RetryError::transient(VykarError::Other(
                    "timed out waiting for SFTP connection from pool".into(),
                )));
            }

            let (new_state, _timeout) = self
                .pool_ready
                .wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner());
            drop(new_state);
        }
    }

    fn release_conn(&self, conn: SftpConn) {
        let mut state = self.lock_pool();
        state.idle.push(conn);
        drop(state);
        self.pool_ready.notify_one();
    }

    fn release_slot(&self) {
        let mut state = self.lock_pool();
        if state.total > 0 {
            state.total -= 1;
        }
        drop(state);
        self.pool_ready.notify_one();
    }

    /// Establish a new SSH + SFTP connection.
    async fn connect(params: &SftpConnectParams) -> RetryResult<SftpConn> {
        let config = Arc::new(client::Config {
            inactivity_timeout: Some(INACTIVITY_TIMEOUT),
            keepalive_interval: Some(DEFAULT_KEEPALIVE_INTERVAL),
            nodelay: true,
            window_size: 16 * 1024 * 1024, // 16 MiB (default: 2 MiB)
            maximum_packet_size: 65535,    // max safe value (russh errors above this)
            ..Default::default()
        });
        let handler = SshHandler {
            host: params.host.clone(),
            port: params.port,
            known_hosts_path: params.known_hosts_path.clone(),
        };

        let addr = (params.host.as_str(), params.port);
        let mut session =
            tokio::time::timeout(CONNECT_TIMEOUT, client::connect(config, addr, handler))
                .await
                .map_err(|_| {
                    RetryError::transient(VykarError::Other(format!(
                        "SSH connect to {}:{} timed out after {}s",
                        params.host,
                        params.port,
                        CONNECT_TIMEOUT.as_secs()
                    )))
                })?
                .map_err(|e| ssh_retry_error("connect", &params.host, params.port, e))?;

        // Authenticate with public key.
        let key_pair = load_key(&params.key_path)?;
        let hash_alg = session
            .best_supported_rsa_hash()
            .await
            .map_err(|e| ssh_retry_error("negotiate hash algorithm", &params.host, params.port, e))?
            .flatten();

        let auth_ok = session
            .authenticate_publickey(
                &params.user,
                PrivateKeyWithHashAlg::new(Arc::new(key_pair), hash_alg),
            )
            .await
            .map_err(|e| ssh_retry_error("authenticate", &params.host, params.port, e))?;

        if !auth_ok.success() {
            return Err(RetryError::permanent(VykarError::Other(format!(
                "SSH public-key authentication failed for user '{}' on {}:{}",
                params.user, params.host, params.port
            ))));
        }

        // Open SFTP subsystem.
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| ssh_retry_error("open channel", &params.host, params.port, e))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| ssh_retry_error("request sftp subsystem", &params.host, params.port, e))?;
        let sftp = SftpSession::new_opts(channel.into_stream(), Some(params.sftp_timeout_secs))
            .await
            .map_err(|e| {
                sftp_retry_error(
                    "session init",
                    &format!("{}:{}", params.host, params.port),
                    e,
                )
            })?;

        Ok(SftpConn {
            sftp,
            _session: session,
        })
    }

    /// Retry a synchronous SFTP operation with exponential backoff + jitter.
    /// The closure receives a pooled `&SftpSession`.
    fn retry_op<T>(&self, op_name: &str, f: impl Fn(&SftpSession) -> RetryResult<T>) -> Result<T> {
        let mut delay_ms = self.retry.retry_delay_ms;

        for attempt in 0..=self.retry.max_retries {
            if attempt > 0 {
                let base = delay_ms.max(1);
                let jitter = rand::random::<u64>() % base;
                std::thread::sleep(Duration::from_millis(base + jitter));
                delay_ms = (base.saturating_mul(2)).min(self.retry.retry_max_delay_ms.max(1));
            }

            let guard = match self.acquire_conn() {
                Ok(guard) => guard,
                Err(e) => {
                    if e.retryable && attempt < self.retry.max_retries {
                        tracing::warn!(
                            "SFTP {op_name}: connection error (attempt {}/{}), retrying: {}",
                            attempt + 1,
                            self.retry.max_retries,
                            e.err
                        );
                        continue;
                    }
                    return Err(e.err);
                }
            };

            match f(guard.sftp()) {
                Ok(val) => {
                    guard.release();
                    return Ok(val);
                }
                Err(e) => {
                    if e.retryable {
                        guard.discard();
                        if attempt < self.retry.max_retries {
                            tracing::warn!(
                                "SFTP {op_name}: transient error (attempt {}/{}), retrying: {}",
                                attempt + 1,
                                self.retry.max_retries,
                                e.err
                            );
                            continue;
                        }
                        return Err(e.err);
                    }

                    guard.release();
                    return Err(e.err);
                }
            }
        }

        unreachable!()
    }
}

/// Normalize the configured SFTP root.
fn normalize_root(root: &str) -> String {
    let root = root.trim_matches('/');
    if root.is_empty() {
        "/".to_string()
    } else {
        format!("/{root}")
    }
}

fn normalize_max_connections(requested: Option<usize>) -> usize {
    requested
        .unwrap_or(DEFAULT_SFTP_MAX_CONNECTIONS)
        .clamp(1, MAX_SFTP_MAX_CONNECTIONS)
}

fn normalize_sftp_timeout(requested: Option<u64>) -> u64 {
    requested
        .unwrap_or(DEFAULT_SFTP_TIMEOUT_SECS)
        .clamp(MIN_SFTP_TIMEOUT_SECS, MAX_SFTP_TIMEOUT_SECS)
}

fn join_root_key(root: &str, key: &str) -> String {
    let key = key.trim_start_matches('/');
    if key.is_empty() {
        return root.to_string();
    }
    Path::new(root).join(key).to_string_lossy().to_string()
}

fn resolve_known_hosts_path(explicit: Option<&str>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(expand_tilde_path(path));
    }

    let home = paths::home_dir()
        .ok_or_else(|| VykarError::Other("cannot determine home directory".into()))?;

    #[cfg(target_os = "windows")]
    {
        Ok(home.join("ssh").join("known_hosts"))
    }

    #[cfg(not(target_os = "windows"))]
    {
        Ok(home.join(".ssh").join("known_hosts"))
    }
}

fn expand_tilde_path(raw: &str) -> PathBuf {
    if raw == "~" {
        if let Some(home) = paths::home_dir() {
            return home;
        }
    }

    if let Some(rest) = raw.strip_prefix("~/").or_else(|| raw.strip_prefix("~\\")) {
        if let Some(home) = paths::home_dir() {
            return home.join(rest);
        }
    }

    PathBuf::from(raw)
}

fn ensure_known_hosts_file(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if path.exists() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        match std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(e),
        }
    }

    #[cfg(not(unix))]
    {
        match std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn verify_or_learn_host_key(
    host: &str,
    port: u16,
    known_hosts_path: &Path,
    server_public_key: &ssh_key::PublicKey,
) -> std::result::Result<HostKeyState, russh::Error> {
    ensure_known_hosts_file(known_hosts_path).map_err(russh::Error::IO)?;

    let known = known_host_keys_path(host, port, known_hosts_path)?;
    if known
        .iter()
        .any(|(_, existing_key)| existing_key == server_public_key)
    {
        return Ok(HostKeyState::Matched);
    }

    if known.is_empty() {
        learn_known_hosts_path(host, port, server_public_key, known_hosts_path)?;
        return Ok(HostKeyState::Learned);
    }

    Err(russh::Error::KeyChanged { line: known[0].0 })
}

/// Load SSH private key, trying explicit path first, then default locations.
fn load_key(explicit: &Option<PathBuf>) -> RetryResult<PrivateKey> {
    if let Some(path) = explicit {
        return load_secret_key(path, None).map_err(|e| {
            RetryError::permanent(VykarError::Other(format!(
                "load SSH key {}: {e}",
                path.display()
            )))
        });
    }

    // Try default key locations.
    let home = paths::home_dir().ok_or_else(|| {
        RetryError::permanent(VykarError::Other("cannot determine home directory".into()))
    })?;
    let candidates = ["id_ed25519", "id_rsa", "id_ecdsa"];
    for name in &candidates {
        let path = home.join(".ssh").join(name);
        if path.exists() {
            match load_secret_key(&path, None) {
                Ok(key) => return Ok(key),
                Err(e) => {
                    tracing::debug!("skipping {}: {e}", path.display());
                }
            }
        }
    }

    Err(RetryError::permanent(VykarError::Other(
        "no SSH private key found; set sftp_key in config or place a key in ~/.ssh/".into(),
    )))
}

fn ssh_retry_error(op: &str, host: &str, port: u16, e: russh::Error) -> RetryError {
    let err = VykarError::Other(format!("SSH {op} {host}:{port}: {e}"));
    if is_retryable_ssh_error(&e) {
        RetryError::transient(err)
    } else {
        RetryError::permanent(err)
    }
}

fn sftp_retry_error(op: &str, path: &str, e: russh_sftp::client::error::Error) -> RetryError {
    let err = VykarError::Other(format!("SFTP {op} '{path}': {e}"));
    if is_retryable_sftp_error(&e) {
        RetryError::transient(err)
    } else {
        RetryError::permanent(err)
    }
}

fn io_retry_error(op: &str, path: &str, e: std::io::Error) -> RetryError {
    let retryable = matches!(
        e.kind(),
        std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::BrokenPipe
            // russh-sftp AsyncWrite/AsyncRead wraps all SFTP errors as
            // ErrorKind::Other, losing the original error type. Default
            // to retryable — the channel is likely broken anyway.
            | std::io::ErrorKind::Other
    );

    let err = VykarError::Other(format!("SFTP {op} '{path}': {e}"));
    if retryable {
        RetryError::transient(err)
    } else {
        RetryError::permanent(err)
    }
}

fn is_retryable_ssh_error(e: &russh::Error) -> bool {
    matches!(
        e,
        russh::Error::KexInit
            | russh::Error::Kex
            | russh::Error::Disconnect
            | russh::Error::HUP
            | russh::Error::ConnectionTimeout
            | russh::Error::KeepaliveTimeout
            | russh::Error::InactivityTimeout
            | russh::Error::SendError
            | russh::Error::Pending
            | russh::Error::IO(_)
            | russh::Error::Elapsed(_)
    )
}

fn is_retryable_sftp_error(e: &russh_sftp::client::error::Error) -> bool {
    match e {
        russh_sftp::client::error::Error::Timeout => true,
        russh_sftp::client::error::Error::IO(_) => true,
        russh_sftp::client::error::Error::Limited(_) => true,
        russh_sftp::client::error::Error::UnexpectedPacket => true,
        russh_sftp::client::error::Error::UnexpectedBehavior(_) => true,
        russh_sftp::client::error::Error::Status(status) => matches!(
            status.status_code,
            StatusCode::NoConnection | StatusCode::ConnectionLost | StatusCode::BadMessage
        ),
    }
}

/// Recursively create parent directories for a remote path.
async fn mkdir_p(sftp: &SftpSession, path: &str) -> RetryResult<()> {
    let mut current = String::new();
    for component in path.split('/') {
        if component.is_empty() {
            current.push('/');
            continue;
        }
        if current.is_empty() || current == "/" {
            current = format!("{current}{component}");
        } else {
            current = format!("{current}/{component}");
        }
        match sftp.create_dir(&current).await {
            Ok(()) => {}
            Err(e) => match &e {
                russh_sftp::client::error::Error::Status(s)
                    if s.status_code == StatusCode::Failure =>
                {
                    // Likely already exists; verify with metadata.
                    if let Err(meta_err) = sftp.metadata(&current).await {
                        return Err(sftp_retry_error("mkdir", &current, meta_err));
                    }
                }
                _ => {
                    return Err(sftp_retry_error("mkdir", &current, e));
                }
            },
        }
    }
    Ok(())
}

/// Check whether an SFTP error indicates "not found".
fn is_not_found(e: &russh_sftp::client::error::Error) -> bool {
    matches!(
        e,
        russh_sftp::client::error::Error::Status(s)
            if s.status_code == StatusCode::NoSuchFile
    )
}

fn validate_range_length(length: u64) -> RetryResult<usize> {
    if length > MAX_GET_RANGE_BYTES {
        return Err(RetryError::permanent(VykarError::Other(format!(
            "requested range length {length} exceeds max {MAX_GET_RANGE_BYTES} bytes"
        ))));
    }

    usize::try_from(length).map_err(|_| {
        RetryError::permanent(VykarError::Other(format!(
            "requested range length {length} does not fit platform usize"
        )))
    })
}

/// Recursively list all files under a directory, returning keys relative to root.
async fn list_recursive(
    sftp: &SftpSession,
    dir_path: &str,
    root: &str,
) -> RetryResult<Vec<String>> {
    let mut keys = Vec::new();
    let mut dirs_to_visit = vec![dir_path.to_string()];

    while let Some(current_dir) = dirs_to_visit.pop() {
        let entries = match sftp.read_dir(&current_dir).await {
            Ok(entries) => entries,
            Err(e) if is_not_found(&e) => return Ok(keys),
            Err(e) => return Err(sftp_retry_error("readdir", &current_dir, e)),
        };

        for entry in entries {
            let name = entry.file_name();
            if name == "." || name == ".." {
                continue;
            }
            let full = Path::new(&*current_dir)
                .join(&name)
                .to_string_lossy()
                .to_string();
            let file_type = entry.metadata().file_type();
            if file_type.is_dir() {
                dirs_to_visit.push(full);
            } else {
                let key = full
                    .strip_prefix(root)
                    .unwrap_or(&full)
                    .trim_start_matches('/');
                if !key.is_empty() {
                    keys.push(key.to_string());
                }
            }
        }
    }

    Ok(keys)
}

impl StorageBackend for SftpBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.full_path(key);
        self.retry_op(&format!("GET {key}"), |sftp| {
            ASYNC_RUNTIME.block_on(async {
                let mut file = match sftp.open(&path).await {
                    Ok(f) => f,
                    Err(e) if is_not_found(&e) => return Ok(None),
                    Err(e) => return Err(sftp_retry_error("open", &path, e)),
                };

                let mut buf = Vec::new();
                file.read_to_end(&mut buf)
                    .await
                    .map_err(|e| io_retry_error("read", &path, e))?;
                Ok(Some(buf))
            })
        })
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.full_path(key);
        let parent_known = match path.rsplit_once('/').map(|(p, _)| p) {
            Some(p) if !p.is_empty() => self.is_known_dir(p),
            _ => true,
        };
        let data = Arc::new(data.to_vec());
        self.retry_op(&format!("PUT {key}"), |sftp| {
            let data = data.clone();
            ASYNC_RUNTIME.block_on(async {
                // Ensure parent directory exists (skip if already cached).
                if !parent_known {
                    if let Some(parent) = path.rsplit_once('/').map(|(p, _)| p) {
                        if !parent.is_empty() {
                            mkdir_p(sftp, parent).await?;
                        }
                    }
                }

                let mut file = sftp
                    .open_with_flags(
                        &path,
                        OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                    )
                    .await
                    .map_err(|e| sftp_retry_error("create", &path, e))?;

                file.write_all(data.as_ref())
                    .await
                    .map_err(|e| io_retry_error("write", &path, e))?;
                file.flush()
                    .await
                    .map_err(|e| io_retry_error("flush", &path, e))?;
                file.shutdown()
                    .await
                    .map_err(|e| io_retry_error("close", &path, e))?;
                Ok(())
            })
        })?;
        // Cache parent dir on success.
        if !parent_known {
            if let Some(parent) = path.rsplit_once('/').map(|(p, _)| p) {
                if !parent.is_empty() {
                    self.mark_dir_known(parent);
                }
            }
        }
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.full_path(key);
        self.retry_op(&format!("DELETE {key}"), |sftp| {
            ASYNC_RUNTIME.block_on(async {
                match sftp.remove_file(&path).await {
                    Ok(()) => Ok(()),
                    Err(e) if is_not_found(&e) => Ok(()),
                    Err(e) => Err(sftp_retry_error("delete", &path, e)),
                }
            })
        })
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let path = self.full_path(key);
        self.retry_op(&format!("EXISTS {key}"), |sftp| {
            ASYNC_RUNTIME.block_on(async {
                match sftp.metadata(&path).await {
                    Ok(_) => Ok(true),
                    Err(e) if is_not_found(&e) => Ok(false),
                    Err(e) => Err(sftp_retry_error("stat", &path, e)),
                }
            })
        })
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let path = self.full_path(key);
        self.retry_op(&format!("SIZE {key}"), |sftp| {
            ASYNC_RUNTIME.block_on(async {
                match sftp.metadata(&path).await {
                    Ok(meta) => match meta.size {
                        Some(sz) => Ok(Some(sz)),
                        None => Err(RetryError::permanent(VykarError::Other(format!(
                            "SFTP stat '{path}': server did not return file size"
                        )))),
                    },
                    Err(e) if is_not_found(&e) => Ok(None),
                    Err(e) => Err(sftp_retry_error("stat", &path, e)),
                }
            })
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let dir_path = self.full_path(prefix);
        let root = self.params.root.clone();
        self.retry_op(&format!("LIST {prefix}"), |sftp| {
            ASYNC_RUNTIME.block_on(list_recursive(sftp, &dir_path, &root))
        })
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let path = self.full_path(key);
        self.retry_op(&format!("GET_RANGE {key}"), |sftp| {
            ASYNC_RUNTIME.block_on(async {
                let requested_len = validate_range_length(length)?;

                let mut file = match sftp.open_with_flags(&path, OpenFlags::READ).await {
                    Ok(f) => f,
                    Err(e) if is_not_found(&e) => return Ok(None),
                    Err(e) => return Err(sftp_retry_error("open", &path, e)),
                };

                file.seek(std::io::SeekFrom::Start(offset))
                    .await
                    .map_err(|e| io_retry_error("seek", &path, e))?;

                let mut buf = vec![0u8; requested_len];
                let mut total = 0;
                while total < buf.len() {
                    match file.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => total += n,
                        Err(e) => return Err(io_retry_error("read_range", &path, e)),
                    }
                }
                buf.truncate(total);
                if total != requested_len {
                    return Err(RetryError::permanent(VykarError::Other(format!(
                        "short read on {key} at offset {offset}: expected {length} bytes, got {total}"
                    ))));
                }
                Ok(Some(buf))
            })
        })
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let path = self.full_path(key.trim_end_matches('/'));
        if self.is_known_dir(&path) {
            return Ok(());
        }
        self.retry_op(&format!("MKDIR {key}"), |sftp| {
            ASYNC_RUNTIME.block_on(mkdir_p(sftp, &path))
        })?;
        self.mark_dir_known(&path);
        Ok(())
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let path = self.full_path(key);
        let parent_known = match path.rsplit_once('/').map(|(p, _)| p) {
            Some(p) if !p.is_empty() => self.is_known_dir(p),
            _ => true,
        };
        // Wrap the owned Vec directly — no clone needed.
        let data = Arc::new(data);
        self.retry_op(&format!("PUT {key}"), |sftp| {
            let data = data.clone();
            ASYNC_RUNTIME.block_on(async {
                if !parent_known {
                    if let Some(parent) = path.rsplit_once('/').map(|(p, _)| p) {
                        if !parent.is_empty() {
                            mkdir_p(sftp, parent).await?;
                        }
                    }
                }

                let mut file = sftp
                    .open_with_flags(
                        &path,
                        OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                    )
                    .await
                    .map_err(|e| sftp_retry_error("create", &path, e))?;

                file.write_all(data.as_ref())
                    .await
                    .map_err(|e| io_retry_error("write", &path, e))?;
                file.flush()
                    .await
                    .map_err(|e| io_retry_error("flush", &path, e))?;
                file.shutdown()
                    .await
                    .map_err(|e| io_retry_error("close", &path, e))?;
                Ok(())
            })
        })?;
        if !parent_known {
            if let Some(parent) = path.rsplit_once('/').map(|(p, _)| p) {
                if !parent.is_empty() {
                    self.mark_dir_known(parent);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_root() {
        assert_eq!(normalize_root(""), "/");
        assert_eq!(normalize_root("/"), "/");
        assert_eq!(normalize_root("backups/vykar"), "/backups/vykar");
        assert_eq!(normalize_root("/backups/vykar/"), "/backups/vykar");
    }

    #[test]
    fn test_join_root_key() {
        assert_eq!(join_root_key("/", "packs/abc"), "/packs/abc");
        assert_eq!(join_root_key("/repo", "packs/abc"), "/repo/packs/abc");
        assert_eq!(join_root_key("/repo", "/packs/abc"), "/repo/packs/abc");
    }

    #[test]
    fn test_normalize_max_connections() {
        assert_eq!(
            normalize_max_connections(None),
            DEFAULT_SFTP_MAX_CONNECTIONS
        );
        assert_eq!(normalize_max_connections(Some(0)), 1);
        assert_eq!(normalize_max_connections(Some(4)), 4);
        assert_eq!(
            normalize_max_connections(Some(99)),
            MAX_SFTP_MAX_CONNECTIONS
        );
    }

    #[test]
    fn test_validate_range_length_limit() {
        assert!(validate_range_length(1024).is_ok());
        assert!(validate_range_length(MAX_GET_RANGE_BYTES).is_ok());
        assert!(validate_range_length(MAX_GET_RANGE_BYTES + 1).is_err());
    }

    #[test]
    fn test_normalize_sftp_timeout() {
        assert_eq!(normalize_sftp_timeout(None), DEFAULT_SFTP_TIMEOUT_SECS);
        assert_eq!(normalize_sftp_timeout(Some(0)), MIN_SFTP_TIMEOUT_SECS);
        assert_eq!(normalize_sftp_timeout(Some(1)), MIN_SFTP_TIMEOUT_SECS);
        assert_eq!(normalize_sftp_timeout(Some(60)), 60);
        assert_eq!(normalize_sftp_timeout(Some(300)), MAX_SFTP_TIMEOUT_SECS);
        assert_eq!(normalize_sftp_timeout(Some(9999)), MAX_SFTP_TIMEOUT_SECS);
    }

    #[test]
    fn test_io_retry_error_other_is_retryable() {
        // russh-sftp wraps all SFTP errors as ErrorKind::Other
        let err = std::io::Error::other("Timeout");
        let retry = io_retry_error("write", "/test/path", err);
        assert!(retry.retryable, "ErrorKind::Other should be retryable");
    }

    #[test]
    fn test_io_retry_error_permission_denied_is_not_retryable() {
        let err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let retry = io_retry_error("write", "/test/path", err);
        assert!(!retry.retryable, "PermissionDenied should not be retryable");
    }
}
