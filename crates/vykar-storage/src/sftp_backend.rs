#![allow(
    clippy::duration_suboptimal_units,
    clippy::elidable_lifetime_names,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::missing_errors_doc,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::ref_option
)]

use std::cell::RefCell;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime};

use russh::client;
use russh::keys::known_hosts::{known_host_keys_path, learn_known_hosts_path};
use russh::keys::ssh_key;
use russh::keys::PublicKeyOrCertificate;
use russh::keys::{load_public_key, load_secret_key, PrivateKey, PrivateKeyWithHashAlg};
use russh_sftp::client::{Config as SftpConfig, SftpSession};
use russh_sftp::protocol::{OpenFlags, StatusCode};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::RetryConfig;
use crate::{ListedObject, StorageBackend};
use vykar_common::paths;
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

/// Upper bound on best-effort cleanup of a temp file after a failed upload.
/// The connection may already be dead; the compact sweep removes leftovers.
const TEMP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

/// SSH keepalive interval. Detects dead connections within ~90s (30s × max 3)
/// and keeps NAT mappings alive.
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Unacknowledged SFTP WRITE requests per upload. With
/// `SFTP_MAX_WRITE_PACKET_LEN` this keeps 2 MiB in flight, matching the
/// 2 MiB channel window OpenSSH grants, so uploads run at window / RTT per
/// connection. The library default (16) caps them at 512 KiB / RTT.
const SFTP_MAX_CONCURRENT_WRITES: usize = 64;

/// Framed size of one SFTP WRITE. 32 KiB is the size every server must
/// accept; for servers that do not advertise `limits@openssh.com` the
/// library caps a larger value only at its own 256 KiB packet limit.
const SFTP_MAX_WRITE_PACKET_LEN: u32 = 32 * 1024;

/// Multiplier on `sftp_timeout` for the pipelined WRITEs of an upload. A
/// request's deadline starts when it is queued, so a WRITE waits behind the
/// whole pipeline. Scaling by the same factor as the depth (64 vs the
/// library's 16) keeps the slowest workable link where it was.
const SFTP_WRITE_TIMEOUT_FACTOR: u64 = 4;

/// Parameters needed to (re-)establish an SFTP connection.
#[derive(Clone)]
struct SftpConnectParams {
    host: String,
    port: u16,
    user: String,
    key_path: Option<PathBuf>,
    /// Public key read from `key_path`: sign with the agent, using this key only.
    agent_key: Option<ssh_key::PublicKey>,
    /// Agent socket; `SSH_AUTH_SOCK` when unset.
    agent_socket: Option<PathBuf>,
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

    /// russh 0.63 widened this to `PublicKeyOrCertificate`. Host certificates
    /// are rejected rather than flattened with `.public_key()`: flattening
    /// would pin the certified host key without ever validating the signing
    /// CA, and vykar's TOFU learning path writes plain keys, so the pin would
    /// not mean what the trust model says it means. Failing loudly is the
    /// honest outcome until certificate support is designed deliberately.
    fn check_server_key(
        &mut self,
        server_public_key: &PublicKeyOrCertificate,
    ) -> impl std::future::Future<Output = std::result::Result<bool, Self::Error>> + Send {
        let server_public_key = match server_public_key {
            PublicKeyOrCertificate::PublicKey { key, .. } => key,
            PublicKeyOrCertificate::Certificate(_) => {
                tracing::error!(
                    host = %self.host,
                    port = self.port,
                    "server offered an SSH host certificate, which vykar does not support; \
                     expected a plain host key"
                );
                return std::future::ready(Ok(false));
            }
        };
        let result = match verify_or_learn_host_key(
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
        };
        std::future::ready(result)
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

/// Maximum number of consecutive pool-acquire timeouts within a single storage
/// operation before we declare the pool wedged and give up. Acts as a fuse for
/// connection-slot leaks; with `POOL_ACQUIRE_TIMEOUT = 60s`, the worst-case
/// wait is ~30 minutes.
const MAX_CONSECUTIVE_POOL_WAITS: usize = 30;

/// Outcome of `acquire_conn` after both pool contention and transport errors
/// are considered. Splits the two so the caller can apply different policies:
/// `PoolWait` does not consume the user-visible retry budget.
#[derive(Debug)]
enum AcquireError {
    /// Pool-acquire deadline elapsed without a slot becoming available.
    /// Indicates pool saturation, not a transport failure.
    PoolWait,
    /// Transport-level failure from `connect()` (transient or permanent).
    Transport(RetryError),
}

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
        agent_socket: Option<&str>,
        known_hosts_path: Option<&str>,
        max_connections: Option<usize>,
        sftp_timeout_secs: Option<u64>,
        retry: RetryConfig,
    ) -> Result<Self> {
        let key_path = key_path.map(expand_tilde_path);
        // Like OpenSSH's IdentityFile, sftp_key may name the public half of an agent-held key.
        let agent_key = key_path.as_deref().and_then(|p| load_public_key(p).ok());
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
                key_path,
                agent_key,
                agent_socket: agent_socket.map(expand_tilde_path),
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

    fn acquire_conn(&self) -> std::result::Result<ConnGuard<'_>, AcquireError> {
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
                        return Err(AcquireError::Transport(e));
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AcquireError::PoolWait);
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
            // Receive-side window and packet size: they bound downloads only.
            // Uploads are bounded by the window the server grants (OpenSSH: 2 MiB).
            window_size: 16 * 1024 * 1024,
            maximum_packet_size: 65535, // russh warns above this
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

        let hash_alg = session
            .best_supported_rsa_hash()
            .await
            .map_err(|e| ssh_retry_error("negotiate hash algorithm", &params.host, params.port, e))?
            .flatten();

        // Without an explicit private key, try ssh-agent first, as OpenSSH does.
        let agent_ok = (params.key_path.is_none() || params.agent_key.is_some())
            && tokio::time::timeout(
                CONNECT_TIMEOUT,
                auth_with_agent(&mut session, params, hash_alg),
            )
            .await
            .map_err(|_| {
                ssh_retry_error(
                    "agent authentication",
                    &params.host,
                    params.port,
                    russh::Error::ConnectionTimeout,
                )
            })??;

        if !agent_ok {
            if params.agent_key.is_some() {
                return Err(RetryError::permanent(VykarError::Other(format!(
                    "SSH agent has no accepted key matching sftp_key for user '{}' on {}:{}",
                    params.user, params.host, params.port
                ))));
            }
            let key_pair = load_key(&params.key_path)?;
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
        let sftp_cfg = sftp_session_config(params.sftp_timeout_secs);
        let sftp = SftpSession::new_with_config(channel.into_stream(), sftp_cfg)
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
    ///
    /// Pool-acquire timeouts are tracked separately from transport retries:
    /// they do not consume the user-configured retry budget and do not trigger
    /// the exponential backoff sleep.
    fn retry_op<T>(&self, op_name: &str, f: impl Fn(&SftpSession) -> RetryResult<T>) -> Result<T> {
        let mut state = RetryState::default();
        let mut delay_ms = self.retry.retry_delay_ms;
        let mut needs_backoff = false;

        loop {
            if needs_backoff {
                let base = delay_ms.max(1);
                let jitter = rand::random::<u64>() % base;
                std::thread::sleep(Duration::from_millis(base + jitter));
                delay_ms = (base.saturating_mul(2)).min(self.retry.retry_max_delay_ms.max(1));
                needs_backoff = false;
            }

            let guard = match self.acquire_conn() {
                Ok(guard) => {
                    state.pool_waits_used = 0;
                    guard
                }
                Err(AcquireError::PoolWait) => {
                    if record_pool_wait(&mut state, MAX_CONSECUTIVE_POOL_WAITS) {
                        return Err(VykarError::Other(format!(
                            "SFTP {op_name}: connection pool wedged: no slot released after {} consecutive {}s waits",
                            MAX_CONSECUTIVE_POOL_WAITS,
                            POOL_ACQUIRE_TIMEOUT.as_secs(),
                        )));
                    }
                    tracing::warn!(
                        "SFTP {op_name}: waited {}s for connection pool slot (all {} connections busy); still waiting",
                        POOL_ACQUIRE_TIMEOUT.as_secs(),
                        self.max_connections,
                    );
                    continue;
                }
                Err(AcquireError::Transport(e)) => {
                    state.pool_waits_used = 0;
                    if !e.retryable {
                        return Err(e.err);
                    }
                    if record_transient(&mut state, self.retry.max_retries) {
                        return Err(e.err);
                    }
                    tracing::warn!(
                        "SFTP {op_name}: retry {} of {} after connection error: {}",
                        state.transport_retries_used,
                        self.retry.max_retries,
                        e.err,
                    );
                    needs_backoff = true;
                    continue;
                }
            };

            match f(guard.sftp()) {
                Ok(val) => {
                    guard.release();
                    return Ok(val);
                }
                Err(e) => {
                    if !e.retryable {
                        guard.release();
                        return Err(e.err);
                    }
                    guard.discard();
                    if record_transient(&mut state, self.retry.max_retries) {
                        return Err(e.err);
                    }
                    tracing::warn!(
                        "SFTP {op_name}: retry {} of {} after transient error: {}",
                        state.transport_retries_used,
                        self.retry.max_retries,
                        e.err,
                    );
                    needs_backoff = true;
                }
            }
        }
    }
}

/// State machine bookkeeping for `retry_op`. Split from the loop so the
/// policy can be unit-tested without needing a real SFTP connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RetryState {
    transport_retries_used: usize,
    pool_waits_used: usize,
}

/// Record a pool-acquire timeout. Returns `true` if the pool appears wedged
/// and the operation should give up. Pool waits never touch the transport
/// retry budget.
///
/// The caller is expected to reset `state.pool_waits_used` to 0 on any
/// non-pool-wait outcome (successful acquire or transport error), so the
/// counter measures *consecutive* pool waits rather than the lifetime total.
fn record_pool_wait(state: &mut RetryState, max_pool_waits: usize) -> bool {
    state.pool_waits_used += 1;
    state.pool_waits_used >= max_pool_waits
}

/// Record a transient transport failure (from `acquire_conn` or the operation
/// closure). Returns `true` if the transport retry budget is exhausted and the
/// caller should propagate the error.
fn record_transient(state: &mut RetryState, max_retries: usize) -> bool {
    if state.transport_retries_used >= max_retries {
        return true;
    }
    state.transport_retries_used += 1;
    false
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

/// Client settings for one SFTP session.
fn sftp_session_config(request_timeout_secs: u64) -> SftpConfig {
    SftpConfig {
        request_timeout_secs,
        max_concurrent_writes: SFTP_MAX_CONCURRENT_WRITES,
        max_write_packet_len: SFTP_MAX_WRITE_PACKET_LEN,
        ..SftpConfig::default()
    }
}

/// Join two POSIX path segments with `/`. Never uses `std::path::Path`,
/// so the result is always `/`-separated regardless of host OS.
fn posix_join(base: &str, child: &str) -> String {
    let base = base.trim_end_matches('/');
    let base = if base.is_empty() { "/" } else { base };
    let child = child.trim_start_matches('/');
    if child.is_empty() {
        return base.to_string();
    }
    if base == "/" {
        return format!("/{child}");
    }
    format!("{base}/{child}")
}

fn join_root_key(root: &str, key: &str) -> String {
    posix_join(root, key)
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

    Err(russh::Error::KeyChanged {
        line: known.first().expect("known is non-empty (checked above)").0,
    })
}

/// Offer each agent key (or only `params.agent_key`) until one is accepted.
/// Returns false when an optional agent is absent or the server accepts none of its keys.
async fn auth_with_agent(
    session: &mut client::Handle<SshHandler>,
    params: &SftpConnectParams,
    hash_alg: Option<ssh_key::HashAlg>,
) -> RetryResult<bool> {
    use russh::keys::agent::AgentIdentity;
    use russh::AgentAuthError;

    let agent_error = |e| ssh_retry_error("agent authentication", &params.host, params.port, e);
    let mut agent = match connect_agent(params.agent_socket.as_deref()).await {
        Ok(agent) => agent,
        Err(e) if params.agent_key.is_some() => {
            return Err(RetryError::transient(VykarError::Other(format!(
                "SSH agent connection: {e}"
            ))));
        }
        Err(e) => {
            // A configured socket that does not work is worth a warning; a missing SSH_AUTH_SOCK is not.
            if let Some(path) = &params.agent_socket {
                tracing::warn!("ssh-agent {}: {e}", path.display());
            }
            return Ok(false);
        }
    };
    let identities = match agent.request_identities().await {
        Ok(identities) => identities,
        Err(russh::keys::Error::IO(e)) => return Err(agent_error(russh::Error::IO(e))),
        Err(_) => return Ok(false),
    };
    for identity in identities {
        // Certificates would need authenticate_certificate_with; not supported yet.
        let AgentIdentity::PublicKey { key, .. } = identity else {
            continue;
        };
        if params
            .agent_key
            .as_ref()
            .is_some_and(|only| only.key_data() != key.key_data())
        {
            continue;
        }
        match session
            .authenticate_publickey_with(&params.user, key, hash_alg, &mut agent)
            .await
        {
            Ok(res) if res.success() => return Ok(true),
            // Typically MaxAuthTries: a retry would offer the same keys and fail the same way.
            Ok(_) if session.is_closed() => {
                return Err(RetryError::permanent(VykarError::Other(format!(
                    "SSH server {}:{} disconnected after rejecting an ssh-agent key \
                     (too many authentication attempts?); set sftp_key to the key to use",
                    params.host, params.port
                ))));
            }
            Ok(_) => {}
            Err(AgentAuthError::Send(_)) => return Err(agent_error(russh::Error::SendError)),
            Err(AgentAuthError::Key(russh::keys::Error::IO(e))) => {
                return Err(agent_error(russh::Error::IO(e)));
            }
            Err(e) => tracing::debug!("ssh-agent: sign: {e}"),
        }
    }
    Ok(false)
}

type DynAgentClient = russh::keys::agent::client::AgentClient<
    Box<dyn russh::keys::agent::client::AgentStream + Send + Unpin>,
>;

/// Connect to the agent at `socket` (a Unix socket or Windows named pipe), or the platform default.
async fn connect_agent(
    socket: Option<&Path>,
) -> std::result::Result<DynAgentClient, russh::keys::Error> {
    use russh::keys::agent::client::AgentClient;

    #[cfg(unix)]
    let agent = match socket {
        Some(path) => AgentClient::connect_uds(path).await?.dynamic(),
        None => AgentClient::connect_env().await?.dynamic(),
    };
    #[cfg(windows)]
    let agent = match socket
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("SSH_AUTH_SOCK").map(PathBuf::from))
    {
        Some(pipe) => AgentClient::connect_named_pipe(pipe).await?.dynamic(),
        // Windows OpenSSH's agent pipe (also served by 1Password and Bitwarden), then Pageant.
        None => match AgentClient::connect_named_pipe(r"\\.\pipe\openssh-ssh-agent").await {
            Ok(agent) => agent.dynamic(),
            Err(_) => AgentClient::connect_pageant().await?.dynamic(),
        },
    };
    Ok(agent)
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
        "no SSH private key found; set sftp_key in config, place a key in ~/.ssh/, or load one into ssh-agent".into(),
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
            // russh-sftp's AsyncRead/AsyncWrite impls collapse SFTP errors
            // into io::Error, losing the original type. 3.0 maps `Timeout` to
            // `TimedOut` and everything else to `Other` (2.x used `Other` for
            // all of them); both kinds are retryable here, so the mapping
            // change does not shift classification. Default to retryable —
            // the channel is likely broken anyway.
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
                russh_sftp::client::error::Error::Status(s) => {
                    // SFTP servers return inconsistent status codes when mkdir
                    // fails on an existing path (e.g. Synology returns
                    // PermissionDenied for symlinks instead of Failure).
                    // Verify with metadata() whether the path is a directory.
                    // metadata() uses stat() which follows symlinks, so
                    // symlink-to-directory correctly reports is_dir() == true.
                    tracing::debug!(
                        path = %current,
                        status = ?s.status_code,
                        "mkdir failed, verifying with metadata"
                    );
                    match sftp.metadata(&current).await {
                        Ok(attrs) if attrs.is_dir() => {}
                        Ok(_) => {
                            return Err(RetryError::permanent(VykarError::Other(format!(
                                "SFTP mkdir '{current}': path exists but is not a directory"
                            ))));
                        }
                        Err(meta_err) => {
                            return Err(sftp_retry_error("mkdir", &current, meta_err));
                        }
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

/// Recursively list all files under a directory, returning keys relative to
/// root with the sizes reported by `readdir`. Temp files are skipped.
async fn list_recursive(
    sftp: &SftpSession,
    dir_path: &str,
    root: &str,
) -> RetryResult<Vec<ListedObject>> {
    let mut out = Vec::new();
    walk_files(sftp, dir_path, root, |key, meta| {
        if !vykar_protocol::is_temp_file(key) {
            out.push(ListedObject {
                key: key.to_string(),
                size: meta.size,
            });
        }
    })
    .await?;
    Ok(out)
}

/// Visit every non-directory entry under `dir_path` with its key relative to
/// `root` and its `readdir` attributes. A missing directory is empty.
async fn walk_files(
    sftp: &SftpSession,
    dir_path: &str,
    root: &str,
    mut visit: impl FnMut(&str, &russh_sftp::client::fs::Metadata),
) -> RetryResult<()> {
    let mut dirs_to_visit = vec![dir_path.to_string()];

    while let Some(current_dir) = dirs_to_visit.pop() {
        let entries = match sftp.read_dir(&current_dir).await {
            Ok(entries) => entries,
            Err(e) if is_not_found(&e) => continue,
            Err(e) => return Err(sftp_retry_error("readdir", &current_dir, e)),
        };

        for entry in entries {
            let name = entry.file_name();
            if name == "." || name == ".." {
                continue;
            }
            let full = posix_join(&current_dir, &name);
            let meta = entry.metadata();
            if meta.file_type().is_dir() {
                dirs_to_visit.push(full);
            } else {
                let key = full
                    .strip_prefix(root)
                    .unwrap_or(&full)
                    .trim_start_matches('/');
                if !key.is_empty() {
                    visit(key, &meta);
                }
            }
        }
    }

    Ok(())
}

/// Like [`walk_files`] from `root`, but only the repository's own namespace:
/// files in `root` itself and everything under
/// [`vykar_protocol::KNOWN_ROOT_DIRS`]. Foreign subdirectories are not entered.
async fn walk_repo_files(
    sftp: &SftpSession,
    root: &str,
    mut visit: impl FnMut(&str, &russh_sftp::client::fs::Metadata),
) -> RetryResult<()> {
    let entries = match sftp.read_dir(root).await {
        Ok(entries) => entries,
        Err(e) if is_not_found(&e) => return Ok(()),
        Err(e) => return Err(sftp_retry_error("readdir", root, e)),
    };
    for entry in entries {
        let name = entry.file_name();
        let meta = entry.metadata();
        if meta.file_type().is_dir() {
            if vykar_protocol::KNOWN_ROOT_DIRS.contains(&name.as_str()) {
                walk_files(sftp, &posix_join(root, &name), root, &mut visit).await?;
            }
        } else if name != "." && name != ".." {
            visit(&name, &meta);
        }
    }
    Ok(())
}

/// Temp path next to `path`: `<parent>/.tmp.<basename>.<16 hex>`. A fresh
/// random suffix per attempt keeps retries from colliding with debris.
fn temp_path_for(path: &str) -> String {
    let (parent, base) = path.rsplit_once('/').unwrap_or(("", path));
    format!(
        "{parent}/{}{base}.{:016x}",
        vykar_protocol::TEMP_FILE_PREFIX,
        rand::random::<u64>()
    )
}

async fn remove_temp_best_effort(sftp: &SftpSession, tmp: &str) {
    match tokio::time::timeout(TEMP_CLEANUP_TIMEOUT, sftp.remove_file(tmp)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) if is_not_found(&e) => {}
        Ok(Err(e)) => tracing::debug!(path = %tmp, "SFTP temp cleanup failed: {e}"),
        Err(_) => tracing::debug!(path = %tmp, "SFTP temp cleanup timed out"),
    }
}

/// The server's current time: the mtime of a freshly created probe file.
/// `None` when the server reports no mtime. The probe carries the temp prefix,
/// so a later sweep removes it if the cleanup here fails.
async fn server_now(sftp: &SftpSession, root: &str) -> RetryResult<Option<SystemTime>> {
    let probe = temp_path_for(&join_root_key(root, "sweep-probe"));
    let created: RetryResult<()> = async {
        let mut file = sftp
            .open_with_flags(
                &probe,
                OpenFlags::CREATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
            )
            .await
            .map_err(|e| sftp_retry_error("create", &probe, e))?;
        file.shutdown()
            .await
            .map_err(|e| io_retry_error("close", &probe, e))?;
        Ok(())
    }
    .await;
    let meta = match created {
        Ok(()) => sftp
            .metadata(&probe)
            .await
            .map_err(|e| sftp_retry_error("stat", &probe, e)),
        Err(e) => Err(e),
    };
    remove_temp_best_effort(sftp, &probe).await;
    Ok(meta?.modified().ok())
}

/// One attempt of an atomic upload: write `data` to the fresh temp file `tmp`,
/// then `posix-rename` it over `path`. `path` is never removed, so a failure
/// leaves either the old object or the new one in place.
async fn atomic_put(
    sftp: &SftpSession,
    path: &str,
    tmp: &str,
    ensure_parent: Option<&str>,
    data: &[u8],
    timeout_secs: u64,
) -> RetryResult<()> {
    if let Some(parent) = ensure_parent {
        mkdir_p(sftp, parent).await?;
    }

    let written: RetryResult<()> = async {
        let mut file = sftp
            .open_with_flags(
                tmp,
                OpenFlags::CREATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
            )
            .await
            .map_err(|e| sftp_retry_error("create", tmp, e))?;
        // Pipelined WRITEs queue behind each other; give them a scaled deadline,
        // and restore the normal one on every outcome since the session is reused.
        sftp.set_timeout(timeout_secs.saturating_mul(SFTP_WRITE_TIMEOUT_FACTOR));
        let wrote = file.write_all(data).await;
        sftp.set_timeout(timeout_secs);
        wrote.map_err(|e| io_retry_error("write", tmp, e))?;
        // Drains write acks and sends fsync@openssh.com when supported.
        file.flush()
            .await
            .map_err(|e| io_retry_error("flush", tmp, e))?;
        file.shutdown()
            .await
            .map_err(|e| io_retry_error("close", tmp, e))?;
        Ok(())
    }
    .await;
    if let Err(e) = written {
        remove_temp_best_effort(sftp, tmp).await;
        return Err(e);
    }

    match sftp.posix_rename(tmp, path).await {
        Ok(()) => Ok(()),
        // The server answered and refused: nothing was published.
        Err(e @ russh_sftp::client::error::Error::Status(_)) => {
            remove_temp_best_effort(sftp, tmp).await;
            Err(sftp_retry_error("rename", path, e))
        }
        // No answer: the rename may or may not have happened. Re-publishing
        // the same bytes under a new temp name is idempotent.
        Err(e) => Err(RetryError::transient(VykarError::Other(format!(
            "SFTP rename '{tmp}' -> '{path}': {e}"
        )))),
    }
}

impl SftpBackend {
    fn put_arc(&self, key: &str, data: Arc<Vec<u8>>) -> Result<()> {
        let path = self.full_path(key);
        let parent = path
            .rsplit_once('/')
            .map(|(p, _)| p)
            .filter(|p| !p.is_empty());
        let ensure_parent = parent.filter(|p| !self.is_known_dir(p));
        // Temp name of the previous attempt. Its own cleanup ran on a session
        // that may have been dead, and a rename with no reply leaves the temp
        // by design, so the next attempt retries the removal on its fresh
        // connection. A temp left by the final attempt waits for compact.
        let prev_tmp: RefCell<Option<String>> = RefCell::new(None);
        self.retry_op(&format!("PUT {key}"), |sftp| {
            // Atomic uploads need an overwriting rename. Plain SSH_FXP_RENAME
            // does not replace an existing target on OpenSSH, and a
            // delete-then-rename fallback would open a window with no object
            // at all. Checked before anything is created; reads still work.
            if !sftp.supports_posix_rename() {
                return Err(RetryError::permanent(VykarError::UnsupportedBackend(
                    format!(
                        "SFTP server {}:{} does not support posix-rename@openssh.com, which \
                         vykar requires for atomic uploads (OpenSSH's sftp-server provides it)",
                        self.params.host, self.params.port
                    ),
                )));
            }
            let tmp = temp_path_for(&path);
            let prev = prev_tmp.replace(Some(tmp.clone()));
            ASYNC_RUNTIME.block_on(async {
                if let Some(prev) = prev {
                    remove_temp_best_effort(sftp, &prev).await;
                }
                atomic_put(
                    sftp,
                    &path,
                    &tmp,
                    ensure_parent,
                    &data,
                    self.params.sftp_timeout_secs,
                )
                .await
            })
        })?;
        if let Some(parent) = ensure_parent {
            self.mark_dir_known(parent);
        }
        Ok(())
    }
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
        self.put_arc(key, Arc::new(data.to_vec()))
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
        Ok(self
            .list_sized(prefix)?
            .into_iter()
            .map(|o| o.key)
            .collect())
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
                    match file
                        .read(buf.get_mut(total..).expect("total <= buf.len()"))
                        .await
                    {
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
        self.put_arc(key, Arc::new(data))
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        let dir_path = self.full_path(prefix);
        let root = self.params.root.clone();
        self.retry_op(&format!("LIST {prefix}"), |sftp| {
            ASYNC_RUNTIME.block_on(list_recursive(sftp, &dir_path, &root))
        })
    }

    fn remove_stale_temp_files(&self, older_than: Duration) -> Result<usize> {
        let root = self.params.root.clone();
        let removed = std::sync::atomic::AtomicUsize::new(0);
        self.retry_op("SWEEP temp files", |sftp| {
            ASYNC_RUNTIME.block_on(async {
                // Ages are measured against the server's clock: comparing a
                // server mtime with the client clock deletes live temps when
                // the client runs ahead.
                let cutoff_clock = if older_than.is_zero() {
                    None
                } else {
                    match server_now(sftp, &root).await? {
                        Some(now) => Some(now),
                        None => {
                            tracing::debug!("SFTP server returned no mtime; skipping temp sweep");
                            return Ok(());
                        }
                    }
                };
                let mut stale = Vec::new();
                walk_repo_files(sftp, &root, |key, meta| {
                    if !vykar_protocol::is_repo_temp_file(key) {
                        return;
                    }
                    let stale_entry = match cutoff_clock {
                        None => true,
                        Some(now) => meta
                            .modified()
                            .is_ok_and(|modified| crate::is_stale(now, modified, older_than)),
                    };
                    if stale_entry {
                        stale.push(key.to_string());
                    }
                })
                .await?;
                for key in stale {
                    let path = join_root_key(&root, &key);
                    match sftp.remove_file(&path).await {
                        Ok(()) => {
                            removed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) if is_not_found(&e) => {}
                        // The server refused (e.g. permissions): one stuck
                        // file must not fail the sweep, and with it
                        // delete-repo. Transport errors still retry.
                        Err(russh_sftp::client::error::Error::Status(status)) => {
                            tracing::warn!(path = %path, "cannot remove temp file: {status:?}");
                        }
                        Err(e) => return Err(sftp_retry_error("delete", &path, e)),
                    }
                }
                Ok(())
            })
        })?;
        Ok(removed.into_inner())
    }

    fn profile(&self) -> crate::BackendProfile {
        crate::BackendProfile {
            local: false,
            staged_put: false,
            // Deliberately exceeds the pool; extra requests wait for a connection.
            request_width: crate::remote_request_width(Some(self.max_connections)),
            // The pool size: each transfer holds one pooled session.
            transfer_width: self.max_connections,
        }
    }

    crate::unsupported_server_ops!(except[remove_stale_temp_files]);
}

#[cfg(test)]
mod fault_server;

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
    fn test_posix_join() {
        // Basic joins
        assert_eq!(posix_join("/repo", "keys/repokey"), "/repo/keys/repokey");
        assert_eq!(
            posix_join("/repo", "packs/ab/deadbeef"),
            "/repo/packs/ab/deadbeef"
        );

        // Trailing/leading slash normalization
        assert_eq!(posix_join("/repo/", "keys"), "/repo/keys");
        assert_eq!(posix_join("/repo", "/keys"), "/repo/keys");
        assert_eq!(posix_join("/repo/", "/keys"), "/repo/keys");

        // Empty child returns base unchanged
        assert_eq!(posix_join("/repo", ""), "/repo");

        // Root-only base preserves "/" when child is empty
        assert_eq!(posix_join("/", ""), "/");
        assert_eq!(posix_join("/", "config"), "/config");

        // Never produces backslashes (the actual bug)
        let result = posix_join("/repo", "packs/ab/deadbeef");
        assert!(
            !result.contains('\\'),
            "posix_join must never produce backslashes: {result}"
        );
    }

    #[test]
    fn test_join_root_key() {
        assert_eq!(join_root_key("/", "packs/abc"), "/packs/abc");
        assert_eq!(join_root_key("/repo", "packs/abc"), "/repo/packs/abc");
        assert_eq!(join_root_key("/repo", "/packs/abc"), "/repo/packs/abc");
        // Root-only repo with empty key (e.g. list("") on SFTP root)
        assert_eq!(join_root_key("/", ""), "/");
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
    fn profile_width_is_three_per_connection() {
        // Construction is lazy: no connection is made.
        let backend = SftpBackend::new(
            "127.0.0.1",
            Some("vykar"),
            None,
            "/repo",
            None,
            None,
            Some("/nonexistent/known_hosts"),
            Some(4),
            None,
            RetryConfig::default(),
        )
        .unwrap();
        assert_eq!(
            backend.profile(),
            crate::BackendProfile {
                local: false,
                staged_put: false,
                request_width: 12,
                transfer_width: 4,
            }
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
        // russh-sftp collapses non-timeout SFTP errors into ErrorKind::Other
        let err = std::io::Error::other("Timeout");
        let retry = io_retry_error("write", "/test/path", err);
        assert!(retry.retryable, "ErrorKind::Other should be retryable");
    }

    /// russh-sftp 3.0 maps its `Timeout` error to `ErrorKind::TimedOut`,
    /// where 2.x collapsed it into `Other`. Both are retryable, so the
    /// upgrade did not silently make a transient timeout permanent.
    #[test]
    fn test_io_retry_error_timed_out_is_retryable() {
        let err = std::io::Error::from(std::io::ErrorKind::TimedOut);
        let retry = io_retry_error("read", "/test/path", err);
        assert!(retry.retryable, "ErrorKind::TimedOut should be retryable");
    }

    #[test]
    fn test_io_retry_error_permission_denied_is_not_retryable() {
        let err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let retry = io_retry_error("write", "/test/path", err);
        assert!(!retry.retryable, "PermissionDenied should not be retryable");
    }

    #[test]
    fn record_pool_wait_under_limit_continues() {
        let mut state = RetryState::default();
        for i in 1..30 {
            assert!(!record_pool_wait(&mut state, 30));
            assert_eq!(state.pool_waits_used, i);
            assert_eq!(state.transport_retries_used, 0);
        }
    }

    #[test]
    fn record_pool_wait_at_limit_signals_wedged() {
        let mut state = RetryState {
            transport_retries_used: 0,
            pool_waits_used: 29,
        };
        assert!(record_pool_wait(&mut state, 30));
        assert_eq!(state.pool_waits_used, 30);
        assert_eq!(state.transport_retries_used, 0);
    }

    #[test]
    fn record_transient_under_budget_increments_and_continues() {
        let mut state = RetryState::default();
        for i in 1..=5 {
            assert!(!record_transient(&mut state, 5));
            assert_eq!(state.transport_retries_used, i);
        }
    }

    #[test]
    fn record_transient_at_budget_returns_exhausted_without_increment() {
        let mut state = RetryState {
            transport_retries_used: 5,
            pool_waits_used: 0,
        };
        assert!(record_transient(&mut state, 5));
        assert_eq!(state.transport_retries_used, 5);
    }

    #[test]
    fn pool_waits_do_not_consume_transport_budget() {
        let mut state = RetryState::default();
        // Interleave many pool waits with the transport retries.
        for _ in 0..10 {
            assert!(!record_pool_wait(&mut state, 30));
            assert_eq!(state.transport_retries_used, 0);
        }
        // The full transport budget is still available.
        for i in 1..=5 {
            assert!(!record_transient(&mut state, 5));
            assert_eq!(state.transport_retries_used, i);
        }
        // More pool waits in the middle do not free transport slots.
        for _ in 0..5 {
            assert!(!record_pool_wait(&mut state, 30));
        }
        // The 6th transient attempt exhausts the budget.
        assert!(record_transient(&mut state, 5));
    }

    // ── Atomic upload against an in-process server with injected faults ──

    use fault_server::FaultServer;
    use std::sync::atomic::Ordering;

    fn read(server: &FaultServer, key: &str) -> Vec<u8> {
        std::fs::read(server.path(key)).unwrap()
    }

    #[test]
    fn reads_work_without_posix_rename_but_put_refuses() {
        let server = FaultServer::start();
        backend_put_seed(&server);
        server
            .faults
            .hide_posix_rename
            .store(true, Ordering::SeqCst);
        let backend = server.backend(3);

        assert_eq!(backend.get("index").unwrap().unwrap(), b"old index");
        assert_eq!(backend.list("").unwrap(), vec!["index".to_string()]);
        assert!(backend.exists("index").unwrap());
        assert_eq!(backend.size("index").unwrap(), Some(9));

        let err = backend.put("index", b"new").unwrap_err();
        assert!(matches!(err, VykarError::UnsupportedBackend(_)), "{err}");
        assert!(err.to_string().contains("posix-rename"), "{err}");
        assert_eq!(read(&server, "index"), b"old index");
        assert!(server.temps_next_to("index").is_empty());
    }

    #[test]
    fn is_stale_uses_given_clock() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let hour = Duration::from_secs(3600);
        assert!(crate::is_stale(now, now - hour, hour));
        assert!(crate::is_stale(now, now - 2 * hour, hour));
        assert!(!crate::is_stale(
            now,
            now - hour + Duration::from_secs(1),
            hour
        ));
        assert!(
            !crate::is_stale(now, now + hour, hour),
            "future mtime is never stale"
        );
        assert!(crate::is_stale(now, now, Duration::ZERO));
    }

    #[test]
    fn sweep_uses_server_clock_not_client_clock() {
        let server = FaultServer::start();
        // Server clock two hours behind the client: by the client clock a
        // fresh temp would look two hours old.
        server.faults.mtime_skew_secs.store(-7200, Ordering::SeqCst);
        std::fs::write(server.path(".tmp.index.0123456789abcdef"), b"live").unwrap();

        let backend = server.backend(0);
        assert_eq!(
            backend
                .remove_stale_temp_files(Duration::from_secs(3600))
                .unwrap(),
            0
        );
        assert_eq!(
            server.temps_next_to("index"),
            vec![".tmp.index.0123456789abcdef".to_string()],
            "live temp kept and probe removed"
        );
    }

    #[test]
    fn put_replaces_existing_object_without_debris() {
        let server = FaultServer::start();
        let backend = server.backend(0);
        backend.put("index", b"old index").unwrap();
        backend.put("index", b"new index bytes").unwrap();
        backend
            .put_owned("packs/ab/abcd", vec![7; 100_000])
            .unwrap();

        assert_eq!(read(&server, "index"), b"new index bytes");
        assert_eq!(read(&server, "packs/ab/abcd"), vec![7; 100_000]);
        assert!(server.temps_next_to("index").is_empty());
        assert!(server.temps_next_to("packs/ab/abcd").is_empty());
        assert_eq!(backend.get("index").unwrap().unwrap(), b"new index bytes");
    }

    #[test]
    fn refused_rename_keeps_old_object_and_removes_temp() {
        let server = FaultServer::start();
        let backend = server.backend(3);
        backend.put("index", b"old index").unwrap();

        server.faults.refuse_rename.store(true, Ordering::SeqCst);
        assert!(backend.put("index", b"new index").is_err());

        assert_eq!(read(&server, "index"), b"old index");
        assert!(server.temps_next_to("index").is_empty());
    }

    #[test]
    fn lost_rename_reply_is_retried_to_success() {
        let server = FaultServer::start();
        let backend = server.backend(3);
        backend.put("index", b"old index").unwrap();

        server
            .faults
            .drop_rename_reply_once
            .store(true, Ordering::SeqCst);
        backend.put("index", b"new index").unwrap();

        assert_eq!(read(&server, "index"), b"new index");
        // One rename whose reply was lost, one from the retry, plus the seed.
        assert_eq!(server.faults.renames.load(Ordering::SeqCst), 3);
        assert!(server.temps_next_to("index").is_empty());
    }

    #[test]
    fn dropped_connection_mid_write_leaves_destination_untouched() {
        let server = FaultServer::start();
        backend_put_seed(&server);

        server
            .faults
            .drop_on_write_once
            .store(true, Ordering::SeqCst);
        let backend = server.backend(0);
        assert!(backend.put("index", b"new index").is_err());

        assert_eq!(read(&server, "index"), b"old index");
        let temps = server.temps_next_to("index");
        assert_eq!(temps.len(), 1, "{temps:?}");
        assert!(temps[0].starts_with(".tmp.index."), "{temps:?}");

        // Listing hides the debris; the sweep removes it.
        let backend = server.backend(0);
        let listed = backend.list_sized("").unwrap();
        assert_eq!(
            listed,
            vec![ListedObject {
                key: "index".into(),
                size: Some(9)
            }]
        );
        assert_eq!(
            backend
                .remove_stale_temp_files(Duration::from_secs(3600))
                .unwrap(),
            0,
            "fresh debris is kept"
        );
        assert_eq!(backend.remove_stale_temp_files(Duration::ZERO).unwrap(), 1);
        assert!(server.temps_next_to("index").is_empty());
        assert_eq!(read(&server, "index"), b"old index");
    }

    #[test]
    fn retry_removes_temp_left_by_dropped_connection() {
        let server = FaultServer::start();
        backend_put_seed(&server);

        server
            .faults
            .drop_on_write_once
            .store(true, Ordering::SeqCst);
        let backend = server.backend(3);
        backend.put("index", b"new index").unwrap();

        assert_eq!(read(&server, "index"), b"new index");
        assert!(server.temps_next_to("index").is_empty());
    }

    #[test]
    fn sweep_leaves_foreign_directories_alone() {
        let server = FaultServer::start();
        backend_put_seed(&server);
        let foreign = server.path("other-app/.tmp.cache");
        std::fs::create_dir_all(foreign.parent().unwrap()).unwrap();
        std::fs::write(&foreign, b"x").unwrap();
        std::fs::create_dir_all(server.path("packs/ab")).unwrap();
        std::fs::write(server.path("packs/ab/.tmp.x.0"), b"x").unwrap();

        let backend = server.backend(0);
        assert_eq!(backend.remove_stale_temp_files(Duration::ZERO).unwrap(), 1);
        assert!(foreign.exists());
        assert!(server.temps_next_to("packs/ab/x").is_empty());
    }

    #[test]
    fn session_config_pipelines_two_mib_of_writes() {
        let cfg = sftp_session_config(42);
        assert_eq!(cfg.request_timeout_secs, 42);
        assert_eq!(cfg.max_concurrent_writes, 64);
        assert_eq!(cfg.max_write_packet_len, 32 * 1024);
        let in_flight = cfg.max_concurrent_writes as u64 * u64::from(cfg.max_write_packet_len);
        assert_eq!(
            in_flight,
            2 * 1024 * 1024,
            "in-flight upload bytes per connection match OpenSSH's channel window"
        );
        assert_eq!(
            in_flight / SFTP_WRITE_TIMEOUT_FACTOR,
            16 * 32 * 1024,
            "a queued WRITE gets the same byte budget per sftp_timeout as at the library default"
        );
        let lib = SftpConfig::default();
        assert_eq!(cfg.max_concurrent_reads, lib.max_concurrent_reads);
        assert_eq!(cfg.max_packet_len, lib.max_packet_len);
    }

    /// 3 MiB with a period that is not a power of two, so a WRITE applied at
    /// the wrong offset changes the bytes.
    fn patterned_payload() -> Vec<u8> {
        (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn large_put_survives_deep_write_pipeline() {
        let server = FaultServer::start();
        let data = patterned_payload();
        let backend = server.backend(0);
        backend
            .put_owned("packs/ab/deadbeef", data.clone())
            .unwrap();

        assert_eq!(read(&server, "packs/ab/deadbeef"), data);
        assert!(server.temps_next_to("packs/ab/deadbeef").is_empty());
        // At least one WRITE per 32 KiB: the write size was not raised.
        let writes = server.faults.writes.load(Ordering::SeqCst);
        assert!(writes >= data.len().div_ceil(32 * 1024), "{writes} writes");
    }

    #[test]
    fn connection_drop_with_full_write_pipeline_retries_to_success() {
        let server = FaultServer::start();
        backend_put_seed(&server);
        let writes_now = server.faults.writes.load(Ordering::SeqCst);
        server
            .faults
            .drop_on_write_number
            .store(writes_now + 80, Ordering::SeqCst);

        let data = patterned_payload();
        let backend = server.backend(3);
        backend
            .put_owned("packs/ab/deadbeef", data.clone())
            .unwrap();

        assert_eq!(read(&server, "packs/ab/deadbeef"), data);
        assert!(server.temps_next_to("packs/ab/deadbeef").is_empty());
        assert_eq!(
            server.faults.drop_on_write_number.load(Ordering::SeqCst),
            0,
            "the fault fired"
        );
    }

    #[test]
    fn authenticates_with_ssh_agent() {
        use russh::keys::ssh_key::private::Ed25519Keypair;

        let server = FaultServer::start();
        let sock = server.start_agent();

        // No sftp_key: any agent key the server accepts.
        let backend = server.backend_with(0, None, Some(&sock));
        backend.put("index", b"no key").unwrap();
        assert_eq!(read(&server, "index"), b"no key");

        // A .pub sftp_key selects that agent key.
        let write_pub = |name: &str, key: PrivateKey| {
            let path = server.dir.path().join(name);
            std::fs::write(&path, key.public_key().to_openssh().unwrap()).unwrap();
            path
        };
        let held = write_pub("held.pub", fault_server::client_private_key());
        let absent = write_pub(
            "absent.pub",
            PrivateKey::from(Ed25519Keypair::from_seed(&[8; 32])),
        );

        let backend = server.backend_with(0, Some(&held), Some(&sock));
        backend.put("index", b"via agent").unwrap();
        assert_eq!(read(&server, "index"), b"via agent");

        // The agent holds an accepted key, but not the one named, so login fails.
        let err = server
            .backend_with(0, Some(&absent), Some(&sock))
            .get("index")
            .unwrap_err();
        assert!(err.to_string().contains("no accepted key"), "{err}");
    }

    #[test]
    #[ignore = "waits the full 30 s agent timeout"]
    fn ssh_agent_stall_times_out() {
        let server = FaultServer::start();
        let sock = server.start_agent();
        server.faults.stall_agent.store(true, Ordering::SeqCst);
        let backend = server.backend_with(0, None, Some(&sock));

        let result = ASYNC_RUNTIME.block_on(async {
            tokio::time::timeout(
                CONNECT_TIMEOUT + Duration::from_secs(5),
                SftpBackend::connect(&backend.params),
            )
            .await
        });
        let err = result
            .expect("agent authentication must have its own deadline")
            .err()
            .expect("a stalled agent must fail");
        assert!(err.retryable, "{err:?}");
        assert!(err.err.to_string().contains("timeout"), "{err:?}");
    }

    #[test]
    fn ssh_agent_disconnect_retries() {
        let server = FaultServer::start();
        let sock = server.start_agent();
        server.faults.drop_agent_once.store(true, Ordering::SeqCst);

        server
            .backend_with(1, None, Some(&sock))
            .put("index", b"retried agent")
            .unwrap();
        assert_eq!(read(&server, "index"), b"retried agent");
        assert!(!server.faults.drop_agent_once.load(Ordering::SeqCst));
    }

    #[test]
    fn ssh_agent_auth_disconnect_is_permanent() {
        let server = FaultServer::start();
        let sock = server.start_agent();
        server
            .faults
            .disconnect_on_auth
            .store(true, Ordering::SeqCst);
        let backend = server.backend_with(0, None, Some(&sock));

        let err = ASYNC_RUNTIME
            .block_on(SftpBackend::connect(&backend.params))
            .err()
            .expect("a disconnect during agent auth must fail");
        assert!(!err.retryable, "{err:?}");
        assert!(err.err.to_string().contains("set sftp_key"), "{err:?}");
    }

    #[test]
    fn ssh_agent_connect_failure_retries_only_when_required() {
        let server = FaultServer::start();
        let sock = server.dir.path().join("no-agent.sock");
        let mut params = server.backend_with(0, None, Some(&sock)).params;
        let mut conn = ASYNC_RUNTIME
            .block_on(SftpBackend::connect(&server.backend(0).params))
            .unwrap();

        assert!(!ASYNC_RUNTIME
            .block_on(auth_with_agent(&mut conn._session, &params, None))
            .unwrap());
        params.agent_key = Some(fault_server::client_private_key().public_key().clone());
        let err = ASYNC_RUNTIME
            .block_on(auth_with_agent(&mut conn._session, &params, None))
            .unwrap_err();
        assert!(err.retryable, "{err:?}");
    }

    fn backend_put_seed(server: &FaultServer) {
        server.backend(0).put("index", b"old index").unwrap();
    }
}
