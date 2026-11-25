use std::io;
use std::process::Child;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use chrono::Utc;
use tracing::{info, warn};

use crate::chunker;
use crate::compress::Compression;
use crate::config::CommandDump;
use crate::platform::shell;
use crate::repo::pack::PackType;
use crate::repo::{Repo, WriteSession};
use crate::snapshot::SnapshotStats;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use super::commit::{FileKind, FileOutcome, finish_file, start_file};
use super::{BackupProgressEvent, ListingWriter, emit_stats_progress, with_rollback_checkpoint};

/// Default timeout for command_dump execution (1 hour).
pub(super) const COMMAND_DUMP_TIMEOUT: Duration = Duration::from_secs(3600);

/// Maximum stderr we keep in memory (1 MiB).
const MAX_STDERR: usize = 1 << 20;

/// RAII guard for a dump child process, its watchdog thread, and stderr reader.
/// On drop, kills the child and joins all threads. Never panics.
struct DumpProcessGuard {
    cancel_tx: Option<mpsc::Sender<()>>,
    watchdog: Option<JoinHandle<bool>>,
    child: Option<Child>,
    stderr_thread: Option<JoinHandle<io::Result<Vec<u8>>>>,
}

impl DumpProcessGuard {
    /// Happy-path completion: wait for child, cancel watchdog, collect stderr.
    fn finish(&mut self) -> Result<(std::process::ExitStatus, Vec<u8>, bool)> {
        // Wait for child to exit (if watchdog fires while we wait, it kills the
        // child, which unblocks wait()).
        let child = self.child.as_mut().ok_or_else(|| {
            VykarError::Other("internal: command_dump child already taken (logic bug)".into())
        })?;
        let status = child
            .wait()
            .map_err(|e| VykarError::Other(format!("failed to wait on child: {e}")))?;

        // Drop cancel_tx → watchdog wakes via Disconnected and returns false.
        self.cancel_tx.take();

        let timed_out = self
            .watchdog
            .take()
            .is_some_and(|h| h.join().unwrap_or(false));

        let stderr = self
            .stderr_thread
            .take()
            .map_or(Ok(Vec::new()), |h| h.join().unwrap_or(Ok(Vec::new())))
            .unwrap_or_default();

        self.child.take();

        Ok((status, stderr, timed_out))
    }
}

impl Drop for DumpProcessGuard {
    fn drop(&mut self) {
        // Cancel watchdog (instant wake via Disconnected).
        self.cancel_tx.take();

        // Kill child if still alive.
        if let Some(ref mut child) = self.child {
            shell::terminate_process_group(child.id());
            let _ = child.wait();
        }
        self.child.take();

        // Join threads.
        if let Some(h) = self.watchdog.take() {
            let _ = h.join();
        }
        if let Some(h) = self.stderr_thread.take() {
            let _ = h.join();
        }
    }
}

/// Stream a single command dump through the chunker with checkpoint/rollback.
fn stream_dump_command(
    repo: &Repo,
    ws: &mut WriteSession,
    dump: &CommandDump,
    compression: Compression,
    stats: &mut SnapshotStats,
    timeout: Duration,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
) -> Result<(Vec<ChunkRef>, u64)> {
    with_rollback_checkpoint(repo, ws, stats, |repo, ws, stats| {
        // Spawn child with piped stdout and stderr.
        let mut cmd = shell::command_for_script(&dump.command);
        let mut child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                VykarError::Other(format!(
                    "failed to spawn command_dump '{}': {}",
                    dump.name, e
                ))
            })?;

        let child_id = child.id();
        let stdout = child.stdout.take();
        let stderr_handle = child.stderr.take();

        // Watchdog thread: recv_timeout-based, no polling.
        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        let watchdog = std::thread::spawn(move || {
            match cancel_rx.recv_timeout(timeout) {
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    shell::terminate_process_group(child_id);
                    true // timed out
                }
                _ => false, // cancelled (Disconnected) or spurious Ok
            }
        });

        // Stderr reader with hard cap.
        let stderr_thread = std::thread::spawn(move || -> io::Result<Vec<u8>> {
            let mut buf = Vec::new();
            if let Some(mut r) = stderr_handle {
                io::Read::read_to_end(&mut io::Read::take(&mut r, MAX_STDERR as u64), &mut buf)?;
                io::copy(&mut r, &mut io::sink())?;
            }
            Ok(buf)
        });

        let mut guard = DumpProcessGuard {
            cancel_tx: Some(cancel_tx),
            watchdog: Some(watchdog),
            child: Some(child),
            stderr_thread: Some(stderr_thread),
        };

        // Stream stdout through chunker.
        let chunk_hasher = repo.crypto().chunk_hasher().clone();
        let stdout = stdout.ok_or_else(|| {
            VykarError::Other("internal: command_dump stdout not piped (config bug)".into())
        })?;
        let chunk_stream = chunker::chunk_stream(stdout, &repo.config().chunker_params);

        let mut chunk_refs = Vec::new();
        let mut total_size: u64 = 0;
        const PROGRESS_INTERVAL_BYTES: u64 = 4 * 1024 * 1024;
        let mut bytes_since_progress: u64 = 0;

        for chunk_result in chunk_stream {
            let chunk = chunk_result.map_err(|e| match e {
                fastcdc::v2020::Error::IoError(ioe) => VykarError::Io(ioe),
                other => VykarError::Other(format!(
                    "chunking failed for command_dump '{}': {other}",
                    dump.name
                )),
            })?;

            // FastCDC bounds chunk size at chunker_params.max_size (≤ 16 MiB),
            // well below u32::MAX — the cast cannot overflow.
            debug_assert!(chunk.data.len() <= u32::MAX as usize);
            let size = chunk.data.len() as u32;
            total_size += size as u64;
            let chunk_id = ChunkId::compute(&chunk_hasher, &chunk.data);

            if let Some(csize) = ws.bump_ref_if_exists(repo, &chunk_id) {
                stats.original_size += size as u64;
                stats.compressed_size += csize as u64;
                chunk_refs.push(ChunkRef {
                    id: chunk_id,
                    size,
                    csize,
                });
            } else {
                let csize = ws.commit_chunk_inline(
                    repo,
                    chunk_id,
                    &chunk.data,
                    compression,
                    PackType::Data,
                )?;
                stats.original_size += size as u64;
                stats.compressed_size += csize as u64;
                stats.deduplicated_size += csize as u64;
                chunk_refs.push(ChunkRef {
                    id: chunk_id,
                    size,
                    csize,
                });
            }

            bytes_since_progress += size as u64;
            if bytes_since_progress >= PROGRESS_INTERVAL_BYTES {
                emit_stats_progress(progress, stats, None);
                bytes_since_progress = 0;
            }
        }

        let (status, stderr, timed_out) = guard.finish()?;

        if timed_out {
            return Err(VykarError::Other(format!(
                "command_dump '{}' timed out after {} seconds",
                dump.name,
                timeout.as_secs()
            )));
        }

        if !status.success() {
            let code = status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let stderr_str = String::from_utf8_lossy(&stderr);
            return Err(VykarError::Other(format!(
                "command_dump '{}' failed (exit code {code}): {stderr_str}",
                dump.name
            )));
        }

        if chunk_refs.is_empty() {
            warn!(name = %dump.name, "command_dump produced empty output");
        }

        Ok((chunk_refs, total_size))
    })
}

/// Listing segment root of the command dumps (06 step 4): the path of the
/// `vykar-dumps` directory item.
pub(crate) const DUMPS_ROOT: &[u8] = b"vykar-dumps";

#[allow(clippy::too_many_arguments)]
pub(super) fn process_command_dumps(
    repo: &Repo,
    ws: &mut WriteSession,
    command_dumps: &[CommandDump],
    compression: Compression,
    listing: &mut ListingWriter,
    stats: &mut SnapshotStats,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    time_start: chrono::DateTime<Utc>,
) -> Result<()> {
    if command_dumps.is_empty() {
        return Ok(());
    }

    let dumps_dir_item = Item {
        path: "vykar-dumps".to_string(),
        entry_type: ItemType::Directory,
        mode: 0o755,
        uid: 0,
        gid: 0,
        user: None,
        group: None,
        mtime: 0,
        atime: None,
        ctime: None,
        size: 0,
        chunks: Vec::new(),
        link_target: None,
        xattrs: None,
        raw_names: None,
        hardlink: None,
    };
    debug_assert_eq!(dumps_dir_item.path.as_bytes(), DUMPS_ROOT);
    listing.begin_segment(repo, ws, DUMPS_ROOT.to_vec())?;
    listing.push(repo, ws, &dumps_dir_item)?;

    let mut dump_items: Vec<Item> = Vec::with_capacity(command_dumps.len());
    for dump in command_dumps {
        info!(
            name = %dump.name,
            command = %dump.command,
            "executing command dump (streaming)"
        );

        let path = format!("vykar-dumps/{}", dump.name);
        let dedup_before = start_file(progress, stats, &path);

        let (chunk_refs, total_size) = stream_dump_command(
            repo,
            ws,
            dump,
            compression,
            stats,
            COMMAND_DUMP_TIMEOUT,
            progress,
        )?;

        let dump_item = Item {
            path,
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: time_start.timestamp_nanos_opt().unwrap_or(0),
            atime: None,
            ctime: None,
            size: total_size,
            chunks: chunk_refs,
            link_target: None,
            xattrs: None,
            raw_names: None,
            hardlink: None,
        };
        // `verbose` is unused for a dump (no FileProcessed event).
        finish_file(
            repo,
            ws,
            stats,
            listing,
            progress,
            false,
            FileOutcome {
                item: dump_item,
                dedup_before,
                kind: FileKind::Dump {
                    items: &mut dump_items,
                },
            },
        )?;
    }

    // Name order within vykar-dumps/ (06 order 1); dumps still run in config
    // order, since a later dump may depend on an earlier one.
    dump_items.sort_unstable_by(|a, b| a.path_bytes().cmp(b.path_bytes()));
    for item in &dump_items {
        listing.push(repo, ws, item)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CommandDump;

    #[cfg(windows)]
    fn shell_echo_hello() -> &'static str {
        "Write-Output hello"
    }

    #[cfg(not(windows))]
    fn shell_echo_hello() -> &'static str {
        "echo hello"
    }

    #[cfg(windows)]
    fn shell_fail() -> &'static str {
        "exit 1"
    }

    #[cfg(not(windows))]
    fn shell_fail() -> &'static str {
        "false"
    }

    #[cfg(windows)]
    fn shell_success_no_output() -> &'static str {
        "$null = 1"
    }

    #[cfg(not(windows))]
    fn shell_success_no_output() -> &'static str {
        "true"
    }

    /// Helper: set up a repo with a write session for streaming dump tests.
    fn setup_test_repo() -> (Repo, WriteSession) {
        crate::testutil::test_repo_plaintext_with_session()
    }

    #[test]
    fn streaming_dump_captures_stdout() {
        let (repo, mut ws) = setup_test_repo();
        let dump = CommandDump {
            name: "test.txt".to_string(),
            command: shell_echo_hello().to_string(),
        };
        let mut stats = SnapshotStats::default();
        let (refs, total_size) = stream_dump_command(
            &repo,
            &mut ws,
            &dump,
            Compression::None,
            &mut stats,
            Duration::from_secs(10),
            &mut None,
        )
        .unwrap();
        assert!(!refs.is_empty());
        assert!(total_size > 0);
    }

    #[test]
    fn streaming_dump_fails_on_nonzero_exit() {
        let (repo, mut ws) = setup_test_repo();
        let dump = CommandDump {
            name: "fail.txt".to_string(),
            command: shell_fail().to_string(),
        };
        let mut stats = SnapshotStats::default();
        let result = stream_dump_command(
            &repo,
            &mut ws,
            &dump,
            Compression::None,
            &mut stats,
            Duration::from_secs(10),
            &mut None,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("command_dump 'fail.txt' failed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn streaming_dump_empty_stdout_succeeds() {
        let (repo, mut ws) = setup_test_repo();
        let dump = CommandDump {
            name: "empty.txt".to_string(),
            command: shell_success_no_output().to_string(),
        };
        let mut stats = SnapshotStats::default();
        let (refs, total_size) = stream_dump_command(
            &repo,
            &mut ws,
            &dump,
            Compression::None,
            &mut stats,
            Duration::from_secs(10),
            &mut None,
        )
        .unwrap();
        assert!(refs.is_empty());
        assert_eq!(total_size, 0);
    }

    #[cfg(unix)]
    #[test]
    fn streaming_dump_timeout_kills_child() {
        let (repo, mut ws) = setup_test_repo();
        let dump = CommandDump {
            name: "hang.txt".to_string(),
            command: "sleep 60".to_string(),
        };
        let mut stats = SnapshotStats::default();
        let result = stream_dump_command(
            &repo,
            &mut ws,
            &dump,
            Compression::None,
            &mut stats,
            Duration::from_millis(500),
            &mut None,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("timed out"),
            "expected timeout error, got: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn streaming_dump_fail_after_large_output() {
        let (repo, mut ws) = setup_test_repo();
        let dump = CommandDump {
            name: "big_fail.txt".to_string(),
            command: "head -c 1000000 /dev/urandom; exit 1".to_string(),
        };
        let mut stats = SnapshotStats::default();
        let result = stream_dump_command(
            &repo,
            &mut ws,
            &dump,
            Compression::None,
            &mut stats,
            Duration::from_secs(30),
            &mut None,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("command_dump 'big_fail.txt' failed"),
            "unexpected error: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn streaming_dump_stderr_capped() {
        let (repo, mut ws) = setup_test_repo();
        // Write 2 MiB to stderr, nothing to stdout
        let dump = CommandDump {
            name: "stderr_test.txt".to_string(),
            command: "head -c 2097152 /dev/urandom >&2; exit 1".to_string(),
        };
        let mut stats = SnapshotStats::default();
        let result = stream_dump_command(
            &repo,
            &mut ws,
            &dump,
            Compression::None,
            &mut stats,
            Duration::from_secs(30),
            &mut None,
        );
        assert!(result.is_err());
        // The error message will contain stderr, verify we didn't OOM
        let err = result.unwrap_err().to_string();
        assert!(err.contains("command_dump 'stderr_test.txt' failed"));
    }
}
