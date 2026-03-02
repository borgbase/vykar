use std::process::{Command, ExitStatus, Output};
use std::time::Duration;

/// Build a shell command for the current platform.
/// On Unix, the child is placed in its own process group so that
/// timeout termination can kill the entire tree.
pub fn command_for_script(script: &str) -> Command {
    #[cfg(windows)]
    {
        let mut cmd = Command::new("powershell");
        cmd.arg("-NoProfile")
            .arg("-NonInteractive")
            .arg("-Command")
            .arg(script);
        cmd
    }

    #[cfg(not(windows))]
    {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(script);
        // Place child in its own process group so we can kill the entire tree on timeout.
        // SAFETY: The pre_exec closure calls only setpgid, which is async-signal-safe
        // (POSIX.1-2008). No heap allocation, locks, or non-reentrant functions are used.
        unsafe {
            cmd.pre_exec(|| {
                // setpgid(0, 0) sets the process group ID to the child's own PID.
                nix::unistd::setpgid(nix::unistd::Pid::from_raw(0), nix::unistd::Pid::from_raw(0))
                    .map_err(std::io::Error::other)?;
                Ok(())
            });
        }
        cmd
    }
}

pub fn run_script(script: &str) -> std::io::Result<Output> {
    command_for_script(script).output()
}

/// Run a shell script with a timeout, capturing stdout/stderr.
/// Returns an error if the command does not complete within the given duration.
pub fn run_script_with_timeout(script: &str, timeout: Duration) -> std::io::Result<Output> {
    let mut cmd = command_for_script(script);
    run_command_with_timeout(&mut cmd, timeout)
}

/// Run an already-configured `Command` with a timeout. The command is spawned
/// with piped stdout/stderr and reader threads to avoid pipe-buffer deadlocks.
/// Returns an error if it does not complete in time.
pub fn run_command_with_timeout(cmd: &mut Command, timeout: Duration) -> std::io::Result<Output> {
    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Take stdout/stderr handles immediately and drain them in background threads
    // to prevent deadlock when the child fills the OS pipe buffer.
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();

    let stdout_thread = std::thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        if let Some(mut r) = stdout_handle {
            std::io::Read::read_to_end(&mut r, &mut buf)?;
        }
        Ok(buf)
    });

    let stderr_thread = std::thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        if let Some(mut r) = stderr_handle {
            std::io::Read::read_to_end(&mut r, &mut buf)?;
        }
        Ok(buf)
    });

    let deadline = std::time::Instant::now() + timeout;
    let poll_interval = Duration::from_millis(100);

    loop {
        match child.try_wait()? {
            Some(status) => {
                let stdout = stdout_thread
                    .join()
                    .map_err(|_| std::io::Error::other("stdout reader panicked"))??;
                let stderr = stderr_thread
                    .join()
                    .map_err(|_| std::io::Error::other("stderr reader panicked"))??;
                return Ok(Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            None => {
                if std::time::Instant::now() >= deadline {
                    terminate_child(&mut child);
                    let _ = child.wait();
                    // Join reader threads (they'll finish once pipes close)
                    let _ = stdout_thread.join();
                    let _ = stderr_thread.join();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("command timed out after {} seconds", timeout.as_secs()),
                    ));
                }
                std::thread::sleep(poll_interval);
            }
        }
    }
}

/// Run an already-configured `Command` with a timeout, inheriting stdout/stderr.
/// Unlike `run_command_with_timeout`, this does not capture output — the child
/// writes directly to the parent's terminal. Suitable for hooks.
pub fn run_command_status_with_timeout(
    cmd: &mut Command,
    timeout: Duration,
) -> std::io::Result<ExitStatus> {
    let mut child = cmd.spawn()?;

    let deadline = std::time::Instant::now() + timeout;
    let poll_interval = Duration::from_millis(100);

    loop {
        match child.try_wait()? {
            Some(status) => return Ok(status),
            None => {
                if std::time::Instant::now() >= deadline {
                    terminate_child(&mut child);
                    let _ = child.wait();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("command timed out after {} seconds", timeout.as_secs()),
                    ));
                }
                std::thread::sleep(poll_interval);
            }
        }
    }
}

/// Run a shell script with a timeout, inheriting stdout/stderr (no capture).
pub fn run_script_status_with_timeout(
    script: &str,
    timeout: Duration,
) -> std::io::Result<ExitStatus> {
    let mut cmd = command_for_script(script);
    run_command_status_with_timeout(&mut cmd, timeout)
}

/// Terminate a process group by PID.
///
/// On Unix: sends SIGTERM to the process group, waits 200ms, then SIGKILL.
/// On Windows: this is a no-op (use `terminate_child` instead).
pub fn terminate_process_group(pid: u32) {
    #[cfg(unix)]
    {
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::Pid;

        let pgid = Pid::from_raw(pid as i32);
        // Try graceful termination of the entire process group first.
        let _ = killpg(pgid, Signal::SIGTERM);
        std::thread::sleep(Duration::from_millis(200));
        // Force kill the process group.
        let _ = killpg(pgid, Signal::SIGKILL);
    }

    #[cfg(windows)]
    {
        let _ = pid;
    }
}

/// Terminate a child process and its process group.
///
/// On Unix: delegates to `terminate_process_group` for SIGTERM→SIGKILL.
/// On Windows: just kills the direct child (grandchild limitation documented).
fn terminate_child(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        terminate_process_group(child.id());
    }

    #[cfg(windows)]
    {
        // On Windows we can only kill the direct child; grandchildren may survive.
        let _ = child.kill();
    }
}

#[cfg(not(windows))]
use std::os::unix::process::CommandExt;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_large_stdout_no_deadlock() {
        // Generate output larger than typical pipe buffer (64KB on most systems).
        // Use yes piped to head to produce ~200K of output portably.
        let script = if cfg!(windows) {
            "1..2000 | ForEach-Object { Write-Output ('x' * 100) }"
        } else {
            "yes | head -c 200000"
        };
        let result = run_script_with_timeout(script, Duration::from_secs(10));
        assert!(result.is_ok(), "should not deadlock on large output");
        let output = result.unwrap();
        // On Unix, head may cause SIGPIPE on yes, so just check we got data
        assert!(
            output.stdout.len() > 65536,
            "expected large output, got {} bytes",
            output.stdout.len()
        );
    }

    #[test]
    fn test_timeout_fires() {
        let script = if cfg!(windows) {
            "Start-Sleep -Seconds 60"
        } else {
            "sleep 60"
        };
        let result = run_script_with_timeout(script, Duration::from_millis(500));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn test_status_timeout_fires() {
        let script = if cfg!(windows) {
            "Start-Sleep -Seconds 60"
        } else {
            "sleep 60"
        };
        let result = run_script_status_with_timeout(script, Duration::from_millis(500));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn test_status_success() {
        let script = if cfg!(windows) {
            "$true | Out-Null"
        } else {
            "true"
        };
        let result = run_script_status_with_timeout(script, Duration::from_secs(5));
        assert!(result.is_ok());
        assert!(result.unwrap().success());
    }

    #[cfg(unix)]
    #[test]
    fn test_timeout_kills_process_group() {
        use std::path::Path;

        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("grandchild_alive");
        let marker_path = marker.display().to_string();

        // Script spawns a background grandchild that creates a marker file after 2s
        let script = format!("(sleep 2 && touch {marker_path}) & sleep 60");

        let result = run_script_with_timeout(&script, Duration::from_millis(500));
        assert!(result.is_err()); // should time out

        // Wait a bit to see if the grandchild survived
        std::thread::sleep(Duration::from_secs(3));
        assert!(
            !Path::new(&marker_path).exists(),
            "grandchild should have been killed by process group termination"
        );
    }
}
