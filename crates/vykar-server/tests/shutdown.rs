#![cfg(unix)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::panic, clippy::indexing_slicing)]

//! Process-level tests for SIGINT/SIGTERM handling. They spawn the real
//! binary, so signal registration, the readiness log line and the drain are
//! all exercised end to end.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const TIMEOUT: Duration = Duration::from_secs(10);

struct Server {
    child: Child,
    port: u16,
    data_dir: TempDir,
    stdout: Receiver<String>,
}

/// Kill and reap the child on every exit path, including a panicking test,
/// so a failed assertion never leaves a server running. Field drop order
/// then removes the data dir after the process has released it.
impl Drop for Server {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

impl Server {
    fn spawn() -> Self {
        let data_dir = tempfile::tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_vykar-server"))
            .args(["--listen", "127.0.0.1:0", "--data-dir"])
            .arg(data_dir.path())
            .env("VYKAR_TOKEN", "test")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        // Drain stdout for the life of the child so it never blocks on a full
        // pipe, forwarding every line so tests can wait for specific ones.
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });

        let mut server = Self {
            child,
            port: 0,
            data_dir,
            stdout: rx,
        };
        let line = server.wait_for_line("listening on 127.0.0.1:");
        let port_str = line.rsplit(':').next().unwrap().trim();
        server.port = port_str.parse().unwrap();
        server
    }

    /// Block until a stdout line containing `needle` arrives, or panic after
    /// `TIMEOUT`.
    fn wait_for_line(&mut self, needle: &str) -> String {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.stdout.recv_timeout(remaining) {
                Ok(line) if line.contains(needle) => return line,
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => {
                    panic!("did not see {needle:?} on stdout within {TIMEOUT:?}");
                }
            }
        }
    }

    fn signal(&self, sig: &str) {
        let status = Command::new("kill")
            .args([sig, &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success(), "kill {sig} failed");
    }

    fn wait_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() <= deadline,
                "server did not exit within {TIMEOUT:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Open a PUT whose body never completes, and return once the handler has
    /// created its temp file, proving the request is in flight.
    fn hold_request_open(&self) -> TcpStream {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        stream
            .write_all(
                b"PUT /snapshots/hold HTTP/1.1\r\n\
                  Host: 127.0.0.1\r\n\
                  Authorization: Bearer test\r\n\
                  Content-Length: 1048576\r\n\
                  \r\n\
                  0123456789abcdef",
            )
            .unwrap();
        stream.flush().unwrap();

        let snapshots = self.data_dir.path().join("snapshots");
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let in_flight = std::fs::read_dir(&snapshots).ok().is_some_and(|entries| {
                entries
                    .flatten()
                    .any(|e| e.file_name().to_string_lossy().starts_with(".tmp.hold."))
            });
            if in_flight {
                return stream;
            }
            assert!(
                Instant::now() < deadline,
                "handler did not create snapshots/.tmp.hold.* within {TIMEOUT:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
fn sigterm_idle_exits_cleanly() {
    let mut server = Server::spawn();
    server.signal("-TERM");
    server.wait_for_line("finishing in-flight requests");
    let status = server.wait_exit();
    assert!(status.success(), "expected exit 0, got {status:?}");
}

fn second_signal_exits_while_request_in_flight(sig: &str, code: i32) {
    let mut server = Server::spawn();
    let stream = server.hold_request_open();

    server.signal(sig);
    server.wait_for_line("finishing in-flight requests");
    assert!(
        server.child.try_wait().unwrap().is_none(),
        "server must keep running while the PUT is in flight"
    );

    server.signal(sig);
    server.wait_for_line("exiting immediately");
    let status = server.wait_exit();
    assert_eq!(status.code(), Some(code), "unexpected status {status:?}");
    drop(stream);
}

#[test]
fn second_sigterm_exits_while_request_in_flight() {
    second_signal_exits_while_request_in_flight("-TERM", 143);
}

#[test]
fn second_sigint_exits_while_request_in_flight() {
    second_signal_exits_while_request_in_flight("-INT", 130);
}
