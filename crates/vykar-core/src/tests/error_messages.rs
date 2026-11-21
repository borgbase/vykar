use tempfile::TempDir;

use crate::commands;
use crate::compress::Compression;

use super::helpers::{make_test_config, source_entry};

/// A REST listener that answers every request with 503; counts requests.
fn unavailable_server() -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::io::{BufRead, BufReader, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = requests.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line.trim().is_empty() {
                    break;
                }
            }
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = stream.write_all(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    (url, requests)
}

/// A config for `url` whose retries wait `delay_ms` each.
fn unavailable_config(url: &str, max_retries: usize, delay_ms: u64) -> crate::config::VykarConfig {
    let dir = TempDir::new().unwrap();
    let mut config = make_test_config(dir.path());
    config.repository.url = url.to_string();
    config.repository.allow_insecure_http = true;
    config.repository.retry = crate::config::RetryConfig {
        max_retries,
        retry_delay_ms: delay_ms,
        retry_max_delay_ms: delay_ms,
    };
    config
}

/// Wait until `requests` reaches 1, so the open is in its retry wait.
fn wait_first_request(requests: &std::sync::atomic::AtomicUsize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while requests.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "no request reached the server"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// `open_repo` hands its token to the backend: a cancel during a 60 s retry
/// wait ends the open as `Interrupted` at once.
#[test]
fn open_repo_cancel_during_retry_wait_is_interrupted() {
    let (url, requests) = unavailable_server();
    let config = unavailable_config(&url, 5, 60_000);
    let token = crate::repo::control::CancelToken::new();
    std::thread::scope(|s| {
        let open = s.spawn(|| {
            commands::util::open_repo(&config, None, crate::repo::OpenOptions::new(), &token)
        });
        wait_first_request(&requests);
        let cancelled_at = std::time::Instant::now();
        token.cancel();
        let err = match open.join().unwrap() {
            Err(e) => e,
            Ok(_) => panic!("open against a 503 server succeeded"),
        };
        let latency = cancelled_at.elapsed();
        assert!(
            matches!(err, vykar_types::error::VykarError::Interrupted),
            "{err}"
        );
        assert!(latency < std::time::Duration::from_secs(5), "{latency:?}");
    });
    assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// `check --repair` in Apply mode must not be cut short past its mutation
/// boundary, so its backend does not observe the caller's token: a cancel
/// during the open's retry wait leaves the wait and the retry to run. In
/// PlanOnly mode the same cancel interrupts the open.
#[test]
fn repair_apply_backend_ignores_the_callers_cancel() {
    use crate::commands::check::RepairMode;
    use vykar_types::error::VykarError;

    for (mode, expect_interrupted) in [(RepairMode::Apply, false), (RepairMode::PlanOnly, true)] {
        let (url, requests) = unavailable_server();
        let config = unavailable_config(&url, 1, 500);
        let token = crate::repo::control::CancelToken::new();
        let err = std::thread::scope(|s| {
            let repair = s.spawn(|| {
                commands::check::run_with_repair(&config, None, false, mode, None, &token)
            });
            wait_first_request(&requests);
            token.cancel();
            match repair.join().unwrap() {
                Err(e) => e,
                Ok(_) => panic!("repair against a 503 server succeeded"),
            }
        });
        let interrupted = matches!(err, VykarError::Interrupted);
        assert_eq!(interrupted, expect_interrupted, "{mode:?}: {err}");
        if !expect_interrupted {
            // The first attempt and its retry, the wait not cut short.
            assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 2);
        }
    }
}

#[test]
fn open_repo_on_empty_storage_includes_url() {
    let repo_dir = TempDir::new().unwrap();
    let config = make_test_config(repo_dir.path());
    let result = commands::util::open_repo(
        &config,
        None,
        crate::repo::OpenOptions::new(),
        &crate::repo::control::CancelToken::new(),
    );
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected RepoNotFound error"),
    };
    let msg = err.to_string();
    assert!(
        msg.contains(&config.repository.url),
        "error should contain repo URL '{url}', got: {msg}",
        url = config.repository.url,
    );
}

#[test]
fn backup_on_uninitialized_repo_includes_url() {
    let repo_dir = TempDir::new().unwrap();
    let source_dir = TempDir::new().unwrap();

    // Create at least one file so backup doesn't bail with "no source paths".
    std::fs::write(source_dir.path().join("file.txt"), b"hello").unwrap();

    let mut config = make_test_config(repo_dir.path());
    let entry = source_entry(source_dir.path(), "test");
    config.exclude_patterns = entry.exclude.clone();

    let source_paths: Vec<String> = entry.paths.clone();
    let result = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: &entry.label,
            exclude_patterns: &config.exclude_patterns,
            exclude_if_present: &config.exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            command_dumps: &[],
            verbose: false,
        },
    );
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected RepoNotFound error"),
    };

    let msg = err.to_string();
    assert!(
        msg.contains(&config.repository.url),
        "error should contain repo URL '{url}', got: {msg}",
        url = config.repository.url,
    );
}
