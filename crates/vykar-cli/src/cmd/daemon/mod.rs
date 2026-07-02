// glibc malloc_trim to release arena pages between cycles; SAFETY documented per block.
#![allow(unsafe_code)]

mod http;
mod poll;
mod render;
mod status;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};

use vykar_core::app::passphrase::configured_passphrase;
use vykar_core::app::scheduler::{self, SchedulerLock};
use vykar_core::app::RuntimeConfig;
use vykar_core::config::{self, ConfigSource, EncryptionModeConfig, ResolvedRepo, ScheduleConfig};

use crate::dispatch::{local_repo_unavailable, run_default_actions, warn_if_untrusted_rest};
use crate::error::{CliError, CliResult};
use crate::signal::{RELOAD, SHUTDOWN, TRIGGER};

use poll::StatusPoller;
use status::SharedStatus;

/// How often the daemon runs the cheap snapshot-set change detection poll
/// between backup cycles (GitHub #159).
const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Ask the system allocator to return freed memory to the OS.
///
/// After a backup cycle the daemon has freed hundreds of megabytes of
/// HashMap entries (chunk index, dedup structures) but glibc retains them
/// in arena free lists. `malloc_trim(0)` tells glibc to release those
/// pages via madvise/munmap.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn release_malloc_arenas() {
    extern "C" {
        fn malloc_trim(pad: libc::size_t) -> libc::c_int;
    }
    // SAFETY: malloc_trim is safe to call at any time and is thread-safe.
    if unsafe { malloc_trim(0) } != 0 {
        tracing::debug!("malloc_trim: released memory to OS");
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn release_malloc_arenas() {}

/// Load and validate daemon config from the given source.
/// Returns the resolved repos and merged schedule, or an error describing
/// what went wrong (suitable for both fatal startup errors and non-fatal
/// reload rejections).
fn load_daemon_config(source: &ConfigSource) -> CliResult<(Vec<ResolvedRepo>, ScheduleConfig)> {
    let repos = config::load_and_resolve(source.path())?;

    if repos.is_empty() {
        return Err(CliError::from("no repositories configured"));
    }

    let runtime = RuntimeConfig {
        source: source.clone(),
        repos,
    };
    let schedule = runtime.schedule();

    if !schedule.enabled {
        return Err(CliError::from(
            "schedule.enabled is false; set it to true in your config to use daemon mode",
        ));
    }

    // Pre-validate passphrases for encrypted repos
    for repo in &runtime.repos {
        let label = repo.label.as_deref().unwrap_or(&repo.config.repository.url);
        if repo.config.encryption.mode != EncryptionModeConfig::None {
            match configured_passphrase(&repo.config) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Err(CliError::from(format!(
                        "encrypted repository '{label}' has no non-interactive passphrase source; \
                         configure encryption.passcommand, encryption.passphrase, or set VYKAR_PASSPHRASE"
                    )));
                }
                Err(e) => {
                    return Err(CliError::from(format!(
                        "failed to validate passphrase for '{label}': {e}"
                    )));
                }
            }
        }
    }

    Ok((runtime.repos, schedule))
}

pub(crate) fn run_daemon(source: ConfigSource, http_listen: Option<SocketAddr>) -> CliResult<()> {
    let _lock = SchedulerLock::try_acquire().ok_or_else(|| {
        CliError::from("another vykar scheduler is already running (daemon or GUI); exiting")
    })?;

    let (mut repos, mut schedule) = load_daemon_config(&source)?;

    let started_at = Instant::now();
    let status = status::new_shared();
    status::init(&status, &repos, &schedule, started_at);
    status::refresh_repos(&status, &repos);

    let http_handle = if let Some(addr) = http_listen {
        Some(http::spawn(addr, status.clone(), &SHUTDOWN)?)
    } else {
        None
    };

    if schedule.is_cron() {
        tracing::info!(
            repos = repos.len(),
            cron = schedule.cron.as_deref().unwrap_or(""),
            on_startup = schedule.on_startup,
            jitter_seconds = schedule.jitter_seconds,
            "daemon starting (cron mode)"
        );
    } else {
        let interval = schedule.every_duration()?;
        tracing::info!(
            repos = repos.len(),
            interval = ?interval,
            on_startup = schedule.on_startup,
            jitter_seconds = schedule.jitter_seconds,
            "daemon starting (interval mode)"
        );
    }

    for repo in &repos {
        let name = repo.label.as_deref().unwrap_or(&repo.config.repository.url);
        tracing::info!(repo = name, "repository registered");
    }

    // Compute first run time. Wall-clock so the target survives system sleep
    // and monotonic-clock freezes (see GitHub #110).
    let mut next_run = if schedule.on_startup {
        SystemTime::now()
    } else {
        let delay = scheduler::next_run_delay(&schedule)?;
        log_next_run(delay);
        SystemTime::now() + delay
    };

    status::touch_process(&status, started_at, Some(next_run));

    // Cheap out-of-band change detection between cycles (GitHub #159). Seed the
    // baseline from current storage so the first poll only fires a refresh on a
    // genuine change.
    let mut poller = StatusPoller::new();
    poller.reset(&repos);
    let mut next_poll = Instant::now() + STATUS_POLL_INTERVAL;

    let exit_result: CliResult<()> = loop {
        if SHUTDOWN.load(Ordering::SeqCst) {
            tracing::info!("shutdown signal received, exiting");
            break Ok(());
        }

        // Check for SIGHUP reload between cycles
        if RELOAD.load(Ordering::SeqCst) {
            RELOAD.store(false, Ordering::SeqCst);
            tracing::info!("SIGHUP received, reloading configuration");

            match load_daemon_config(&source) {
                Ok((new_repos, new_schedule)) => {
                    tracing::info!(
                        repos = new_repos.len(),
                        "configuration reloaded successfully"
                    );
                    repos = new_repos;
                    schedule = new_schedule;
                    status::init(&status, &repos, &schedule, started_at);
                    status::refresh_repos(&status, &repos);
                    poller.reset(&repos);
                    next_poll = Instant::now() + STATUS_POLL_INTERVAL;

                    for repo in &repos {
                        let name = repo.label.as_deref().unwrap_or(&repo.config.repository.url);
                        tracing::info!(repo = name, "repository registered");
                    }

                    // Recalculate next_run from schedule (ignore on_startup)
                    match scheduler::next_run_delay(&schedule) {
                        Ok(delay) => {
                            next_run = SystemTime::now() + delay;
                            log_next_run(delay);
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "failed to compute next run delay after reload, keeping previous schedule"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "configuration reload rejected, continuing with previous config"
                    );
                }
            }
        }

        // Check for SIGUSR1 ad-hoc trigger
        if TRIGGER.load(Ordering::SeqCst) {
            TRIGGER.store(false, Ordering::SeqCst);
            tracing::info!("SIGUSR1 received, triggering immediate backup");
            run_backup_cycle(&repos, &status);

            if SHUTDOWN.load(Ordering::SeqCst) {
                tracing::info!("shutdown signal received, exiting");
                break Ok(());
            }

            // The cycle already ran refresh_repos; re-baseline the poller.
            poller.reset(&repos);
            next_poll = Instant::now() + STATUS_POLL_INTERVAL;

            // If the scheduled slot was missed during the ad-hoc cycle, recalculate
            // next_run from now. Otherwise leave next_run untouched — the scheduled
            // cadence is preserved.
            if next_run.duration_since(SystemTime::now()).is_err() {
                let delay = match scheduler::next_run_delay(&schedule) {
                    Ok(d) => d,
                    Err(e) => break Err(e.into()),
                };
                next_run = SystemTime::now() + delay;
                log_next_run(delay);
            }
        }

        if next_run.duration_since(SystemTime::now()).is_err() {
            run_backup_cycle(&repos, &status);

            if SHUTDOWN.load(Ordering::SeqCst) {
                tracing::info!("shutdown signal received, exiting");
                break Ok(());
            }

            // Schedule next run
            let delay = match scheduler::next_run_delay(&schedule) {
                Ok(d) => d,
                Err(e) => break Err(e.into()),
            };
            next_run = SystemTime::now() + delay;
            log_next_run(delay);

            // The cycle already ran refresh_repos; re-baseline the poller.
            poller.reset(&repos);
            next_poll = Instant::now() + STATUS_POLL_INTERVAL;
        }

        // Between cycles, cheaply detect out-of-band snapshot changes (CLI
        // delete/prune, backups from other hosts) and run the full status
        // refresh only when the snapshot set actually changed (GitHub #159).
        if Instant::now() >= next_poll {
            poller.poll_and_refresh(&status, &repos);
            next_poll = Instant::now() + STATUS_POLL_INTERVAL;
        }

        status::touch_process(&status, started_at, Some(next_run));

        std::thread::sleep(Duration::from_secs(1));
    };

    if let Some(handle) = http_handle {
        // SHUTDOWN is set; the HTTP loop polls it within POLL_INTERVAL.
        if let Err(e) = handle.join() {
            tracing::warn!(?e, "http thread panicked");
        }
    }

    exit_result
}

fn run_backup_cycle(repos: &[ResolvedRepo], status: &SharedStatus) {
    tracing::info!("backup cycle starting");
    status::record_cycle_start(status);
    let cycle_start = Instant::now();
    let mut had_error = false;
    let mut had_partial = false;

    let multi = repos.len() > 1;

    for repo in repos {
        if SHUTDOWN.load(Ordering::SeqCst) {
            tracing::info!("shutdown signal received, skipping remaining repos");
            break;
        }

        let name = repo.label.as_deref().unwrap_or(&repo.config.repository.url);

        // Pre-flight: skip unavailable local repos in multi-repo configs
        if multi {
            if let Some(path) = local_repo_unavailable(repo) {
                tracing::info!(repo = name, path, "skipping unavailable repository");
                continue;
            }
        }

        if multi {
            eprintln!("=== Repository: {name} ===");
            if repo.label.is_some() {
                eprintln!("url: {}", repo.config.repository.url);
            }
        }

        warn_if_untrusted_rest(&repo.config, repo.label.as_deref());

        match run_default_actions(repo, Some(&SHUTDOWN), 0, &[]) {
            Ok(partial) => {
                if partial {
                    tracing::warn!(repo = name, "backup cycle partial: some files were skipped");
                    had_partial = true;
                }
            }
            Err(e) => {
                tracing::error!(repo = name, error = %e, "backup cycle failed for repo");
                had_error = true;
            }
        }
    }

    let elapsed = cycle_start.elapsed();
    if had_error {
        tracing::warn!(duration = ?elapsed, "backup cycle finished with errors");
    } else if had_partial {
        tracing::warn!(duration = ?elapsed, "backup cycle finished with partial success (some files skipped)");
    } else {
        tracing::info!(duration = ?elapsed, "backup cycle finished successfully");
    }

    status::record_cycle_end(status, elapsed, had_error, had_partial);
    if !SHUTDOWN.load(Ordering::SeqCst) {
        status::refresh_repos(status, repos);
    }

    // All Repository instances are dropped. Ask glibc to return freed pages.
    if !SHUTDOWN.load(Ordering::SeqCst) {
        release_malloc_arenas();
    }
}

fn log_next_run(delay: Duration) {
    let next_wall = chrono::Local::now() + delay;
    tracing::info!(
        next_run = %next_wall.format("%Y-%m-%d %H:%M:%S %Z"),
        delay = ?delay,
        "next backup scheduled"
    );
}
