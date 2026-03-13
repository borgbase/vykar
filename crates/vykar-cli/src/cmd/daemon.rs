use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use vykar_core::app::passphrase::configured_passphrase;
use vykar_core::app::scheduler::{self, SchedulerLock};
use vykar_core::app::RuntimeConfig;
use vykar_core::config::{self, ConfigSource, EncryptionModeConfig, ResolvedRepo, ScheduleConfig};

use crate::dispatch::{run_default_actions, warn_if_untrusted_rest};
use crate::signal::{RELOAD, SHUTDOWN};

/// Load and validate daemon config from the given source.
/// Returns the resolved repos and merged schedule, or an error describing
/// what went wrong (suitable for both fatal startup errors and non-fatal
/// reload rejections).
fn load_daemon_config(
    source: &ConfigSource,
) -> Result<(Vec<ResolvedRepo>, ScheduleConfig), Box<dyn std::error::Error>> {
    let repos = config::load_and_resolve(source.path())?;

    if repos.is_empty() {
        return Err("no repositories configured".into());
    }

    let runtime = RuntimeConfig {
        source: source.clone(),
        repos,
    };
    let schedule = runtime.schedule();

    if !schedule.enabled {
        return Err(
            "schedule.enabled is false; set it to true in your config to use daemon mode".into(),
        );
    }

    // Pre-validate passphrases for encrypted repos
    for repo in &runtime.repos {
        let label = repo.label.as_deref().unwrap_or(&repo.config.repository.url);
        if repo.config.encryption.mode != EncryptionModeConfig::None {
            match configured_passphrase(&repo.config) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Err(format!(
                        "encrypted repository '{label}' has no non-interactive passphrase source; \
                         configure encryption.passcommand, encryption.passphrase, or set VYKAR_PASSPHRASE"
                    ).into());
                }
                Err(e) => {
                    return Err(format!("failed to validate passphrase for '{label}': {e}").into());
                }
            }
        }
    }

    Ok((runtime.repos, schedule))
}

pub(crate) fn run_daemon(source: ConfigSource) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = SchedulerLock::try_acquire()
        .ok_or("another vykar scheduler is already running (daemon or GUI); exiting")?;

    let (mut repos, mut schedule) = load_daemon_config(&source)?;

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

    // Compute first run time
    let mut next_run = if schedule.on_startup {
        Instant::now()
    } else {
        let delay = scheduler::next_run_delay(&schedule)?;
        log_next_run(delay);
        Instant::now() + delay
    };

    loop {
        if SHUTDOWN.load(Ordering::SeqCst) {
            tracing::info!("shutdown signal received, exiting");
            return Ok(());
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

                    for repo in &repos {
                        let name = repo.label.as_deref().unwrap_or(&repo.config.repository.url);
                        tracing::info!(repo = name, "repository registered");
                    }

                    // Recalculate next_run from schedule (ignore on_startup)
                    match scheduler::next_run_delay(&schedule) {
                        Ok(delay) => {
                            next_run = Instant::now() + delay;
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

        if Instant::now() >= next_run {
            run_backup_cycle(&repos);

            if SHUTDOWN.load(Ordering::SeqCst) {
                tracing::info!("shutdown signal received, exiting");
                return Ok(());
            }

            // Schedule next run
            let delay = scheduler::next_run_delay(&schedule)?;
            next_run = Instant::now() + delay;
            log_next_run(delay);
        }

        std::thread::sleep(Duration::from_secs(1));
    }
}

fn run_backup_cycle(repos: &[ResolvedRepo]) {
    tracing::info!("backup cycle starting");
    let cycle_start = Instant::now();
    let mut had_error = false;
    let mut had_partial = false;

    for repo in repos {
        if SHUTDOWN.load(Ordering::SeqCst) {
            tracing::info!("shutdown signal received, skipping remaining repos");
            break;
        }

        let name = repo.label.as_deref().unwrap_or(&repo.config.repository.url);
        let multi = repos.len() > 1;
        if multi {
            eprintln!("=== Repository: {name} ===");
            if repo.label.is_some() {
                eprintln!("url: {}", repo.config.repository.url);
            }
        }

        warn_if_untrusted_rest(&repo.config, repo.label.as_deref());

        match run_default_actions(repo, Some(&SHUTDOWN), 0) {
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
}

fn log_next_run(delay: Duration) {
    let next_wall = chrono::Local::now() + delay;
    tracing::info!(
        next_run = %next_wall.format("%Y-%m-%d %H:%M:%S %Z"),
        delay = ?delay,
        "next backup scheduled"
    );
}
