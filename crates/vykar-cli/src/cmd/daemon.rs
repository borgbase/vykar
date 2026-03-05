use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use vykar_core::app::passphrase::configured_passphrase;
use vykar_core::app::scheduler;
use vykar_core::config::{EncryptionModeConfig, ResolvedRepo, ScheduleConfig};

use crate::dispatch::{run_default_actions, warn_if_untrusted_rest};
use crate::signal::SHUTDOWN;

pub(crate) fn run_daemon(
    repos: &[&ResolvedRepo],
    schedule: &ScheduleConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if !schedule.enabled {
        return Err(
            "schedule.enabled is false; set it to true in your config to use daemon mode".into(),
        );
    }

    // Pre-validate passphrases for encrypted repos
    for repo in repos {
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

    for repo in repos {
        let name = repo.label.as_deref().unwrap_or(&repo.config.repository.url);
        tracing::info!(repo = name, "repository registered");
    }

    // Compute first run time
    let mut next_run = if schedule.on_startup {
        Instant::now()
    } else {
        let delay = scheduler::next_run_delay(schedule)?;
        log_next_run(delay);
        Instant::now() + delay
    };

    loop {
        if SHUTDOWN.load(Ordering::SeqCst) {
            tracing::info!("shutdown signal received, exiting");
            return Ok(());
        }

        if Instant::now() >= next_run {
            run_backup_cycle(repos);

            if SHUTDOWN.load(Ordering::SeqCst) {
                tracing::info!("shutdown signal received, exiting");
                return Ok(());
            }

            // Schedule next run
            let delay = scheduler::next_run_delay(schedule)?;
            next_run = Instant::now() + delay;
            log_next_run(delay);
        }

        std::thread::sleep(Duration::from_secs(1));
    }
}

fn run_backup_cycle(repos: &[&ResolvedRepo]) {
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
            eprintln!("--- Repository: {name} ---");
        }

        let label = repo.label.as_deref();
        let cfg = &repo.config;
        warn_if_untrusted_rest(cfg, label);

        match run_default_actions(
            cfg,
            label,
            &repo.sources,
            &repo.global_hooks,
            &repo.repo_hooks,
            &repo.label,
            Some(&SHUTDOWN),
            0,
        ) {
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
