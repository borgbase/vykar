// glibc malloc_trim to release arena pages between cycles; SAFETY documented per block.
#![allow(unsafe_code)]

mod http;
mod poll;
mod render;
mod status;
mod worker;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant, SystemTime};

use vykar_core::app::passphrase::configured_passphrase;
use vykar_core::app::preflight::UnavailableRepo;
use vykar_core::app::scheduler::{SchedulePlan, SchedulerLock};
use vykar_core::config::{self, ConfigSource, EncryptionModeConfig, ResolvedRepo, ScheduleConfig};
use vykar_types::error::VykarError;

use crate::dispatch::{run_default_actions, warn_if_untrusted_rest};
use crate::error::{CliError, CliResult};
use crate::signal::{RELOAD, SHUTDOWN, TRIGGER};

use status::SharedStatus;

/// Ask the system allocator to return freed memory to the OS.
///
/// After a backup cycle the daemon has freed hundreds of megabytes of
/// HashMap entries (chunk index, dedup structures) but glibc retains them
/// in arena free lists. `malloc_trim(0)` tells glibc to release those
/// pages via madvise/munmap.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(super) fn release_malloc_arenas() {
    unsafe extern "C" {
        fn malloc_trim(pad: libc::size_t) -> libc::c_int;
    }
    // SAFETY: malloc_trim is safe to call at any time and is thread-safe.
    if unsafe { malloc_trim(0) } != 0 {
        tracing::debug!("malloc_trim: released memory to OS");
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(super) fn release_malloc_arenas() {}

/// Borrow every repository's effective schedule, index-aligned with `repos`.
fn repo_schedules(repos: &[ResolvedRepo]) -> Vec<&ScheduleConfig> {
    repos.iter().map(|r| &r.config.schedule).collect()
}

/// Report per-repository scheduling failures. A repo whose cadence cannot be
/// computed is dropped from the timer, not fatal — the rest keep running.
fn log_plan_errors(repos: &[ResolvedRepo], errors: &[(usize, VykarError)]) {
    for (idx, e) in errors {
        let name = repos
            .get(*idx)
            .map(ResolvedRepo::label_or_url)
            .unwrap_or("?");
        tracing::warn!(
            repo = name,
            error = %e,
            "cannot compute next run; this repository will not be scheduled"
        );
    }
}

/// Name the repositories that `on_startup` makes due immediately. Without this
/// the log jumps from "daemon starting" to the cycle output with nothing in
/// between — `log_earliest_next_run` stays silent while the earliest slot is
/// already due.
fn log_startup_runs(repos: &[ResolvedRepo], plan: &SchedulePlan) {
    let due: Vec<&str> = plan
        .due(SystemTime::now())
        .iter()
        .filter_map(|&idx| repos.get(idx))
        .map(ResolvedRepo::label_or_url)
        .collect();
    if !due.is_empty() {
        tracing::info!(
            repos = due.join(", "),
            "on_startup set; backing up immediately"
        );
    }
}

/// Log the earliest upcoming run across all repositories. Silent when nothing
/// is scheduled or the earliest slot is already due (it is about to run —
/// `log_startup_runs` covers that case at startup).
fn log_earliest_next_run(plan: &SchedulePlan) {
    if let Some(delay) = plan
        .next_wake()
        .and_then(|w| w.duration_since(SystemTime::now()).ok())
    {
        log_next_run(delay);
    }
}

/// Pair each repository name with its next scheduled run for the status page.
fn repo_next_runs(
    repos: &[ResolvedRepo],
    plan: &SchedulePlan,
) -> Vec<(String, Option<SystemTime>)> {
    repos
        .iter()
        .enumerate()
        .map(|(idx, repo)| (repo.label_or_url().to_string(), plan.next_run(idx)))
        .collect()
}

/// Load and validate daemon config from the given source.
/// Returns the resolved repos, or an error describing what went wrong
/// (suitable for both fatal startup errors and non-fatal reload rejections).
fn load_daemon_config(source: &ConfigSource) -> CliResult<Vec<ResolvedRepo>> {
    let repos = config::load_and_resolve(source.path())?;

    if repos.is_empty() {
        return Err(CliError::from("no repositories configured"));
    }

    if !repos.iter().any(|r| r.config.schedule.enabled) {
        return Err(CliError::from(
            "schedule.enabled is false for all repositories; set it to true in your config \
             (globally or on a repository) to use daemon mode",
        ));
    }

    // Pre-validate passphrases for every repo, including those whose schedule is
    // disabled — they still take part in SIGUSR1 cycles and the status page.
    for repo in &repos {
        let label = repo.label_or_url();
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

    Ok(repos)
}

pub(crate) fn run_daemon(source: ConfigSource, http_listen: Option<SocketAddr>) -> CliResult<()> {
    let _lock = SchedulerLock::try_acquire().ok_or_else(|| {
        CliError::from("another vykar scheduler is already running (daemon or GUI); exiting")
    })?;

    let mut repos = load_daemon_config(&source)?;

    let started_at = Instant::now();
    let status = status::new_shared();
    status::init(&status, &repos, started_at);

    let http_handle = if let Some(addr) = http_listen {
        Some(http::spawn(addr, status.clone(), &SHUTDOWN)?)
    } else {
        None
    };

    tracing::info!(repos = repos.len(), "daemon starting");
    log_registered_repos(&repos);

    // Per-repo next run times. Wall-clock so targets survive system sleep and
    // monotonic-clock freezes (see GitHub #110).
    let (mut plan, plan_errors) = SchedulePlan::new(&repo_schedules(&repos), true);
    log_plan_errors(&repos, &plan_errors);
    log_startup_runs(&repos, &plan);
    log_earliest_next_run(&plan);

    // Status I/O runs on its own thread (GitHub #206). When a cycle is due at
    // startup, hold the worker off until it ends so hook-skipped repos are
    // never contacted.
    let cycle_running = Arc::new(AtomicBool::new(!plan.due(SystemTime::now()).is_empty()));
    let status_tx = worker::spawn(status.clone(), cycle_running.clone(), &SHUTDOWN)
        .map_err(|e| CliError::from(format!("failed to spawn status worker: {e}")))?;
    send_configure(&status_tx, &repos);

    status::touch_process(&status, started_at, plan.next_wake());
    status::set_repo_next_runs(&status, &repo_next_runs(&repos, &plan));

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
                Ok(new_repos) => {
                    tracing::info!(
                        repos = new_repos.len(),
                        "configuration reloaded successfully"
                    );
                    repos = new_repos;
                    crate::passphrase::clear_passphrase_cache();
                    status::init(&status, &repos, started_at);
                    send_configure(&status_tx, &repos);

                    log_registered_repos(&repos);

                    // Recalculate next runs from the new schedules (ignore on_startup).
                    let (new_plan, errors) = SchedulePlan::new(&repo_schedules(&repos), false);
                    plan = new_plan;
                    log_plan_errors(&repos, &errors);
                    log_earliest_next_run(&plan);
                    status::set_repo_next_runs(&status, &repo_next_runs(&repos, &plan));
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
            // An ad-hoc trigger runs every repository regardless of cadence.
            let all: Vec<&ResolvedRepo> = repos.iter().collect();
            run_cycle(&all, &repos, &status, &status_tx, &cycle_running);

            if SHUTDOWN.load(Ordering::SeqCst) {
                tracing::info!("shutdown signal received, exiting");
                break Ok(());
            }

            // Only slots missed during the ad-hoc cycle are recalculated;
            // repos whose slot is still ahead keep their configured cadence.
            let now = SystemTime::now();
            if !plan.due(now).is_empty() {
                let errors = plan.reschedule_overdue(&repo_schedules(&repos), now);
                log_plan_errors(&repos, &errors);
                log_earliest_next_run(&plan);
            }
        }

        let due = plan.due(SystemTime::now());
        if !due.is_empty() {
            let due_repos: Vec<&ResolvedRepo> =
                due.iter().filter_map(|&idx| repos.get(idx)).collect();
            run_cycle(&due_repos, &repos, &status, &status_tx, &cycle_running);

            if SHUTDOWN.load(Ordering::SeqCst) {
                tracing::info!("shutdown signal received, exiting");
                break Ok(());
            }

            // Schedule the next run of the repos that just ran.
            let schedules = repo_schedules(&repos);
            for idx in due {
                let Some(schedule) = schedules.get(idx) else {
                    continue;
                };
                if let Some(e) = plan.reschedule(idx, schedule) {
                    log_plan_errors(&repos, &[(idx, e)]);
                }
            }
            log_earliest_next_run(&plan);
        }

        status::touch_process(&status, started_at, plan.next_wake());
        status::set_repo_next_runs(&status, &repo_next_runs(&repos, &plan));

        std::thread::sleep(Duration::from_secs(1));
    };

    // The status worker is not joined: it may be inside a connect timeout, and
    // it exits on its own once it sees SHUTDOWN.
    if let Some(handle) = http_handle {
        // SHUTDOWN is set; the HTTP loop polls it within POLL_INTERVAL.
        if let Err(e) = handle.join() {
            tracing::warn!(?e, "http thread panicked");
        }
    }

    exit_result
}

/// Log one line per configured repository, including its effective cadence.
fn log_registered_repos(repos: &[ResolvedRepo]) {
    for repo in repos {
        let s = &repo.config.schedule;
        tracing::info!(
            repo = repo.label_or_url(),
            enabled = s.enabled,
            cadence = s.cron.as_deref().or(s.every.as_deref()).unwrap_or("24h"),
            on_startup = s.on_startup,
            jitter_seconds = s.jitter_seconds,
            "repository registered"
        );
    }
}

fn send_configure(tx: &Sender<worker::Msg>, repos: &[ResolvedRepo]) {
    if tx
        .send(worker::Msg::Configure(Arc::new(repos.to_vec())))
        .is_err()
    {
        tracing::warn!("status worker is gone; status page will not update");
    }
}

/// Run a cycle with status I/O held off, then tell the worker which repos ran
/// and which were skipped. The message goes first so the worker applies the
/// skip marks before it is allowed to do I/O again.
fn run_cycle(
    due: &[&ResolvedRepo],
    all: &[ResolvedRepo],
    status: &SharedStatus,
    status_tx: &Sender<worker::Msg>,
    cycle_running: &AtomicBool,
) {
    cycle_running.store(true, Ordering::SeqCst);
    let report = run_backup_cycle(due, all, status);
    let _ = status_tx.send(worker::Msg::CycleDone {
        ran: report.ran,
        skipped: report.skipped,
    });
    cycle_running.store(false, Ordering::SeqCst);
}

/// Repositories a cycle reached, by name. Repos not reached because of
/// `SHUTDOWN` are in neither list.
struct CycleReport {
    ran: Vec<String>,
    skipped: Vec<String>,
}

/// Run a backup cycle for the `due` repositories.
///
/// `all` is the full configured set: it decides whether the
/// `=== Repository ===` headers print, which must not switch off just because
/// a single repo came due.
fn run_backup_cycle(
    due: &[&ResolvedRepo],
    all: &[ResolvedRepo],
    status: &SharedStatus,
) -> CycleReport {
    tracing::info!("backup cycle starting");
    status::record_cycle_start(status);
    let cycle_start = Instant::now();
    let mut had_error = false;
    let mut had_partial = false;
    let mut attempted = 0usize;
    let mut skipped = 0usize;
    let mut report = CycleReport {
        ran: Vec::new(),
        skipped: Vec::new(),
    };

    let multi = all.len() > 1;

    for repo in due {
        if SHUTDOWN.load(Ordering::SeqCst) {
            tracing::info!("shutdown signal received, skipping remaining repos");
            break;
        }

        let name = repo.label_or_url();

        if multi {
            eprintln!("=== Repository: {name} ===");
            if repo.label.is_some() {
                eprintln!("url: {}", repo.config.repository.url);
            }
        }

        warn_if_untrusted_rest(&repo.config, repo.label.as_deref());

        attempted += 1;
        match run_default_actions(
            repo,
            crate::signal::shutdown_token(),
            0,
            &[],
            UnavailableRepo::Skip,
        ) {
            Ok(outcome) => {
                if outcome.partial {
                    tracing::warn!(repo = name, "backup cycle partial: some files were skipped");
                    had_partial = true;
                }
                if let Some(reason) = outcome.skipped {
                    if outcome.unavailable {
                        tracing::warn!(repo = name, reason, "skipping unavailable repository");
                    } else {
                        tracing::info!(repo = name, reason, "backup cycle skipped");
                    }
                    skipped += 1;
                    report.skipped.push(name.to_string());
                } else {
                    report.ran.push(name.to_string());
                }
            }
            Err(e) => {
                tracing::error!(repo = name, error = %e, "backup cycle failed for repo");
                had_error = true;
                report.ran.push(name.to_string());
            }
        }
    }

    let elapsed = cycle_start.elapsed();
    let all_skipped = attempted > 0 && skipped == attempted;
    if had_error {
        tracing::warn!(duration = ?elapsed, "backup cycle finished with errors");
    } else if had_partial {
        tracing::warn!(duration = ?elapsed, "backup cycle finished with partial success (some files skipped)");
    } else if all_skipped {
        tracing::info!(duration = ?elapsed, "backup cycle finished: every repository was skipped");
    } else {
        tracing::info!(duration = ?elapsed, "backup cycle finished successfully");
    }

    status::record_cycle_end(status, elapsed, had_error, had_partial, all_skipped);

    // All Repository instances are dropped. Ask glibc to return freed pages.
    if !SHUTDOWN.load(Ordering::SeqCst) {
        release_malloc_arenas();
    }
    report
}

fn log_next_run(delay: Duration) {
    let next_wall = chrono::Local::now() + delay;
    tracing::info!(
        next_run = %next_wall.format("%Y-%m-%d %H:%M:%S %Z"),
        delay = ?delay,
        "next backup scheduled"
    );
}
