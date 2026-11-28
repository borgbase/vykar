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

use vykar_core::app::AppService;
use vykar_core::app::passphrase::{NoPrompt, has_configured_source};
use vykar_core::app::scheduler::{Scheduler, SchedulerLock};
use vykar_core::app::service::{AppEvent, CycleReport, CycleRequest, RepoSelection};
use vykar_core::config::{self, ConfigSource, ResolvedRepo, ScheduleConfig};
use vykar_core::repo::control::FnMutSink;
use vykar_types::error::VykarError;
use zeroize::Zeroizing;

use crate::cycle::CycleRenderer;
use crate::dispatch::warn_if_untrusted_rest;
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

/// Every repository's effective schedule, index-aligned with `repos`.
fn repo_schedules(repos: &[ResolvedRepo]) -> Vec<ScheduleConfig> {
    repos.iter().map(|r| r.config.schedule.clone()).collect()
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
fn log_startup_runs(repos: &[ResolvedRepo], scheduler: &Scheduler) {
    let due: Vec<&str> = scheduler
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
fn log_earliest_next_run(scheduler: &Scheduler) {
    if let Some(delay) = scheduler
        .next_wake()
        .and_then(|w| w.duration_since(SystemTime::now()).ok())
    {
        log_next_run(delay);
    }
}

/// Pair each repository name with its next scheduled run for the status page.
fn repo_next_runs(
    repos: &[ResolvedRepo],
    scheduler: &Scheduler,
) -> Vec<(String, Option<SystemTime>)> {
    repos
        .iter()
        .enumerate()
        .map(|(idx, repo)| (repo.label_or_url().to_string(), scheduler.next_run(idx)))
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

    // Every repository, including those whose schedule is disabled (they still
    // take part in SIGUSR1 cycles and the status page), must name a source the
    // daemon can use without prompting. Nothing runs here: a passcommand runs
    // in the cycle, after the before hooks, and a failure is a failed step.
    for repo in &repos {
        if !has_configured_source(&repo.config) {
            let label = repo.label_or_url();
            return Err(CliError::from(format!(
                "repository '{label}' has no non-interactive passphrase source; \
                 configure encryption.passcommand, encryption.passphrase, or set VYKAR_PASSPHRASE"
            )));
        }
    }

    Ok(repos)
}

pub(crate) fn run_daemon(source: ConfigSource, http_listen: Option<SocketAddr>) -> CliResult<()> {
    let _lock = SchedulerLock::try_acquire().ok_or_else(|| {
        CliError::from("another vykar scheduler is already running (daemon or GUI); exiting")
    })?;

    let mut repos = load_daemon_config(&source)?;
    // Caches accepted passphrases across cycles; a reload drops those whose
    // source changed.
    let mut service = AppService::new();

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
    let mut scheduler = Scheduler::default();
    let plan_errors = scheduler.load(repo_schedules(&repos), SystemTime::now(), true);
    log_plan_errors(&repos, &plan_errors);
    log_startup_runs(&repos, &scheduler);
    log_earliest_next_run(&scheduler);

    // Status I/O runs on its own thread (GitHub #206). When a cycle is due at
    // startup, hold the worker off until it ends so hook-skipped repos are
    // never contacted.
    let cycle_running = Arc::new(AtomicBool::new(
        !scheduler.due(SystemTime::now()).is_empty(),
    ));
    let status_tx = worker::spawn(status.clone(), cycle_running.clone(), &SHUTDOWN)
        .map_err(|e| CliError::from(format!("failed to spawn status worker: {e}")))?;
    send_configure(&status_tx, &repos, &service);

    status::touch_process(&status, started_at, scheduler.next_wake());
    status::set_repo_next_runs(&status, &repo_next_runs(&repos, &scheduler));

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
                    service.reload(&repos);
                    status::init(&status, &repos, started_at);
                    send_configure(&status_tx, &repos, &service);

                    log_registered_repos(&repos);

                    // Recalculate next runs from the new schedules (ignore on_startup).
                    let errors = scheduler.load(repo_schedules(&repos), SystemTime::now(), false);
                    log_plan_errors(&repos, &errors);
                    log_earliest_next_run(&scheduler);
                    status::set_repo_next_runs(&status, &repo_next_runs(&repos, &scheduler));
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "configuration reload rejected, continuing with previous config"
                    );
                }
            }
        }

        // SIGUSR1 asks for one ad-hoc cycle over every repository. It runs
        // before any due slot; only the slots it overran are recalculated.
        if TRIGGER.swap(false, Ordering::SeqCst) {
            tracing::info!("SIGUSR1 received, triggering immediate backup");
            scheduler.trigger();
        }

        if let Some(order) = scheduler.poll(SystemTime::now()).run {
            run_cycle(
                &mut service,
                order.selection.clone(),
                &repos,
                &status,
                &status_tx,
                &cycle_running,
            );

            if SHUTDOWN.load(Ordering::SeqCst) {
                tracing::info!("shutdown signal received, exiting");
                break Ok(());
            }

            // Schedule the next run of the repos the cycle moved, from its end.
            let done = scheduler.finished(order.id, SystemTime::now());
            log_plan_errors(&repos, &done.errors);
            if !done.indices.is_empty() {
                log_earliest_next_run(&scheduler);
            }
        }

        status::touch_process(&status, started_at, scheduler.next_wake());
        status::set_repo_next_runs(&status, &repo_next_runs(&repos, &scheduler));

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

/// Hand the worker the repositories and, index-aligned, the passphrase each
/// one accepted under its current source (none at startup; on reload, those
/// `AppService::reload` kept).
fn send_configure(tx: &Sender<worker::Msg>, repos: &[ResolvedRepo], service: &AppService) {
    let passphrases = repos
        .iter()
        .map(|r| service.accepted_passphrase(&r.config))
        .collect();
    if tx
        .send(worker::Msg::Configure {
            repos: Arc::new(repos.to_vec()),
            passphrases,
        })
        .is_err()
    {
        tracing::warn!("status worker is gone; status page will not update");
    }
}

/// Run a cycle with status I/O held off, then tell the worker which repos ran
/// and which were skipped. The message goes first so the worker applies the
/// skip marks before it is allowed to do I/O again.
fn run_cycle(
    service: &mut AppService,
    selection: RepoSelection,
    all: &[ResolvedRepo],
    status: &SharedStatus,
    status_tx: &Sender<worker::Msg>,
    cycle_running: &AtomicBool,
) {
    cycle_running.store(true, Ordering::SeqCst);
    let names = run_backup_cycle(service, selection, all, status);
    let _ = status_tx.send(worker::Msg::CycleDone {
        ran: names.ran,
        skipped: names.skipped,
        passphrases: names.passphrases,
    });
    cycle_running.store(false, Ordering::SeqCst);
}

/// Repositories a cycle reached, by name. Repos not reached because of
/// `SHUTDOWN` are in neither list.
struct CycleNames {
    ran: Vec<String>,
    skipped: Vec<String>,
    /// For each repo in `ran`, the passphrase it accepted, if any.
    passphrases: Vec<(String, Option<Zeroizing<String>>)>,
}

/// Run a backup cycle for the `selection` of `all`, the full configured set:
/// it decides whether the `=== Repo ===` headers print, which must not switch
/// off just because a single repo came due.
fn run_backup_cycle(
    service: &mut AppService,
    selection: RepoSelection,
    all: &[ResolvedRepo],
    status: &SharedStatus,
) -> CycleNames {
    run_backup_cycle_report(service, selection, all, status).0
}

/// [`run_backup_cycle`], also returning the core report (tests).
fn run_backup_cycle_report(
    service: &mut AppService,
    selection: RepoSelection,
    all: &[ResolvedRepo],
    status: &SharedStatus,
) -> (CycleNames, CycleReport) {
    tracing::info!("backup cycle starting");
    status::record_cycle_start(status);
    let cycle_start = Instant::now();

    let multi = all.len() > 1;
    let mut renderer = CycleRenderer::new(0);
    let request = CycleRequest {
        selection,
        ..Default::default()
    };
    let report = service.run_cycle(
        all,
        &request,
        &mut NoPrompt,
        crate::signal::shutdown_token(),
        &FnMutSink::new(|event: AppEvent<'_>| match event {
            AppEvent::RepoStarted { repo, .. } => {
                if multi {
                    eprintln!("=== Repository: {} ===", repo.label_or_url());
                    if repo.label.is_some() {
                        eprintln!("url: {}", repo.config.repository.url);
                    }
                }
                warn_if_untrusted_rest(&repo.config, repo.label.as_deref());
                renderer.repo_started();
            }
            AppEvent::Cycle { event, .. } => renderer.on_cycle_event(&event),
            AppEvent::RepoFinished { repo, report } => {
                renderer.repo_finished(&report.result);
                let name = repo.label_or_url();
                if report.result.has_failures() {
                    tracing::error!(
                        repo = name,
                        error = "one or more steps failed",
                        "backup cycle failed for repo"
                    );
                    return;
                }
                if report.result.had_partial() {
                    tracing::warn!(repo = name, "backup cycle partial: some files were skipped");
                }
                if let Some(reason) = report.skipped() {
                    // A `&str` field, so the log text stays as it was.
                    let reason = reason.message.as_str();
                    if report.result.repo_unavailable() {
                        tracing::warn!(repo = name, reason, "skipping unavailable repository");
                    } else {
                        tracing::info!(repo = name, reason, "backup cycle skipped");
                    }
                }
            }
        }),
    );
    if !report.not_reached.is_empty() {
        tracing::info!("shutdown signal received, skipping remaining repos");
    }

    let mut names = CycleNames {
        ran: Vec::new(),
        skipped: Vec::new(),
        passphrases: Vec::new(),
    };
    for r in &report.repos {
        let name = all
            .get(r.index)
            .map_or("?", ResolvedRepo::label_or_url)
            .to_string();
        if r.skipped().is_some() {
            names.skipped.push(name);
        } else {
            let accepted = all
                .get(r.index)
                .and_then(|repo| service.accepted_passphrase(&repo.config));
            names.passphrases.push((name.clone(), accepted));
            names.ran.push(name);
        }
    }

    let elapsed = cycle_start.elapsed();
    // Partial counts only for repositories that did not fail.
    let status::CycleFlags {
        had_error,
        had_partial,
        all_skipped,
    } = status::cycle_flags(&report);
    if had_error {
        tracing::warn!(duration = ?elapsed, "backup cycle finished with errors");
    } else if had_partial {
        tracing::warn!(duration = ?elapsed, "backup cycle finished with partial success (some files skipped)");
    } else if all_skipped {
        tracing::info!(duration = ?elapsed, "backup cycle finished: every repository was skipped");
    } else {
        tracing::info!(duration = ?elapsed, "backup cycle finished successfully");
    }

    status::record_cycle_end(status, elapsed, &report, all);

    // All Repository instances are dropped. Ask glibc to return freed pages.
    if !SHUTDOWN.load(Ordering::SeqCst) {
        release_malloc_arenas();
    }
    (names, report)
}

fn log_next_run(delay: Duration) {
    let next_wall = chrono::Local::now() + delay;
    tracing::info!(
        next_run = %next_wall.format("%Y-%m-%d %H:%M:%S %Z"),
        delay = ?delay,
        "next backup scheduled"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_core::testing::conformance;

    /// The daemon's cycle over the conformance fixture reports what the core
    /// cycle does.
    #[test]
    fn conformance_daemon_cycle() {
        crate::init_test_environment();
        let tmp = tempfile::tempdir().unwrap();
        let repos = conformance::fixture(tmp.path());
        let mut service = AppService::new();

        let (names, report) = run_backup_cycle_report(
            &mut service,
            RepoSelection::All,
            &repos,
            &status::new_shared(),
        );

        assert_eq!(
            conformance::summary(&report, &repos),
            conformance::EXPECTED_SUMMARY
        );
        assert_eq!(
            conformance::hook_logs(tmp.path()),
            conformance::EXPECTED_HOOKS
        );
        assert_eq!(names.ran, ["present"]);
        assert_eq!(names.skipped, ["missing"]);
    }
}
