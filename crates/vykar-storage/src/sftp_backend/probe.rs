//! SFTP probe: write a temp file, `hardlink@openssh.com` it to the final
//! name, remove the temp, and `stat` after a refusal to tell "exists" apart
//! from other failures: the status code (often a bare `Failure`) does not say why.

use std::sync::Arc;

use russh_sftp::client::SftpSession;
use russh_sftp::client::error::Error as SftpError;
use russh_sftp::protocol::OpenFlags;
use tokio::io::AsyncWriteExt;
use vykar_types::error::{Result, VykarError};

use super::{SftpBackend, SftpConn, is_not_found, posix_join};
use crate::probe::{
    CREATE, CaseResult, Cleanup, Ledger, Outcome, ProbeOptions, REPEAT_OTHER, REPEAT_SAME,
    RaceResult, Report, is_created, is_exists, payload,
};
use crate::runtime::ASYNC_RUNTIME;
use crate::{ParsedUrl, StorageConfig};

const BODY_LEN: usize = 1024;
const MECHANISM: &str = "hardlink@openssh.com";

/// Probe hardlink@openssh.com over SSH, connecting with the backend's own code
/// (`SftpBackend::new` + `SftpBackend::connect`). Must not be called inside a tokio
/// runtime (uses ASYNC_RUNTIME). Errors only before the first write.
pub fn run_sftp(cfg: &StorageConfig, opts: &ProbeOptions) -> Result<Report> {
    let ParsedUrl::Sftp {
        user,
        host,
        port,
        path,
    } = crate::parse_repo_url(&cfg.url)?
    else {
        return Err(VykarError::Config(
            "probe sftp: URL must be sftp://[user@]host[:port]/path".into(),
        ));
    };
    let backend = SftpBackend::new(
        &host,
        user.as_deref(),
        port,
        &path,
        cfg.sftp_key.as_deref(),
        cfg.sftp_agent.as_deref(),
        cfg.sftp_known_hosts.as_deref(),
        cfg.max_connections,
        cfg.sftp_timeout,
        cfg.retry,
    )?;
    let location = format!(
        "sftp://{}@{}:{}{}",
        backend.params.user, backend.params.host, backend.params.port, backend.params.root
    );
    run_with_backend(&backend, &location, opts)
}

/// Same on an already built backend (tests use the fault server's backend).
/// Opens one SSH connection per race writer, outside the backend's pool.
pub(crate) fn run_with_backend(
    backend: &SftpBackend,
    location: &str,
    opts: &ProbeOptions,
) -> Result<Report> {
    let mut sessions = Vec::new();
    // The SSH handles keep the connections open until the probe returns.
    let mut handles = Vec::new();
    for _ in 0..opts.writers() {
        let SftpConn { sftp, _session } = ASYNC_RUNTIME
            .block_on(SftpBackend::connect(&backend.params))
            .map_err(|e| e.err)?;
        sessions.push(sftp);
        handles.push(_session);
    }
    let report = ASYNC_RUNTIME.block_on(run_sftp_sessions(
        sessions,
        &backend.params.root,
        location,
        "ssh",
        opts,
    ));
    drop(handles);
    report
}

/// The probe proper, on caller-supplied sessions. `sessions.len()` sets the race
/// width (at least 2 are needed for a race; with 1 the race is skipped and the
/// verdict is at best Inconclusive). `root` is an absolute server path that must exist,
/// or relative to the login directory with `opts.sftp_root_relative_to_home`.
/// `transport` is copied into the report ("ssh", "exec:/usr/libexec/sftp-server").
pub async fn run_sftp_sessions(
    sessions: Vec<SftpSession>,
    root: &str,
    location: &str,
    transport: &str,
    opts: &ProbeOptions,
) -> Result<Report> {
    let Some(first) = sessions.first() else {
        return Err(VykarError::Other("probe sftp: no sessions".into()));
    };
    let preflight = |what: &str, path: &str, e: SftpError| {
        VykarError::Other(format!("probe sftp: {what} '{path}': {e}"))
    };

    let (root, location) = if opts.sftp_root_relative_to_home {
        let home = first
            .canonicalize(".")
            .await
            .map_err(|e| preflight("realpath", ".", e))?;
        let root = posix_join(&home, root.trim_start_matches('/'));
        let location = format!("{location} (root resolved to {root})");
        (root, location)
    } else {
        if !root.starts_with('/') {
            return Err(VykarError::Config(format!(
                "probe sftp: root '{root}' is not absolute; use --relative-root for a path \
                 under the login directory"
            )));
        }
        (root.to_string(), location.to_string())
    };

    let meta = first
        .metadata(root.as_str())
        .await
        .map_err(|e| preflight("stat", &root, e))?;
    if !meta.is_dir() {
        return Err(VykarError::Config(format!(
            "probe sftp: root '{root}' is not a directory"
        )));
    }
    if !opts.allow_repo_root {
        let config = posix_join(&root, "config");
        match first.metadata(config.as_str()).await {
            Err(e) if is_not_found(&e) => {}
            Ok(_) => {
                return Err(VykarError::Config(format!(
                    "probe sftp: root '{root}' holds a 'config' (a repository?); pass \
                     --allow-repo-root to probe it"
                )));
            }
            Err(e) => return Err(preflight("stat", &config, e)),
        }
    }
    let mut ledger = Ledger::for_run(opts)?;
    let prefix_dir = posix_join(&root, ledger.prefix());
    match first.metadata(prefix_dir.as_str()).await {
        Err(e) if is_not_found(&e) => {}
        Ok(_) => {
            return Err(VykarError::Other(format!(
                "probe sftp: '{prefix_dir}' already exists"
            )));
        }
        Err(e) => return Err(preflight("stat", &prefix_dir, e)),
    }
    first
        .create_dir(prefix_dir.as_str())
        .await
        .map_err(|e| preflight("mkdir", &prefix_dir, e))?;
    ledger.record_dir(ledger.prefix().to_string());

    let mut run = Run {
        sessions: Arc::new(sessions),
        root,
        ledger,
        notes: Vec::new(),
    };
    let (cases, race) = run.cases(opts).await;
    let cleanup = run.cleanup().await;
    Ok(Report::finish(
        opts,
        "sftp",
        transport.to_string(),
        MECHANISM,
        location,
        &run.ledger,
        cases,
        race,
        run.notes,
        cleanup,
    ))
}

fn status_detail(e: &SftpError) -> Option<String> {
    match e {
        SftpError::Status(s) => Some(
            format!("{:?} {}", s.status_code, s.error_message)
                .trim_end()
                .to_string(),
        ),
        _ => None,
    }
}

fn outcome_of(what: &str, e: &SftpError) -> Outcome {
    match status_detail(e) {
        Some(detail) => Outcome::Rejected {
            detail: format!("{what}: {detail}"),
        },
        None => Outcome::Transport {
            detail: format!("{what}: {e}"),
        },
    }
}

/// Write `data` to the new file `path`, as `atomic_put` writes its temp.
async fn write_new(s: &SftpSession, path: &str, data: &[u8]) -> std::result::Result<(), Outcome> {
    let io = |what: &str, e: std::io::Error| Outcome::Transport {
        detail: format!("temp {what}: {e}"),
    };
    let mut file = s
        .open_with_flags(
            path,
            OpenFlags::CREATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
        )
        .await
        .map_err(|e| outcome_of("temp create", &e))?;
    file.write_all(data).await.map_err(|e| io("write", e))?;
    file.flush().await.map_err(|e| io("flush", e))?;
    file.shutdown().await.map_err(|e| io("close", e))?;
    Ok(())
}

/// One `hardlink@openssh.com`. A refusal is classified by a `stat` of the
/// target, never by its status code.
async fn link(s: &SftpSession, from: &str, to: &str) -> Outcome {
    match s.hardlink(from, to).await {
        Ok(false) => Outcome::NotAdvertised,
        Ok(true) => Outcome::Created,
        Err(e) => {
            let Some(detail) = status_detail(&e) else {
                return Outcome::Transport {
                    detail: e.to_string(),
                };
            };
            match s.metadata(to).await {
                Ok(_) => Outcome::AlreadyExists { detail },
                Err(se) if is_not_found(&se) => Outcome::Rejected { detail },
                Err(se) => Outcome::Transport {
                    detail: format!("{detail}; stat of target: {se}"),
                },
            }
        }
    }
}

struct Run {
    sessions: Arc<Vec<SftpSession>>,
    root: String,
    ledger: Ledger,
    notes: Vec<String>,
}

impl Run {
    fn path(&self, key: &str) -> String {
        posix_join(&self.root, key)
    }

    fn first(&self) -> &SftpSession {
        self.sessions
            .first()
            .expect("run_sftp_sessions checked for a session")
    }

    /// Record a fresh temp name for `base` and return its full path.
    fn temp_for(&mut self, base: &str) -> std::result::Result<String, Outcome> {
        let name = format!(
            "{}{base}.{:016x}",
            vykar_protocol::TEMP_FILE_PREFIX,
            rand::random::<u64>()
        );
        let key = self
            .ledger
            .record_temp(&name)
            .map_err(|e| Outcome::Rejected {
                detail: e.to_string(),
            })?;
        Ok(self.path(&key))
    }

    async fn temp_with(
        &mut self,
        session: usize,
        base: &str,
        data: &[u8],
    ) -> std::result::Result<String, Outcome> {
        let tmp = self.temp_for(base)?;
        let s = self
            .sessions
            .get(session)
            .ok_or_else(|| Outcome::Rejected {
                detail: "no such session".into(),
            })?;
        write_new(s, &tmp, data).await?;
        Ok(tmp)
    }

    async fn remove_quietly(&self, path: &str) {
        let _ = self.first().remove_file(path).await;
    }

    /// Write a temp with `data` and link it to `target`; the temp is removed after.
    async fn create(&mut self, target: &str, base: &str, data: &[u8]) -> Outcome {
        let tmp = match self.temp_with(0, base, data).await {
            Ok(t) => t,
            Err(o) => return o,
        };
        let o = link(self.first(), &tmp, target).await;
        self.remove_quietly(&tmp).await;
        o
    }

    async fn content(&mut self, path: &str, want: &[u8]) -> Option<bool> {
        match self.first().read(path).await {
            Ok(got) => Some(got == want),
            Err(e) => {
                self.notes.push(format!("content check {path}: {e}"));
                None
            }
        }
    }

    async fn exists(&self, path: &str) -> String {
        match self.first().metadata(path).await {
            Ok(_) => "yes".into(),
            Err(e) if is_not_found(&e) => "no".into(),
            Err(e) => format!("unknown ({e})"),
        }
    }

    fn key(&mut self, name: &str) -> std::result::Result<String, String> {
        self.ledger
            .key(name)
            .map(|k| self.path(&k))
            .map_err(|e| e.to_string())
    }

    async fn cases(&mut self, opts: &ProbeOptions) -> (Vec<CaseResult>, Option<RaceResult>) {
        let mut cases = Vec::new();
        let prefix = self.ledger.prefix().to_string();
        let a = payload("A", &prefix, BODY_LEN);
        let b = payload("B", &prefix, BODY_LEN);
        let posix_rename = if self.first().supports_posix_rename() {
            "yes"
        } else {
            "no"
        };

        let target = match self.key("k") {
            Ok(t) => t,
            Err(e) => {
                self.notes.push(e);
                return (cases, None);
            }
        };
        let created = self.create(&target, "k", &a).await;
        let hardlink = match &created {
            Outcome::NotAdvertised => "no",
            // The temp could not be written, so no hardlink request was sent.
            Outcome::Rejected { detail } | Outcome::Transport { detail }
                if detail.starts_with("temp ") =>
            {
                "unknown"
            }
            _ => "yes",
        };
        cases.push(CaseResult::info(
            "advertised",
            Outcome::Info {
                detail: format!(
                    "hardlink@openssh.com: {hardlink}, posix-rename@openssh.com: {posix_rename}"
                ),
            },
            None,
        ));
        let content = if is_created(&created) {
            self.content(&target, &a).await
        } else {
            None
        };
        let not_advertised = matches!(created, Outcome::NotAdvertised);
        let create = CaseResult::expect(CREATE, "created", is_created, created, content, true);
        let create_ok = create.ok;
        cases.push(create);
        if not_advertised {
            return (cases, None);
        }

        // Repeats are judged against the bytes of create, so they need it fully ok.
        if create_ok {
            for (name, data) in [(REPEAT_SAME, &a), (REPEAT_OTHER, &b)] {
                let o = self.create(&target, "k", data).await;
                let content = self.content(&target, &a).await;
                cases.push(CaseResult::expect(
                    name,
                    "status with target present, bytes of create kept",
                    is_exists,
                    o,
                    content,
                    true,
                ));
            }
        } else {
            for name in [REPEAT_SAME, REPEAT_OTHER] {
                cases.push(CaseResult::skipped(
                    name,
                    "status with target present, bytes of create kept",
                    "create was not ok",
                ));
            }
        }

        // Which codes a failure that is not "exists" carries.
        match (self.key("absent-src"), self.key("absent-dst")) {
            (Ok(src), Ok(dst)) => {
                let o = link(self.first(), &src, &dst).await;
                let after = self.exists(&dst).await;
                cases.push(CaseResult::info(
                    "err_missing_source",
                    Outcome::Info {
                        detail: format!("{}; target present after: {after}", o.short()),
                    },
                    None,
                ));
            }
            (Err(e), _) | (_, Err(e)) => self.notes.push(e),
        }

        let missing_dir = self
            .ledger
            .nested_key("no-such-dir", "x")
            .map(|k| self.path(&k));
        match (missing_dir, self.temp_with(0, "x", &b).await) {
            (Ok(dst), Ok(tmp)) => {
                let o = link(self.first(), &tmp, &dst).await;
                let after = self.exists(&dst).await;
                self.remove_quietly(&tmp).await;
                cases.push(CaseResult::info(
                    "err_missing_dir",
                    Outcome::Info {
                        detail: format!("{}; target present after: {after}", o.short()),
                    },
                    None,
                ));
            }
            (Err(e), _) => self.notes.push(e.to_string()),
            (_, Err(o)) => cases.push(CaseResult::info("err_missing_dir", o, None)),
        }

        // Why the table rejects SSH_FXP_RENAME: does it replace an existing target?
        if create_ok {
            match self.temp_with(0, "k", &b).await {
                Ok(tmp) => {
                    let o = match self.first().rename(tmp.as_str(), target.as_str()).await {
                        Ok(()) => Outcome::Created,
                        Err(e) => outcome_of("rename", &e),
                    };
                    self.remove_quietly(&tmp).await;
                    let content = self.content(&target, &a).await;
                    cases.push(CaseResult::info(
                        "plain_rename_over_existing",
                        Outcome::Info {
                            detail: o.short().replace("created", "renamed over the target"),
                        },
                        content,
                    ));
                }
                Err(o) => cases.push(CaseResult::info("plain_rename_over_existing", o, None)),
            }
        }

        let race = if self.sessions.len() >= 2 {
            Some(self.race(opts.rounds(), &prefix).await)
        } else {
            self.notes
                .push("race skipped: fewer than 2 sessions".into());
            None
        };
        (cases, race)
    }

    async fn race(&mut self, rounds: usize, prefix: &str) -> RaceResult {
        let writers = self.sessions.len();
        let mut result = RaceResult::new(writers);
        for r in 0..rounds {
            let base = format!("race-{r}");
            let target = match self.key(&base) {
                Ok(t) => t,
                Err(e) => {
                    self.notes.push(e);
                    break;
                }
            };
            let payloads: Vec<Vec<u8>> = (0..writers)
                .map(|i| payload(&format!("r{r}w{i}"), prefix, BODY_LEN))
                .collect();
            // Every temp is written before any link is sent.
            let mut temps = Vec::with_capacity(writers);
            for (i, data) in payloads.iter().enumerate() {
                temps.push(self.temp_with(i, &base, data).await);
            }
            let mut tasks = Vec::with_capacity(writers);
            for (i, tmp) in temps.iter().enumerate() {
                tasks.push(match tmp {
                    Ok(tmp) => {
                        let sessions = Arc::clone(&self.sessions);
                        let tmp = tmp.clone();
                        let target = target.clone();
                        Ok(tokio::spawn(async move {
                            match sessions.get(i) {
                                Some(s) => link(s, &tmp, &target).await,
                                None => Outcome::Rejected {
                                    detail: "no such session".into(),
                                },
                            }
                        }))
                    }
                    Err(o) => Err(o.clone()),
                });
            }
            let mut outcomes = Vec::with_capacity(writers);
            for task in tasks {
                outcomes.push(match task {
                    Ok(handle) => handle.await.unwrap_or_else(|e| Outcome::Transport {
                        detail: format!("writer task: {e}"),
                    }),
                    Err(o) => o,
                });
            }
            for tmp in temps.iter().flatten() {
                self.remove_quietly(tmp).await;
            }
            if let Some(w) = result.add_round(&outcomes)
                && let Some(want) = payloads.get(w)
                && self.content(&target, want).await == Some(true)
            {
                result.content_matches_winner += 1;
            }
        }
        result
    }

    /// Remove every recorded key and temp, then the recorded directories,
    /// non-recursively, trying the next session when one is dead. Whatever is
    /// still in the prefix directory is listed, not removed.
    async fn cleanup(&self) -> Cleanup {
        let mut out = Cleanup::default();
        let mut live = 0;
        let files: Vec<String> = self
            .ledger
            .keys()
            .iter()
            .chain(self.ledger.temps())
            .map(|k| self.path(k))
            .collect();
        for path in &files {
            loop {
                let Some(s) = self.sessions.get(live) else {
                    out.errors.push(format!("remove {path}: no live session"));
                    break;
                };
                match s.remove_file(path.as_str()).await {
                    Ok(()) => out.removed += 1,
                    Err(e) if is_not_found(&e) => {}
                    Err(e @ SftpError::Status(_)) => out.errors.push(format!("remove {path}: {e}")),
                    Err(_) => {
                        live += 1;
                        continue;
                    }
                }
                break;
            }
        }
        for dir in self.ledger.dirs().iter().rev() {
            let path = self.path(dir);
            loop {
                let Some(s) = self.sessions.get(live) else {
                    out.errors.push(format!("rmdir {path}: no live session"));
                    break;
                };
                match s.remove_dir(path.as_str()).await {
                    Ok(()) => out.removed += 1,
                    // A non-empty directory answers a plain failure; the
                    // listing below reports what is left in it.
                    Err(SftpError::Status(_)) => {}
                    Err(_) => {
                        live += 1;
                        continue;
                    }
                }
                break;
            }
        }
        let prefix = self.ledger.prefix();
        let prefix_dir = self.path(prefix);
        if let Some(s) = self.sessions.get(live) {
            match s.metadata(prefix_dir.as_str()).await {
                Err(e) if is_not_found(&e) => {}
                Ok(_) => match s.read_dir(prefix_dir.as_str()).await {
                    Ok(entries) => {
                        for entry in entries {
                            let name = entry.file_name();
                            if name != "." && name != ".." {
                                out.leftovers.push(format!("{prefix}/{name}"));
                            }
                        }
                        if out.leftovers.is_empty() {
                            out.leftovers.push(prefix.to_string());
                        }
                    }
                    Err(e) => out.errors.push(format!("list {prefix_dir}: {e}")),
                },
                Err(e) => out.errors.push(format!("stat {prefix_dir}: {e}")),
            }
        } else {
            out.errors
                .push(format!("list {prefix_dir}: no live session"));
        }
        out.leftovers.sort();
        out
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::super::fault_server::FaultServer;
    use super::*;
    use crate::probe::{PROBE_PREFIX, Verdict};

    fn opts() -> ProbeOptions {
        ProbeOptions {
            race_writers: 4,
            race_rounds: 3,
            ..ProbeOptions::default()
        }
    }

    fn info_detail<'a>(report: &'a Report, name: &str) -> &'a str {
        match &report
            .cases
            .iter()
            .find(|c| c.name == name)
            .unwrap()
            .outcome
        {
            Outcome::Info { detail } => detail,
            other => panic!("{name}: {other:?}"),
        }
    }

    fn probe_entries(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(PROBE_PREFIX))
            .collect()
    }

    #[test]
    fn sftp_probe_works_on_fault_server() {
        let server = FaultServer::start();
        let report = run_with_backend(&server.backend(0), "fault server", &opts()).unwrap();
        assert_eq!(report.verdict, Verdict::Works, "{report:#?}");
        assert_eq!(report.exit_code(), 0, "{:?}", report.cleanup);
        let race = report.race.as_ref().unwrap();
        assert_eq!((race.rounds, race.writers), (3, 4));
        let repeat = report.cases.iter().find(|c| c.name == REPEAT_SAME).unwrap();
        assert!(
            matches!(&repeat.outcome, Outcome::AlreadyExists { detail } if detail.contains("Failure")),
            "{repeat:?}"
        );
        assert!(info_detail(&report, "err_missing_source").contains("NoSuchFile"));
        assert!(info_detail(&report, "advertised").contains("hardlink@openssh.com: yes"));
        assert!(probe_entries(&server.dir.path().join("repo")).is_empty());
    }

    #[test]
    fn sftp_probe_relative_root() {
        let server = FaultServer::start();
        std::fs::create_dir(server.dir.path().join("repo/sub")).unwrap();
        let relative = ProbeOptions {
            sftp_root_relative_to_home: true,
            ..opts()
        };
        // The fault server's REALPATH of "." is /repo, so /sub means /repo/sub.
        let sessions = connect_sessions(&server, 2);
        let report = ASYNC_RUNTIME
            .block_on(run_sftp_sessions(
                sessions.0,
                "/sub",
                "fault server",
                "ssh",
                &relative,
            ))
            .unwrap();
        drop(sessions.1);
        assert_eq!(report.verdict, Verdict::Works, "{report:#?}");
        assert!(report.location.contains("/repo/sub"), "{}", report.location);
        assert!(server.faults.hardlinks.load(Ordering::SeqCst) > 0);
        // Without the flag, /sub is the server's /sub, which does not exist.
        let sessions = connect_sessions(&server, 2);
        let err = ASYNC_RUNTIME.block_on(run_sftp_sessions(
            sessions.0,
            "/sub",
            "fault server",
            "ssh",
            &opts(),
        ));
        assert!(err.is_err());
        assert!(probe_entries(&server.dir.path().join("repo/sub")).is_empty());
    }

    #[allow(clippy::type_complexity)]
    fn connect_sessions(
        server: &FaultServer,
        n: usize,
    ) -> (
        Vec<SftpSession>,
        Vec<russh::client::Handle<super::super::SshHandler>>,
    ) {
        let backend = server.backend(0);
        let mut sessions = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..n {
            let SftpConn { sftp, _session } = ASYNC_RUNTIME
                .block_on(SftpBackend::connect(&backend.params))
                .unwrap();
            sessions.push(sftp);
            handles.push(_session);
        }
        (sessions, handles)
    }

    #[test]
    fn sftp_probe_hidden_hardlink_is_unsupported() {
        let server = FaultServer::start();
        server.faults.hide_hardlink.store(true, Ordering::SeqCst);
        let report = run_with_backend(&server.backend(0), "fault server", &opts()).unwrap();
        assert_eq!(report.verdict, Verdict::Unsupported, "{report:#?}");
        assert!(matches!(
            report
                .cases
                .iter()
                .find(|c| c.name == CREATE)
                .unwrap()
                .outcome,
            Outcome::NotAdvertised
        ));
        assert!(info_detail(&report, "advertised").contains("hardlink@openssh.com: no"));
        assert!(report.cleanup.leftovers.is_empty(), "{:?}", report.cleanup);
        assert!(report.cleanup.errors.is_empty(), "{:?}", report.cleanup);
        assert_eq!(server.faults.hardlinks.load(Ordering::SeqCst), 0);
        assert!(probe_entries(&server.dir.path().join("repo")).is_empty());
    }

    #[test]
    fn fault_server_hardlink_maps_exists_to_failure() {
        let server = FaultServer::start();
        std::fs::write(server.path("src"), b"new").unwrap();
        std::fs::write(server.path("dst"), b"old").unwrap();
        let (sessions, handles) = connect_sessions(&server, 1);
        let s = sessions.first().unwrap();
        let err = ASYNC_RUNTIME
            .block_on(s.hardlink("/repo/src", "/repo/dst"))
            .unwrap_err();
        drop(handles);
        assert!(
            matches!(&err, SftpError::Status(st)
                if st.status_code == russh_sftp::protocol::StatusCode::Failure),
            "{err:?}"
        );
        assert_eq!(std::fs::read(server.path("dst")).unwrap(), b"old");
    }
}
