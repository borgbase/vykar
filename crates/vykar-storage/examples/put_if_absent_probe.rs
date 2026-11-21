//! Probe put-if-absent on one storage target and print a Markdown result row.
//!
//! ```text
//! put_if_absent_probe [--label TEXT] [--json PATH] [--race-writers N] [--race-rounds N] [--allow-repo-root] <target>
//!   local <DIR>
//!   s3 [URL] [--credentials-file PATH] [--root ROOT] [--region R] [--allow-insecure-http]
//!         URL: s3://endpoint[:port]/bucket[/root] | s3+http://...; if omitted, built from the
//!              file's Endpoint and Bucket plus --root (default "vykar-probe-runs")
//!         credentials: --credentials-file, else AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//!   sftp <sftp://[user@]host[:port]/path> [--relative-root] [--key PATH] [--agent PATH] [--known-hosts PATH] [--timeout SECS]
//!   sftp-exec <ROOT> -- <PROGRAM> [ARGS...]
//! ```
//!
//! Before the first write it prints the run's prefix to stderr, so a killed run
//! still says which `vykar-probe-…` names to remove.
//!
//! Exit codes: 0 works, 1 ignores the condition, 2 unsupported, 3 inconclusive
//! or a pre-flight error, 4 works but cleanup left names or errors. A failed
//! `--json` write is reported on stderr and does not change the exit code.
//!
//! Build: `cargo build --release -p vykar-storage --features backend-sftp --example put_if_absent_probe`.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::PathBuf;
use std::process::Stdio;

use clap::{Parser, Subcommand};
use vykar_storage::probe::{self, ProbeOptions, Report};
use vykar_storage::{RetryConfig, StorageConfig};

#[derive(Parser)]
#[command(about = "Probe put-if-absent on one storage target")]
struct Cli {
    /// Free text naming the target (software, version, host OS, filesystem).
    #[arg(long, global = true, default_value = "")]
    label: String,
    /// Also write the full report as JSON to this path.
    #[arg(long, global = true)]
    json: Option<PathBuf>,
    /// Concurrent creators per race round (2..=32).
    #[arg(long, global = true, default_value_t = 8)]
    race_writers: usize,
    /// Race rounds, each on a fresh key (1..=100).
    #[arg(long, global = true, default_value_t = 10)]
    race_rounds: usize,
    /// Probe a root that holds a `config` object.
    #[arg(long, global = true)]
    allow_repo_root: bool,
    #[command(subcommand)]
    target: Target,
}

#[derive(Subcommand)]
enum Target {
    /// A local directory.
    Local { dir: PathBuf },
    /// An S3 or S3-compatible bucket.
    S3 {
        /// s3://endpoint[:port]/bucket[/root] or s3+http://...
        url: Option<String>,
        /// `Name: value` file with the key ID and secret (and optionally
        /// Endpoint, Bucket, Region Code).
        #[arg(long)]
        credentials_file: Option<PathBuf>,
        /// Root under the bucket when the URL is built from the file.
        #[arg(long, default_value = "vykar-probe-runs")]
        root: String,
        #[arg(long)]
        region: Option<String>,
        #[arg(long)]
        allow_insecure_http: bool,
    },
    /// An SFTP server over SSH, connected with the backend's own code.
    Sftp {
        /// sftp://[user@]host[:port]/path
        url: String,
        /// The URL's path is relative to the login directory.
        #[arg(long)]
        relative_root: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        agent: Option<String>,
        #[arg(long)]
        known_hosts: Option<String>,
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// An sftp-server binary spoken to over its stdin/stdout, no SSH.
    SftpExec {
        /// Absolute directory on the server to probe under.
        root: String,
        /// The server program and its arguments.
        #[arg(last = true, required = true, num_args = 1..)]
        command: Vec<String>,
    },
}

fn storage_config(url: String) -> StorageConfig {
    StorageConfig {
        url,
        region: None,
        access_key_id: None,
        secret_access_key: None,
        sftp_key: None,
        sftp_agent: None,
        sftp_known_hosts: None,
        max_connections: None,
        sftp_timeout: None,
        access_token: None,
        allow_insecure_http: false,
        retry: RetryConfig::default(),
        s3_soft_delete: false,
        s3_conditional_writes: vykar_storage::S3ConditionalWrites::default(),
        cancel: vykar_types::cancel::CancelToken::new(),
    }
}

/// `s3://<host>/<bucket>/<root>` from a credentials file's endpoint.
fn s3_url_from(endpoint: &str, bucket: &str, root: &str) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    let lower = endpoint.to_ascii_lowercase();
    let (scheme, host) = if lower.starts_with("http://") {
        (
            "s3+http",
            endpoint.get("http://".len()..).unwrap_or_default(),
        )
    } else if lower.starts_with("https://") {
        ("s3", endpoint.get("https://".len()..).unwrap_or_default())
    } else {
        ("s3", endpoint)
    };
    let root = root.trim_matches('/');
    if root.is_empty() {
        format!("{scheme}://{host}/{bucket}")
    } else {
        format!("{scheme}://{host}/{bucket}/{root}")
    }
}

/// Print the prefix and the place the probe writes under, before it starts.
fn announce(opts: &ProbeOptions, location: &str, note: &str) {
    let prefix = opts.prefix.as_deref().unwrap_or_default();
    let location = location.trim_end_matches('/');
    eprintln!("put_if_absent_probe: prefix {prefix}, writing under {location}/{prefix}{note}");
}

fn run(cli: Cli) -> Result<Report, String> {
    let opts = ProbeOptions {
        label: cli.label,
        race_writers: cli.race_writers,
        race_rounds: cli.race_rounds,
        allow_repo_root: cli.allow_repo_root,
        sftp_root_relative_to_home: false,
        prefix: Some(probe::new_prefix()),
    };
    match cli.target {
        Target::Local { dir } => {
            let location = std::fs::canonicalize(&dir).unwrap_or_else(|_| dir.clone());
            announce(&opts, &location.display().to_string(), "");
            probe::run_local(&dir, &opts).map_err(|e| e.to_string())
        }
        Target::S3 {
            url,
            credentials_file,
            root,
            region,
            allow_insecure_http,
        } => {
            let file = credentials_file
                .as_deref()
                .map(probe::read_s3_credentials_file)
                .transpose()
                .map_err(|e| e.to_string())?;
            let url = match (url, &file) {
                (Some(url), _) => url,
                (None, Some(f)) => match (&f.endpoint, &f.bucket) {
                    (Some(endpoint), Some(bucket)) => s3_url_from(endpoint, bucket, &root),
                    _ => {
                        return Err(
                            "no URL given and the credentials file lacks Endpoint or Bucket".into(),
                        );
                    }
                },
                (None, None) => return Err("s3: give a URL or --credentials-file".into()),
            };
            let (key_id, secret) = match &file {
                Some(f) => (f.access_key_id.clone(), f.secret_access_key.clone()),
                None => (
                    std::env::var("AWS_ACCESS_KEY_ID")
                        .map_err(|_| "AWS_ACCESS_KEY_ID is not set".to_string())?,
                    std::env::var("AWS_SECRET_ACCESS_KEY")
                        .map_err(|_| "AWS_SECRET_ACCESS_KEY is not set".to_string())?,
                ),
            };
            let region = region
                .or_else(|| file.as_ref().and_then(|f| f.region.clone()))
                .or_else(|| std::env::var("AWS_REGION").ok())
                .unwrap_or_else(|| "us-east-1".into());
            let cfg = StorageConfig {
                region: Some(region),
                access_key_id: Some(key_id),
                secret_access_key: Some(secret),
                allow_insecure_http,
                ..storage_config(url)
            };
            announce(&opts, &cfg.url, "");
            probe::run_s3(&cfg, &opts).map_err(|e| e.to_string())
        }
        Target::Sftp {
            url,
            relative_root,
            key,
            agent,
            known_hosts,
            timeout,
        } => {
            let cfg = StorageConfig {
                sftp_key: key,
                sftp_agent: agent,
                sftp_known_hosts: known_hosts,
                sftp_timeout: timeout,
                ..storage_config(url)
            };
            let opts = ProbeOptions {
                sftp_root_relative_to_home: relative_root,
                ..opts
            };
            let note = if relative_root {
                " (path relative to the login directory)"
            } else {
                ""
            };
            announce(&opts, &cfg.url, note);
            probe::run_sftp(&cfg, &opts).map_err(|e| e.to_string())
        }
        Target::SftpExec { root, command } => {
            announce(&opts, &root, "");
            sftp_exec(&root, &command, &opts)
        }
    }
}

/// Spawn one `sftp-server` per race writer and probe over their stdio.
fn sftp_exec(root: &str, command: &[String], opts: &ProbeOptions) -> Result<Report, String> {
    let Some((program, args)) = command.split_first() else {
        return Err("sftp-exec: no program given".into());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    runtime.block_on(async {
        let mut children = Vec::new();
        let mut sessions = Vec::new();
        for _ in 0..opts.writers() {
            let mut child = tokio::process::Command::new(program)
                .args(args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()
                .map_err(|e| format!("spawn {program}: {e}"))?;
            let (Some(stdout), Some(stdin)) = (child.stdout.take(), child.stdin.take()) else {
                return Err(format!("{program}: no stdio pipes"));
            };
            let session = russh_sftp::client::SftpSession::new(tokio::io::join(stdout, stdin))
                .await
                .map_err(|e| format!("{program}: SFTP init: {e}"))?;
            sessions.push(session);
            children.push(child);
        }
        let report = probe::run_sftp_sessions(
            sessions,
            root,
            &format!("{root} via {program}"),
            &format!("exec:{program}"),
            opts,
        )
        .await
        .map_err(|e| e.to_string());
        drop(children);
        report
    })
}

fn main() {
    let cli = Cli::parse();
    let json = cli.json.clone();
    let report = match run(cli) {
        Ok(report) => report,
        Err(e) => {
            eprintln!("put_if_absent_probe: {e}");
            std::process::exit(3);
        }
    };
    println!("{}", Report::markdown_header());
    println!("{}", report.markdown_row());
    if let Some(path) = json {
        let written = serde_json::to_string_pretty(&report)
            .map_err(|e| e.to_string())
            .and_then(|s| std::fs::write(&path, s).map_err(|e| e.to_string()));
        if let Err(e) = written {
            eprintln!("put_if_absent_probe: write {}: {e}", path.display());
        }
    }
    std::process::exit(report.exit_code());
}
