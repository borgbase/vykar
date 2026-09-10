#![allow(clippy::print_stderr)]
#![cfg_attr(
    test,
    allow(
        clippy::cast_possible_truncation,
        clippy::doc_markdown,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::manual_let_else,
        clippy::panic,
        clippy::single_match_else,
        clippy::unwrap_used
    )
)]

pub mod config;
pub mod error;
pub mod handlers;
pub mod quota;
pub mod state;

use axum::serve::ListenerExt;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::config::{parse_size, ServerSection};
use crate::error::StartupError;
use crate::state::AppState;

/// Command-line interface for the server binary.
#[derive(Parser)]
#[command(name = "vykar-server", version, about = "vykar backup server")]
pub struct Cli {
    /// Address to listen on
    #[arg(short, long, default_value = "localhost:8585")]
    listen: String,

    /// Root directory where repositories are stored
    #[arg(short, long, default_value = "/var/lib/vykar")]
    data_dir: String,

    /// Append-only mode: only index/index.gen/locks/sessions are mutable; all other objects are immutable once written
    #[arg(long, default_value_t = false)]
    append_only: bool,

    /// Log output format: "json" or "pretty"
    #[arg(long, default_value = "pretty")]
    log_format: String,

    /// Storage quota (e.g. "500M", "10G"). Overrides auto-detection.
    /// Omit for automatic detection from filesystem quotas or free space.
    #[arg(long, value_parser = parse_size)]
    quota: Option<u64>,

    /// Number of async threads for handling network connections (minimum 1)
    #[arg(long, default_value_t = 4, value_parser = parse_min_one)]
    network_threads: usize,

    /// Number of threads for blocking disk I/O (reads, writes, hashing) (minimum 1)
    #[arg(long, default_value_t = 6, value_parser = parse_min_one)]
    io_threads: usize,

    /// Enable debug logging
    #[arg(long, default_value_t = false)]
    debug: bool,
}

/// Parse a thread count, rejecting zero.
pub fn parse_min_one(s: &str) -> Result<usize, String> {
    let n: usize = s.parse().map_err(|e| format!("{e}"))?;
    if n == 0 {
        return Err("value must be at least 1".into());
    }
    Ok(n)
}

/// Build the tokio runtime and serve until the process is terminated.
pub fn run(cli: Cli) -> Result<(), StartupError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.network_threads)
        .max_blocking_threads(cli.io_threads)
        .enable_all()
        .build()
        .map_err(StartupError::Runtime)?;

    runtime.block_on(serve(cli))
}

async fn serve(cli: Cli) -> Result<(), StartupError> {
    // Read token from environment
    let token = std::env::var("VYKAR_TOKEN").unwrap_or_default();
    if token.is_empty() {
        return Err(StartupError::MissingToken);
    }

    let config = ServerSection {
        listen: cli.listen,
        data_dir: cli.data_dir,
        token,
        append_only: cli.append_only,
        log_format: cli.log_format,
    };

    // Initialize tracing
    let log_level = if cli.debug {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };
    match config.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_max_level(log_level)
                .init();
        }
        _ => {
            tracing_subscriber::fmt().with_max_level(log_level).init();
        }
    }

    // Ensure data directory exists
    std::fs::create_dir_all(&config.data_dir).map_err(|source| StartupError::DataDir {
        path: config.data_dir.clone(),
        source,
    })?;

    let listen_addr = config.listen.clone();
    let state = AppState::new(config, cli.quota);

    let app = handlers::router(state);

    // Install before bind so the handlers are live by the time the readiness
    // line below is printed.
    let signals = ShutdownSignals::install().map_err(StartupError::Signals)?;

    let listener = TcpListener::bind(&listen_addr)
        .await
        .map_err(|source| StartupError::Bind {
            addr: listen_addr.clone(),
            source,
        })?;
    let bound_addr = listener
        .local_addr()
        .map_or(listen_addr, |addr| addr.to_string());
    info!("vykar-server listening on {bound_addr}");

    // Responses are written in several small pieces (headers, then body
    // frames); Nagle's algorithm holds those back waiting for the client's
    // ACK, adding a round-trip to every request.
    let listener = listener.tap_io(|stream| {
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!("failed to set TCP_NODELAY on incoming connection: {e}");
        }
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(signals.wait())
        .await
        .map_err(StartupError::Serve)
}

/// Persistent SIGINT/SIGTERM streams (Ctrl-C on Windows).
///
/// Constructed synchronously so registration happens before the listener is
/// bound. The streams stay alive across both signals: the first one starts a
/// graceful drain, the second exits immediately, mirroring the CLI convention.
struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_c: tokio::signal::windows::CtrlC,
}

/// Which signal arrived, with its name for logging and its conventional
/// death-by-signal exit code (128 + signal number).
#[derive(Clone, Copy)]
struct Received {
    name: &'static str,
    exit_code: i32,
}

impl ShutdownSignals {
    fn install() -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())?,
                terminate: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(windows)]
        {
            Ok(Self {
                ctrl_c: tokio::signal::windows::ctrl_c()?,
            })
        }
    }

    async fn next(&mut self) -> Received {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => Received { name: "SIGINT", exit_code: 130 },
                _ = self.terminate.recv() => Received { name: "SIGTERM", exit_code: 143 },
            }
        }
        #[cfg(windows)]
        {
            self.ctrl_c.recv().await;
            Received {
                name: "Ctrl-C",
                exit_code: 130,
            }
        }
    }

    /// Resolve on the first signal, which tells axum to stop accepting and
    /// drain in-flight requests. A second signal exits the process at once.
    async fn wait(mut self) {
        let first = self.next().await;
        info!(
            "received {}; finishing in-flight requests before exit",
            first.name
        );
        // The streams were already polled once, so a signal delivered before
        // this task is first polled is buffered rather than lost.
        tokio::spawn(async move {
            let second = self.next().await;
            warn!("received second {}; exiting immediately", second.name);
            std::process::exit(second.exit_code);
        });
    }
}
