mod config;
mod error;
mod handlers;
mod quota;
mod state;

use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::{parse_size, ServerSection};
use crate::state::{write_unpoisoned, AppState};

#[derive(Parser)]
#[command(name = "vykar-server", version, about = "vykar backup server")]
struct Cli {
    /// Address to listen on
    #[arg(short, long, default_value = "localhost:8585")]
    listen: String,

    /// Root directory where repositories are stored
    #[arg(short, long, default_value = "/var/lib/vykar")]
    data_dir: String,

    /// Reject DELETE and overwrite operations on pack files
    #[arg(long, default_value_t = false)]
    append_only: bool,

    /// Log output format: "json" or "pretty"
    #[arg(long, default_value = "pretty")]
    log_format: String,

    /// Storage quota (e.g. "500M", "10G"). Overrides auto-detection.
    /// Omit for automatic detection from filesystem quotas or free space.
    #[arg(long, value_parser = parse_size)]
    quota: Option<u64>,

    /// Lock TTL in seconds
    #[arg(long, default_value_t = 3600)]
    lock_ttl_seconds: u64,

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

fn parse_min_one(s: &str) -> Result<usize, String> {
    let n: usize = s.parse().map_err(|e| format!("{e}"))?;
    if n == 0 {
        return Err("value must be at least 1".into());
    }
    Ok(n)
}

fn main() {
    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.network_threads)
        .max_blocking_threads(cli.io_threads)
        .enable_all()
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Error: failed to build tokio runtime: {e}");
            std::process::exit(1);
        });

    runtime.block_on(async_main(cli));
}

async fn async_main(cli: Cli) {
    // Read token from environment
    let token = std::env::var("VYKAR_TOKEN").unwrap_or_default();
    if token.is_empty() {
        eprintln!("Error: VYKAR_TOKEN environment variable must be set");
        std::process::exit(1);
    }

    let config = ServerSection {
        listen: cli.listen,
        data_dir: cli.data_dir,
        token,
        append_only: cli.append_only,
        log_format: cli.log_format,
        lock_ttl_seconds: cli.lock_ttl_seconds,
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
    std::fs::create_dir_all(&config.data_dir).unwrap_or_else(|e| {
        eprintln!(
            "Error: cannot create data directory '{}': {e}",
            config.data_dir
        );
        std::process::exit(1);
    });

    let listen_addr = config.listen.clone();
    let state = AppState::new(config, cli.quota);

    // Spawn lock cleanup background task
    let cleanup_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            cleanup_expired_locks(&cleanup_state);
        }
    });

    let app = handlers::router(state);

    info!("vykar-server listening on {listen_addr}");
    let listener = TcpListener::bind(&listen_addr).await.unwrap_or_else(|e| {
        eprintln!("Error: cannot bind to {listen_addr}: {e}");
        std::process::exit(1);
    });
    axum::serve(listener, app).await.unwrap();
}

fn cleanup_expired_locks(state: &AppState) {
    let mut locks = write_unpoisoned(&state.inner.locks, "locks");
    locks.retain(|_id, info| !info.is_expired());
}
