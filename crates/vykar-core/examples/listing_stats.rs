#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Directory shape, new-pointer positions and a simulated pointer-leaf cut
//! for the snapshots of a repository (06 step 0). Passphrase from
//! `VYKAR_PASSPHRASE`.
//!
//! Registers no session marker and backfills no key copy. Against a
//! repository that must not change, run it on a read-only mount. Locally it
//! writes an identity pin and caches under `--cache` (use an empty scratch
//! directory, never a real client cache) and sweeps stale index temp files
//! from `TMPDIR`.
//!
//! ```text
//! listing_stats --repo <url> --cache <dir> [--label <l>] [--after <RFC3339>] [--before <RFC3339>]
//!               [--targets 4096,16384,32768] [--no-shape] [--assume-key-len <n>]
//!               [--connections <n>] [--list]
//! ```

use std::time::Instant;

use vykar_core::OpenOptions;
use vykar_core::commands::util::open_repo;
use vykar_core::config::VykarConfig;
use vykar_core::measure::listing::{ListingOptions, analyze, select_snapshots};

type Error = Box<dyn std::error::Error>;

struct Args {
    repo: String,
    cache: String,
    connections: usize,
    list: bool,
    opts: ListingOptions,
}

fn value(it: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, Error> {
    it.next()
        .ok_or_else(|| format!("{flag} needs a value").into())
}

fn time(s: &str) -> Result<chrono::DateTime<chrono::Utc>, Error> {
    Ok(chrono::DateTime::parse_from_rfc3339(s)?.with_timezone(&chrono::Utc))
}

fn parse_args() -> Result<Args, Error> {
    let mut repo = None;
    let mut cache = None;
    let mut connections = 2;
    let mut list = false;
    let mut opts = ListingOptions::default();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--repo" => repo = Some(value(&mut it, &flag)?),
            "--cache" => cache = Some(value(&mut it, &flag)?),
            "--label" => opts.label = Some(value(&mut it, &flag)?),
            "--after" => opts.after = Some(time(&value(&mut it, &flag)?)?),
            "--before" => opts.before = Some(time(&value(&mut it, &flag)?)?),
            "--targets" => {
                opts.leaf_targets = value(&mut it, &flag)?
                    .split(',')
                    .map(|t| t.trim().parse::<usize>())
                    .collect::<Result<_, _>>()?;
            }
            "--no-shape" => opts.shape = false,
            "--assume-key-len" => opts.assume_key_len = Some(value(&mut it, &flag)?.parse()?),
            "--connections" => connections = value(&mut it, &flag)?.parse()?,
            "--list" => list = true,
            other => return Err(format!("unknown argument {other:?}").into()),
        }
    }
    Ok(Args {
        repo: repo.ok_or("--repo is required")?,
        cache: cache.ok_or("--cache is required")?,
        connections,
        list,
        opts,
    })
}

fn main() -> Result<(), Error> {
    let args = parse_args()?;

    let mut config = VykarConfig::default();
    config.repository.url = args.repo.clone();
    config.cache_dir = Some(args.cache.clone());
    config.trust_repo = true;
    config.limits.connections = args.connections;
    let passphrase = std::env::var("VYKAR_PASSPHRASE").ok();

    let t = Instant::now();
    eprintln!("opening {} (connections {})", args.repo, args.connections);
    let mut repo = open_repo(
        &config,
        passphrase.as_deref(),
        OpenOptions::new().with_index().skip_key_backfill(),
        &vykar_core::repo::control::CancelToken::new(),
    )?;
    eprintln!(
        "opened in {} ms; {} index entries, {} snapshots",
        t.elapsed().as_millis(),
        repo.chunk_index().len(),
        repo.manifest().snapshots.len()
    );

    if args.list {
        for s in select_snapshots(&repo, &args.opts) {
            let id = s.id.to_hex();
            println!(
                "{} {} {:<10} {}",
                id.get(..8).unwrap_or(&id),
                s.time.to_rfc3339(),
                s.source_label,
                s.name
            );
        }
        return Ok(());
    }

    let report = analyze(&mut repo, &args.opts)?;
    eprintln!(
        "analyzed {} snapshots, decoded {} tree chunks in {} ms",
        report.snapshots.len(),
        report.decoded_chunks,
        report.elapsed_ms
    );
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
