#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Time a walk of one configured source in stat or name emission order
//! (06 step 0). Prints one JSON `WalkReport` per repetition to stdout.
//!
//! ```text
//! walk_order [--config <path>] [--repo <label|url>] --source <label>
//!            [--order stat|name] [--inode-sort auto|on|off] [--no-materialize] [--no-shape]
//!            [--read-changed-since <RFC3339>] [--repeat <n>]
//! ```

use vykar_core::commands::backup::walk_measure::{
    InodeSort, WalkMeasureOptions, WalkOrder, measure_walk,
};
use vykar_core::config::{load_and_resolve, resolve_config_path};

type Error = Box<dyn std::error::Error>;

struct Args {
    config: Option<String>,
    repo: Option<String>,
    source: String,
    opts: WalkMeasureOptions,
    repeat: u32,
}

fn value(it: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, Error> {
    it.next()
        .ok_or_else(|| format!("{flag} needs a value").into())
}

fn parse_args() -> Result<Args, Error> {
    let mut config = None;
    let mut repo = None;
    let mut source = None;
    let mut opts = WalkMeasureOptions::default();
    let mut repeat = 1;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--config" => config = Some(value(&mut it, &flag)?),
            "--repo" => repo = Some(value(&mut it, &flag)?),
            "--source" => source = Some(value(&mut it, &flag)?),
            "--order" => {
                opts.order = match value(&mut it, &flag)?.as_str() {
                    "stat" => WalkOrder::Stat,
                    "name" => WalkOrder::Name,
                    other => return Err(format!("--order: unknown value {other:?}").into()),
                }
            }
            "--inode-sort" => {
                opts.inode_sort = match value(&mut it, &flag)?.as_str() {
                    "auto" => InodeSort::Auto,
                    "on" => InodeSort::On,
                    "off" => InodeSort::Off,
                    other => return Err(format!("--inode-sort: unknown value {other:?}").into()),
                }
            }
            "--no-materialize" => opts.materialize = false,
            "--no-shape" => opts.shape = false,
            "--read-changed-since" => {
                let t = chrono::DateTime::parse_from_rfc3339(&value(&mut it, &flag)?)?;
                let ns = t
                    .timestamp_nanos_opt()
                    .ok_or("--read-changed-since: out of range")?;
                opts.read_changed_since_ns = Some(ns);
            }
            "--repeat" => repeat = value(&mut it, &flag)?.parse()?,
            other => return Err(format!("unknown argument {other:?}").into()),
        }
    }
    Ok(Args {
        config,
        repo,
        source: source.ok_or("--source is required")?,
        opts,
        repeat,
    })
}

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    if let Ok(v) = std::env::var("VYKAR_WALK_EMIT_ORDER") {
        eprintln!("note: VYKAR_WALK_EMIT_ORDER={v:?} is ignored; --order decides");
    }

    let config_path = resolve_config_path(args.config.as_deref()).ok_or("no config file found")?;
    let repos = load_and_resolve(config_path.path())?;
    let repo = match &args.repo {
        Some(sel) => repos
            .iter()
            .find(|r| r.label_or_url() == sel || r.config.repository.url == *sel),
        None if repos.len() == 1 => repos.first(),
        None => None,
    };
    let Some(repo) = repo else {
        let names: Vec<&str> = repos.iter().map(|r| r.label_or_url()).collect();
        return Err(format!("select a repository with --repo; available: {names:?}").into());
    };
    let Some(source) = repo.sources.iter().find(|s| s.label == args.source) else {
        let labels: Vec<&str> = repo.sources.iter().map(|s| s.label.as_str()).collect();
        return Err(format!("unknown source {:?}; available: {labels:?}", args.source).into());
    };

    for _ in 0..args.repeat {
        let report = measure_walk(&repo.config, source, &args.opts)?;
        println!("{}", serde_json::to_string(&report)?);
    }
    Ok(())
}
