#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Path-scoped reads of one snapshot through its listing key index (06 step
//! 4), compared with a full scan. Prints one JSON line for the snapshot's
//! index, one per query and a summary. Passphrase from `VYKAR_PASSPHRASE`.
//!
//! Registers no session marker and backfills no key copy. Locally it writes
//! an identity pin and caches under `--cache` (use an empty scratch
//! directory per run to measure storage reads: the tree-chunk cache lives
//! there).
//!
//! ```text
//! listing_read --repo <url> --cache <dir> --snapshot <name> [--dir <path>]... [--subtree <path>]...
//!              [--sample-leaves <n>] [--no-compare] [--connections <n>]
//! ```

use std::collections::HashSet;
use std::time::Instant;

use serde_json::json;
use vykar_core::OpenOptions;
use vykar_core::commands::list::{ItemStreamReader, load_snapshot_meta};
use vykar_core::commands::listing::{ListingRead, ListingView};
use vykar_core::commands::util::open_repo;
use vykar_core::config::VykarConfig;
use vykar_core::repo::Repo;
use vykar_core::repo::control::CancelToken;
use vykar_core::snapshot::SnapshotMeta;
use vykar_core::snapshot::item::ItemType;

type Error = Box<dyn std::error::Error>;

struct Args {
    repo: String,
    cache: String,
    snapshot: String,
    dirs: Vec<String>,
    subtrees: Vec<String>,
    sample_leaves: usize,
    compare: bool,
    connections: usize,
}

fn value(it: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, Error> {
    it.next()
        .ok_or_else(|| format!("{flag} needs a value").into())
}

fn parse_args() -> Result<Args, Error> {
    let mut repo = None;
    let mut cache = None;
    let mut snapshot = None;
    let mut args = Args {
        repo: String::new(),
        cache: String::new(),
        snapshot: String::new(),
        dirs: Vec::new(),
        subtrees: Vec::new(),
        sample_leaves: 0,
        compare: true,
        connections: 2,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--repo" => repo = Some(value(&mut it, &flag)?),
            "--cache" => cache = Some(value(&mut it, &flag)?),
            "--snapshot" => snapshot = Some(value(&mut it, &flag)?),
            "--dir" => args.dirs.push(value(&mut it, &flag)?),
            "--subtree" => args.subtrees.push(value(&mut it, &flag)?),
            "--sample-leaves" => args.sample_leaves = value(&mut it, &flag)?.parse()?,
            "--no-compare" => args.compare = false,
            "--connections" => args.connections = value(&mut it, &flag)?.parse()?,
            other => return Err(format!("unknown argument {other:?}").into()),
        }
    }
    args.repo = repo.ok_or("--repo is required")?;
    args.cache = cache.ok_or("--cache is required")?;
    args.snapshot = snapshot.ok_or("--snapshot is required")?;
    Ok(args)
}

/// (p50, p99, max) of `v`; zeros when empty.
fn pct(mut v: Vec<u64>) -> (u64, u64, u64) {
    if v.is_empty() {
        return (0, 0, 0);
    }
    v.sort_unstable();
    // Nearest rank of percentile `q`.
    let at = |q: usize| v.get((v.len() - 1) * q / 100).copied().unwrap_or_default();
    (at(50), at(99), v.last().copied().unwrap_or_default())
}

fn lossy(p: &[u8]) -> String {
    String::from_utf8_lossy(p).into_owned()
}

/// One query through `view`: the paths, the read statistics and the wall ms.
fn run(
    repo: &Repo,
    view: &ListingView<'_>,
    dir: &[u8],
    subtree: bool,
) -> Result<(Vec<Vec<u8>>, ListingRead, f64), Error> {
    let cancel = CancelToken::new();
    let mut paths = Vec::new();
    let visit = |i: vykar_core::snapshot::item::Item| {
        paths.push(i.path_bytes().to_vec());
        Ok(())
    };
    let t = Instant::now();
    let stats = if subtree {
        view.subtree(repo, dir, &cancel, visit)?
    } else {
        view.dir_entries(repo, dir, &cancel, visit)?
    };
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    Ok((paths, stats, ms))
}

/// Up to `n` leaf directories (no directory child), evenly spaced in
/// stream order, from one scan.
fn sample_leaves(repo: &Repo, meta: &SnapshotMeta, n: usize) -> Result<Vec<Vec<u8>>, Error> {
    let mut dirs: Vec<Vec<u8>> = Vec::new();
    let mut has_dir_child: HashSet<Vec<u8>> = HashSet::new();
    ListingView::ignore_index(meta).subtree(repo, b"", &CancelToken::new(), |i| {
        if i.entry_type == ItemType::Directory {
            let p = i.path_bytes();
            if let Some(slash) = p.iter().rposition(|b| *b == b'/') {
                has_dir_child.insert(p.get(..slash).unwrap_or_default().to_vec());
            } else {
                has_dir_child.insert(Vec::new());
            }
            dirs.push(p.to_vec());
        }
        Ok(())
    })?;
    let leaves: Vec<Vec<u8>> = dirs
        .into_iter()
        .filter(|d| !has_dir_child.contains(d))
        .collect();
    if n == 0 || leaves.is_empty() {
        return Ok(Vec::new());
    }
    let take = n.min(leaves.len());
    Ok((0..take)
        .filter_map(|k| leaves.get(k * leaves.len() / take).cloned())
        .collect())
}

fn main() -> Result<(), Error> {
    let args = parse_args()?;

    let mut config = VykarConfig::default();
    config.repository.url = args.repo.clone();
    config.cache_dir = Some(args.cache.clone());
    config.trust_repo = true;
    config.limits.connections = args.connections;
    let passphrase = std::env::var("VYKAR_PASSPHRASE").ok();
    let mut repo = open_repo(
        &config,
        passphrase.as_deref(),
        OpenOptions::new().with_index().skip_key_backfill(),
        &CancelToken::new(),
    )?;
    let name = repo
        .manifest()
        .resolve_snapshot(&args.snapshot)?
        .name
        .clone();
    let meta = load_snapshot_meta(&repo, &name)?;
    ItemStreamReader::new(&repo).ensure_index(&mut repo)?;
    let repo = repo;

    let view = ListingView::new(&meta);
    let mut without_ext = meta.clone();
    without_ext.ext = None;
    let growth =
        rmp_serde::to_vec(&meta)?.len() as i64 - rmp_serde::to_vec(&without_ext)?.len() as i64;
    let index_line = match view.index() {
        Ok(ix) => {
            let (p50, p99, max) = pct(ix.keys.iter().map(|k| k.len() as u64).collect());
            json!({
                "snapshot": name,
                "item_ptrs": meta.item_ptrs.len(),
                "ext_bytes": meta.ext.as_ref().map_or(0, Vec::len),
                "meta_growth_bytes": growth,
                "order": ix.order,
                "segments": ix.segments.iter().map(|s| json!({"root": lossy(&s.root), "first_ptr": s.first_ptr})).collect::<Vec<_>>(),
                "stride": ix.stride,
                "keys": ix.keys.len(),
                "key_len_p50": p50, "key_len_p99": p99, "key_len_max": max,
            })
        }
        Err(no) => json!({
            "snapshot": name,
            "item_ptrs": meta.item_ptrs.len(),
            "ext_bytes": meta.ext.as_ref().map_or(0, Vec::len),
            "meta_growth_bytes": growth,
            "no_index": no.to_string(),
        }),
    };
    println!("{index_line}");

    let mut queries: Vec<(Vec<u8>, bool)> = Vec::new();
    queries.extend(args.dirs.iter().map(|d| (d.as_bytes().to_vec(), false)));
    queries.extend(
        sample_leaves(&repo, &meta, args.sample_leaves)?
            .into_iter()
            .map(|d| (d, false)),
    );
    queries.extend(args.subtrees.iter().map(|d| (d.as_bytes().to_vec(), true)));

    let scan_view = ListingView::ignore_index(&meta);
    let (mut fallbacks, mut differing) = (0usize, 0usize);
    let (mut small_chunks, mut small_bytes) = (Vec::new(), Vec::new());
    for (dir, subtree) in &queries {
        let (paths, stats, ms) = run(&repo, &view, dir, *subtree)?;
        let (identical, scan_ms) = if args.compare {
            let (scanned, _, scan_ms) = run(&repo, &scan_view, dir, *subtree)?;
            (Some(scanned == paths), Some(scan_ms))
        } else {
            (None, None)
        };
        fallbacks += usize::from(stats.fallback.is_some());
        differing += usize::from(identical == Some(false));
        if stats.match_chunks <= 1 {
            small_chunks.push(stats.chunks_read as u64);
            small_bytes.push(stats.stored_bytes);
        }
        println!(
            "{}",
            json!({
                "dir": lossy(dir),
                "kind": if *subtree { "subtree" } else { "dir_entries" },
                "results": paths.len(),
                "via_index": stats.via_index,
                "fallback": stats.fallback,
                "chunks_read": stats.chunks_read,
                "match_chunks": stats.match_chunks,
                "stored_bytes": stats.stored_bytes,
                "plain_bytes": stats.plain_bytes,
                "ms": ms,
                "scan_ms": scan_ms,
                "identical": identical,
            })
        );
    }
    let (c50, c99, cmax) = pct(small_chunks);
    let (b50, b99, bmax) = pct(small_bytes);
    println!(
        "{}",
        json!({
            "queries": queries.len(),
            "fallbacks": fallbacks,
            "non_identical": differing,
            "single_chunk_queries": {
                "chunks_read_p50": c50, "chunks_read_p99": c99, "chunks_read_max": cmax,
                "stored_bytes_p50": b50, "stored_bytes_p99": b99, "stored_bytes_max": bmax,
            },
        })
    );
    Ok(())
}
