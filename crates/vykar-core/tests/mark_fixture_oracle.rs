//! "Unreferenced" by the mark equals "refcount 0 after a sequential rebuild"
//! on every frozen v2/v3 fixture after its upgrade (the upgraded base keeps
//! the legacy counts; v4/v5 fixtures are refused since 05 step 8). The oracle uses public functions only; the
//! archives are extracted to a temp dir and never written.

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

mod common;

use std::collections::HashMap;

use vykar_core::commands;
use vykar_core::commands::list::{
    for_each_decoded_item, load_snapshot_item_stream, load_snapshot_meta,
};
use vykar_core::repo::control::CancelToken;
use vykar_core::snapshot::item::ItemType;
use vykar_types::chunk_id::ChunkId;

use crate::common::fixtures::{self, Fixture};

const MODES: &[&str] = &["none", "aes256gcm", "chacha20poly1305"];

/// Today's sequential refcount rebuild over every snapshot.
fn sequential_refcounts(fx: &Fixture) -> HashMap<ChunkId, u32> {
    let mut repo = fx.open();
    let names: Vec<String> = repo
        .manifest()
        .snapshots
        .iter()
        .map(|e| e.name.clone())
        .collect();
    let mut counts: HashMap<ChunkId, u32> = HashMap::new();
    for name in &names {
        let meta = load_snapshot_meta(&repo, name).unwrap();
        for id in &meta.item_ptrs {
            if repo.chunk_index().contains(id) {
                *counts.entry(*id).or_insert(0) += 1;
            }
        }
        let stream = load_snapshot_item_stream(&mut repo, name, &CancelToken::new()).unwrap();
        for_each_decoded_item(&stream, |item| {
            if item.entry_type == ItemType::RegularFile {
                for r in &item.chunks {
                    if repo.chunk_index().contains(&r.id) {
                        *counts.entry(r.id).or_insert(0) += 1;
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    }
    counts
}

fn assert_report_matches_the_oracle(fx: &Fixture) {
    let label = format!("{}-{}", fx.prefix, fx.mode);
    let oracle = sequential_refcounts(fx);
    let entries: Vec<_> = fx.open().chunk_index().iter().collect();
    let mut zero: Vec<ChunkId> = entries
        .iter()
        .filter(|(id, _)| !oracle.contains_key(id))
        .map(|(id, _)| *id)
        .collect();
    zero.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    let mismatches = entries
        .iter()
        .filter(|(id, e)| oracle.get(id).copied().unwrap_or(0) != e.refcount)
        .count();

    let report =
        commands::compact::report_unreferenced(&fx.config(), fx.passphrase(), &CancelToken::new())
            .unwrap();
    assert_eq!(report.snapshots, fx.snapshots.len(), "{label}");
    assert_eq!(report.entries, entries.len(), "{label}");
    assert_eq!(report.unreferenced, zero, "{label}");
    assert_eq!(report.refcount_mismatches, mismatches, "{label}");
}

fn check_fixtures(prefix: &str, version: u64) {
    for mode in MODES {
        let fx = fixtures::extract(prefix, mode, version);
        commands::upgrade::run(&fx.config(), fx.passphrase()).unwrap();
        assert_report_matches_the_oracle(&fx);
    }
}

#[test]
fn v2_fixtures_unreferenced_equals_a_sequential_rebuild() {
    check_fixtures("v2", 2);
}

#[test]
fn v3_fixtures_unreferenced_equals_a_sequential_rebuild() {
    check_fixtures("v3", 3);
}
