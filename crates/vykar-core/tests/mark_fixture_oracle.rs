//! "Unreferenced" by the mark equals "not referenced by a sequential walk"
//! on every frozen v2/v3 fixture after its upgrade. The oracle uses public
//! functions only; the archives are extracted to a temp dir and never
//! written.

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

mod common;

use std::collections::HashSet;

use vykar_core::commands;
use vykar_core::commands::list::{
    for_each_decoded_item, load_snapshot_item_stream, load_snapshot_meta,
};
use vykar_core::repo::control::CancelToken;
use vykar_core::snapshot::item::ItemType;
use vykar_types::chunk_id::ChunkId;

use crate::common::fixtures::{self, Fixture};

const MODES: &[&str] = &["aes256gcm", "chacha20poly1305"];

/// Every index entry a sequential walk over every snapshot reaches: the
/// tree chunks and the chunk references of regular files.
fn sequential_references(fx: &Fixture) -> HashSet<ChunkId> {
    let mut repo = fx.open();
    let names: Vec<String> = repo
        .manifest()
        .snapshots
        .iter()
        .map(|e| e.name.clone())
        .collect();
    let mut found: HashSet<ChunkId> = HashSet::new();
    for name in &names {
        let meta = load_snapshot_meta(&repo, name).unwrap();
        for id in &meta.item_ptrs {
            if repo.chunk_index().contains(id) {
                found.insert(*id);
            }
        }
        let stream = load_snapshot_item_stream(&mut repo, name, &CancelToken::new()).unwrap();
        for_each_decoded_item(&stream, |item| {
            if item.entry_type == ItemType::RegularFile {
                for r in &item.chunks {
                    if repo.chunk_index().contains(&r.id) {
                        found.insert(r.id);
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    }
    found
}

fn assert_report_matches_the_oracle(fx: &Fixture) {
    let label = format!("{}-{}", fx.prefix, fx.mode);
    let oracle = sequential_references(fx);
    let entries: Vec<_> = fx.open().chunk_index().iter().collect();
    let mut zero: Vec<ChunkId> = entries
        .iter()
        .filter(|(id, _)| !oracle.contains(id))
        .map(|(id, _)| *id)
        .collect();
    zero.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    let report =
        commands::compact::report_unreferenced(&fx.config(), fx.passphrase(), &CancelToken::new())
            .unwrap();
    assert_eq!(report.snapshots, fx.snapshots.len(), "{label}");
    assert_eq!(report.entries, entries.len(), "{label}");
    assert_eq!(report.unreferenced, zero, "{label}");
}

fn check_fixtures(prefix: &str, version: u64) {
    for mode in MODES {
        let fx = fixtures::extract(prefix, mode, version);
        commands::upgrade::run(&fx.config(), fx.passphrase()).unwrap();
        assert_report_matches_the_oracle(&fx);
    }
}

#[test]
fn v2_fixtures_unreferenced_equals_a_sequential_walk() {
    check_fixtures("v2", 2);
}

#[test]
fn v3_fixtures_unreferenced_equals_a_sequential_walk() {
    check_fixtures("v3", 3);
}
