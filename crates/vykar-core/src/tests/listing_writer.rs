//! `ListingWriter`: the listing bytes are pinned. A fixed item sequence and
//! chunk-ID key give a fixed `item_ptrs`, so a change to the item
//! encoding, the cut rule or the writer shows up here. Recorded on the
//! writer that preceded `ListingWriter` (append_item_to_stream). Step 6a of
//! the snapshot-listing plan changes the encoding and must re-record it on purpose.

use std::sync::Arc;

use crate::commands::backup::ListingWriter;
use crate::commands::list::for_each_decoded_item;
use crate::compress::Compression;
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use crate::testutil::{test_chunk_hasher, test_repo_plaintext};
use vykar_types::chunk_id::ChunkId;

/// `item_ptrs` of [`fixed_items`] on [`fixed_key_repo`].
const KNOWN: &[&str] = &[
    "0a3db4199d4c60ca8a5a38ce4f9c5177c4657623194190e87aeac0acc94f08b2",
    "87ec4de225d350b1c3c7bbf6ff714b7e4b210f86323928a0971d890e045e2f8b",
    "b924f69ebe90bcd9f39b8ce07121e9661b1f9d8701e9a80b3edac82dfc429829",
    "ee8308cd8336438e653a05399fd110da7c3fde84201805b62b92aab233bc487c",
    "f002750bc686d22de881414c0d42f5c435ebd15e1cd263ecc1f16c3f7769f3c0",
    "f471ba0d9b676b5808cf565d17f4bbe9230e3ee177c3b7613d8f2dcda99002ec",
    "11cbd67cca22c90890ff56c68c86ab7ffbab0836b434a51605a8f77cf16f2189",
    "25036b98dd274c12fd3e75447cf7ea3af02418ddba617950f19be58ba21da9b8",
    "8529ad7fc3cdd85d9344cc1a0da22255b16bbd44490966da21e328f90079a169",
    "7f0479d45a0dd482fa22f9c2f06988094cee7a376606dd847d89fb05ba47f921",
    "0c8f89a172e2b49286a62f7cca4853659805843d810a59d335dc43c90d945ff3",
    "0ad17ccd527ab48d884f488290ac2c7fb7f9e0fb4e08dc39ed4e3094db38ff8b",
];

/// A plaintext in-memory repository whose chunk-ID key (and so the cut
/// key) is fixed: `test_repo_plaintext` draws a fresh key per init.
fn fixed_key_repo() -> Repository {
    let mut repo = test_repo_plaintext();
    repo.crypto = Arc::new(vykar_crypto::PlaintextEngine::new(test_chunk_hasher()));
    repo
}

fn item(path: String, entry_type: ItemType, mode: u32, chunks: Vec<ChunkRef>) -> Item {
    let size = chunks.iter().map(|c| u64::from(c.size)).sum();
    Item {
        path,
        entry_type,
        mode,
        uid: 1000,
        gid: 1000,
        user: None,
        group: None,
        mtime: 1_700_000_000_000_000_000,
        atime: None,
        ctime: None,
        size,
        chunks,
        link_target: None,
        xattrs: None,
        raw_names: None,
        hardlink: None,
    }
}

/// `n` chunk references with IDs derived from `*next`, which advances.
fn refs(next: &mut u64, n: usize) -> Vec<ChunkRef> {
    let hasher = test_chunk_hasher();
    (0..n)
        .map(|_| {
            let i = *next;
            *next += 1;
            ChunkRef {
                id: ChunkId::compute(&hasher, &i.to_le_bytes()),
                size: 1000 + (i % 7000) as u32,
                csize: 900 + (i % 5000) as u32,
            }
        })
        .collect()
}

/// Deterministic listing: a dumps-like directory, 40 directories of 60
/// files with 0..4 chunk references each, a symlink per directory, and one
/// regular file whose chunk list alone exceeds the 128 KiB forced cut.
fn fixed_items() -> Vec<Item> {
    let mut next = 0u64;
    let mut items = vec![item(
        "vykar-dumps".into(),
        ItemType::Directory,
        0o755,
        Vec::new(),
    )];
    for d in 0..40 {
        let dir = format!("dir-{d:02}");
        items.push(item(dir.clone(), ItemType::Directory, 0o755, Vec::new()));
        for f in 0..60 {
            let chunks = refs(&mut next, (d + f) % 5);
            items.push(item(
                format!("{dir}/file-{f:04}.txt"),
                ItemType::RegularFile,
                0o644,
                chunks,
            ));
        }
        let mut link = item(format!("{dir}/link"), ItemType::Symlink, 0o777, Vec::new());
        link.link_target = Some(format!("file-{d:04}.txt"));
        items.push(link);
        if d == 20 {
            let chunks = refs(&mut next, 4000);
            items.push(item(
                format!("{dir}/zz-huge.bin"),
                ItemType::RegularFile,
                0o644,
                chunks,
            ));
        }
    }
    items
}

/// The writer under test, applied to `items`.
fn write_listing(repo: &mut Repository, items: &[Item], compression: Compression) -> Vec<ChunkId> {
    let mut listing = ListingWriter::new(repo.crypto.chunk_hasher(), compression);
    for item in items {
        listing.push(repo, item).unwrap();
    }
    listing.finish(repo).unwrap().item_ptrs
}

fn hex(ptrs: &[ChunkId]) -> Vec<String> {
    ptrs.iter().map(|id| id.to_hex()).collect()
}

#[test]
fn known_item_ptrs() {
    let items = fixed_items();
    let ptrs = hex(&write_listing(
        &mut fixed_key_repo(),
        &items,
        Compression::None,
    ));
    assert!(ptrs.len() >= 8, "only {} tree chunks", ptrs.len());
    assert_eq!(ptrs, KNOWN);
    // The ID is over the plaintext. A fresh repository, because a dedup hit
    // returns before compressing.
    let compressed = hex(&write_listing(
        &mut fixed_key_repo(),
        &items,
        Compression::default(),
    ));
    assert_eq!(compressed, ptrs);
}

/// `ITEM_CUT_MIN` in `commands/backup/item_cut.rs`.
const MIN_CHUNK: usize = 8 * 1024;

#[test]
fn every_chunk_holds_whole_entries_and_concatenates() {
    let items = fixed_items();
    let mut repo = fixed_key_repo();
    let ptrs = write_listing(&mut repo, &items, Compression::None);
    repo.flush_packs().unwrap();
    repo.save_state().unwrap();

    let mut paths = Vec::new();
    let mut huge_ends_chunk = false;
    for (i, id) in ptrs.iter().enumerate() {
        let chunk = repo.read_chunk(id).unwrap();
        let mut last = None;
        for_each_decoded_item(&chunk, |item| {
            paths.push(item.path.clone());
            last = Some(item.path);
            Ok(())
        })
        .unwrap_or_else(|e| panic!("chunk {i} does not hold whole entries: {e}"));
        if i + 1 < ptrs.len() {
            assert!(
                chunk.len() >= MIN_CHUNK,
                "chunk {i} is {} bytes",
                chunk.len()
            );
        }
        huge_ends_chunk |= last.as_deref() == Some("dir-20/zz-huge.bin");
    }
    let pushed: Vec<String> = items.iter().map(|item| item.path.clone()).collect();
    assert_eq!(paths, pushed);
    assert!(
        huge_ends_chunk,
        "the oversized entry does not end its chunk"
    );
}

#[test]
fn empty_listing_has_no_item_ptrs() {
    let mut repo = fixed_key_repo();
    let listing = ListingWriter::new(repo.crypto.chunk_hasher(), Compression::None);
    assert!(listing.finish(&mut repo).unwrap().item_ptrs.is_empty());
}

#[test]
fn one_small_entry_is_one_chunk() {
    let items = &fixed_items()[..1];
    let bytes = rmp_serde::to_vec(&items[0]).unwrap();
    let ptrs = write_listing(&mut fixed_key_repo(), items, Compression::None);
    assert_eq!(ptrs, [ChunkId::compute(&test_chunk_hasher(), &bytes)]);
}
