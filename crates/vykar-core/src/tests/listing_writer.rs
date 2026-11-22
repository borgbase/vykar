//! `ListingWriter`: the listing bytes are pinned. A fixed item sequence and
//! chunk-ID key give a fixed `item_ptrs`, so a change to the item
//! encoding, the cut rule or the writer shows up here. `KNOWN_V1` was
//! recorded on the writer that preceded `ListingWriter` (append_item_to_stream)
//! and is now reproduced through the format-1 mirror encoder. `KNOWN_V2` was
//! recorded on purpose by 06 step 6a (chunk IDs as bin, snapshot format 2).

use std::sync::Arc;

use crate::commands::backup::ListingWriter;
use crate::commands::list::for_each_decoded_item;
use crate::compress::Compression;
use crate::repo::Repository;
use crate::snapshot::CURRENT_FORMAT_VERSION;
use crate::snapshot::item::{ChunkRef, Item, ItemType, v1_wire};
use crate::testutil::{test_chunk_hasher, test_repo_plaintext};
use vykar_types::chunk_id::ChunkId;

/// `item_ptrs` of [`fixed_items`] on [`fixed_key_repo`], format 1.
const KNOWN_V1: &[&str] = &[
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

/// `item_ptrs` of [`fixed_items`] on [`fixed_key_repo`], format 2.
const KNOWN_V2: &[&str] = &[
    "40b725ff9b71883e717e760f44212ce8da148f34674cf09be381c434b63e1186",
    "e4314a52611cb4016f7c3b00e4729fcd3bc8c789e84d9c41af95f9189849a1dd",
    "edd62ad09005f06143a4cb6818fec2677b9b5683eb96e935d0264114eaf3fb8e",
    "0920ca7efa32bca44290300ba268e0efb6eb7a87c4fcc98cadfa329632cafca7",
    "07a871f33d936f7c1113cf5729c17f866d175d14193566f59f1afbe0ec4fbdd5",
    "8e026295664a61f979f121729fe3569b3ba652ced4108d05f642c2cdae673138",
    "c79e49974abd4c94d5cac873a8983d2ef09530d4507d9362787ee7e75b0148e2",
    "44ab13f6f711cd2c9be700163a18a171d4fd3d20eade5e910760fd9311bd3573",
    "0c0cdf68f5be79e15b4eaf930b95ae6bebc69e32e50a71667fca216f8223c3ae",
    "e839fc824cf132010765d77e68ce2cdf28f68373b9c8734d5d805ec97e7acf67",
    "c517c8cd7b5f5ad82730177ff2779931a837799e3d06891e10035755a74e577a",
    "0910a8c914b971fad40887b06481148a39d9e326ee100ef47ab2a8db0f6119c6",
];

/// The writer under test, applied to `items`.
fn write_listing(repo: &mut Repository, items: &[Item], compression: Compression) -> Vec<ChunkId> {
    let mut listing = ListingWriter::new(repo.crypto.chunk_hasher(), compression);
    for item in items {
        listing.push(repo, item).unwrap();
    }
    let finished = listing.finish(repo).unwrap();
    assert_eq!(finished.format_version, CURRENT_FORMAT_VERSION);
    finished.item_ptrs
}

/// The writer fed format-1 entries (`v1_wire`): what the pre-6a writer
/// produced.
fn write_listing_v1(
    repo: &mut Repository,
    items: &[Item],
    compression: Compression,
) -> Vec<ChunkId> {
    let mut listing = ListingWriter::new(repo.crypto.chunk_hasher(), compression);
    let mut entry = Vec::new();
    for item in items {
        entry.clear();
        v1_wire::encode_item(item, &mut entry).unwrap();
        listing.push_encoded(repo, &item.path, &entry).unwrap();
    }
    listing.finish(repo).unwrap().item_ptrs
}

fn hex(ptrs: &[ChunkId]) -> Vec<String> {
    ptrs.iter().map(|id| id.to_hex()).collect()
}

#[test]
fn known_item_ptrs_v1() {
    let items = fixed_items();
    for compression in [Compression::None, Compression::default()] {
        let ptrs = hex(&write_listing_v1(
            &mut fixed_key_repo(),
            &items,
            compression,
        ));
        assert_eq!(ptrs, KNOWN_V1, "{compression:?}");
    }
}

#[test]
fn known_item_ptrs() {
    assert_eq!(CURRENT_FORMAT_VERSION, 2);
    let items = fixed_items();
    let ptrs = hex(&write_listing(
        &mut fixed_key_repo(),
        &items,
        Compression::None,
    ));
    assert!(ptrs.len() >= 8, "only {} tree chunks", ptrs.len());
    assert_eq!(ptrs, KNOWN_V2);
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

/// Decode each chunk of `ptrs` alone; the items of all chunks in order and
/// the plaintext bytes they took.
fn read_listing(repo: &mut Repository, ptrs: &[ChunkId]) -> (Vec<Item>, usize) {
    let mut items = Vec::new();
    let mut bytes = 0;
    for (i, id) in ptrs.iter().enumerate() {
        let chunk = repo.read_chunk(id).unwrap();
        bytes += chunk.len();
        for_each_decoded_item(&chunk, |item| {
            items.push(item);
            Ok(())
        })
        .unwrap_or_else(|e| panic!("chunk {i} does not decode alone: {e}"));
    }
    (items, bytes)
}

#[test]
fn v1_and_v2_listings_decode_to_the_same_items() {
    let items = fixed_items();
    let mut repo = fixed_key_repo();
    let v1 = write_listing_v1(&mut repo, &items, Compression::None);
    let v2 = write_listing(&mut repo, &items, Compression::None);
    repo.flush_packs().unwrap();
    repo.save_state().unwrap();
    assert_ne!(v1, v2);

    let (from_v1, v1_bytes) = read_listing(&mut repo, &v1);
    let (from_v2, v2_bytes) = read_listing(&mut repo, &v2);
    assert_eq!(from_v1, items);
    assert_eq!(from_v2, items);
    assert!(
        v2_bytes < v1_bytes,
        "format 2 ({v2_bytes} B) must be smaller than format 1 ({v1_bytes} B)"
    );
}

/// `begin_segment` closes a non-empty open chunk, so the segment starts at a
/// chunk boundary; on an empty buffer it adds no chunk. The key index
/// records the segments. Without a `begin_segment` the listing is one
/// implicit segment (the pinned listings above).
#[test]
fn begin_segment_cuts_and_records_segments() {
    use crate::commands::backup::item_cut::ItemStreamCutter;
    use crate::snapshot::listing::{NoIndex, ORDER_DFS_NAME, decode_ext};

    let mut repo = fixed_key_repo();
    // Never cuts on its own.
    let cutter = ItemStreamCutter::with_params([1; 32], usize::MAX, u64::MAX, usize::MAX);
    let mut listing = ListingWriter::with_cutter(cutter, Compression::None);
    let at = |path: &str| item(path.into(), ItemType::RegularFile, 0o644, Vec::new());
    listing
        .begin_segment(&mut repo, b"vykar-dumps".to_vec())
        .unwrap();
    listing.push(&mut repo, &at("vykar-dumps")).unwrap();
    listing.push(&mut repo, &at("vykar-dumps/a")).unwrap();
    listing.begin_segment(&mut repo, b"r".to_vec()).unwrap();
    // A second start on the now empty buffer adds no chunk and replaces the root.
    listing
        .begin_segment(&mut repo, b"home/u".to_vec())
        .unwrap();
    listing.push(&mut repo, &at("home/u")).unwrap();
    listing.push(&mut repo, &at("home/u/x")).unwrap();
    let finished = listing.finish(&mut repo).unwrap();
    assert_eq!(finished.item_ptrs.len(), 2);
    let index = decode_ext(finished.ext.as_deref(), 2).unwrap();
    assert_eq!(index.order, ORDER_DFS_NAME);
    let segments: Vec<(&[u8], u32)> = index
        .segments
        .iter()
        .map(|s| (s.root.as_slice(), s.first_ptr))
        .collect();
    assert_eq!(
        segments,
        vec![(&b"vykar-dumps"[..], 0), (&b"home/u"[..], 1)]
    );

    // No begin_segment: one implicit segment rooted at "".
    let mut repo = fixed_key_repo();
    let mut listing = ListingWriter::new(repo.crypto.chunk_hasher(), Compression::None);
    for path in ["a", "b", "a/x"] {
        listing.push(&mut repo, &at(path)).unwrap();
    }
    let finished = listing.finish(&mut repo).unwrap();
    let index = decode_ext(finished.ext.as_deref(), finished.item_ptrs.len()).unwrap();
    assert_eq!(index.segments.len(), 1);
    assert!(index.segments[0].root.is_empty());

    // The pinned listing is not in walker order ("dir-01" after
    // "dir-00/..."): order 0.
    let mut repo = fixed_key_repo();
    let mut listing = ListingWriter::new(repo.crypto.chunk_hasher(), Compression::None);
    for item in &fixed_items() {
        listing.push(&mut repo, item).unwrap();
    }
    let finished = listing.finish(&mut repo).unwrap();
    assert_eq!(
        decode_ext(finished.ext.as_deref(), finished.item_ptrs.len()),
        Err(NoIndex::Unordered)
    );
}
