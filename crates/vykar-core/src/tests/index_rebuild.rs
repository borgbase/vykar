//! Rebuilding the index from the packs (`check --repair` when the index
//! cannot be read), pack maps, and the trailer checks of `check
//! --verify-data`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::helpers::{backup_single_source_with, make_test_config};
use crate::commands;
use crate::commands::check::{
    IndexLoss, IndexRecovery, RebuildCounts, RepairAction, RepairMode, RepairPlan, RepairResult,
};
use crate::config::{EncryptionModeConfig, VykarConfig};
use crate::repo::control::CancelToken;
use crate::repo::format::ObjectType;
use crate::repo::index_store::INDEX_OBJECT_DIR;
use crate::repo::pack_map::{PackMap, PackMapEntry, pack_map_key};
use crate::repo::pack_trailer::{parse_footer, read_trailer_from_pack_bytes, trailer_frame_start};
use crate::repo::{OpenOptions, Repository};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;
use vykar_types::pack_id::PackId;

struct Fx {
    tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    source: PathBuf,
    config: VykarConfig,
    /// The source tree as each snapshot saw it.
    trees: BTreeMap<String, BTreeMap<String, Vec<u8>>>,
}

impl Fx {
    fn new() -> Self {
        Self::with_encryption(false)
    }

    /// A repository, AES-GCM encrypted when `encrypted`.
    fn with_encryption(encrypted: bool) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let source = tmp.path().join("src");
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&source).unwrap();
        let mut config = make_test_config(&repo_dir);
        if encrypted {
            config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
            config.encryption.passphrase = Some("index-rebuild".into());
        }
        commands::init::run(&config, pass(&config)).unwrap();
        config.cache_dir = Some(tmp.path().join("cache").to_string_lossy().into_owned());
        Self {
            tmp,
            repo_dir,
            source,
            config,
            trees: BTreeMap::new(),
        }
    }

    fn write(&self, name: &str, data: &[u8]) {
        std::fs::write(self.source.join(name), data).unwrap();
    }

    fn backup(&mut self, name: &str) {
        backup_single_source_with(&self.config, pass(&self.config), &self.source, "src", name);
        self.trees.insert(name.to_string(), read_tree(&self.source));
    }

    /// The same repository from a machine without a cache.
    fn cold(&self, name: &str) -> VykarConfig {
        let mut config = self.config.clone();
        config.cache_dir = Some(self.tmp.path().join(name).to_string_lossy().into_owned());
        config
    }

    fn cache_dir(&self) -> PathBuf {
        PathBuf::from(self.config.cache_dir.as_ref().unwrap())
    }

    fn open(&self) -> Repository {
        commands::util::open_repo(
            &self.config,
            pass(&self.config),
            OpenOptions::new().with_index(),
            &CancelToken::new(),
        )
        .unwrap()
    }

    /// Fold the index into a base, so this machine holds its table
    /// (`index_base.*`).
    fn fold(&self) {
        let mut repo = self.open();
        commands::util::with_maintenance_lock(&mut repo, |repo| {
            repo.request_index_fold();
            repo.mark_index_dirty();
            repo.save_state()
        })
        .unwrap();
    }

    /// Delete every head and every object in `index.d/`.
    fn delete_index(&self) {
        super::helpers::remove_index_heads(&self.repo_dir);
        for entry in std::fs::read_dir(self.repo_dir.join(INDEX_OBJECT_DIR)).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                std::fs::remove_file(path).unwrap();
            }
        }
    }

    fn packs(&self) -> Vec<PathBuf> {
        files_below(&self.repo_dir.join("packs"))
    }

    fn pack_maps(&self) -> usize {
        files_below(&self.repo_dir.join("packmaps")).len()
    }

    /// The pack holding `file`'s first chunk in the latest index.
    fn pack_of_largest(&self) -> PathBuf {
        let repo = self.open();
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for (_, e) in repo.chunk_index().iter() {
            *counts.entry(e.pack_id.storage_key()).or_default() += 1;
        }
        let key = counts.into_iter().max_by_key(|(_, n)| *n).unwrap().0;
        self.repo_dir.join(key)
    }

    /// The ID of the pack at `path` (below the repository).
    fn pack_id_of(&self, path: &Path) -> PackId {
        PackId::from_storage_key(path.strip_prefix(&self.repo_dir).unwrap().to_str().unwrap())
            .unwrap()
    }

    /// The index entries of `pack_id`, in offset order.
    fn entries_of(&self, pack_id: PackId) -> Vec<PackMapEntry> {
        let mut entries: Vec<PackMapEntry> = self
            .open()
            .chunk_index()
            .iter()
            .filter(|(_, e)| e.pack_id == pack_id)
            .map(|(chunk_id, e)| PackMapEntry {
                chunk_id,
                offset: u32::try_from(e.pack_offset).unwrap(),
                stored_size: e.stored_size,
            })
            .collect();
        entries.sort_by_key(|e| e.offset);
        entries
    }

    /// Store a pack map of `packs` as the repository's crypto seals it.
    fn put_pack_map(&self, packs: Vec<(PackId, Vec<PackMapEntry>)>) {
        let crypto = std::sync::Arc::clone(&self.open().crypto);
        let (name, stored) = PackMap { packs }.seal(crypto.as_ref()).unwrap();
        let path = self.repo_dir.join(pack_map_key(&name));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, stored).unwrap();
    }

    /// Restore `snapshot` through `config` and compare it with the tree the
    /// backup saw.
    fn assert_restores(&self, config: &VykarConfig, snapshot: &str) {
        let dest = self
            .tmp
            .path()
            .join(format!("restore-{snapshot}-{}", rand_suffix()));
        commands::restore::run(
            config,
            pass(config),
            snapshot,
            dest.to_str().unwrap(),
            None,
            false,
            true,
            &CancelToken::new(),
        )
        .unwrap();
        let restored = read_tree(&find_dir(&dest, "a.bin"));
        assert_eq!(&restored, &self.trees[snapshot], "{snapshot}");
    }

    /// Every file in the repository and its bytes.
    fn state(&self) -> BTreeMap<String, Vec<u8>> {
        read_tree(&self.repo_dir)
    }
}

fn rand_suffix() -> u64 {
    rand::random()
}

fn files_below(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(files_below(&path));
        } else {
            out.push(path);
        }
    }
    out.sort();
    out
}

fn read_tree(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    files_below(dir)
        .into_iter()
        .map(|p| {
            let rel = p.strip_prefix(dir).unwrap().to_string_lossy().into_owned();
            let bytes = std::fs::read(&p).unwrap();
            (rel, bytes)
        })
        .collect()
}

/// The directory below `dir` that holds a file called `name`.
fn find_dir(dir: &Path, name: &str) -> PathBuf {
    files_below(dir)
        .into_iter()
        .find(|p| p.file_name().is_some_and(|n| n == name))
        .and_then(|p| p.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| panic!("{name} not found below {}", dir.display()))
}

/// Deterministic bytes.
fn data(seed: u8, len: usize) -> Vec<u8> {
    let mut x = u64::from(seed).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// The passphrase of an encrypted test repository, carried in its config.
fn pass(config: &VykarConfig) -> Option<&str> {
    config.encryption.passphrase.as_deref()
}

fn repair(config: &VykarConfig, verify_data: bool, mode: RepairMode) -> Result<RepairResult> {
    commands::check::run_with_repair(
        config,
        pass(config),
        verify_data,
        mode,
        None,
        &CancelToken::new(),
    )
}

fn rebuilt(plan: &RepairPlan) -> (IndexLoss, RebuildCounts) {
    match plan.index_recovery {
        IndexRecovery::RebuiltFromPacks { cause, counts } => (cause, counts),
        other => panic!("not rebuilt: {other:?}"),
    }
}

fn write_pack_maps_of(plan: &RepairPlan) -> Option<&[PackId]> {
    plan.actions.iter().find_map(|a| match a {
        RepairAction::WritePackMaps { pack_ids } => Some(pack_ids.as_slice()),
        _ => None,
    })
}

fn check_clean(config: &VykarConfig) {
    let result = commands::check::run(config, pass(config), true, false).unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

/// Where the trailer frame of the pack at `path` starts.
fn frame_start(bytes: &[u8]) -> usize {
    let len = parse_footer(bytes).unwrap();
    trailer_frame_start(bytes.len() as u64, len).unwrap() as usize
}

fn edit_pack(path: &Path, f: impl FnOnce(&mut Vec<u8>, usize)) {
    let mut bytes = std::fs::read(path).unwrap();
    let start = frame_start(&bytes);
    f(&mut bytes, start);
    std::fs::write(path, &bytes).unwrap();
}

/// Cut the trailer off, as if the pack had been written before trailers.
fn cut_trailer(path: &Path) {
    edit_pack(path, |b, start| b.truncate(start));
}

/// Three backups sharing a large file, then `s1` deleted and a compact at
/// threshold 0, so a pack written by compact exists.
fn three_backups_and_a_compact(encrypted: bool) -> Fx {
    let mut fx = Fx::with_encryption(encrypted);
    fx.write("a.bin", &data(1, 300_000));
    fx.write("b.bin", &data(2, 50_000));
    fx.backup("s1");
    fx.write("b.bin", &data(3, 60_000));
    fx.write("c.bin", &data(4, 70_000));
    fx.backup("s2");
    fx.write("d.bin", &data(5, 80_000));
    fx.backup("s3");
    let p = pass(&fx.config);
    commands::delete::run(&fx.config, p, &["s1"], false, &CancelToken::new()).unwrap();
    fx.trees.remove("s1");
    let stats =
        commands::compact::run(&fx.config, p, 0.0, None, false, &CancelToken::new()).unwrap();
    assert!(stats.packs_repacked > 0, "{stats:?}");
    fx
}

/// **The done criterion of the rebuild.** With `index`, every object in
/// `index.d/` and the client cache gone, `check --repair` on a cold client
/// rebuilds the index from the pack trailers (one of them written by
/// compact), loses nothing, and the repository works as before.
#[test]
fn repair_restores_every_snapshot_after_index_and_index_d_are_deleted() {
    restores_every_snapshot_after_the_index_is_deleted(false);
}

/// The done criterion on an encrypted repository: the trailers are read
/// through the AEAD.
#[test]
fn repair_restores_every_snapshot_of_an_encrypted_repository() {
    restores_every_snapshot_after_the_index_is_deleted(true);
}

fn restores_every_snapshot_after_the_index_is_deleted(encrypted: bool) {
    let fx = three_backups_and_a_compact(encrypted);
    let before = {
        let repo = fx.open();
        let mut v: Vec<String> = repo
            .chunk_index()
            .iter()
            .map(|(id, e)| format!("{id:?} {:?} {} {}", e.pack_id, e.pack_offset, e.stored_size))
            .collect();
        v.sort();
        v
    };
    fx.delete_index();
    std::fs::remove_dir_all(fx.cache_dir()).unwrap();

    let cold = fx.cold("cold");
    let result = repair(&cold, false, RepairMode::Apply).unwrap();
    let (cause, counts) = rebuilt(&result.plan);
    assert_eq!(cause, IndexLoss::HeadMissing);
    assert_eq!(counts.packs, fx.packs().len());
    assert_eq!(counts.from_trailers, counts.packs);
    assert_eq!(counts.uncovered, 0);
    assert!(!result.plan.has_data_loss, "{:?}", result.plan.actions);
    assert!(
        result.repair_errors.is_empty(),
        "{:?}",
        result.repair_errors
    );

    let after = {
        let repo = commands::util::open_repo(
            &cold,
            pass(&cold),
            OpenOptions::new().with_index(),
            &CancelToken::new(),
        )
        .unwrap();
        let mut v: Vec<String> = repo
            .chunk_index()
            .iter()
            .map(|(id, e)| format!("{id:?} {:?} {} {}", e.pack_id, e.pack_offset, e.stored_size))
            .collect();
        v.sort();
        v
    };
    assert_eq!(after, before);
    for snapshot in ["s2", "s3"] {
        fx.assert_restores(&cold, snapshot);
    }
    check_clean(&cold);
    backup_single_source_with(&cold, pass(&cold), &fx.source, "src", "s4");
    commands::compact::run(&cold, pass(&cold), 0.0, None, false, &CancelToken::new()).unwrap();
    check_clean(&cold);
}

/// A trailer whose frame is damaged is reported by `check --verify-data`
/// (and nothing else is), repair writes a pack map for the pack without
/// data loss, after which check is clean and a further repair plans no map;
/// a later rebuild of the index takes the pack from the map.
#[test]
fn verify_data_reports_a_damaged_trailer_and_repair_maps_the_pack() {
    let mut fx = Fx::new();
    fx.write("a.bin", &data(1, 200_000));
    fx.write("b.bin", &data(2, 20_000));
    fx.backup("s1");
    let pack = fx.pack_of_largest();
    let pack_id = fx.pack_id_of(&pack);
    // Not the footer: the pack type byte, after the length prefix, the
    // envelope tag, the codec tag and the body version.
    edit_pack(&pack, |b, start| b[start + 7] = 0xFF);

    let result = commands::check::run(&fx.config, None, true, false).unwrap();
    assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
    assert!(
        result.errors[0].message.contains("trailer"),
        "{:?}",
        result.errors
    );
    assert!(
        result.errors[0].message.contains(&pack_id.to_string()),
        "{:?}",
        result.errors
    );

    let shown = repair(&fx.config, true, RepairMode::PlanOnly).unwrap().plan;
    assert!(!shown.has_data_loss, "{:?}", shown.actions);
    assert_eq!(write_pack_maps_of(&shown), Some(&[pack_id][..]));
    let applied = repair(&fx.config, true, RepairMode::Apply).unwrap();
    assert!(
        applied.repair_errors.is_empty(),
        "{:?}",
        applied.repair_errors
    );
    assert_eq!(fx.pack_maps(), 1);

    let result = commands::check::run(&fx.config, None, true, true).unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    let again = repair(&fx.config, true, RepairMode::PlanOnly).unwrap().plan;
    assert_eq!(write_pack_maps_of(&again), None, "{:?}", again.actions);

    fx.delete_index();
    let cold = fx.cold("cold");
    let result = repair(&cold, false, RepairMode::Apply).unwrap();
    let (_, counts) = rebuilt(&result.plan);
    assert_eq!(counts.from_maps, 1);
    assert_eq!(counts.uncovered, 0);
    assert!(!result.plan.has_data_loss, "{:?}", result.plan.actions);
    check_clean(&cold);
    fx.assert_restores(&cold, "s1");
}

/// A valid trailer that does not list an indexed chunk where the index puts
/// it is reported.
#[test]
fn verify_data_reports_a_trailer_that_disagrees_with_the_index() {
    let mut fx = Fx::new();
    fx.write("a.bin", &data(1, 200_000));
    fx.backup("s1");
    let pack = fx.pack_of_largest();
    let crypto = std::sync::Arc::clone(&fx.open().crypto);
    let mut bytes = std::fs::read(&pack).unwrap();
    let start = frame_start(&bytes);
    let mut trailer = read_trailer_from_pack_bytes(&bytes, crypto.as_ref())
        .unwrap()
        .unwrap();
    trailer.entries[0].chunk_id = vykar_types::chunk_id::ChunkId::from_bytes([0x5A; 32]);
    let blob = trailer.seal_blob(crypto.as_ref()).unwrap();
    bytes.truncate(start + 4);
    bytes.extend_from_slice(&blob);
    std::fs::write(&pack, &bytes).unwrap();

    let result = commands::check::run(&fx.config, None, true, false).unwrap();
    assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
    assert!(
        result.errors[0].message.contains("lists 1 of"),
        "{:?}",
        result.errors
    );
}

/// A pack without a trailer (one written before trailers) is taken from the
/// base table of the machine that has one (REB-2), and repair maps it, so a
/// cold machine's later rebuild takes it from the map.
#[test]
fn repair_maps_packs_recovered_from_a_local_table() {
    let mut fx = Fx::new();
    fx.write("a.bin", &data(1, 200_000));
    fx.backup("s1");
    fx.fold();
    let pack = fx.pack_of_largest();
    cut_trailer(&pack);
    fx.delete_index();

    let shown = repair(&fx.config, false, RepairMode::PlanOnly)
        .unwrap()
        .plan;
    let (_, counts) = rebuilt(&shown);
    assert_eq!(counts.from_local_tables, 1);
    assert_eq!(counts.uncovered, 0);
    assert!(!shown.has_data_loss, "{:?}", shown.actions);
    assert_eq!(write_pack_maps_of(&shown).map(<[PackId]>::len), Some(1));
    let applied = repair(&fx.config, false, RepairMode::Apply).unwrap();
    assert!(
        applied.repair_errors.is_empty(),
        "{:?}",
        applied.repair_errors
    );
    assert_eq!(fx.pack_maps(), 1);

    fx.delete_index();
    let cold = fx.cold("cold");
    let plan = repair(&cold, false, RepairMode::PlanOnly).unwrap().plan;
    let (_, counts) = rebuilt(&plan);
    assert_eq!(counts.from_maps, 1);
    assert_eq!((counts.from_local_tables, counts.uncovered), (0, 0));
    assert!(!plan.has_data_loss, "{:?}", plan.actions);
}

/// On a cold machine a pack without trailer or map is uncovered: the plan
/// says so and removes the snapshot, and an apply that was shown a plan
/// without data loss refuses and changes nothing.
#[test]
fn repair_with_an_uncovered_pack_shows_the_loss() {
    let mut fx = Fx::new();
    fx.write("a.bin", &data(1, 200_000));
    fx.backup("s1");
    cut_trailer(&fx.pack_of_largest());
    fx.delete_index();

    let cold = fx.cold("cold");
    let shown = repair(&cold, false, RepairMode::PlanOnly).unwrap().plan;
    let (_, counts) = rebuilt(&shown);
    assert_eq!(counts.uncovered, 1);
    assert_eq!(shown.uncovered_packs.len(), 1);
    assert_eq!(
        shown.uncovered_packs[0].reason,
        "no trailer and no pack map"
    );
    assert!(shown.has_data_loss, "{:?}", shown.actions);

    let before = fx.state();
    let err = repair(
        &cold,
        false,
        RepairMode::ApplyShown {
            has_data_loss: false,
            index_recovery: shown.index_recovery,
        },
    )
    .err()
    .unwrap()
    .to_string();
    assert!(err.contains("removes data"), "{err}");
    assert_eq!(fx.state(), before);
}

/// A trailer of a newer body version (in a pack that matches its ID) and an
/// object of an unknown type under `packmaps/` come from a newer vykar: the
/// rebuild refuses, naming the object, and nothing is changed.
#[test]
fn rebuild_refuses_objects_of_a_newer_vykar() {
    for newer_map in [false, true] {
        let mut fx = Fx::new();
        fx.write("a.bin", &data(1, 100_000));
        fx.backup("s1");
        let key = if newer_map {
            let mut object = vec![ObjectType::SweepRecord as u8 + 1];
            object.extend_from_slice(b"from the future");
            let name = blake3::hash(&object).to_hex().to_string();
            let dir = fx.repo_dir.join("packmaps");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(&name), &object).unwrap();
            format!("packmaps/{name}")
        } else {
            // The body version: after the length prefix and both tags. The
            // pack is renamed to the ID of its new bytes, as a newer writer
            // would have named it.
            let hash = fx.open().content_hash();
            let pack = fx.pack_of_largest();
            edit_pack(&pack, |b, start| b[start + 6] = 2);
            let bytes = std::fs::read(&pack).unwrap();
            let key = PackId::compute(&bytes, hash).storage_key();
            let renamed = fx.repo_dir.join(&key);
            std::fs::create_dir_all(renamed.parent().unwrap()).unwrap();
            std::fs::rename(&pack, &renamed).unwrap();
            key
        };
        fx.delete_index();
        let before = fx.state();
        let cold = fx.cold("cold");
        for mode in [RepairMode::PlanOnly, RepairMode::Apply] {
            let err = repair(&cold, false, mode).err().unwrap().to_string();
            assert!(
                err.contains("written by a newer vykar"),
                "{newer_map}: {err}"
            );
            assert!(err.contains(&key), "{newer_map}: {err}");
            assert!(err.contains("Nothing was changed"), "{newer_map}: {err}");
            assert_eq!(fx.state(), before, "{newer_map}");
        }
    }
}

/// Without encryption one flipped byte makes a trailer read as a newer
/// version. The pack no longer matches its ID, so it is damaged: uncovered,
/// and the repair goes on.
#[test]
fn rebuild_treats_a_rotted_trailer_version_as_damage() {
    let mut fx = Fx::new();
    fx.write("a.bin", &data(1, 200_000));
    fx.backup("s1");
    let pack = fx.pack_of_largest();
    let pack_id = fx.pack_id_of(&pack);
    edit_pack(&pack, |b, start| b[start + 6] = 2);
    fx.delete_index();

    let cold = fx.cold("cold");
    let shown = repair(&cold, false, RepairMode::PlanOnly).unwrap().plan;
    let (_, counts) = rebuilt(&shown);
    assert_eq!(counts.uncovered, 1);
    assert_eq!(shown.uncovered_packs[0].pack_id, pack_id);
    assert!(
        shown.uncovered_packs[0]
            .reason
            .contains("does not match its ID"),
        "{:?}",
        shown.uncovered_packs
    );
    let applied = repair(&cold, false, RepairMode::Apply).unwrap();
    assert_eq!(rebuilt(&applied.plan).1.uncovered, 1);
}

/// A pack whose trailer is damaged and whose map lacks one indexed entry is
/// mapped again by repair, after which a full check is clean and a second
/// plan writes no map.
#[test]
fn repair_maps_a_pack_whose_map_is_partial() {
    let mut fx = Fx::new();
    fx.write("a.bin", &data(1, 200_000));
    fx.write("b.bin", &data(2, 20_000));
    fx.backup("s1");
    let pack = fx.pack_of_largest();
    let pack_id = fx.pack_id_of(&pack);
    let mut entries = fx.entries_of(pack_id);
    assert!(entries.len() > 1, "{entries:?}");
    entries.pop();
    fx.put_pack_map(vec![(pack_id, entries)]);
    edit_pack(&pack, |b, start| b[start + 7] = 0xFF);

    let applied = repair(&fx.config, true, RepairMode::Apply).unwrap();
    assert!(
        applied.repair_errors.is_empty(),
        "{:?}",
        applied.repair_errors
    );
    assert_eq!(write_pack_maps_of(&applied.plan), Some(&[pack_id][..]));
    assert_eq!(fx.pack_maps(), 2);

    let result = commands::check::run(&fx.config, None, true, true).unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    let again = repair(&fx.config, true, RepairMode::PlanOnly).unwrap().plan;
    assert_eq!(write_pack_maps_of(&again), None, "{:?}", again.actions);
}

/// After repair removes a corrupt chunk from a pack that keeps other
/// entries (and had no map), a rebuild of the index does not bring the
/// chunk back: the pack's map, which the repair wrote, wins over its
/// trailer.
#[test]
fn rebuild_after_removing_a_corrupt_chunk_leaves_it_out() {
    let mut fx = Fx::new();
    for (i, name) in ["a.bin", "b.bin", "c.bin"].into_iter().enumerate() {
        fx.write(name, &data(i as u8 + 1, 20_000));
    }
    fx.backup("s1");
    // `s2` keeps the chunks of b.bin and c.bin, so the pack keeps entries
    // when `s1` goes with the chunk of a.bin.
    std::fs::remove_file(fx.source.join("a.bin")).unwrap();
    fx.backup("s2");
    let pack = fx.pack_of_largest();
    let pack_id = fx.pack_id_of(&pack);
    let entries = fx.entries_of(pack_id);
    assert!(entries.len() > 1, "{entries:?}");
    assert_eq!(fx.pack_maps(), 0);
    // The chunk of a.bin: in the pack, and not referenced by `s2`.
    let mut repo = fx.open();
    let in_s2: std::collections::HashSet<ChunkId> =
        commands::list::load_snapshot_items(&mut repo, "s2", &CancelToken::new())
            .unwrap()
            .iter()
            .flat_map(|i| i.chunks.iter().map(|c| c.id))
            .collect();
    let (corrupt, offset) = repo
        .chunk_index()
        .iter()
        .find(|(id, e)| e.pack_id == pack_id && !in_s2.contains(id))
        .map(|(id, e)| (id, e.pack_offset))
        .unwrap();
    drop(repo);
    let mut bytes = std::fs::read(&pack).unwrap();
    bytes[offset as usize + 1] ^= 0xFF;
    std::fs::write(&pack, &bytes).unwrap();

    let applied = repair(&fx.config, true, RepairMode::Apply).unwrap();
    assert!(
        applied.repair_errors.is_empty(),
        "{:?}",
        applied.repair_errors
    );
    assert!(
        applied.applied.iter().any(|a| matches!(
            a,
            RepairAction::RemoveCorruptChunks { pack_id: p, chunk_ids }
                if *p == pack_id && chunk_ids == &[corrupt]
        )),
        "{:?}",
        applied.applied
    );
    assert_eq!(fx.pack_maps(), 1);

    fx.delete_index();
    let cold = fx.cold("cold");
    let result = repair(&cold, false, RepairMode::Apply).unwrap();
    assert_eq!(rebuilt(&result.plan).1.from_maps, 1);
    let repo = commands::util::open_repo(
        &cold,
        None,
        OpenOptions::new().with_index(),
        &CancelToken::new(),
    )
    .unwrap();
    let kept: Vec<ChunkId> = repo
        .chunk_index()
        .iter()
        .filter(|(_, e)| e.pack_id == pack_id)
        .map(|(id, _)| id)
        .collect();
    assert!(!kept.contains(&corrupt), "{kept:?}");
    assert_eq!(kept.len(), entries.len() - 1);
}
