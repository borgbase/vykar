//! Content-defined cut points for the item stream (tree chunks).
//!
//! A tree chunk ends after an entry whose keyed path hash falls under a
//! threshold proportional to the entry's encoded length, subject to minimum
//! and maximum chunk sizes. Because the decision depends on the entry and not
//! on its stream offset, an inserted or removed entry only changes the chunks
//! around it, and later chunks deduplicate against the previous snapshot.
//! Backup and the `check --repair` listing rewriter both cut with this rule.

use vykar_types::chunk_id::ChunkHasher;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Minimum tree-chunk size: a boundary entry is ignored until the chunk holds this many bytes.
const ITEM_CUT_MIN: usize = 8 * 1024;
/// Expected bytes between candidate boundaries beyond the minimum (the boundary "rate").
const ITEM_CUT_RATE: u64 = 24 * 1024;
/// Forced cut once the chunk holds this many bytes (it may exceed this by one entry).
const ITEM_CUT_MAX: usize = 128 * 1024;
/// BLAKE3 derive_key context for the boundary key. Changing it moves every cut.
const ITEM_CUT_KEY_CONTEXT: &str = "vykar 2026-11-19 item stream cut points v1";

/// Decides where a tree chunk ends. A chunk ends after an entry, never inside one.
///
/// No `Debug`: the key is derived from the (possibly secret) chunk-ID key.
#[derive(Zeroize, ZeroizeOnDrop)]
pub(crate) struct ItemStreamCutter {
    key: [u8; 32],
    #[zeroize(skip)]
    min: usize,
    #[zeroize(skip)]
    rate: u64,
    #[zeroize(skip)]
    max: usize,
}

impl ItemStreamCutter {
    pub(crate) fn new(chunk_hasher: &ChunkHasher) -> Self {
        Self {
            key: blake3::derive_key(ITEM_CUT_KEY_CONTEXT, chunk_hasher.key()),
            min: ITEM_CUT_MIN,
            rate: ITEM_CUT_RATE,
            max: ITEM_CUT_MAX,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_params(key: [u8; 32], min: usize, rate: u64, max: usize) -> Self {
        Self {
            key,
            min,
            rate,
            max,
        }
    }

    /// `entry_len`: encoded length of the entry just appended; `chunk_len`: buffer length after it.
    pub(crate) fn should_cut(&self, path: &str, entry_len: usize, chunk_len: usize) -> bool {
        chunk_len >= self.max || (chunk_len >= self.min && self.is_boundary(path, entry_len))
    }

    /// True with probability `min(1, entry_len / rate)`, decided by a keyed hash of `path`.
    fn is_boundary(&self, path: &str, entry_len: usize) -> bool {
        let digest = blake3::keyed_hash(&self.key, path.as_bytes());
        let mut first = [0u8; 8];
        first.copy_from_slice(&digest.as_bytes()[..8]);
        let h = u64::from_le_bytes(first);
        (h as u128) * (self.rate as u128) < (entry_len as u128) << 64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use vykar_types::hash::HashAlgorithm;

    const KEY: [u8; 32] = [9u8; 32];

    fn paths(n: usize) -> impl Iterator<Item = String> {
        (0..n).map(|i| format!("dir-{:02}/file-{i:05}", i % 37))
    }

    #[test]
    fn never_cuts_below_min() {
        let c = ItemStreamCutter::with_params(KEY, 1000, 1, 10_000);
        for p in paths(100) {
            assert!(!c.should_cut(&p, 10, 999));
            assert!(c.should_cut(&p, 10, 1000));
        }
    }

    #[test]
    fn forces_cut_at_max() {
        let c = ItemStreamCutter::with_params(KEY, 1000, u64::MAX, 10_000);
        for p in paths(1000) {
            assert!(!c.should_cut(&p, 10, 9_999));
            assert!(c.should_cut(&p, 10, 10_000));
        }
    }

    #[test]
    fn entry_at_least_rate_is_always_boundary() {
        let rate = 4096u64;
        let c = ItemStreamCutter::with_params(KEY, 0, rate, usize::MAX);
        for p in paths(1000) {
            assert!(c.is_boundary(&p, rate as usize));
            assert!(c.is_boundary(&p, rate as usize * 3));
            assert!(!c.is_boundary(&p, 0));
        }
    }

    #[test]
    fn boundary_depends_on_key() {
        let a = ItemStreamCutter::new(&ChunkHasher::new(HashAlgorithm::Blake3, [1; 32]));
        let b = ItemStreamCutter::new(&ChunkHasher::new(HashAlgorithm::Blake3, [2; 32]));
        let set = |c: &ItemStreamCutter| -> HashSet<String> {
            paths(10_000).filter(|p| c.is_boundary(p, 4096)).collect()
        };
        let (sa, sb) = (set(&a), set(&b));
        assert!(!sa.is_empty());
        assert!(!sb.is_empty());
        assert_ne!(sa, sb);
    }

    // Changing any of these values moves every cut and costs every repository
    // one backup's worth of tree-chunk sharing.
    #[test]
    fn known_answer() {
        let c = ItemStreamCutter::new(&ChunkHasher::new(HashAlgorithm::Blake3, [7u8; 32]));
        assert_eq!(hex::encode(&c.key[..8]), "a8ae32a96965d628");

        let cases: [(&str, usize); 8] = [
            ("etc/passwd", 200),
            ("etc/passwd", 12_000),
            ("home/user/.bashrc", 200),
            ("home/user/.bashrc", 12_000),
            ("var/log/syslog", 200),
            ("var/log/syslog", 12_000),
            ("usr/bin/env", 200),
            ("usr/bin/env", 12_000),
        ];
        let got: Vec<bool> = cases.iter().map(|&(p, n)| c.is_boundary(p, n)).collect();
        assert_eq!(got, [false, true, false, true, false, true, false, false]);

        // Recompute two cases directly so the frozen values are not purely circular.
        let key = blake3::derive_key("vykar 2026-11-19 item stream cut points v1", &[7u8; 32]);
        let direct = |path: &[u8], n: u128| {
            let d = blake3::keyed_hash(&key, path);
            let h = u64::from_le_bytes(d.as_bytes()[..8].try_into().unwrap());
            (h as u128) * (24 * 1024) < n << 64
        };
        assert!(direct(b"home/user/.bashrc", 12_000));
        assert!(!direct(b"usr/bin/env", 12_000));
    }

    /// Deterministic synthetic listing: `(path, encoded_len)` per entry.
    fn synthetic_entries(n: usize, seed: u64) -> Vec<(String, usize)> {
        let mut x = seed;
        (0..n)
            .map(|i| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (
                    format!("d{:03}/f{:06}", i / 500, i),
                    150 + (x % 151) as usize,
                )
            })
            .collect()
    }

    type Chunk = Vec<(String, usize)>;

    fn cut_with(
        entries: &[(String, usize)],
        cut: impl Fn(&str, usize, usize) -> bool,
    ) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        let mut cur: Chunk = Vec::new();
        let mut len = 0usize;
        for (p, n) in entries {
            cur.push((p.clone(), *n));
            len += n;
            if cut(p, *n, len) {
                chunks.push(std::mem::take(&mut cur));
                len = 0;
            }
        }
        if !cur.is_empty() {
            chunks.push(cur);
        }
        chunks
    }

    fn cut_new(c: &ItemStreamCutter, entries: &[(String, usize)]) -> Vec<Chunk> {
        cut_with(entries, |p, n, l| c.should_cut(p, n, l))
    }

    fn cut_old(entries: &[(String, usize)]) -> Vec<Chunk> {
        cut_with(entries, |_, _, l| l >= 128 * 1024)
    }

    fn count_new(base: &[Chunk], variant: &[Chunk]) -> usize {
        let base: HashSet<&Chunk> = base.iter().collect();
        variant.iter().filter(|c| !base.contains(c)).count()
    }

    const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

    fn real_cutter() -> ItemStreamCutter {
        ItemStreamCutter::new(&ChunkHasher::new(HashAlgorithm::Blake3, [3u8; 32]))
    }

    #[test]
    fn realigns_after_insert_remove_and_resize() {
        let c = real_cutter();
        let base_entries = synthetic_entries(50_000, SEED);
        let base = cut_new(&c, &base_entries);

        let mut inserted = base_entries.clone();
        inserted.insert(25_000, ("d050/f025000a".to_string(), 220));
        let n = count_new(&base, &cut_new(&c, &inserted));
        assert!(n <= 3, "insert: {n} new chunks (seed {SEED:#x})");

        let mut removed = base_entries.clone();
        removed.remove(25_000);
        let n = count_new(&base, &cut_new(&c, &removed));
        assert!(n <= 3, "remove: {n} new chunks (seed {SEED:#x})");

        let mut resized = base_entries.clone();
        resized[25_000].1 += 3;
        let n = count_new(&base, &cut_new(&c, &resized));
        assert!(n <= 2, "resize: {n} new chunks (seed {SEED:#x})");

        // Scattered inserts: the new rule realigns after each, the old
        // fixed-size threshold shifts every later boundary.
        let mut scattered = Vec::with_capacity(base_entries.len() + 10);
        for (i, e) in base_entries.iter().enumerate() {
            if i % 5000 == 2500 {
                scattered.push((format!("new/{i:06}"), 220));
            }
            scattered.push(e.clone());
        }
        let variant = cut_new(&c, &scattered);
        let n = count_new(&base, &variant);
        assert!(
            n <= 20 && n * 100 <= variant.len() * 15,
            "scattered (new rule): {n} of {} chunks new (seed {SEED:#x})",
            variant.len()
        );

        let old_base = cut_old(&base_entries);
        let old_variant = cut_old(&scattered);
        let n = count_new(&old_base, &old_variant);
        assert!(
            n * 2 >= old_variant.len(),
            "scattered (old rule): {n} of {} chunks new (seed {SEED:#x})",
            old_variant.len()
        );
    }

    #[test]
    fn mean_chunk_size_near_target() {
        let c = real_cutter();
        let entries = synthetic_entries(50_000, SEED);
        let chunks = cut_new(&c, &entries);
        let sizes: Vec<usize> = chunks
            .iter()
            .map(|ch| ch.iter().map(|(_, n)| n).sum())
            .collect();
        let total: usize = sizes.iter().sum();
        let mean = total / sizes.len();
        assert!(
            (24 * 1024..=40 * 1024).contains(&mean),
            "mean chunk size {mean} over {} chunks",
            sizes.len()
        );
        let largest_entry = entries.iter().map(|(_, n)| *n).max().unwrap();
        for (i, &s) in sizes.iter().enumerate() {
            if i + 1 < sizes.len() {
                assert!(s >= ITEM_CUT_MIN, "chunk {i} is {s} bytes");
            }
            assert!(s <= ITEM_CUT_MAX + largest_entry, "chunk {i} is {s} bytes");
        }
    }
}
