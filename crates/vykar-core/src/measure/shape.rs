//! Directory shape of a listing: entries per directory, depth, bytes per
//! directory and sibling order. Paths use `/` as the separator.

use std::collections::HashMap;

/// `ITEM_CUT_MAX` in `commands/backup/item_cut.rs`.
const OVER_TREE_CHUNK_MAX: u64 = 128 * 1024;
const OVER_1M: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct Percentiles {
    pub count: u64,
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
    pub sum: u64,
}

/// Nearest-rank percentiles over `values` (sorted in place). Empty gives all
/// zero. The rank for q is `((len - 1) * q) / 100`, in integers.
pub fn percentiles(values: &mut [u64]) -> Percentiles {
    values.sort_unstable();
    let len = values.len();
    if len == 0 {
        return Percentiles::default();
    }
    let rank = |q: usize| values.get((len - 1) * q / 100).copied().unwrap_or(0);
    Percentiles {
        count: len as u64,
        p50: rank(50),
        p99: rank(99),
        max: values.last().copied().unwrap_or(0),
        sum: values.iter().sum(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Dir,
    Symlink,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ShapeReport {
    pub entries: u64,
    /// Directory items plus parents never seen as items (the root, segment roots).
    pub directories: u64,
    pub files: u64,
    pub symlinks: u64,
    /// Direct children of every directory, empty ones included.
    pub entries_per_dir: Percentiles,
    /// Path components per entry.
    pub depth: Percentiles,
    /// Sum of the encoded lengths of each directory's direct children; `None`
    /// when bytes are not tracked.
    pub bytes_per_dir: Option<Percentiles>,
    /// Directories whose direct children exceed the maximum tree-chunk size.
    pub dirs_over_128k: Option<u64>,
    pub dirs_over_1m: Option<u64>,
    /// Stream-adjacent entry pairs with the same parent.
    pub adjacent_siblings: u64,
    /// Of those, pairs whose name decreases in byte order.
    pub name_order_violations: u64,
}

/// Accumulates [`ShapeReport`] over a stream of entries.
pub struct DirShape {
    track_bytes: bool,
    /// Parent path -> (direct children, their encoded bytes).
    dirs: HashMap<Vec<u8>, (u64, u64)>,
    depths: Vec<u64>,
    files: u64,
    symlinks: u64,
    adjacent_siblings: u64,
    name_order_violations: u64,
    prev: Vec<u8>,
    has_prev: bool,
}

fn split_parent(path: &[u8]) -> (&[u8], &[u8]) {
    match path.iter().rposition(|b| *b == b'/') {
        Some(i) => (
            path.get(..i).unwrap_or(b""),
            path.get(i + 1..).unwrap_or(b""),
        ),
        None => (b"", path),
    }
}

impl DirShape {
    pub fn new(track_bytes: bool) -> Self {
        Self {
            track_bytes,
            dirs: HashMap::new(),
            depths: Vec::new(),
            files: 0,
            symlinks: 0,
            adjacent_siblings: 0,
            name_order_violations: 0,
            prev: Vec::new(),
            has_prev: false,
        }
    }

    /// `path`: snapshot path bytes. The parent is everything before the last
    /// `/`, or `b""` for a top-level entry.
    pub fn push(&mut self, path: &[u8], kind: EntryKind, encoded_len: u64) {
        let (parent, name) = split_parent(path);
        let slot = self.dirs.entry(parent.to_vec()).or_default();
        slot.0 += 1;
        if self.track_bytes {
            slot.1 += encoded_len;
        }
        match kind {
            EntryKind::Dir => {
                self.dirs.entry(path.to_vec()).or_default();
            }
            EntryKind::File => self.files += 1,
            EntryKind::Symlink => self.symlinks += 1,
        }
        self.depths.push(path.split(|b| *b == b'/').count() as u64);

        if self.has_prev {
            let (prev_parent, prev_name) = split_parent(&self.prev);
            if prev_parent == parent {
                self.adjacent_siblings += 1;
                if name < prev_name {
                    self.name_order_violations += 1;
                }
            }
        }
        self.prev.clear();
        self.prev.extend_from_slice(path);
        self.has_prev = true;
    }

    pub fn finish(mut self) -> ShapeReport {
        let mut counts: Vec<u64> = self.dirs.values().map(|v| v.0).collect();
        let (bytes_per_dir, over_max, over_1m) = if self.track_bytes {
            let mut bytes: Vec<u64> = self.dirs.values().map(|v| v.1).collect();
            let over_max = bytes.iter().filter(|b| **b > OVER_TREE_CHUNK_MAX).count() as u64;
            let over_1m = bytes.iter().filter(|b| **b > OVER_1M).count() as u64;
            (Some(percentiles(&mut bytes)), Some(over_max), Some(over_1m))
        } else {
            (None, None, None)
        };
        ShapeReport {
            entries: self.depths.len() as u64,
            directories: self.dirs.len() as u64,
            files: self.files,
            symlinks: self.symlinks,
            entries_per_dir: percentiles(&mut counts),
            depth: percentiles(&mut self.depths),
            bytes_per_dir,
            dirs_over_128k: over_max,
            dirs_over_1m: over_1m,
            adjacent_siblings: self.adjacent_siblings,
            name_order_violations: self.name_order_violations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_nearest_rank() {
        assert_eq!(percentiles(&mut []), Percentiles::default());

        let p = percentiles(&mut [5]);
        assert_eq!((p.p50, p.p99, p.max, p.count, p.sum), (5, 5, 5, 1, 5));

        let mut v: Vec<u64> = (1..=100).rev().collect();
        let p = percentiles(&mut v);
        assert_eq!((p.p50, p.p99, p.max), (50, 99, 100));
        assert_eq!((p.count, p.sum), (100, 5050));
    }

    #[test]
    fn dir_shape_counts() {
        let mut s = DirShape::new(true);
        s.push(b"a", EntryKind::Dir, 10);
        s.push(b"a/x", EntryKind::File, 20);
        s.push(b"a/y", EntryKind::File, 30);
        s.push(b"b", EntryKind::Dir, 10);
        s.push(b"c", EntryKind::File, 5);
        let r = s.finish();
        assert_eq!(r.entries, 5);
        assert_eq!(r.directories, 3);
        assert_eq!(r.files, 3);
        // Children: root 3, a 2, b 0.
        assert_eq!(r.entries_per_dir.max, 3);
        assert_eq!(r.entries_per_dir.sum, 5);
        assert_eq!(r.entries_per_dir.count, 3);
        assert_eq!(r.depth.max, 2);
        let bytes = r.bytes_per_dir.unwrap();
        assert_eq!(bytes.max, 50, "a sums its children");
        assert_eq!(bytes.sum, 75);
        assert_eq!(r.dirs_over_128k, Some(0));
        assert_eq!(r.name_order_violations, 0);
        // (a/x, a/y) and (b, c) are adjacent siblings; (a/y, b) is not.
        assert_eq!(r.adjacent_siblings, 2);

        let mut s = DirShape::new(false);
        s.push(b"a", EntryKind::Dir, 0);
        s.push(b"a/y", EntryKind::File, 0);
        s.push(b"a/x", EntryKind::File, 0);
        let r = s.finish();
        assert_eq!(r.name_order_violations, 1);
        assert_eq!(r.bytes_per_dir, None);
    }
}
