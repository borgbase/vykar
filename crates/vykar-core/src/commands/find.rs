use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use globset::GlobMatcher;

use crate::config::VykarConfig;
use crate::repo::OpenOptions;
use crate::snapshot::item::{Item, ItemType};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

use super::list;

/// Filter criteria (all fields are AND-combined).
///
/// `*_glob` patterns containing a `/` are auto-routed to the corresponding
/// `*_path_glob` field at build time and matched against the full item path
/// (with `*` not crossing `/`; use `**` to span directories). Patterns
/// without `/` keep the historical filename-only semantics.
pub struct FindFilter {
    pub path_prefix: Option<String>,
    pub name_glob: Option<GlobMatcher>,
    pub iname_glob: Option<GlobMatcher>,
    pub path_glob: Option<GlobMatcher>,
    pub ipath_glob: Option<GlobMatcher>,
    pub item_type: Option<ItemType>,
    pub since: Option<DateTime<Utc>>,
    pub larger_than: Option<u64>,
    pub smaller_than: Option<u64>,
}

impl FindFilter {
    /// Build a FindFilter from string arguments, handling glob compilation.
    pub fn build(
        path_prefix: Option<String>,
        name: Option<&str>,
        iname: Option<&str>,
        item_type: Option<ItemType>,
        since: Option<DateTime<Utc>>,
        larger_than: Option<u64>,
        smaller_than: Option<u64>,
    ) -> std::result::Result<Self, String> {
        let (name_glob, path_glob) = compile_pattern(name, false, "--name")?;
        let (iname_glob, ipath_glob) = compile_pattern(iname, true, "--iname")?;

        Ok(Self {
            path_prefix,
            name_glob,
            iname_glob,
            path_glob,
            ipath_glob,
            item_type,
            since,
            larger_than,
            smaller_than,
        })
    }
}

/// Compile a user pattern into either a filename glob or a path glob.
/// Returns `(filename_glob, path_glob)` — exactly one is `Some` when `pat` is `Some`.
fn compile_pattern(
    pat: Option<&str>,
    case_insensitive: bool,
    arg_label: &str,
) -> std::result::Result<(Option<GlobMatcher>, Option<GlobMatcher>), String> {
    let Some(pat) = pat else {
        return Ok((None, None));
    };

    let mut builder = globset::GlobBuilder::new(pat);
    builder.case_insensitive(case_insensitive);

    if pat.contains('/') {
        // Path pattern: anchor `*` to a single path segment so users can write
        // `cyberpunk/*` and `**/IMG_*` with conventional shell-glob semantics.
        builder.literal_separator(true);
        let glob = builder
            .build()
            .map(|g| g.compile_matcher())
            .map_err(|e| format!("invalid {arg_label} glob: {e}"))?;
        Ok((None, Some(glob)))
    } else {
        let glob = builder
            .build()
            .map(|g| g.compile_matcher())
            .map_err(|e| format!("invalid {arg_label} glob: {e}"))?;
        Ok((Some(glob), None))
    }
}

/// Which snapshots to search.
pub struct FindScope {
    pub source_label: Option<String>,
    pub last_n: Option<usize>,
}

/// One match in one snapshot.
pub struct FindHit {
    pub snapshot_name: String,
    pub snapshot_time: DateTime<Utc>,
    pub size: u64,
    pub mtime: i64,
    pub entry_type: ItemType,
    pub chunk_ids: Vec<ChunkId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    Added,
    Modified,
    Unchanged,
}

pub struct AnnotatedHit {
    pub hit: FindHit,
    pub status: FileStatus,
}

pub struct PathTimeline {
    pub path: String,
    pub hits: Vec<AnnotatedHit>,
}

/// Run the find command: search for files across snapshots.
/// Tries the local restore cache first to avoid downloading the full index.
/// Falls back to the full index (with blob cache) on cache miss.
pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    scope: &FindScope,
    filter: &FindFilter,
) -> Result<Vec<PathTimeline>> {
    let (mut repo, _session_guard) =
        super::util::open_repo_with_read_session(config, passphrase, OpenOptions::new())?;

    // Select and sort snapshots chronologically
    let mut entries: Vec<_> = repo.manifest().snapshots.clone();

    if let Some(ref source) = scope.source_label {
        entries.retain(|e| &e.source_label == source);
    }

    entries.sort_by_key(|a| a.time);

    if let Some(n) = scope.last_n {
        let len = entries.len();
        if n < len {
            entries.drain(..len - n);
        }
    }

    // Try restore cache first
    let mut restore_cache = repo.open_restore_cache();
    let mut index_loaded = false;

    // Collect hits per path
    let mut hits_by_path: BTreeMap<String, Vec<FindHit>> = BTreeMap::new();

    for entry in &entries {
        let snapshot_name = entry.name.clone();
        let snapshot_time = entry.time;

        // Try restore cache path, fall back to full index
        let items_stream = if let Some(ref cache) = restore_cache {
            match list::load_snapshot_item_stream_via_lookup(&mut repo, &snapshot_name, |id| {
                cache.lookup(id)
            }) {
                Ok(s) => s,
                Err(_) => {
                    // Restore cache incomplete or stale — fall back to full index
                    restore_cache = None;
                    repo.load_chunk_index()?;
                    index_loaded = true;
                    list::load_snapshot_item_stream(&mut repo, &snapshot_name)?
                }
            }
        } else {
            if !index_loaded {
                repo.load_chunk_index()?;
                index_loaded = true;
            }
            list::load_snapshot_item_stream(&mut repo, &snapshot_name)?
        };

        list::for_each_decoded_item(&items_stream, |item| {
            if matches_filter(&item, filter) {
                let chunk_ids: Vec<ChunkId> = item.chunks.iter().map(|c| c.id).collect();
                hits_by_path
                    .entry(item.path.clone())
                    .or_default()
                    .push(FindHit {
                        snapshot_name: snapshot_name.clone(),
                        snapshot_time,
                        size: item.size,
                        mtime: item.mtime,
                        entry_type: item.entry_type,
                        chunk_ids,
                    });
            }
            Ok(())
        })?;
    }

    // Compute statuses and build timelines
    let timelines: Vec<PathTimeline> = hits_by_path
        .into_iter()
        .map(|(path, hits)| {
            let annotated = annotate_hits(hits);
            PathTimeline {
                path,
                hits: annotated,
            }
        })
        .collect();

    Ok(timelines)
}

fn matches_filter(item: &Item, filter: &FindFilter) -> bool {
    // Type filter (cheapest)
    if let Some(ref t) = filter.item_type {
        if &item.entry_type != t {
            return false;
        }
    }

    // Path prefix
    if let Some(ref prefix) = filter.path_prefix {
        if item.path != *prefix && !item.path.starts_with(&format!("{prefix}/")) {
            return false;
        }
    }

    // Size filters
    if let Some(min) = filter.larger_than {
        if item.size < min {
            return false;
        }
    }
    if let Some(max) = filter.smaller_than {
        if item.size > max {
            return false;
        }
    }

    // Since filter (mtime in nanos)
    if let Some(ref since) = filter.since {
        let item_secs = item.mtime / 1_000_000_000;
        if item_secs < since.timestamp() {
            return false;
        }
    }

    // Name glob (case-sensitive, filename-only)
    if let Some(ref glob) = filter.name_glob {
        let filename = item.path.rsplit('/').next().unwrap_or(&item.path);
        if !glob.is_match(filename) {
            return false;
        }
    }

    // Name glob (case-insensitive, filename-only)
    if let Some(ref glob) = filter.iname_glob {
        let filename = item.path.rsplit('/').next().unwrap_or(&item.path);
        if !glob.is_match(filename) {
            return false;
        }
    }

    // Path glob (case-sensitive, full path)
    if let Some(ref glob) = filter.path_glob {
        if !glob.is_match(&item.path) {
            return false;
        }
    }

    // Path glob (case-insensitive, full path)
    if let Some(ref glob) = filter.ipath_glob {
        if !glob.is_match(&item.path) {
            return false;
        }
    }

    true
}

fn annotate_hits(hits: Vec<FindHit>) -> Vec<AnnotatedHit> {
    let mut annotated = Vec::with_capacity(hits.len());
    let mut prev_chunk_ids: Option<&[ChunkId]> = None;
    let mut prev_size: Option<u64> = None;

    // We need to keep references to previous chunk_ids, so build incrementally
    for (i, hit) in hits.into_iter().enumerate() {
        let status = if i == 0 {
            FileStatus::Added
        } else if prev_chunk_ids == Some(&hit.chunk_ids) && prev_size == Some(hit.size) {
            FileStatus::Unchanged
        } else {
            FileStatus::Modified
        };

        annotated.push(AnnotatedHit { hit, status });

        let last = annotated.last().expect("invariant: just pushed");
        prev_chunk_ids = Some(&last.hit.chunk_ids);
        prev_size = Some(last.hit.size);
    }

    annotated
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::item::{Item, ItemType};

    fn make_item(path: &str, size: u64, mtime: i64) -> Item {
        Item {
            path: path.into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime,
            atime: None,
            ctime: None,
            size,
            chunks: vec![],
            link_target: None,
            xattrs: None,
        }
    }

    fn empty_filter() -> FindFilter {
        FindFilter {
            path_prefix: None,
            name_glob: None,
            iname_glob: None,
            path_glob: None,
            ipath_glob: None,
            item_type: None,
            since: None,
            larger_than: None,
            smaller_than: None,
        }
    }

    #[test]
    fn filter_by_type() {
        let item = make_item("foo.txt", 100, 0);
        let mut filter = empty_filter();
        filter.item_type = Some(ItemType::Directory);
        assert!(!matches_filter(&item, &filter));

        filter.item_type = Some(ItemType::RegularFile);
        assert!(matches_filter(&item, &filter));
    }

    #[test]
    fn filter_by_path_prefix() {
        let item = make_item("docs/readme.md", 50, 0);
        let mut filter = empty_filter();
        filter.path_prefix = Some("docs".into());
        assert!(matches_filter(&item, &filter));

        filter.path_prefix = Some("src".into());
        assert!(!matches_filter(&item, &filter));
    }

    #[test]
    fn filter_by_size() {
        let item = make_item("file.bin", 500, 0);
        let mut filter = empty_filter();
        filter.larger_than = Some(100);
        assert!(matches_filter(&item, &filter));

        filter.larger_than = Some(1000);
        assert!(!matches_filter(&item, &filter));

        filter.larger_than = None;
        filter.smaller_than = Some(1000);
        assert!(matches_filter(&item, &filter));

        filter.smaller_than = Some(100);
        assert!(!matches_filter(&item, &filter));
    }

    #[test]
    fn filter_by_name_glob() {
        let item = make_item("src/main.rs", 100, 0);
        let mut filter = empty_filter();
        let glob = globset::GlobBuilder::new("*.rs")
            .build()
            .unwrap()
            .compile_matcher();
        filter.name_glob = Some(glob);
        assert!(matches_filter(&item, &filter));

        let glob = globset::GlobBuilder::new("*.txt")
            .build()
            .unwrap()
            .compile_matcher();
        filter.name_glob = Some(glob);
        assert!(!matches_filter(&item, &filter));
    }

    #[test]
    fn filter_by_iname_glob() {
        let item = make_item("src/Main.RS", 100, 0);
        let mut filter = empty_filter();
        let glob = globset::GlobBuilder::new("*.rs")
            .case_insensitive(true)
            .build()
            .unwrap()
            .compile_matcher();
        filter.iname_glob = Some(glob);
        assert!(matches_filter(&item, &filter));
    }

    #[test]
    fn build_routes_slash_pattern_to_path_glob() {
        let filter =
            FindFilter::build(None, Some("cyberpunk/**"), None, None, None, None, None).unwrap();
        assert!(filter.name_glob.is_none());
        assert!(filter.path_glob.is_some());

        let item = make_item("cyberpunk/sub/file.txt", 10, 0);
        assert!(matches_filter(&item, &filter));

        let item = make_item("other/file.txt", 10, 0);
        assert!(!matches_filter(&item, &filter));
    }

    #[test]
    fn build_keeps_filename_only_pattern_as_name_glob() {
        let filter = FindFilter::build(None, Some("*.dng"), None, None, None, None, None).unwrap();
        assert!(filter.name_glob.is_some());
        assert!(filter.path_glob.is_none());

        let item = make_item("Pictures/2026/IMG.dng", 10, 0);
        assert!(matches_filter(&item, &filter));
    }

    #[test]
    fn path_glob_star_does_not_cross_separator() {
        // `cyberpunk/*` should match direct children but not recurse.
        let filter =
            FindFilter::build(None, Some("cyberpunk/*"), None, None, None, None, None).unwrap();

        let direct = make_item("cyberpunk/file.txt", 10, 0);
        assert!(matches_filter(&direct, &filter));

        let nested = make_item("cyberpunk/sub/file.txt", 10, 0);
        assert!(!matches_filter(&nested, &filter));
    }

    #[test]
    fn path_glob_double_star_spans_directories() {
        let filter =
            FindFilter::build(None, Some("**/IMG_570*"), None, None, None, None, None).unwrap();

        let nested = make_item("Pictures/2026/IMG_5701.dng", 10, 0);
        assert!(matches_filter(&nested, &filter));

        let other = make_item("Pictures/2026/other.dng", 10, 0);
        assert!(!matches_filter(&other, &filter));
    }

    #[test]
    fn ipath_glob_is_case_insensitive() {
        let filter =
            FindFilter::build(None, None, Some("**/img_*"), None, None, None, None).unwrap();
        assert!(filter.ipath_glob.is_some());

        let upper = make_item("Pictures/IMG_001.jpg", 10, 0);
        assert!(matches_filter(&upper, &filter));
    }

    #[test]
    fn annotate_detects_added_modified_unchanged() {
        let id_a = ChunkId::from_bytes([1u8; 32]);
        let id_b = ChunkId::from_bytes([2u8; 32]);
        let now = Utc::now();

        let hits = vec![
            FindHit {
                snapshot_name: "s1".into(),
                snapshot_time: now,
                size: 100,
                mtime: 0,
                entry_type: ItemType::RegularFile,
                chunk_ids: vec![id_a],
            },
            FindHit {
                snapshot_name: "s2".into(),
                snapshot_time: now,
                size: 100,
                mtime: 0,
                entry_type: ItemType::RegularFile,
                chunk_ids: vec![id_a],
            },
            FindHit {
                snapshot_name: "s3".into(),
                snapshot_time: now,
                size: 200,
                mtime: 0,
                entry_type: ItemType::RegularFile,
                chunk_ids: vec![id_b],
            },
        ];

        let annotated = annotate_hits(hits);
        assert_eq!(annotated[0].status, FileStatus::Added);
        assert_eq!(annotated[1].status, FileStatus::Unchanged);
        assert_eq!(annotated[2].status, FileStatus::Modified);
    }
}
