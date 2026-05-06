use vykar_core::config::ResolvedRepo;

use crate::error::CliResult;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn repo_display_name(repo: &ResolvedRepo) -> &str {
    repo.label.as_deref().unwrap_or(&repo.config.repository.url)
}

/// Result of probing multiple repos for a snapshot name.
pub(crate) enum SnapshotDispatch {
    /// "latest" is ambiguous across repos — caller must specify --repo.
    RequireRepo,
    /// Snapshot not found in any repo (and all probes succeeded).
    NotFound,
    /// Exactly one repo contains the snapshot.
    Unique(usize),
    /// Multiple repos contain the snapshot.
    Ambiguous(Vec<usize>),
    /// At least one probe failed — we can't be sure of the result.
    ProbeError {
        matches: Vec<usize>,
        errors: Vec<(usize, String)>,
    },
}

/// Classify where a snapshot lives across multiple repos.
/// Pure decision logic — no I/O side effects beyond the lightweight probes.
pub(crate) fn classify_snapshot_target(snap: &str, repos: &[&ResolvedRepo]) -> SnapshotDispatch {
    if snap.eq_ignore_ascii_case("latest") {
        return SnapshotDispatch::RequireRepo;
    }

    let mut matches: Vec<usize> = Vec::new();
    let mut errors: Vec<(usize, String)> = Vec::new();

    for (i, repo) in repos.iter().enumerate() {
        match probe_snapshot(&repo.config, repo.label.as_deref(), snap) {
            Ok(true) => matches.push(i),
            Ok(false) => {}
            Err(e) => errors.push((i, e.to_string())),
        }
    }

    if !errors.is_empty() {
        return SnapshotDispatch::ProbeError { matches, errors };
    }

    match matches.as_slice() {
        [] => SnapshotDispatch::NotFound,
        [only] => SnapshotDispatch::Unique(*only),
        _ => SnapshotDispatch::Ambiguous(matches),
    }
}

/// Probe whether a repo's manifest contains a snapshot (lightweight open).
fn probe_snapshot(
    config: &vykar_core::config::VykarConfig,
    label: Option<&str>,
    snapshot_name: &str,
) -> CliResult<bool> {
    with_repo_passphrase(config, label, |passphrase| {
        let repo = vykar_core::commands::util::open_repo(
            config,
            passphrase,
            vykar_core::OpenOptions::new(),
        )?;
        Ok(repo.manifest().find_snapshot(snapshot_name).is_some())
    })
}

/// Result of probing multiple repos for two snapshot names (snapshot diff).
pub(crate) enum DiffDispatch {
    /// Both snapshots found uniquely in the same repo.
    Unique(usize),
    /// One of the snapshots is missing in every probed repo.
    SnapshotNotFound { snapshot: String },
    /// Snapshots resolve to different repos.
    DifferentRepos { a_repo: String, b_repo: String },
    /// One of the snapshots matches multiple repos with no unique pairing.
    Ambiguous { snapshot: String, repos: Vec<usize> },
    /// "latest" is meaningless across repos.
    LatestRequiresRepo,
    /// At least one probe failed.
    ProbeError { errors: Vec<(usize, String)> },
}

/// Classify where two snapshots live across multiple repos.
pub(crate) fn classify_diff_target(
    snap_a: &str,
    snap_b: &str,
    repos: &[&ResolvedRepo],
) -> DiffDispatch {
    if snap_a.eq_ignore_ascii_case("latest") || snap_b.eq_ignore_ascii_case("latest") {
        return DiffDispatch::LatestRequiresRepo;
    }

    let mut matches_a: Vec<usize> = Vec::new();
    let mut matches_b: Vec<usize> = Vec::new();
    let mut errors: Vec<(usize, String)> = Vec::new();

    for (i, repo) in repos.iter().enumerate() {
        let a = probe_snapshot(&repo.config, repo.label.as_deref(), snap_a);
        let b = probe_snapshot(&repo.config, repo.label.as_deref(), snap_b);
        match (a, b) {
            (Ok(found_a), Ok(found_b)) => {
                if found_a {
                    matches_a.push(i);
                }
                if found_b {
                    matches_b.push(i);
                }
            }
            (Err(e), _) | (_, Err(e)) => errors.push((i, e.to_string())),
        }
    }

    if !errors.is_empty() {
        return DiffDispatch::ProbeError { errors };
    }

    if matches_a.is_empty() {
        return DiffDispatch::SnapshotNotFound {
            snapshot: snap_a.to_string(),
        };
    }
    if matches_b.is_empty() {
        return DiffDispatch::SnapshotNotFound {
            snapshot: snap_b.to_string(),
        };
    }

    // Classify by intersection: the diff can run iff there is exactly one
    // repo containing both snapshots, even if one name is also present in
    // other repos that lack the other snapshot.
    let intersection: Vec<usize> = matches_a
        .iter()
        .copied()
        .filter(|i| matches_b.contains(i))
        .collect();
    match intersection.as_slice() {
        [unique] => DiffDispatch::Unique(*unique),
        [] => {
            // matches_a/matches_b are non-empty (checked above), so .first() is Some.
            let a_idx = *matches_a.first().expect("matches_a non-empty");
            let b_idx = *matches_b.first().expect("matches_b non-empty");
            let a_repo = repo_display_name(
                repos
                    .get(a_idx)
                    .copied()
                    .expect("diff repo index for snapshot A is valid"),
            )
            .to_string();
            let b_repo = repo_display_name(
                repos
                    .get(b_idx)
                    .copied()
                    .expect("diff repo index for snapshot B is valid"),
            )
            .to_string();
            DiffDispatch::DifferentRepos { a_repo, b_repo }
        }
        _ => {
            // Both snapshot names collide in multiple repos. Report the
            // narrower of the two name's match sets so the user can see
            // exactly where the ambiguity is.
            let (snapshot, ambiguous_in) = if matches_a.len() <= matches_b.len() {
                (snap_a, intersection)
            } else {
                (snap_b, intersection)
            };
            DiffDispatch::Ambiguous {
                snapshot: snapshot.to_string(),
                repos: ambiguous_in,
            }
        }
    }
}
