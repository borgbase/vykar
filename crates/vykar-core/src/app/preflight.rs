//! Whether a cycle can reach a local repository.

use crate::config::ResolvedRepo;
use crate::storage::{ParsedUrl, parse_repo_url};

/// What a full cycle does with a local repository whose `config` is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnavailableRepo {
    /// Skip it after the backup step's `before` hooks (bare and scheduled cycles).
    Skip,
    /// Run normally and let the open fail (a repository selected with `-R`).
    Attempt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    /// A local repository whose `config` file exists.
    Available,
    /// A local repository whose `config` file is definitely missing.
    LocalMissing(String),
    /// Not a local path, or the check could not tell.
    Unknown,
}

impl Availability {
    /// Why a cycle skips this repository; `None` unless it is missing.
    pub fn unavailable_reason(&self) -> Option<String> {
        match self {
            Self::LocalMissing(path) => Some(format!(
                "repository not found at '{path}' (not mounted, or not initialized: run `vykar init`)"
            )),
            Self::Available | Self::Unknown => None,
        }
    }
}

/// A missing drive, an empty mount point and a never-initialized path all look
/// the same here: the repository's `config` file is not there.
pub fn availability(repo: &ResolvedRepo) -> Availability {
    let Ok(ParsedUrl::Local { path }) = parse_repo_url(&repo.config.repository.url) else {
        return Availability::Unknown;
    };
    match std::path::Path::new(&path).join("config").try_exists() {
        Ok(true) => Availability::Available,
        Ok(false) => Availability::LocalMissing(path),
        Err(_) => Availability::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::make_test_repo;

    fn repo_at(url: &str) -> ResolvedRepo {
        make_test_repo(url, None)
    }

    #[test]
    fn preflight_availability_classifies_local_paths() {
        let tmp = tempfile::tempdir().unwrap();

        let missing = tmp.path().join("volume").join("repo");
        let missing_url = missing.to_string_lossy().into_owned();
        let state = availability(&repo_at(&missing_url));
        assert_eq!(state, Availability::LocalMissing(missing_url.clone()));
        assert_eq!(
            state.unavailable_reason().as_deref(),
            Some(
                format!(
                    "repository not found at '{missing_url}' (not mounted, or not initialized: run `vykar init`)"
                )
                .as_str()
            )
        );

        let empty = tmp.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        assert!(matches!(
            availability(&repo_at(&empty.to_string_lossy())),
            Availability::LocalMissing(_)
        ));

        let present = tmp.path().join("present");
        std::fs::create_dir(&present).unwrap();
        std::fs::write(present.join("config"), b"x").unwrap();
        let state = availability(&repo_at(&present.to_string_lossy()));
        assert_eq!(state, Availability::Available);
        assert_eq!(state.unavailable_reason(), None);

        for url in ["s3://s3.example.com/bucket/repo", "sftp://user@host/repo"] {
            let state = availability(&repo_at(url));
            assert_eq!(state, Availability::Unknown, "{url}");
            assert_eq!(state.unavailable_reason(), None);
        }
    }
}
