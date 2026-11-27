//! `vykar upgrade`: move a repository to the current format in place, or
//! switch its commit mode (`--commit-mode`).

use std::sync::Arc;

use super::util::{ExclusiveSession, open_repo, with_exclusive_session_as};
use crate::config::VykarConfig;
use crate::repo::control::CancelToken;
use crate::repo::{LegacyUpgradeLock, OpenOptions, Repo, RepoWriter, UpgradeOutcome, WriterMode};
use vykar_types::error::Result;

/// Upgrade a v2/v3 repository to format v6 (keeping its hash) under the
/// legacy lock key and an exclusive session, or complete an interrupted
/// upgrade as an exclusive session.
/// The only command that opens a v2/v3 repository; it opens it without its
/// index, and the upgrade reads the legacy index itself.
pub fn run(config: &VykarConfig, passphrase: Option<&str>) -> Result<UpgradeOutcome> {
    run_with_mode(config, passphrase, None)
}

/// [`run`] with `--commit-mode`: on a v2/v3 repository the mode the upgrade
/// records (`Multi` refuses unless the storage passes the init-time check;
/// `None`: the check decides); on a v6 repository a switch (`Multi`: the
/// check must pass; `Single`: no check). Same exclusion and session refusal.
pub fn run_with_mode(
    config: &VykarConfig,
    passphrase: Option<&str>,
    mode: Option<WriterMode>,
) -> Result<UpgradeOutcome> {
    let opts = OpenOptions::new().legacy_format();
    // Upgrade has no token.
    let mut repo = open_repo(config, passphrase, opts, &CancelToken::new())?;
    upgrade_repo(&mut repo, mode)
}

/// Upgrade (or switch the mode of) a repository opened with
/// [`OpenOptions::legacy_format`]: a v2/v3 one under the legacy lock key
/// (10-single-protocol step 6) and an exclusive session, a v6 one as an
/// exclusive session (step 8). Both refuse while any other session is live
/// (the exclusive registration). Public for the interrupted-upgrade tests.
#[doc(hidden)]
pub fn upgrade_repo(repo: &mut Repo, mode: Option<WriterMode>) -> Result<UpgradeOutcome> {
    let action = |w: &mut RepoWriter<'_>, _: &ExclusiveSession| w.upgrade_index_layout_with(mode);
    if repo.format().is_upgrade_only() {
        with_legacy_upgrade_lock(repo, action)
    } else {
        with_exclusive_session_as(repo, None, action)
    }
}

/// Acquire the legacy lock key first, then run `action` as an exclusive
/// session whose write check also runs the key's check
/// (`with_exclusive_session_as`), then release the key. The key's PUT
/// comes before the exclusive registration's listing of `sessions/`: a 0.24
/// backup registers its marker and then lists `locks/` only, so of the two
/// one sees the other. The session deregisters before the key is released:
/// the reverse of acquisition.
///
/// A failed release is `tracing::warn!`-only, with the leaked-lock
/// sentence after a successful action (as `clear_fence_and_release`).
fn with_legacy_upgrade_lock<T>(
    repo: &mut Repo,
    action: impl FnOnce(&mut RepoWriter<'_>, &ExclusiveSession) -> Result<T>,
) -> Result<T> {
    let raw = Arc::clone(repo.storage());
    let lock = LegacyUpgradeLock::acquire(Arc::clone(&raw), repo.host_env())?;

    let result = with_exclusive_session_as(repo, Some(lock.check_fn()), action);

    let key = lock.key().to_string();
    if let Err(release_err) = lock.release() {
        if result.is_ok() {
            tracing::warn!(
                "the operation completed successfully, but releasing the repository lock \
                 failed: {release_err}. The advisory lock at `{key}` may persist; future \
                 operations on this repository may be blocked for up to 6 hours until \
                 automatic stale-lock cleanup, or run `vykar break-lock` to clear it \
                 manually."
            );
        } else {
            tracing::warn!("failed to release repository lock: {release_err}");
        }
    }
    result
}
