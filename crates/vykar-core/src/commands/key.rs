//! `vykar key export` and `vykar key import`.
//!
//! Both work on **raw storage**, without `Repository::open`. `import` has to:
//! it runs on a repository that can no longer be opened, which is exactly when
//! a key needs restoring. `export` must too: a recovery tool for the key cannot
//! be allowed to fail on an unreadable `index.gen` sidecar, scale with the
//! snapshot count, or write anything — all of which `open` does.
//!
//! The CLI owns the armor format, file I/O and printing. Everything that
//! decides *whether* a key may be exported or imported lives here.

use std::path::PathBuf;

use crate::config::VykarConfig;
use crate::repo::identity::{self, KeyProof};
use crate::repo::{
    select_repository_key, EncryptionMode, KeyCopyState, RepoConfig, RepoFormat, KEY_PRIMARY,
    KEY_SECONDARY,
};
use crate::{limits, storage};
use vykar_crypto::key::MasterKey;
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

/// A repository key as it travels outside the repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyExport {
    /// The repository this export belongs to. A **usability** aid — "did you
    /// paste the right export?" — and explicitly *not* a security control:
    /// in an armored file it is plain editable text. Authorization comes from
    /// [`identity::prove_key`], never from this field.
    pub repo_id: Vec<u8>,
    /// The wrapped `EncryptedKey`, exactly as stored at `keys/repokey`.
    pub blob: Vec<u8>,
}

/// A successful export, with what the stored copies looked like.
pub struct ExportedKey {
    pub export: KeyExport,
    pub copies: KeyCopyState,
}

/// What happened to one key copy during an import.
#[derive(Debug, PartialEq, Eq)]
pub enum CopyOutcome {
    AlreadyCorrect,
    Created,
    Replaced,
    /// The write failed; the reason is user-facing.
    Refused(String),
}

impl CopyOutcome {
    fn is_correct(&self) -> bool {
        !matches!(self, Self::Refused(_))
    }
}

/// The result of an authorized import.
pub struct ImportReport {
    /// How the key was authorized. `Unproven` only ever appears here when the
    /// caller passed `force`.
    pub proof: KeyProof,
    /// One entry per key copy, in the order they were handled.
    pub outcomes: Vec<(&'static str, CopyOutcome)>,
    /// Whether the local identity pin was rewritten to this key.
    pub repinned: bool,
}

impl ImportReport {
    /// At least one copy holds the imported key. A run that created the
    /// secondary but was refused the primary is a success, not a failure.
    pub fn restored(&self) -> bool {
        self.outcomes.iter().any(|(_, o)| o.is_correct())
    }

    /// Restored, but not every copy could be written.
    pub fn partial(&self) -> bool {
        self.restored() && self.outcomes.iter().any(|(_, o)| !o.is_correct())
    }
}

/// Open raw storage and read the repository `config`, refusing unencrypted
/// repositories up front — they have no key to export or import.
fn open_raw(config: &VykarConfig) -> Result<(Box<dyn StorageBackend>, RepoConfig, RepoFormat)> {
    let backend = storage::backend_from_config(&config.repository, config.limits.connections)?;
    let backend = limits::wrap_storage_backend(backend, &config.limits);
    let data = backend
        .get("config")?
        .ok_or_else(|| VykarError::RepoNotFound(config.repository.url.clone()))?;
    let repo_config: RepoConfig = rmp_serde::from_slice(&data)?;
    let format = RepoFormat::from_version(repo_config.version)?;
    if repo_config.encryption == EncryptionMode::None {
        return Err(VykarError::Config(
            "this repository is unencrypted (mode: none), so it has no repository key".into(),
        ));
    }
    Ok((backend, repo_config, format))
}

fn cache_dir(config: &VykarConfig) -> Option<PathBuf> {
    config.cache_dir.as_deref().map(PathBuf::from)
}

/// The URL the identity pin is keyed by, or `None` when `--trust-repo` waives
/// the pin — in which case it must not be consulted at all, or a stale pin
/// would reject every candidate before the waiver ever applied.
fn pin_url(config: &VykarConfig) -> Option<&str> {
    if config.trust_repo {
        None
    } else {
        Some(config.repository.url.as_str())
    }
}

fn require_passphrase<'a>(passphrase: Option<&'a str>, what: &str) -> Result<&'a str> {
    passphrase.ok_or_else(|| {
        VykarError::Config(format!(
            "a passphrase is required to {what} this repository's key"
        ))
    })
}

/// Read, verify and hand back the repository key for safekeeping.
///
/// Uses the same copy selection as `Repository::open`, so a planted copy can
/// never be exported while the genuine key sits in the other one. Unwrapping
/// the key means `key export` doubles as a check that the passphrase and key
/// file still work together. Runs the same identity check every ordinary
/// command runs after opening: an export that is not this client's pinned
/// repository is not a usable recovery backup for it.
///
/// Never writes to the repository.
///
/// # Errors
///
/// Anything `Repository::open` would refuse the key for, plus a missing
/// passphrase, an unencrypted repository, or an identity mismatch.
pub fn export(config: &VykarConfig, passphrase: Option<&str>) -> Result<ExportedKey> {
    let (backend, repo_config, format) = open_raw(config)?;
    let passphrase = require_passphrase(passphrase, "export")?;
    let cache_dir = cache_dir(config);

    let loaded = select_repository_key(
        backend.as_ref(),
        Some(passphrase),
        &repo_config,
        format,
        pin_url(config),
        cache_dir.as_deref(),
    )?;

    identity::verify_or_pin(
        &config.repository.url,
        &repo_config.id,
        &loaded.master_key.chunk_id_key,
        cache_dir.as_deref(),
        config.trust_repo,
    )?;

    Ok(ExportedKey {
        export: KeyExport {
            repo_id: repo_config.id,
            blob: loaded.blob,
        },
        copies: loaded.copies,
    })
}

/// Write an exported key back into the repository, both copies.
///
/// Authorization, in order:
///
/// 1. The export's `repo_id` must match the repository's — a paste check,
///    not a trust decision.
/// 2. The passphrase must unwrap the export.
/// 3. The unwrapped key is weighed with [`identity::prove_key`]. A pin that
///    contradicts it refuses the import; `force` does **not** override that,
///    only `--trust-repo` does, and it re-pins after a successful write. A
///    matching pin is sufficient on its own. With no applicable pin the key
///    must decrypt a repository object, or the caller must pass `force`.
/// 4. Without `force`, a stored copy that unwraps to a *different* key is a
///    conflict and refuses the import.
///
/// # Errors
///
/// Any refusal above, or a storage error reading the existing copies. Write
/// failures are reported per copy in the returned [`ImportReport`], not as
/// errors, so a partial success is visible as such.
pub fn import(
    config: &VykarConfig,
    passphrase: Option<&str>,
    export: &KeyExport,
    force: bool,
) -> Result<ImportReport> {
    let (backend, repo_config, format) = open_raw(config)?;

    if export.repo_id != repo_config.id {
        return Err(VykarError::Config(format!(
            "this export is for a different repository: it carries repository id {}, but the \
             repository at '{}' has id {}",
            hex::encode(&export.repo_id),
            config.repository.url,
            hex::encode(&repo_config.id),
        )));
    }

    let passphrase = require_passphrase(passphrase, "import")?;
    let encrypted = vykar_crypto::key::inspect_key_blob(&export.blob)
        .map_err(|d| VykarError::Other(format!("malformed key export: {d}")))?;
    let candidate = MasterKey::from_encrypted(&encrypted, passphrase).map_err(|_| {
        VykarError::RepositoryKeyUnwrapFailed(
            "the supplied passphrase does not unwrap this key export; check that the \
             passphrase and the export belong together"
                .into(),
        )
    })?;

    let cache_dir = cache_dir(config);
    let proof = identity::prove_key(
        backend.as_ref(),
        &repo_config,
        format,
        pin_url(config),
        cache_dir.as_deref(),
        &candidate,
    )?;
    match proof {
        KeyProof::AuthenticatedData | KeyProof::PinMatch => {}
        KeyProof::Mismatch => {
            return Err(VykarError::RepositoryMismatch(format!(
                "refusing to import: this key does not belong to the repository this client \
                 pinned for '{}'. --force does not override this; if the identity changed \
                 deliberately (after a re-init), re-run with --trust-repo.",
                config.repository.url,
            )));
        }
        KeyProof::Unproven if !force => {
            return Err(VykarError::Other(
                "refusing to import: this key could not be verified against the repository \
                 (no repository object could be decrypted with it, and this client has no \
                 identity pin to weigh it against). Re-run with --force to import it anyway."
                    .into(),
            ));
        }
        KeyProof::Unproven => {}
    }

    let existing: Vec<(&'static str, Option<Vec<u8>>)> = [KEY_PRIMARY, KEY_SECONDARY]
        .into_iter()
        .map(|k| backend.get(k).map(|v| (k, v)))
        .collect::<Result<_>>()?;

    if !force {
        for (storage_key, bytes) in &existing {
            let Some(bytes) = bytes else { continue };
            if bytes == &export.blob {
                continue;
            }
            let Ok(stored) = vykar_crypto::key::inspect_key_blob(bytes) else {
                continue;
            };
            let Ok(stored) = MasterKey::from_encrypted(&stored, passphrase) else {
                continue;
            };
            if !stored.ct_eq(&candidate) {
                return Err(VykarError::Other(format!(
                    "refusing to import: {storage_key} already holds a different repository \
                     key that unwraps with this passphrase. Re-run with --force to replace it."
                )));
            }
        }
    }

    // Best-effort: `keys/` usually exists already, and backends differ in
    // whether re-creating it is a no-op or an error. If it is genuinely
    // missing, the writes below report that far more clearly.
    if let Err(e) = backend.create_dir("keys/") {
        tracing::debug!("could not create keys/ before import: {e}");
    }
    let outcomes = reconcile_copies(backend.as_ref(), &existing, &export.blob);

    let mut report = ImportReport {
        proof,
        outcomes,
        repinned: false,
    };

    // `--trust-repo` waived the pin to *authorize* this import; the pin has to
    // be re-established too, or the very next ordinary command fails on the
    // stale identity and the import looks like it did nothing. Only after a
    // successful write — a refused import leaves every pin untouched.
    if config.trust_repo && report.restored() {
        identity::verify_or_pin(
            &config.repository.url,
            &repo_config.id,
            &candidate.chunk_id_key,
            cache_dir.as_deref(),
            true,
        )?;
        report.repinned = true;
    }
    Ok(report)
}

/// Write the imported blob to both copies, **in this order**: skip, create,
/// replace.
///
/// The ordering is what makes the common recovery case work. Skipping a copy
/// that already matches avoids a pointless overwrite — *fatal* on append-only
/// storage, which permits creation but refuses to replace an existing object.
/// Creating missing copies next means the redundant copy lands even when the
/// primary cannot be touched. Replacements go last, so a refusal there costs
/// nothing that was already achievable.
fn reconcile_copies(
    storage: &dyn StorageBackend,
    existing: &[(&'static str, Option<Vec<u8>>)],
    blob: &[u8],
) -> Vec<(&'static str, CopyOutcome)> {
    let write = |storage_key: &'static str, on_ok: CopyOutcome| match storage.put(storage_key, blob)
    {
        Ok(()) => on_ok,
        Err(e) => CopyOutcome::Refused(describe_write_refusal(storage_key, &e)),
    };

    let mut outcomes = Vec::with_capacity(existing.len());
    for (storage_key, bytes) in existing {
        if bytes.as_deref() == Some(blob) {
            outcomes.push((*storage_key, CopyOutcome::AlreadyCorrect));
        }
    }
    for (storage_key, bytes) in existing {
        if bytes.is_none() {
            outcomes.push((*storage_key, write(storage_key, CopyOutcome::Created)));
        }
    }
    for (storage_key, bytes) in existing {
        if bytes.as_deref().is_some_and(|b| b != blob) {
            outcomes.push((*storage_key, write(storage_key, CopyOutcome::Replaced)));
        }
    }
    outcomes
}

/// Keep the backend's own reason; only add the hint when the error looks like
/// a policy refusal. A 403 is not proof of append-only mode.
fn describe_write_refusal(storage_key: &str, err: &VykarError) -> String {
    if err.is_write_refusal() {
        format!(
            "the backend refused the write: {err}. If the repository is append-only or \
             read-only, remove {storage_key} there manually and re-run `vykar key import`."
        )
    } else {
        err.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MemoryBackend;

    fn sample_blob() -> Vec<u8> {
        let key = MasterKey::generate().unwrap();
        rmp_serde::to_vec(&key.to_encrypted("reconcile-test").unwrap()).unwrap()
    }

    fn outcome_of<'a>(outcomes: &'a [(&'static str, CopyOutcome)], key: &str) -> &'a CopyOutcome {
        &outcomes
            .iter()
            .find(|(k, _)| *k == key)
            .expect("reported")
            .1
    }

    /// The common recovery case: the primary is already right and the
    /// secondary is missing. Writing both blindly would fail on the redundant
    /// primary overwrite and lose the creation that was the entire point.
    #[test]
    fn append_only_import_skips_a_matching_primary_and_creates_the_secondary() {
        let blob = sample_blob();
        let storage = MemoryBackend::append_only();
        storage.put(KEY_PRIMARY, &blob).unwrap();
        let existing = vec![(KEY_PRIMARY, Some(blob.clone())), (KEY_SECONDARY, None)];

        let outcomes = reconcile_copies(&storage, &existing, &blob);

        assert_eq!(
            outcome_of(&outcomes, KEY_PRIMARY),
            &CopyOutcome::AlreadyCorrect
        );
        assert_eq!(outcome_of(&outcomes, KEY_SECONDARY), &CopyOutcome::Created);
        assert_eq!(
            storage.get(KEY_SECONDARY).unwrap().as_deref(),
            Some(&blob[..])
        );
    }

    /// The inverse: the primary differs, so replacing it is an overwrite the
    /// backend refuses. The secondary still gets created, and a correct copy
    /// now exists — a partial success, not a failure.
    #[test]
    fn append_only_import_creates_the_secondary_and_reports_the_refused_primary() {
        let blob = sample_blob();
        let stale = sample_blob();
        let storage = MemoryBackend::append_only();
        storage.put(KEY_PRIMARY, &stale).unwrap();
        let existing = vec![(KEY_PRIMARY, Some(stale.clone())), (KEY_SECONDARY, None)];

        let outcomes = reconcile_copies(&storage, &existing, &blob);

        assert_eq!(outcome_of(&outcomes, KEY_SECONDARY), &CopyOutcome::Created);
        match outcome_of(&outcomes, KEY_PRIMARY) {
            CopyOutcome::Refused(reason) => {
                assert!(reason.contains("refused the write"), "{reason}");
                assert!(
                    reason.contains("append-only"),
                    "the backend's own reason must survive: {reason}"
                );
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert_eq!(
            storage.get(KEY_PRIMARY).unwrap().as_deref(),
            Some(&stale[..])
        );

        let report = ImportReport {
            proof: KeyProof::AuthenticatedData,
            outcomes,
            repinned: false,
        };
        assert!(report.restored());
        assert!(report.partial());
    }

    #[test]
    fn import_that_writes_nothing_correct_is_not_restored() {
        let report = ImportReport {
            proof: KeyProof::AuthenticatedData,
            outcomes: vec![(KEY_PRIMARY, CopyOutcome::Refused("append-only".into()))],
            repinned: false,
        };
        assert!(!report.restored());
        assert!(!report.partial());
    }

    /// A transient failure is reported as-is: no append-only hint for an
    /// error that does not look like a policy refusal.
    #[test]
    fn unrecognised_write_errors_are_passed_through() {
        let err = VykarError::Other("connection reset by peer".into());
        assert_eq!(
            describe_write_refusal(KEY_PRIMARY, &err),
            "connection reset by peer"
        );
    }
}
