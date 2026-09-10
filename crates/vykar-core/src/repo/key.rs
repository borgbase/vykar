//! Loading the repository key from its two stored copies.
//!
//! This is the single place `keys/repokey` and `keys/repokey.2` are read, so
//! the corroboration between copies, the corruption diagnosis, divergence
//! resolution and the backfill of a missing copy all live here — for
//! `Repository::open` and for `vykar key export`, which works on raw storage.

use std::path::Path;
use std::sync::Arc;

use super::identity::{self, KeyProof};
use super::{EncryptionMode, KeyCopyState, RepoConfig, RepoFormat, KEY_PRIMARY, KEY_SECONDARY};
use vykar_crypto::key::{KeyBlobDefect, MasterKey};
use vykar_crypto::{self as crypto, CryptoEngine};
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::error::{Result, VykarError};

/// Build the crypto engine for an encrypted repository from its master key.
///
/// # Errors
///
/// `EncryptionMode::None` has no master key and is an error here; callers
/// handle the plaintext engine themselves.
pub(super) fn build_engine(
    mode: &EncryptionMode,
    format: RepoFormat,
    key: &MasterKey,
) -> Result<Arc<dyn CryptoEngine>> {
    let hasher = ChunkHasher::new(format.chunk_hash(), key.chunk_id_key);
    match mode {
        EncryptionMode::None => Err(VykarError::Config(
            "repository is unencrypted; there is no repository key".into(),
        )),
        EncryptionMode::Aes256Gcm => Ok(Arc::new(crypto::aes_gcm::Aes256GcmEngine::new(
            &key.encryption_key,
            hasher,
        ))),
        EncryptionMode::Chacha20Poly1305 => Ok(Arc::new(
            crypto::chacha20_poly1305::ChaCha20Poly1305Engine::new(&key.encryption_key, hasher),
        )),
    }
}

/// The key the loader settled on.
pub struct LoadedKey {
    pub master_key: MasterKey,
    /// The stored bytes that unwrapped. `check` compares storage against these
    /// instead of unwrapping again, which would cost a second Argon2id
    /// derivation and need the passphrase threaded into the scan.
    pub blob: Vec<u8>,
    /// What the two copies looked like.
    pub copies: KeyCopyState,
}

/// Everything the loader needs beyond the storage backend itself.
pub(super) struct KeyProofCtx<'a> {
    pub(super) repo_config: &'a RepoConfig,
    pub(super) format: RepoFormat,
    /// Repository URL, when the caller knows it. Without it the identity pin
    /// cannot be consulted and only data proof is available.
    pub(super) url: Option<&'a str>,
    pub(super) cache_dir: Option<&'a Path>,
}

/// Why one key copy could not be unwrapped.
enum CopyFault {
    /// Unambiguous damage, detectable without the passphrase.
    Corrupt(KeyBlobDefect),
    /// Well-formed but did not authenticate. Wrong passphrase or damage
    /// inside the AEAD payload — indistinguishable from the blob alone.
    Ambiguous,
}

impl std::fmt::Display for CopyFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Corrupt(defect) => write!(f, "{defect}"),
            Self::Ambiguous => write!(f, "did not authenticate"),
        }
    }
}

/// Decode and unwrap one key copy.
///
/// The inner result classifies the *copy*: `Corrupt` only from the structural
/// screen, `Ambiguous` only from an authentication failure. Anything else out
/// of `from_encrypted` — a derivation failure on parameters that already
/// passed the screen — is an internal fault, not damaged storage, and is
/// propagated unchanged as the outer error rather than guessed at.
fn unwrap_copy(
    bytes: &[u8],
    passphrase: &str,
) -> Result<std::result::Result<MasterKey, CopyFault>> {
    let encrypted = match vykar_crypto::key::inspect_key_blob(bytes) {
        Ok(encrypted) => encrypted,
        Err(defect) => return Ok(Err(CopyFault::Corrupt(defect))),
    };
    match MasterKey::from_encrypted(&encrypted, passphrase) {
        Ok(key) => Ok(Ok(key)),
        Err(VykarError::DecryptionFailed) => Ok(Err(CopyFault::Ambiguous)),
        Err(other) => Err(other),
    }
}

/// What a single key object looks like on storage right now.
enum CopyRead {
    Present(Vec<u8>),
    Absent,
    /// The object could not be read — EIO, a permission failure, a transient
    /// backend error. **Not the same as absent**: something may well be there,
    /// so it must never be treated as missing and never silently overwritten.
    Unreadable(VykarError),
}

fn read_copy(storage: &dyn StorageBackend, storage_key: &str) -> CopyRead {
    match storage.get(storage_key) {
        Ok(Some(bytes)) => CopyRead::Present(bytes),
        Ok(None) => CopyRead::Absent,
        Err(e) => CopyRead::Unreadable(e),
    }
}

fn require_passphrase(passphrase: Option<&str>) -> Result<&str> {
    passphrase
        .ok_or_else(|| VykarError::Config("passphrase required for encrypted repository".into()))
}

/// Select and unwrap the repository key from **raw storage**, without opening
/// the repository.
///
/// Exactly the selection `Repository::open` performs — divergence resolution,
/// the proof requirement, the corruption diagnosis — so a caller working on raw
/// storage cannot end up holding a copy the open path would have rejected.
/// `vykar key export` uses this: picking "the first copy that unwraps" would
/// happily export a planted foreign key while the genuine one sat in the other
/// copy, producing a recovery backup that cannot restore anything.
///
/// Never writes: no backfill, no repair. Identity verification against the
/// local pin is the caller's to run, the same way the open path runs it.
///
/// # Errors
///
/// A missing passphrase, no usable copy, a copy that does not unwrap, or a
/// divergence that cannot be resolved — the same failures `Repository::open`
/// reports.
pub fn select_repository_key(
    storage: &dyn StorageBackend,
    passphrase: Option<&str>,
    repo_config: &RepoConfig,
    format: RepoFormat,
    url: Option<&str>,
    cache_dir: Option<&Path>,
) -> Result<LoadedKey> {
    let ctx = KeyProofCtx {
        repo_config,
        format,
        url,
        cache_dir,
    };
    load_master_key(storage, passphrase, &ctx, false)
}

/// Fetch, corroborate and unwrap the repository key.
///
/// Both objects are read and classified **independently**. Propagating the
/// first read error would let an I/O failure on one object defeat the very
/// redundancy the second copy exists to provide.
///
/// `backfill` allows writing a copy that is genuinely *absent* back from the
/// present one. Best effort and never fatal. It runs before the caller's
/// identity check, which is acceptable because it only ever copies a blob
/// that is already present into an empty slot — a substituted repository
/// gains nothing it did not already hold.
pub(super) fn load_master_key(
    storage: &dyn StorageBackend,
    passphrase: Option<&str>,
    ctx: &KeyProofCtx,
    backfill: bool,
) -> Result<LoadedKey> {
    match (
        read_copy(storage, KEY_PRIMARY),
        read_copy(storage, KEY_SECONDARY),
    ) {
        (CopyRead::Present(a), CopyRead::Present(b)) => {
            let pass = require_passphrase(passphrase)?;
            if a != b {
                return resolve_divergent(storage, ctx, pass, a, b);
            }
            // The copies corroborate each other.
            match unwrap_copy(&a, pass)? {
                Ok(master_key) => Ok(LoadedKey {
                    master_key,
                    blob: a,
                    copies: KeyCopyState::Matched,
                }),
                Err(fault) => Err(matched_copies_error(&fault)),
            }
        }

        // One copy present, the other genuinely absent: a legacy repository,
        // or a backfill that never landed. Writing it back overwrites nothing.
        (CopyRead::Present(bytes), CopyRead::Absent) => load_single_copy(
            storage,
            passphrase,
            bytes,
            KEY_PRIMARY,
            OtherCopy::Absent,
            backfill,
        ),
        (CopyRead::Absent, CopyRead::Present(bytes)) => {
            tracing::warn!(
                "{KEY_PRIMARY} is missing; opening from the redundant copy {KEY_SECONDARY}"
            );
            load_single_copy(
                storage,
                passphrase,
                bytes,
                KEY_SECONDARY,
                OtherCopy::Absent,
                backfill,
            )
        }

        // One copy present, the other unreadable. Open off the survivor, but
        // never write over the unreadable object: it may be perfectly intact
        // and merely unreachable right now.
        (CopyRead::Present(bytes), CopyRead::Unreadable(e)) => {
            tracing::warn!("{KEY_SECONDARY} could not be read ({e}); opening from {KEY_PRIMARY}");
            load_single_copy(
                storage,
                passphrase,
                bytes,
                KEY_PRIMARY,
                OtherCopy::Unreadable,
                backfill,
            )
        }
        (CopyRead::Unreadable(e), CopyRead::Present(bytes)) => {
            tracing::warn!("{KEY_PRIMARY} could not be read ({e}); opening from {KEY_SECONDARY}");
            load_single_copy(
                storage,
                passphrase,
                bytes,
                KEY_SECONDARY,
                OtherCopy::Unreadable,
                backfill,
            )
        }

        (CopyRead::Absent, CopyRead::Absent) => Err(missing_key_error()),

        // Nothing usable, and at least one read failed: say so rather than
        // claiming the key is gone.
        (CopyRead::Unreadable(e), _) | (_, CopyRead::Unreadable(e)) => {
            Err(VykarError::Other(format!(
                "could not read the repository key: {e}. Neither {KEY_PRIMARY} nor \
                 {KEY_SECONDARY} could be fetched; fix access to the repository and retry."
            )))
        }
    }
}

/// Why the other copy could not be used. Decides both whether a backfill is
/// permitted and how a failure of the present copy is described: an absent
/// copy is gone, an unreadable one may come back once access is fixed.
#[derive(Clone, Copy)]
enum OtherCopy {
    Absent,
    Unreadable,
}

/// Open from the only usable copy.
///
/// No `prove_key` here, deliberately: there is no rival candidate to weigh
/// this one against, and the caller's identity check covers the rest — the
/// same as the matched two-copy case.
fn load_single_copy(
    storage: &dyn StorageBackend,
    passphrase: Option<&str>,
    bytes: Vec<u8>,
    present: &'static str,
    other: OtherCopy,
    backfill: bool,
) -> Result<LoadedKey> {
    let missing = if present == KEY_PRIMARY {
        KEY_SECONDARY
    } else {
        KEY_PRIMARY
    };
    let pass = require_passphrase(passphrase)?;
    let master_key = match unwrap_copy(&bytes, pass)? {
        Ok(key) => key,
        Err(fault) => return Err(single_copy_error(present, missing, other, &fault)),
    };
    // Only an *absent* copy is ever written: an unreadable object may be
    // perfectly intact.
    if backfill && matches!(other, OtherCopy::Absent) {
        // No compare-and-swap needed: any racing opener writes byte-identical
        // content, so last-write-wins is a no-op.
        match storage.put(missing, &bytes) {
            Ok(()) => tracing::debug!("wrote missing repository key copy {missing}"),
            Err(e) => tracing::warn!(
                "{missing} is missing and could not be written back ({e}); \
                 run `vykar check --repair` once the repository is writable"
            ),
        }
    }
    Ok(LoadedKey {
        master_key,
        blob: bytes,
        copies: KeyCopyState::OneMissing { missing },
    })
}

/// Resolve two key copies that differ byte-for-byte.
///
/// Both are unwrapped before anything is decided: "first success wins" is
/// wrong, because two divergent blobs can each unwrap under the same
/// passphrase — independent wraps of the same key, or a foreign key that
/// happens to share the passphrase.
///
/// A divergent candidate is accepted only on **positive** proof. The copies
/// disagree, so accepting an uncorroborated one would let a planted key win
/// purely by being the one that happened to unwrap — and `check --repair`
/// would then rewrite the genuine copy from it, which cannot be undone.
/// Absence of evidence is only tolerable where there is no rival candidate.
fn resolve_divergent(
    storage: &dyn StorageBackend,
    ctx: &KeyProofCtx,
    pass: &str,
    a: Vec<u8>,
    b: Vec<u8>,
) -> Result<LoadedKey> {
    let key_a = unwrap_copy(&a, pass)?;
    let key_b = unwrap_copy(&b, pass)?;

    let pick = |master_key: MasterKey, blob: Vec<u8>, good: &'static str, bad: &'static str| {
        tracing::warn!("{bad} disagrees with {good}; run `vykar check --repair` to rewrite it");
        LoadedKey {
            master_key,
            blob,
            copies: KeyCopyState::Divergent { good, bad },
        }
    };

    match (key_a, key_b) {
        (Err(fault_a), Err(fault_b)) => Err(both_copies_failed_error(&fault_a, &fault_b)),

        (Ok(key), Err(_)) => {
            require_proof(storage, ctx, &key, KEY_PRIMARY, KEY_SECONDARY)?;
            Ok(pick(key, a, KEY_PRIMARY, KEY_SECONDARY))
        }
        (Err(_), Ok(key)) => {
            require_proof(storage, ctx, &key, KEY_SECONDARY, KEY_PRIMARY)?;
            Ok(pick(key, b, KEY_SECONDARY, KEY_PRIMARY))
        }

        (Ok(key_a), Ok(key_b)) => {
            // Independent wraps of one key: different salt and nonce, same
            // material. There is no rival candidate, so no proof is required;
            // canonicalize on the primary.
            if key_a.ct_eq(&key_b) {
                return Ok(pick(key_a, a, KEY_PRIMARY, KEY_SECONDARY));
            }

            // Different key material: at most one is genuine.
            let proof_a = prove(storage, ctx, &key_a)?;
            let proof_b = prove(storage, ctx, &key_b)?;
            match (proof_a.is_established(), proof_b.is_established()) {
                (true, false) => Ok(pick(key_a, a, KEY_PRIMARY, KEY_SECONDARY)),
                (false, true) => Ok(pick(key_b, b, KEY_SECONDARY, KEY_PRIMARY)),
                // A pin exists and contradicts both: a stale pin, not an
                // unresolvable repository. `--trust-repo` is the remedy.
                (false, false)
                    if proof_a == KeyProof::Mismatch || proof_b == KeyProof::Mismatch =>
                {
                    Err(pin_mismatch_error(
                        &format!("neither {KEY_PRIMARY} nor {KEY_SECONDARY} belongs to"),
                        ctx.url,
                    ))
                }
                // Neither established, or both — no basis to choose.
                _ => Err(VykarError::Other(format!(
                    "{KEY_PRIMARY} and {KEY_SECONDARY} hold different repository keys and \
                     neither can be established as this repository's. Refusing to guess; \
                     restore the correct key with `vykar key import <file>`."
                ))),
            }
        }
    }
}

fn prove(
    storage: &dyn StorageBackend,
    ctx: &KeyProofCtx,
    candidate: &MasterKey,
) -> Result<KeyProof> {
    identity::prove_key(
        storage,
        ctx.repo_config,
        ctx.format,
        ctx.url,
        ctx.cache_dir,
        candidate,
    )
}

/// Accept the sole unwrapping divergent candidate only on positive proof.
fn require_proof(
    storage: &dyn StorageBackend,
    ctx: &KeyProofCtx,
    candidate: &MasterKey,
    good: &str,
    other: &str,
) -> Result<()> {
    match prove(storage, ctx, candidate)? {
        KeyProof::AuthenticatedData | KeyProof::PinMatch => Ok(()),
        KeyProof::Mismatch => Err(pin_mismatch_error(
            &format!("the key in {good} does not belong to"),
            ctx.url,
        )),
        KeyProof::Unproven => Err(VykarError::Other(format!(
            "{good} and {other} disagree, and {good} cannot be established as this \
             repository's key: no repository object could be decrypted with it and no \
             identity pin resolves it. Refusing to guess; restore the correct key with \
             `vykar key import <file>`."
        ))),
    }
}

// ---------------------------------------------------------------------------
// Error constructors
// ---------------------------------------------------------------------------

/// `clause` is the subject and verb, e.g. "the key in keys/repokey does not
/// belong to"; the constructor supplies the object and the remedy.
fn pin_mismatch_error(clause: &str, url: Option<&str>) -> VykarError {
    VykarError::RepositoryMismatch(format!(
        "{clause} the repository this client pinned for '{}'. If this is expected \
         (after a re-init or a deliberate key import), re-run with --trust-repo.",
        url.unwrap_or("this repository"),
    ))
}

fn missing_key_error() -> VykarError {
    VykarError::Other(format!(
        "the repository key is missing: neither {KEY_PRIMARY} nor {KEY_SECONDARY} exists, \
         and the master key cannot be derived from the passphrase. Restore it with \
         `vykar key import <file>` from a `vykar key export` backup."
    ))
}

/// The only readable copy failed. Absence and unreadability of the other copy
/// get different remedies: a gone copy points at `key import`, an unreadable
/// one at fixing access first, since it may hold the key intact.
fn single_copy_error(
    present: &str,
    missing: &str,
    other: OtherCopy,
    fault: &CopyFault,
) -> VykarError {
    match (fault, other) {
        (CopyFault::Corrupt(defect), OtherCopy::Absent) => VykarError::Other(format!(
            "{present} is corrupt ({defect}) and no other copy exists. Restore the key \
             with `vykar key import <file>` from a `vykar key export` backup."
        )),
        (CopyFault::Corrupt(defect), OtherCopy::Unreadable) => VykarError::Other(format!(
            "{present} is corrupt ({defect}) and the other copy, {missing}, could not be \
             read. Fix access to {missing} first; if it is damaged too, restore the key \
             with `vykar key import <file>`."
        )),
        (CopyFault::Ambiguous, OtherCopy::Absent) => {
            VykarError::RepositoryKeyUnwrapFailed(format!(
                "could not unwrap the repository key from {present}: wrong passphrase, or \
                 the stored key is damaged. It is the only copy, so there is nothing to \
                 weigh it against."
            ))
        }
        (CopyFault::Ambiguous, OtherCopy::Unreadable) => {
            VykarError::RepositoryKeyUnwrapFailed(format!(
                "could not unwrap the repository key from {present}: wrong passphrase, or \
                 the stored key is damaged. The other copy, {missing}, could not be read, so \
                 there is nothing to weigh it against; fixing access to it may settle this."
            ))
        }
    }
}

/// The headline case: two byte-identical, well-formed copies that still fail
/// to unwrap.
///
/// **Deliberately hedged.** Two matching copies are strong evidence, not
/// proof: on a CoW or deduplicating filesystem, or after `cp --reflink`, the
/// two objects can share physical extents, so one bad block corrupts both
/// identically. Do not strengthen this wording into a claim that the bytes
/// are intact.
fn matched_copies_error(fault: &CopyFault) -> VykarError {
    match fault {
        CopyFault::Corrupt(defect) => VykarError::Other(format!(
            "both repository key copies are corrupt in the same way ({defect}); \
             {KEY_PRIMARY} and {KEY_SECONDARY} are byte-identical. Restore the key with \
             `vykar key import <file>`."
        )),
        CopyFault::Ambiguous => VykarError::RepositoryKeyUnwrapFailed(format!(
            "could not unwrap the repository key: likely incorrect passphrase; both key \
             copies match ({KEY_PRIMARY} and {KEY_SECONDARY} are byte-identical and \
             well-formed). Two matching copies are strong evidence, not certainty: a shared \
             extent on a copy-on-write filesystem can corrupt both identically."
        )),
    }
}

/// Neither divergent copy unwrapped.
///
/// Typed as a passphrase failure **only** when a wrong passphrase is still a
/// live explanation. Two proven structural defects cannot be, and calling
/// them one would send the GUI round a retry loop that cannot succeed.
fn both_copies_failed_error(fault_a: &CopyFault, fault_b: &CopyFault) -> VykarError {
    let detail = format!("{KEY_PRIMARY} ({fault_a}); {KEY_SECONDARY} ({fault_b})");
    if matches!(fault_a, CopyFault::Corrupt(_)) && matches!(fault_b, CopyFault::Corrupt(_)) {
        return VykarError::Other(format!(
            "both repository key copies are corrupt, in different ways: {detail}. Restore \
             the key with `vykar key import <file>`."
        ));
    }
    VykarError::RepositoryKeyUnwrapFailed(format!(
        "could not unwrap the repository key from either copy, and the two disagree: \
         {detail}. The passphrase may be wrong, or both copies damaged; restore the key \
         with `vykar key import <file>` if it is the latter."
    ))
}
