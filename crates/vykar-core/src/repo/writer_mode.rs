//! The repository's writer mode (10-single-protocol step 5): the init-time
//! check whose verdict init and `vykar upgrade` record in `config`, and the
//! per-handle requirement a multi-writer repository puts on its clients.

use vykar_storage::{ConditionalSupport, StorageBackend};
use vykar_types::error::{Result, VykarError};

use super::{Repo, WriterMode};

/// Why a repository is single-writer when the storage offers no
/// put-if-absent at all (no `ConditionalWrite`, or one an earlier check on
/// this handle concluded unsupported).
pub(crate) const NO_PUT_IF_ABSENT: &str = "the storage backend offers no put-if-absent (on S3: \
     s3_conditional_writes is off, or auto on a host that is not on the tested list)";

/// What the init-time check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ModeCheck {
    Multi,
    Single { reason: String },
}

/// `ConditionalWrite::verify_at_init` on `storage` (sequential check plus
/// the concurrent race), or `Single { NO_PUT_IF_ABSENT }` without a
/// request when the backend offers no put-if-absent. Callers run it before
/// any put-if-absent create on this backend instance (a failed race
/// switches put-if-absent off for the instance).
///
/// # Errors
///
/// Undecided (no write permission, transport failure): the check's error.
pub(crate) fn init_time_check(storage: &dyn StorageBackend) -> Result<ModeCheck> {
    let Some(cond) = storage.conditional() else {
        return Ok(ModeCheck::Single {
            reason: NO_PUT_IF_ABSENT.to_string(),
        });
    };
    Ok(match cond.verify_at_init()? {
        ConditionalSupport::Verified => ModeCheck::Multi,
        ConditionalSupport::Unsupported { reason } => ModeCheck::Single { reason },
    })
}

impl Repo {
    /// The repository's mode as its config records it.
    pub fn writer_mode(&self) -> WriterMode {
        self.config.writer_mode()
    }

    /// Why init recorded single-writer (the init-time check's verdict, or
    /// that the storage offers no put-if-absent); `None` on a multi-writer
    /// repository and on
    /// every handle `open` returned.
    pub fn single_writer_reason(&self) -> Option<&str> {
        self.single_writer_reason.as_deref()
    }

    /// On a multi-writer repository: `Ok` once this handle's storage passed
    /// the put-if-absent check (`ConditionalWrite::verify`, once per backend
    /// instance), else `MultiWriterUnsupported`. `Ok` without a request on a
    /// single-writer repository. Caches nothing on the handle.
    ///
    /// # Errors
    ///
    /// `MultiWriterUnsupported`; an undecided check's error.
    pub(crate) fn require_writer_mode(&self) -> Result<()> {
        if self.writer_mode() == WriterMode::Single {
            return Ok(());
        }
        let Some(cond) = self.storage.conditional() else {
            return Err(self.multi_writer_unsupported(NO_PUT_IF_ABSENT));
        };
        match cond.verify()? {
            ConditionalSupport::Verified => Ok(()),
            ConditionalSupport::Unsupported { reason } => {
                Err(self.multi_writer_unsupported(reason))
            }
        }
    }

    /// `MultiWriterUnsupported` naming this handle's repository URL (or
    /// "the storage backend") and `reason`.
    pub(crate) fn multi_writer_unsupported(&self, reason: impl Into<String>) -> VykarError {
        VykarError::MultiWriterUnsupported {
            backend: self.backend_label().to_string(),
            reason: reason.into(),
        }
    }

    /// The URL, or "the storage backend", for messages.
    pub(crate) fn backend_label(&self) -> &str {
        self.repo_url.as_deref().unwrap_or("the storage backend")
    }

    /// Read `config` again (10-single-protocol step 7) and take the
    /// repository's mode from it: GET `config`, decode and validate it as
    /// open does (`RepoConfig::decode`, `DecodedConfig::validate`), and
    /// refuse unless it is this handle's config but for the mode: the same
    /// `id`, format (version and chunk hash), encryption, chunker
    /// parameters and pack sizes, compared field by field so the rule fails
    /// closed. A mode that differs from this handle's is adopted:
    /// `self.config`'s features are set to it (`set_writer_mode`) and the
    /// cached commit mode is dropped, so the next [`Self::commit_mode`]
    /// decides again (a multi-writer one runs this handle's put-if-absent
    /// check). Returns the mode now in force. Callers: a backup whose
    /// marker record was not continuous at its commit mark (the suspend
    /// rule), and every head-writing maintenance session right after its
    /// registration (a mode switch between its open and its registration
    /// is seen by neither side otherwise).
    ///
    /// # Errors
    ///
    /// A failed GET or a missing `config`; `Other("the repository config
    /// changed …")` for another identity, format or encryption;
    /// `validate`'s refusal of an unknown feature.
    pub(crate) fn reread_writer_mode(&mut self) -> Result<WriterMode> {
        let stored = self
            .storage
            .get("config")?
            .ok_or_else(|| VykarError::RepoNotFound("config not found".into()))?;
        let decoded = super::RepoConfig::decode(&stored)?;
        let format = decoded.validate()?;
        let changed = |what: &str| {
            VykarError::Other(format!(
                "the repository config changed while this client had it open ({what}); \
                 nothing was committed, run the command again"
            ))
        };
        let super::DecodedConfig::Map(config) = decoded else {
            return Err(changed("no longer a v6 config"));
        };
        let mine = &self.config;
        let differs = [
            (config.id != mine.id, "another repository id"),
            (format != self.format, "another format"),
            (config.version != mine.version, "another version"),
            (config.chunk_hash != mine.chunk_hash, "another chunk hash"),
            (config.encryption != mine.encryption, "another encryption"),
            (
                config.chunker_params != mine.chunker_params,
                "other chunker parameters",
            ),
            (
                config.min_pack_size != mine.min_pack_size
                    || config.max_pack_size != mine.max_pack_size,
                "other pack sizes",
            ),
        ];
        if let Some((_, what)) = differs.iter().find(|(d, _)| *d) {
            return Err(changed(what));
        }
        let mode = config.writer_mode();
        if mode != self.config.writer_mode() {
            tracing::info!(
                mode = mode.as_str(),
                "the repository's commit mode changed since this client opened it"
            );
            self.config.set_writer_mode(mode);
            self.commit_mode = None;
        }
        Ok(mode)
    }

    /// Whether this handle's storage offers put-if-absent, as far as it
    /// knows without a request (`conditional()` is Some): a single-writer
    /// repository on it could be switched to multi-writer.
    pub fn storage_offers_put_if_absent(&self) -> bool {
        self.storage.conditional().is_some()
    }
}
