//! Shared wire-format types and constants for vykar client ↔ server communication.
//!
//! This crate is intentionally minimal: DTOs, pack format constants, protocol
//! versioning, and transport-level validation. No storage I/O, no crypto.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

use serde::{Deserialize, Serialize};
use vykar_types::hash::HashAlgorithm;

// ── Pack format constants ──────────────────────────────────────────────────

/// Magic bytes at the start of every pack file.
pub const PACK_MAGIC: &[u8; 8] = b"VGERPACK";

/// Size of the pack header (magic + version byte).
pub const PACK_HEADER_SIZE: usize = 9;

/// Version byte written into new packs by this binary.
pub const PACK_VERSION_CURRENT: u8 = 1;

/// Oldest pack version this binary can read.
///
/// A repo contains packs from many backup runs — bumping `PACK_VERSION_CURRENT`
/// to 2 must not break reading existing v1 packs. Bump MIN only when a version
/// is truly retired (requires a migration).
pub const PACK_VERSION_MIN: u8 = 1;

/// Newest pack version this binary understands.
///
/// Always == `PACK_VERSION_CURRENT` (we can read anything we can write).
pub const PACK_VERSION_MAX: u8 = PACK_VERSION_CURRENT;

/// Validate a pack file's header bytes: magic, then version range.
///
/// Returns the pack's version byte. `hdr` must be at least
/// [`PACK_HEADER_SIZE`] long; extra bytes (e.g. the whole pack) are ignored.
/// Shared by the client's pack reader and both server-side verifiers so all
/// three agree on what a readable pack looks like.
///
/// # Errors
///
/// Returns a message when `hdr` is too short, the magic does not match, or the
/// version is outside `PACK_VERSION_MIN..=PACK_VERSION_MAX`.
pub fn validate_pack_header(hdr: &[u8]) -> Result<u8, String> {
    let Some(header) = hdr.get(..PACK_HEADER_SIZE) else {
        return Err("pack too small".to_string());
    };
    let (magic, rest) = header.split_at(PACK_MAGIC.len());
    if magic != PACK_MAGIC {
        return Err("invalid pack magic".to_string());
    }
    // PACK_HEADER_SIZE == PACK_MAGIC.len() + 1, so `rest` holds the version byte.
    let Some(&version) = rest.first() else {
        return Err("pack too small".to_string());
    };
    if !(PACK_VERSION_MIN..=PACK_VERSION_MAX).contains(&version) {
        return Err(format!(
            "unsupported pack version {version} (supported: {PACK_VERSION_MIN}..={PACK_VERSION_MAX})"
        ));
    }
    Ok(version)
}

// ── Server-side operation caps (shared client ↔ server) ────────────────────

/// Maximum total output bytes a single server-side repack plan may produce.
/// The server rejects larger plans with 400; the client pre-chunks plans to
/// stay within this so both sides agree on the boundary.
pub const MAX_REPACK_OUTPUT_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

// ── Protocol version ───────────────────────────────────────────────────────

/// Current protocol version. Sent by clients in requests.
///
/// Bumped to 2 by BLAKE3 repositories. A new client talking about a *v2*
/// repository still declares 1, so it keeps working against pre-BLAKE3
/// servers; only BLAKE3 traffic requires the newer server.
///
/// Bumped to 3 by repack plans that carry a pack trailer
/// ([`RepackOperationRequest::trailer`]); a plan without one still declares
/// 1 or 2, so it keeps working against older servers.
pub const PROTOCOL_VERSION: u32 = 3;

/// The protocol version a repack plan must declare when any of its operations
/// carries a [`RepackOperationRequest::trailer`]. Servers 0.8 through 0.24
/// refuse such a plan by version before reading any operation; older ones
/// never advertise `repack_trailer`, so a client gated on it never sends them
/// one.
pub const REPACK_TRAILER_PROTOCOL_VERSION: u32 = 3;

/// Largest trailer one repack operation may carry. A trailer holds 41 bytes
/// per chunk plus a fixed overhead (05-index-gc-format.md), so 16 MiB covers
/// the server's [`MAX_KEEP_BLOBS_PER_OP`] kept blobs per operation with room
/// to spare; the hex encoding doubles it on the wire, which keeps one maximal
/// trailer well inside [`MAX_ADMIN_BODY_BYTES`].
///
/// A caller that sends trailers must also bound each plan's JSON size: many
/// trailers can exceed the body limit before the output cap
/// ([`MAX_REPACK_OUTPUT_BYTES`]). [`repack_op_json_bound`] and
/// [`MAX_REPACK_PLAN_JSON_BYTES`] are that bound.
pub const MAX_REPACK_TRAILER_BYTES: u64 = 16 * 1024 * 1024;

/// Most operations one repack plan may hold. The server refuses a larger
/// plan with 400; the client batches below it.
pub const MAX_REPACK_OPS: usize = 10_000;

/// Most kept blobs one repack operation may hold. The server refuses a
/// larger operation with 400; the client repacks such a pack itself.
pub const MAX_KEEP_BLOBS_PER_OP: usize = 200_000;

/// Body limit of the server's admin JSON endpoints (repack, verify-packs).
/// The server enforces it; a client sizes its plans below it.
pub const MAX_ADMIN_BODY_BYTES: u64 = 64 * 1024 * 1024;

/// Room left in a repack plan's body for everything but its operations: the
/// braces and the `hash` and `protocol_version` fields.
pub const REPACK_PLAN_JSON_ENVELOPE: u64 = 4096;

/// What a client may spend on one plan's operations.
pub const MAX_REPACK_PLAN_JSON_BYTES: u64 = MAX_ADMIN_BODY_BYTES - REPACK_PLAN_JSON_ENVELOPE;

/// Minimum protocol version the server accepts.
///
/// Bump this when a new version introduces breaking semantic changes
/// that make older request formats unsafe to process.
pub const MIN_PROTOCOL_VERSION: u32 = 1;

/// Validate a request's protocol version. Returns `Err(message)` if incompatible.
///
/// Compatibility contract:
/// - `version == 0` → legacy client (pre-versioning). Accepted while
///   `MIN_PROTOCOL_VERSION == 1`. When a future breaking change bumps MIN to 2,
///   legacy clients are rejected.
/// - `version < MIN_PROTOCOL_VERSION` (and != 0) → client too old, reject
/// - `version > PROTOCOL_VERSION` → client too new, reject
/// - `MIN_PROTOCOL_VERSION <= version <= PROTOCOL_VERSION` → accepted
///
/// # Errors
///
/// Returns a message when `version` is older than [`MIN_PROTOCOL_VERSION`] or
/// newer than [`PROTOCOL_VERSION`].
pub fn check_protocol_version(version: u32) -> Result<(), String> {
    if version == 0 {
        // Legacy client (pre-versioning). Accept while MIN == 1.
        if MIN_PROTOCOL_VERSION > 1 {
            return Err(format!(
                "legacy client (no protocol version); server requires >= {MIN_PROTOCOL_VERSION}"
            ));
        }
        return Ok(());
    }
    if version < MIN_PROTOCOL_VERSION {
        return Err(format!(
            "protocol version {version} too old; server requires >= {MIN_PROTOCOL_VERSION}"
        ));
    }
    if version > PROTOCOL_VERSION {
        return Err(format!(
            "protocol version {version} not supported; server supports <= {PROTOCOL_VERSION}"
        ));
    }
    Ok(())
}

// ── Sized listing (`GET ?list&sized`, `GET ?list&sized&mtime`) ─────────────

/// One entry of a `?list&sized` response. Servers that predate the `sized`
/// parameter ignore it and return plain key strings instead.
///
/// `?list&sized&mtime` adds `mtime_ms` to every entry. The flag is additive:
/// a server that predates it ignores the unknown query parameter and answers
/// as for `?list&sized`, so a client reads a missing `mtime_ms` as "not
/// reported". Without `&mtime` the field is omitted, keeping the
/// `?list&sized` response byte-identical to older servers'.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SizedKey {
    pub key: String,
    pub size: u64,
    /// Last modification, ms since the Unix epoch, by the server's clock. Sent
    /// only when the request has `&mtime`; absent from older servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_ms: Option<u64>,
}

// ── Batch object sizes (`POST /<dir>?sizes`) ──────────────────────────────

/// Most keys one `POST /<dir>?sizes` request may name. Clients split larger
/// batches; the server rejects a request above it with 400.
///
/// The request body is a JSON array of full keys, every one below `<dir>/`;
/// the response is a JSON array of the same length and order holding each
/// object's size in bytes, or `null` when it does not exist. A server that
/// predates the endpoint answers 400, which clients treat as "unsupported".
pub const MAX_OBJECT_SIZES_KEYS: usize = 100_000;

// ── Server capabilities (`GET /health`) ────────────────────────────────────

/// The unauthenticated `GET /health` response.
///
/// `protocol_version` and `hashes` are additive: a pre-BLAKE3 server omits
/// both, so the client sees `protocol_version` 0 and an empty `hashes` list,
/// i.e. "blake2b only". That is what makes the `init` pre-flight probe able to
/// tell an old server from a new one without a separate endpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerCapabilities {
    pub status: String,
    pub version: String,
    #[serde(default)]
    pub protocol_version: u32,
    /// Content-digest algorithms the server can verify, by wire name.
    ///
    /// `Vec<String>` rather than `Vec<HashAlgorithm>` deliberately: a *newer*
    /// server may advertise an algorithm this binary has never heard of, and
    /// that must not fail deserialization of the whole response.
    #[serde(default)]
    pub hashes: Vec<String>,
    /// The server accepts pack uploads in parts (`PUT packs/..?upload=..&offset=..&total=..`,
    /// then `POST packs/..?complete&upload=..`), so a client can send one pack
    /// over several connections. Absent on older servers, which a client reads
    /// as "whole-object PUT only".
    #[serde(default)]
    pub pack_parts: bool,
    /// The server serves the segmented index (format v6): it accepts
    /// `index.d/` keys, named by the BLAKE3 of their bytes (checked on every
    /// PUT). Absent on older servers.
    #[serde(default)]
    pub segmented_index: bool,
    /// Top-level key directories the server accepts (its `KNOWN_ROOT_DIRS`).
    /// Strings, like `hashes`, so a newer server's directories do not fail
    /// parsing. Absent on older servers: empty, i.e. only the directories every
    /// server accepted.
    #[serde(default)]
    pub key_dirs: Vec<String>,
    /// The server honours `If-None-Match: *` on PUT (412 when the key exists),
    /// atomically. Absent on older servers, which ignore the header and
    /// overwrite: a client must not send a conditional PUT without it.
    #[serde(default)]
    pub conditional_put: bool,
    /// The server answers `POST /<dir>?sizes` (see [`MAX_OBJECT_SIZES_KEYS`]).
    /// Absent on servers that predate the field; servers since 0.23 have the
    /// endpoint, and a client falls back on a 400 from an older one.
    #[serde(default)]
    pub object_sizes: bool,
    /// The server lists a whole tree in one `GET <prefix>?list&sized`. Absent on
    /// servers that predate the field, which all serve the listing.
    #[serde(default)]
    pub list_tree: bool,
    /// The server runs `POST /?verify-packs`. Absent on servers that predate the
    /// field, which all serve it.
    #[serde(default)]
    pub verify_packs: bool,
    /// The server runs `POST /?repack`. A client repacks server-side only when
    /// this is advertised; older servers omit it and get a client-side repack.
    #[serde(default)]
    pub repack: bool,
    /// The server accepts [`RepackOperationRequest::trailer`] (protocol
    /// version 3). Servers that predate it omit the field; a client sends a
    /// trailer only when it is advertised.
    #[serde(default)]
    pub repack_trailer: bool,
    /// The server runs in append-only mode: it refuses overwrites and deletes
    /// per the key-class table ([`KeyClass::append_only_allows`]), which
    /// includes deletes of `index.d/`, `index.h/` and `packmaps/` keys. A
    /// client skips work the server would refuse. Absent on servers that
    /// predate the field, which reads as `false`: such a server may still be
    /// append-only, so `false` means "not known to be".
    #[serde(default)]
    pub append_only: bool,
}

impl ServerCapabilities {
    /// Whether the server can verify uploads under `algo`.
    ///
    /// BLAKE2b is unconditional: every server that ever existed verifies it,
    /// and the oldest ones advertise nothing at all.
    pub fn supports_hash(&self, algo: HashAlgorithm) -> bool {
        algo == HashAlgorithm::Blake2b || self.hashes.iter().any(|h| h == algo.as_str())
    }

    /// Whether the server accepts keys under `dir` (a [`KNOWN_ROOT_DIRS`] name).
    pub fn accepts_key_dir(&self, dir: &str) -> bool {
        self.key_dirs.iter().any(|d| d == dir)
    }

    /// Whether the server accepts every key directory of format v6
    /// ([`V6_KEY_DIRS`]). A server that predates them answers 400 to those keys.
    pub fn serves_v6_key_dirs(&self) -> bool {
        V6_KEY_DIRS.iter().all(|d| self.accepts_key_dir(d))
    }
}

// ── Pack part uploads ──────────────────────────────────────────────────────

/// Length of a part-upload token: lowercase hex, chosen by the client per
/// upload attempt (128 random bits).
pub const PART_UPLOAD_TOKEN_LEN: usize = 32;

/// Whether `token` is a well-formed part-upload token. The server builds a
/// file name from it, so anything but [`PART_UPLOAD_TOKEN_LEN`] lowercase hex
/// characters is refused.
pub fn is_valid_upload_token(token: &str) -> bool {
    token.len() == PART_UPLOAD_TOKEN_LEN
        && token
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

// ── Repack wire types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackBlobRef {
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackOperationRequest {
    pub source_pack: String,
    pub keep_blobs: Vec<RepackBlobRef>,
    pub delete_after: bool,
    /// Bytes the server appends to this operation's output pack as one more
    /// length-prefixed blob (`[u32 LE len][trailer]`) after the last kept blob.
    /// The server treats them as opaque; their format is the client's (the pack
    /// trailer, 05-index-gc-format.md). The pack ID covers them. Requires
    /// protocol version [`REPACK_TRAILER_PROTOCOL_VERSION`]; send only to a
    /// server that advertises `repack_trailer`. Non-empty, at most
    /// [`MAX_REPACK_TRAILER_BYTES`], and only on an operation that keeps blobs.
    /// JSON: a lowercase hex string; omitted when `None`, so a plan without
    /// trailers serializes exactly as before the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_hex")]
    pub trailer: Option<Vec<u8>>,
}

/// `Option<Vec<u8>>` as an optional hex string (JSON has no byte type, and a
/// number array costs ~3.6 bytes per byte).
mod opt_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(bytes) => s.serialize_str(&hex::encode(bytes)),
            None => s.serialize_none(),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|s| hex::decode(s).map_err(serde::de::Error::custom))
            .transpose()
    }
}

/// Exact on-disk output size of one repack operation: `PACK_HEADER_SIZE +
/// Σ(4 + blob.length)`, plus `4 + trailer.len()` when the operation carries
/// a trailer. Delete-only operations (empty `keep_blobs`) produce no
/// pack, so they count as 0. Saturating.
///
/// Shared by the server (plan validation against [`MAX_REPACK_OUTPUT_BYTES`])
/// and the client (pre-chunking plans), so both sides agree by construction.
pub fn repack_op_output_size(op: &RepackOperationRequest) -> u64 {
    if op.keep_blobs.is_empty() {
        return 0;
    }
    let mut total = u64::try_from(PACK_HEADER_SIZE).unwrap_or(u64::MAX);
    for blob in &op.keep_blobs {
        total = total.saturating_add(4).saturating_add(blob.length);
    }
    if let Some(t) = &op.trailer {
        total = total
            .saturating_add(4)
            .saturating_add(u64::try_from(t.len()).unwrap_or(u64::MAX));
    }
    total
}

/// JSON bytes the keys, brackets and `delete_after` of one operation take,
/// with the comma separating it from the next.
const REPACK_OP_JSON_FIXED: u64 = 96;
/// JSON bytes of one `{"offset":N,"length":N}` object with its comma, for
/// any two u64 values.
const REPACK_BLOB_JSON_MAX: u64 = 64;
/// Worst-case JSON bytes of one key character (a `\u00XX` escape).
const JSON_ESCAPED_CHAR_MAX: u64 = 6;

/// Upper bound of the JSON bytes `op` adds to a plan's operations array,
/// separating comma included, with its trailer if it carries one. Numbers
/// count as 20 digits and key characters as 6 (worst-case escape), so the
/// bound holds for any value. Saturating.
pub fn repack_op_json_bound(op: &RepackOperationRequest) -> u64 {
    let key = u64::try_from(op.source_pack.len()).unwrap_or(u64::MAX);
    let blobs = u64::try_from(op.keep_blobs.len()).unwrap_or(u64::MAX);
    let trailer = op.trailer.as_ref().map_or(0, |t| {
        repack_trailer_json_len(u64::try_from(t.len()).unwrap_or(u64::MAX))
    });
    REPACK_OP_JSON_FIXED
        .saturating_add(key.saturating_mul(JSON_ESCAPED_CHAR_MAX))
        .saturating_add(blobs.saturating_mul(REPACK_BLOB_JSON_MAX))
        .saturating_add(trailer)
}

/// JSON bytes a trailer of `len` bytes adds to an operation: the
/// `,"trailer":` key and a quoted string of `2 * len` hex digits.
/// Saturating.
pub const fn repack_trailer_json_len(len: u64) -> u64 {
    len.saturating_mul(2).saturating_add(16)
}

/// The protocol version a request must declare to be safe under `hash`.
///
/// BLAKE3 requires 2 so an old server refuses the request outright. Silently
/// ignoring an unknown `hash` field would be actively dangerous: `verify_packs`
/// would report `hash_valid: false` for *every* pack — a false
/// whole-repository corruption report — and `repack` would write packs at
/// BLAKE2b-named keys, breaking the `pack_id == hash(contents)` invariant.
fn required_protocol_version(hash: HashAlgorithm) -> u32 {
    match hash {
        HashAlgorithm::Blake2b => 1,
        HashAlgorithm::Blake3 => 2,
    }
}

/// A server-side repack plan.
///
/// `hash` and `protocol_version` are **private with read-only accessors**, and
/// the only way to build one is [`RepackPlanRequest::new`], which derives the
/// version from the algorithm and from whether any operation carries a
/// trailer. That makes the dangerous pair — BLAKE3 declared
/// at protocol 1 — unrepresentable in Rust.
///
/// `#[non_exhaustive]` would not be a substitute: it blocks external struct
/// literals but leaves `pub` fields assignable, so
/// `r.protocol_version = 1` would still compile.
///
/// Private fields still deserialize normally, so the server side is
/// unaffected — but a hand-crafted JSON body can declare any pair, which is
/// why the server validates the invariant semantically as well.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackPlanRequest {
    pub operations: Vec<RepackOperationRequest>,
    #[serde(default)]
    hash: HashAlgorithm,
    #[serde(default)]
    protocol_version: u32,
}

impl RepackPlanRequest {
    pub fn new(operations: Vec<RepackOperationRequest>, hash: HashAlgorithm) -> Self {
        let mut protocol_version = required_protocol_version(hash);
        if operations.iter().any(|op| op.trailer.is_some()) {
            protocol_version = protocol_version.max(REPACK_TRAILER_PROTOCOL_VERSION);
        }
        Self {
            operations,
            hash,
            protocol_version,
        }
    }

    /// Whether any operation carries a trailer.
    pub fn has_trailer(&self) -> bool {
        self.operations.iter().any(|op| op.trailer.is_some())
    }

    /// Whether the declared protocol version is high enough for the trailers
    /// the plan carries. `operations` is public, so a trailer added after
    /// [`Self::new`] can leave the version behind; the client checks this
    /// before sending and the server before executing.
    ///
    /// # Errors
    ///
    /// A message when an operation carries a trailer and the declared version
    /// is below [`REPACK_TRAILER_PROTOCOL_VERSION`].
    pub fn validate_trailer_version(&self) -> Result<(), String> {
        if self.has_trailer() && self.protocol_version < REPACK_TRAILER_PROTOCOL_VERSION {
            return Err(format!(
                "a repack trailer requires protocol version >= {REPACK_TRAILER_PROTOCOL_VERSION}, \
                 but the plan declared {}",
                self.protocol_version
            ));
        }
        Ok(())
    }

    /// The algorithm the server must name output packs with.
    pub fn hash(&self) -> HashAlgorithm {
        self.hash
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Whether `hash` and `protocol_version` are a pair this binary would have
    /// produced. Only a hand-crafted request body can fail this.
    ///
    /// # Errors
    ///
    /// Returns a message when the declared version is too low for the
    /// declared algorithm.
    pub fn validate_hash_pairing(&self) -> Result<(), String> {
        validate_hash_pairing(self.hash, self.protocol_version)
    }
}

/// Shared by both plan types: a declared algorithm must come with a protocol
/// version high enough to mean it.
fn validate_hash_pairing(hash: HashAlgorithm, protocol_version: u32) -> Result<(), String> {
    let required = required_protocol_version(hash);
    // Version 0 is a pre-versioning client, which `check_protocol_version`
    // accepts as 1 — and which by definition predates BLAKE3, so it can only
    // legitimately pair with the default algorithm.
    let effective = if protocol_version == 0 {
        1
    } else {
        protocol_version
    };
    if effective >= required {
        return Ok(());
    }
    Err(format!(
        "hash {} requires protocol version >= {required}, but the request declared {protocol_version}",
        hash.as_str()
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackOperationResult {
    pub source_pack: String,
    pub new_pack: Option<String>,
    pub new_offsets: Vec<u64>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackResultResponse {
    pub completed: Vec<RepackOperationResult>,
}

// ── Verify-packs wire types ────────────────────────────────────────────────

/// A single blob expected at a given offset+length in a pack.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyBlobRef {
    pub offset: u64,
    pub length: u64,
}

/// Request to verify a single pack file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyPackRequest {
    pub pack_key: String,
    /// Estimated on-disk size of the pack (used for server-side rate limiting).
    #[serde(default)]
    pub expected_size: u64,
    pub expected_blobs: Vec<VerifyBlobRef>,
}

/// Batch request to verify multiple packs.
///
/// Same construction discipline as [`RepackPlanRequest`]: `hash` and
/// `protocol_version` are private and derived together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyPacksPlanRequest {
    pub packs: Vec<VerifyPackRequest>,
    #[serde(default)]
    hash: HashAlgorithm,
    #[serde(default)]
    protocol_version: u32,
}

impl VerifyPacksPlanRequest {
    pub fn new(packs: Vec<VerifyPackRequest>, hash: HashAlgorithm) -> Self {
        Self {
            packs,
            hash,
            protocol_version: required_protocol_version(hash),
        }
    }

    /// The algorithm the server must recompute pack digests with.
    pub fn hash(&self) -> HashAlgorithm {
        self.hash
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// See [`RepackPlanRequest::validate_hash_pairing`].
    ///
    /// # Errors
    ///
    /// Returns a message when the declared version is too low for the
    /// declared algorithm.
    pub fn validate_hash_pairing(&self) -> Result<(), String> {
        validate_hash_pairing(self.hash, self.protocol_version)
    }
}

/// Result of verifying a single pack file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyPackResult {
    pub pack_key: String,
    pub hash_valid: bool,
    pub header_valid: bool,
    pub blobs_valid: bool,
    pub error: Option<String>,
}

/// Batch response from verify-packs endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyPacksResponse {
    pub results: Vec<VerifyPackResult>,
    /// True when the server stopped early (e.g. byte-volume cap reached).
    /// The client should re-queue unprocessed packs in a subsequent batch.
    #[serde(default)]
    pub truncated: bool,
}

// ── Repository layout ─────────────────────────────────────────────────────

/// Top-level file entries that can appear in a vykar repository root.
/// "manifest" is a v1 legacy artifact — remove once v1 clients are retired.
pub const KNOWN_ROOT_FILES: &[&str] = &["config", "index", "index.gen", "manifest"];

/// Top-level directory entries that can appear in a vykar repository root.
pub const KNOWN_ROOT_DIRS: &[&str] = &[
    "keys",
    "snapshots",
    "packs",
    "locks",
    "sessions",
    "pending_index",
    INDEX_OBJECT_DIR,
    HEAD_DIR,
    PACK_MAP_DIR,
];

/// Directory of the segmented index's write-once objects (format v6).
pub const INDEX_OBJECT_DIR: &str = "index.d";

/// Directory of the numbered index heads (format v6): `index.h/<gen>`.
pub const HEAD_DIR: &str = "index.h";

/// Directory of the pack maps (format v6): `packmaps/<64 hex>`.
pub const PACK_MAP_DIR: &str = "packmaps";

/// The key directories a format-v6 repository needs beyond the older layouts.
pub const V6_KEY_DIRS: &[&str] = &[HEAD_DIR, PACK_MAP_DIR];

/// Whether `key` is `<dir>/<64 lowercase hex>`.
fn is_hex_name_in(dir: &str, key: &str) -> bool {
    key.strip_prefix(dir)
        .and_then(|rest| rest.strip_prefix('/'))
        .is_some_and(|name| {
            name.len() == 64
                && name
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}

/// Whether `key` is exactly `index.d/<64 lowercase hex>`: an object of the
/// segmented index.
pub fn is_index_object_key(key: &str) -> bool {
    is_hex_name_in(INDEX_OBJECT_DIR, key)
}

/// Whether `key` is exactly `packmaps/<64 lowercase hex>`.
pub fn is_pack_map_key(key: &str) -> bool {
    is_hex_name_in(PACK_MAP_DIR, key)
}

/// The generation of a head key `index.h/<gen>`, where `<gen>` is a u64 in
/// canonical decimal (no sign, no leading zero), so one generation has exactly
/// one key. Raw key, no slash trimming, like [`is_index_object_key`].
pub fn head_generation(key: &str) -> Option<u64> {
    let name = key.strip_prefix(HEAD_DIR)?.strip_prefix('/')?;
    let generation: u64 = name.parse().ok()?;
    (generation.to_string() == name).then_some(generation)
}

/// The storage key of head generation `generation`.
pub fn head_key(generation: u64) -> String {
    format!("{HEAD_DIR}/{generation}")
}

/// Prefix of every temp file written next to its final destination for an
/// atomic rename (server PUT/repack, local backend). Shared with
/// [`is_temp_file`] so the writer and the matcher cannot drift.
pub const TEMP_FILE_PREFIX: &str = ".tmp.";

/// Returns true if `name` matches the temp-file naming convention
/// ([`TEMP_FILE_PREFIX`] followed by a random suffix), used for atomic writes.
///
/// Also matches the legacy `.repack_tmp.*` prefix so already-deployed repack
/// debris is still recognized after the prefix was unified to `.tmp.repack.*`.
///
/// Trailing slashes are trimmed before extracting the basename: callers use
/// this on raw request keys, and `snapshots/.tmp.x.1/` must classify the same
/// as `snapshots/.tmp.x.1` (path resolution trims the slashes too — a
/// mismatch would let a temp-named key slip through committed-key checks).
pub fn is_temp_file(name: &str) -> bool {
    let trimmed = name.trim_end_matches('/');
    let basename = trimmed.rsplit('/').next().unwrap_or(trimmed);
    basename.starts_with(TEMP_FILE_PREFIX) || basename.starts_with(".repack_tmp.")
}

/// Returns true if `key` is a temp file in the repository's own namespace: at
/// the root or under one of [`KNOWN_ROOT_DIRS`]. Sweeps use this so they never
/// touch files in foreign subdirectories, which delete-repo preserves.
pub fn is_repo_temp_file(key: &str) -> bool {
    is_temp_file(key)
        && match key.trim_start_matches('/').split_once('/') {
            None => true,
            Some((top, _)) => KNOWN_ROOT_DIRS.contains(&top),
        }
}

/// Returns true if `key` is a known vykar repository storage key.
///
/// Matches root files, directory-prefixed paths, and `.tmp.*` temp files.
pub fn is_known_repo_key(key: &str) -> bool {
    KNOWN_ROOT_FILES.contains(&key)
        || KNOWN_ROOT_DIRS
            .iter()
            .any(|d| key.starts_with(d) && key.as_bytes().get(d.len()) == Some(&b'/'))
        || is_temp_file(key)
}

// ── Transport-level validation ─────────────────────────────────────────────

/// Validate a pack storage key: must be `packs/<2-hex-shard>/<64-hex-id>`.
pub fn is_valid_pack_key(key: &str) -> bool {
    let mut iter = key.trim_matches('/').split('/');
    let (Some(prefix), Some(shard), Some(id), None) =
        (iter.next(), iter.next(), iter.next(), iter.next())
    else {
        return false;
    };
    prefix == "packs"
        && shard.len() == 2
        && shard.chars().all(|c| c.is_ascii_hexdigit())
        && id.len() == 64
        && id.chars().all(|c| c.is_ascii_hexdigit())
}

/// Path-traversal safety check without schema enforcement, for deleting
/// leftover files. Rejects null bytes, backslashes, empty segments, `.` and
/// `..`, but allows any filename. The empty key (the root) is accepted.
pub fn is_safe_relative_path(key: &str) -> bool {
    if key.contains('\0') || key.contains('\\') {
        return false;
    }
    let trimmed = key.trim_matches('/');
    if trimmed.is_empty() {
        return true;
    }
    !trimmed
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
}

/// Whether the server accepts `key` as a committed object key: path-safe as
/// [`is_safe_relative_path`], and inside the repository's key schema.
///
/// Temp-file names are never valid committed keys. Append-only mode allows
/// deleting temp-named files (uncommitted upload debris); if PUT could commit
/// an object under a temp-looking name (e.g. `snapshots/.tmp.x.1` — several
/// namespaces accept any basename), that object would be deletable in
/// append-only mode, breaking its guarantee.
pub fn is_valid_storage_key(key: &str) -> bool {
    classify_committed(key).is_some()
}

// ── Key classes ────────────────────────────────────────────────────────────

/// How an append-only server treats the objects under a key. One table for
/// PUT, DELETE and batch delete on the server and for the test doubles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyClass {
    /// `config`, `keys/*`, `snapshots/*`, `packs/<shard>/<id>`, `index.h/<gen>`,
    /// `packmaps/<hex>`, and the legacy `pending_index/*`. `index.h/<gen>` is
    /// write-once in every mode, not only append-only (see [`put_rule`]).
    WriteOnce,
    /// `index.d/<64 hex>`: named by the BLAKE3 of its bytes, which the server
    /// checks on every PUT. A PUT onto an existing object stores nothing. Not
    /// deletable in append-only mode.
    ContentAddressed,
    /// `index`, `index.gen`, `manifest`: overwritten in place by v2/v3 clients.
    LegacyMutable,
    /// `locks/*`, `sessions/*`: coordination state.
    Ephemeral,
    /// A temp-file name ([`is_temp_file`]) anywhere: upload debris, never a
    /// committed object. Says nothing about path safety.
    Temp,
}

/// What a request does to one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Verb {
    /// PUT of a key that does not exist yet.
    Create,
    /// PUT of a key that exists.
    Overwrite,
    /// DELETE, alone or in a batch.
    Delete,
}

impl KeyClass {
    pub const ALL: [KeyClass; 5] = [
        KeyClass::WriteOnce,
        KeyClass::ContentAddressed,
        KeyClass::LegacyMutable,
        KeyClass::Ephemeral,
        KeyClass::Temp,
    ];

    /// Whether an append-only server allows `verb` on a key of this class.
    /// Without append-only, every verb on every committed key is allowed.
    pub const fn append_only_allows(self, verb: Verb) -> bool {
        match (self, verb) {
            (Self::Temp, Verb::Create | Verb::Overwrite) => false,
            (Self::Temp, Verb::Delete) => true,
            (_, Verb::Create) => true,
            (Self::WriteOnce, Verb::Overwrite | Verb::Delete) => false,
            // Named by its BLAKE3, which the server checks on every PUT, so an
            // accepted PUT onto an existing object stores nothing new.
            (Self::ContentAddressed, Verb::Overwrite) => true,
            (Self::ContentAddressed, Verb::Delete) => false,
            (Self::LegacyMutable, Verb::Overwrite) => true,
            (Self::LegacyMutable, Verb::Delete) => false,
            (Self::Ephemeral, Verb::Overwrite | Verb::Delete) => true,
        }
    }
}

/// The class of the object `key` names: `Temp` for a temp-file name, else
/// the class of a committed object key that [`is_valid_storage_key`] accepts.
/// `None` for the root, directories, and keys outside the schema.
/// Leading and trailing slashes are ignored, as in the server's path resolution.
pub fn key_class(key: &str) -> Option<KeyClass> {
    if is_temp_file(key) {
        return Some(KeyClass::Temp);
    }
    match classify_committed(key)? {
        Entry::Object(class) => Some(class),
        Entry::Dir => None,
    }
}

/// `key_class(key)` allows `verb` on an append-only server. A key without a
/// class (a directory, an unknown key) allows nothing.
pub fn append_only_allows(key: &str, verb: Verb) -> bool {
    key_class(key).is_some_and(|class| class.append_only_allows(verb))
}

/// What a server checks on a PUT of a key in every mode, beyond the
/// append-only table (03 step 1a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutRule {
    /// No rule of its own: a PUT creates or replaces (subject to append-only).
    Plain,
    /// `index.d/<64 hex>`: the body's BLAKE3 must be the key's name, else 409.
    /// A PUT onto an existing object stores nothing and answers 204, without
    /// renewing its mtime.
    NamedByBlake3,
    /// `index.h/<gen>`: write-once in every mode. A PUT onto an existing head
    /// stores nothing: 204 when the bytes are the stored ones, else 409.
    WriteOnce,
}

/// The [`PutRule`] of `key`. Leading and trailing slashes are ignored, as in
/// [`key_class`]. Keys outside the schema are [`PutRule::Plain`] (the server
/// refuses them before this matters).
pub fn put_rule(key: &str) -> PutRule {
    match key_class(key) {
        Some(KeyClass::ContentAddressed) => PutRule::NamedByBlake3,
        Some(KeyClass::WriteOnce) if head_generation(key.trim_matches('/')).is_some() => {
            PutRule::WriteOnce
        }
        _ => PutRule::Plain,
    }
}

/// The name an `index.d/` key claims: its last path segment (64 lowercase
/// hex, as [`is_index_object_key`] requires). `None` for any other key.
/// Leading and trailing slashes are ignored.
pub fn index_object_name(key: &str) -> Option<&str> {
    let trimmed = key.trim_matches('/');
    if !is_index_object_key(trimmed) {
        return None;
    }
    trimmed.strip_prefix(INDEX_OBJECT_DIR)?.strip_prefix('/')
}

/// What a committed key names.
enum Entry {
    Dir,
    Object(KeyClass),
}

/// What a committed (non-temp) key names, or `None` outside the schema.
/// The one classifier behind [`is_valid_storage_key`] and [`key_class`].
fn classify_committed(key: &str) -> Option<Entry> {
    if key.contains('\0') || key.contains('\\') || is_temp_file(key) {
        return None;
    }
    let trimmed = key.trim_matches('/');
    if trimmed.is_empty() {
        return Some(Entry::Dir);
    }
    let parts: Vec<&str> = trimmed.split('/').collect();
    if parts
        .iter()
        .any(|part| part.is_empty() || *part == "." || *part == "..")
    {
        return None;
    }
    let (first, rest) = parts.split_first()?;
    let entry = match (*first, rest) {
        (
            "keys" | "snapshots" | "pending_index" | "locks" | "sessions" | "packs"
            | INDEX_OBJECT_DIR | HEAD_DIR | PACK_MAP_DIR,
            [],
        ) => Entry::Dir,
        ("keys" | "snapshots" | "pending_index", [_]) => Entry::Object(KeyClass::WriteOnce),
        ("locks" | "sessions", [_]) => Entry::Object(KeyClass::Ephemeral),
        ("packs", [shard]) if is_pack_shard(shard) => Entry::Dir,
        ("packs", [shard, id]) if is_pack_shard(shard) && is_pack_id(id) => {
            Entry::Object(KeyClass::WriteOnce)
        }
        (INDEX_OBJECT_DIR, [_]) if is_index_object_key(trimmed) => {
            Entry::Object(KeyClass::ContentAddressed)
        }
        (HEAD_DIR, [_]) if head_generation(trimmed).is_some() => Entry::Object(KeyClass::WriteOnce),
        (PACK_MAP_DIR, [_]) if is_pack_map_key(trimmed) => Entry::Object(KeyClass::WriteOnce),
        ("config", []) => Entry::Object(KeyClass::WriteOnce),
        ("index" | "index.gen" | "manifest", []) => Entry::Object(KeyClass::LegacyMutable),
        _ => return None,
    };
    Some(entry)
}

/// A pack shard directory name: two hex digits, either case.
fn is_pack_shard(name: &str) -> bool {
    name.len() == 2 && name.chars().all(|c| c.is_ascii_hexdigit())
}

/// A pack ID in a pack key: 64 hex digits, either case.
fn is_pack_id(name: &str) -> bool {
    name.len() == 64 && name.chars().all(|c| c.is_ascii_hexdigit())
}

/// Parse an HTTP `Range` header of the one form the client sends,
/// `bytes=<start>-<end>` (inclusive), into `(start, length)`.
///
/// # Errors
///
/// Returns a message for any other form, `end < start`, or a length that
/// overflows `u64`.
pub fn parse_byte_range(header: &str) -> Result<(u64, u64), String> {
    let range = header
        .strip_prefix("bytes=")
        .ok_or_else(|| "invalid Range header".to_string())?;
    let mut iter = range.split('-');
    let (Some(start), Some(end), None) = (iter.next(), iter.next(), iter.next()) else {
        return Err("invalid Range header".into());
    };
    let start: u64 = start
        .parse()
        .map_err(|_| "invalid range start".to_string())?;
    let end: u64 = end.parse().map_err(|_| "invalid range end".to_string())?;
    if end < start {
        return Err("invalid Range header: end before start".into());
    }
    let length = (end - start)
        .checked_add(1)
        .ok_or_else(|| "invalid Range header".to_string())?;
    Ok((start, length))
}

/// Validate a single blob reference from a wire-format request.
///
/// Returns `Ok(())` on success, `Err(message)` on failure.
/// `context` appears in error messages (e.g. "operation 3 blob 5").
///
/// # Errors
///
/// Returns a message when the blob length is zero, exceeds the pack format's
/// `u32` length field, or when `offset + length` overflows.
pub fn validate_blob_ref(offset: u64, length: u64, context: &str) -> Result<(), String> {
    if length == 0 {
        return Err(format!("blob length must be > 0 at {context}"));
    }
    if length > u64::from(u32::MAX) {
        return Err(format!("blob length exceeds pack format max at {context}"));
    }
    if offset.checked_add(length).is_none() {
        return Err(format!("blob range overflow at {context}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `repack_op_json_bound` is never below the bytes an operation (with
    /// its separating comma) really takes, and the envelope covers the rest.
    #[test]
    fn repack_op_json_bound_is_an_upper_bound() {
        let keys = [
            format!("packs/ab/{}", "ab".repeat(32)),
            "packs/\"\\\u{1}/x".to_string(),
        ];
        for key in &keys {
            for blobs in [0usize, 1, 1000] {
                for trailer in [None, Some(vec![0xA5; 1]), Some(vec![0xFF; 5000])] {
                    for extreme in [false, true] {
                        let n = if extreme { u64::MAX } else { 13 };
                        let op = RepackOperationRequest {
                            source_pack: key.clone(),
                            keep_blobs: (0..blobs)
                                .map(|_| RepackBlobRef {
                                    offset: n,
                                    length: n,
                                })
                                .collect(),
                            delete_after: extreme,
                            trailer: trailer.clone(),
                        };
                        let json = serde_json::to_vec(&op).unwrap().len() as u64;
                        assert!(
                            json < repack_op_json_bound(&op),
                            "{key:?}/{blobs}/{extreme}: {json} > {}",
                            repack_op_json_bound(&op)
                        );
                    }
                }
            }
        }
        let empty = RepackPlanRequest::new(Vec::new(), HashAlgorithm::Blake3);
        let mut with_trailer = empty.clone();
        with_trailer.protocol_version = REPACK_TRAILER_PROTOCOL_VERSION;
        let envelope = serde_json::to_vec(&with_trailer).unwrap().len() as u64;
        assert!(envelope < REPACK_PLAN_JSON_ENVELOPE, "{envelope}");
    }

    #[test]
    fn capabilities_without_conditional_put_read_as_false() {
        let caps: ServerCapabilities =
            serde_json::from_str(r#"{"status":"ok","version":"0.24.0"}"#).unwrap();
        assert!(!caps.conditional_put);
    }

    #[test]
    fn capabilities_without_operation_fields_read_as_false() {
        let caps: ServerCapabilities = serde_json::from_str(
            r#"{"status":"ok","version":"0.24.0","protocol_version":2,"hashes":["blake2b","blake3"]}"#,
        )
        .unwrap();
        assert!(!caps.object_sizes && !caps.list_tree && !caps.verify_packs);
        assert!(!caps.repack && !caps.repack_trailer);
        let caps: ServerCapabilities = serde_json::from_str(
            r#"{"status":"ok","version":"0.25.0","object_sizes":true,"list_tree":true,"verify_packs":true,"repack":true,"repack_trailer":true}"#,
        )
        .unwrap();
        assert!(caps.object_sizes && caps.list_tree && caps.verify_packs);
        assert!(caps.repack && caps.repack_trailer);
    }

    #[test]
    fn sized_key_mtime_is_optional_on_the_wire() {
        let k: SizedKey = serde_json::from_str(r#"{"key":"a","size":3}"#).unwrap();
        assert_eq!(k.mtime_ms, None);
        assert_eq!(
            serde_json::to_string(&k).unwrap(),
            r#"{"key":"a","size":3}"#
        );
        let k: SizedKey =
            serde_json::from_str(r#"{"key":"a","size":3,"mtime_ms":1700000000123}"#).unwrap();
        assert_eq!(k.mtime_ms, Some(1_700_000_000_123));
    }

    #[test]
    fn parse_byte_range_accepts_only_bytes_start_end() {
        assert_eq!(parse_byte_range("bytes=0-0"), Ok((0, 1)));
        assert_eq!(parse_byte_range("bytes=200-299"), Ok((200, 100)));
        assert_eq!(
            parse_byte_range(&format!("bytes=1-{}", u64::MAX)),
            Ok((1, u64::MAX))
        );
        for bad in [
            "",
            "bytes=",
            "bytes=5",
            "bytes=-5",
            "bytes=5-",
            "bytes=1-2-3",
            "bytes=9-8",
            "items=0-1",
            "bytes=0-1,4-5",
        ] {
            assert!(parse_byte_range(bad).is_err(), "{bad}");
        }
        assert!(parse_byte_range(&format!("bytes=0-{}", u64::MAX)).is_err());
    }

    // ── Serde default round-trip ───────────────────────────────────────

    /// A pre-versioning client sends neither field. It must read back as
    /// "protocol 0, BLAKE2b" — that default is what keeps old clients working.
    #[test]
    fn verify_plan_defaults_without_optional_fields() {
        let json = r#"{"packs":[]}"#;
        let plan: VerifyPacksPlanRequest = serde_json::from_str(json).unwrap();
        assert_eq!(plan.protocol_version(), 0);
        assert_eq!(plan.hash(), HashAlgorithm::Blake2b);
        assert!(plan.packs.is_empty());
    }

    #[test]
    fn verify_plan_round_trip_with_all_fields() {
        let plan = VerifyPacksPlanRequest::new(
            vec![VerifyPackRequest {
                pack_key: "packs/ab/abcd".repeat(5),
                expected_size: 1024,
                expected_blobs: vec![VerifyBlobRef {
                    offset: 13,
                    length: 100,
                }],
            }],
            HashAlgorithm::Blake3,
        );
        let json = serde_json::to_string(&plan).unwrap();
        let deser: VerifyPacksPlanRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.protocol_version(), 2);
        assert_eq!(deser.hash(), HashAlgorithm::Blake3);
        assert_eq!(deser.packs.first().unwrap().expected_size, 1024);
    }

    #[test]
    fn repack_plan_defaults_without_protocol_version() {
        let json = r#"{"operations":[]}"#;
        let plan: RepackPlanRequest = serde_json::from_str(json).unwrap();
        assert_eq!(plan.protocol_version(), 0);
        assert_eq!(plan.hash(), HashAlgorithm::Blake2b);
    }

    // ── hash / protocol_version pairing ────────────────────────────────

    /// The whole point of the private fields: the constructors cannot produce
    /// the dangerous pair, so a caller cannot either.
    #[test]
    fn constructors_pair_hash_with_protocol_version() {
        for (hash, expected) in [(HashAlgorithm::Blake2b, 1u32), (HashAlgorithm::Blake3, 2)] {
            let repack = RepackPlanRequest::new(Vec::new(), hash);
            assert_eq!(repack.hash(), hash);
            assert_eq!(repack.protocol_version(), expected);
            assert!(repack.validate_hash_pairing().is_ok());

            let verify = VerifyPacksPlanRequest::new(Vec::new(), hash);
            assert_eq!(verify.hash(), hash);
            assert_eq!(verify.protocol_version(), expected);
            assert!(verify.validate_hash_pairing().is_ok());
        }
    }

    /// A hand-crafted body can still declare any pair, so the server has to
    /// check it semantically. BLAKE3 at protocol 1 is the dangerous one.
    #[test]
    fn blake3_at_protocol_1_is_rejected() {
        let json = r#"{"operations":[],"hash":"blake3","protocol_version":1}"#;
        let plan: RepackPlanRequest = serde_json::from_str(json).unwrap();
        let err = plan.validate_hash_pairing().unwrap_err();
        assert!(err.contains("requires protocol version >= 2"), "got: {err}");

        let json = r#"{"packs":[],"hash":"blake3","protocol_version":1}"#;
        let plan: VerifyPacksPlanRequest = serde_json::from_str(json).unwrap();
        assert!(plan.validate_hash_pairing().is_err());
    }

    /// A legacy client declares version 0 and no hash at all; that pairing is
    /// fine, and must not be caught by the check above.
    #[test]
    fn legacy_pairing_is_accepted() {
        let json = r#"{"operations":[]}"#;
        let plan: RepackPlanRequest = serde_json::from_str(json).unwrap();
        assert!(plan.validate_hash_pairing().is_ok());
    }

    #[test]
    fn unknown_hash_name_fails_deserialization() {
        // This is what makes the server return 400 for an unknown algorithm:
        // nothing else validates the string.
        let json = r#"{"operations":[],"hash":"blake9","protocol_version":2}"#;
        assert!(serde_json::from_str::<RepackPlanRequest>(json).is_err());
    }

    #[test]
    fn verify_response_defaults_without_truncated() {
        let json = r#"{"results":[]}"#;
        let resp: VerifyPacksResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.truncated);
    }

    #[test]
    fn verify_pack_request_defaults_without_expected_size() {
        let json = r#"{"pack_key":"packs/ab/cc","expected_blobs":[]}"#;
        let req: VerifyPackRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.expected_size, 0);
    }

    // ── validate_pack_header ───────────────────────────────────────────

    fn header(version: u8) -> Vec<u8> {
        let mut hdr = PACK_MAGIC.to_vec();
        hdr.push(version);
        hdr
    }

    #[test]
    fn validate_pack_header_accepts_current_version() {
        assert_eq!(
            validate_pack_header(&header(PACK_VERSION_CURRENT)),
            Ok(PACK_VERSION_CURRENT)
        );
    }

    #[test]
    fn validate_pack_header_ignores_trailing_pack_bytes() {
        let mut pack = header(PACK_VERSION_CURRENT);
        pack.extend_from_slice(&[0xAA; 128]);
        assert_eq!(validate_pack_header(&pack), Ok(PACK_VERSION_CURRENT));
    }

    #[test]
    fn validate_pack_header_rejects_short_input() {
        for len in 0..PACK_HEADER_SIZE {
            let mut truncated = header(PACK_VERSION_CURRENT);
            truncated.truncate(len);
            let err = validate_pack_header(&truncated).unwrap_err();
            assert_eq!(err, "pack too small", "len {len}");
        }
    }

    #[test]
    fn validate_pack_header_rejects_bad_magic() {
        let mut hdr = b"XGERPACK".to_vec();
        hdr.push(PACK_VERSION_CURRENT);
        assert_eq!(hdr.len(), PACK_HEADER_SIZE);
        assert_eq!(
            validate_pack_header(&hdr).unwrap_err(),
            "invalid pack magic"
        );
    }

    #[test]
    fn validate_pack_header_rejects_out_of_range_version() {
        for version in [0u8, PACK_VERSION_MAX + 1, 255] {
            let err = validate_pack_header(&header(version)).unwrap_err();
            assert!(
                err.starts_with(&format!("unsupported pack version {version}")),
                "got: {err}"
            );
        }
    }

    // ── validate_blob_ref ──────────────────────────────────────────────

    #[test]
    fn validate_blob_ref_rejects_zero_length() {
        let err = validate_blob_ref(0, 0, "test").unwrap_err();
        assert!(err.contains("length must be > 0"));
    }

    #[test]
    fn validate_blob_ref_rejects_too_large_length() {
        let err = validate_blob_ref(0, u64::from(u32::MAX) + 1, "test").unwrap_err();
        assert!(err.contains("exceeds pack format max"));
    }

    #[test]
    fn validate_blob_ref_rejects_overflow() {
        let err = validate_blob_ref(u64::MAX, 1, "test").unwrap_err();
        assert!(err.contains("overflow"));
    }

    #[test]
    fn validate_blob_ref_accepts_valid() {
        assert!(validate_blob_ref(13, 100, "test").is_ok());
    }

    // ── is_valid_pack_key ──────────────────────────────────────────────

    #[test]
    fn valid_pack_key_accepted() {
        let key = format!("packs/ab/{}", "a1".repeat(32));
        assert!(is_valid_pack_key(&key));
    }

    #[test]
    fn pack_key_wrong_segments_rejected() {
        assert!(!is_valid_pack_key("packs/ab"));
        assert!(!is_valid_pack_key("packs/ab/cd/ef"));
        assert!(!is_valid_pack_key("notpacks/ab/abcd"));
    }

    #[test]
    fn pack_key_wrong_shard_length_rejected() {
        let key = format!("packs/abc/{}", "a1".repeat(32));
        assert!(!is_valid_pack_key(&key));
    }

    #[test]
    fn pack_key_non_hex_rejected() {
        let key = format!("packs/ab/{}", "zz".repeat(32));
        assert!(!is_valid_pack_key(&key));
    }

    // ── check_protocol_version ─────────────────────────────────────────

    #[test]
    fn protocol_version_0_legacy_accepted() {
        assert!(check_protocol_version(0).is_ok());
    }

    #[test]
    fn protocol_version_current_accepted() {
        assert!(check_protocol_version(PROTOCOL_VERSION).is_ok());
    }

    #[test]
    fn protocol_version_too_new_rejected() {
        let err = check_protocol_version(PROTOCOL_VERSION + 1).unwrap_err();
        assert!(err.contains("not supported"));
    }

    #[test]
    fn protocol_version_max_rejected() {
        let err = check_protocol_version(u32::MAX).unwrap_err();
        assert!(err.contains("not supported"));
    }

    // ── is_known_repo_key ─────────────────────────────────────────────

    #[test]
    fn known_root_files_accepted() {
        for f in KNOWN_ROOT_FILES {
            assert!(is_known_repo_key(f), "{f} should be known");
        }
    }

    #[test]
    fn repo_temp_file_only_in_repo_namespace() {
        assert!(is_repo_temp_file(".tmp.index.0"));
        assert!(is_repo_temp_file("packs/ab/.tmp.x"));
        assert!(is_repo_temp_file("snapshots/.repack_tmp.y"));
        assert!(!is_repo_temp_file("other-app/.tmp.cache"));
        assert!(!is_repo_temp_file("other-app/packs/.tmp.cache"));
        assert!(!is_repo_temp_file("packs/ab/abcd"));
    }

    #[test]
    fn known_dir_prefixed_keys_accepted() {
        assert!(is_known_repo_key("keys/repokey"));
        assert!(is_known_repo_key("snapshots/abc123"));
        assert!(is_known_repo_key("packs/ab/deadbeef"));
        assert!(is_known_repo_key("locks/lock.json"));
        assert!(is_known_repo_key("sessions/abc123.json"));
        assert!(is_known_repo_key("pending_index/session123"));
        assert!(is_known_repo_key(&format!("index.d/{}", "ab".repeat(32))));
    }

    #[test]
    fn index_object_keys() {
        let name = "0123456789abcdef".repeat(4);
        assert!(is_index_object_key(&format!("index.d/{name}")));
        for bad in [
            "index.d".to_string(),
            "index.d/".to_string(),
            format!("index.d/{}", name.to_uppercase()),
            format!("index.d/{}", &name[1..]),
            format!("index.d/{name}0"),
            format!("index.d/{name}/"),
            format!("index.d/x/{name}"),
            format!("index.dx/{name}"),
            format!("/index.d/{name}"),
            format!("index.d/.tmp.{}", &name[5..]),
        ] {
            assert!(!is_index_object_key(&bad), "{bad}");
        }
    }

    #[test]
    fn bare_dir_names_rejected() {
        for d in KNOWN_ROOT_DIRS {
            assert!(!is_known_repo_key(d), "bare dir '{d}' should not match");
        }
    }

    #[test]
    fn unknown_keys_rejected() {
        assert!(!is_known_repo_key("random_file"));
        assert!(!is_known_repo_key("data/something"));
    }

    // ── Key classes ───────────────────────────────────────────────────

    /// `is_valid_storage_key` before the key-class table, verbatim.
    fn legacy_is_valid_storage_key(key: &str) -> bool {
        if key.contains('\0') || key.contains('\\') {
            return false;
        }
        if is_temp_file(key) {
            return false;
        }
        let trimmed = key.trim_matches('/');
        if trimmed.is_empty() {
            return true;
        }
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts
            .iter()
            .any(|part| part.is_empty() || *part == "." || *part == "..")
        {
            return false;
        }
        let Some(first) = parts.first() else {
            return false;
        };
        match *first {
            "keys" | "snapshots" | "locks" | "sessions" | "pending_index" => {
                return (1..=2).contains(&parts.len());
            }
            "packs" => return legacy_is_valid_packs_key(&parts),
            INDEX_OBJECT_DIR => {
                return parts.len() == 1 || is_index_object_key(trimmed);
            }
            _ => {}
        }
        if KNOWN_ROOT_FILES.contains(first) {
            return parts.len() == 1;
        }
        false
    }

    fn legacy_is_valid_packs_key(parts: &[&str]) -> bool {
        let Some(first) = parts.first() else {
            return false;
        };
        if *first != "packs" {
            return false;
        }
        if parts.len() == 1 {
            return true;
        }
        let Some(shard) = parts.get(1) else {
            return false;
        };
        if shard.len() != 2 || !shard.chars().all(|c| c.is_ascii_hexdigit()) {
            return false;
        }
        if parts.len() == 2 {
            return true;
        }
        if parts.len() == 3 {
            let Some(pack) = parts.get(2) else {
                return false;
            };
            return pack.len() == 64 && pack.chars().all(|c| c.is_ascii_hexdigit());
        }
        false
    }

    /// The server's append-only PUT overwrite allowlist before the table.
    fn legacy_put_overwrite_allowed(key: &str) -> bool {
        key == "index"
            || key == "index.gen"
            || key == "manifest"
            || is_index_object_key(key)
            || key.starts_with("locks/")
            || key.starts_with("sessions/")
    }

    /// The server's append-only DELETE allowlist before the table.
    fn legacy_delete_allowed(key: &str) -> bool {
        is_temp_file(key)
            || is_index_object_key(key)
            || key.starts_with("locks/")
            || key.starts_with("sessions/")
    }

    /// The server's append-only batch-delete allowlist before the table.
    fn legacy_batch_delete_allowed(key: &str) -> bool {
        is_temp_file(key) || is_index_object_key(key)
    }

    /// Every sequence of up to four segments from a fixed alphabet, as
    /// `(key, slash_variants)`: the plain join, then with a leading and with a
    /// trailing slash.
    fn key_corpus() -> &'static [(String, [String; 2])] {
        static CORPUS: std::sync::OnceLock<Vec<(String, [String; 2])>> = std::sync::OnceLock::new();
        CORPUS.get_or_init(|| {
            let hex_lower = "a".repeat(64);
            let hex_upper = "A".repeat(64);
            let hex_short = "a".repeat(63);
            let segments: Vec<&str> = vec![
                "",
                ".",
                "..",
                "keys",
                "snapshots",
                "packs",
                "ab",
                "AB",
                "zz",
                &hex_lower,
                &hex_upper,
                &hex_short,
                "index.d",
                "index",
                "index.gen",
                "config",
                "manifest",
                "locks",
                "sessions",
                "pending_index",
                HEAD_DIR,
                PACK_MAP_DIR,
                "1",
                "01",
                ".tmp.x",
                ".repack_tmp.y",
                "x\\y",
                "nul\0",
            ];
            let mut seqs: Vec<Vec<&str>> = vec![vec![]];
            let mut frontier: Vec<Vec<&str>> = vec![vec![]];
            for _ in 0..4 {
                let mut next = Vec::new();
                for seq in &frontier {
                    for s in &segments {
                        let mut longer = seq.clone();
                        longer.push(s);
                        next.push(longer);
                    }
                }
                seqs.extend(next.iter().cloned());
                frontier = next;
            }
            seqs.into_iter()
                .map(|seq| {
                    let key = seq.join("/");
                    let variants = [format!("/{key}"), format!("{key}/")];
                    (key, variants)
                })
                .collect()
        })
    }

    fn first_segment(key: &str) -> &str {
        key.split('/').find(|s| !s.is_empty()).unwrap_or("")
    }

    #[test]
    fn is_valid_storage_key_is_unchanged_outside_v6_dirs() {
        for (key, variants) in key_corpus() {
            for k in std::iter::once(key).chain(variants) {
                if V6_KEY_DIRS.contains(&first_segment(k)) {
                    continue;
                }
                assert_eq!(
                    is_valid_storage_key(k),
                    legacy_is_valid_storage_key(k),
                    "{k:?}"
                );
            }
        }
    }

    #[test]
    fn key_class_agrees_with_is_valid_storage_key() {
        let v6 = [head_key(1), format!("{PACK_MAP_DIR}/{}", "0".repeat(64))];
        let corpus = key_corpus()
            .iter()
            .flat_map(|(key, variants)| std::iter::once(key).chain(variants))
            .chain(v6.iter());
        for k in corpus {
            match key_class(k) {
                Some(KeyClass::Temp) => assert!(is_temp_file(k), "{k:?}"),
                Some(_) => assert!(is_valid_storage_key(k), "{k:?}"),
                None => {}
            }
            assert_eq!(
                key_class(k) == Some(KeyClass::Temp),
                is_temp_file(k),
                "{k:?}"
            );
        }
        for f in KNOWN_ROOT_FILES {
            assert!(key_class(f).is_some(), "{f}");
        }
        for d in KNOWN_ROOT_DIRS.iter().chain(&[""]) {
            assert!(is_valid_storage_key(d), "{d}");
            assert_eq!(key_class(d), None, "{d}");
        }
    }

    #[test]
    fn v6_keys() {
        for good in ["index.h/1", "index.h/0", "index.h/18446744073709551615"] {
            assert_eq!(key_class(good), Some(KeyClass::WriteOnce), "{good}");
        }
        let digits21 = format!("index.h/{}", "1".repeat(21));
        for bad in [
            "index.h/01",
            "index.h/+1",
            &digits21,
            "index.h/18446744073709551616",
            "index.h/1/x",
            "index.h/a",
            "index.hx/1",
        ] {
            assert!(!is_valid_storage_key(bad), "{bad}");
            assert_eq!(key_class(bad), None, "{bad}");
        }
        let name = "0123456789abcdef".repeat(4);
        assert_eq!(
            key_class(&format!("packmaps/{name}")),
            Some(KeyClass::WriteOnce)
        );
        for bad in [
            format!("packmaps/{}", name.to_uppercase()),
            format!("packmaps/{}", &name[1..]),
            format!("packmaps/{name}0"),
            format!("packmaps/x/{name}"),
            "packmaps/.tmp.x".to_string(),
        ] {
            assert!(!is_valid_storage_key(&bad), "{bad}");
        }
        assert_eq!(key_class("packmaps/.tmp.x"), Some(KeyClass::Temp));
        for g in [0, 1, u64::MAX] {
            assert_eq!(head_generation(&head_key(g)), Some(g));
        }
        assert!(is_known_repo_key("index.h/1"));
        assert!(is_repo_temp_file("packmaps/.tmp.x"));
    }

    #[test]
    fn append_only_matches_the_legacy_allowlists() {
        for (key, _) in key_corpus() {
            let k = key.as_str();
            // The old allowlists compared the raw key; see the slash test below.
            if k != k.trim_matches('/') {
                continue;
            }
            if is_valid_storage_key(k) {
                assert_eq!(
                    append_only_allows(k, Verb::Overwrite),
                    legacy_put_overwrite_allowed(k),
                    "{k:?}"
                );
            }
            if is_valid_storage_key(k) || is_temp_file(k) {
                // 03 step 1a: index objects are no longer deletable in
                // append-only mode, which the old allowlists allowed.
                assert_eq!(
                    append_only_allows(k, Verb::Delete),
                    legacy_delete_allowed(k) && !is_index_object_key(k),
                    "{k:?}"
                );
                // Batch delete now follows single DELETE for locks and sessions.
                let expected = (legacy_batch_delete_allowed(k) && !is_index_object_key(k))
                    || key_class(k) == Some(KeyClass::Ephemeral);
                assert_eq!(append_only_allows(k, Verb::Delete), expected, "{k:?}");
            }
        }
    }

    #[test]
    fn append_only_ignores_surrounding_slashes() {
        for (key, variants) in key_corpus() {
            for k in std::iter::once(key).chain(variants) {
                for verb in [Verb::Create, Verb::Overwrite, Verb::Delete] {
                    assert_eq!(
                        append_only_allows(k, verb),
                        append_only_allows(k.trim_matches('/'), verb),
                        "{k:?} {verb:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn every_pack_key_is_write_once() {
        for (key, variants) in key_corpus() {
            for k in std::iter::once(key).chain(variants) {
                if is_valid_pack_key(k) {
                    assert_eq!(key_class(k), Some(KeyClass::WriteOnce), "{k:?}");
                }
            }
        }
    }

    #[test]
    fn policy_table() {
        use KeyClass::*;
        // (class, create, overwrite, delete) on an append-only server.
        let table = [
            (WriteOnce, true, false, false),
            (ContentAddressed, true, true, false),
            (LegacyMutable, true, true, false),
            (Ephemeral, true, true, true),
            (Temp, false, false, true),
        ];
        assert_eq!(table.map(|row| row.0), KeyClass::ALL);
        for (class, create, overwrite, delete) in table {
            assert_eq!(class.append_only_allows(Verb::Create), create, "{class:?}");
            assert_eq!(
                class.append_only_allows(Verb::Overwrite),
                overwrite,
                "{class:?}"
            );
            assert_eq!(class.append_only_allows(Verb::Delete), delete, "{class:?}");
        }
    }

    #[test]
    fn put_rules() {
        for (key, variants) in key_corpus() {
            for k in std::iter::once(key).chain(variants) {
                let trimmed = k.trim_matches('/');
                let rule = put_rule(k);
                assert_eq!(
                    rule == PutRule::NamedByBlake3,
                    key_class(k) == Some(KeyClass::ContentAddressed),
                    "{k:?}"
                );
                assert_eq!(
                    rule == PutRule::WriteOnce,
                    key_class(k) == Some(KeyClass::WriteOnce) && head_generation(trimmed).is_some(),
                    "{k:?}"
                );
                assert_eq!(rule, put_rule(trimmed), "{k:?}");
                match index_object_name(k) {
                    Some(name) => {
                        assert!(is_index_object_key(trimmed), "{k:?}");
                        assert_eq!(format!("{INDEX_OBJECT_DIR}/{name}"), trimmed, "{k:?}");
                    }
                    None => assert!(!is_index_object_key(trimmed), "{k:?}"),
                }
            }
        }
        let hex = "a".repeat(64);
        assert_eq!(put_rule(&format!("index.d/{hex}")), PutRule::NamedByBlake3);
        assert_eq!(
            index_object_name(&format!("/index.d/{hex}/")),
            Some(hex.as_str())
        );
        assert_eq!(put_rule("index.h/7"), PutRule::WriteOnce);
        assert_eq!(put_rule("index.h/07"), PutRule::Plain);
        assert_eq!(put_rule(&format!("packmaps/{hex}")), PutRule::Plain);
        assert_eq!(put_rule("config"), PutRule::Plain);
        assert_eq!(index_object_name("index.h/7"), None);
    }

    // ── Server capabilities / part uploads ────────────────────────────

    #[test]
    fn capabilities_without_pack_parts_default_to_false() {
        let caps: ServerCapabilities =
            serde_json::from_str(r#"{"status":"ok","version":"0.20.0","protocol_version":2}"#)
                .unwrap();
        assert!(!caps.pack_parts);
        let caps: ServerCapabilities = serde_json::from_str(
            r#"{"status":"ok","version":"0.21.0","protocol_version":2,"pack_parts":true}"#,
        )
        .unwrap();
        assert!(caps.pack_parts);
        assert!(!caps.append_only);
        let caps: ServerCapabilities =
            serde_json::from_str(r#"{"status":"ok","version":"0.25.0","append_only":true}"#)
                .unwrap();
        assert!(caps.append_only);
    }

    #[test]
    fn capabilities_without_key_dirs_default_to_empty() {
        let caps: ServerCapabilities =
            serde_json::from_str(r#"{"status":"ok","version":"0.24.0","protocol_version":2}"#)
                .unwrap();
        assert!(caps.key_dirs.is_empty());
        assert!(!caps.serves_v6_key_dirs());
        let caps: ServerCapabilities = serde_json::from_str(
            r#"{"status":"ok","version":"0.25.0","key_dirs":["index.h","packmaps","future"]}"#,
        )
        .unwrap();
        assert!(caps.serves_v6_key_dirs());
        assert!(caps.accepts_key_dir("future"));
        let caps: ServerCapabilities =
            serde_json::from_str(r#"{"status":"ok","version":"0.25.0","key_dirs":["index.h"]}"#)
                .unwrap();
        assert!(caps.accepts_key_dir(HEAD_DIR));
        assert!(!caps.serves_v6_key_dirs());
    }

    #[test]
    fn default_capabilities_are_the_oldest_server() {
        let caps = ServerCapabilities::default();
        assert!(caps.key_dirs.is_empty());
        assert!(!caps.pack_parts && !caps.segmented_index);
        assert!(!caps.object_sizes && !caps.list_tree && !caps.verify_packs);
        assert!(!caps.repack && !caps.repack_trailer);
        assert!(caps.supports_hash(HashAlgorithm::Blake2b));
        assert!(!caps.supports_hash(HashAlgorithm::Blake3));
    }

    #[test]
    fn upload_token_validation() {
        assert!(is_valid_upload_token(&"0123456789abcdef".repeat(2)));
        assert!(!is_valid_upload_token(&"0123456789ABCDEF".repeat(2)));
        assert!(!is_valid_upload_token("abc"));
        assert!(!is_valid_upload_token(&"g".repeat(32)));
        assert!(!is_valid_upload_token(&format!("{}/", "a".repeat(31))));
    }

    // ── is_temp_file ──────────────────────────────────────────────────

    #[test]
    fn temp_files_detected() {
        assert!(is_temp_file(".tmp.config.0"));
        assert!(is_temp_file("packs/ab/.tmp.deadbeef.0"));
        assert!(is_temp_file(".tmp.repack.0"));
    }

    #[test]
    fn legacy_repack_temp_files_detected() {
        // Debris from before the prefix was unified to `.tmp.repack.*`.
        assert!(is_temp_file(".repack_tmp.0"));
        assert!(is_temp_file("packs/ab/.repack_tmp.42"));
    }

    #[test]
    fn temp_files_detected_with_trailing_slash() {
        // Path resolution trims trailing slashes, so classification must too —
        // otherwise `snapshots/.tmp.x.1/` bypasses committed-key rejection.
        assert!(is_temp_file(".tmp.config.0/"));
        assert!(is_temp_file("snapshots/.tmp.evil.1/"));
        assert!(is_temp_file("packs/ab/.repack_tmp.42//"));
        assert!(!is_temp_file("config/"));
    }

    #[test]
    fn non_temp_files_not_detected() {
        assert!(!is_temp_file("config"));
        assert!(!is_temp_file("tmp.config"));
    }

    #[test]
    fn repack_op_output_size_formula() {
        let op = |blobs: Vec<RepackBlobRef>| RepackOperationRequest {
            source_pack: "packs/ab/ab".to_string(),
            keep_blobs: blobs,
            delete_after: true,
            trailer: None,
        };
        // Delete-only ops produce no pack.
        assert_eq!(repack_op_output_size(&op(vec![])), 0);
        // Header + per-blob length prefix + blob bytes.
        let blobs = vec![
            RepackBlobRef {
                offset: 13,
                length: 100,
            },
            RepackBlobRef {
                offset: 117,
                length: 25,
            },
        ];
        assert_eq!(
            repack_op_output_size(&op(blobs)),
            PACK_HEADER_SIZE as u64 + (4 + 100) + (4 + 25)
        );
    }

    // ── repack trailer ─────────────────────────────────────────────────

    const TRAILER_TEST_PACK: &str =
        "packs/ab/ab00000000000000000000000000000000000000000000000000000000000000";

    fn trailer_test_op(trailer: Option<Vec<u8>>) -> RepackOperationRequest {
        RepackOperationRequest {
            source_pack: TRAILER_TEST_PACK.to_string(),
            keep_blobs: vec![RepackBlobRef {
                offset: 13,
                length: 5,
            }],
            delete_after: false,
            trailer,
        }
    }

    /// A plan without trailers is byte-identical to what this binary sent
    /// before the field existed, so it keeps working against old servers.
    #[test]
    fn plan_without_trailer_serializes_as_before() {
        for (hash, name, version) in [
            (HashAlgorithm::Blake3, "blake3", 2),
            (HashAlgorithm::Blake2b, "blake2b", 1),
        ] {
            let plan = RepackPlanRequest::new(vec![trailer_test_op(None)], hash);
            let expected = format!(
                r#"{{"operations":[{{"source_pack":"{TRAILER_TEST_PACK}","keep_blobs":[{{"offset":13,"length":5}}],"delete_after":false}}],"hash":"{name}","protocol_version":{version}}}"#
            );
            assert_eq!(serde_json::to_string(&plan).unwrap(), expected);
        }
    }

    #[test]
    fn old_shape_operation_decodes_without_trailer() {
        let json = format!(
            r#"{{"operations":[{{"source_pack":"{TRAILER_TEST_PACK}","keep_blobs":[{{"offset":13,"length":5}}],"delete_after":false}}],"hash":"blake3","protocol_version":2}}"#
        );
        let plan: RepackPlanRequest = serde_json::from_str(&json).unwrap();
        assert!(plan.operations.first().unwrap().trailer.is_none());
        assert!(!plan.has_trailer());
    }

    #[test]
    fn trailer_round_trips_as_hex() {
        let op = trailer_test_op(Some(vec![0x01, 0xab, 0xff]));
        let json = serde_json::to_string(&op).unwrap();
        assert!(json.ends_with(r#""trailer":"01abff"}"#), "got: {json}");
        let back: RepackOperationRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.trailer, Some(vec![0x01, 0xab, 0xff]));

        let json = format!(
            r#"{{"source_pack":"{TRAILER_TEST_PACK}","keep_blobs":[],"delete_after":false,"trailer":null}}"#
        );
        let back: RepackOperationRequest = serde_json::from_str(&json).unwrap();
        assert!(back.trailer.is_none());
    }

    #[test]
    fn invalid_trailer_hex_fails_deserialization() {
        for bad in ["abc", "zz"] {
            let json = format!(
                r#"{{"source_pack":"{TRAILER_TEST_PACK}","keep_blobs":[],"delete_after":false,"trailer":"{bad}"}}"#
            );
            assert!(
                serde_json::from_str::<RepackOperationRequest>(&json).is_err(),
                "{bad} decoded"
            );
        }
    }

    #[test]
    fn trailer_plan_declares_protocol_3() {
        for hash in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            let plan = RepackPlanRequest::new(vec![trailer_test_op(Some(b"T".to_vec()))], hash);
            assert_eq!(plan.protocol_version(), REPACK_TRAILER_PROTOCOL_VERSION);
            assert!(plan.has_trailer());
            assert!(plan.validate_trailer_version().is_ok());
            assert!(plan.validate_hash_pairing().is_ok());
        }
    }

    #[test]
    fn trailer_at_protocol_2_is_rejected() {
        let json = format!(
            r#"{{"operations":[{{"source_pack":"{TRAILER_TEST_PACK}","keep_blobs":[{{"offset":13,"length":5}}],"delete_after":false,"trailer":"01"}}],"hash":"blake3","protocol_version":2}}"#
        );
        let plan: RepackPlanRequest = serde_json::from_str(&json).unwrap();
        let err = plan.validate_trailer_version().unwrap_err();
        assert!(err.contains("requires protocol version >= 3"), "got: {err}");

        // `operations` is public: a trailer added after `new` leaves the
        // version behind, and the check catches it.
        let mut plan = RepackPlanRequest::new(vec![trailer_test_op(None)], HashAlgorithm::Blake3);
        plan.operations.first_mut().unwrap().trailer = Some(b"T".to_vec());
        let err = plan.validate_trailer_version().unwrap_err();
        assert!(err.contains("requires protocol version >= 3"), "got: {err}");
    }

    /// Frozen copy of v0.24.0's repack plan types and version check
    /// (`git show v0.24.0:crates/vykar-protocol/src/lib.rs`). Never update it:
    /// it stands for the released server.
    mod v0_24 {
        use serde::Deserialize;
        use vykar_types::hash::HashAlgorithm;

        pub const PROTOCOL_VERSION: u32 = 2;

        #[derive(Debug, Deserialize)]
        pub struct RepackBlobRef {
            #[allow(dead_code)]
            pub offset: u64,
            #[allow(dead_code)]
            pub length: u64,
        }

        #[derive(Debug, Deserialize)]
        pub struct RepackOperationRequest {
            #[allow(dead_code)]
            pub source_pack: String,
            pub keep_blobs: Vec<RepackBlobRef>,
            #[allow(dead_code)]
            pub delete_after: bool,
        }

        #[derive(Debug, Deserialize)]
        pub struct RepackPlanRequest {
            pub operations: Vec<RepackOperationRequest>,
            #[serde(default)]
            #[allow(dead_code)]
            pub hash: HashAlgorithm,
            #[serde(default)]
            pub protocol_version: u32,
        }

        /// The comparison of 0.24's `check_protocol_version` for a version > 0.
        pub fn check_protocol_version(version: u32) -> Result<(), String> {
            if version > PROTOCOL_VERSION {
                return Err(format!(
                    "protocol version {version} not supported; server supports <= {PROTOCOL_VERSION}"
                ));
            }
            Ok(())
        }
    }

    /// A 0.24 server ignores the unknown `trailer` field (so the capability
    /// gate alone would let a trailer be dropped silently), but it checks the
    /// declared version first and refuses the plan by version.
    #[test]
    fn a_0_24_server_refuses_a_trailer_plan_by_version() {
        let plan = RepackPlanRequest::new(
            vec![trailer_test_op(Some(b"TRAILER".to_vec()))],
            HashAlgorithm::Blake3,
        );
        let json = serde_json::to_string(&plan).unwrap();
        let old: v0_24::RepackPlanRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(old.operations.first().unwrap().keep_blobs.len(), 1);
        assert_eq!(old.protocol_version, 3);
        let err = v0_24::check_protocol_version(old.protocol_version).unwrap_err();
        assert!(
            err.contains("protocol version 3 not supported"),
            "got: {err}"
        );
    }

    #[test]
    fn repack_op_output_size_counts_the_trailer() {
        let op = trailer_test_op(Some(b"abc".to_vec()));
        assert_eq!(
            repack_op_output_size(&op),
            PACK_HEADER_SIZE as u64 + (4 + 5) + (4 + 3)
        );
        let delete_only = RepackOperationRequest {
            keep_blobs: Vec::new(),
            delete_after: true,
            ..op
        };
        assert_eq!(repack_op_output_size(&delete_only), 0);
    }
}
