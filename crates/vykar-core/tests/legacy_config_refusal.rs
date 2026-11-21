//! Old releases must refuse a future repository `config` **by version**, not
//! with a parse error.
//!
//! The format-v6 `config` is a msgpack map. A user who opens a v6 repository
//! with 0.19.x or 0.20–0.24 must be told "unsupported repository version: 6",
//! which only happens if the release's `RepoConfig` decodes the map before its
//! version gate runs. `tests/vendored/` holds those releases' `config` types,
//! copied verbatim from the tags, and their decode-and-gate sequence; they are
//! compiled with the same rmp-serde the releases shipped
//! (`vendored_decoders_use_the_release_rmp_serde`).
//!
//! The rule this suite fixes: the v6 map carries all seven old `RepoConfig`
//! fields under their old names and with a compatible msgpack type, for good.
//! Five of them (`version`, `id`, `chunker_params`, `encryption`, `created`)
//! are what the refusal depends on; `min_pack_size` and `max_pack_size` have a
//! serde default in both releases.
//!
//! v6 does not exist yet: `v6_probe_fields` is a stand-in for its writer. When
//! the real encoder lands, it replaces the probe and every assertion stays.
//!
//! `scripts/check-old-release-refusal.sh` runs the same bytes through the
//! actual release binaries (`old_release_binaries_refuse_v6_map_by_version`).

#![allow(clippy::unwrap_used)]
#![allow(clippy::pedantic)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

mod common;

#[path = "vendored/v0_19_1.rs"]
mod v0_19_1;
#[path = "vendored/v0_24_0.rs"]
mod v0_24_0;

use chrono::{DateTime, TimeZone, Utc};
use serde::ser::{SerializeMap, Serializer};
use vykar_core::repo::{ChunkerParams, EncryptionMode};

use crate::common::fixtures;

/// Today's `RepoConfig` re-encodes every frozen fixture's `config` byte for
/// byte, so the type still reads and writes what released binaries wrote.
#[test]
fn repo_config_reencodes_every_fixture_config_byte_for_byte() {
    for (prefix, version) in [("v2", 2), ("v3", 3), ("v4", 4), ("v5", 5)] {
        for mode in ["none", "aes256gcm", "chacha20poly1305"] {
            let fx = fixtures::extract(prefix, mode, version);
            let bytes = std::fs::read(fx.repo_dir().join("config")).unwrap();
            let config: vykar_core::repo::RepoConfig =
                rmp_serde::from_slice(&bytes).unwrap_or_else(|e| panic!("{prefix}-{mode}: {e}"));
            assert_eq!(
                rmp_serde::to_vec(&config).unwrap(),
                bytes,
                "{prefix}-{mode}"
            );
        }
    }
}

/// The two outcomes of an old release's `config` gate that this suite tells apart.
#[derive(Debug)]
pub enum Refusal {
    /// `VykarError::Deserialization` in both releases: "deserialization error: …".
    Deserialization(rmp_serde::decode::Error),
    /// `VykarError::UnsupportedVersion`: "unsupported repository version: N".
    UnsupportedVersion(u32),
}

type Gate = fn(&[u8]) -> Result<(), Refusal>;

fn gate_0_19_1(bytes: &[u8]) -> Result<(), Refusal> {
    v0_19_1::open_gate(bytes).map(|_| ())
}

fn gate_0_24_0(bytes: &[u8]) -> Result<(), Refusal> {
    v0_24_0::open_gate(bytes).map(|_| ())
}

const RELEASES: &[(&str, Gate)] = &[("v0.19.1", gate_0_19_1), ("v0.24.0", gate_0_24_0)];

const MODES: &[EncryptionMode] = &[
    EncryptionMode::None,
    EncryptionMode::Aes256Gcm,
    EncryptionMode::Chacha20Poly1305,
];

// ── Probe encoder ──────────────────────────────────────────────────────────

/// One msgpack value in a hand-built map. `untagged` serializes the inner value as-is.
#[derive(Clone, serde::Serialize)]
#[serde(untagged)]
enum Val {
    U32(u32),
    U64(u64),
    /// Array of ints, as `Vec<u8>` encodes today.
    Bytes(Vec<u8>),
    Bin(#[serde(with = "serde_bytes")] Vec<u8>),
    Str(String),
    StrList(Vec<String>),
    U32List(Vec<u32>),
    Chunker(ChunkerParams),
    Encryption(EncryptionMode),
    Created(DateTime<Utc>),
}

/// Serializes as a msgpack map in the given key order.
struct MsgMap(Vec<(&'static str, Val)>);

impl serde::Serialize for MsgMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

fn encode_map(fields: Vec<(&'static str, Val)>) -> Vec<u8> {
    rmp_serde::to_vec_named(&MsgMap(fields)).unwrap()
}

/// Every value differs from its serde default, so a field the old struct
/// silently defaulted would show up as a mismatch.
struct Probe {
    version: u32,
    id: [u8; 32],
    chunker: ChunkerParams,
    encryption: EncryptionMode,
    created: DateTime<Utc>,
    min_pack_size: u32,
    max_pack_size: u32,
}

fn probe(version: u32, encryption: EncryptionMode) -> Probe {
    Probe {
        version,
        id: [0xA5; 32],
        chunker: ChunkerParams {
            min_size: 8192,
            avg_size: 16384,
            max_size: 65536,
        },
        encryption,
        created: Utc.with_ymd_and_hms(2026, 11, 21, 0, 0, 0).unwrap(),
        min_pack_size: 16 * 1024 * 1024,
        max_pack_size: 128 * 1024 * 1024,
    }
}

/// The v6 shape: the seven old fields under their old names, plus the new
/// `chunk_hash` and `features`, deliberately NOT in the old struct order.
///
/// Duplicate keys would be a serde "duplicate field" error; not tested.
fn v6_probe_fields(p: &Probe, features: &[&str]) -> Vec<(&'static str, Val)> {
    vec![
        (
            "features",
            Val::StrList(features.iter().map(|s| s.to_string()).collect()),
        ),
        ("chunk_hash", Val::Str("blake3".into())),
        ("max_pack_size", Val::U32(p.max_pack_size)),
        ("created", Val::Created(p.created)),
        ("encryption", Val::Encryption(p.encryption.clone())),
        ("chunker_params", Val::Chunker(p.chunker.clone())),
        ("min_pack_size", Val::U32(p.min_pack_size)),
        ("id", Val::Bytes(p.id.to_vec())),
        ("version", Val::U32(p.version)),
    ]
}

/// `fields` with `key`'s value replaced, or removed if `val` is `None`.
fn with_field(
    mut fields: Vec<(&'static str, Val)>,
    key: &str,
    val: Option<Val>,
) -> Vec<(&'static str, Val)> {
    let pos = fields
        .iter()
        .position(|(k, _)| *k == key)
        .unwrap_or_else(|| panic!("probe has no field {key}"));
    match val {
        Some(v) => fields[pos].1 = v,
        None => {
            fields.remove(pos);
        }
    }
    fields
}

fn v6_probe(mode: EncryptionMode) -> Vec<(&'static str, Val)> {
    v6_probe_fields(&probe(6, mode), &[])
}

/// The same nine values as `v6_probe_fields`, positional: the seven old fields
/// in the old struct order, then the two new ones appended.
#[derive(serde::Serialize)]
struct PositionalV6 {
    version: u32,
    id: Vec<u8>,
    chunker_params: ChunkerParams,
    encryption: EncryptionMode,
    created: DateTime<Utc>,
    min_pack_size: u32,
    max_pack_size: u32,
    chunk_hash: String,
    features: Vec<String>,
}

fn positional_v6(p: &Probe) -> Vec<u8> {
    rmp_serde::to_vec(&PositionalV6 {
        version: p.version,
        id: p.id.to_vec(),
        chunker_params: p.chunker.clone(),
        encryption: p.encryption.clone(),
        created: p.created,
        min_pack_size: p.min_pack_size,
        max_pack_size: p.max_pack_size,
        chunk_hash: "blake3".into(),
        features: Vec::new(),
    })
    .unwrap()
}

fn expect_version_refusal(release: &str, result: Result<(), Refusal>, what: &str) {
    match result {
        Err(Refusal::UnsupportedVersion(6)) => {}
        Err(Refusal::Deserialization(e)) => {
            panic!("{release}: {what}: parse error instead of a version refusal: {e}")
        }
        other => panic!("{release}: {what}: expected UnsupportedVersion(6), got {other:?}"),
    }
}

fn expect_parse_error(release: &str, result: Result<(), Refusal>, what: &str) {
    assert!(
        matches!(result, Err(Refusal::Deserialization(_))),
        "{release}: {what}: expected a parse error, got {result:?}"
    );
}

/// The variant name the vendored enums decode, for the live `EncryptionMode`.
fn variant_name(mode: &EncryptionMode) -> &'static str {
    match mode {
        EncryptionMode::None => "None",
        EncryptionMode::Aes256Gcm => "Aes256Gcm",
        EncryptionMode::Chacha20Poly1305 => "Chacha20Poly1305",
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[test]
fn old_releases_refuse_a_v6_map_by_version() {
    for (release, gate) in RELEASES {
        for mode in MODES {
            for features in [&[][..], &["some-future-feature"][..]] {
                let bytes = encode_map(v6_probe_fields(&probe(6, mode.clone()), features));
                expect_version_refusal(
                    release,
                    gate(&bytes),
                    &format!("{mode:?}, features {features:?}"),
                );
            }
        }
    }
}

/// Decoded without the gate, the probe yields every old field with its value:
/// none is silently defaulted.
#[test]
fn v6_map_carries_every_old_field() {
    for mode in MODES {
        let p = probe(6, mode.clone());
        let bytes = encode_map(v6_probe_fields(&p, &["some-future-feature"]));

        let old: v0_19_1::RepoConfig = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(old.version, 6);
        assert_eq!(old.id, p.id.to_vec());
        assert_eq!(
            (
                old.chunker_params.min_size,
                old.chunker_params.avg_size,
                old.chunker_params.max_size
            ),
            (p.chunker.min_size, p.chunker.avg_size, p.chunker.max_size),
        );
        assert_eq!(format!("{:?}", old.encryption), variant_name(mode));
        assert_eq!(old.created, p.created);
        assert_eq!(old.min_pack_size, p.min_pack_size);
        assert_eq!(old.max_pack_size, p.max_pack_size);

        let old: v0_24_0::RepoConfig = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(old.version, 6);
        assert_eq!(old.id, p.id.to_vec());
        assert_eq!(
            (
                old.chunker_params.min_size,
                old.chunker_params.avg_size,
                old.chunker_params.max_size
            ),
            (p.chunker.min_size, p.chunker.avg_size, p.chunker.max_size),
        );
        assert_eq!(format!("{:?}", old.encryption), variant_name(mode));
        assert_eq!(old.created, p.created);
        assert_eq!(old.min_pack_size, p.min_pack_size);
        assert_eq!(old.max_pack_size, p.max_pack_size);
    }
}

/// Positive control: the map encoding itself is readable, so the refusal
/// above comes from the version gate.
#[test]
fn old_releases_accept_a_map_config_at_their_own_version() {
    for (release, gate) in RELEASES {
        let v2 = encode_map(v6_probe_fields(&probe(2, EncryptionMode::Aes256Gcm), &[]));
        assert!(gate(&v2).is_ok(), "{release}: {:?}", gate(&v2));
    }
    let v3 = encode_map(v6_probe_fields(&probe(3, EncryptionMode::Aes256Gcm), &[]));
    assert!(gate_0_24_0(&v3).is_ok(), "{:?}", gate_0_24_0(&v3));
    assert!(
        matches!(gate_0_19_1(&v3), Err(Refusal::UnsupportedVersion(3))),
        "{:?}",
        gate_0_19_1(&v3)
    );
}

/// Appending two fields to the positional array, the change AGENTS.md
/// forbids, is a parse error in both releases: why v6 must be a map.
#[test]
fn a_positional_v6_config_is_a_parse_error() {
    let bytes = positional_v6(&probe(6, EncryptionMode::Aes256Gcm));
    for (release, gate) in RELEASES {
        let result = gate(&bytes);
        assert!(
            matches!(
                result,
                Err(Refusal::Deserialization(
                    rmp_serde::decode::Error::LengthMismatch(_)
                ))
            ),
            "{release}: {result:?}"
        );
    }
}

#[test]
fn dropping_a_required_old_field_is_a_parse_error() {
    for (release, gate) in RELEASES {
        for key in ["version", "id", "chunker_params", "encryption", "created"] {
            let bytes = encode_map(with_field(v6_probe(EncryptionMode::Aes256Gcm), key, None));
            expect_parse_error(release, gate(&bytes), &format!("without {key}"));
        }
        // Both have a serde default in both releases.
        for key in ["min_pack_size", "max_pack_size"] {
            let bytes = encode_map(with_field(v6_probe(EncryptionMode::Aes256Gcm), key, None));
            expect_version_refusal(release, gate(&bytes), &format!("without {key}"));
        }
    }
}

/// An unknown encryption mode is a parse error: a v6 writer that adds a mode
/// must keep a known name in `encryption` and put the new one elsewhere.
#[test]
fn retyping_an_old_field_is_a_parse_error() {
    let cases: Vec<(&str, &str, Val)> = vec![
        (
            "unknown encryption variant",
            "encryption",
            Val::Str("Xchacha20Poly1305".into()),
        ),
        ("version as a string", "version", Val::Str("6".into())),
        (
            "version above u32::MAX",
            "version",
            Val::U64(u64::from(u32::MAX) + 1),
        ),
        ("created as an integer", "created", Val::U64(0)),
        (
            "chunker_params as a 4-element array",
            "chunker_params",
            Val::U32List(vec![8192, 16384, 65536, 1]),
        ),
    ];
    for (release, gate) in RELEASES {
        for (what, key, val) in &cases {
            let bytes = encode_map(with_field(
                v6_probe(EncryptionMode::Aes256Gcm),
                key,
                Some(val.clone()),
            ));
            expect_parse_error(release, gate(&bytes), what);
        }
    }
}

/// The cases below only mean something if the probe really emits a map and a
/// msgpack `bin`.
#[test]
fn probe_encodings_have_the_claimed_wire_shape() {
    let map = encode_map(v6_probe(EncryptionMode::Aes256Gcm));
    assert_eq!(map[0], 0x89, "probe is not a 9-entry fixmap");
    let with_bin = encode_map(with_field(
        v6_probe(EncryptionMode::Aes256Gcm),
        "id",
        Some(Val::Bin(vec![0xA5; 32])),
    ));
    assert!(
        with_bin.windows(2).any(|w| w == [0xc4, 0x20]),
        "id was not encoded as bin8 of length 32"
    );
}

/// Encodings the old types accept besides the probe's: either is open to the
/// v6 writer.
#[test]
fn compatible_encodings_are_accepted() {
    let cases: Vec<(&str, &str, Val)> = vec![
        ("id as bin", "id", Val::Bin(vec![0xA5; 32])),
        (
            "chunker_params as a 3-element array",
            "chunker_params",
            Val::U32List(vec![8192, 16384, 65536]),
        ),
    ];
    for (release, gate) in RELEASES {
        for (what, key, val) in &cases {
            let bytes = encode_map(with_field(
                v6_probe(EncryptionMode::Aes256Gcm),
                key,
                Some(val.clone()),
            ));
            expect_version_refusal(release, gate(&bytes), what);
        }
    }
}

/// The vendored types decode `config` bytes real binaries wrote. The v2
/// fixtures come from development commit `bfd0a85` (an ancestor of v0.20.0,
/// not of v0.19.1), whose `RepoConfig`/`ChunkerConfig` text is identical to
/// both tags; the v3 fixtures from the v0.24.0 binary.
#[test]
fn vendored_types_decode_the_configs_those_releases_wrote() {
    for mode in ["none", "aes256gcm", "chacha20poly1305"] {
        let fx = fixtures::extract("v2", mode, 2);
        let bytes = std::fs::read(fx.repo_dir().join("config")).unwrap();
        let old = v0_19_1::open_gate(&bytes).unwrap_or_else(|e| panic!("v2-{mode}: {e:?}"));
        assert_eq!(old.version, 2, "v2-{mode}");
        let old = v0_24_0::open_gate(&bytes).unwrap_or_else(|e| panic!("v2-{mode}: {e:?}"));
        assert_eq!(old.version, 2, "v2-{mode}");

        let fx = fixtures::extract("v3", mode, 3);
        let bytes = std::fs::read(fx.repo_dir().join("config")).unwrap();
        let old = v0_24_0::open_gate(&bytes).unwrap_or_else(|e| panic!("v3-{mode}: {e:?}"));
        assert_eq!(old.version, 3, "v3-{mode}");
        assert!(
            matches!(
                v0_19_1::open_gate(&bytes),
                Err(Refusal::UnsupportedVersion(3))
            ),
            "v3-{mode}"
        );
    }
}

/// `(crate, version)` both tags lock; HEAD must lock the same so the vendored
/// types decode with the releases' decoder. Not pinned (patch-level drift
/// accepted): serde 1.0.228 (v0.19.1) / 1.0.229 (v0.24.0), chrono 0.4.44 /
/// 0.4.45.
const RELEASE_DECODER: &[(&str, &str)] = &[("rmp-serde", "1.3.1"), ("rmp", "0.8.15")];

#[test]
fn vendored_decoders_use_the_release_rmp_serde() {
    let lock =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock")).unwrap();
    for (name, pinned) in RELEASE_DECODER {
        let versions: Vec<&str> = lock
            .split("[[package]]")
            .filter(|block| {
                block
                    .lines()
                    .any(|l| l.trim() == format!("name = \"{name}\""))
            })
            .filter_map(|block| {
                block
                    .lines()
                    .find_map(|l| l.trim().strip_prefix("version = "))
                    .map(|v| v.trim_matches('"'))
            })
            .collect();
        assert_eq!(
            versions,
            vec![*pinned],
            "Cargo.lock no longer locks {name} {pinned} alone, so the vendored \
             types no longer decode with the old releases' decoder. Re-run \
             scripts/check-old-release-refusal.sh, then update this table"
        );
    }
}

// ── The release binaries themselves ────────────────────────────────────────

/// Runs `<bin> info` against a repository whose only object is `config`.
/// Returns `(success, stderr)`.
fn run_info(bin: &str, config_bytes: &[u8]) -> (bool, String) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let repo = root.join("repo");
    let src = root.join("src");
    let home = root.join("home");
    for dir in [&repo, &src, &home] {
        std::fs::create_dir_all(dir).unwrap();
    }
    std::fs::write(repo.join("config"), config_bytes).unwrap();
    // JSON is YAML; it also quotes Windows paths correctly.
    let yaml = serde_json::json!({
        "repositories": [{ "url": repo, "label": "main" }],
        "sources": [{ "path": src, "label": "probe" }],
        "cache_dir": root.join("cache"),
    });
    let yaml_path = root.join("vykar.yaml");
    std::fs::write(&yaml_path, yaml.to_string()).unwrap();

    let output = std::process::Command::new(bin)
        .arg("--config")
        .arg(&yaml_path)
        .arg("info")
        .env_remove("VYKAR_CONFIG")
        .env("HOME", &home)
        .env("XDG_CACHE_HOME", home.join(".cache"))
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_DATA_HOME", home.join(".local/share"))
        .env("USERPROFILE", &home)
        .env("LOCALAPPDATA", home.join("AppData"))
        .env("NO_COLOR", "1")
        .env("VYKAR_PASSPHRASE", "probe")
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap_or_else(|e| panic!("run {bin}: {e}"));
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Set by `scripts/check-old-release-refusal.sh`; skipped otherwise.
#[test]
#[allow(
    clippy::print_stderr,
    reason = "reports a skip and the binaries' messages"
)]
fn old_release_binaries_refuse_v6_map_by_version() {
    let required = std::env::var_os("VYKAR_REQUIRE_OLD_BINARIES").is_some_and(|v| v == "1");
    let bins: Vec<(&str, Option<String>)> = [
        ("v0.19.1", "VYKAR_BIN_V0_19_1"),
        ("v0.24.0", "VYKAR_BIN_V0_24_0"),
    ]
    .iter()
    .map(|(release, var)| (*release, std::env::var(var).ok()))
    .collect();
    if required {
        for (release, bin) in &bins {
            assert!(
                bin.is_some(),
                "VYKAR_REQUIRE_OLD_BINARIES=1 but no binary for {release} is set"
            );
        }
    }
    if bins.iter().all(|(_, b)| b.is_none()) {
        eprintln!(
            "skipped: VYKAR_BIN_V0_19_1/VYKAR_BIN_V0_24_0 unset, no old binary was \
             tested (run scripts/check-old-release-refusal.sh)"
        );
        return;
    }

    let p = probe(6, EncryptionMode::Aes256Gcm);
    let map = encode_map(v6_probe_fields(&p, &[]));
    let positional = positional_v6(&p);
    for (release, bin) in &bins {
        let Some(bin) = bin else {
            eprintln!("skipped {release}: no binary set");
            continue;
        };
        let (ok, stderr) = run_info(bin, &map);
        eprintln!("{release} map: {}", stderr.trim());
        assert!(!ok, "{release}: info succeeded on a v6 config");
        assert!(
            stderr.contains("unsupported repository version: 6"),
            "{release}: {stderr}"
        );
        assert!(
            !stderr.contains("deserialization error"),
            "{release}: {stderr}"
        );

        let (ok, stderr) = run_info(bin, &positional);
        eprintln!("{release} positional: {}", stderr.trim());
        assert!(!ok, "{release}: info succeeded on a positional v6 config");
        assert!(
            stderr.contains("deserialization error"),
            "{release}: {stderr}"
        );
    }
}
