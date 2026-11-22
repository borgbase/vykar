//! The index head: the small object at `index.h/<gen>` that names the live
//! base and segments of the index (format v6). One head per generation; the
//! highest generation is the repository's index state.
//!
//! Serialized as a msgpack **map** (layout 3) with the field names as keys.
//! Readers ignore unknown keys, so a field can be added later without a new
//! layout: such a field must carry `#[serde(default)]`, and one whose
//! meaning a reader must understand must also add a name to `features`,
//! which this binary refuses when it does not know the name. Every field
//! below is required on decode; a missing key is damage.
//!
//! The head names objects only, no state hash: the index state is the base
//! plus the segments applied in the head's order, and local files are
//! matched by object name.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use vykar_types::error::{Result, VykarError};

/// The only head layout this binary reads and writes. 2 was the
/// development layout of 05 step 8 (with the v1 chain fields); 1 the
/// positional head of v4/v5.
pub const HEAD_LAYOUT_VERSION: u32 = 3;

/// Head features this binary understands. A head that names any other is
/// refused as written by a newer vykar. Written empty.
pub const KNOWN_HEAD_FEATURES: &[&str] = &[];

/// Who writes a head of `layout`, for a refusal: a layout below this
/// binary's was only ever written by a development build, one above by a
/// newer vykar.
pub(crate) fn layout_writer(layout: u32) -> &'static str {
    if layout < HEAD_LAYOUT_VERSION {
        "it was written by an older development build of vykar; recreate the repository"
    } else {
        "it was written by a newer vykar. Upgrade vykar"
    }
}

/// The error for a head whose layout this binary does not read.
pub fn unsupported_layout(layout: u32) -> VykarError {
    VykarError::InvalidFormat(format!(
        "unsupported index head layout {layout} (this version reads {HEAD_LAYOUT_VERSION}); {}",
        layout_writer(layout)
    ))
}

/// The error for a head that names features this binary does not know.
pub fn unknown_features(names: &[String]) -> VykarError {
    VykarError::InvalidFormat(format!(
        "index head uses feature(s) {} that this vykar does not know; \
         it was written by a newer vykar. Upgrade vykar",
        names.join(", ")
    ))
}

/// The `layout` of an encoded head without decoding the rest. A map head
/// is read through a struct with the single field `layout`, ignoring every
/// other key; a positional (layout 1) head through its first array
/// element. None when neither works.
pub fn peek_layout(bytes: &[u8]) -> Option<u32> {
    #[derive(Deserialize)]
    struct LayoutOnly {
        layout: u32,
    }
    let mut rd = bytes;
    match rmp::decode::read_marker(&mut rd).ok()? {
        rmp::Marker::FixArray(_) | rmp::Marker::Array16 | rmp::Marker::Array32 => {
            let mut rd = bytes;
            if rmp::decode::read_array_len(&mut rd).ok()? == 0 {
                return None;
            }
            rmp::decode::read_int::<u32, _>(&mut rd).ok()
        }
        rmp::Marker::FixMap(_) | rmp::Marker::Map16 | rmp::Marker::Map32 => {
            rmp_serde::from_slice::<LayoutOnly>(bytes)
                .ok()
                .map(|l| l.layout)
        }
        _ => None,
    }
}

/// Name of a write-once object under `index.d/`: BLAKE3 of its stored bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectName(#[serde(with = "serde_bytes")] pub [u8; 32]);

impl ObjectName {
    /// The name of an object with these stored bytes.
    pub fn of(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes).into())
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

/// Who wrote a segment. On the wire a u8 (0 = backup, 1 = maintenance);
/// any other value is a decode error. A backup segment only adds entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "u8", try_from = "u8")]
pub enum SegmentKind {
    Backup,
    Maintenance,
}

impl From<SegmentKind> for u8 {
    fn from(kind: SegmentKind) -> u8 {
        match kind {
            SegmentKind::Backup => 0,
            SegmentKind::Maintenance => 1,
        }
    }
}

impl TryFrom<u8> for SegmentKind {
    type Error = String;

    fn try_from(v: u8) -> std::result::Result<Self, String> {
        match v {
            0 => Ok(SegmentKind::Backup),
            1 => Ok(SegmentKind::Maintenance),
            other => Err(format!("unknown index segment kind {other}")),
        }
    }
}

/// An `index.d/` object a head names that is not a base or a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub name: ObjectName,
    pub size: u64,
}

/// `generation`: the generation of the last head written by a sweep or by
/// `check --repair` (0: none since init or upgrade). `record`: the sweep
/// record object of the last sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SweepState {
    #[serde(rename = "gen")]
    pub generation: u64,
    pub record: Option<ObjectRef>,
}

/// A base object: the full sorted records of one index state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseRef {
    pub name: ObjectName,
    pub size: u64,
    /// Records in the base; bounds the decode.
    pub entries: u64,
}

/// A segment object and who wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRef {
    pub name: ObjectName,
    pub size: u64,
    pub kind: SegmentKind,
    /// Additions + removals + relocations; bounds the decode.
    pub entries: u64,
}

/// The head. `base: None` means the base state is the empty index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexHead {
    pub layout: u32,
    /// Its own generation: equal to the `<gen>` of its key. The wire key
    /// is `gen`.
    #[serde(rename = "gen")]
    pub generation: u64,
    /// Features a reader must understand; this binary writes none.
    pub features: Vec<String>,
    pub base: Option<BaseRef>,
    /// Applied over the base in this order.
    pub segments: Vec<SegmentRef>,
    /// Objects of the last head before the most recent fold, kept so a
    /// reader that loaded that head can finish.
    pub retired: Vec<ObjectName>,
    pub sweep: SweepState,
}

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(format!("index head: {}", msg.into()))
}

impl IndexHead {
    /// The head `init` writes: generation 1, the empty state, no sweep.
    pub fn initial() -> Self {
        Self {
            layout: HEAD_LAYOUT_VERSION,
            generation: 1,
            features: Vec::new(),
            base: None,
            segments: Vec::new(),
            retired: Vec::new(),
            sweep: SweepState::default(),
        }
    }

    /// A copy with the next generation; the caller then edits the objects
    /// and the sweep.
    pub fn successor(&self) -> Self {
        Self {
            generation: self.generation + 1,
            ..self.clone()
        }
    }

    /// No base and no segments: the empty index.
    pub fn is_empty(&self) -> bool {
        self.base.is_none() && self.segments.is_empty()
    }

    /// Names of the objects this head references as live: base and segments.
    pub fn live_objects(&self) -> impl Iterator<Item = ObjectName> + '_ {
        self.base
            .iter()
            .map(|b| b.name)
            .chain(self.segments.iter().map(|s| s.name))
    }

    /// Every `index.d/` object this head keeps alive: the live objects,
    /// the retired ones and the sweep record.
    pub fn named_objects(&self) -> impl Iterator<Item = ObjectName> + '_ {
        self.live_objects()
            .chain(self.retired.iter().copied())
            .chain(self.sweep.record.iter().map(|r| r.name))
    }

    /// The features this head names that this binary does not know.
    pub fn unknown_features(&self) -> Vec<String> {
        self.features
            .iter()
            .filter(|f| !KNOWN_HEAD_FEATURES.contains(&f.as_str()))
            .cloned()
            .collect()
    }

    /// Check the head's internal consistency: known layout and features, a
    /// generation of at least 1 and not below the sweep's, entry counts that
    /// fit a u32, and no object named twice.
    pub fn validate(&self) -> Result<()> {
        if self.layout != HEAD_LAYOUT_VERSION {
            return Err(unsupported_layout(self.layout));
        }
        let unknown = self.unknown_features();
        if !unknown.is_empty() {
            return Err(unknown_features(&unknown));
        }
        if self.generation == 0 {
            return Err(invalid("generation 0"));
        }
        if self.sweep.generation > self.generation {
            return Err(invalid(format!(
                "sweep generation {} after the head's {}",
                self.sweep.generation, self.generation
            )));
        }
        if self
            .base
            .iter()
            .map(|b| b.entries)
            .chain(self.segments.iter().map(|s| s.entries))
            .any(|n| n > u64::from(u32::MAX))
        {
            return Err(invalid("entry count out of range"));
        }
        let mut seen = HashSet::new();
        for name in self.named_objects() {
            if !seen.insert(name) {
                return Err(invalid(format!("object {} named twice", name.to_hex())));
            }
        }
        Ok(())
    }

    /// Serialize as a msgpack map with the field names as keys.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(rmp_serde::to_vec_named(self)?)
    }

    /// Deserialize without validating.
    pub fn decode_unchecked(bytes: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(bytes)?)
    }

    /// Deserialize and [`validate`](Self::validate).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let head = Self::decode_unchecked(bytes)?;
        head.validate()?;
        Ok(head)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(b: u8) -> ObjectName {
        ObjectName([b; 32])
    }

    fn sample() -> IndexHead {
        IndexHead {
            layout: HEAD_LAYOUT_VERSION,
            generation: 7,
            features: Vec::new(),
            base: Some(BaseRef {
                name: name(1),
                size: 100,
                entries: 10,
            }),
            segments: vec![
                SegmentRef {
                    name: name(2),
                    size: 20,
                    kind: SegmentKind::Backup,
                    entries: 1,
                },
                SegmentRef {
                    name: name(3),
                    size: 30,
                    kind: SegmentKind::Maintenance,
                    entries: 2,
                },
            ],
            retired: vec![name(9)],
            sweep: SweepState {
                generation: 5,
                record: Some(ObjectRef {
                    name: name(8),
                    size: 40,
                }),
            },
        }
    }

    /// HEAD-1.
    #[test]
    fn round_trips() {
        let head = sample();
        assert_eq!(IndexHead::decode(&head.encode().unwrap()).unwrap(), head);
        let initial = IndexHead::initial();
        assert_eq!(
            IndexHead::decode(&initial.encode().unwrap()).unwrap(),
            initial
        );
        assert_eq!(initial.generation, 1);
        assert!(initial.is_empty());
        assert!(!head.is_empty());
        assert_eq!(initial.sweep, SweepState::default());
        assert_eq!(initial.successor().generation, 2);
    }

    /// HEAD-2: a map with named keys; names stay msgpack `bin`.
    #[test]
    fn encoding_is_a_map() {
        let bytes = IndexHead::initial().encode().unwrap();
        assert!(
            (0x80..=0x8f).contains(&bytes[0]) || bytes[0] == 0xde,
            "map header, got {:#x}",
            bytes[0]
        );
        let full = sample().encode().unwrap();
        for key in [
            &b"layout"[..],
            b"gen",
            b"sweep",
            b"features",
            b"kind",
            b"entries",
        ] {
            assert!(
                full.windows(key.len()).any(|w| w == key),
                "key {}",
                String::from_utf8_lossy(key)
            );
        }
        assert!(
            !full.windows(10).any(|w| w == b"state_hash"),
            "no state hash"
        );
        let bin32 = [0xc4, 32];
        assert!(full.windows(2).any(|w| w == bin32), "names as bin8");
        assert!(full.len() < 600, "{} bytes", full.len());
    }

    /// HEAD-3: an unknown key is ignored.
    #[test]
    fn unknown_keys_are_ignored() {
        #[derive(Serialize)]
        struct Newer<'a> {
            #[serde(flatten)]
            head: &'a IndexHead,
            later_field: u32,
        }
        let head = sample();
        let bytes = rmp_serde::to_vec_named(&Newer {
            head: &head,
            later_field: 42,
        })
        .unwrap();
        assert_eq!(IndexHead::decode(&bytes).unwrap(), head);
    }

    /// HEAD-4 (the classification half is in repo::index_store's tests).
    #[test]
    fn unknown_features_are_refused() {
        let mut h = sample();
        h.features = vec!["x".into()];
        let err = IndexHead::decode(&h.encode().unwrap()).unwrap_err();
        assert!(err.to_string().contains("newer vykar"), "{err}");
        assert_eq!(h.unknown_features(), vec!["x".to_string()]);
    }

    /// HEAD-5: peek_layout on map and positional heads.
    #[test]
    fn peek_layout_reads_map_and_positional_heads() {
        assert_eq!(peek_layout(&sample().encode().unwrap()), Some(3));
        let mut h = sample();
        h.layout = 4;
        assert_eq!(peek_layout(&h.encode().unwrap()), Some(4));
        assert!(IndexHead::decode(&h.encode().unwrap()).is_err());

        // A positional layout-1 head, as v4/v5 wrote it.
        let positional =
            rmp_serde::to_vec(&(1u32, serde_bytes::Bytes::new(&[0; 32]), 0u64)).unwrap();
        assert_eq!(peek_layout(&positional), Some(1));
        assert!(IndexHead::decode(&positional).is_err());

        assert_eq!(peek_layout(&[0xc0]), None);
        assert_eq!(peek_layout(&[]), None);
        assert_eq!(peek_layout(&[0x90]), None);
        assert_eq!(peek_layout(&[0x80]), None);
    }

    /// HEAD-6.
    #[test]
    fn rejects_inconsistent_heads() {
        let bad = |h: IndexHead| IndexHead::decode(&h.encode().unwrap()).is_err();

        let mut h = sample();
        h.generation = 0;
        h.sweep.generation = 0;
        assert!(bad(h));

        let mut h = sample();
        h.sweep.generation = h.generation + 1;
        assert!(bad(h));

        let mut h = sample();
        h.retired.push(name(8));
        assert!(bad(h), "the record named again as retired");

        let mut h = sample();
        h.sweep.record = Some(ObjectRef {
            name: name(2),
            size: 1,
        });
        assert!(bad(h), "the record named again as a segment");

        let mut h = sample();
        h.retired.push(name(2));
        assert!(bad(h));

        let mut h = sample();
        h.segments[1].entries = u64::from(u32::MAX) + 1;
        assert!(bad(h));

        let mut h = sample();
        if let Some(b) = h.base.as_mut() {
            b.entries = u64::from(u32::MAX) + 1;
        }
        assert!(bad(h));

        // Every field is required.
        #[derive(Serialize)]
        struct NoSweep {
            layout: u32,
            #[serde(rename = "gen")]
            generation: u64,
        }
        let bytes = rmp_serde::to_vec_named(&NoSweep {
            layout: HEAD_LAYOUT_VERSION,
            generation: 1,
        })
        .unwrap();
        assert!(IndexHead::decode(&bytes).is_err());
    }

    /// HEAD-7.
    #[test]
    fn segment_kind_wire_values() {
        for (kind, v) in [(SegmentKind::Backup, 0u8), (SegmentKind::Maintenance, 1)] {
            let bytes = rmp_serde::to_vec(&kind).unwrap();
            assert_eq!(bytes, rmp_serde::to_vec(&v).unwrap());
            assert_eq!(rmp_serde::from_slice::<SegmentKind>(&bytes).unwrap(), kind);
        }
        let two = rmp_serde::to_vec(&2u8).unwrap();
        assert!(rmp_serde::from_slice::<SegmentKind>(&two).is_err());
    }

    #[test]
    fn named_objects() {
        let head = sample();
        assert_eq!(
            head.live_objects().collect::<Vec<_>>(),
            vec![name(1), name(2), name(3)]
        );
        assert_eq!(
            head.named_objects().collect::<Vec<_>>(),
            vec![name(1), name(2), name(3), name(9), name(8)]
        );
    }

    /// A layout-2 head as 05 step 8 wrote it (map, v1 chain fields).
    #[derive(Serialize)]
    pub(crate) struct Layout2Head {
        pub layout: u32,
        #[serde(rename = "gen")]
        pub generation: u64,
        pub features: Vec<String>,
        #[serde(with = "serde_bytes")]
        pub state_hash: [u8; 32],
        pub entries: u64,
        pub base: Option<()>,
        pub segments: Vec<()>,
        pub retired: Vec<ObjectName>,
        pub sweep: SweepState,
    }

    /// HEAD-8 (decode half; classify_head's is in repo::index_store's tests):
    /// a layout-2 head is refused by its layout.
    #[test]
    fn a_layout_2_head_is_refused_by_layout() {
        let bytes = rmp_serde::to_vec_named(&Layout2Head {
            layout: 2,
            generation: 1,
            features: Vec::new(),
            state_hash: *blake3::hash(b"").as_bytes(),
            entries: 0,
            base: None,
            segments: Vec::new(),
            retired: Vec::new(),
            sweep: SweepState::default(),
        })
        .unwrap();
        assert_eq!(peek_layout(&bytes), Some(2));
        let err = IndexHead::decode(&bytes).unwrap_err().to_string();
        assert!(err.contains("layout 2"), "{err}");
    }
}
