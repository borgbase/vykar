//! The index head: the small mutable object at `index` that names the live
//! base and segments of a segmented index (formats v4/v5).
//!
//! Serialized as a positional msgpack array. The layout is frozen: rmp-serde
//! rejects an array longer than the struct, so a field can never be appended
//! without a new `layout` value that old binaries refuse.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use vykar_types::error::{Result, VykarError};

use super::dedup_cache::StateHash;

/// The only head layout this binary reads and writes.
pub const HEAD_LAYOUT_VERSION: u32 = 1;

/// The state hash of the empty index: BLAKE3 over no records.
pub fn empty_state_hash() -> StateHash {
    blake3::hash(b"").into()
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

/// A base object: the full sorted records of one index state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseRef {
    pub name: ObjectName,
    pub size: u64,
    #[serde(with = "serde_bytes")]
    pub state_hash: StateHash,
    pub entries: u64,
}

/// A segment object and the state it produces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRef {
    pub name: ObjectName,
    pub size: u64,
    #[serde(with = "serde_bytes")]
    pub state_hash_after: StateHash,
    pub entries_after: u64,
}

/// The head. `base: None` means the base state is the empty index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexHead {
    pub layout: u32,
    #[serde(with = "serde_bytes")]
    pub state_hash: StateHash,
    pub entries: u64,
    pub base: Option<BaseRef>,
    pub segments: Vec<SegmentRef>,
    /// Objects of the last head before the most recent fold, kept so a
    /// reader that loaded that head can finish.
    pub retired: Vec<ObjectName>,
}

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(format!("index head: {}", msg.into()))
}

impl IndexHead {
    /// The head of an empty index, as `init` writes it.
    pub fn empty() -> Self {
        Self {
            layout: HEAD_LAYOUT_VERSION,
            state_hash: empty_state_hash(),
            entries: 0,
            base: None,
            segments: Vec::new(),
            retired: Vec::new(),
        }
    }

    /// State hash and entry count of the base state.
    pub fn base_state(&self) -> (StateHash, u64) {
        match &self.base {
            Some(b) => (b.state_hash, b.entries),
            None => (empty_state_hash(), 0),
        }
    }

    /// Every state in the chain, oldest first: the base state, then the
    /// state after each segment. `chain_states()[k]` is the state from which
    /// `segments[k..]` remain to be applied.
    pub fn chain_states(&self) -> Vec<(StateHash, u64)> {
        let mut states = Vec::with_capacity(self.segments.len() + 1);
        states.push(self.base_state());
        states.extend(
            self.segments
                .iter()
                .map(|s| (s.state_hash_after, s.entries_after)),
        );
        states
    }

    /// Names of the objects this head references as live.
    pub fn live_objects(&self) -> impl Iterator<Item = ObjectName> + '_ {
        self.base
            .iter()
            .map(|b| b.name)
            .chain(self.segments.iter().map(|s| s.name))
    }

    /// Check the head's internal consistency: known layout, the chain ends
    /// at the head's state, entry counts fit a full index cache, and no
    /// object is named twice.
    pub fn validate(&self) -> Result<()> {
        if self.layout != HEAD_LAYOUT_VERSION {
            return Err(VykarError::InvalidFormat(format!(
                "unsupported index head layout {} (this version reads {HEAD_LAYOUT_VERSION})",
                self.layout
            )));
        }
        let last = *self
            .chain_states()
            .last()
            .expect("chain_states holds the base state");
        if last != (self.state_hash, self.entries) {
            return Err(invalid("chain does not end at the head's state"));
        }
        if self
            .chain_states()
            .iter()
            .any(|&(_, n)| n > u64::from(u32::MAX))
        {
            return Err(invalid("entry count out of range"));
        }
        let mut seen = HashSet::new();
        for name in self.live_objects().chain(self.retired.iter().copied()) {
            if !seen.insert(name) {
                return Err(invalid(format!("object {} named twice", name.to_hex())));
            }
        }
        Ok(())
    }

    /// Serialize as a positional msgpack array.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(rmp_serde::to_vec(self)?)
    }

    /// Deserialize and [`validate`](Self::validate).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let head: Self = rmp_serde::from_slice(bytes)?;
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
            state_hash: [3; 32],
            entries: 12,
            base: Some(BaseRef {
                name: name(1),
                size: 100,
                state_hash: [1; 32],
                entries: 10,
            }),
            segments: vec![
                SegmentRef {
                    name: name(2),
                    size: 20,
                    state_hash_after: [2; 32],
                    entries_after: 11,
                },
                SegmentRef {
                    name: name(3),
                    size: 30,
                    state_hash_after: [3; 32],
                    entries_after: 12,
                },
            ],
            retired: vec![name(9)],
        }
    }

    #[test]
    fn round_trips() {
        let head = sample();
        assert_eq!(IndexHead::decode(&head.encode().unwrap()).unwrap(), head);
        let empty = IndexHead::empty();
        assert_eq!(IndexHead::decode(&empty.encode().unwrap()).unwrap(), empty);
    }

    /// Names and hashes are msgpack `bin`, not arrays of integers, and the
    /// head is a 6-element positional array.
    #[test]
    fn encoding_is_compact_and_positional() {
        let bytes = IndexHead::empty().encode().unwrap();
        assert_eq!(bytes[0], 0x96);
        assert_eq!(&bytes[2..4], &[0xc4, 32], "state_hash as bin8 of 32 bytes");
        let full = sample().encode().unwrap();
        assert!(full.len() < 300, "{} bytes", full.len());
    }

    #[test]
    fn chain_states_and_live_objects() {
        let head = sample();
        assert_eq!(
            head.chain_states(),
            vec![([1; 32], 10), ([2; 32], 11), ([3; 32], 12)]
        );
        assert_eq!(
            head.live_objects().collect::<Vec<_>>(),
            vec![name(1), name(2), name(3)]
        );
        assert_eq!(
            IndexHead::empty().chain_states(),
            vec![(empty_state_hash(), 0)]
        );
    }

    #[test]
    fn rejects_inconsistent_heads() {
        let mut h = sample();
        h.layout = 2;
        assert!(IndexHead::decode(&h.encode().unwrap()).is_err());

        let mut h = sample();
        h.state_hash = [7; 32];
        assert!(IndexHead::decode(&h.encode().unwrap()).is_err());

        let mut h = sample();
        h.entries = 13;
        assert!(IndexHead::decode(&h.encode().unwrap()).is_err());

        let mut h = sample();
        h.retired.push(name(2));
        assert!(IndexHead::decode(&h.encode().unwrap()).is_err());

        let mut h = IndexHead::empty();
        h.state_hash = [0; 32];
        assert!(IndexHead::decode(&h.encode().unwrap()).is_err());

        let mut h = sample();
        h.segments[1].entries_after = u64::from(u32::MAX) + 1;
        h.entries = h.segments[1].entries_after;
        assert!(IndexHead::decode(&h.encode().unwrap()).is_err());
    }

    /// An appended field must not decode: the layout is frozen.
    #[test]
    fn longer_array_is_rejected() {
        let mut bytes = IndexHead::empty().encode().unwrap();
        bytes[0] = 0x97;
        bytes.push(0x00);
        assert!(IndexHead::decode(&bytes).is_err());
    }
}
