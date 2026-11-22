//! Chunk IDs as msgpack bin (snapshot format 2) in `Item.chunks` and
//! `SnapshotMeta.item_ptrs`. Decoding accepts the format-1 encoding too
//! (`ChunkId`'s derived serde: an array of 32 integers), so one decoder reads
//! every snapshot. `ChunkId`'s own derived encoding is unchanged everywhere
//! else.
//!
//! Exactly 32 bytes or 32 elements are accepted. rmp-serde hands a msgpack
//! `str` that is not valid UTF-8 to `visit_bytes`, so a 32-byte `str` of
//! that kind decodes as well; a valid UTF-8 `str` is refused.

use serde::de::{self, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vykar_types::chunk_id::ChunkId;

pub(crate) fn serialize<S: Serializer>(id: &ChunkId, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_bytes(id.as_bytes())
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ChunkId, D::Error> {
    d.deserialize_bytes(IdVisitor)
}

struct IdVisitor;

impl<'de> Visitor<'de> for IdVisitor {
    type Value = ChunkId;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a chunk ID (32-byte bin or an array of 32 integers)")
    }

    fn visit_bytes<E: de::Error>(self, b: &[u8]) -> Result<ChunkId, E> {
        <[u8; 32]>::try_from(b)
            .map(ChunkId::from_bytes)
            .map_err(|_| E::invalid_length(b.len(), &self))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ChunkId, A::Error> {
        let mut out = [0u8; 32];
        for (i, slot) in out.iter_mut().enumerate() {
            *slot = seq
                .next_element::<u8>()?
                .ok_or_else(|| de::Error::invalid_length(i, &self))?;
        }
        // rmp-serde rejects leftovers itself (LengthMismatch); this keeps
        // the decoder strict on any deserializer.
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(de::Error::invalid_length(33, &self));
        }
        Ok(ChunkId::from_bytes(out))
    }
}

/// `Vec<ChunkId>` with each element as above (`SnapshotMeta.item_ptrs`).
pub(crate) mod seq {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(ids: &[ChunkId], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(ids.iter().map(BinRef))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<ChunkId>, D::Error> {
        d.deserialize_seq(SeqVisitor)
    }

    struct BinRef<'a>(&'a ChunkId);

    impl Serialize for BinRef<'_> {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            super::serialize(self.0, s)
        }
    }

    struct BinId(ChunkId);

    impl<'de> Deserialize<'de> for BinId {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            super::deserialize(d).map(BinId)
        }
    }

    struct SeqVisitor;

    impl<'de> Visitor<'de> for SeqVisitor {
        type Value = Vec<ChunkId>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a sequence of chunk IDs")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<ChunkId>, A::Error> {
            // A hostile length header must not size the allocation.
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
            while let Some(BinId(id)) = seq.next_element()? {
                out.push(id);
            }
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::item::ChunkRef;

    /// The golden ID of `vykar-types/tests/wire_format.rs`.
    fn golden() -> [u8; 32] {
        std::array::from_fn(|i| u8::try_from(i).unwrap().wrapping_mul(9))
    }

    /// Its derived (format-1) encoding, `GOLDEN_HEX` there.
    const GOLDEN_HEX: &str = concat!(
        "dc0020",
        "0009121b242d363f48515a636c757e",
        "cc87cc90cc99cca2ccabccb4ccbdccc6cccfccd8cce1cceaccf3ccfc",
        "050e17",
    );

    #[derive(Debug, PartialEq, Deserialize)]
    struct Wrapped(#[serde(with = "super")] ChunkId);

    /// Decode `bytes` through both the borrowed and the copied reader;
    /// both must agree.
    fn decode(bytes: &[u8]) -> Option<ChunkId> {
        let borrowed = rmp_serde::from_slice::<Wrapped>(bytes).ok();
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(bytes));
        let copied = Wrapped::deserialize(&mut de).ok();
        assert_eq!(borrowed, copied, "readers disagree on {bytes:02x?}");
        borrowed.map(|w| w.0)
    }

    fn bin(n: usize) -> Vec<u8> {
        let mut out = vec![0xc4, u8::try_from(n).unwrap()];
        out.extend((0..n).map(|i| u8::try_from(i % 256).unwrap()));
        out
    }

    fn array(elems: &[u16]) -> Vec<u8> {
        let mut out = Vec::new();
        rmp::encode::write_array_len(&mut out, u32::try_from(elems.len()).unwrap()).unwrap();
        for &e in elems {
            rmp::encode::write_uint(&mut out, u64::from(e)).unwrap();
        }
        out
    }

    #[test]
    fn chunk_ref_encodes_id_as_bin() {
        let r = ChunkRef {
            id: ChunkId::from_bytes(golden()),
            size: 1,
            csize: 2,
        };
        let mut expected = vec![0x93, 0xc4, 0x20];
        expected.extend_from_slice(&golden());
        expected.extend_from_slice(&[0x01, 0x02]);
        assert_eq!(rmp_serde::to_vec(&r).unwrap(), expected);
    }

    #[test]
    fn decodes_both_encodings() {
        let id = ChunkId::from_bytes(golden());
        // Format 1: ChunkId's derived encoding.
        let derived = hex::decode(GOLDEN_HEX).unwrap();
        assert_eq!(rmp_serde::to_vec(&id).unwrap(), derived);
        assert_eq!(decode(&derived), Some(id));
        // Format 2.
        let mut b = vec![0xc4, 0x20];
        b.extend_from_slice(&golden());
        assert_eq!(decode(&b), Some(id));
    }

    #[test]
    fn rejects_wrong_lengths_and_types() {
        let ok32: Vec<u16> = (0..32).collect();
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("bin 31", bin(31)),
            ("bin 33", bin(33)),
            ("array 31", array(&ok32[..31])),
            ("array 33", array(&(0..33).collect::<Vec<_>>())),
            ("element 256", {
                let mut e = ok32.clone();
                e[5] = 256;
                array(&e)
            }),
            ("str", {
                let mut out = Vec::new();
                rmp::encode::write_str(&mut out, &"a".repeat(32)).unwrap();
                out
            }),
        ];
        assert_eq!(decode(&bin(32)).map(|id| id.as_bytes()[31]), Some(31));
        assert!(decode(&array(&ok32)).is_some());
        for (name, bytes) in cases {
            assert_eq!(decode(&bytes), None, "{name} must be refused");
        }
    }

    #[test]
    fn seq_round_trips_and_reads_format_1() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Ptrs(#[serde(with = "super::seq")] Vec<ChunkId>);

        let ids = vec![ChunkId::from_bytes(golden()), ChunkId::from_bytes([9; 32])];
        let bytes = rmp_serde::to_vec(&Ptrs(ids.clone())).unwrap();
        assert_eq!(&bytes[..3], &[0x92, 0xc4, 0x20]);
        assert_eq!(&bytes[35..37], &[0xc4, 0x20]);
        assert_eq!(bytes.len(), 1 + 2 * 34);
        assert_eq!(rmp_serde::from_slice::<Ptrs>(&bytes).unwrap().0, ids);

        let v1 = rmp_serde::to_vec(&ids).unwrap();
        assert_eq!(rmp_serde::from_slice::<Ptrs>(&v1).unwrap().0, ids);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(&v1));
        assert_eq!(Ptrs::deserialize(&mut de).unwrap().0, ids);
    }
}
