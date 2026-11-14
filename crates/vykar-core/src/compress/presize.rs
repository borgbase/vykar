//! Bounded pre-sizing of large maps while decoding metadata.
//!
//! serde's stock `HashMap` impl clamps the msgpack length prefix to ~1 MiB of
//! elements ("cautious" size hint) so that a corrupt or hostile prefix cannot
//! trigger a giant allocation before a single entry has been read. For a
//! multi-hundred-thousand-entry chunk index or file cache that means the table
//! is grown by doubling, and at every growth step the old and the new table are
//! live together (1.5x the final table at the last rehash).
//!
//! This module replaces the fixed clamp with one tied to the size of the
//! *encoded input*, which the caller already holds in memory. The decode entry
//! point ([`super::decode_metadata_msgpack`]) installs a budget of `input_len`
//! bytes for the duration of the decode; each pre-sized map then claims at most
//! `budget / min_input_bytes_per_entry` entries and deducts what it claimed, so
//! the sum of all reservations is bounded by the input size even across many
//! maps (e.g. one file-cache section per source root). `min_input_bytes_per_entry`
//! is a lower bound on what one *legitimate* entry costs in that input — the
//! incompressible random key — so honest data is always pre-sized exactly,
//! while a lying length prefix can reserve at most a small constant multiple
//! of the input. Under-estimating is harmless: the map simply grows as before.
//!
//! The budget is thread-local because rmp-serde decodes synchronously on the
//! calling thread and the serde `deserialize_with` hook has no other channel
//! to the caller. Outside an installed budget the serde-equivalent cautious
//! clamp applies, so other decode sites behave exactly as before.

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;

use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};

thread_local! {
    /// Remaining input bytes that pre-sized maps may still claim, or `None`
    /// when no metadata decode is in progress on this thread.
    static INPUT_BUDGET: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Same element budget serde uses for its cautious size hint.
const CAUTIOUS_MAX_BYTES: usize = 1024 * 1024;

/// Run `f` with a pre-sizing budget of `input_len` bytes. Restores the
/// previous budget afterwards, also on unwind, so nested decodes are safe.
pub(crate) fn with_input_budget<R>(input_len: usize, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            INPUT_BUDGET.with(|b| b.set(self.0));
        }
    }
    let _restore = Restore(INPUT_BUDGET.with(|b| b.replace(Some(input_len))));
    f()
}

/// Capacity to reserve for a map whose length prefix claims `hint` entries,
/// each of which costs at least `min_input_bytes_per_entry` bytes of input.
/// Deducts the claim from the active budget.
fn claim_capacity<T>(hint: Option<usize>, min_input_bytes_per_entry: usize) -> usize {
    let hint = hint.unwrap_or(0);
    INPUT_BUDGET.with(|b| match b.get() {
        Some(budget) => {
            let cap = hint.min(budget / min_input_bytes_per_entry.max(1));
            b.set(Some(budget - cap * min_input_bytes_per_entry.max(1)));
            cap
        }
        None => hint.min(CAUTIOUS_MAX_BYTES / std::mem::size_of::<T>().max(1)),
    })
}

/// Deserialize a `HashMap` pre-sized from its length prefix, within the
/// active input budget. Wire-compatible with the stock `HashMap` impl (and
/// like it, a duplicate key keeps the last value).
pub(crate) fn deserialize_hash_map<'de, D, K, V, S>(
    deserializer: D,
    min_input_bytes_per_entry: usize,
) -> Result<HashMap<K, V, S>, D::Error>
where
    D: Deserializer<'de>,
    K: Deserialize<'de> + Eq + Hash,
    V: Deserialize<'de>,
    S: BuildHasher + Default,
{
    struct MapVisitor<K, V, S> {
        min_input_bytes_per_entry: usize,
        marker: PhantomData<HashMap<K, V, S>>,
    }

    impl<'de, K, V, S> Visitor<'de> for MapVisitor<K, V, S>
    where
        K: Deserialize<'de> + Eq + Hash,
        V: Deserialize<'de>,
        S: BuildHasher + Default,
    {
        type Value = HashMap<K, V, S>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a map")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
            let cap = claim_capacity::<(K, V)>(access.size_hint(), self.min_input_bytes_per_entry);
            let mut map = HashMap::with_capacity_and_hasher(cap, S::default());
            while let Some((k, v)) = access.next_entry()? {
                map.insert(k, v);
            }
            Ok(map)
        }
    }

    deserializer.deserialize_map(MapVisitor {
        min_input_bytes_per_entry,
        marker: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct Wrap {
        #[serde(deserialize_with = "de_u64_map")]
        m: HashMap<u64, u64>,
    }

    fn de_u64_map<'de, D: Deserializer<'de>>(d: D) -> Result<HashMap<u64, u64>, D::Error> {
        deserialize_hash_map(d, 8)
    }

    /// msgpack `[ {map32 header claiming `claimed` entries} k v ... ]` with
    /// `actual` positive-fixint key/value pairs.
    fn forged(claimed: u32, actual: u8) -> Vec<u8> {
        let mut buf = vec![0x91, 0xdf];
        buf.extend_from_slice(&claimed.to_be_bytes());
        for i in 0..actual.min(0x7f) {
            buf.extend_from_slice(&[i, i]);
        }
        buf
    }

    #[test]
    fn honest_map_is_presized_exactly() {
        let m: HashMap<u64, u64> = (0..50_000u64).map(|i| (i << 20, i)).collect();
        let bytes = rmp_serde::to_vec(&(&m,)).unwrap();
        let w: Wrap = with_input_budget(bytes.len(), || rmp_serde::from_slice(&bytes)).unwrap();
        assert_eq!(w.m, m);
        // Exact pre-size: no growth, so capacity is what with_capacity(n) gives.
        assert_eq!(
            w.m.capacity(),
            HashMap::<u64, u64>::with_capacity(m.len()).capacity()
        );
    }

    #[test]
    fn lying_length_prefix_is_bounded_by_input() {
        // Claims 4 billion entries but carries three: a clean decode error,
        // not an allocation failure.
        let bytes = forged(u32::MAX, 3);
        let res: Result<Wrap, _> = with_input_budget(bytes.len(), || rmp_serde::from_slice(&bytes));
        assert!(res.is_err());
        // The reservation is capped by the budget ...
        let cap = with_input_budget(1000, || claim_capacity::<(u64, u64)>(Some(usize::MAX), 8));
        assert_eq!(cap, 125);
        // ... and, with no budget installed, by serde's cautious clamp.
        assert_eq!(
            claim_capacity::<(u64, u64)>(Some(usize::MAX), 8),
            CAUTIOUS_MAX_BYTES / 16
        );
    }

    #[test]
    fn short_map_decodes_without_budget() {
        let bytes = forged(3, 3);
        let w: Wrap = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(w.m.len(), 3);
    }

    #[test]
    fn budget_is_shared_across_maps_and_restored() {
        with_input_budget(800, || {
            assert_eq!(claim_capacity::<u8>(Some(60), 8), 60);
            // 800 - 480 = 320 bytes left.
            assert_eq!(claim_capacity::<u8>(Some(60), 8), 40);
            assert_eq!(claim_capacity::<u8>(Some(60), 8), 0);
            with_input_budget(16, || assert_eq!(claim_capacity::<u8>(Some(60), 8), 2));
            assert_eq!(INPUT_BUDGET.with(Cell::get), Some(0));
        });
        assert_eq!(INPUT_BUDGET.with(Cell::get), None);
    }
}
