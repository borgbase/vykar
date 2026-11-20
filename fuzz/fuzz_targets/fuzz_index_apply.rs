#![no_main]
//! A decoded base and 1 to 3 decoded segments, chained to it and to each
//! other, applied with `apply_to_temp` and checked against the reference
//! model (`index::segment::model`).
//!
//! Input:
//!
//! ```text
//! byte 0 control: bits 0-1: n = (c & 0b11) % 3 + 1 segments
//!                 bits 2-4: layer k declares a wrong result if bit (2 + k) is set
//!                 bits 5-6: which wrong result, ((c >> 5) & 0b11) % 3:
//!                           0 state_hash_after[0] ^= 1, 1 entries_after + 1,
//!                           2 entries_after - 1 (the hash flip when it is 0)
//! u16 LE len, base body (len bytes)
//! for each of the first n - 1 segments: u16 LE len, segment body
//! last segment body: the rest of the input
//! ```
//!
//! Each segment is patched to follow the state the layer before it declares
//! (the base's real state for the first), and its ops at positions past
//! that state or the model's real input are dropped, so that the chain
//! links and a wrong result is caught by the layer, not the link check.
use libfuzzer_sys::fuzz_target;
use vykar_core::index::base::decode_unnamed_base_to_temp;
use vykar_core::index::dedup_cache::{FullCacheEntry, MmapFullIndexCache};
use vykar_core::index::segment::{apply_to_temp, model, ChainStep, Segment};
use vykar_types::error::VykarError;

fn take_framed<'a>(data: &mut &'a [u8]) -> Option<&'a [u8]> {
    let (len, rest) = data.split_first_chunk::<2>()?;
    let len = usize::from(u16::from_le_bytes(*len));
    let body = rest.get(..len)?;
    *data = &rest[len..];
    Some(body)
}

fn lie(declared: &mut ([u8; 32], u64), kind: u8) {
    match kind {
        1 => declared.1 += 1,
        2 if declared.1 > 0 => declared.1 -= 1,
        _ => declared.0[0] ^= 1,
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&control, mut rest)) = data.split_first() else {
        return;
    };
    let n = usize::from(control & 0b11) % 3 + 1;
    let kind = ((control >> 5) & 0b11) % 3;

    let Some(base_body) = take_framed(&mut rest) else {
        return;
    };
    let mut bodies = Vec::with_capacity(n);
    for _ in 1..n {
        let Some(body) = take_framed(&mut rest) else {
            return;
        };
        bodies.push(body);
    }
    bodies.push(rest);

    let anchor = std::env::temp_dir().join("vykar-fuzz-index-apply");
    let Ok((base_tmp, _, _)) = decode_unnamed_base_to_temp(base_body, &anchor) else {
        return;
    };
    let cache = MmapFullIndexCache::open_verified(base_tmp.path()).expect("decoded base verifies");
    let prev: Vec<FullCacheEntry> = cache.iter().collect();

    let mut segments = Vec::with_capacity(n);
    for body in bodies {
        let Ok(seg) = Segment::decode(body) else {
            return;
        };
        segments.push(seg);
    }

    let mut cur = prev.clone();
    let mut declared_prev = (cache.state_hash(), prev.len() as u64);
    let mut kept: Vec<(Segment, ([u8; 32], u64))> = Vec::with_capacity(n);
    for (k, mut seg) in segments.into_iter().enumerate() {
        seg.prev_state_hash = declared_prev.0;
        seg.prev_entries = declared_prev.1;
        let limit = seg.prev_entries.min(cur.len() as u64);
        seg.deltas.retain(|d| u64::from(d.position) < limit);
        seg.relocations.retain(|r| u64::from(r.position) < limit);
        assert!(seg.validate().is_ok(), "patched segment is valid");

        let result = model::apply_layer(&cur, &seg, k);
        // A rejected layer declares its input state.
        let produced = result.as_deref().unwrap_or(&cur);
        let mut declared = (model::state_hash(produced), produced.len() as u64);
        if control & (1 << (2 + k)) != 0 {
            lie(&mut declared, kind);
        }
        kept.push((seg, declared));
        declared_prev = declared;
        match result {
            Ok(out) => cur = out,
            Err(_) => break,
        }
    }

    let chain: Vec<ChainStep<'_>> = kept
        .iter()
        .map(|(segment, (state_hash_after, entries_after))| ChainStep {
            segment,
            state_hash_after: *state_hash_after,
            entries_after: *entries_after,
        })
        .collect();
    let model = model::apply_chain(&prev, &chain);
    let real = apply_to_temp(&cache, &chain, &anchor);
    match (model, real) {
        (Ok(m), Ok((tmp, hash, count))) => {
            let out = MmapFullIndexCache::open_verified(&tmp).expect("output cache verifies");
            assert_eq!(out.iter().collect::<Vec<_>>(), m);
            assert_eq!(hash, model::state_hash(&m));
            assert_eq!(count, m.len() as u64);
        }
        (Err(_), Err(e)) => assert!(matches!(e, VykarError::InvalidFormat(_)), "{e:?}"),
        (Ok(m), Err(e)) => panic!("model accepted ({} entries), apply failed: {e}", m.len()),
        (Err(r), Ok(_)) => panic!("model rejected ({r:?}), apply succeeded"),
    }
});
