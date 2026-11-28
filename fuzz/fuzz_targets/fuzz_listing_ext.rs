#![no_main]
//! The snapshot key index (`SnapshotMeta.ext`): `decode_ext`, and
//! `check_ext` against a listing that the writer's index builder derives
//! from the same input.
//!
//! Input layout:
//!
//! ```text
//! byte 0       flags: bit 0 set = ext absent (None); other bits ignored
//! byte 1       s: the writer model's stride is 1 + s % 32 (1..=32; 32 = STRIDE)
//! bytes 2..4   L: u16 little-endian, the ext length
//! next L bytes ext (clamped to what is left of the input; consumed even when absent)
//! rest         the listing: ops separated by b'\n'
//!                empty line        -> ListingOp::Cut
//!                0x01 ++ root      -> ListingOp::BeginSegment(root)
//!                0x02 ++ path      -> ListingOp::Push(path)   (any path)
//!                any other line    -> ListingOp::Push(line)
//! ```
//!
//! Properties:
//!
//! - P1: `decode_ext` is total, also at 0 and `usize::MAX` tree chunks.
//! - P2: an accepted index re-encodes to one that decodes to the same value
//!   (value, not byte, identity: the decoder accepts encodings the writer
//!   never emits).
//! - P3: `check_ext` is `decode_ext` followed by the per-chunk check.
//! - P4: the index the writer derives decodes and passes `check_ext`, or is
//!   order 0 and reported as unordered.
use libfuzzer_sys::fuzz_target;
use vykar_core::fuzzing::{self, ExtVerdict, ListingOp};
use vykar_core::snapshot::listing::{NoIndex, ORDER_DFS_NAME, decode_ext, encode_ext};

struct Input<'a> {
    ext: Option<&'a [u8]>,
    stride: u32,
    ops: Vec<ListingOp<'a>>,
}

fn parse(data: &[u8]) -> Option<Input<'_>> {
    let (&flags, rest) = data.split_first()?;
    let (&s, rest) = rest.split_first()?;
    let (len, rest) = rest.split_at_checked(2)?;
    let len = usize::from(u16::from_le_bytes(len.try_into().ok()?));
    let (ext, rest) = rest.split_at(len.min(rest.len()));
    let ops = rest
        .split(|b| *b == b'\n')
        .map(|line| match line.split_first() {
            None => ListingOp::Cut,
            Some((0x01, root)) => ListingOp::BeginSegment(root),
            Some((0x02, path)) => ListingOp::Push(path),
            Some(_) => ListingOp::Push(line),
        })
        .collect();
    Some(Input {
        ext: (flags & 1 == 0).then_some(ext),
        stride: 1 + u32::from(s % 32),
        ops,
    })
}

fuzz_target!(|data: &[u8]| {
    let Some(input) = parse(data) else { return };
    let listing = fuzzing::write_listing(&input.ops, input.stride);
    let n = listing.chunks.len();

    // P1: total on hostile bytes, at the listing's size and at the extremes.
    let decoded = decode_ext(input.ext, n);
    let _ = decode_ext(input.ext, 0);
    let _ = decode_ext(input.ext, usize::MAX);

    // P2: an accepted index re-encodes to one that decodes to the same value.
    if let Ok(index) = &decoded {
        let again = encode_ext(index).expect("a decoded index encodes");
        assert_eq!(decode_ext(Some(&again), n).as_ref(), Ok(index));
    }

    // P3: check_ext is decode_ext plus the per-chunk check.
    let verdict = fuzzing::check_ext(input.ext, &listing.chunks);
    match &decoded {
        Err(no) if no.warns() => assert_eq!(verdict, ExtVerdict::Mismatch(no.to_string())),
        Err(no) => assert_eq!(verdict, ExtVerdict::NoIndex(no.clone())),
        Ok(_) => assert!(
            matches!(verdict, ExtVerdict::Ok | ExtVerdict::Mismatch(_)),
            "{verdict:?}"
        ),
    }

    // P4: what the writer derives, the reader accepts and check confirms.
    let own = encode_ext(&listing.index).expect("the writer's index encodes");
    let own_decoded = decode_ext(Some(&own), n);
    let own_verdict = fuzzing::check_ext(Some(&own), &listing.chunks);
    if listing.index.order == ORDER_DFS_NAME {
        assert_eq!(own_decoded.as_ref(), Ok(&listing.index));
        assert_eq!(own_verdict, ExtVerdict::Ok);
    } else {
        assert_eq!(own_decoded, Err(NoIndex::Unordered));
        assert_eq!(own_verdict, ExtVerdict::NoIndex(NoIndex::Unordered));
    }
});
