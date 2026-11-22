#![no_main]
//! Input: a keyed index segment body (version 2). A body that decodes is
//! canonical: it re-encodes to the same bytes.
use libfuzzer_sys::fuzz_target;
use vykar_core::index::segment_v2::SegmentBody;

fuzz_target!(|data: &[u8]| {
    let Ok(seg) = SegmentBody::decode(data, u64::MAX) else {
        return;
    };
    assert_eq!(seg.encode().expect("a decoded segment encodes"), data);
});
