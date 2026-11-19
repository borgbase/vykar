#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::index::segment::Segment;

fuzz_target!(|data: &[u8]| {
    if let Ok(segment) = Segment::decode(data) {
        assert_eq!(
            segment.encode(),
            data,
            "decoded segment must re-encode identically"
        );
    }
});
