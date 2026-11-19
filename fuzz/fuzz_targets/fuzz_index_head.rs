#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::index::head::IndexHead;

fuzz_target!(|data: &[u8]| {
    if let Ok(head) = IndexHead::decode(data) {
        let encoded = head.encode().expect("a decoded head encodes");
        assert_eq!(
            IndexHead::decode(&encoded).expect("a re-encoded head decodes"),
            head
        );
    }
});
