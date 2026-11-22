#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::index::sweep_record::SweepRecord;

fuzz_target!(|data: &[u8]| {
    if let Ok(record) = SweepRecord::decode(data) {
        let encoded = record.encode().expect("a decoded record encodes");
        assert_eq!(
            SweepRecord::decode(&encoded).expect("a re-encoded record decodes"),
            record
        );
    }
});
