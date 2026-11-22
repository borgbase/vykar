#![no_main]
//! Input: an index base body (version 2). A body that validates is
//! canonical: re-encoded through `BaseWriter` from its pack table and
//! records, it gives the same bytes.
use libfuzzer_sys::fuzz_target;
use vykar_core::fuzzing;
use vykar_core::index::base_v2::{BaseWriter, validate_base_body};

fuzz_target!(|data: &[u8]| {
    let Ok((packs, entries)) = validate_base_body(data) else {
        return;
    };
    let (pack_table, records) = fuzzing::base_v2_contents(data).expect("a valid body reads");
    assert_eq!(pack_table.len() as u32, packs);
    assert_eq!(records.len() as u32, entries);
    let mut w = BaseWriter::new(Vec::new(), &pack_table, entries).expect("a valid pack table");
    for (id, slot, offset, size) in &records {
        w.push(id, *slot, u64::from(*offset), *size)
            .expect("a valid record");
    }
    assert_eq!(w.finish().expect("a valid body finishes"), data);
});
