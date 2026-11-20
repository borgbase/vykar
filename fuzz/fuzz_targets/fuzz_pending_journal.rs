#![no_main]
//! `sessions/<id>.index`: written by other clients, read during crash
//! recovery. Covers the versioned and the legacy shapes.

use std::collections::HashSet;

use libfuzzer_sys::fuzz_target;
use vykar_core::index::{
    DecodedJournal, PENDING_JOURNAL_VERSION, PendingIndexJournal, decode_pending_journal,
    encode_pending_journal,
};

fuzz_target!(|data: &[u8]| {
    let entries = match decode_pending_journal(data) {
        Ok(DecodedJournal::V2(entries)) => entries,
        Ok(DecodedJournal::Unknown(version)) => {
            assert_ne!(version, PENDING_JOURNAL_VERSION);
            return;
        }
        Ok(DecodedJournal::Legacy) | Err(_) => return,
    };

    // Re-encoding is a fixed point.
    let encoded = encode_pending_journal(&entries).unwrap();
    let Ok(DecodedJournal::V2(again)) = decode_pending_journal(&encoded) else {
        panic!("re-encoded journal does not decode as v2");
    };
    assert_eq!(encode_pending_journal(&again).unwrap(), encoded);

    // Recovery seeds the session's journal pack by pack; a later entry for
    // the same pack replaces the earlier one.
    let distinct: HashSet<_> = entries.iter().map(|e| e.pack_id).collect();
    let mut journal = PendingIndexJournal::new();
    for entry in entries {
        journal.record_pack(entry.pack_id, entry.chunks);
    }
    assert_eq!(journal.len(), distinct.len());
    let wire = journal.to_wire();
    let encoded = encode_pending_journal(&wire).unwrap();
    let Ok(DecodedJournal::V2(round)) = decode_pending_journal(&encoded) else {
        panic!("journal written after recovery does not decode as v2");
    };
    assert_eq!(round.len(), distinct.len());
});
