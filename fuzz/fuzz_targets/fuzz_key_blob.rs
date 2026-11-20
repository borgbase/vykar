#![no_main]
//! `keys/repokey` is not authenticated before key derivation, so its KDF
//! parameters are attacker-chosen. Screened only; the KDF never runs.

use libfuzzer_sys::fuzz_target;
use vykar_crypto::key::inspect_key_blob;

fuzz_target!(|data: &[u8]| {
    let Ok(key) = inspect_key_blob(data) else {
        return;
    };
    let kdf = &key.kdf;
    assert_eq!(kdf.algorithm, "argon2id");
    assert!((1..=10).contains(&kdf.time_cost));
    assert!((1..=16).contains(&kdf.parallelism));
    assert!((1..=512 * 1024).contains(&kdf.memory_cost));
    assert!((16..=64).contains(&kdf.salt.len()));
    assert!(argon2::Params::new(kdf.memory_cost, kdf.time_cost, kdf.parallelism, Some(32)).is_ok());
    assert_eq!(key.nonce.len(), 12);

    // A blob that passed re-encodes to one that passes.
    let reencoded = rmp_serde::to_vec(&key).unwrap();
    assert!(inspect_key_blob(&reencoded).is_ok());
});
