#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::commands::list::for_each_decoded_item;

fuzz_target!(|data: &[u8]| {
    // Every item the decoder accepts survives re-encoding in both snapshot
    // formats (2: chunk IDs as bin, 1: as int arrays) with the same value.
    let _ = for_each_decoded_item(data, |item| {
        let v2 = rmp_serde::to_vec(&item).expect("encode format 2");
        let mut v1 = Vec::new();
        vykar_core::fuzzing::encode_item_v1(&item, &mut v1).expect("encode format 1");
        for bytes in [&v2, &v1] {
            let mut back = Vec::new();
            for_each_decoded_item(bytes, |i| {
                back.push(i);
                Ok(())
            })
            .expect("re-decode");
            assert_eq!(back, [item.clone()]);
        }
        Ok(())
    });
});
