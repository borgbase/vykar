#![no_main]
//! Storage keys and `Range` headers as `vykar-server` receives them over
//! the network, and the pack header it checks on upload.
//!
//! Input: a selector byte, then the string (or header bytes).

use std::path::{Component, Path};

use libfuzzer_sys::fuzz_target;
use vykar_protocol::{
    is_index_object_key, is_known_repo_key, is_safe_relative_path, is_temp_file, is_valid_pack_key,
    is_valid_storage_key, is_valid_upload_token, parse_byte_range, validate_pack_header,
    PACK_MAGIC, PACK_VERSION_MAX, PACK_VERSION_MIN, TEMP_FILE_PREFIX,
};

/// The lexical half of the server's key → path resolution: what
/// `AppState::file_path` joins under the data directory.
fn assert_stays_under_root(key: &str) {
    let root = Path::new("/srv/vykar/repo");
    let trimmed = key.trim_matches('/');
    let path = if trimmed.is_empty() {
        root.to_path_buf()
    } else {
        root.join(trimmed)
    };
    assert!(path.starts_with(root), "{key:?}");
    let rel = path.strip_prefix(root).unwrap();
    assert!(
        rel.components().all(|c| matches!(c, Component::Normal(_))),
        "{key:?}"
    );
    assert_eq!(
        rel.components().count(),
        trimmed.split('/').filter(|s| !s.is_empty()).count()
    );
    assert!(!key.contains('\0') && !key.contains('\\'));
}

fn check_key(key: &str) {
    let safe = is_safe_relative_path(key);
    let storage = is_valid_storage_key(key);
    if safe {
        assert_stays_under_root(key);
    }
    if storage {
        assert!(safe, "{key:?}");
        assert!(!is_temp_file(key), "{key:?}");
    }
    if is_valid_pack_key(key) {
        assert!(storage, "{key:?}");
        // The part-upload temp file (`handlers/parts.rs` `resolve`) is a
        // sibling of the pack, never somewhere else.
        let token = "0123456789abcdef0123456789abcdef";
        assert!(is_valid_upload_token(token));
        let path = Path::new("/srv/vykar/repo").join(key.trim_matches('/'));
        let name = path.file_name().unwrap().to_string_lossy();
        let temp = path
            .parent()
            .unwrap()
            .join(format!("{TEMP_FILE_PREFIX}{name}.{token}"));
        assert_eq!(temp.parent(), path.parent());
        assert!(is_temp_file(temp.to_str().unwrap()));
    }
    if is_index_object_key(key) {
        assert!(storage, "{key:?}");
    }
    let _ = is_known_repo_key(key);
    if is_valid_upload_token(key) {
        assert!(!key.contains('/') && !key.contains('.'));
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, rest)) = data.split_first() else {
        return;
    };
    match selector % 3 {
        0 => {
            if let Ok(key) = std::str::from_utf8(rest) {
                check_key(key);
            }
        }
        1 => {
            let Ok(header) = std::str::from_utf8(rest) else {
                return;
            };
            if let Ok((start, length)) = parse_byte_range(header) {
                assert!(length >= 1);
                let end = start.checked_add(length - 1).unwrap();
                assert_eq!(
                    parse_byte_range(&format!("bytes={start}-{end}")),
                    Ok((start, length))
                );
            }
        }
        _ => {
            if let Ok(version) = validate_pack_header(rest) {
                assert!(rest.starts_with(PACK_MAGIC));
                assert_eq!(rest[PACK_MAGIC.len()], version);
                assert!((PACK_VERSION_MIN..=PACK_VERSION_MAX).contains(&version));
            }
        }
    }
});
