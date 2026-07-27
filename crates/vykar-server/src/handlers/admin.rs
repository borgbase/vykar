mod batch_delete;
mod dispatch;
mod init;
mod list;
mod repack;
mod stats;
#[cfg(test)]
mod test_support;
mod verify_packs;
mod verify_structure;

pub use dispatch::{health, repo_action_dispatch, repo_dispatch};

/// Finish a streaming BLAKE2b-256 hash and render it as lowercase hex.
///
/// Shared by the repack and verify-packs handlers, which both hash pack bytes
/// as they stream past rather than buffering the whole pack.
fn finalize_blake2b_256_hex(hasher: blake2::Blake2bVar) -> String {
    use blake2::digest::VariableOutput;
    let mut out = [0u8; 32];
    hasher
        .finalize_variable(&mut out)
        .expect("valid output buffer length");
    hex::encode(out)
}
