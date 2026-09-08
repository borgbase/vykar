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

// Pack digests are computed with `vykar_types::hash::Hasher256`, the same code
// path the client's `PackId::compute` uses, so the two agree by construction
// rather than by two hand-rolled implementations happening to match.
