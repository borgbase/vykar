#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

#[macro_use]
mod hash_id;

pub mod chunk_id;
pub mod error;
pub mod pack_id;
pub mod snapshot_id;
