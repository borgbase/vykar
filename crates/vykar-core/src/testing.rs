//! Test support for crates/vykar-core/tests/, vykar-cli and vykar-gui tests.
//! Not API; never enable the testing feature in a product build.

pub use crate::repo::with_cheap_kdf;

/// The passphrase of every repository the test helpers create.
pub const TEST_PASSPHRASE: &str = "vykar-test-passphrase";
