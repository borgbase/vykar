//! Shared shape for the repository's 32-byte identifiers.

/// Declare a 32-byte identifier newtype with the surface all of them share.
///
/// Generates the struct, `from_bytes`/`as_bytes`/`to_hex`, and `Debug`/`Display`
/// (both abbreviated to the first 16 hex chars). Everything else is
/// deliberately left to the individual types — their constructors differ (keyed
/// hash / unkeyed hash / random), and `from_hex`, `shard_prefix` and
/// `storage_key` each apply to only two of the three.
///
/// The identifiers are wire-visible: the derived `Serialize`/`Deserialize` are
/// pinned by golden MessagePack byte assertions in `tests/wire_format.rs`.
macro_rules! hash_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, ::serde::Serialize, ::serde::Deserialize)]
        pub struct $name([u8; 32]);

        impl $name {
            #[doc = concat!("Wrap a 32-byte array as a `", stringify!($name),
                            "`. Any 32-byte value is a valid ID.")]
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// Borrow the raw 32 bytes (e.g. for AAD or wire-format writes).
            pub const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            #[doc = concat!("Hex-encode the full `", stringify!($name), "`.")]
            pub fn to_hex(&self) -> String {
                hex::encode(self.0)
            }
        }

        impl ::std::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), &self.to_hex()[..16])
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}", &self.to_hex()[..16])
            }
        }
    };
}
