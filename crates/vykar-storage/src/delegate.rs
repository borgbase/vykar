//! Boilerplate-free `StorageBackend` implementations: delegation for wrappers,
//! "unsupported" bodies for leaves.
//!
//! Every wrapper that holds an inner backend (rate limiter, test recorder, the
//! `Arc<dyn StorageBackend>` coercion impl) has to forward all 14 trait methods,
//! and hand-written forwards rot — a forgotten one disabled server-side pack
//! verification and server init on any throttled REST repo.
//!
//! Two pieces keep that from recurring:
//!
//! * [`crate::delegate_storage_backend!`] holds the forwarding inventory in one
//!   place. A wrapper implements [`InnerBackend`], names the methods it writes by
//!   hand, and gets forwards for the rest.
//! * The trait's four capability methods (`server_repack`, `batch_delete_keys`,
//!   `server_verify_packs`, `server_init`) have **no defaults**, so the inventory
//!   cannot silently fall behind the trait: adding a fifth breaks every impl,
//!   this macro included, until it is handled. Leaves that support none of them
//!   use [`crate::unsupported_server_ops!`].
//!
//! What is *not* guarded: a new method with a default body. Wrappers would
//! inherit it, which is safe by construction for the defaults that exist today
//! (each routes back through the wrapper's own `get`/`put`/`get_range`), but a
//! future default that reports a capability must be added as a required method
//! instead — see the comment on the trait.

use crate::StorageBackend;

/// A `StorageBackend` wrapper that can name the backend it wraps.
///
/// Implement this alongside [`crate::delegate_storage_backend!`], which builds the
/// forwarding half of the `StorageBackend` impl on top of it.
pub trait InnerBackend {
    /// The wrapped backend that non-overridden methods delegate to.
    fn inner_backend(&self) -> &dyn StorageBackend;
}

/// Types the macro expansion names. Re-exported so the macro works in any crate
/// without requiring `vykar-types`/`vykar-protocol` to be in scope there.
#[doc(hidden)]
pub mod __macro_support {
    pub use vykar_protocol::{
        RepackPlanRequest, RepackResultResponse, VerifyPacksPlanRequest, VerifyPacksResponse,
    };
    pub use vykar_types::error::{Result, VykarError};
}

/// Implement the four capability methods as `UnsupportedBackend` for a backend
/// with no server-side APIs.
///
/// Used inside an `impl StorageBackend for _` block. `except [...]` names the
/// ones the backend does implement itself, in trait-declaration order — same
/// convention as [`crate::delegate_storage_backend!`].
///
/// ```text
/// impl StorageBackend for LocalBackend {
///     // ... data methods ...
///     unsupported_server_ops!();
/// }
/// ```
#[macro_export]
macro_rules! unsupported_server_ops {
    () => {
        $crate::unsupported_server_ops!(except []);
    };
    (except [$($except:ident),* $(,)?]) => {
        $crate::unsupported_server_ops!(@emit
            [server_repack batch_delete_keys server_verify_packs server_init]
            [$($except)*]);
    };

    (@emit [] []) => {};
    (@emit [] [$($unknown:ident)*]) => {
        compile_error!(
            "unsupported_server_ops!: `except` entries must be capability methods \
             listed in trait-declaration order"
        );
    };

    (@emit [server_repack $($rest:ident)*] [server_repack $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [server_repack $($rest:ident)*] [$($except:ident)*]) => {
        fn server_repack(
            &self,
            _plan: &$crate::__macro_support::RepackPlanRequest,
        ) -> $crate::__macro_support::Result<$crate::__macro_support::RepackResultResponse> {
            Err($crate::__macro_support::VykarError::UnsupportedBackend(
                "server-side repack API".into(),
            ))
        }
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };

    (@emit [batch_delete_keys $($rest:ident)*] [batch_delete_keys $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [batch_delete_keys $($rest:ident)*] [$($except:ident)*]) => {
        fn batch_delete_keys(&self, _keys: &[String]) -> $crate::__macro_support::Result<()> {
            Err($crate::__macro_support::VykarError::UnsupportedBackend(
                "batch delete API".into(),
            ))
        }
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };

    (@emit [server_verify_packs $($rest:ident)*] [server_verify_packs $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [server_verify_packs $($rest:ident)*] [$($except:ident)*]) => {
        fn server_verify_packs(
            &self,
            _plan: &$crate::__macro_support::VerifyPacksPlanRequest,
        ) -> $crate::__macro_support::Result<$crate::__macro_support::VerifyPacksResponse> {
            Err($crate::__macro_support::VykarError::UnsupportedBackend(
                "server-side verify-packs API".into(),
            ))
        }
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };

    (@emit [server_init $($rest:ident)*] [server_init $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [server_init $($rest:ident)*] [$($except:ident)*]) => {
        fn server_init(&self) -> $crate::__macro_support::Result<()> {
            Err($crate::__macro_support::VykarError::UnsupportedBackend(
                "server-side init API".into(),
            ))
        }
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
}

/// Implement `StorageBackend` for a wrapper by forwarding to its
/// [`InnerBackend::inner_backend`].
///
/// ```text
/// impl InnerBackend for Throttled {
///     fn inner_backend(&self) -> &dyn StorageBackend { &*self.inner }
/// }
///
/// delegate_storage_backend! {
///     for Throttled;
///     except [get, put];
///
///     fn get(&self, key: &str) -> Result<Option<Vec<u8>>> { /* custom */ }
///     fn put(&self, key: &str, data: &[u8]) -> Result<()> { /* custom */ }
/// }
/// ```
///
/// The `except` list names the methods written by hand below it; every other
/// trait method gets a plain forward. Entries must appear in trait-declaration
/// order — a misordered or unknown name is a compile error, and a hand-written
/// method missing from the list collides with the generated forward.
#[macro_export]
macro_rules! delegate_storage_backend {
    (for $ty:ty; except [$($except:ident),* $(,)?]; $($custom:tt)*) => {
        impl $crate::StorageBackend for $ty {
            $($custom)*

            $crate::delegate_storage_backend!(@forward
                [get put delete exists list get_range get_range_into create_dir
                 put_owned size server_repack batch_delete_keys
                 server_verify_packs server_init]
                [$($except)*]);
        }
    };

    // ── Muncher ────────────────────────────────────────────────────────────
    //
    // Walks the trait method list and the `except` list in lockstep. For each
    // method there are two arms: the first matches when the method heads the
    // `except` list (skip it — the caller wrote it), the second emits the
    // forward. Both lists are in trait order, so the parallel walk is exact.

    (@forward [] []) => {};
    (@forward [] [$($unknown:ident)*]) => {
        compile_error!(
            "delegate_storage_backend!: `except` entries must be StorageBackend \
             methods listed in trait-declaration order"
        );
    };

    (@forward [get $($rest:ident)*] [get $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [get $($rest:ident)*] [$($except:ident)*]) => {
        fn get(&self, key: &str) -> $crate::__macro_support::Result<Option<Vec<u8>>> {
            $crate::InnerBackend::inner_backend(self).get(key)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [put $($rest:ident)*] [put $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [put $($rest:ident)*] [$($except:ident)*]) => {
        fn put(&self, key: &str, data: &[u8]) -> $crate::__macro_support::Result<()> {
            $crate::InnerBackend::inner_backend(self).put(key, data)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [delete $($rest:ident)*] [delete $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [delete $($rest:ident)*] [$($except:ident)*]) => {
        fn delete(&self, key: &str) -> $crate::__macro_support::Result<()> {
            $crate::InnerBackend::inner_backend(self).delete(key)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [exists $($rest:ident)*] [exists $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [exists $($rest:ident)*] [$($except:ident)*]) => {
        fn exists(&self, key: &str) -> $crate::__macro_support::Result<bool> {
            $crate::InnerBackend::inner_backend(self).exists(key)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [list $($rest:ident)*] [list $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [list $($rest:ident)*] [$($except:ident)*]) => {
        fn list(&self, prefix: &str) -> $crate::__macro_support::Result<Vec<String>> {
            $crate::InnerBackend::inner_backend(self).list(prefix)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [get_range $($rest:ident)*] [get_range $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [get_range $($rest:ident)*] [$($except:ident)*]) => {
        fn get_range(
            &self,
            key: &str,
            offset: u64,
            length: u64,
        ) -> $crate::__macro_support::Result<Option<Vec<u8>>> {
            $crate::InnerBackend::inner_backend(self).get_range(key, offset, length)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [get_range_into $($rest:ident)*] [get_range_into $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [get_range_into $($rest:ident)*] [$($except:ident)*]) => {
        fn get_range_into(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &mut Vec<u8>,
        ) -> $crate::__macro_support::Result<bool> {
            $crate::InnerBackend::inner_backend(self).get_range_into(key, offset, length, buf)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [create_dir $($rest:ident)*] [create_dir $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [create_dir $($rest:ident)*] [$($except:ident)*]) => {
        fn create_dir(&self, key: &str) -> $crate::__macro_support::Result<()> {
            $crate::InnerBackend::inner_backend(self).create_dir(key)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [put_owned $($rest:ident)*] [put_owned $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [put_owned $($rest:ident)*] [$($except:ident)*]) => {
        fn put_owned(&self, key: &str, data: Vec<u8>) -> $crate::__macro_support::Result<()> {
            $crate::InnerBackend::inner_backend(self).put_owned(key, data)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [size $($rest:ident)*] [size $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [size $($rest:ident)*] [$($except:ident)*]) => {
        fn size(&self, key: &str) -> $crate::__macro_support::Result<Option<u64>> {
            $crate::InnerBackend::inner_backend(self).size(key)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [server_repack $($rest:ident)*] [server_repack $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [server_repack $($rest:ident)*] [$($except:ident)*]) => {
        fn server_repack(
            &self,
            plan: &$crate::__macro_support::RepackPlanRequest,
        ) -> $crate::__macro_support::Result<$crate::__macro_support::RepackResultResponse> {
            $crate::InnerBackend::inner_backend(self).server_repack(plan)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [batch_delete_keys $($rest:ident)*] [batch_delete_keys $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [batch_delete_keys $($rest:ident)*] [$($except:ident)*]) => {
        fn batch_delete_keys(&self, keys: &[String]) -> $crate::__macro_support::Result<()> {
            $crate::InnerBackend::inner_backend(self).batch_delete_keys(keys)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [server_verify_packs $($rest:ident)*] [server_verify_packs $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [server_verify_packs $($rest:ident)*] [$($except:ident)*]) => {
        fn server_verify_packs(
            &self,
            plan: &$crate::__macro_support::VerifyPacksPlanRequest,
        ) -> $crate::__macro_support::Result<$crate::__macro_support::VerifyPacksResponse> {
            $crate::InnerBackend::inner_backend(self).server_verify_packs(plan)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [server_init $($rest:ident)*] [server_init $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [server_init $($rest:ident)*] [$($except:ident)*]) => {
        fn server_init(&self) -> $crate::__macro_support::Result<()> {
            $crate::InnerBackend::inner_backend(self).server_init()
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use vykar_types::error::Result;

    /// Counts how many distinct trait methods reached the inner backend.
    ///
    /// Implements every method explicitly, so it doubles as a canary: a new
    /// capability method (one without a trait default) fails to compile here
    /// as well as in the macro's inventory.
    struct Counter {
        calls: AtomicUsize,
    }

    impl Counter {
        fn hit(&self) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl StorageBackend for Counter {
        fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            self.hit();
            Ok(None)
        }
        fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn delete(&self, _key: &str) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn exists(&self, _key: &str) -> Result<bool> {
            self.hit();
            Ok(false)
        }
        fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            self.hit();
            Ok(Vec::new())
        }
        fn get_range(&self, _key: &str, _offset: u64, _length: u64) -> Result<Option<Vec<u8>>> {
            self.hit();
            Ok(None)
        }
        fn get_range_into(
            &self,
            _key: &str,
            _offset: u64,
            _length: u64,
            _buf: &mut Vec<u8>,
        ) -> Result<bool> {
            self.hit();
            Ok(false)
        }
        fn create_dir(&self, _key: &str) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn put_owned(&self, _key: &str, _data: Vec<u8>) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn size(&self, _key: &str) -> Result<Option<u64>> {
            self.hit();
            Ok(None)
        }
        fn server_repack(
            &self,
            _plan: &vykar_protocol::RepackPlanRequest,
        ) -> Result<vykar_protocol::RepackResultResponse> {
            self.hit();
            Ok(vykar_protocol::RepackResultResponse {
                completed: Vec::new(),
            })
        }
        fn batch_delete_keys(&self, _keys: &[String]) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn server_verify_packs(
            &self,
            _plan: &vykar_protocol::VerifyPacksPlanRequest,
        ) -> Result<vykar_protocol::VerifyPacksResponse> {
            self.hit();
            Ok(vykar_protocol::VerifyPacksResponse {
                results: Vec::new(),
                truncated: false,
            })
        }
        fn server_init(&self) -> Result<()> {
            self.hit();
            Ok(())
        }
    }

    struct Wrapper {
        inner: Counter,
        custom_gets: AtomicUsize,
    }

    impl InnerBackend for Wrapper {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.inner
        }
    }

    delegate_storage_backend! {
        for Wrapper;
        except [get];

        fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            self.custom_gets.fetch_add(1, Ordering::SeqCst);
            Ok(Some(vec![1, 2, 3]))
        }
    }

    /// Every method except the overridden one must reach the inner backend.
    ///
    /// This covers the methods that *have* trait defaults — for those a
    /// forgotten forward compiles, so only a call-count assertion catches it.
    /// The four capability methods are covered by the compiler instead (no
    /// default ⇒ a missing forward is an error), which is what keeps the
    /// inventory from drifting behind the trait.
    #[test]
    fn forwards_every_method_but_the_override() {
        let w = Wrapper {
            inner: Counter {
                calls: AtomicUsize::new(0),
            },
            custom_gets: AtomicUsize::new(0),
        };

        assert_eq!(w.get("k").unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(w.custom_gets.load(Ordering::SeqCst), 1);
        assert_eq!(w.inner.calls.load(Ordering::SeqCst), 0, "get is overridden");

        w.put("k", b"v").unwrap();
        w.delete("k").unwrap();
        w.exists("k").unwrap();
        w.list("p").unwrap();
        w.get_range("k", 0, 1).unwrap();
        w.get_range_into("k", 0, 1, &mut Vec::new()).unwrap();
        w.create_dir("d").unwrap();
        w.put_owned("k", vec![1]).unwrap();
        w.size("k").unwrap();
        w.server_repack(&vykar_protocol::RepackPlanRequest {
            operations: Vec::new(),
            protocol_version: vykar_protocol::PROTOCOL_VERSION,
        })
        .unwrap();
        w.batch_delete_keys(&[]).unwrap();
        w.server_verify_packs(&vykar_protocol::VerifyPacksPlanRequest {
            packs: Vec::new(),
            protocol_version: vykar_protocol::PROTOCOL_VERSION,
        })
        .unwrap();
        w.server_init().unwrap();

        // 14 trait methods, 13 of them forwarded.
        assert_eq!(w.inner.calls.load(Ordering::SeqCst), 13);
    }
}
