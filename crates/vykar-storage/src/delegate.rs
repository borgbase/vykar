//! Boilerplate-free `StorageBackend` implementations: delegation for wrappers,
//! "unsupported" bodies for leaves.
//!
//! Every wrapper that holds an inner backend (rate limiter, test recorder, the
//! `Arc<dyn StorageBackend>` coercion impl) has to forward all 21 trait methods,
//! and hand-written forwards rot — a forgotten one disabled server-side pack
//! verification and server init on any throttled REST repo.
//!
//! Two pieces keep that from recurring:
//!
//! * [`crate::delegate_storage_backend!`] holds the forwarding inventory in one
//!   place. A wrapper implements [`InnerBackend`], names the methods it writes by
//!   hand, and gets forwards for the rest.
//! * The trait's six capability methods (`remove_stale_temp_files`,
//!   `bind_content_hash`, `conditional`, `batch_delete`, `server_ops`,
//!   `profile`) have **no defaults**, so the inventory cannot silently fall
//!   behind the trait: adding a seventh breaks every impl, this macro included,
//!   until it is handled. Leaves get bodies for the ones they do not implement
//!   from [`crate::unsupported_server_ops!`] (all but `profile`; the three
//!   accessors are `None` there).
//!
//! The defaulted `list_meta` is forwarded like every other method; a wrapper
//! that overrides `list_sized` must override it too (a compile-time guard).
//! Two more guards cover the accessors that hand out the inner backend's
//! objects: a wrapper that overrides `list_sized` must override `server_ops`
//! (its tree listing would bypass the override), and one that overrides
//! `delete` must override `batch_delete` (its deletes would bypass it).
//!
//! What is *not* guarded: a new method with a default body. Wrappers would
//! inherit it, which is safe by construction for the defaults that exist today
//! (each routes back through the wrapper's own `get`/`put`/`get_range`/`list`), but a
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
/// without requiring `vykar-types` to be in scope there.
#[doc(hidden)]
pub mod __macro_support {
    pub use vykar_types::error::{Result, VykarError};
    pub use vykar_types::hash::HashAlgorithm;
}

/// Implement the capability methods for a backend with no vykar-server behind
/// it: a no-op for `bind_content_hash` (nothing to bind — only `RestBackend`
/// sends a content-digest header), `Ok(0)` for `remove_stale_temp_files` (for
/// backends that leave no client-side temp files; Local and SFTP implement it),
/// and `None` for the three accessors `conditional` (no put-if-absent),
/// `batch_delete` (delete key by key) and `server_ops` (no server).
///
/// Used inside an `impl StorageBackend for _` block. `except [...]` names the
/// ones the backend does implement itself, in trait-declaration order — same
/// convention as [`crate::delegate_storage_backend!`].
///
/// Does not supply `profile`: every backend states its own.
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
            [remove_stale_temp_files bind_content_hash conditional batch_delete server_ops]
            [$($except)*]);
    };

    (@emit [] []) => {};
    (@emit [] [$($unknown:ident)*]) => {
        compile_error!(
            "unsupported_server_ops!: `except` entries must be capability methods \
             listed in trait-declaration order"
        );
    };

    (@emit [remove_stale_temp_files $($rest:ident)*] [remove_stale_temp_files $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [remove_stale_temp_files $($rest:ident)*] [$($except:ident)*]) => {
        fn remove_stale_temp_files(
            &self,
            _older_than: std::time::Duration,
        ) -> $crate::__macro_support::Result<usize> {
            Ok(0)
        }
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };

    (@emit [bind_content_hash $($rest:ident)*] [bind_content_hash $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [bind_content_hash $($rest:ident)*] [$($except:ident)*]) => {
        // No-op, not an error: these backends send no content-digest header,
        // so there is nothing for the algorithm to select. Only `RestBackend`
        // has a real implementation.
        fn bind_content_hash(
            &self,
            _algo: $crate::__macro_support::HashAlgorithm,
        ) -> $crate::__macro_support::Result<()> {
            Ok(())
        }
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };

    (@emit [conditional $($rest:ident)*] [conditional $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [conditional $($rest:ident)*] [$($except:ident)*]) => {
        fn conditional(&self) -> Option<&dyn $crate::ConditionalWrite> {
            None
        }
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };

    (@emit [batch_delete $($rest:ident)*] [batch_delete $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [batch_delete $($rest:ident)*] [$($except:ident)*]) => {
        fn batch_delete(&self) -> Option<&dyn $crate::BatchDelete> {
            None
        }
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };

    (@emit [server_ops $($rest:ident)*] [server_ops $($except:ident)*]) => {
        $crate::unsupported_server_ops!(@emit [$($rest)*] [$($except)*]);
    };
    (@emit [server_ops $($rest:ident)*] [$($except:ident)*]) => {
        fn server_ops(&self) -> Option<&dyn $crate::ServerOps> {
            None
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
                [get put delete exists list get_range get_range_into open_range create_dir
                 put_owned put_pack begin_staged_put size list_sized list_meta
                 remove_stale_temp_files bind_content_hash conditional batch_delete
                 server_ops profile]
                [$($except)*]);
        }
        $crate::delegate_storage_backend!(@check_list [$($except)*] [] []);
        $crate::delegate_storage_backend!(@check_meta [$($except)*] [] []);
        $crate::delegate_storage_backend!(@check_cond [$($except)*] [] []);
        $crate::delegate_storage_backend!(@check_put [$($except)*] [] []);
        $crate::delegate_storage_backend!(@check_pack [$($except)*] [] []);
        $crate::delegate_storage_backend!(@check_range [$($except)*] [] []);
        $crate::delegate_storage_backend!(@check_staged [$($except)*] [] []);
        $crate::delegate_storage_backend!(@check_server [$($except)*] [] []);
        $crate::delegate_storage_backend!(@check_batch [$($except)*] [] []);
    };

    // ── Guard: overriding `begin_staged_put` requires `profile` ───────────
    //
    // A forwarded `profile` reports the inner backend's `staged_put`, which
    // is wrong for a wrapper that answers `begin_staged_put` itself.

    (@check_staged [] [] $profile:tt) => {};
    (@check_staged [] [begin_staged_put] [profile]) => {};
    (@check_staged [] [begin_staged_put] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `begin_staged_put` \
             must also override `profile`, so `staged_put` matches what it returns"
        );
    };
    (@check_staged [begin_staged_put $($rest:ident)*] [] $profile:tt) => {
        $crate::delegate_storage_backend!(@check_staged [$($rest)*] [begin_staged_put] $profile);
    };
    (@check_staged [profile $($rest:ident)*] $staged:tt []) => {
        $crate::delegate_storage_backend!(@check_staged [$($rest)*] $staged [profile]);
    };
    (@check_staged [$other:ident $($rest:ident)*] $staged:tt $profile:tt) => {
        $crate::delegate_storage_backend!(@check_staged [$($rest)*] $staged $profile);
    };

    // ── Guard: overriding `get_range_into` requires `open_range` ──────────
    //
    // A forwarded `open_range` hands out the inner backend's reader, which
    // never passes through the wrapper's `get_range_into` override (rate
    // limit, request counter, fault injection). `vykar_storage::buffered_range`
    // with `self` keeps every read on the intercepted path.

    (@check_range [] [] $open:tt) => {};
    (@check_range [] [get_range_into] [open_range]) => {};
    (@check_range [] [get_range_into] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `get_range_into` \
             must also override `open_range`, or streamed range reads bypass it"
        );
    };
    (@check_range [get_range_into $($rest:ident)*] [] $open:tt) => {
        $crate::delegate_storage_backend!(@check_range [$($rest)*] [get_range_into] $open);
    };
    (@check_range [open_range $($rest:ident)*] $into:tt []) => {
        $crate::delegate_storage_backend!(@check_range [$($rest)*] $into [open_range]);
    };
    (@check_range [$other:ident $($rest:ident)*] $into:tt $open:tt) => {
        $crate::delegate_storage_backend!(@check_range [$($rest)*] $into $open);
    };

    // ── Guard: overriding `put`/`put_owned`/`put_pack` requires `begin_staged_put`
    //
    // A forwarded `begin_staged_put` hands out the inner backend's staged
    // writer, whose `commit` publishes without passing through the wrapper's
    // `put` override (rate limit, recorder, fault injection). The wrapper
    // must state what it wants; `Ok(None)` keeps everything on `put`.

    (@check_put [] [] $staged:tt) => {};
    (@check_put [] [put] [begin_staged_put]) => {};
    (@check_put [] [put] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `put`, `put_owned` or \
             `put_pack` must also override `begin_staged_put`, or staged writes bypass it"
        );
    };
    (@check_put [put $($rest:ident)*] $put:tt $staged:tt) => {
        $crate::delegate_storage_backend!(@check_put [$($rest)*] [put] $staged);
    };
    (@check_put [put_owned $($rest:ident)*] $put:tt $staged:tt) => {
        $crate::delegate_storage_backend!(@check_put [$($rest)*] [put] $staged);
    };
    (@check_put [put_pack $($rest:ident)*] $put:tt $staged:tt) => {
        $crate::delegate_storage_backend!(@check_put [$($rest)*] [put] $staged);
    };
    (@check_put [begin_staged_put $($rest:ident)*] $put:tt []) => {
        $crate::delegate_storage_backend!(@check_put [$($rest)*] $put [begin_staged_put]);
    };
    (@check_put [$other:ident $($rest:ident)*] $put:tt $staged:tt) => {
        $crate::delegate_storage_backend!(@check_put [$($rest)*] $put $staged);
    };

    // ── Guard: overriding `put`/`put_owned` requires `put_pack` ───────────
    //
    // A forwarded `put_pack` hands sealed packs straight to the inner backend,
    // past the wrapper's `put` override (rate limit, recorder, fault injection).
    // No runtime test: the workspace has no `trybuild`.

    (@check_pack [] [] $pack:tt) => {};
    (@check_pack [] [put] [put_pack]) => {};
    (@check_pack [] [put] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `put` or `put_owned` \
             must also override `put_pack`, or pack uploads bypass it"
        );
    };
    (@check_pack [put $($rest:ident)*] $put:tt $pack:tt) => {
        $crate::delegate_storage_backend!(@check_pack [$($rest)*] [put] $pack);
    };
    (@check_pack [put_owned $($rest:ident)*] $put:tt $pack:tt) => {
        $crate::delegate_storage_backend!(@check_pack [$($rest)*] [put] $pack);
    };
    (@check_pack [put_pack $($rest:ident)*] $put:tt []) => {
        $crate::delegate_storage_backend!(@check_pack [$($rest)*] $put [put_pack]);
    };
    (@check_pack [$other:ident $($rest:ident)*] $put:tt $pack:tt) => {
        $crate::delegate_storage_backend!(@check_pack [$($rest)*] $put $pack);
    };

    // ── Guard: overriding `put`/`put_owned`/`put_pack` requires `conditional`
    //
    // A forwarded `conditional` hands out the inner backend's put-if-absent,
    // which writes without passing through the wrapper's `put` override.

    (@check_cond [] [] $cond:tt) => {};
    (@check_cond [] [put] [conditional]) => {};
    (@check_cond [] [put] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `put`, `put_owned` or \
             `put_pack` must also override `conditional`, or conditional writes bypass \
             it; returning None keeps every write on `put`"
        );
    };
    (@check_cond [put $($rest:ident)*] $put:tt $cond:tt) => {
        $crate::delegate_storage_backend!(@check_cond [$($rest)*] [put] $cond);
    };
    (@check_cond [put_owned $($rest:ident)*] $put:tt $cond:tt) => {
        $crate::delegate_storage_backend!(@check_cond [$($rest)*] [put] $cond);
    };
    (@check_cond [put_pack $($rest:ident)*] $put:tt $cond:tt) => {
        $crate::delegate_storage_backend!(@check_cond [$($rest)*] [put] $cond);
    };
    (@check_cond [conditional $($rest:ident)*] $put:tt []) => {
        $crate::delegate_storage_backend!(@check_cond [$($rest)*] $put [conditional]);
    };
    (@check_cond [$other:ident $($rest:ident)*] $put:tt $cond:tt) => {
        $crate::delegate_storage_backend!(@check_cond [$($rest)*] $put $cond);
    };

    // ── Guard: overriding `list_sized` requires `list_meta` ───────────────
    //
    // Otherwise the generated `list_meta` forwards straight to the inner
    // backend, bypassing whatever the `list_sized` override intercepts.

    (@check_meta [] [] $meta:tt) => {};
    (@check_meta [] [list_sized] [list_meta]) => {};
    (@check_meta [] [list_sized] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `list_sized` must also \
             override `list_meta`, or listings with mtime bypass it"
        );
    };
    (@check_meta [list_sized $($rest:ident)*] [] $meta:tt) => {
        $crate::delegate_storage_backend!(@check_meta [$($rest)*] [list_sized] $meta);
    };
    (@check_meta [list_meta $($rest:ident)*] $sized:tt []) => {
        $crate::delegate_storage_backend!(@check_meta [$($rest)*] $sized [list_meta]);
    };
    (@check_meta [$other:ident $($rest:ident)*] $sized:tt $meta:tt) => {
        $crate::delegate_storage_backend!(@check_meta [$($rest)*] $sized $meta);
    };

    // ── Guard: overriding `list` requires overriding `list_sized` ─────────
    //
    // Otherwise the generated `list_sized` forwards straight to the inner
    // backend, silently bypassing whatever the `list` override intercepts.

    (@check_list [] [] $sized:tt) => {};
    (@check_list [] [list] [list_sized]) => {};
    (@check_list [] [list] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `list` must also \
             override `list_sized`, or sized listings bypass it"
        );
    };
    (@check_list [list $($rest:ident)*] [] $sized:tt) => {
        $crate::delegate_storage_backend!(@check_list [$($rest)*] [list] $sized);
    };
    (@check_list [list_sized $($rest:ident)*] $list:tt []) => {
        $crate::delegate_storage_backend!(@check_list [$($rest)*] $list [list_sized]);
    };
    (@check_list [$other:ident $($rest:ident)*] $list:tt $sized:tt) => {
        $crate::delegate_storage_backend!(@check_list [$($rest)*] $list $sized);
    };

    // ── Guard: overriding `list_sized` requires `server_ops` ──────────────
    //
    // A forwarded `server_ops` hands out the inner backend's one-request tree
    // listing, which bypasses whatever the `list_sized` override intercepts.

    (@check_server [] [] $ops:tt) => {};
    (@check_server [] [list_sized] [server_ops]) => {};
    (@check_server [] [list_sized] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `list_sized` must also \
             override `server_ops`, or one-request tree listings bypass it; returning \
             None keeps listings on `list_sized`"
        );
    };
    (@check_server [list_sized $($rest:ident)*] [] $ops:tt) => {
        $crate::delegate_storage_backend!(@check_server [$($rest)*] [list_sized] $ops);
    };
    (@check_server [server_ops $($rest:ident)*] $sized:tt []) => {
        $crate::delegate_storage_backend!(@check_server [$($rest)*] $sized [server_ops]);
    };
    (@check_server [$other:ident $($rest:ident)*] $sized:tt $ops:tt) => {
        $crate::delegate_storage_backend!(@check_server [$($rest)*] $sized $ops);
    };

    // ── Guard: overriding `delete` requires `batch_delete` ─────────────────
    //
    // A forwarded `batch_delete` hands out the inner backend's deleter, whose
    // deletes bypass the wrapper's `delete` override.

    (@check_batch [] [] $batch:tt) => {};
    (@check_batch [] [delete] [batch_delete]) => {};
    (@check_batch [] [delete] []) => {
        compile_error!(
            "delegate_storage_backend!: a wrapper that overrides `delete` must also \
             override `batch_delete`, or batch deletes bypass it; returning None keeps \
             every delete on `delete`"
        );
    };
    (@check_batch [delete $($rest:ident)*] [] $batch:tt) => {
        $crate::delegate_storage_backend!(@check_batch [$($rest)*] [delete] $batch);
    };
    (@check_batch [batch_delete $($rest:ident)*] $del:tt []) => {
        $crate::delegate_storage_backend!(@check_batch [$($rest)*] $del [batch_delete]);
    };
    (@check_batch [$other:ident $($rest:ident)*] $del:tt $batch:tt) => {
        $crate::delegate_storage_backend!(@check_batch [$($rest)*] $del $batch);
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

    (@forward [open_range $($rest:ident)*] [open_range $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [open_range $($rest:ident)*] [$($except:ident)*]) => {
        fn open_range<'__range>(
            &self,
            key: &str,
            offset: u64,
            length: u64,
            buf: &'__range mut Vec<u8>,
        ) -> $crate::__macro_support::Result<
            Option<Box<dyn $crate::RangeReader + '__range>>,
        > {
            $crate::InnerBackend::inner_backend(self).open_range(key, offset, length, buf)
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

    (@forward [put_pack $($rest:ident)*] [put_pack $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [put_pack $($rest:ident)*] [$($except:ident)*]) => {
        fn put_pack(
            &self,
            key: &str,
            data: $crate::PackData,
        ) -> $crate::__macro_support::Result<()> {
            $crate::InnerBackend::inner_backend(self).put_pack(key, data)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [begin_staged_put $($rest:ident)*] [begin_staged_put $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [begin_staged_put $($rest:ident)*] [$($except:ident)*]) => {
        fn begin_staged_put(
            &self,
            prefix: &str,
        ) -> $crate::__macro_support::Result<Option<Box<dyn $crate::StagedPut>>> {
            $crate::InnerBackend::inner_backend(self).begin_staged_put(prefix)
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

    (@forward [list_sized $($rest:ident)*] [list_sized $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [list_sized $($rest:ident)*] [$($except:ident)*]) => {
        fn list_sized(
            &self,
            prefix: &str,
        ) -> $crate::__macro_support::Result<Vec<$crate::ListedObject>> {
            $crate::InnerBackend::inner_backend(self).list_sized(prefix)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [list_meta $($rest:ident)*] [list_meta $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [list_meta $($rest:ident)*] [$($except:ident)*]) => {
        fn list_meta(
            &self,
            prefix: &str,
        ) -> $crate::__macro_support::Result<Vec<$crate::ListedObject>> {
            $crate::InnerBackend::inner_backend(self).list_meta(prefix)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [remove_stale_temp_files $($rest:ident)*] [remove_stale_temp_files $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [remove_stale_temp_files $($rest:ident)*] [$($except:ident)*]) => {
        fn remove_stale_temp_files(
            &self,
            older_than: ::std::time::Duration,
        ) -> $crate::__macro_support::Result<usize> {
            $crate::InnerBackend::inner_backend(self).remove_stale_temp_files(older_than)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [bind_content_hash $($rest:ident)*] [bind_content_hash $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [bind_content_hash $($rest:ident)*] [$($except:ident)*]) => {
        fn bind_content_hash(
            &self,
            algo: $crate::__macro_support::HashAlgorithm,
        ) -> $crate::__macro_support::Result<()> {
            $crate::InnerBackend::inner_backend(self).bind_content_hash(algo)
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [conditional $($rest:ident)*] [conditional $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [conditional $($rest:ident)*] [$($except:ident)*]) => {
        fn conditional(&self) -> Option<&dyn $crate::ConditionalWrite> {
            $crate::InnerBackend::inner_backend(self).conditional()
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [batch_delete $($rest:ident)*] [batch_delete $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [batch_delete $($rest:ident)*] [$($except:ident)*]) => {
        fn batch_delete(&self) -> Option<&dyn $crate::BatchDelete> {
            $crate::InnerBackend::inner_backend(self).batch_delete()
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [server_ops $($rest:ident)*] [server_ops $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [server_ops $($rest:ident)*] [$($except:ident)*]) => {
        fn server_ops(&self) -> Option<&dyn $crate::ServerOps> {
            $crate::InnerBackend::inner_backend(self).server_ops()
        }
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };

    (@forward [profile $($rest:ident)*] [profile $($except:ident)*]) => {
        $crate::delegate_storage_backend!(@forward [$($rest)*] [$($except)*]);
    };
    (@forward [profile $($rest:ident)*] [$($except:ident)*]) => {
        fn profile(&self) -> $crate::BackendProfile {
            $crate::InnerBackend::inner_backend(self).profile()
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
        fn open_range<'a>(
            &self,
            _key: &str,
            _offset: u64,
            _length: u64,
            _buf: &'a mut Vec<u8>,
        ) -> Result<Option<Box<dyn crate::RangeReader + 'a>>> {
            self.hit();
            Ok(None)
        }
        fn create_dir(&self, _key: &str) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn put_owned(&self, _key: &str, _data: Vec<u8>) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn put_pack(&self, _key: &str, _data: crate::PackData) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn begin_staged_put(&self, _prefix: &str) -> Result<Option<Box<dyn crate::StagedPut>>> {
            self.hit();
            Ok(None)
        }
        fn size(&self, _key: &str) -> Result<Option<u64>> {
            self.hit();
            Ok(None)
        }
        fn list_sized(&self, _prefix: &str) -> Result<Vec<crate::ListedObject>> {
            self.hit();
            Ok(Vec::new())
        }
        fn list_meta(&self, _prefix: &str) -> Result<Vec<crate::ListedObject>> {
            self.hit();
            Ok(Vec::new())
        }
        fn remove_stale_temp_files(&self, _older_than: std::time::Duration) -> Result<usize> {
            self.hit();
            Ok(0)
        }
        fn bind_content_hash(&self, _algo: vykar_types::hash::HashAlgorithm) -> Result<()> {
            self.hit();
            Ok(())
        }
        fn conditional(&self) -> Option<&dyn crate::ConditionalWrite> {
            self.hit();
            None
        }
        fn batch_delete(&self) -> Option<&dyn crate::BatchDelete> {
            self.hit();
            None
        }
        fn server_ops(&self) -> Option<&dyn crate::ServerOps> {
            self.hit();
            None
        }
        fn profile(&self) -> crate::BackendProfile {
            self.hit();
            crate::BackendProfile {
                local: false,
                staged_put: false,
                request_width: 3,
                transfer_width: 3,
            }
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
    /// The six capability methods are covered by the compiler instead (no
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
        assert!(w.open_range("k", 0, 1, &mut Vec::new()).unwrap().is_none());
        w.create_dir("d").unwrap();
        w.put_owned("k", vec![1]).unwrap();
        w.put_pack("k", crate::PackData::new(vec![1])).unwrap();
        assert!(w.begin_staged_put("p").unwrap().is_none());
        w.size("k").unwrap();
        w.list_sized("p").unwrap();
        w.remove_stale_temp_files(std::time::Duration::ZERO)
            .unwrap();
        w.bind_content_hash(vykar_types::hash::HashAlgorithm::Blake3)
            .unwrap();
        w.list_meta("p").unwrap();
        assert!(w.conditional().is_none());
        assert!(w.batch_delete().is_none());
        assert!(w.server_ops().is_none());
        let p = w.profile();
        assert_eq!(p.request_width, 3);
        assert_eq!(p.transfer_width, 3);

        // 21 trait methods, 20 of them forwarded.
        assert_eq!(w.inner.calls.load(Ordering::SeqCst), 20);
    }

    /// A leaf with a batch delete and a server behind it, for
    /// `forwards_the_accessors_objects`.
    struct ServerLeaf {
        deletes: AtomicUsize,
        inits: AtomicUsize,
    }

    impl crate::BatchDelete for ServerLeaf {
        fn delete_keys(&self, _keys: &[String], _scope: crate::BatchDeleteScope) -> Result<()> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl crate::ServerOps for ServerLeaf {
        fn offers(&self, _op: crate::ServerOp) -> bool {
            false
        }
        fn capabilities(&self) -> Result<vykar_protocol::ServerCapabilities> {
            Ok(vykar_protocol::ServerCapabilities::default())
        }
        fn init(&self) -> Result<()> {
            self.inits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl StorageBackend for ServerLeaf {
        fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        fn exists(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }
        fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn get_range(&self, _key: &str, _offset: u64, _length: u64) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn create_dir(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        crate::unsupported_server_ops!(except[batch_delete, server_ops]);
        fn batch_delete(&self) -> Option<&dyn crate::BatchDelete> {
            Some(self)
        }
        fn server_ops(&self) -> Option<&dyn crate::ServerOps> {
            Some(self)
        }
        fn profile(&self) -> crate::BackendProfile {
            crate::BackendProfile {
                local: false,
                staged_put: false,
                request_width: 1,
                transfer_width: 1,
            }
        }
    }

    struct PlainWrapper(ServerLeaf);

    impl InnerBackend for PlainWrapper {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.0
        }
    }

    delegate_storage_backend! {
        for PlainWrapper;
        except [];
    }

    /// The forwarded accessors hand out the leaf's own objects.
    #[test]
    fn forwards_the_accessors_objects() {
        let w = PlainWrapper(ServerLeaf {
            deletes: AtomicUsize::new(0),
            inits: AtomicUsize::new(0),
        });
        w.batch_delete()
            .expect("batch delete forwarded")
            .delete_keys(&[], crate::BatchDeleteScope::KeysOnly)
            .unwrap();
        w.server_ops()
            .expect("server ops forwarded")
            .init()
            .unwrap();
        assert_eq!(w.0.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(w.0.inits.load(Ordering::SeqCst), 1);
    }
}
