---
name: performance-review
description: >
  Review Rust codebases for performance anti-patterns, with emphasis on async
  runtimes, client-server networking, memory allocation, serialization, and
  concurrency. Use when asked to audit, review, or optimize Rust code for
  performance.
disable-model-invocation: true
---

# Rust Performance Review Skill

You are a Rust performance auditor. Your job is to scan Rust source code and
project configuration for known performance anti-patterns and report findings
with concrete fix suggestions.

## How to use this skill

1. Start by reading `Cargo.toml` and `Cargo.lock` to understand dependencies,
   the build profile, target platform, and whether the project uses async
   (tokio/async-std), networking (hyper/tonic/axum/actix/reqwest), or
   serialization (serde, serde_json, bincode, etc.).

2. Check Tier 1 (project config) issues first — these are free wins that
   require no code changes: LTO, codegen-units, target-cpu, panic mode,
   global allocator on musl.

3. Scan source files for Tier 2 (code pattern) issues using the reference
   below. Prioritize findings by estimated impact: async executor blocking
   and missing TCP_NODELAY are nearly always the highest-impact fixes.

4. Report findings grouped by severity (critical / warning / suggestion),
   with the file path, line reference, the problematic pattern, why it
   matters, and the recommended fix. Keep explanations terse — link to
   the relevant section of this reference for details.

## Severity classification

- **Critical**: Will cause production incidents under load (executor blocking,
  unbounded channels, connection-per-request, missing buffered I/O).
- **Warning**: Measurable performance loss, 2x+ degradation in affected path
  (wrong mutex type, SeqCst everywhere, Arc<Mutex<HashMap>>, serde_json for
  internal APIs, Vec without with_capacity in hot loops, clone abuse).
- **Suggestion**: Optimization opportunity, 5–20% improvement potential (LTO,
  PGO, binary serialization, zero-copy deserialization, SmallVec, vectored I/O,
  custom allocator).

## Key detection patterns (quick reference)

Blocking in async: `std::fs::`, `std::thread::sleep`, `std::net::`, `block_on`
inside `async fn`.

Networking: Missing `set_nodelay(true)`, raw `TcpStream` without
`BufReader`/`BufWriter`, `TcpStream::connect` in request handlers (no pool),
`to_socket_addrs()` in async code.

Memory: `Vec::new()` + loop `.push()` without `with_capacity`, `.clone()`
passed to `&T` parameter, `format!()` inside loops, `Arc<Mutex<HashMap>>`.

Serialization: `serde_json::Value` with known schema, `#[serde(untagged)]`,
`String` fields in `Deserialize` structs that could borrow.

Concurrency: `unbounded_channel()`, `Ordering::SeqCst`, adjacent atomics
without cache padding, `tokio::sync::Mutex` with no `.await` inside lock scope.

Config: Missing `lto`, `codegen-units > 1`, no `target-cpu=native`, musl
without `#[global_allocator]`.

## Full reference

The sections below contain the complete catalog of ~50 anti-patterns with
detection strategies, impact analysis, fixes, and real-world case studies.
Consult them for details when reporting findings.

---

## 1. Async runtime pitfalls that silently kill throughput

### Blocking the executor

The single most common Rust async performance bug is running blocking operations on tokio worker threads. Every call to `std::thread::sleep`, `std::fs::read`, synchronous DNS resolution, or CPU-heavy computation (bcrypt, compression) prevents the cooperative scheduler from switching tasks. Alice Ryhl (Tokio maintainer) states that **async code should never spend more than 10–100 microseconds without reaching an `.await`**. With 4 runtime threads and 100ms blocking per request, maximum throughput collapses to 40 req/s — with no errors or panics, just silent latency degradation.

**Detection signals:** Grep for `std::thread::sleep`, `std::fs::`, `std::net::TcpStream`, `std::io::Read`, `std::io::Write`, and `block_on` inside `async fn` or `async` blocks. Check for synchronous database drivers (e.g., `diesel` without `spawn_blocking`). The `tokio-console` tool measures task poll durations, and the `hud` crate provides zero-instrumentation blocking detection.

**Fix:** Use `tokio::task::spawn_blocking` for blocking I/O, `tokio::fs` for file operations, `tokio::time::sleep` for delays, and offload CPU-heavy work to `rayon` via a oneshot channel bridge.

### Sequential `.await` chains destroying concurrency

Rust futures are lazy — they do nothing until polled. Consecutive `let a = fetch_a().await; let b = fetch_b().await;` runs operations serially even when they're independent, making total time equal to the sum rather than the maximum.

**Detection:** Look for multiple consecutive `let x = something().await;` lines where operations don't depend on each other's results. An AST check can identify independent await expressions in sequence.

**Fix:** Use `tokio::join!` for a fixed set of futures, `JoinSet` for dynamic collections, or `FuturesUnordered` for stream-based processing.

### Wrong mutex type selection

Using `tokio::sync::Mutex` when `std::sync::Mutex` suffices incurs **~2x overhead**. The async mutex is only needed when the lock must be held across `.await` points. Conversely, holding a `std::sync::MutexGuard` across an `.await` causes compile errors on multi-threaded runtimes (the guard is `!Send`) or deadlocks on single-threaded runtimes.

**Detection:** Flag `tokio::sync::Mutex` where no `.await` occurs between lock acquisition and release. Flag `std::sync::Mutex` guards whose scope spans an `.await` point. Pattern: `.lock()` followed by `.await` without an intervening `drop()` or scope boundary.

### Large futures and stack pressure

Async functions compile into state machines where every variable held across an `.await` becomes a field in the future's struct. Large stack arrays, deeply nested async calls, or many local variables can create futures of **tens to hundreds of kilobytes** — the Fuchsia team observed 400KB single futures. Since Tokio 1.41.0, futures larger than 16KB (release) passed to `tokio::spawn` are automatically boxed, but the allocation cost remains.

**Detection:** Grep for large array declarations `[T; N]` where N is large inside async functions. Use `std::mem::size_of_val(&future)` to measure. `tokio-console` reports `size.bytes` per task.

### Cancellation safety in `select!`

When a `tokio::select!` branch completes, other branches' futures are dropped. Operations like `read_exact`, `write_all`, and `Mutex::lock` are **not cancellation-safe** — partial progress is silently lost, causing data corruption or starvation.

**Detection:** Flag `tokio::select!` or `futures::select!` in loops containing `read_exact`, `write_all`, `BufReader::read_line`, or other non-cancellation-safe methods. The Tokio docs annotate each method's cancellation safety status.

---

## 2. Networking configuration mistakes with outsized latency impact

### TCP_NODELAY: the number-one networking fix

By default, TCP uses Nagle's algorithm, which buffers small writes and delays sending until a full segment accumulates or an ACK arrives. For RPC and interactive protocols, this adds **40–200ms of latency per request**. Axum issue #2521 documented massive latency regressions because `TCP_NODELAY` wasn't set by default. The community consensus is emphatic: "It's always TCP_NODELAY."

**Detection:** Grep for every `TcpStream::connect` and `listener.accept()` call and verify that `set_nodelay(true)` follows. Check hyper/axum/tonic configuration for TCP_NODELAY settings.

### Missing buffered I/O on network streams

Every `read()` or `write()` on a raw `TcpStream` triggers a system call. The ScyllaDB Rust driver team discovered this was their **#1 performance bottleneck** — their driver issued at least one syscall per query, causing 2x CPU usage compared to their C++ driver. Wrapping streams with `BufReader`/`BufWriter` is a drop-in fix that batches operations into 8KB chunks by default.

**Detection:** Flag `TcpStream` used directly for read/write without `BufReader`/`BufWriter` wrapping. Check flamegraphs for excessive time in `sendmsg`/`recvmsg` syscalls.

### Connection pool exhaustion and DNS blocking

Without connection pooling, each request opens a new TCP connection plus TLS handshake plus authentication — **50–150ms overhead per connection**. Even with pools, exhaustion occurs when pools are undersized or connections are held across slow operations. Standard library DNS resolution (`ToSocketAddrs::to_socket_addrs()`) is synchronous and will block the async runtime.

**Detection:** Flag `TcpStream::connect` in request handlers (should use a pool). Flag `to_socket_addrs()` in async code. Flag `pool.get()` where the resulting connection spans unrelated `.await` calls. Verify reqwest clients configure `pool_idle_timeout` and `pool_max_idle_per_host`.

### HTTP/2 flow control and window sizing

Default HTTP/2 flow control windows (typically 64KB) throttle large message transfers. Tonic issue #569 documented that transferring 0.1GB took over a minute with default settings because flow control required many round-trips for window updates. Setting `http2_initial_stream_window_size`, `http2_initial_connection_window_size`, and enabling `http2_adaptive_window` resolves this.

**Detection:** Flag tonic/hyper servers transferring large payloads without custom HTTP/2 window configuration. Check for `SETTINGS_MAX_CONCURRENT_STREAMS` configuration to prevent resource exhaustion.

---

## 3. Memory allocation patterns that compound under load

### Heap allocation in hot paths

Each heap allocation involves acquiring a global lock, non-trivial bookkeeping, and possibly a system call. The Rust Performance Book states that **reducing allocations by 10 per million instructions can yield measurable improvements (~1%)**. Common offenders include `Vec::new()`, `String::new()`, `Box::new()`, and `format!()` inside loops.

**Detection:** Flag allocating constructors (`Vec::new()`, `String::new()`, `format!()`, `Box::new()`) inside `loop`, `for`, and `while` blocks. Use DHAT profiler to identify hot allocation sites.

**Fix:** Reuse buffers across calls by passing `&mut String` or `&mut Vec<T>` parameters. Use `write!` to a reusable buffer instead of `format!()`. Pre-allocate with `Vec::with_capacity()` when the size is known.

### Vec reallocations from missing `with_capacity`

Vec's growth strategy (0→4→8→16→32→64...) means pushing 1000 items one-by-one causes ~10 reallocations, each copying all existing elements. A rustc PR showed that adjusting Vec's initial growth reduced allocations by **10%+ and sped up benchmarks by up to 4%**.

**Detection:** Flag `Vec::new()` or `vec![]` followed by `.push()` in a loop with a known or estimable bound. Same for `String::with_capacity()` and `HashMap::with_capacity()`.

### Clone abuse and alternatives

Cloning heap-allocated data (Vec, String, HashMap) copies all data. The Rust Design Patterns book explicitly identifies "clone to satisfy the borrow checker" as an anti-pattern. Key alternatives:

- **`Cow<'a, str>`** for conditional ownership — avoids allocation when no modification is needed
- **`Arc<T>`** for shared ownership — clone only increments a reference count
- **`clone_from(&b)`** instead of `a = b.clone()` — reuses `a`'s existing heap allocation
- **`as_deref()`** for `Option<String>` → `Option<&str>` conversion

**Detection:** Flag `.clone()` calls where the value is immediately passed to a function taking `&T`. Clippy's `redundant_clone` lint catches some cases. Flag functions returning `String` that sometimes return the input unchanged (should use `Cow`).

### SmallVec for short-lived small collections

`Vec` always heap-allocates when non-empty. For collections that typically hold fewer than 8–16 elements, `SmallVec<[T; N]>` stores them inline on the stack. The rustc compiler uses SmallVec extensively — PRs #50565 and #55383 showed **measurable compilation speed improvements**.

**Detection:** Flag `Vec<T>` in struct fields where profiling shows vectors typically contain fewer than 8–16 elements. Particularly valuable for compiler-like workloads, parsers, and intermediate results.

### Arc\<Mutex\<HashMap\>\> contention

Coarse-grained locking on a single `Arc<Mutex<HashMap>>` serializes all operations — even reads on different keys block each other. Alternatives ordered by use case:

- **`DashMap`**: Sharded `RwLock<HashMap>` with per-bucket locking. Best general-purpose concurrent map.
- **`scc::HashMap`**: Fine-grained bucket locks with epoch-based GC. Best for write-heavy workloads.
- **`papaya`**: Lock-free reads (RCU-style). Best for read-heavy workloads (no reader-side locking at all).

**Detection:** Flag `Arc<Mutex<HashMap<_,_>>>` and `Arc<RwLock<HashMap<_,_>>>` — suggest sharded alternatives.

---

## 4. Serialization choices that multiply latency

### Typed deserialization vs `serde_json::Value`

`serde_json::Value` creates a tree of heap-allocated nodes — every string, array, and object is a separate allocation. Typed deserialization into a struct is **1.5–2x faster** (550–710 MB/s vs 300–420 MB/s in json-benchmark).

**Detection:** Flag `serde_json::from_str` or `from_reader` with target type `serde_json::Value` when the schema is known. Pattern: deserialization to `Value` followed by field access with string keys like `v["name"]`.

### Zero-copy deserialization with `#[serde(borrow)]`

Every `String` field in a `#[derive(Deserialize)]` struct allocates heap memory and copies bytes from the input. Using `&'a str` with `#[serde(borrow)]` borrows directly from the input buffer — **~2x faster** for string-heavy payloads. The `zerovec` crate enables zero-heap-allocation deserialization for vectors.

**Detection:** Flag structs with `String` fields used with `Deserialize` where the input lifetime outlives usage. Pattern: `#[derive(Deserialize)]` structs with `String` fields that could be `&str` or `Cow<str>`.

**Constraint:** Only works with `from_str`/`from_slice`, not `from_reader`. JSON escape sequences force allocation.

### Binary formats vs JSON for internal APIs

JSON is **5–15x slower** than binary formats for serialization. Key benchmarks from `rust_serialization_benchmark`:

| Format | Relative speed | Best for |
|--------|---------------|----------|
| **bincode** | ~40ns ser, ~100ns deser | Fastest general-purpose |
| **rkyv** | ~21ns zero-copy deser | Total zero-copy, no parsing step |
| **bitcode** | Best combined scores | Newest, excellent compression |
| **simd-json** | 2–3x faster than serde_json | JSON-compatible with SIMD |
| **postcard** | ~60ns ser, ~180ns deser | Embedded-friendly, compact |
| **serde_json** | ~250ns ser, ~500ns deser | Human-readable only |

**Detection:** Flag `serde_json` usage in non-user-facing code paths (internal APIs, caches, IPC). Suggest binary formats for machine-to-machine communication.

### `#[serde(untagged)]` enum performance trap

Serde's official docs warn that untagged enums try each variant in order, deserializing and backtracking on failure. The input may be parsed **multiple times**. Use `#[serde(tag = "type")]` (internally tagged) or `#[serde(tag = "type", content = "data")]` (adjacently tagged) instead.

**Detection:** Flag `#[serde(untagged)]` attribute on enums, especially with many variants in hot deserialization paths.

---

## 5. Concurrency primitives that become bottlenecks

### Unbounded channels: a ticking OOM bomb

Unbounded channels (`tokio::sync::mpsc::unbounded_channel`, `crossbeam::channel::unbounded`) have no backpressure. If producers outpace consumers, memory grows without bound until OOM. Tokio issue #4321 documents that even after the spike clears, **memory is never deallocated** from blocks allocated during the spike. Community consensus: "nobody likes unboundedness and most have experienced production outages because of it."

**Detection:** Flag any usage of `unbounded_channel()` or `unbounded()`. This is a hard rule — always use bounded channels in production.

### Atomic ordering: SeqCst is almost never needed

`Ordering::SeqCst` establishes a total global ordering across all threads, requiring expensive memory barriers. Nomicon issue #166 (51+ upvotes) argues **"SeqCst as default ordering considered harmful."** Mara Bos's *Rust Atomics and Locks* calls it a "warning sign" — virtually all real-world uses can be expressed with `Acquire`/`Release`.

**Detection:** Flag all `Ordering::SeqCst` usage and suggest review. Guidelines: `Relaxed` for independent counters; `Acquire`/`Release` pairs for lock-like patterns; `SeqCst` only for cross-variable total ordering visible to all threads (very rare).

### False sharing destroying cache performance

Multiple threads modifying data on the same 64-byte cache line causes coherency traffic. Benchmarks show up to **300x slowdown with 32 threads** on Zen4. A std::sync::RwLock bug (rust-lang/rust #117470) where lock state is adjacent to protected data caused >10% throughput loss in the Hugging Face tokenizer.

**Detection:** Flag arrays of atomics `[AtomicXxx; N]` or structs with multiple adjacent atomics without `crossbeam_utils::CachePadded` or `#[repr(align(64))]`. Profile with `perf c2c` to identify false sharing.

### Global allocator contention with musl

The musl allocator has severe lock contention in multi-threaded workloads — **7x slowdown** vs glibc, with 13x more time in futex. The tweag.io team documented **20x slowdowns**. Ripgrep and Apache DataFusion explicitly swap allocators.

**Detection:** Flag musl target builds (`target_env = "musl"`) without a custom `#[global_allocator]`. Suggest `tikv-jemallocator` or `mimalloc` for multi-threaded workloads.

### Rayon nested parallelism deadlock

Rayon uses a fixed-size work-stealing pool (default: number of logical CPUs). If all threads run outer `par_iter()` work that each spawns inner `par_iter()` work on the same pool, inner tasks can never be scheduled — **instant deadlock with zero CPU, no error message**.

**Detection:** Flag nested `par_iter()` calls or `rayon::join`/`rayon::scope` inside `par_iter()`. Detect libraries that internally use rayon's global pool.

---

## 6. Compilation settings that leave performance on the table

### The maximum performance Cargo profile

The default release profile leaves significant optimization opportunities unused. **10–20% runtime improvement** is common from proper settings alone:

```toml
[profile.release]
opt-level = 3          # Already default, but verify
lto = "fat"            # Cross-crate optimization (10-20% improvement)
codegen-units = 1      # Maximum single-crate optimization
panic = "abort"        # Eliminates unwinding tables
strip = "symbols"      # Reduces binary size
```

In `.cargo/config.toml`:
```toml
[build]
rustflags = ["-C", "target-cpu=native"]  # Enable AVX2/SSE4.2 auto-vectorization
```

**Detection:** Parse `Cargo.toml` and flag when `lto` is absent or `false`, `codegen-units` is not 1, and `panic` is not `"abort"` in the release profile. Check `.cargo/config.toml` for `target-cpu` settings.

### Profile-Guided Optimization (PGO)

PGO can improve runtime speed by **10–40%** by feeding real-world execution profiles back to the compiler. The `cargo-pgo` crate simplifies the workflow. BOLT post-link optimization adds an additional 2–5% on top of PGO.

### Monomorphization bloat vs dynamic dispatch

Rust generics produce a specialized copy for every concrete type. When a generic function has a large body and is instantiated with many types, the resulting binary bloat causes **instruction cache (I-cache) pressure**, potentially degrading performance despite the inlining benefits. The tradeoff: static dispatch is ~3.4x faster inside tight loops, but outside loops the overhead is minimal (~1.2x).

**Detection:** Analyze generic function instantiation counts. Flag generic functions instantiated >N times with large bodies. Consider `dyn Trait` or enum dispatch when the set of implementing types is small and fixed.

---

## 7. Error handling costs that accumulate in hot paths

`Box<dyn Error>` requires a **heap allocation for every error instance**. In high-frequency error paths (parsing invalid data, validation), this adds measurable latency. `anyhow::Error` also heap-allocates (though it uses a narrow pointer — 8 bytes vs `Box<dyn Error>`'s 16 bytes).

**Detection:** Flag `Box<dyn Error>` and `anyhow::Error` in function signatures identified as hot paths. Flag `format!()` inside `Err()` constructors — each creates a heap-allocated String. Suggest `thiserror`-derived concrete enum types, which have **zero runtime overhead** (stack-allocated, no heap allocation).

The `?` operator itself compiles to a branch + early return that LLVM optimizes well. The real cost is in `From` conversions — if the `From` impl involves allocation, each `?` can heap-allocate on error.

---

## 8. I/O patterns that multiply syscall overhead

### Synchronous file I/O in async contexts

File I/O on Linux is **always blocking** — even `O_NONBLOCK` doesn't help for files. Using `std::fs` in async code blocks the entire worker thread. If all workers block, the runtime freezes completely — no timers, no network I/O, no progress.

**Detection:** Flag any `std::fs::*` call inside `async fn` or `async` blocks. Flag `std::io::Read`/`Write` on `std::fs::File` in async contexts.

### Missing vectored I/O (writev)

Sequential `write_all(&header); write_all(&body); write_all(&trailer);` makes three syscalls instead of one. Each involves a user-kernel context switch. Using `write_vectored` with `IoSlice` sends all buffers in a single `writev()` syscall — atomic and efficient.

**Detection:** Flag sequential `write()`/`write_all()` calls on the same writer. Suggest `write_vectored`. Note: hyper found that vectored writes can **hurt** performance with TLS implementations — verify with `is_write_vectored()`.

### io_uring for high-throughput Linux services

Traditional epoll requires one syscall per I/O operation and cannot do truly async file I/O. io_uring batches submissions via shared-memory ring buffers, can eliminate syscalls entirely in SQPOLL mode, and supports true async file I/O. ByteDance's **Monoio outperforms tokio by 26% in RPC benchmarks** with better linear scaling.

---

## 9. Clippy performance lints every project should enable

Clippy's `perf` category contains **25+ lints** that catch common performance regressions. The most impactful ones for networked applications:

| Lint | Detects | Fix |
|------|---------|-----|
| `box_collection` | `Box<Vec<T>>`, `Box<String>` | Remove outer Box — collections are already heap-allocated |
| `large_enum_variant` | Enum variants with >3x size disparity | Box the large variant |
| `needless_collect` | `.collect::<Vec<_>>()` followed by `.iter()` | Chain iterators directly |
| `or_fun_call` | `.unwrap_or(expensive_fn())` | Use `.unwrap_or_else(\|\| ...)` for lazy evaluation |
| `expect_fun_call` | `.expect(&format!("error: {}", x))` | format! always allocates, even in success path |
| `map_entry` | `.contains_key()` then `.insert()` | Use `.entry(key).or_insert(value)` — single lookup |
| `redundant_clone` | `.clone()` on value not used after | Remove the clone |
| `unnecessary_to_owned` | Unnecessary `.to_owned()`, `.to_string()` | Use borrowed value directly |
| `readonly_write_lock` | Write lock taken but only reads performed | Use `.read()` instead of `.write()` |
| `manual_memcpy` | Loop manually copying slices | Use `copy_from_slice` / memcpy |
| `useless_vec` | `vec![1, 2, 3]` used only as a slice | Use `&[1, 2, 3]` array literal |
| `single_char_pattern` | `"x"` instead of `'x'` in string search | Char matching is faster than string matching |

Additional non-perf lints with performance impact: `ptr_arg` (flag `&Vec<T>`, `&String` parameters — use `&[T]`, `&str`), and configurable `disallowed_types` (flag `std::collections::HashMap` and suggest `ahash::HashMap` or `FxHashMap`).

---

## 10. Real-world case studies from production systems

### Discord: microsecond latency from eliminating GC pauses

Discord's Read States service (billions of records, hundreds of thousands of updates/second) suffered **10–40ms latency spikes every 2 minutes** in Go due to garbage collection scanning millions of LRU cache objects. After rewriting in Rust with tokio, average latency dropped to **microseconds**, max @mention latency fell to single-digit milliseconds, and they scaled the LRU cache to 8 million entries — impossible in Go. Upgrading from tokio 0.1 to 0.2 gave "CPU benefits for free" thanks to the scheduler rewrite.

### Cloudflare Pingora: 70% less CPU than NGINX

Cloudflare replaced NGINX with Pingora, a Rust proxy framework built on tokio, for >1 trillion requests/day. Multi-threaded architecture (vs NGINX's multi-process) enabled shared connection pools, **reducing origin connections to 1/3**. Results: **70% less CPU, 67% less memory, 25% faster median response times**. Key patterns: shared references behind atomic reference counters, zero-copy where possible, elimination of C↔Lua data conversion overhead.

### Tokio scheduler rewrite: 10x microbenchmark improvement

Tokio's October 2019 scheduler rewrite (PR #1657) replaced a single global run queue (mutex-contended) with per-worker local queues using lock-free circular buffers. A "next task" slot prioritizes message receivers for cache-friendly execution. Result: **up to 10x improvement** on microbenchmarks with real-world gains in hyper and tonic.

### Library-specific gotchas for automated detection

- **reqwest:** `Client` uses internal `Arc` — wrapping in `Arc<Mutex<Client>>` serializes all requests (hyper issue #777 showed **12x slowdown**). Creating new `Client` per request wastes connection pools. Missing `pool_idle_timeout` causes file descriptor leaks (issue #1162).
- **tonic:** `Client::connect()` per gRPC request creates a new HTTP/2 connection each time — clone the client instead (it shares the underlying connection). Single channel for streaming becomes a serialization bottleneck (discussion #915).
- **hyper:** Vectored writes (`http1_writev`) can **hurt** performance with TLS. Default HTTP/2 window sizes throttle large transfers. HTTP/2 connections can enter 100% CPU loops on GoAway frames without `pool_idle_timeout` (issue #3140).
- **axum:** Pre-0.3, long `.route()` chains caused exponential compile time growth from nested generic types (issue #200). Always returns `Poll::Ready` from `poll_ready`, bypassing tower service backpressure.
- **actix-web:** Achieves top benchmark scores through per-worker single-threaded runtimes (eliminating work-stealing overhead), `FxHash` for routing tables, SSE4.2 SIMD for HTML escaping, and aggressive static dispatch via generics.

---

## Automated detection strategy for large codebases

An automated analysis tool should implement detection in three tiers:

**Tier 1 — Cargo.toml and project configuration** (always check): Missing LTO, codegen-units > 1, no `target-cpu=native`, no custom allocator with musl target, absence of `panic = "abort"` for binaries.

**Tier 2 — AST/pattern-based static analysis** (scan all source files): Blocking calls in async contexts (`std::fs::*`, `std::thread::sleep`, `to_socket_addrs()`), missing `TCP_NODELAY` after connection setup, `TcpStream` without `BufReader`/`BufWriter`, unbounded channels, `Arc<Mutex<HashMap>>`, `Box<dyn Error>` in hot paths, `serde_json::Value` with known schemas, `#[serde(untagged)]` on enums, `Ordering::SeqCst` usage, sequential `.await` on independent operations, `format!()` in loops, `.clone()` followed by `&` usage, `Vec::new()` followed by loop with `.push()`, `Client::connect()` per request.

**Tier 3 — Profile-guided analysis** (requires runtime data): Lock contention hotspots via `perf lock`, allocation site frequency via DHAT, false sharing via `perf c2c`, future sizes via `tokio-console`, syscall frequency via `strace`/flamegraphs, and monomorphization counts via `-Z print-type-sizes`.

## Conclusion

The most impactful fixes for Rust networking applications cluster around three themes. First, **respecting the async contract** — never block the executor, never hold locks across `.await` points, and use structured concurrency (`join!`) instead of sequential awaits. Second, **reducing per-request overhead** — set `TCP_NODELAY`, use buffered I/O, reuse connections and clients, prefer binary serialization and zero-copy deserialization. Third, **eliminating hidden allocation** — pre-allocate collections, use `Cow` and borrowing instead of cloning, choose concrete error types over `Box<dyn Error>`, and swap the global allocator on musl targets. Each of these patterns produces no compile errors, no runtime panics, and no log warnings — only silent performance degradation that compounds under production load. An automated tool that checks these ~50 patterns against a codebase would catch the vast majority of performance issues before they reach production.
