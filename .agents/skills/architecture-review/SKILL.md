---
name: architecture-review
description: "Review and improve Rust application architecture for large CLI and backup systems, including workspace layout, crate boundaries, error handling, async/sync design, and testing strategy."
---

# Skill: Rust Application Architecture Review

Use this skill when reviewing, refactoring, or planning the structure of a large Rust application — especially CLI tools, backup utilities, or projects that combine local processing with REST API communication. This document covers workspace layout, module organization, error handling strategy, async/sync boundaries, CLI design, REST client patterns, testing, dependency management, performance optimization, CI enforcement, and state management.

Apply this skill by comparing the target codebase against each section's recommendations. Flag deviations as review findings, prioritizing structural issues (workspace layout, crate boundaries, error handling strategy) over stylistic ones (clippy lint choices, formatting). Not every recommendation will apply to every project — use judgment based on codebase size, team size, and project maturity.

**A CLI backup tool like Vykar — combining backup logic, REST client/server communication, and a rich command-line interface — demands disciplined architecture to remain maintainable as it grows.** This report distills best practices from the Rust ecosystem, drawing on patterns from ripgrep, rustic-rs, cargo, rust-analyzer, restic, and Borg. Each section provides actionable recommendations and concrete code patterns that a skilled developer can apply immediately when reviewing or refactoring a large Rust codebase.

---

# Structuring large Rust applications: a review guide for CLI backup tools

## 1. Project and module structure that scales

The single most impactful architectural decision is **how you split code across crates and modules**. A project with both CLI and REST server components should use a Cargo workspace from day one.

**The flat `crates/` layout** is the recommended pattern for projects between 10K–1M lines. Aleksey Kladov (rust-analyzer creator, 200K+ lines) advocates this strongly: `ls ./crates` gives an instant bird's-eye view, folder names match crate names, and the structure never deteriorates because flat hierarchies have no "where does this new crate go?" problem. ripgrep uses exactly this layout with 9 workspace crates under `crates/`.

A backup tool with a REST component should target this structure:

```
vykar/
├── Cargo.toml              # Virtual workspace manifest (no [package])
├── Cargo.lock              # Committed for reproducible builds
├── deny.toml               # cargo-deny configuration
├── rust-toolchain.toml     # Pin Rust version for contributors
├── crates/
│   ├── vykar-types/         # Shared data types (serde-enabled)
│   ├── vykar-core/          # Core backup logic (no network I/O)
│   ├── vykar-backend/       # Storage backend abstraction (local, S3, REST)
│   ├── vykar-client/        # REST API client library
│   ├── vykar-server/        # REST server binary
│   ├── vykar-cli/           # CLI binary
│   └── vykar-testing/       # Test fixtures and helpers
└── xtask/                  # Build automation (cargo xtask pattern)
```

**The "thin binary" pattern** keeps `main.rs` to under 20 lines. All business logic lives in `lib.rs` or separate library crates. This makes the codebase independently testable, reusable across multiple binaries, and accessible to integration tests:

```rust
// crates/vykar-cli/src/main.rs — thin entry point
fn main() -> std::process::ExitCode {
    if let Err(err) = vykar_cli::run() {
        eprintln!("Error: {err:#}");
        std::process::ExitCode::FAILURE
    } else {
        std::process::ExitCode::SUCCESS
    }
}
```

**Module hierarchy** should stay 2–3 levels deep maximum. Prefer file-based modules (Rust 2018+ style) over `mod.rs`. When a module file exceeds ~500 lines, split it into a directory with submodules. Use `pub use` re-exports in parent modules to flatten the public API — consumers should not need to navigate deep paths.

**Feature flags** must be additive: enabling a feature should never break existing functionality. Use the `dep:` syntax (Rust 1.60+) to separate feature names from dependency names. rustic-rs demonstrates excellent feature organization with groups for allocators (`mimalloc`, `jemallocator`), commands (`mount`, `self-update`, `webdav`), and filtering (`jq`). Test key feature combinations in CI — at minimum the default set, no-default-features, and all-features.

---

## 2. Workspace organization with shared types and clean boundaries

**`workspace.dependencies`** (stable since Rust 1.64) eliminates version drift by defining shared dependency versions at the workspace root. Every member crate then references these with `dep.workspace = true`:

```toml
# Root Cargo.toml
[workspace]
resolver = "3"
members = ["crates/*"]

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls-native-roots"] }
thiserror = "2"
anyhow = "1"
# Internal crates
vykar-core = { path = "crates/vykar-core" }
vykar-types = { path = "crates/vykar-types" }

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
```

**A shared types crate** (`vykar-types`) is critical for projects with both CLI and server components. It contains all data structures that cross crate boundaries: API request/response types, configuration structures, snapshot metadata, repository definitions, and error types that are part of the public API. Both `vykar-cli` and `vykar-server` depend on this crate, ensuring type consistency.

**Visibility strategy** should default to minimal exposure. Use `pub(crate)` aggressively for internal helpers that need cross-module access within a crate but shouldn't leak to consumers. Reserve `pub` for items that genuinely form the crate's public API. Avoid `pub(super)` — it usually signals that modules need reorganization.

The rustic-rs ecosystem demonstrates two viable approaches to workspace splitting: **mono-repo** (all crates in one repository under `crates/`) for tightly coupled components, or **multi-repo** (separate repositories for `rustic_core`, `rustic_backend`, `rustic_server`) for independently versioned libraries. For most projects, mono-repo with a flat `crates/` layout offers the best balance of cohesion and modularity.

---

## 3. Error handling that serves both developers and users

The common heuristic "thiserror for libraries, anyhow for binaries" is an oversimplification. **The real criterion is intent**: use `thiserror` when callers need to *handle* errors (match on variants, take different actions), and `anyhow` when callers need to *report* errors (propagate, log, display). Jane Lusby (Rust error handling WG) and Luca Palmieri both emphasize this distinction.

**For a backup CLI, the recommended hybrid approach** uses thiserror for structured error types in library crates and anyhow at the application boundary:

```rust
// In library/core crates: thiserror for structured errors
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("Failed to connect to server at {url}")]
    ConnectionFailed { url: String, #[source] source: reqwest::Error },
    #[error("Checksum mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch { path: PathBuf, expected: String, actual: String },
    #[error("Authentication failed: {reason}")]
    AuthFailed { reason: String },
}

// In CLI layer: anyhow for convenient propagation with context
fn main() -> ExitCode {
    if let Err(err) = run() {
        eprintln!("Error: {err:#}");  // {:#} shows full error chain
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn run() -> anyhow::Result<()> {
    let config = load_config().context("Failed to load backup configuration")?;
    run_backup(&config).context("Backup operation failed")?;
    Ok(())
}
```

**Design error enums per module**, not one monolithic error type. An `ApiError` for network operations, a `StorageError` for I/O, a `CryptoError` for encryption failures — then compose them with `#[error(transparent)]` and `#[from]` at higher levels. Error variants should be chosen by what the caller finds interesting, not by implementation failure modes.

**Performance note on context**: use `.context("static string")` for static messages and `.with_context(|| format!(...))` for dynamic ones. The `with_context` variant is lazy and only allocates when an error actually occurs.

**Never return `Result` from `main()` directly** — Rust uses `Debug` formatting by default, producing ugly output. Handle errors manually in `main()` or use **miette** for rich diagnostic reporting with error codes, help text, and source spans. The pattern is to derive both `thiserror::Error` and `miette::Diagnostic` on user-facing error types, keeping standard `Error` types in library code.

---

## 4. Async architecture for backup pipelines

**Async is justified for a backup CLI that talks to REST APIs** — concurrent uploads/downloads and network I/O are the sweet spot. But the key principle is that **async should be a small delta atop sync Rust**. Keep domain logic synchronous; use async only for I/O boundaries.

**Runtime setup**: `#[tokio::main]` suffices for most CLI tools. Use manual runtime construction only when you need to configure thread count explicitly or create separate runtimes for CPU-bound work.

**The critical boundary: CPU-bound vs I/O-bound work.** Alice Ryhl's definitive guidance states that async code should never spend more than **10–100 microseconds** without reaching an `.await`. For a backup tool:

- **Use tokio** for: HTTP API calls, concurrent uploads/downloads, network I/O
- **Use rayon** for: file hashing (BLAKE3/SHA-256), compression (zstd), content-defined chunking
- **Use spawn_blocking** for: filesystem operations, synchronous database access
- **Never mix rayon and tokio on the same thread** — the Meilisearch team documented severe deadlocks from this. Communicate between runtimes via channels.

**A backup pipeline maps naturally to bounded channels with backpressure**:

```rust
// Scanner → Hasher → Uploader pipeline
let (scan_tx, scan_rx) = mpsc::channel::<PathBuf>(100);
let (upload_tx, upload_rx) = mpsc::channel::<PreparedFile>(50);

// Stage 1: File scanner (filesystem I/O via spawn_blocking)
// Stage 2: Hasher/compressor (CPU via rayon, bridged with oneshot channels)
// Stage 3: Uploader (async network I/O with JoinSet + Semaphore for bounded concurrency)
```

**Bounded channels are the backpressure mechanism** — `send().await` suspends the sender when the buffer is full, naturally throttling the pipeline. Start with buffer sizes of 32–100 and tune based on profiling.

**Connection pooling** is automatic with reqwest — the critical practice is to **create one `Client` and reuse it**. `Client::clone()` is cheap (just clones an Arc to the same pool). Configure `pool_max_idle_per_host(10)`, `pool_idle_timeout(30s)`, and `tcp_keepalive(60s)`.

**For graceful shutdown**, use `tokio_util::sync::CancellationToken` propagated to all tasks, combined with `tokio::select!` on `ctrl_c()`. The "double Ctrl-C" pattern works well for backup tools: first press triggers graceful shutdown (flush buffers, save progress), second press exits immediately.

---

## 5. CLI design with clap, configuration layering, and progress reporting

**The derive API is the right choice for 95%+ of use cases.** Structure the top-level as a struct (not an enum) so global options can be added without breaking changes:

```rust
#[derive(Parser)]
#[command(name = "vykar", version, about)]
pub struct Cli {
    #[command(flatten)]
    global: GlobalOpts,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args)]
struct GlobalOpts {
    #[arg(long, short, global = true, env = "VYKAR_REPO")]
    repo: Option<String>,
    #[arg(long, short, global = true, action = ArgAction::Count)]
    verbose: u8,
    #[arg(long, global = true, default_value = "text")]
    output: OutputFormat,
}

#[derive(Subcommand)]
enum Commands {
    Backup(BackupArgs),
    Restore(RestoreArgs),
    Snapshots(SnapshotArgs),
    Prune(PruneArgs),
    Init(InitArgs),
}
```

**Configuration layering** should follow the priority: defaults < config file < environment variables < CLI arguments. The **figment** crate handles this elegantly with composable providers, though the `config` crate is simpler for common cases. Clap's built-in `env` attribute integrates environment variables directly into argument parsing.

**For logging**, use **tracing** (the modern standard, maintained by the Tokio project) over `log`. Map `-v` flags to filter levels: 0→warn, 1→info, 2→debug, 3→trace. Use `EnvFilter` for per-module filtering (`VYKAR_LOG=vykar_core::chunker=trace`). Write logs to stderr, data to stdout.

**Progress reporting** with **indicatif** is essential for backup tools. Use `MultiProgress` for concurrent operations, `wrap_read`/`wrap_write` for transparent I/O progress, and `ProgressBar::hidden()` when output is not a TTY. Always call `suspend()` before printing log lines to avoid corrupting the progress display.

---

## 6. REST client patterns with typed abstractions

**Structure the HTTP client as a typed abstraction layer** that hides raw reqwest calls behind domain-specific methods. This is the pattern from Luca Palmieri's "Zero to Production":

```rust
pub struct VykarApiClient {
    client: reqwest::Client,
    base_url: String,
    auth_token: String,
}

impl VykarApiClient {
    // Generic request handler with structured error mapping
    async fn request<T: DeserializeOwned>(
        &self, method: Method, path: &str, body: Option<&impl Serialize>,
    ) -> Result<T, ApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.request(method, &url)
            .header("Authorization", format!("Bearer {}", self.auth_token));
        if let Some(b) = body { req = req.json(b); }
        let resp = req.send().await?;
        match resp.status() {
            s if s.is_success() => Ok(resp.json().await?),
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::TOO_MANY_REQUESTS => Err(ApiError::RateLimited { /* ... */ }),
            status => Err(ApiError::Server { status, message: resp.text().await? }),
        }
    }

    pub async fn create_snapshot(&self, req: &CreateSnapshotRequest) -> Result<Snapshot, ApiError> {
        self.request(Method::POST, "/api/v1/snapshots", Some(req)).await
    }
}
```

**For retry logic**, use **reqwest-middleware** with **reqwest-retry** (from TrueLayer) for exponential backoff with jitter. Configure retries only for transient errors (5xx, timeouts, connection failures) — never retry 4xx client errors. Newer reqwest versions (0.13+) include native retry support via tower layers.

**Timeout configuration** should differentiate by operation type: **10s** connect timeout, **30s** for metadata operations, **120s+** for large file uploads/downloads. Use per-request `RequestBuilder::timeout()` to override client defaults for long-running operations.

Use `#[serde(rename_all = "camelCase")]` or `"snake_case"` to match API conventions. Use lifetime parameters (`&'a str`) instead of `String` in request types to avoid unnecessary allocations.

---

## 7. Testing strategy across unit, integration, and property layers

**Unit tests** live in `#[cfg(test)] mod tests` blocks alongside the code they test. Rust uniquely allows testing private functions directly via `use super::*`. For cross-module test utilities, create a dedicated `vykar-testing` crate as a dev-dependency.

**CLI integration tests** use **assert_cmd** with **predicates** for testing the compiled binary:

```rust
#[test]
fn backup_missing_repo_shows_error() {
    Command::cargo_bin("vykar").unwrap()
        .args(&["backup", "/tmp/test"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("repository not configured"));
}
```

**HTTP mocking** with **wiremock** (by Luca Palmieri) is the gold standard — it supports parallel tests with isolated mock servers on random ports, custom matchers, and automatic expectation verification on drop. The critical design pattern: **make base URLs configurable** (via struct fields) to enable mock server injection in tests. Never hardcode URLs.

**Property-based testing** with **proptest** catches edge cases that unit tests miss. For a backup tool, test: serialization/deserialization roundtrips, chunking algorithms (all data preserved, chunk sizes within bounds), path encoding invariants, and configuration parsing. **Snapshot testing** with **insta** works well for verifying CLI output formats and error messages — use `cargo insta review` for interactive approval of changes.

**Test organization** follows the standard Cargo layout: `tests/common/mod.rs` (not `tests/common.rs`) for shared utilities, since files directly in `tests/` are each compiled as separate test crates. Use `tempfile` for all filesystem-based tests — temporary directories auto-delete on drop.

---

## 8. Dependency management and auditing

**Commit `Cargo.lock`** for applications (ensures reproducible builds). Use caret requirements (default) for most dependencies. Run `cargo tree --duplicates` regularly to find dependencies compiled multiple times. Use `cargo-udeps` or `cargo-machete` to detect unused dependencies.

**cargo-deny** (from Embark Studios) is the comprehensive solution for dependency auditing, covering four dimensions: security advisories, license compliance, duplicate detection, and source restrictions. A minimal `deny.toml`:

```toml
[advisories]
vulnerability = "deny"
unmaintained = "warn"

[licenses]
allow = ["MIT", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "ISC"]

[bans]
multiple-versions = "warn"
wildcards = "deny"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

**MSRV policy**: always declare `rust-version` in Cargo.toml and test it in CI with `cargo +1.XX.0 check`. Common policies range from tracking latest stable (ripgrep, rustic) to N-2 (current stable minus two releases). MSRV bumps should be treated as at least a minor version bump.

---

## 9. Performance patterns for backup data pipelines

**BLAKE3 is the recommended hash** for new backup tools: **6.4 GB/s** single-threaded (AVX2), up to **92 GB/s** multithreaded on 16 cores. Its built-in Merkle tree structure enables parallelism at the algorithm level. Use the `rayon` feature for `update_rayon()` and the `mmap` feature for memory-mapped file hashing.

**Content-defined chunking** with **fastcdc** (v2020 algorithm) provides ~10x faster chunking than Rabin-based approaches. Use the streaming API (`StreamCDC`) to avoid loading entire files into memory. Typical chunk sizes for backup: min 512 KiB, average 1–2 MiB, max 8 MiB.

**File I/O must be buffered** — Rust's file I/O is unbuffered by default. Use `BufReader::with_capacity(64 * 1024, file)` for backup workloads; the default 8 KiB buffer is far too small. For zero-copy sharing of chunk data across pipeline stages (hash, compress, encrypt, pack), use `bytes::Bytes` — `split()` and `freeze()` create independent handles without copying data.

**Compression** with **zstd** at level 3 (default) offers the best speed/ratio balance for backup workloads. Level 1 for fast storage, levels 10+ for cold/archival storage. Dictionary training (`zstd::dict::from_samples()`) dramatically improves ratios for many small similar chunks like metadata.

**Buffer pooling** reduces allocation pressure in hot loops. A simple `Mutex<Vec<Vec<u8>>>` pool or `crossbeam::queue::ArrayQueue` for lock-free pooling prevents per-chunk allocation/deallocation overhead.

**Unsafe code** should be minimal and audited: require `// SAFETY:` comments on all unsafe blocks, use `#[deny(unsafe_op_in_unsafe_fn)]`, and run **Miri** in CI. Acceptable uses in a backup tool include FFI bindings (zstd-sys), performance-critical CDC inner loops, and memory-mapped file access.

**Profile early** with `cargo flamegraph` (enable `[profile.release] debug = true`) and use `samply` for interactive profiling. `tokio-console` provides async task introspection for diagnosing runtime bottlenecks.

---

## 10. Code quality enforcement in CI

**Clippy configuration** should enable the `pedantic` group as warnings, then selectively allow overly noisy lints. Define this in `[workspace.lints.clippy]` for consistency across all crates:

```toml
[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
module_name_repetitions = "allow"
must_use_candidate = "allow"
unwrap_used = "deny"
dbg_macro = "deny"
print_stdout = "warn"
```

Never enable the `restriction` group wholesale — cherry-pick individual lints. The `correctness` group (deny by default) should never be weakened.

**CI workflow** should include five parallel jobs: check, format, clippy, test (cross-platform matrix), and dependency audit. Use `dtolnay/rust-toolchain@stable` and `Swatinem/rust-cache@v2` for efficient caching. Include MSRV verification in the test matrix.

**Documentation standards**: enforce `#![deny(missing_docs)]` on public API crates. Every public function needs a summary sentence, `# Examples` with `?` (not `unwrap()`), `# Errors` documenting error conditions, and `# Panics` documenting panic conditions. Use `#![doc = include_str!("../README.md")]` to make README examples into doc tests.

**Common anti-patterns** to watch for in review: excessive `.unwrap()`/`.expect()` outside tests, monolithic `main.rs`, `String` where `&str` or `Path` suffices, excessive `.clone()` without profiling justification, mixing I/O and pure logic in the same functions (kills testability), and unbounded channels in production pipelines.

---

## 11. State management without god objects

**Avoid global mutable state entirely.** Pass a context struct through the application instead:

```rust
struct AppContext {
    config: AppConfig,
    repo: Repository<Open>,
    client: VykarApiClient,
    progress: ProgressReporter,
}
```

**The typestate pattern** enforces valid state transitions at compile time with zero runtime cost. Rustic demonstrates this with `Repository::new(&opts, &backends)?.open()?` — you cannot call backup operations on an unopened repository. Implement this with zero-sized type parameters and `PhantomData`:

```rust
struct Repository<State> { /* ... */ _state: PhantomData<State> }
impl Repository<Unopened> { fn open(self) -> Result<Repository<Open>> { /* ... */ } }
impl Repository<Open> { fn backup(&self) -> Result<Snapshot> { /* ... */ } }
```

**Dependency injection** in Rust uses trait objects or generics — no framework needed. Define a `StorageBackend` trait, implement it for `LocalBackend`, `S3Backend`, `RestBackend`. Use generics (`Repository<B: StorageBackend>`) for monomorphized performance or `Box<dyn StorageBackend>` for runtime flexibility.

**Decompose state** into focused, single-responsibility components: `ChunkIndex`, `PackManager`, `SnapshotManager`, `CryptoProvider`, `CacheManager`. This mirrors both restic's and borg's internal architectures. Use **`OnceLock`** (stable since Rust 1.70) for thread-safe lazy initialization of expensive resources like crypto contexts and repository indices.

**Configuration loading** should use serde + TOML with the builder pattern for programmatic construction. Validate configuration after deserialization with a dedicated `.validate()` method — serde handles structural correctness, but semantic validation (e.g., chunk size ranges, valid repository paths) requires application logic.

---

## Conclusion: what matters most

Three decisions dominate the long-term maintainability of a large Rust backup tool. **First, the workspace structure**: a flat `crates/` layout with thin binaries and a shared types crate prevents the architectural decay that makes refactoring painful. **Second, the async/sync boundary**: keeping CPU-intensive backup operations (chunking, hashing, compression) in rayon while using tokio only for network I/O avoids the complexity explosion that comes from making everything async. **Third, error handling discipline**: thiserror in libraries with anyhow at the CLI boundary gives both structured error handling for programmatic consumers and rich error reporting for users.

The patterns from rustic-rs are particularly instructive as the closest real-world analog — its library/CLI split, typestate repository lifecycle, TOML configuration profiles, and lock-free operations represent hard-won architectural wisdom from reimplementing restic's concepts in Rust. Where Vykar synthesizes ideas from multiple backup tools, these structural patterns provide the scaffolding to keep that synthesis coherent as the codebase grows.
