---
name: security-review
description: "Security review skill for the vykar backup tool (Rust). Use when reviewing code for security issues, auditing cryptographic implementations, checking for credential leaks, reviewing unsafe code, or validating the security model of the backup tool. Covers encryption, key management, repository integrity, hook execution, config parsing, dependency auditing, and backup-specific attack surfaces."
---

# vykar Security Review

A skill for reviewing the security of the vykar backup tool — a Rust-based deduplicated, encrypted backup tool inspired by BorgBackup, Restic, and Kopia.

## Context

vykar is a backup tool that handles extremely sensitive data: users' files, database dumps, credentials, and encryption keys. A security flaw here is catastrophic — it can mean silent data loss, leaked secrets, or ransomware destroying all backups. The bar for security is higher than a typical application.

This skill should be used to review any code change, new feature, or periodic audit of the vykar codebase. It draws on real-world vulnerabilities and security models from BorgBackup, Restic, Kopia, and Rustic.

## How to Use This Skill

When asked to review vykar code for security, follow this process:

1. **Identify the scope** — Is this a full audit, a PR review, or a focused review of a specific subsystem (crypto, hooks, config, networking)?
2. **Walk through the checklist** below for the relevant areas
3. **Produce findings** as a prioritized list: Critical > High > Medium > Low > Informational
4. **Suggest fixes** with specific code-level guidance, not just descriptions of problems

---

## Review Checklist

### 1. Cryptography and Encryption

This is the most critical area. Backup tools live or die by their encryption correctness.

**What to check:**

- **Use AEAD ciphers only.** vykar should use AES-256-GCM or ChaCha20-Poly1305. Flag any use of AES-CTR, AES-CBC, or other non-authenticated modes. Borg 1.x used AES-CTR + HMAC (Encrypt-then-MAC) which was secure but fragile; Borg 2.0 moved to AEAD for good reason.
- **Never roll custom crypto.** All cryptographic operations should use well-audited crates: `aes-gcm`, `chacha20poly1305`, `ring`, or `rustcrypto` family. Flag any manual XOR, custom MAC construction, or hand-rolled key derivation.
- **Use a CSPRNG for secrets.** Keys, nonces, salts, and tokens must come from `OsRng`/`getrandom` (or equivalent CSPRNG). Flag `SmallRng`, `rand_pcg`, `rand_xoshiro`, or other non-cryptographic RNGs in security-sensitive paths.
- **Nonce/IV management.** Every encryption operation needs a unique nonce. Check for:
  - Random nonces from `OsRng` or `getrandom` (preferred for simplicity)
  - Counter-based nonces with proper persistence (Borg's approach — complex, has had bugs)
  - **Never** reusing a nonce with the same key. This is catastrophic for AES-GCM.
  - If using random nonces with AES-256-GCM, verify the nonce is 96 bits and that the number of encryptions under one key stays well below 2^32 (birthday bound).
- **Key derivation from passphrase.** Must use Argon2id (preferred) or scrypt. Flag any use of PBKDF2, bcrypt, or raw SHA-based derivation. Check that:
  - Salt is random and at least 128 bits
  - Argon2id parameters are reasonable (memory ≥ 64 MiB, iterations ≥ 3, parallelism ≥ 1)
  - Parameters are stored with the key so they can be upgraded later
- **Master key architecture.** The passphrase should derive a Key Encryption Key (KEK) that wraps a randomly-generated master key. Never derive the encryption key directly from the passphrase — this allows passphrase changes without re-encrypting the entire repo.
- **Session keys.** Borg 2.0 derives per-session keys from the master key + a session ID. This limits the impact of nonce reuse. Consider whether vykar does something similar.
- **Chunk ID derivation.** Chunk IDs must be derived using a keyed MAC (HMAC-SHA256 or keyed BLAKE2b), not a plain hash. Plain hashes allow an attacker with repo access to confirm whether specific files exist in the backup (content fingerprinting attack).
- **Compression + encryption interaction.** Compressing before encrypting can leak information about plaintext through ciphertext size (CRIME/BREACH-style attacks). For a backup tool with offline storage this is low risk, but flag it if compression ratios are observable to an attacker (e.g., exposed via a web API or monitoring metrics).
- **Encrypt-then-MAC ordering.** If not using AEAD, the only acceptable construction is Encrypt-then-MAC. MAC-then-Encrypt and Encrypt-and-MAC are vulnerable to padding oracles. But prefer AEAD which handles this automatically.
- **Associated Authenticated Data (AAD).** When using AEAD, bind the chunk/object ID to the ciphertext via AAD. This prevents an attacker from moving encrypted data between object IDs. Borg 2.0 does this; Restic does not.
- **Constant-time secret checks.** Any comparison of MACs, tokens, or authentication secrets should use constant-time comparisons (for example, `subtle::ConstantTimeEq`), not `==`.
- **Avoid deprecated/insecure crypto dependencies.** Flag deprecated crypto crates, weak modes (ECB), and legacy constructions kept for compatibility without clear migration plans.

**Real-world lessons:**
- Borg CVE-2023-36811: A flaw in the authentication scheme allowed an attacker to fake archives and cause data loss. The manifest authentication was tied to the data format for backward compatibility, creating a gap.
- Restic's Poly1305-AES masking: Filippo Valsorda's audit found the Poly1305 key masking was "pointless, dangerous, and took 45+ minutes to audit" — an example of unnecessary complexity in crypto code.
- Borg's AES-CTR nonce management: Required a persistent nonce counter with reservation system to prevent reuse across crashes. Session-key-based approaches (Borg 2.0) eliminate this class of bugs.

### 2. Key Management and Secrets Handling

**What to check:**

- **Passphrase in memory.** After deriving the KEK, the passphrase should be zeroized from memory immediately. Use the `zeroize` crate with `Zeroizing<>` wrapper. Check that passphrases are not stored in `String` (which may be copied by the allocator) — use `SecretString` or `Zeroizing<Vec<u8>>`.
- **Key material in memory.** All key material (master key, session keys, KEK) should use `Zeroizing<>`. Verify keys are zeroized on drop, not just when the program exits cleanly.
- **No keys in logs or error messages.** Grep for any logging of key material, passphrases, or encryption parameters that include secrets. Check `Debug` trait implementations on key-containing structs — they should redact sensitive fields.
- **Config file permissions.** If the config file contains `passphrase:` in plaintext, vykar should warn the user and recommend `passcommand` instead. Check that vykar sets restrictive permissions (0600) on any file it creates that contains secrets.
- **passcommand execution.** The `passcommand` is a shell command. Check:
  - It's executed via the user's shell, not via direct exec (to support pipes, variable expansion)
  - stdout is captured, stderr is passed through to the user
  - The output is trimmed of trailing newlines
  - The command is not logged
  - Environment variables like `VYKAR_PASSPHRASE` are cleared from the environment after use
- **Key file storage.** If vykar stores the encrypted key in the repo (like Borg's repokey mode), verify:
  - The key is encrypted with the KEK before storage
  - The encrypted key includes a MAC/authentication tag
  - The salt is stored alongside the encrypted key
  - Multiple keys can exist (for key rotation)
- **Repository swapping attack.** An attacker who controls the storage could swap the entire repository (including key material) with one they control. Borg mitigates this by aborting if BORG_PASSPHRASE doesn't match. Check that vykar detects repository identity changes.

### 3. Repository Integrity and Data Verification

**What to check:**

- **Content-addressable verification.** When reading a chunk, verify that its hash matches its ID before decryption. This catches both corruption and tampering.
- **Authentication before decryption.** The MAC/auth tag must be verified before any decryption occurs. Never decrypt unauthenticated data — this prevents chosen-ciphertext attacks and avoids processing attacker-controlled plaintext.
- **Index/manifest integrity.** The snapshot index and manifest must be authenticated. An attacker who can modify the index can:
  - Point snapshots at wrong data (data substitution)
  - Remove snapshots (makes `forget` delete the only copies)
  - Add fake snapshots (to trick retention policies)
- **Lock file security.** If using file-based locks:
  - Lock files on cloud storage are inherently unreliable (no atomic operations)
  - Stale locks must be detectable and breakable (include PID, hostname, timestamp)
  - Lock-free designs (like Duplicacy/Rustic) are preferred
- **Repair safety.** Repository repair operations (`vykar check --repair`) must never delete data that might be needed. Flag any repair logic that removes chunks without first verifying they're unreferenced by ALL snapshots.
- **Time-of-check-to-time-of-use (TOCTOU).** Check for gaps between verifying a file's metadata and reading its content during backup. Filesystem snapshots (ZFS/Btrfs/LVM) are the proper fix, but if not available, vykar should at minimum detect and warn about files that changed during backup.
- **Arithmetic on untrusted sizes/lengths.** Metadata-driven sizes, offsets, and capacities must use `checked_*`/`try_into()` style conversions. In release builds, integer overflow wraps silently and can invalidate bounds checks.

### 4. Hook Execution Security

Hooks (`before_backup`, `after`, `failed`, etc.) execute arbitrary shell commands. This is a significant attack surface.

**What to check:**

- **Variable injection in hooks.** Hook templates use variables like `{error}`, `{label}`, `{repository}`. If these are interpolated directly into shell commands, an attacker who controls the error message or label name can inject shell commands. Example: a malicious repository could return an error containing `$(rm -rf /)`. Variables must be shell-escaped or passed via environment variables only (not interpolated into the command string).
- **Shell command construction.** Flag any hook implementation that shells out through `sh -c`/`bash -c`/`cmd /c` with untrusted input. Prefer direct argument passing where possible, or strict escaping plus allowlists for dynamic values.
- **Hook execution environment.** Check that hooks:
  - Run with the same privileges as vykar (not elevated)
  - Have a clean, minimal environment (don't leak `VYKAR_PASSPHRASE` or key material)
  - Have a timeout to prevent hangs
  - Have their exit codes checked (`before` hooks should abort on failure)
- **Config file hook injection.** If the config is writable by other users, they can inject hooks. The config file should be owned by the user running vykar, with permissions 0600 or 0644.
- **Per-repo hooks in untrusted repos.** If hooks can be stored in the repository itself (like `.vykar-hooks`), this is a remote code execution vector. vykar should NOT load hooks from repositories — only from the local config file. Kopia requires explicit `--enable-actions` for this reason.

### 5. Configuration Parsing and Input Validation

**What to check:**

- **YAML parsing safety.** YAML has dangerous features (arbitrary code execution in some parsers, billion laughs DoS via entity expansion). Verify vykar uses `serde_yaml` or a safe subset parser, not a parser that supports tags or arbitrary constructors.
- **Parser resource limits.** Untrusted config and remote metadata parsing should enforce size/depth limits to prevent recursion bombs, deep nesting stack overflows, and memory exhaustion.
- **Deserialization hardening.** Use dedicated input DTOs and `#[serde(deny_unknown_fields)]` where practical. Avoid deserializing untrusted payloads directly into internal privileged structs.
- **Path traversal.** Source directories, exclude patterns, and restore targets must be validated:
  - No `../` escaping the intended directory
  - Symlinks should be handled explicitly (follow or don't, but document and enforce)
  - Restore should never overwrite files outside the target directory
- **Integer overflow in chunker parameters.** `min_size`, `avg_size`, `max_size` are user-configurable. Verify:
  - They're bounds-checked (min > 0, min ≤ avg ≤ max, max within reasonable bounds)
  - No integer overflow when calculating chunk boundaries (use `checked_*` arithmetic on user-influenced values)
  - No panic in release mode from arithmetic operations on these values
  - Narrowing casts (`as i32`, `as u32`, `as c_int`) are replaced with `try_into()` plus explicit bounds checks
- **Repository path validation.** S3 bucket names, SFTP paths, and REST URLs must be validated. A malicious config could point to unexpected locations.
- **Label/tag validation.** Labels used in CLI commands and config should be restricted to safe characters (alphanumeric, hyphens, underscores). No shell metacharacters, no path separators.
- **Retention policy validation.** Ensure `keep_*` values can't be negative or cause underflow. A retention policy of `keep_last: 0` combined with `forget` could delete all snapshots.

### 6. Unsafe Code Review

**What to check:**

- **Minimize `unsafe` blocks.** Every `unsafe` block should have a `// SAFETY:` comment explaining why it's safe. Flag any `unsafe` without justification.
- **FFI boundaries.** If vykar calls into C libraries (libzstd, liblz4, OpenSSL), check:
  - Buffer sizes are validated before passing to C functions
  - Return values are checked for errors
  - Pointers are valid for the expected lifetime
  - No use-after-free across FFI boundary
- **Raw-parts constructors.** Treat `Vec::from_raw_parts` / `slice::from_raw_parts` as high-risk. Verify pointer provenance, allocation ownership, length/capacity correctness, and lifetimes.
- **Unsafe trait impls.** `unsafe impl Send/Sync` must include correct trait bounds on generic types. Missing bounds can create data races reachable from safe code.
- **Transmute and raw pointer use.** Flag any `std::mem::transmute`, `ptr::read`, `ptr::write`, or raw pointer dereference. These are rarely needed in a backup tool.
- **`unsafe` in dependencies.** Run `cargo-geiger` to assess unsafe usage in the dependency tree. Pay special attention to crypto and compression crates.

### 7. Dependency Security

**What to check:**

- **`cargo audit`** — Run and flag any known vulnerabilities. The lz4-sys crate had a memory corruption vulnerability (RUSTSEC-2022-0051); these need to be caught.
- **`cargo deny`** — Check for:
  - License compatibility (all deps should be compatible with BSD-3-Clause)
  - Duplicate versions of the same crate (can indicate supply chain issues)
  - Banned crates (e.g., `openssl` if `ring` or RustCrypto is preferred)
- **Dependency minimization.** A backup tool should have a small dependency tree. Flag unnecessary dependencies that increase attack surface.
- **Pinned versions.** `Cargo.lock` should be committed for the binary. Verify critical security dependencies (crypto, compression) are pinned to specific versions.
- **Build-time dependencies.** Proc macros and build scripts execute arbitrary code at compile time. Review any unfamiliar build dependencies.
- **Typosquatting and lookalike crates.** Verify crate names carefully (`serde` vs lookalikes, etc.) and scrutinize recently added dependencies with low reputation or suspicious naming.
- **Build script/proc-macro trust boundary.** Treat `build.rs` and proc macros as arbitrary code execution during build. Audit new ones explicitly and prefer well-known, maintained crates.
- **Policy tooling.** Consider `cargo vet` or `cargo crev` for dependency trust policy, especially for security-critical releases.

### 8. Network Security (S3, SFTP, REST backends)

**What to check:**

- **TLS certificate validation.** Must be enabled by default. Any option to disable TLS verification should require an explicit flag and print a prominent warning. Never skip certificate validation silently.
- **Danger flags in HTTP clients.** Explicitly flag `danger_accept_invalid_certs(true)` and similar bypasses in `reqwest`/HTTP stacks; these should never be enabled in production paths.
- **Credential handling for cloud backends.**
  - AWS credentials should come from the standard credential chain (env vars, instance profile, config file), never from the vykar config
  - SFTP keys should be handled by the system SSH agent, not stored in vykar's config
  - REST API tokens should use environment variables or a credential helper
- **Request signing.** S3 requests must use SigV4 signing. Check for any fallback to unsigned requests.
- **Bandwidth limiting.** If vykar supports `--limit-upload` / `--limit-download`, verify the rate limiting doesn't have timing side channels that leak information about data content.
- **HTTP parser/protocol hardening.** Review handling of `Content-Length` / `Transfer-Encoding` combinations and malformed headers to avoid request smuggling classes seen in `hyper`-ecosystem CVEs.
- **DoS controls.** Require connection timeouts, request/body size limits, and concurrency caps to reduce slowloris and frame-flooding style attacks.
- **Error message information leakage.** Cloud backend errors should not expose internal paths, credentials, or bucket structures in user-facing error messages or logs.

### 9. Backup-Specific Attack Surfaces

These are unique to backup tools and not covered by generic security checklists.

**What to check:**

- **Append-only mode bypass.** If vykar supports append-only repositories (for ransomware protection), verify that:
  - `forget` and `prune` are genuinely blocked, not just at the CLI level
  - The server/storage enforces the policy, not just the client
  - An attacker can't trick `forget` into deleting valid snapshots by adding fake ones
- **Snapshot poisoning.** Can an attacker who gains temporary write access to the repo insert a snapshot that, when later processed by `forget`, causes legitimate snapshots to be deleted? Restic issue #5041 discusses this in detail.
- **Chunk size fingerprinting.** Repository chunk sizes are visible even with encryption. An attacker with repo access can potentially determine if specific known files are in the backup. Mitigations: keyed chunker (Borg's approach), size obfuscation, or configurable chunker parameters with a per-repo secret seed.
- **Metadata leakage.** Even with encryption, the following may be visible:
  - Number of chunks (reveals approximate data size)
  - Chunk modification timestamps (reveals backup schedule)
  - Snapshot count and sizes (reveals data change patterns)
  - File/directory structure if tree metadata isn't encrypted
- **Restore to arbitrary paths.** The restore command must not allow restoring files outside the target directory. Malicious archives could contain entries with `../../etc/crontab` paths. Always resolve and validate the final path.
- **Symlink attacks during backup and restore.** During backup: a symlink could point outside the intended backup scope. During restore: a malicious archive could contain a symlink that, when followed by a subsequent file restore, writes outside the target directory. Restore symlinks first, then validate all target paths.

### 10. Concurrency and Race Conditions

**What to check:**

- **Parallel backup safety.** If multiple vykar instances back up to the same repo concurrently, verify:
  - No data corruption from concurrent writes
  - Index updates are atomic or use a conflict-resolution strategy
  - No chunk is partially written and then referenced by another process
- **Signal handling.** Check that SIGINT/SIGTERM during backup:
  - Leaves the repository in a consistent state
  - Doesn't leave orphaned temporary files
  - Doesn't corrupt the index
  - Properly releases any locks
- **Temporary file security.** Any temp files should be created with restricted permissions (0600) in a secure directory. Use `tempfile` crate with appropriate settings. Temp files containing sensitive data should be zeroized before deletion.
- **Bounded async queues.** Flag `tokio::sync::mpsc::unbounded_channel()` in network-facing paths unless there is strict upstream backpressure.
- **`tokio::select!` cancellation safety.** Losing branches are dropped; verify partial state is not lost or left inconsistent when one branch wins.
- **Panic behavior in async locks.** `tokio::sync::Mutex` does not poison on panic. Verify state validation/recovery when tasks panic while holding locks.

---

## Output Format

Present findings as a prioritized list:

```
## Security Review: [component/PR name]

### Critical
- **[CRIT-1] Nonce reuse in chunk encryption** — `src/crypto/encrypt.rs:42`
  The nonce counter is not persisted across process restarts, allowing nonce
  reuse after a crash. With AES-256-GCM, this compromises both confidentiality
  and authenticity of all data encrypted with the reused nonce.
  **Fix:** Use random nonces (96-bit from OsRng) instead of counters, or
  persist the counter with atomic file writes before encrypting.

### High
- ...

### Medium
- ...

### Low
- ...

### Informational
- ...

### Positive Findings
- [List things that are done well — this helps maintain good practices]
```

---

## Automated Checks to Run

Before or during the review, run these commands and include the results:

```bash
# Dependency vulnerabilities
cargo audit

# License and dependency policy
cargo deny check

# Unsafe code usage
cargo geiger --all-features

# Clippy with security-relevant lints
cargo clippy --workspace --all-targets -- \
  -W clippy::unwrap_used -W clippy::expect_used \
  -W clippy::panic -W clippy::todo -W clippy::unimplemented \
  -W clippy::dbg_macro -W clippy::print_stderr \
  -W clippy::cast_possible_truncation -W clippy::cast_sign_loss

# Optional: UB checks for unsafe-heavy changes (requires nightly toolchain)
cargo +nightly miri test

# Optional: fuzz parsers/protocol handlers
cargo fuzz run <target_name>

# Search for secrets in code
rg -n "passphrase|password|secret|private_key|PRIVATE" crates/ -g "*.rs"

# Search for unsafe blocks and unsafe Send/Sync impls
rg -n "\\bunsafe\\b|unsafe impl\\s+(Send|Sync)" crates/ -g "*.rs"

# Search for unwrap/expect that could panic on attacker input
rg -n "\\.unwrap\\(|\\.expect\\(" crates/ -g "*.rs"

# Search for dangerous network/runtime patterns
rg -n "danger_accept_invalid_certs\\(true\\)|unbounded_channel\\(|Command::new\\(\"(sh|bash|cmd)\"\\)" crates/ -g "*.rs"
```

---

## Periodic Review Schedule

- **Every PR**: Run the automated checks and review changes against the relevant checklist sections
- **Monthly**: Full dependency audit (`cargo audit` + `cargo deny`)
- **Quarterly**: Full security review against all checklist sections
- **On any crypto change**: Mandatory review of sections 1 and 2 by two reviewers
- **Before each release**: Full checklist review + automated checks
