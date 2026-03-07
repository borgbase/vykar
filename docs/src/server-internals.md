# Server Internals

Technical reference for `vykar-server`: crate layout, REST API surface, authentication, policy enforcement, and server-side maintenance helpers.

*For deployment and configuration, see [Setup](server-setup.md).*

---

## Crate Layout

| Component | Location | Purpose |
|-----------|----------|---------|
| **vykar-server** | `crates/vykar-server/` | axum HTTP server and admin operations |
| **vykar-protocol** | `crates/vykar-protocol/` | Shared wire-format types, pack format constants, and transport validation (no I/O or crypto) |
| **RestBackend** | `crates/vykar-storage/src/rest_backend.rs` | `StorageBackend` implementation over HTTP |

## REST API

The server exposes normal storage-object routes plus a small set of admin query endpoints. Repository state still lives as ordinary keys under the configured `data_dir`.

### Storage object routes

| Method | Path | Maps to | Notes |
|--------|------|---------|-------|
| `GET` | `/{*path}` | `get(key)` | Returns `200` + body or `404`. With a `Range` header, this becomes a ranged read and returns `206`. |
| `HEAD` | `/{*path}` | `exists(key)` | Returns `200` with metadata or `404`. |
| `PUT` | `/{*path}` | `put(key, data)` | Raw bytes body. REST clients send `X-Content-BLAKE2b`; the server verifies it while streaming the write. |
| `DELETE` | `/{*path}` | `delete(key)` | Returns `204` or `404`. Rejected with `403` in append-only mode. |
| `GET` | `/{*path}?list` | `list(prefix)` | Returns a JSON array of matching keys. |
| `POST` | `/{*path}?mkdir` | `create_dir(key)` | Creates directory scaffolding. |

### Admin routes

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/?init` | Create repo directory scaffolding (`keys`, `snapshots`, `locks`, `packs/00..ff`) |
| `POST` | `/?batch-delete` | Delete a JSON list of keys |
| `POST` | `/?batch-delete&cleanup-dirs` | Delete keys and try to remove now-empty parent directories |
| `POST` | `/?repack` | Server-side pack repack using a client-supplied plan |
| `POST` | `/?verify-packs` | Server-side pack verification using a client-supplied plan |
| `GET` | `/?stats` | Repository size, object count, pack count, `last_backup_at`, and quota info |
| `GET` | `/?verify-structure` | Structural repository validation |
| `GET` | `/?list` | List all keys in the repository |
| `GET` | `/health` | Unauthenticated liveness endpoint returning `status` and `version` |

There are no dedicated `/locks` endpoints. Clients store lock and session objects through the normal object API (`locks/*`, `sessions/*`).

## Authentication

All routes except `GET /health` require `Authorization: Bearer <token>`. The token comes from the `VYKAR_TOKEN` environment variable and is checked with a constant-time comparison.

## Append-Only Enforcement

When `append_only = true`:

- `DELETE` on any object path returns `403 Forbidden`
- `PUT` to an existing key returns `403` **unless** the key is on the mutable-allowlist
- Mutable-allowlist: `index`, `index.gen`, `locks/*`, `sessions/*` — these may be overwritten freely
- All other keys (`config`, `keys/*`, `snapshots/*`, `packs/*`) are immutable once written
- `/?batch-delete` is rejected
- `/?repack` operations that delete old packs are rejected

This protects existing history from a compromised client while still allowing normal backup commits. In particular, snapshot blobs under `snapshots/` are immutable — a compromised client cannot hide historical backups by overwriting or deleting them.

## Quota Enforcement

Quota is enforced on writes. If `--quota` is omitted, the server auto-detects a limit from filesystem quota information or free space. If a write would exceed the active limit, the request is rejected before or during upload.

The stats response includes:

```json
{
  "total_bytes": 1073741824,
  "total_objects": 234,
  "total_packs": 42,
  "last_backup_at": "2026-02-11T14:30:00Z",
  "quota_bytes": 5368709120,
  "quota_used_bytes": 1073741824,
  "quota_source": "Explicit"
}
```

## Backup Freshness Monitoring

The server updates `last_backup_at` when it observes a new `snapshots/*` key being written for the first time. This marks the completion of a backup commit.

## Server-Side Verify Packs

`vykar check` can offload pack verification to the server when the backend is REST and the server supports `/?verify-packs`.

The client sends a verification plan describing packs and expected blob boundaries. The server validates:

- pack header magic and version
- blob boundaries and length-prefix structure
- BLAKE2b hash of pack contents

If the user passes `vykar check --distrust-server`, the client falls back to downloading and verifying data locally.

## Server-Side Repack

`vykar compact` can use `/?repack` to rewrite packs server-side without downloading encrypted blobs to the client.

High-level flow:

1. The client opens the repo and analyzes pack liveness from the index.
2. The client sends a repack plan describing source packs and live blob offsets.
3. The server copies the referenced encrypted blobs into new pack files, preserving the pack wire format.
4. The server returns new pack keys and offsets so the client can update the chunk index.

This is encrypted passthrough: the server never decrypts chunk payloads.

## Structure Checks

`GET /?verify-structure` validates repository shape without needing encryption keys. It checks:

- required directories and expected key layout
- pack shard naming and pack header magic/version
- malformed or obviously invalid pack files

This complements client-side `vykar check`, which still owns full cryptographic verification.

## RestBackend

`crates/vykar-storage/src/rest_backend.rs` implements `StorageBackend` with `ureq`. In addition to the trait surface, it exposes helper methods used by client commands:

- `batch_delete()`
- `stats()`
- `verify_packs()`
- `repack()`

It also sends `X-Content-BLAKE2b` on `PUT` requests and validates `Content-Range` on ranged reads.

Client config:

```yaml
repositories:
  - label: server
    url: https://backup.example.com
    access_token: "secret-token-here"
```

---

*Related: [Setup](server-setup.md), [Architecture](architecture.md)*
