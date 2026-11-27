---
title: "Server Internals"
---

Technical reference for `vykar-server`: crate layout, REST API surface, authentication, policy enforcement, and server-side maintenance helpers.

*For deployment and configuration, see [Setup](/server-setup).*

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
| `GET` | `/{*path}` | `get(key)` | Returns `200` + body or `404`. With a `Range` header, this becomes a ranged read and returns `206`. `GET /index` honours `If-None-Match: "<BLAKE3 hex>"`: it answers `304` with `ETag` when the value names the digest verified on the last `PUT /index` (`X-Content-BLAKE3`; a BLAKE2b-verified PUT leaves no record) and the file that PUT produced is still in place. The record is held in server memory, so the first conditional GET after a restart is a `200`. Other keys ignore the header, and so does a `Range` read. An `index` corrupted in place (same inode, size and mtime) still gets a `304`, so the client proceeds on its own good copy where a plain GET would fail to decode; `check` always reads `index` unconditionally. |
| `HEAD` | `/{*path}` | `exists(key)` | Returns `200` with metadata or `404`. |
| `PUT` | `/{*path}` | `put(key, data)` | Raw bytes body. REST clients send exactly one of `X-Content-BLAKE3` (format-v3 packs, and non-pack objects to a server advertising `blake3`) or `X-Content-BLAKE2b` (format-v2 packs, and non-pack objects to a pre-0.20 server); the server verifies it while streaming the write. Both headers at once is a 400, and a pack upload with neither is a 400. `If-None-Match: *` makes it a create-only PUT (put-if-absent): `201` when it created the key, `412` when the key exists, decided atomically by a no-clobber rename, so of concurrent creators exactly one gets `201`. The existence check comes before the append-only check, so an existing immutable key answers `412`, not `403`; it also comes before the body is read, so for a body near the size cap the client may see the connection reset instead of the `412` and fail after its retries. Any other `If-None-Match` value is a `400`, and part uploads and completions refuse the header with `400`. `501` when the data directory's filesystem cannot rename without replacing. A plain PUT still replaces an object created this way, except under `index.d/` and `index.h/`. In every mode, a PUT of `index.d/<id>` is refused with `409` unless `<id>` is the BLAKE3 of the body (whatever digest header it carries), and a PUT onto an existing `index.d/` object stores nothing and answers `204` (its mtime is not renewed). `index.h/<gen>` is write-once in every mode: a plain PUT onto an existing head answers `204` without a write when the bytes are identical, `409` otherwise. Both are published with a no-clobber rename; on a filesystem without one, a plain PUT of these keys falls back to an existence check and a replacing rename. |
| `DELETE` | `/{*path}` | `delete(key)` | Returns `204` or `404`. Rejected with `403` in append-only mode unless the key class allows it (see [Append-Only Enforcement](#append-only-enforcement)). |
| `GET` | `/{*path}?list` | `list(prefix)` | Returns a JSON array of matching keys. `&sized` returns `{"key","size"}` objects instead (`list_sized`), and `&sized&mtime` adds `mtime_ms`, the last modification in ms since the Unix epoch by the server's clock (`list_meta`). Servers that predate a flag ignore it, which a client reads as "not reported". |
| `POST` | `/{*path}?mkdir` | `create_dir(key)` | Creates directory scaffolding. |
| `POST` | `/{*path}?sizes` | `object_sizes(keys)` | Body: a JSON array of up to 100,000 full keys, all below `{path}/`. Returns a JSON array of the same length with each object's size in bytes, or `null` when it is absent. A backup commit uses it to verify all its packs (`POST /packs?sizes`) in one request; servers that predate it answer `400`, and the client falls back to per-shard listings or `HEAD`s. |
| `PUT` | `/packs/<xx>/<id>?upload=<token>&offset=<n>&total=<size>` | `put(key, data)`, one part | One part of a pack upload; see [Pack uploads in parts](#pack-uploads-in-parts). Returns `204`. |
| `POST` | `/packs/<xx>/<id>?complete&upload=<token>` | `put(key, data)`, completion | Verifies the assembled pack against the digest header, makes it durable and publishes it. Returns `201`/`204`. |

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
| `GET` | `/health` | Unauthenticated liveness endpoint returning `status`, `version` and the capabilities below |

There are no dedicated endpoints for sessions. Clients store session markers through the normal object API (`sessions/*`). `locks/*` is written only by vykar 0.24 clients and by the legacy lock key of a v2/v3 upgrade, and emptied by `vykar clear-sessions`.

## Authentication

All routes except `GET /health` require `Authorization: Bearer <token>`. The token comes from the `VYKAR_TOKEN` environment variable and is checked with a constant-time comparison.

## Append-Only Enforcement

When `append_only = true`, `PUT` of an existing key, `DELETE` and `/?batch-delete` consult one key-class table (`vykar_protocol::key_class`). Creating a key is always allowed, except under a temp-file name, which is never a valid key. A refused request returns `403 Forbidden`.

| Class | Keys | Overwrite | Delete |
|-------|------|-----------|--------|
| Write-once | `config`, `keys/*`, `snapshots/*`, `packs/<xx>/<id>`, `index.h/<gen>`, `packmaps/<id>`, legacy `pending_index/*` | no | no |
| Content-addressed | `index.d/<id>` | same bytes only (no write) | no |
| Legacy mutable | `index`, `index.gen`, `manifest` | yes | no |
| Ephemeral | `locks/*`, `sessions/*` | yes | yes |
| Temp | leftover temporary upload files (`.tmp.*`, `.repack_tmp.*`) | — | yes |

Keys are classified after trimming leading and trailing slashes, as path resolution does. Directories and keys outside the schema have no class and are refused.

Independently of append-only, `index.h/<gen>` is write-once and `index.d/<id>` is checked against its BLAKE3 name in every mode (see the `PUT` row above). A PUT onto an existing head with other bytes is therefore a `409`, not a `403`, in append-only mode too.

- `/?batch-delete` is refused as a whole, with nothing deleted, if any key's class refuses deletion
- A part upload of an existing pack is refused with `403` at its first part; a completion that finds the pack already present succeeds only if the stored object carries the expected digest (nothing is overwritten), and fsyncs the directory before answering
- `/?repack` operations that delete old packs are rejected
- A backup that seals a pack the server already holds (the retry of an upload whose reply was lost) gets `403` for it; when the server advertises `append_only` and the stored object has the pack's size, the client uses the stored copy and pulls its session's start back by the object's age (architecture, "Crash Recovery")

Old index heads, index objects and pack maps therefore accumulate on an append-only server until a maintenance window removes them.

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

## Pack Uploads in Parts

Over a long-RTT link one TCP stream is capped by its window (about 100 MiB/s at 40 ms), and a whole-object `PUT` keeps one pack on one stream. Servers that advertise `"pack_parts": true` in `/health` also accept a pack in parts, so the client can send one pack over several connections:

1. `PUT /packs/<xx>/<id>?upload=<token>&offset=<n>&total=<size>` — one per part, in any order and concurrently. `token` is 32 lowercase hex characters the client picks per upload; `Content-Length` is required and `offset + length` must not exceed `total`. Each part is a positioned write into the temp file `packs/<xx>/.tmp.<id>.<token>`, so resending a part is harmless and parts are retried like any other request. The first part reserves quota for the whole `total`; the reservation is held across requests until completion.
2. `POST /packs/<xx>/<id>?complete&upload=<token>` with `X-Content-BLAKE3` or `X-Content-BLAKE2b`. The server hashes the assembled file (from the page cache, off the async runtime), checks its size against `total` and its digest, fdatasyncs it, renames it to its final name and fsyncs the directory **before** answering — the same durability contract as a whole-object `PUT`. A size mismatch is `400`, a digest mismatch `409`; both discard the upload. A completion that finds no upload but a stored object with the expected digest fsyncs the directory and answers `204`, so retrying after a lost response succeeds without acking a rename that is not yet durable (the first completion may have failed or not finished its directory fsync). An unknown upload is `404`; a completion while a part request is still being written is `503` (retryable).

Completion first seals the assembled file — renames it away from the temp name — and only then hashes it. For the whole completion, from the moment it takes the upload until it has answered (verified, published and synced, or failed, or been cancelled by the client disconnecting), a part or a second completion for the same upload is answered `503` (retryable) instead of opening or creating a file, so no new request can write into the bytes being verified or seal other bytes under the name being published. That window lasts as long as the hash and fdatasync of the pack. A retried completion that arrives after it gets `204` if the first one published the pack (the stored digest matches) and `404` if it failed. The client joins every part request before completing, so it only meets these `503`s when a timed-out attempt of its own is still being processed. A stale part that the server receives only after the completion has ended starts a fresh upload under the temp name that nothing completes; it cannot touch the committed pack, and it holds its quota reservation until it is abandoned (below).

Uploads idle for an hour are abandoned: their temp file is removed and their reservation released (checked whenever an upload starts and on the hourly sweep). The registry is in memory, so a server restart orphans an in-progress upload; its completion then answers `404` and the temp file is swept as ordinary debris after 24 hours. Temp files are hidden from listings and purged by delete-repo like every other temp file.

The client splits packs larger than 16 MiB into equal parts of at most 16 MiB and keeps at most `4 × connections` (4..32) part requests in flight across all packs. A failed part makes the client send that pack as a whole `PUT`; a `4xx` refusal also turns the part path off for the rest of the run. Once one completion has succeeded, a backup frees each pack's buffer as soon as its last part is acknowledged, while the server hashes and fsyncs it: a completion that then still fails after its retries (for example `404` after a server restart) fails the backup instead of falling back to a whole `PUT`. Until then a failed completion falls back as well. Servers that do not advertise the capability only ever see whole-object `PUT`s, and old clients never call the new endpoints.

## Backup Freshness Monitoring

The server updates `last_backup_at` when it observes a new `snapshots/*` key being written for the first time. This marks the completion of a backup commit.

## Server-Side Verify Packs

`vykar check` can offload pack verification to the server when the backend is REST and the server supports `/?verify-packs`.

The client sends a verification plan describing packs and expected blob boundaries. The server validates:

- pack header magic and version
- blob boundaries and length-prefix structure
- the pack's content hash, under the algorithm the plan declares (`"hash": "blake2b" | "blake3"`; absent means blake2b, i.e. a pre-BLAKE3 client)

The server holds no keys, so this proves each pack is the object the client uploaded and is structurally sound; it does not decrypt chunks or recompute chunk IDs. Packs the server reports as verified are skipped by the client, including under `--verify-data`. To download and verify every chunk locally instead, pass `vykar check --verify-data --distrust-server` (`--distrust-server` is only accepted together with `--verify-data`).

## Server-Side Repack

`vykar compact` can use `/?repack` to rewrite packs server-side without downloading encrypted blobs to the client, when the server advertises `repack` and `repack_trailer` in `/health`.

High-level flow:

1. The client opens the repo, marks the live snapshots, sweeps the index, then analyzes pack liveness from it.
2. The client sends a repack plan describing source packs and live blob offsets.
3. The server copies the referenced encrypted blobs into new pack files, preserving the pack wire format.
   An operation may carry a `trailer` (hex in the JSON plan): opaque bytes the server appends to that output pack as one more length-prefixed blob after the kept blobs and includes in the pack ID, the output cap and the quota. It requires `protocol_version: 3` (so a server from 0.8 through 0.24 refuses the plan by version instead of dropping the trailer) and is sent only to a server that advertises `repack_trailer`. Compact sends one with every repacked pack: the encrypted pack trailer (architecture.md, Pack File Format). Against a server without `repack_trailer` the client repacks itself.
4. The server returns new pack keys and offsets so the client can update the chunk index.
5. After saving the index, the client deletes the source packs with one `POST /?batch-delete` (without `cleanup-dirs`), falling back to per-pack `DELETE`.

This is encrypted passthrough: the server never decrypts chunk payloads.

## Structure Checks

`GET /?verify-structure` validates repository shape without needing encryption keys. It checks:

- required files (`config`, `keys/repokey`, and the index head: `index`, or for format v6 and later a head under `index.h/`) and expected key layout
- pack shard naming and pack header magic/version
- malformed or obviously invalid pack files

This complements client-side `vykar check`, which still owns full cryptographic verification.

## RestBackend

`crates/vykar-storage/src/rest_backend.rs` implements `StorageBackend` with `ureq`. The server-side operations (batch delete, init, verify-packs, repack, object sizes, one-request tree listing, the `/health` probe) are the `BatchDelete` and `ServerOps` traits, reached through `StorageBackend::batch_delete()` and `server_ops()`; callers ask `ServerOps::offers` before using an optional one.

It also sends the content-digest header on `PUT` requests — bound once to the repository's algorithm when the repository is opened — and validates `Content-Range` on ranged reads.

`GET /health` is unauthenticated and advertises what the server supports:

```json
{"status":"ok","version":"0.20.0","protocol_version":3,"hashes":["blake2b","blake3"],"pack_parts":true,"segmented_index":true,"key_dirs":["keys","snapshots","packs","locks","sessions","pending_index","index.d","index.h","packmaps"],"conditional_put":true,"object_sizes":true,"list_tree":true,"verify_packs":true,"repack":true,"repack_trailer":true,"append_only":false}
```

`append_only` reports whether the server runs with `--append-only`, so a client can skip deletes the server would refuse: clients skip the index garbage collection there and refuse compact, prune, snapshot delete and a data-dropping `check --repair` before changing anything. Older servers omit it, which a client reads as `false`: "not known to be append-only".

`conditional_put` says the server honours `If-None-Match: *` on `PUT` (see the `PUT` row above). Older servers omit it and would ignore the header and overwrite, so a client sends a conditional PUT only to a server that advertises it.

`object_sizes`, `list_tree`, `verify_packs` and `repack` name the operations a client may gate on. The client uses server-side repack only when both `repack` and `repack_trailer` are advertised: against an older server, or when the `/health` probe failed, compact repacks on the client and logs a warning naming the cause. The other three are served by every server the client supports, so the client does not wait for the field; an old server's `400` to `?sizes` is handled by the client. `repack_trailer` says a repack operation may carry a trailer (see [Server-Side Repack](#server-side-repack)).

`key_dirs` lists the top-level key directories the server accepts. `index.h` and `packmaps` (format v6) are accepted only by servers that list them; older servers omit the field and answer `400` to those keys.

`pack_parts` advertises [pack uploads in parts](#pack-uploads-in-parts); the client probes it lazily, on the first pack large enough to split (or the first non-pack upload when the repository is v2, to pick its digest header), so a backup that uploads nothing pays no extra round trip. All fields beyond `status` and `version` are additive: a pre-BLAKE3 server omits `protocol_version` and `hashes`, which a client reads as "blake2b only", a server without part support omits `pack_parts`, and a server without the segmented index omits `segmented_index`. `vykar init` probes this **before writing anything**, because `init` writes only non-pack keys that an old server happily accepts — without the probe, `init` would succeed and only the first `backup` would fail. Requests that declare `"hash": "blake3"` must also declare `protocol_version: 2`, so an old server rejects them outright rather than silently verifying every pack under the wrong algorithm. Likewise a repack plan that carries a trailer declares `protocol_version: 3`.

Client config:

```yaml
repositories:
  - label: server
    url: https://backup.example.com
    access_token: "secret-token-here"
```

---

*Related: [Setup](/server-setup), [Architecture](/architecture)*
