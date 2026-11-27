---
title: "Storage Backends"
---

The repository URL in your config determines which backend is used. All backends are included in the pre-built binaries. Every repository field, with defaults, is listed in [Configuration → Repositories](/configuration#repositories).

<figure>

| Backend | URL example | Bandwidth\* |
|---------|-------------|-------------|
| Local filesystem | `/backups/repo` | |
| S3 / S3-compatible (HTTPS) | `s3://endpoint[:port]/bucket/prefix` | |
| S3 / S3-compatible (HTTP, unsafe) | `s3+http://endpoint[:port]/bucket/prefix` | 134 MiB/s |
| SFTP | `sftp://host/path` | 37 MiB/s |
| REST (vykar-server) | `https://host` | 146 MiB/s |

<figcaption>* Average bandwidth over a fresh backup of a 14.63 GB corpus with 40 ms round-trip latency. Each value is the median of three runs. The S3 benchmark used MinIO over HTTP, and the REST benchmark also used HTTP.</figcaption>
</figure>

## Transport security

HTTP transport is blocked by default for remote backends.

- `https://...` is accepted by default.
- `http://...` (or `s3+http://...`) requires explicit opt-in with `allow_insecure_http: true`.

```yaml
repositories:
  - label: "dev-only"
    url: "http://localhost:8484"
    allow_insecure_http: true
```

Use plaintext HTTP only on trusted local/dev networks.

### TLS certificates

REST and S3 connections verify server certificates against the operating system's trust store, so a private CA that your OS already trusts works without extra configuration.

- **macOS and Windows:** verification uses the OS certificate APIs, including any revocation checks they perform.
- **Linux:** certificates are loaded from the system CA bundle. Install your distribution's `ca-certificates` package; minimal containers often omit it. Without a bundle, HTTPS connections fail, including those to public endpoints, with `No CA certificates were loaded from the system`.
- **Custom bundle on Linux:** set `SSL_CERT_FILE` to a PEM file, or `SSL_CERT_DIR` to a directory of certificates.

## Local filesystem

Store backups on a local or mounted disk. No extra configuration needed.

```yaml
repositories:
  - label: "local"
    url: "/backups/repo"
```

Accepted URL formats: absolute paths (`/backups/repo`), relative paths (`./repo`), or `file:///backups/repo`.

## S3 / S3-compatible

Store backups in Amazon S3 or any S3-compatible service (MinIO, Wasabi, Backblaze B2, etc.).
S3 URLs must include an explicit endpoint and bucket path.

**AWS S3:**

```yaml
repositories:
  - label: "s3"
    url: "s3://s3.us-east-1.amazonaws.com/my-bucket/vykar"
    region: "us-east-1"                    # Default if omitted
    access_key_id: "AKIA..."
    secret_access_key: "..."
```

**S3-compatible (custom endpoint):**

The endpoint is always the URL host, and the first path segment is the bucket:

```yaml
repositories:
  - label: "minio"
    url: "s3://minio.local:9000/my-bucket/vykar"
    region: "us-east-1"
    access_key_id: "minioadmin"
    secret_access_key: "minioadmin"
```

**S3-compatible over plaintext HTTP (unsafe):**

```yaml
repositories:
  - label: "minio-dev"
    url: "s3+http://minio.local:9000/my-bucket/vykar"
    region: "us-east-1"
    access_key_id: "minioadmin"
    secret_access_key: "minioadmin"
    allow_insecure_http: true
```

To run without `s3:DeleteObject` permission, see [S3 append-only / ransomware protection](#s3-append-only--ransomware-protection) below.

### Large object uploads (multipart)

Objects larger than 16 MiB — primarily packs, but also a large `index` — are
uploaded with the S3 multipart API (16 MiB parts) instead of a single PUT. Each
part is retried independently, so a connection reset mid-transfer costs one part
rather than restarting the whole object. This is resilience on flaky or proxied
links; it needs no configuration and applies to every S3-compatible backend that
supports multipart uploads (including Backblaze B2's S3 endpoint).

A process crash mid-upload, or a lost response from S3, can occasionally leave an
*in-progress* multipart upload that vykar never aborts. These carry no completed
object and are invisible to backups, but some providers bill for the storage held
by their uploaded parts. Vykar cannot guarantee cleanup of these, so configure an
**`AbortIncompleteMultipartUpload`** lifecycle rule on the bucket (1–7 days is a
reasonable window) to expire them automatically:

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket my-bucket \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "abort-incomplete-multipart-uploads",
      "Status": "Enabled",
      "Filter": {"Prefix": ""},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}
    }]
  }'
```

### Conditional writes

Some repository operations can use put-if-absent: create an object only if no object exists under its key, decided atomically by the storage. On S3 this is `If-None-Match: *` on PutObject. Not every S3-compatible service implements it correctly, so the `s3_conditional_writes` setting decides whether vykar offers it:

| Value | Meaning |
|-------|---------|
| `auto` (default) | Offered only on hosts of a service on vykar's list below whose conditional writes are tested or documented: today AWS (a host under `amazonaws.com` or `amazonaws.com.cn`). Every other endpoint, including a custom domain in front of AWS, is treated as `off`. |
| `on` | You assert that your service honours the condition atomically, including for concurrent requests. Use it only for a service you have verified. On a host of a known-bad service (IDrive e2) put-if-absent is refused whatever the setting: such a client can only use a single-writer repository (one writer at a time, no lock), with a warning. |
| `off` | Never offered. |

Before the first conditional write, vykar runs a check on a fresh key under `sessions/`: it creates the key twice and expects the second create to be refused with the first content kept, then deletes the key. A service that ignores the header fails this check, and vykar then stops offering put-if-absent for the rest of the run. The check is sequential: it cannot detect a service that refuses a repeated create but lets two concurrent creates both succeed. That is why `auto` trusts only the services listed below, and why `on` is your assertion about the service, not something vykar proves. A stricter check that also races concurrent creates of one key exists for recording a repository's mode.

Services vykar knows (runs of the `put_if_absent_probe` example, see [Testing](/testing)):

| Service | Version | Date | Verdict | Run |
|---------|---------|------|---------|-----|
| AWS S3 | — | — | trusted by `auto` on AWS's documented conditional writes | no vykar probe run recorded |
| MinIO | RELEASE.2025-09-07T16-13-09Z | 26 Nov 2026 | works, every case including the race and the listing case | local loopback instance |
| MinIO | 2025-10-15 | 21 Nov 2026 | works | |
| Garage | 2.5.0 | | ignores the header and overwrites; the check catches this and refuses | |
| IDrive e2 | | 21 Nov 2026 | passes the sequential check but lets concurrent creates both win (1 of 10 race rounds with a single winner, up to 8 winners): known-bad, refused even with `on` | fra1 |
| Ceph RGW, Cloudflare R2, Backblaze B2, Wasabi | | | untested | |

vykar recognises IDrive e2 by its endpoint host: one under `idrivee2.com`, or under `idrivee2-<n>.com` (`<n>` digits). These two forms are a reading of IDrive's public endpoint formats, not the endpoint of the recorded run; a host of another form is not recognised and is treated like any unknown endpoint. MinIO has no host to recognise (it runs under any name): set `on` for it.

With `s3_soft_delete`, a deleted key leaves a zero-byte tombstone, which still counts as existing for put-if-absent: a create of a collected head's generation finds it "taken", and the writer rebases onto the latest head.

**What uses it.** Every index write creates its head (`index.h/<gen>`) with put-if-absent where the backend offers it: backup commits, compact, `check --repair` and upgrade (the first head `init` writes is a plain PUT). Local repositories (no-clobber rename), SFTP servers with `hardlink@openssh.com` (OpenSSH), vykar-server and S3 as configured above offer it. A head whose generation another writer already created is never replaced: a backup rebases and tries again, a maintenance command stops with "index changed … retry". The check runs once per run: for a backup on an encrypted repository after its upload, before any commit write, because its verdict decides how the backup commits (below); for every other writer at the lock fence of its first index write, just before the lock and pack-age checks that precede the head create (four small requests under `sessions/`; a probe left by a failed delete is removed once an hour old by the next maintenance command); a check that cannot conclude (a transport error) fails that backup or that write like a failed PUT, and the next run checks again. When the check fails, or the backend does not offer the mechanism, the repository is single-writer and heads are written with an existence check, a plain PUT and a read-back, behind the best-effort single-writer check (no lock): logged once per opened repository, at its first index write, at warn when the storage ignored the condition (or an S3 endpoint with conditional writes enabled, by `on` or by `auto` on AWS, refused the header, or is a known-bad service), at info when the mechanism is simply not available. A program that opens several repositories (the GUI, a daemon) logs it for each.

**Lock-free backups.** A backup on an **encrypted** repository whose storage passed the check commits without the repository lock: its head is a create, and two concurrent backups meet only there (the loser rebases). On a single-writer repository (a backend that does not offer put-if-absent or whose check failed) a backup commits without the lock too, one at a time: it waits while another backup commits or a compact runs, and its head is an existence check, a plain PUT and a read-back. Once a client's check passed, a later refusal of a create fails its commit; it never falls back to a plain head PUT on a multi-writer repository. Compact, prune and delete run without the lock there too (see [Lock-free maintenance](/architecture#lock-free-maintenance)); pack maps, `check --repair` and upgrade run as exclusive sessions: they register, list `sessions/` and refuse while any other session is live; backups and maintenance refuse while one runs. With `s3_conditional_writes: on` lock-free commits rest on your assertion that the service lets only one of two concurrent creates win: a service that passes the sequential check but lets both win can then lose a backup's index entries when two backups commit at once. On IDrive e2, the one such service known, vykar refuses put-if-absent whatever the setting, so its clients use single-writer repositories.

## SFTP

Store backups on a remote server via SFTP. Uses a native [russh](https://github.com/Eugeny/russh) implementation (pure Rust SSH/SFTP) — no system `ssh` binary required. Works on all platforms including Windows.

Host keys are verified with an OpenSSH `known_hosts` file. Unknown hosts use TOFU (trust-on-first-use): the first key is stored, and later key changes fail connection.

```yaml
repositories:
  - label: "nas"
    url: "sftp://backup@nas.local/backups/vykar"
    # sftp_key: "/home/user/.ssh/id_rsa"  # Path to private key, or public key held by the agent (optional)
    # sftp_agent: "/path/to/agent.sock"  # SSH agent socket (optional, default: SSH_AUTH_SOCK)
    # sftp_known_hosts: "/home/user/.ssh/known_hosts"  # Optional known_hosts path
    # sftp_timeout: 30         # Per-request timeout in seconds (default: 30, range: 5–300)
```

URL format: `sftp://[user@]host[:port]/path`. Default port is 22.

When `sftp_key` is not set, vykar first offers the keys held by the SSH agent. If the agent is absent or the server rejects its keys while keeping the connection open, vykar falls back to `~/.ssh/{id_ed25519, id_rsa, id_ecdsa}`. Use the agent for passphrase-protected keys, because vykar cannot read them from disk.

Servers can disconnect after too many rejected keys, preventing fallback. If your agent holds many keys, set `sftp_key` to the intended `.pub` file, or to a private key to bypass the agent.

Agent authentication has a 30-second timeout, including any approval prompt. Timeouts and temporary connection failures use the configured retries.

`sftp_agent` sets the agent socket, like OpenSSH's `IdentityAgent`. Without it, vykar uses `SSH_AUTH_SOCK`. On Windows, `sftp_agent` is a named pipe. If neither is set, vykar tries the Windows OpenSSH agent at `\\.\pipe\openssh-ssh-agent`, which 1Password and Bitwarden also serve, and then Pageant.

If `sftp_key` names a public key file, vykar signs with the agent and offers only that key, like an OpenSSH `IdentityFile` that points to a `.pub` file. If `sftp_key` names a private key, vykar does not use the agent.

```yaml
    sftp_agent: "~/.ssh/agent.sock"
    sftp_key: "~/.ssh/id_ed25519.pub"
```

The server must support the `posix-rename@openssh.com` extension. Vykar uploads each object to a temporary file and renames it over the destination, so an interrupted upload never leaves a partial object in place. OpenSSH's `sftp-server` provides the extension. On servers that do not advertise it, reads (restore, list, check) still work, but every write fails with an error naming the missing extension.

Put-if-absent uses the `hardlink@openssh.com` extension when the server offers it (OpenSSH's `sftp-server` does): the object is written to a temporary file and hard-linked to its key, which fails if the key exists. It does not need `posix-rename@openssh.com`. A server that does not offer the extension, or refuses the link, has no put-if-absent; the first check finds out and turns it off for the rest of the run.

`sftp_timeout` applies to each SFTP request. The data writes of an upload are sent up to 2 MiB ahead of their acknowledgements, so each of them is allowed four times `sftp_timeout`.

## REST (vykar-server)

Store backups on a dedicated [vykar-server](/server-setup) instance via HTTP/HTTPS. The server provides append-only enforcement, quotas, lock management, and server-side compaction.

```yaml
repositories:
  - label: "server"
    url: "https://backup.example.com"
    access_token: "my-secret-token"          # Bearer token for authentication
```

See [Server Setup](/server-setup) for how to set up and configure the server.

Put-if-absent is offered when the server advertises `conditional_put` in `/health`; it is a `PUT` with `If-None-Match: *`, answered `412` when the key exists. Older servers do not advertise it, and vykar never sends them a conditional PUT.

## S3 append-only / ransomware protection

When using S3 directly (without `vykar-server`), a compromised client that has the
S3 credentials can delete or overwrite any object in the bucket. S3 Object Lock
preserves previous versions of all objects for a configurable retention period,
giving you a window to detect and recover from an attack. Vykar's soft-delete mode
(`s3_soft_delete`) enables prune and compact to work without `s3:DeleteObject`
permission by replacing deletes with zero-byte tombstone overwrites.

For full application-level append-only enforcement (rejects both overwrites and
deletes of immutable keys), use [vykar-server](/server-setup) instead.

### Setup

Three components work together:

1. **S3 Object Lock** — preserves previous object versions for a retention period
2. **`s3_soft_delete`** — vykar overwrites objects with zero-byte tombstones
   instead of issuing real DELETEs, so prune and compact work without needing
   `s3:DeleteObject` permission
3. **S3 lifecycle rule** — automatically cleans up non-current (expired) versions

### Step 1: Create a bucket with Object Lock

Object Lock can be enabled on a new or existing bucket (existing buckets must have
versioning enabled first).

```bash
# New bucket:
# For regions other than us-east-1, add:
#   --create-bucket-configuration LocationConstraint=REGION
aws s3api create-bucket \
  --bucket my-backup-bucket \
  --object-lock-enabled-for-bucket

# Or enable on an existing versioned bucket:
# aws s3api put-object-lock-configuration \
#   --bucket my-backup-bucket \
#   --object-lock-configuration '{"ObjectLockEnabled": "Enabled"}'

# Set a default retention policy (GOVERNANCE mode, 30-day retention)
aws s3api put-object-lock-configuration \
  --bucket my-backup-bucket \
  --object-lock-configuration '{
    "ObjectLockEnabled": "Enabled",
    "Rule": {
      "DefaultRetention": {
        "Mode": "GOVERNANCE",
        "Days": 30
      }
    }
  }'
```

The retention period is your recovery window. If an attacker overwrites backup data,
you have this many days to detect the attack and restore from the previous version.
30 days is a starting point; increase it if you need a longer detection window.

**GOVERNANCE vs COMPLIANCE mode:**

- **GOVERNANCE**: Users with `s3:BypassGovernanceRetention` can delete locked
  objects before retention expires. Recommended for backup repositories.
- **COMPLIANCE**: No one can delete locked objects until retention expires, not
  even the root account. Use only if regulatory requirements demand it.

Object Lock automatically enables bucket versioning.

### Step 2: Add a lifecycle rule for cleanup

Without a lifecycle rule, non-current versions accumulate indefinitely. Add a rule
to expire them after the retention period:

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket my-backup-bucket \
  --lifecycle-configuration '{
    "Rules": [
      {
        "ID": "CleanupExpiredVersions",
        "Status": "Enabled",
        "Filter": {},
        "NoncurrentVersionExpiration": {
          "NoncurrentDays": 30
        },
        "Expiration": {
          "ExpiredObjectDeleteMarker": true
        }
      }
    ]
  }'
```

Set `NoncurrentDays` to match your Object Lock retention period. Versions that are
still locked will not be deleted — S3 respects the lock.

### Step 3: Enable soft-delete in vykar

```yaml
repositories:
  - label: "s3-locked"
    url: "s3://s3.us-east-1.amazonaws.com/my-backup-bucket/vykar"
    region: "us-east-1"
    access_key_id: "AKIA..."
    secret_access_key: "..."
    s3_soft_delete: true
```

With `s3_soft_delete: true`, vykar replaces `DELETE` calls with zero-byte `PUT`
overwrites. The S3 backend transparently filters out these tombstones — they are
invisible to list, get, exists, and size operations. Prune and compact work
normally; the "deleted" data is retained as a non-current version until the
Object Lock retention period expires and the lifecycle rule removes it.

The backup client needs `s3:PutObject`, `s3:GetObject`, and `s3:ListBucket` — no
`s3:DeleteObject` permission required.

**Important:** `s3_soft_delete` must only be used with buckets that have S3 Object
Lock and versioning enabled. On a plain bucket without versioning, the zero-byte
overwrite is irreversible — the original data is lost.

### Recovery after an attack

If a compromised client has overwritten objects with garbage, the original versions
are preserved as non-current versions in S3. To recover, restore the pre-attack
versions using the AWS CLI.

The examples below assume the repository lives under the `vykar/` prefix, as in
the URL `s3://…/my-backup-bucket/vykar`. Use your own repository prefix, and keep
it on every command so nothing else in the bucket is touched.

**1. Identify affected objects.** List versions of a specific key to find the good
version:

```bash
aws s3api list-object-versions \
  --bucket my-backup-bucket \
  --prefix "vykar/packs/ab/" \
  --query 'Versions[?Key==`vykar/packs/ab/PACK_ID`].[VersionId,LastModified,Size]' \
  --output table
```

Versions with `Size: 0` are tombstones from soft-delete. Versions with the expected
size from before the attack timestamp are the ones to restore.

**2. Restore a specific version** by copying it back as the current version:

```bash
aws s3api copy-object \
  --bucket my-backup-bucket \
  --key "vykar/packs/ab/PACK_ID" \
  --copy-source "my-backup-bucket/vykar/packs/ab/PACK_ID?versionId=VERSION_ID"
```

**3. Restore all affected objects to a point in time.** Only objects whose
*current* version was written during or after the attack should be rolled back.
An object whose current version predates the attack is healthy — rolling back
`index`, for example, would hide snapshots that are still intact. The script
therefore selects per key, and only then picks the newest pre-attack version
(requires `jq`):

```bash
BUCKET=my-backup-bucket
PREFIX=vykar/                    # repository prefix, with trailing slash
ATTACK=2025-01-15T00:00:00       # UTC, start of the attack

aws s3api list-object-versions \
  --bucket "$BUCKET" --prefix "$PREFIX" --output json > versions.json

# Keys whose current version dates from the attack, with the newest version
# from before it.
jq -r --arg t "$ATTACK" '
  [.Versions[]?] | group_by(.Key)[]
  | select(any(.[]; .IsLatest and .LastModified >= $t))
  | [.[] | select(.LastModified < $t)] | max_by(.LastModified)
  | select(. != null) | [.Key, .VersionId, .LastModified, .Size] | @tsv
' versions.json > restore.tsv
```

Review `restore.tsv` before changing anything, then apply it:

```bash
while IFS=$'\t' read -r key version_id _; do
  aws s3api copy-object \
    --bucket "$BUCKET" \
    --key "$key" \
    --copy-source "$BUCKET/${key}?versionId=${version_id}"
done < restore.tsv
```

Objects first created during or after the attack have no earlier version and are
left alone. List them to decide what to do with each — they may be legitimate
snapshots and packs from other clients, or attacker-written data:

```bash
jq -r --arg t "$ATTACK" '
  [.Versions[]?] | group_by(.Key)[]
  | select(all(.[]; .LastModified >= $t)) | .[0].Key
' versions.json
```

After restoring, verify the repository with `vykar check --verify-data` before restoring data.

The recovery commands require `s3:ListBucketVersions` (to list versions),
`s3:GetObjectVersion` (to read a specific version via `?versionId=`), and
`s3:PutObject` (to copy it back as current). The backup client should not have
`s3:ListBucketVersions` or `s3:GetObjectVersion` during normal operation — use
separate admin credentials for recovery.

### Limitations

This setup provides a **deletion delay**, not strict immutability. A compromised
client can still overwrite objects with garbage. The protection is that the previous
version is preserved for the retention period, allowing recovery if the attack is
detected in time.

For stronger guarantees, use [vykar-server --append-only](/server-setup), which
rejects both overwrites and deletes of immutable keys at the application layer.
