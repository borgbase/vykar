# Storage Backends

The repository URL in your config determines which backend is used. All backends are included in the pre-built binaries. Every repository field, with defaults, is listed in [Configuration → Repositories](configuration.md#repositories).

| Backend | URL example |
|---------|-------------|
| Local filesystem | `/backups/repo` |
| S3 / S3-compatible (HTTPS) | `s3://endpoint[:port]/bucket/prefix` |
| S3 / S3-compatible (HTTP, unsafe) | `s3+http://endpoint[:port]/bucket/prefix` |
| SFTP | `sftp://host/path` |
| REST (vykar-server) | `https://host` |

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

## SFTP

Store backups on a remote server via SFTP. Uses a native [russh](https://github.com/Eugeny/russh) implementation (pure Rust SSH/SFTP) — no system `ssh` binary required. Works on all platforms including Windows.

Host keys are verified with an OpenSSH `known_hosts` file. Unknown hosts use TOFU (trust-on-first-use): the first key is stored, and later key changes fail connection.

```yaml
repositories:
  - label: "nas"
    url: "sftp://backup@nas.local/backups/vykar"
    # sftp_key: "/home/user/.ssh/id_rsa"  # Path to private key (optional)
    # sftp_known_hosts: "/home/user/.ssh/known_hosts"  # Optional known_hosts path
    # sftp_timeout: 30         # Per-request timeout in seconds (default: 30, range: 5–300)
```

URL format: `sftp://[user@]host[:port]/path`. Default port is 22.

The server must support the `posix-rename@openssh.com` extension. Vykar uploads each object to a temporary file and renames it over the destination, so an interrupted upload never leaves a partial object in place. OpenSSH's `sftp-server` provides the extension. On servers that do not advertise it, reads (restore, list, check) still work, but every write fails with an error naming the missing extension.

## REST (vykar-server)

Store backups on a dedicated [vykar-server](server-setup.md) instance via HTTP/HTTPS. The server provides append-only enforcement, quotas, lock management, and server-side compaction.

```yaml
repositories:
  - label: "server"
    url: "https://backup.example.com"
    access_token: "my-secret-token"          # Bearer token for authentication
```

See [Server Setup](server-setup.md) for how to set up and configure the server.

## S3 append-only / ransomware protection

When using S3 directly (without `vykar-server`), a compromised client that has the
S3 credentials can delete or overwrite any object in the bucket. S3 Object Lock
preserves previous versions of all objects for a configurable retention period,
giving you a window to detect and recover from an attack. Vykar's soft-delete mode
(`s3_soft_delete`) enables prune and compact to work without `s3:DeleteObject`
permission by replacing deletes with zero-byte tombstone overwrites.

For full application-level append-only enforcement (rejects both overwrites and
deletes of immutable keys), use [vykar-server](server-setup.md) instead.

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

For stronger guarantees, use [vykar-server --append-only](server-setup.md), which
rejects both overwrites and deletes of immutable keys at the application layer.
