# Storage Backends

The repository URL in your config determines which backend is used.

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

### S3 configuration options

| Field | Description |
|-------|-------------|
| `region` | AWS region (default: `us-east-1`) |
| `access_key_id` | Access key ID (required) |
| `secret_access_key` | Secret access key (required) |
| `allow_insecure_http` | Permit `s3+http://` URLs (unsafe; default: `false`) |
| `s3_soft_delete` | Use soft-delete for S3 Object Lock compatibility (default: `false`) |

### S3 append-only / ransomware protection

When using S3 directly (without `vykar-server`), a compromised client that has the
S3 credentials can delete or overwrite any object in the bucket. S3 Object Lock
preserves previous versions of all objects for a configurable retention period,
giving you a window to detect and recover from an attack. Vykar's soft-delete mode
(`s3_soft_delete`) enables prune and compact to work without `s3:DeleteObject`
permission by replacing deletes with zero-byte tombstone overwrites.

For full application-level append-only enforcement (rejects both overwrites and
deletes of immutable keys), use [vykar-server](server-setup.md) instead.

#### Setup

Three components work together:

1. **S3 Object Lock** — preserves previous object versions for a retention period
2. **`s3_soft_delete`** — vykar overwrites objects with zero-byte tombstones
   instead of issuing real DELETEs, so prune and compact work without needing
   `s3:DeleteObject` permission
3. **S3 lifecycle rule** — automatically cleans up non-current (expired) versions

#### Step 1: Create a bucket with Object Lock

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

#### Step 2: Add a lifecycle rule for cleanup

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

#### Step 3: Enable soft-delete in vykar

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

#### Recovery after an attack

If a compromised client has overwritten objects with garbage, the original versions
are preserved as non-current versions in S3. To recover, restore the pre-attack
versions using the AWS CLI.

**1. Identify affected objects.** List versions of a specific key to find the good
version:

```bash
aws s3api list-object-versions \
  --bucket my-backup-bucket \
  --prefix "packs/ab/" \
  --query 'Versions[?Key==`packs/ab/PACK_ID`].[VersionId,LastModified,Size]' \
  --output table
```

Versions with `Size: 0` are tombstones from soft-delete. Versions with the expected
size from before the attack timestamp are the ones to restore.

**2. Restore a specific version** by copying it back as the current version:

```bash
aws s3api copy-object \
  --bucket my-backup-bucket \
  --key "packs/ab/PACK_ID" \
  --copy-source "my-backup-bucket/packs/ab/PACK_ID?versionId=VERSION_ID"
```

**3. Restore all objects to a point in time.** To bulk-restore the latest good
version of every object modified after a known-good timestamp:

```bash
# For each key, find the most recent non-current version before the attack
# timestamp and copy it back as the current version.
aws s3api list-object-versions \
  --bucket my-backup-bucket \
  --query 'Versions[?LastModified<`2025-01-15T00:00:00Z` && !IsLatest].[Key,VersionId,LastModified]' \
  --output text \
| sort -k1,1 -k3,3r \
| awk '!seen[$1]++ {print $1, $2}' \
| while read -r key version_id; do
    aws s3api copy-object \
      --bucket my-backup-bucket \
      --key "$key" \
      --copy-source "my-backup-bucket/${key}?versionId=${version_id}"
  done
```

The `sort | awk` pipeline selects only the latest version per key — it sorts by key
then by timestamp (newest first), and `awk` keeps only the first occurrence of each
key.

After restoring, verify the repository with `vykar check` before restoring data.

The recovery commands require `s3:ListBucketVersions` (to list versions),
`s3:GetObjectVersion` (to read a specific version via `?versionId=`), and
`s3:PutObject` (to copy it back as current). The backup client should not have
`s3:ListBucketVersions` or `s3:GetObjectVersion` during normal operation — use
separate admin credentials for recovery.

#### Limitations

This setup provides a **deletion delay**, not strict immutability. A compromised
client can still overwrite objects with garbage. The protection is that the previous
version is preserved for the retention period, allowing recovery if the attack is
detected in time.

For stronger guarantees, use [vykar-server --append-only](server-setup.md), which
rejects both overwrites and deletes of immutable keys at the application layer.

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

### SFTP configuration options

| Field | Description |
|-------|-------------|
| `sftp_key` | Path to SSH private key (auto-detects `~/.ssh/id_ed25519`, `id_rsa`, `id_ecdsa`) |
| `sftp_known_hosts` | Path to OpenSSH `known_hosts` file (default: `~/.ssh/known_hosts`) |
| `sftp_timeout` | Per-request SFTP timeout in seconds (default: `30`, clamped to `5..=300`) |

## REST (vykar-server)

Store backups on a dedicated [vykar-server](server-setup.md) instance via HTTP/HTTPS. The server provides append-only enforcement, quotas, lock management, and server-side compaction.

```yaml
repositories:
  - label: "server"
    url: "https://backup.example.com"
    access_token: "my-secret-token"          # Bearer token for authentication
```

### REST configuration options

| Field | Description |
|-------|-------------|
| `access_token` | Bearer token sent as `Authorization: Bearer <token>` |
| `allow_insecure_http` | Permit `http://` REST URLs (unsafe; default: `false`) |

See [Server Setup](server-setup.md) for how to set up and configure the server.

All backends are included in pre-built binaries from the [releases page](https://github.com/borgbase/vykar/releases).
