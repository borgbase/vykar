# Storage Backends

The repository URL in your config determines which backend is used.

| Backend | URL example |
|---------|-------------|
| Local filesystem | `/backups/repo` |
| S3 / S3-compatible (HTTPS) | `s3://endpoint[:port]/bucket/prefix` |
| S3 / S3-compatible (HTTP, unsafe) | `s3+http://endpoint[:port]/bucket/prefix` |
| SFTP | `sftp://host/path` |
| REST (vykar-server) | `https://host/repo` |

## Transport security

HTTP transport is blocked by default for remote backends.

- `https://...` is accepted by default.
- `http://...` (or `s3+http://...`) requires explicit opt-in with `allow_insecure_http: true`.

```yaml
repositories:
  - label: "dev-only"
    url: "http://localhost:8484/myrepo"
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
    url: "https://backup.example.com/myrepo"
    access_token: "my-secret-token"          # Bearer token for authentication
```

### REST configuration options

| Field | Description |
|-------|-------------|
| `access_token` | Bearer token sent as `Authorization: Bearer <token>` |
| `allow_insecure_http` | Permit `http://` REST URLs (unsafe; default: `false`) |

See [Server Setup](server-setup.md) for how to set up and configure the server.

All backends are included in pre-built binaries from the [releases page](https://github.com/borgbase/vykar/releases).
