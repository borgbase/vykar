# Server Setup

Vykar includes a dedicated backup server for secure, policy-enforced remote backups. TLS is typically handled by a reverse proxy such as nginx or Caddy.

## Why a dedicated REST server instead of plain S3

Dumb storage backends (S3, WebDAV, SFTP) work well for basic backups, but they cannot enforce policy or do server-side work. `vykar-server` adds capabilities that object storage alone cannot provide.

| Capability | S3 / dumb storage | vykar-server |
|------------|-------------------|-------------|
| Append-only mode | [S3 Object Lock + soft-delete](backends.md#s3-append-only--ransomware-protection) preserves previous versions for a configurable retention period; overwrites are not blocked but are recoverable within the retention window | Rejects deletes and overwrites of immutable keys; only `index`, `index.gen`, `locks/*`, and `sessions/*` remain mutable |
| Server-side compaction | Client must download and re-upload all live blobs | Server repacks locally on disk from a compact plan |
| Quota enforcement | Requires external bucket policy/IAM setup | Built-in byte quota checks on writes |
| Backup freshness monitoring | Requires external polling and parsing | Tracks `last_backup_at` on new snapshot writes |
| Upload integrity | Relies on backend checksums only | Verifies `X-Content-BLAKE3` / `X-Content-BLAKE2b` during uploads |
| Structural health checks | Client has to fetch data to verify structure | Server validates repository shape directly |

All data remains client-side encrypted. The server never has the encryption key and cannot read backup contents.

## Install

Download a binary for your platform from the [releases page](https://github.com/borgbase/vykar/releases).

## Server configuration

All settings are passed as CLI flags. The authentication token is read from the `VYKAR_TOKEN` environment variable so it does not appear in process arguments.

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `-l, --listen` | `localhost:8585` | Address to listen on |
| `-d, --data-dir` | `/var/lib/vykar` | Root directory where repositories are stored |
| `--append-only` | `false` | Reject `DELETE` and overwriting immutable keys (config, keys, snapshots, packs). Mutable keys (index, index.gen, locks, sessions) remain writable. |
| `--log-format` | `pretty` | Log output format: `json` or `pretty` |
| `--quota` | auto-detect | Storage quota (`500M`, `10G`, plain bytes). If omitted, the server detects filesystem quota or falls back to free space |
| `--network-threads` | `4` | Async threads for handling network connections |
| `--io-threads` | `6` | Threads for blocking disk I/O (reads, writes, hashing) |
| `--debug` | `false` | Enable debug logging |

### Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `VYKAR_TOKEN` | Yes | Shared bearer token for authentication |

## Start the server

```bash
export VYKAR_TOKEN="some-secret-token"
vykar-server --data-dir /var/lib/vykar --append-only --quota 10G
```

On `SIGINT` (Ctrl-C) or `SIGTERM` the server stops accepting new connections,
lets in-flight requests finish, then exits with status 0. A second signal exits
immediately (status 130 for `SIGINT`, 143 for `SIGTERM`). There is no built-in
drain deadline, so a stalled upload holds the process until the second signal
or the supervisor's stop timeout (Docker defaults to 10 s, systemd
`TimeoutStopSec` to 90 s) sends `SIGKILL`.

## Run as a systemd service

Create a dedicated system account for the server, then an environment file at `/etc/vykar/vykar-server.env` with restricted permissions:

```bash
sudo useradd --system --home-dir /var/lib/vykar --shell /usr/sbin/nologin vykar
sudo mkdir -p /etc/vykar
echo 'VYKAR_TOKEN=some-secret-token' | sudo tee /etc/vykar/vykar-server.env
sudo chmod 600 /etc/vykar/vykar-server.env
sudo chown vykar:vykar /etc/vykar/vykar-server.env
```

Create `/etc/systemd/system/vykar-server.service`:

```ini
[Unit]
Description=Vykar backup REST server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=vykar
Group=vykar
EnvironmentFile=/etc/vykar/vykar-server.env
ExecStart=/usr/local/bin/vykar-server --data-dir /var/lib/vykar --append-only
Restart=on-failure
RestartSec=2
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
# systemd creates /var/lib/vykar, owned by the service user
StateDirectory=vykar

[Install]
WantedBy=multi-user.target
```

Then reload and enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vykar-server.service
sudo systemctl status vykar-server.service
```

## Reverse proxy

`vykar-server` listens on HTTP and expects a reverse proxy to handle TLS. Pack uploads can be up to 512 MiB, so the proxy must allow large request bodies.

### Nginx

```nginx
server {
    listen 443 ssl http2;
    server_name backup.example.com;

    ssl_certificate     /etc/letsencrypt/live/backup.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/backup.example.com/privkey.pem;

    client_max_body_size    600m;
    proxy_request_buffering off;

    location / {
        proxy_pass http://127.0.0.1:8585;
    }
}
```

### Caddy

```caddyfile
backup.example.com {
    request_body {
        max_size 600MB
    }
    reverse_proxy 127.0.0.1:8585
}
```

## Client configuration (REST backend)

```yaml
repositories:
  - label: "server"
    url: "https://backup.example.com"
    access_token: "some-secret-token"

encryption:
  mode: "auto"

sources:
  - "/home/user/documents"
```

All standard repository commands work over REST without changing the CLI workflow. Against a server started with `--append-only`, the ones that remove data are refused — see below.

## Maintenance on an append-only server

`--append-only` rejects every request that deletes or overwrites committed data, so these commands fail with `403 Forbidden`:

- `vykar snapshot delete` and `vykar prune`
- `vykar compact`
- `vykar check --repair`
- `vykar delete`

`init`, `backup`, `list`, `info`, `restore`, `mount`, and `check` (without `--repair`) work as usual. In the full cycle (bare `vykar`, the daemon, the GUI), a configured `retention` makes the prune step fail on every run. Leave `retention` out of the backup clients' config — the cycle then skips pruning.

Retention therefore has to be applied from a trusted machine during a maintenance window:

1. Stop the server and start it again **without** `--append-only`, with a different `VYKAR_TOKEN` so the regular backup clients cannot use the window.
2. From the trusted machine, with that token as `access_token`, run `vykar prune --compact`.
3. Restart the server with `--append-only` and the original token.

Backups in progress are interrupted by each restart.

## Health check

```bash
# No auth required
curl http://localhost:8585/health
```

Returns JSON like:

```json
{"status":"ok","version":"0.20.0","protocol_version":2,"hashes":["blake2b","blake3"]}
```
