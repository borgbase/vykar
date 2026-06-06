# Daemon Mode

`vykar daemon` runs scheduled backup cycles as a foreground process. Each cycle executes the default actions (`backup → prune → compact → check`) for all configured repositories, sequentially. The shutdown flag is checked between steps.

- **Scheduling**: sleep-loop with configurable interval (`schedule.every`, e.g. `"6h"`) or cron expression (`schedule.cron`, e.g. `"0 3 * * *"`). Optional random jitter (`jitter_seconds`) spreads load across hosts.
- **Passphrase**: the daemon validates at startup that all encrypted repos have a non-interactive passphrase source (`passcommand`, `passphrase`, or `VYKAR_PASSPHRASE` env). It cannot prompt interactively.
- **Scheduler lock**: the daemon and GUI share a process-wide scheduler lock under the local config directory so only one scheduler is active at a time. On Unix this uses `flock(2)` and is released automatically on process exit.

Configuration:
```yaml
schedule:
  enabled: true
  every: "6h"                  # fixed interval
  # cron: "0 3 * * *"         # OR 5-field cron (mutually exclusive with every)
  on_startup: false
  jitter_seconds: 0
```

## Read-only status page

The daemon can serve a small read-only HTML page that mirrors the GUI overview — repository list, recent snapshots, sources, last cycle outcome, next scheduled run. It is **disabled by default**; opt in with `--http-listen` (or the `VYKAR_HTTP_LISTEN` environment variable):

```bash
vykar daemon --http-listen 127.0.0.1:7575
```

The flag takes a full `host:port` address. There is no implicit default — passing the flag without a value is an error. Port `7575` is the recommended convention but is not assumed.

What the page shows:
- Process info: hostname, pid, version, uptime, next scheduled run
- Schedule summary (interval / cron expression / `Off`)
- Per-repository snapshot count, last snapshot time, total stored size
- The 10 most recent snapshots across all repositories
- Configured sources and their target repositories
- Last cycle: started/finished timestamps, duration, outcome (`ok` / `partial` / `errors`)

The page auto-refreshes every 30 seconds via a `<meta http-equiv="refresh">` tag — no JavaScript, no external assets, no cache. Data is refreshed at process startup, after every backup cycle, and after a SIGHUP reload.

Endpoints:
- `GET /` — HTML overview
- `GET /healthz` — `200 OK` plain text, suitable for Docker / Kubernetes liveness probes
- `GET /api/status.json` — same data as `/`, JSON-serialized

There are no write actions: no "Run Backup" button, no config edits, no authentication. The page is purely an inspection surface.

### Bind safety

Non-loopback bind addresses (anything outside `127.0.0.0/8` and `::1`, including `0.0.0.0` and `::`) are **rejected at startup** unless you also pass `--http-allow-public` (or set `VYKAR_HTTP_ALLOW_PUBLIC=1`):

```bash
vykar daemon --http-listen 0.0.0.0:7575 --http-allow-public
```

The page exposes repository names, URLs, snapshot identifiers, and source paths — information that is sensitive on most deployments. The two-flag rule prevents accidentally exposing this on a public interface. If you need to expose it beyond the host, terminate TLS and add authentication in a reverse proxy (nginx, Caddy, Traefik) — vykar speaks plain HTTP only.

```
+----------------+   loopback   +------------+   public TLS   +------+
| vykar daemon   | <----------- | reverse    | <------------- | user |
| 127.0.0.1:7575 |              | proxy      |                +------+
+----------------+              +------------+
```

## Config reload via SIGHUP

Send `SIGHUP` to the daemon process to reload the configuration file without restarting:

```bash
kill -HUP $(pidof vykar)
```

Reload behavior:
- The reload takes effect **between backup cycles** — a cycle in progress runs to completion first
- `on_startup` is ignored on reload; `next_run` is recalculated from the schedule relative to now
- If the new config is **invalid** (parse error, empty repositories, `schedule.enabled: false`, passphrase validation failure), the daemon logs a warning and continues with the previous config
- If the new config is **valid**, repos and schedule are replaced and the next run time is recalculated

## Ad-hoc backup via SIGUSR1

Send `SIGUSR1` to the daemon to trigger an immediate backup cycle:

```bash
kill -USR1 $(pidof vykar)
```

- The cycle runs **between scheduled backups** — a cycle in progress runs to completion first, then the triggered cycle starts
- The existing schedule is **preserved** when the ad-hoc cycle finishes before the next scheduled slot; if it overruns the slot, the next run is recalculated from the current time (same as after any regular cycle)
- With systemd: `systemctl kill -s USR1 vykar`

## Deployment

### systemd

Create a unit file at `/etc/systemd/system/vykar.service`:

```ini
[Unit]
Description=Vykar Backup Daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStartPre=+/bin/mkdir -p %h/.cache/vykar %h/.config/vykar
ExecStart=/usr/local/bin/vykar --config /etc/vykar/config.yaml daemon
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=60

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=%h/.cache/vykar %h/.config/vykar
# If backing up to a local path, add it here too, e.g.:
# ReadWritePaths=%h/.cache/vykar %h/.config/vykar /mnt/backup/vykar
PrivateTmp=true
PrivateDevices=true

# Passphrase via environment file (optional)
# EnvironmentFile=/etc/vykar/env

[Install]
WantedBy=multi-user.target
```

> **Local repositories**: the `ProtectSystem=strict` directive makes the filesystem read-only by default. If any repository target is a local path, add it to `ReadWritePaths` or the backup will fail with "Read-only file system".

Then enable and start:

```bash
systemctl daemon-reload
systemctl enable --now vykar
```

Reload configuration after editing the config file:

```bash
systemctl reload vykar
```

Check status and logs:

```bash
systemctl status vykar
journalctl -u vykar -f
```

### Docker

The default Docker entrypoint runs `vykar daemon`. See [Installing — Docker](install.md#docker) for container setup, volume mounts, and Docker Compose examples.

To enable the read-only status page in Docker, set `VYKAR_HTTP_LISTEN` (and `VYKAR_HTTP_ALLOW_PUBLIC=1` if binding to `0.0.0.0`) and publish port 7575 — the entrypoint and CMD do not need to change:

```bash
docker run -d --name vykar-daemon \
  -p 7575:7575 \
  -e VYKAR_HTTP_LISTEN=0.0.0.0:7575 \
  -e VYKAR_HTTP_ALLOW_PUBLIC=1 \
  -v /etc/vykar:/etc/vykar:ro \
  vykar
```

Compose equivalent:

```yaml
services:
  vykar:
    image: vykar
    environment:
      VYKAR_HTTP_LISTEN: "0.0.0.0:7575"
      VYKAR_HTTP_ALLOW_PUBLIC: "1"
    ports:
      - "7575:7575"
    volumes:
      - /etc/vykar:/etc/vykar:ro
```

To reload configuration in a running container:

```bash
docker kill --signal=HUP vykar-daemon
# or with Compose:
docker compose kill -s HUP vykar
```

To trigger an immediate backup:

```bash
docker kill --signal=USR1 vykar-daemon
# or with Compose:
docker compose kill -s USR1 vykar
```
