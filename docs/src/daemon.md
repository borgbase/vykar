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
PrivateTmp=true
PrivateDevices=true

# Passphrase via environment file (optional)
# EnvironmentFile=/etc/vykar/env

[Install]
WantedBy=multi-user.target
```

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

The default Docker entrypoint runs `vykar daemon`. See [Installing — Docker](install.md#docker) for container setup, volume mounts, and Docker Compose examples. To reload configuration in a running container:

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
