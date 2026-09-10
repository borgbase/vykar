# Backup Recipes

Vykar provides hooks, command dumps, and source directories as universal building blocks. Rather than adding dedicated flags for each database or container runtime, the same patterns work for any application.

These recipes are starting points — adapt the commands to your setup.


## Databases

Databases should never be backed up by copying their data files while running. Use the database's own dump tool to produce a consistent export.

Where possible, use **command dumps** — they stream stdout directly into the backup without temporary files. For tools that can't stream to stdout, use **hooks** to dump to a temporary directory, back it up, then clean up.


### PostgreSQL

```yaml
sources:
  - label: postgres
    command_dumps:
      - name: mydb.dump
        command: "pg_dump -U myuser -Fc mydb"
```

For all databases at once:

```yaml
sources:
  - label: postgres
    command_dumps:
      - name: all.sql
        command: "pg_dumpall -U postgres"
```

If you need to run additional steps around the dump (e.g. custom authentication, pre/post scripts), use hooks instead. Note that this saves the dump to disk instead of reading it directly with the `command_dump` feature.

```yaml
sources:
  - label: postgres
    path: /var/backups/postgres
    hooks:
      before: >
        mkdir -p /var/backups/postgres &&
        pg_dump -U myuser -Fc mydb > /var/backups/postgres/mydb.dump
      after: "rm -rf /var/backups/postgres"
```


### MySQL / MariaDB

```yaml
sources:
  - label: mysql
    command_dumps:
      - name: all.sql
        command: "mysqldump -u root -p\"$MYSQL_ROOT_PASSWORD\" --all-databases"
```

With hooks:

```yaml
sources:
  - label: mysql
    path: /var/backups/mysql
    hooks:
      before: >
        mkdir -p /var/backups/mysql &&
        mysqldump -u root -p"$MYSQL_ROOT_PASSWORD" --all-databases
        > /var/backups/mysql/all.sql
      after: "rm -rf /var/backups/mysql"
```


### MongoDB

```yaml
sources:
  - label: mongodb
    command_dumps:
      - name: mydb.archive.gz
        command: "mongodump --archive --gzip --db mydb"
```

For all databases, omit `--db`:

```yaml
sources:
  - label: mongodb
    command_dumps:
      - name: all.archive.gz
        command: "mongodump --archive --gzip"
```


### SQLite

SQLite can't stream to stdout, so use a hook. Copying the database file directly risks corruption if a process holds a write lock.

```yaml
sources:
  - label: app-database
    path: /var/backups/sqlite
    hooks:
      before: >
        mkdir -p /var/backups/sqlite &&
        sqlite3 /var/lib/myapp/app.db ".backup '/var/backups/sqlite/app.db'"
      after: "rm -rf /var/backups/sqlite"
```


### Redis

```yaml
sources:
  - label: redis
    path: /var/backups/redis
    hooks:
      before: >
        mkdir -p /var/backups/redis &&
        redis-cli BGSAVE &&
        sleep 2 &&
        cp /var/lib/redis/dump.rdb /var/backups/redis/dump.rdb
      after: "rm -rf /var/backups/redis"
```

The `sleep` gives Redis time to finish the background save. For large datasets, check `redis-cli LASTSAVE` in a loop instead.


## Docker and Containers

The same patterns work for containerized applications. Use `docker exec` for command dumps and hooks, or back up Docker volumes directly from the host.

These examples use Docker, but the same approach works with Podman or any other container runtime.


### Docker volumes (static data)

For volumes that hold files not actively written to by a running process — configuration, uploaded media, static assets — back up the host path directly.

```yaml
sources:
  - label: myapp
    path: /var/lib/docker/volumes/myapp_data/_data
```

> **Note:** The default volume path `/var/lib/docker/volumes/` applies to standard Docker installs on Linux. It differs for Docker Desktop on macOS/Windows, rootless Docker, Podman (`/var/lib/containers/storage/volumes/` for root, `~/.local/share/containers/storage/volumes/` for rootless), and custom `data-root` configurations. Run `docker volume inspect <n>` or `podman volume inspect <n>` to find the actual path.


### Docker volumes with brief downtime

For applications that write to the volume but can tolerate a short stop, stop the container during backup.

```yaml
sources:
  - label: wiki
    path: /var/lib/docker/volumes/wiki_data/_data
    hooks:
      before: "docker stop wiki"
      finally: "docker start wiki"
```


### Database containers

Use command dumps with `docker exec` to stream database exports directly from a container.

**PostgreSQL in Docker:**

```yaml
sources:
  - label: app-database
    command_dumps:
      - name: mydb.dump
        command: "docker exec my-postgres pg_dump -U myuser -Fc mydb"
```

**MySQL / MariaDB in Docker:**

```yaml
sources:
  - label: app-database
    command_dumps:
      - name: mydb.sql
        command: "docker exec my-mysql mysqldump -u root -p\"$MYSQL_ROOT_PASSWORD\" mydb"
```

**MongoDB in Docker:**

```yaml
sources:
  - label: app-database
    command_dumps:
      - name: mydb.archive.gz
        command: "docker exec my-mongo mongodump --archive --gzip --db mydb"
```


### Multiple containers

Use separate source entries so each service gets its own label, retention policy, and hooks.

```yaml
sources:
  - label: nginx
    path: /var/lib/docker/volumes/nginx_config/_data
    retention:
      keep_daily: 7

  - label: app-database
    command_dumps:
      - name: mydb.dump
        command: "docker exec my-postgres pg_dump -U myuser -Fc mydb"
    retention:
      keep_daily: 30

  - label: uploads
    path: /var/lib/docker/volumes/uploads/_data
```


## Virtual Machine Disk Images

Virtual machine disk images are an excellent use case for deduplicated backups. Large portions of a VM's disk remain unchanged between snapshots, so Vykar's content-defined chunking achieves high deduplication ratios — often reducing storage to a fraction of the raw image size.

### Prerequisites

The guest VM must have the QEMU guest agent installed and running, and QEMU must be started with a guest agent socket (e.g. `-chardev socket,path=/tmp/qga.sock,server=on,wait=off,id=qga0`). Install `socat` on the host if not already present.


### Freeze, Backup, Thaw

Use hooks to freeze the guest filesystem before backing up the disk image, then thaw it afterwards:

```yaml
sources:
  - label: vm-images
    path: /var/lib/libvirt/images
    hooks:
      before: >
        echo '{"execute":"guest-fsfreeze-freeze"}' |
        socat - unix-connect:/tmp/qga.sock
      finally: >
        echo '{"execute":"guest-fsfreeze-thaw"}' |
        socat - unix-connect:/tmp/qga.sock
```

The freeze ensures the filesystem is in a clean state while Vykar reads the image. For incremental backups (every run after the first), only changed chunks are processed, so the freeze window is short.

### Tips

- **Raw images dedup better than qcow2.** The qcow2 format uses internal copy-on-write structures that can shuffle data, reducing byte-level similarity between snapshots. If practical, convert with `qemu-img convert -f qcow2 -O raw`.
- **Multiple VMs in one repo** provides cross-VM deduplication. VMs running the same OS share many common chunks.
- For environments that cannot tolerate any guest I/O pause, use QEMU external snapshots instead. This redirects writes to an overlay file via QMP `blockdev-snapshot-sync`, allowing the base image to be backed up with zero interruption. This is the approach used by Proxmox VE and libvirt.


## Filesystem Snapshots

For filesystems that support snapshots, the safest approach is to snapshot first, back up the snapshot, then delete it. This gives you a consistent point-in-time view without stopping any services.


### Btrfs

```yaml
sources:
  - label: data
    path: /mnt/.snapshots/data-backup
    hooks:
      before: "btrfs subvolume snapshot -r /mnt/data /mnt/.snapshots/data-backup"
      after:  "btrfs subvolume delete /mnt/.snapshots/data-backup"
```

The snapshot parent directory (`/mnt/.snapshots/`) must exist before the first backup. Create it once:

```bash
mkdir -p /mnt/.snapshots
```


### ZFS

```yaml
sources:
  - label: data
    path: /tank/data/.zfs/snapshot/vykar-tmp
    hooks:
      before: "zfs snapshot tank/data@vykar-tmp"
      after:  "zfs destroy tank/data@vykar-tmp"
```

> **Important:** The `.zfs/snapshot` directory is only accessible if `snapdir` is set to `visible` on the dataset. This is not the default. Set it before using this recipe:
>
> ```bash
> zfs set snapdir=visible tank/data
> ```


### LVM

```yaml
sources:
  - label: data
    path: /mnt/lvm-snapshot
    hooks:
      before: >
        lvcreate -s -n vykar-snap -L 5G /dev/vg0/data &&
        mkdir -p /mnt/lvm-snapshot &&
        mount -o ro /dev/vg0/vykar-snap /mnt/lvm-snapshot
      after: >
        umount /mnt/lvm-snapshot &&
        lvremove -f /dev/vg0/vykar-snap
```

Set the snapshot size (`-L 5G`) large enough to hold changes during the backup.


## Low-Resource Background Backup

If backups should run in the background with minimal impact on interactive work, use conservative resource limits. This will usually increase backup duration.

```yaml
compression:
  algorithm: lz4

limits:
  threads: 1
  nice: 19
  connections: 1
  upload_mib_per_sec: 2
  download_mib_per_sec: 4
```

- `threads: 1` keeps backup transforms mostly sequential.
- `nice: 19` lowers CPU scheduling priority on Unix; it is ignored on Windows.
  The value is applied to every thread and never lowered again (that needs
  `CAP_SYS_NICE` or a raised `RLIMIT_NICE` on Linux, root on macOS), so the
  daemon and GUI keep it for the rest of their lifetime, and with several
  repositories the highest configured value wins. To keep the daemon or GUI at
  its own priority between runs, leave `nice` at `0` and run backups as separate
  `vykar backup` processes under `nice -n 19` or a systemd `Nice=` setting.
- `connections: 1` minimizes backend parallelism (SFTP pool, upload concurrency, restore readers).
- `upload_mib_per_sec` and `download_mib_per_sec` cap backend throughput in MiB/s.
- If this is too slow, raise `upload_mib_per_sec` first, then increase `connections`.


## Network-Aware Backups

A `before_backup` hook that exits non-zero skips the backup. This lets you restrict backups to specific networks without any changes to Vykar itself.


### WiFi SSID filtering

Only run backups when connected to a specific WiFi network.

**macOS:**

```yaml
hooks:
  before_backup: >-
    networksetup -getairportnetwork en0
    | grep -q 'HomeNetwork'
```

**Linux (NetworkManager):**

```yaml
hooks:
  before_backup: >-
    nmcli -t -f active,ssid dev wifi
    | grep -q '^yes:HomeNetwork$'
```

Multiple allowed SSIDs:

```yaml
hooks:
  before_backup: >-
    nmcli -t -f active,ssid dev wifi
    | grep -qE '^yes:(HomeNetwork|OfficeNetwork)$'
```

Inverted logic — run on any network *except* a blocklist:

```yaml
hooks:
  before_backup: >-
    ! nmcli -t -f active,ssid dev wifi
    | grep -q '^yes:CoffeeShopWiFi$'
```


### Metered network detection

Android hotspots and tethered connections advertise metered status via DHCP. Linux network managers read this automatically, so you can skip backups on metered connections without maintaining an SSID list.

**NetworkManager:**

```yaml
hooks:
  before_backup: >-
    METERED=$(nmcli -t -f GENERAL.METERED dev show
    | grep -m1 GENERAL.METERED
    | cut -d: -f2);
    [ "$METERED" != "yes" ] && [ "$METERED" != "guess-yes" ]
```

NetworkManager reports four values: `yes` (explicitly metered), `guess-yes` (heuristic, e.g. Android hotspot), `no`, and `unknown`. The hook above skips on both `yes` and `guess-yes`.

**systemd-networkd:**

```yaml
hooks:
  before_backup: >-
    ! networkctl status
    | grep -qi 'metered.*yes'
```

> **Note:** macOS has no CLI-exposed metered attribute. Use SSID filtering instead.


## Monitoring

Vykar hooks can notify monitoring services on success or failure. A `curl` in an `after` hook replaces the need for dedicated integrations.


### Apprise (multi-service)

[Apprise](https://github.com/caronc/apprise) sends notifications to 100+ services (Gotify, Slack, Discord, Telegram, ntfy, email, and more) from the command line. Since vykar hooks run arbitrary shell commands, you can use the `apprise` CLI directly — no built-in integration needed.

Install it with:

```bash
pip install apprise
```

If you use the Docker image, the `apprise` variant has it pre-installed — use the `latest-apprise` tag (or e.g. `0.12.6-apprise`). See [Docker installation](install.md#docker).

```yaml
hooks:
  after_backup:
    - >-
      apprise -t "Backup complete"
      -b "vykar {command} finished for {repository}"
      "gotify://hostname/token"
      "slack://tokenA/tokenB/tokenC"
  failed:
    - >-
      apprise -t "Backup failed"
      -b "vykar {command} failed for {repository}: {error}"
      "gotify://hostname/token"
```

Common service URL examples:

| Service  | URL format                                          |
|----------|-----------------------------------------------------|
| Gotify   | `gotify://hostname/token`                           |
| Slack    | `slack://tokenA/tokenB/tokenC`                      |
| Discord  | `discord://webhook_id/webhook_token`                |
| Telegram | `tgram://bot_token/chat_id`                         |
| ntfy     | `ntfy://topic`                                      |
| Email    | `mailto://user:pass@gmail.com`                      |

You can pass multiple URLs in a single command to notify several services at once. See the [Apprise wiki](https://github.com/caronc/apprise/wiki) for the full list of supported services and URL formats.


### Healthchecks

[Healthchecks](https://healthchecks.io/) alerts you when backups stop arriving. Ping the check URL after each successful backup.

```yaml
hooks:
  after: "curl -fsS -m 10 --retry 5 https://hc-ping.com/your-uuid-here"
```

To report failures too, use separate success and failure URLs:

```yaml
hooks:
  after: "curl -fsS -m 10 --retry 5 https://hc-ping.com/your-uuid-here"
  failed: "curl -fsS -m 10 --retry 5 https://hc-ping.com/your-uuid-here/fail"
```


### ntfy

[ntfy](https://ntfy.sh/) sends push notifications to your phone. Useful for immediate failure alerts.

```yaml
hooks:
  failed: >
    curl -fsS -m 10
    -H "Title: Backup failed"
    -H "Priority: high"
    -H "Tags: warning"
    -d "vykar backup failed on $(hostname)"
    https://ntfy.sh/my-backup-alerts
```


### Uptime Kuma

[Uptime Kuma](https://github.com/louislam/uptime-kuma) is a self-hosted monitoring tool. Use a push monitor to track backup runs.

```yaml
hooks:
  after: "curl -fsS -m 10 http://your-kuma-instance:3001/api/push/your-token?status=up"
```


### Generic webhook

Any service that accepts HTTP requests works the same way.

```yaml
hooks:
  after: >
    curl -fsS -m 10 -X POST
    -H "Content-Type: application/json"
    -d '{"text": "Backup completed on $(hostname)"}'
    https://hooks.slack.com/services/your/webhook/url
```
