---
name: zfs
description: "Validate ZFS snapshot hooks pattern from vykar recipes"
---

# ZFS Filesystem Snapshots

## Goal

Validate vykar's filesystem snapshot backup pattern for ZFS using hooks:
1. Create a dataset snapshot before backup
2. Back up the `.zfs/snapshot/...` path
3. Destroy the snapshot after backup

Recipe reference: https://vykar.borgbase.com/recipes#filesystem-snapshots (ZFS section)

## Safety Gate (REQUIRED)

Before touching any disk, confirm the test partition is safe:
1. Verify system/root disk is NOT the test partition:
   ```bash
   findmnt -no SOURCE /
   lsblk -f
   ```
2. Confirm the test partition holds only disposable test data
3. **Stop immediately** if any non-test data exists on the target partition

## Prerequisite Install

If ZFS packages are missing:
```bash
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y linux-headers-$(uname -r) zfs-dkms zfsutils-linux
sudo DEBIAN_FRONTEND=noninteractive dpkg --configure -a
sudo modprobe zfs
```

Use `DEBIAN_FRONTEND=noninteractive` to avoid `zfs-dkms` TTY prompt blocking automation. DKMS module build can take several minutes.

## Test Partition Setup

Use the dedicated test partition. If previously formatted for another FS (e.g., Btrfs), unmount first:
```bash
sudo zpool create -f vykerpool /dev/<test_partition>
sudo zfs create vykerpool/data
sudo zfs set snapdir=visible vykerpool/data   # CRITICAL — without this, .zfs/snapshot/ is inaccessible
```

Default mountpoint is typically `/vykerpool/data`.

## Test Data

Seed representative files (200 files) under the dataset mountpoint:
```bash
sudo bash -c 'for i in $(seq 1 200); do echo "zfs-file-$i $(date -u +%s)" > /vykerpool/data/file-$i.txt; done'
```

## Source Definition

Configure in vykar config:
```yaml
sources:
  - path: /vykerpool/data/.zfs/snapshot/vykar-tmp
    label: zfs-data
    hooks:
      before: zfs snapshot vykerpool/data@vykar-tmp
      after: zfs destroy vykerpool/data@vykar-tmp
```

Use `sudo vykar` since the source path is root-owned.

## Run Matrix

1. `local` repository first
2. `rest` second (local `vykar-server`)
3. `s3` third
4. `sftp` optional and last (use `timeout` wrappers, skip if unstable)

## Validation

1. `vykar backup` exits 0
2. `vykar list` shows new snapshot for `zfs-data`
3. `vykar --config <config> snapshot list -R <repo> <id>` includes seeded files
4. Hook cleanup verified:
   ```bash
   sudo zfs list -t snapshot | grep 'vykerpool/data@vykar-tmp'
   # Should return no match
   ```
5. Restore to temp dir and verify content matches, ignoring the virtual `.zfs` path:
   ```bash
   diff -qr --no-dereference --exclude='.zfs' /vykerpool/data <restore_dir>
   ```

## Failure Cases to Explicitly Test

- Snapshot name collision (`vykerpool/data@vykar-tmp` already exists)
- `snapdir` hidden (path inaccessible at `.zfs/snapshot/...`)
- `hooks.after` failure leaves snapshot present
- Source data churn during backup (verify snapshot consistency)

## Common Issues

- **Always** verify `snapdir=visible` before running backups — without it, the snapshot path is invisible
- Keep explicit snapshot existence checks after each run
- Isolate SFTP failures from local/rest/s3 results
- The live dataset contains a virtual `.zfs` directory that should be excluded from restore diffs
- After `zpool destroy`, partition may still show `zfs_member` — next FS test can safely overwrite

## Cleanup

1. Remove temp restore directories
2. Destroy dataset and pool:
   ```bash
   sudo zfs destroy -r vykerpool/data || true
   sudo zpool destroy vykerpool || true
   ```
3. Clean remote paths with `rclone delete --rmdirs` between reruns
