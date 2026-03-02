---
name: btrfs
description: "Validate Btrfs snapshot hooks pattern from vykar recipes"
---

# Btrfs Filesystem Snapshots

## Goal

Validate vykar's filesystem snapshot backup pattern for Btrfs using hooks:
1. Create a read-only snapshot before backup
2. Back up the snapshot path
3. Delete the snapshot after backup

Recipe reference: https://vykar.borgbase.com/recipes#filesystem-snapshots (Btrfs section)

## Safety Gate (REQUIRED)

Before touching any disk, confirm the test partition is safe:
1. Verify system/root disk is NOT the test partition:
   ```bash
   findmnt -no SOURCE /
   lsblk -f
   ```
2. Confirm the test partition holds only disposable test data
3. **Stop immediately** if any non-test data exists on the target partition

## Test Partition Setup

Use the dedicated test partition (no repartitioning needed):
```bash
sudo mkfs.btrfs -f /dev/<test_partition>
sudo mkdir -p /mnt/btrfs-test
sudo mount /dev/<test_partition> /mnt/btrfs-test
```

## Test Data

1. Create source path and snapshot parent:
   ```bash
   sudo mkdir -p /mnt/btrfs-test/.snapshots
   sudo btrfs subvolume create /mnt/btrfs-test/data
   ```
2. Seed representative files (200 files):
   ```bash
   sudo bash -c 'for i in $(seq 1 200); do echo "file-$i $(date -u +%s)" > /mnt/btrfs-test/data/file-$i.txt; done'
   ```

## Source Definition

Configure in vykar config:
```yaml
sources:
  - path: /mnt/btrfs-test/.snapshots/data-backup
    label: btrfs-data
    hooks:
      before: btrfs subvolume snapshot -r /mnt/btrfs-test/data /mnt/btrfs-test/.snapshots/data-backup
      after: btrfs subvolume delete /mnt/btrfs-test/.snapshots/data-backup
```

Use `sudo vykar` since the source path is root-owned.

## Run Matrix

1. `local` repository first
2. `rest` second (local `vykar-server`)
3. `s3` third
4. `sftp` optional and last (use `timeout` wrappers, skip if unstable)

## Validation

1. `vykar backup` exits 0
2. `vykar list` shows new snapshot for `btrfs-data`
3. `vykar --config <config> snapshot list -R <repo> <id>` includes seeded files
4. Hook cleanup verified:
   ```bash
   test ! -d /mnt/btrfs-test/.snapshots/data-backup
   ```
5. Restore to temp dir and verify file count matches

## Failure Cases to Explicitly Test

- Snapshot already exists before backup (name collision)
- Missing `.snapshots` directory
- `hooks.after` failure leaves snapshot behind
- Source data churn during backup (verify snapshot consistency)

## Common Issues

- Keep snapshot existence checks explicit after each backup
- Restore validation catches hook misconfiguration early
- Isolate SFTP failures from local/rest/s3 results

## Cleanup

1. Remove temp restore directories
2. Unmount test mount:
   ```bash
   sudo umount /mnt/btrfs-test
   ```
3. Clean remote paths with `rclone delete --rmdirs` between reruns
