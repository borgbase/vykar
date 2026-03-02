---
name: sftp
description: "Validate vykar SFTP backend with timeout handling for connectivity instability"
---

# SFTP Backend — Corpus Backup & Restore

## Goal

Validate vykar backup and restore correctness on the SFTP backend while handling known connectivity instability.

## Scope

- **Backend**: `sftp` (credentials and host from `~/vykar.sample.yaml`)
- **Source dataset**: `~/corpus-remote`
- **Verification**: restored tree matches source tree exactly

## Prerequisites

1. Create config from `~/vykar.sample.yaml` with SFTP repo definition
2. `export VYKAR_PASSPHRASE=123`
3. Confirm SSH key auth works from host
4. Ensure `rclone` is configured for SFTP cleanup

## Timeout Strategy

SFTP connectivity can be intermittent even when rclone works against the same server. Wrap all vykar commands with `timeout` to prevent indefinite hangs:
- Init/list/check: `timeout 120s`
- Backup/restore: `timeout 3600s`

Exit code 124 means timeout — mark test as **BLOCKED**.

## Remote Cleanup (before each run)

```bash
timeout 180 rclone delete <sftp_remote:path> --rmdirs
```
- If path is absent, treat `directory not found` as non-fatal

## Test Procedure

1. Delete repo from previous runs (best effort):
   ```bash
   timeout 180 vykar --config <config> delete -R sftp --yes-delete-this-repo || true
   ```
2. Initialize repo:
   ```bash
   timeout 120 vykar --config <config> init -R sftp
   ```
3. Run backup:
   ```bash
   timeout 3600 vykar --config <config> backup -R sftp -l remote-corpus ~/corpus-remote
   ```
4. Confirm snapshot:
   ```bash
   timeout 120 vykar --config <config> list -R sftp --last 3
   ```
5. Capture latest snapshot ID.
6. Restore to empty temp directory:
   ```bash
   timeout 3600 vykar --config <config> restore -R sftp <snapshot_id> <restore_dir>
   ```
7. Integrity check:
   ```bash
   timeout 300 vykar --config <config> check -R sftp
   ```
8. Delete the tested snapshot:
   ```bash
   timeout 300 vykar --config <config> snapshot delete -R sftp <snapshot_id>
   ```
9. Compact repository packs:
   ```bash
   timeout 300 vykar --config <config> compact -R sftp
   ```

## Validation

1. Snapshot exists for label `remote-corpus`
2. Restore exits 0 (not timeout 124)
3. `diff -qr --no-dereference ~/corpus-remote <restore_dir>` reports no differences
4. `vykar snapshot ... delete <snapshot_id>` exits 0 (not timeout 124)
5. `vykar compact` exits 0 (not timeout 124)
6. Optional: SHA256 manifest comparison

## Failure Cases to Record

- Init/list/backup timeouts
- Connection retries that never converge
- Restore mismatch vs source
- Repository check failures
- `vykar snapshot delete` or `vykar compact` failures/timeouts
- False lock detection (locks/ directory empty but lock error reported)

## Cleanup

1. Remove restore temp directory
2. Clean SFTP path with `rclone delete --rmdirs` between reruns
3. Preserve timeout logs for troubleshooting
4. Ensure no stuck `vykar` process remains after aborted steps (`pkill -f '^vykar( |$)'` if needed)
