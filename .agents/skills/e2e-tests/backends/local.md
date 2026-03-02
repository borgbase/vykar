---
name: local
description: "Validate vykar local backend with full corpus backup and restore"
---

# Local Backend — Corpus Backup & Restore

## Goal

Validate vykar filesystem backup and restore correctness on the local backend using the large corpus dataset.

## Scope

- **Backend**: `local`
- **Source dataset**: `~/corpus-local`
- **Verification**: restored tree matches source tree exactly

## Prerequisites

1. Create config from `~/vykar.sample.yaml` with local repo path set to a writable location (e.g., `~/runtime/repos/local`)
2. `export VYKAR_PASSPHRASE=123`
3. Ensure enough free disk space for repository + restore directory

## Test Procedure

1. Clean local repo path:
   ```bash
   rm -rf ~/runtime/repos/local
   ```
2. Delete repo from previous runs (best effort):
   ```bash
   vykar --config <config> delete -R local --yes-delete-this-repo || true
   ```
3. Initialize repo:
   ```bash
   vykar --config <config> init -R local
   ```
4. Run backup:
   ```bash
   vykar --config <config> backup -R local -l local-corpus ~/corpus-local
   ```
5. Confirm snapshot:
   ```bash
   vykar --config <config> list -R local --last 3
   ```
6. Capture latest snapshot ID from output.
7. Restore into empty temp directory:
   ```bash
   vykar --config <config> restore -R local <snapshot_id> <restore_dir>
   ```
8. Integrity check:
   ```bash
   vykar --config <config> check -R local
   ```
9. Delete the tested snapshot:
   ```bash
   vykar --config <config> snapshot delete -R local <snapshot_id>
   ```
10. Compact repository packs:
   ```bash
   vykar --config <config> compact -R local
   ```

## Validation

1. Snapshot exists for label `local-corpus`
2. Restore command exits 0
3. `diff -qr --no-dereference ~/corpus-local <restore_dir>` produces no differences
4. `vykar snapshot ... delete <snapshot_id>` exits 0
5. `vykar compact` exits 0
6. Optional: compare sorted SHA256 manifests from both trees

## Failure Cases to Record

- Local repo path permission errors
- Insufficient disk space during backup or restore
- Restore completes but diff reports missing or changed files
- `vykar check` reports repository issues
- `vykar snapshot delete` or `vykar compact` fails

## Cleanup

1. Remove temporary restore directory
2. Keep logs under `~/runtime/logs/`
