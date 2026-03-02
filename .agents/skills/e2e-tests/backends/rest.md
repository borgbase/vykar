---
name: rest
description: "Validate vykar REST backend against a local vykar-server instance"
---

# REST Backend (Local Server) — Corpus Backup & Restore

## Goal

Validate vykar backup and restore correctness over the REST backend using a local `vykar-server` instance.

## Scope

- **Backend**: `rest` (URL and token from `~/vykar.sample.yaml`)
- **Source dataset**: `~/corpus-remote` (default), optionally `~/corpus-local` for stress
- **Verification**: restored tree matches source tree exactly

## Prerequisites

1. Ensure user service is running:
   ```bash
   systemctl --user enable --now vykar-server.service
   systemctl --user is-active vykar-server.service
   curl -fsS http://127.0.0.1:8585/health
   ```
2. Create config from `~/vykar.sample.yaml` with REST repo definition:
   - `url: "http://127.0.0.1:8585"` (single-repo mode)
   - `label: "rest"`
   - `access_token: "<token>"`
   - `allow_insecure_http: true`
3. `export VYKAR_PASSPHRASE=123`

## Local REST Cleanup (before each run)

Single-repo mode reuses one server-side repository. Reset server data directory between reruns (sandbox default):
```bash
rm -rf /mnt/repos/bench-vykar/vykar-server-data/*
```

## Test Procedure

1. Delete REST repo from previous runs (best effort):
   ```bash
   vykar --config <config> delete -R rest --yes-delete-this-repo || true
   ```
   In single-repo mode this may return HTTP 400/404; treat as non-fatal.
2. Initialize REST repo:
   ```bash
   vykar --config <config> init -R rest
   ```
3. Run backup:
   ```bash
   vykar --config <config> backup -R rest -l rest-corpus ~/corpus-remote
   ```
4. Confirm snapshot:
   ```bash
   vykar --config <config> list -R rest
   ```
5. Capture latest snapshot ID.
6. Restore to empty temp directory:
   ```bash
   vykar --config <config> restore -R rest <snapshot_id> <restore_dir>
   ```
7. Integrity check:
   ```bash
   vykar --config <config> check -R rest
   ```
8. Delete the tested snapshot:
   ```bash
   vykar --config <config> snapshot delete -R rest <snapshot_id>
   ```
9. Compact repository packs:
   ```bash
   vykar --config <config> compact -R rest
   ```

## Validation

1. Snapshot exists for label `rest-corpus`
2. Restore completes successfully
3. `diff -qr --no-dereference ~/corpus-remote <restore_dir>` reports no differences
4. `vykar snapshot ... delete <snapshot_id>` exits 0
5. `vykar compact` exits 0
6. Optional: SHA256 manifest comparison

## Failure Cases to Record

- REST auth or token mismatch (`401`)
- Request body limit errors (`413`) on larger uploads
- Server-side connection resets (`broken pipe`) during pack uploads
- Restore mismatch vs source
- `vykar check` failures
- `vykar snapshot delete` or `vykar compact` failures

## Cleanup

1. Remove restore temp directory
2. Keep logs under `~/runtime/logs/`
3. Keep report under `~/runtime/reports/`
