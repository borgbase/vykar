"""Subprocess wrappers for vykar CLI commands."""

import os
import re
import subprocess
import sys


_SNAPSHOT_RE = re.compile(r"Snapshot created: (\S+)")


def _env_with_passphrase() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("VYKAR_PASSPHRASE", "123")
    return env


def _run(vykar_bin: str, config_path: str, args: list[str], *,
         timeout: int = 120, label: str = "") -> subprocess.CompletedProcess:
    cmd = [vykar_bin, "--config", config_path] + args
    result = subprocess.run(cmd, capture_output=True, text=True,
                            env=_env_with_passphrase(), timeout=timeout)
    if result.returncode != 0:
        print(f"[vykar {label or ' '.join(args)}] FAILED (rc={result.returncode})", file=sys.stderr)
        if result.stderr:
            print(result.stderr[-2000:], file=sys.stderr)
    return result


def vykar_init(vykar_bin: str, config_path: str, repo_label: str) -> subprocess.CompletedProcess:
    return _run(vykar_bin, config_path, ["init", "-R", repo_label], label="init")


def vykar_backup(vykar_bin: str, config_path: str, repo_label: str,
                 snapshot_label: str = "") -> tuple[subprocess.CompletedProcess, str | None]:
    """Run backup, return (result, snapshot_id or None)."""
    args = ["backup", "-R", repo_label]
    if snapshot_label:
        args += ["-l", snapshot_label]
    result = _run(vykar_bin, config_path, args, timeout=3600, label="backup")
    snapshot_id = None
    if result.returncode == 0:
        m = _SNAPSHOT_RE.search(result.stdout)
        if m:
            snapshot_id = m.group(1)
    return result, snapshot_id


def vykar_list(vykar_bin: str, config_path: str, repo_label: str) -> subprocess.CompletedProcess:
    return _run(vykar_bin, config_path, ["list", "-R", repo_label, "--last", "100"], label="list")


def vykar_restore(vykar_bin: str, config_path: str, repo_label: str,
                  snapshot_id: str, target_dir: str) -> subprocess.CompletedProcess:
    return _run(vykar_bin, config_path,
                ["restore", "-R", repo_label, snapshot_id, target_dir],
                timeout=3600, label="restore")


def vykar_check(vykar_bin: str, config_path: str, repo_label: str) -> subprocess.CompletedProcess:
    return _run(vykar_bin, config_path, ["check", "-R", repo_label], timeout=3600, label="check")


def vykar_prune(vykar_bin: str, config_path: str, repo_label: str) -> subprocess.CompletedProcess:
    return _run(vykar_bin, config_path, ["prune", "-R", repo_label], label="prune")


def vykar_compact(vykar_bin: str, config_path: str, repo_label: str) -> subprocess.CompletedProcess:
    return _run(vykar_bin, config_path,
                ["compact", "-R", repo_label, "--threshold", "0"], label="compact")


def vykar_snapshot_delete(vykar_bin: str, config_path: str, repo_label: str,
                          snapshot_id: str) -> subprocess.CompletedProcess:
    return _run(vykar_bin, config_path,
                ["snapshot", "delete", snapshot_id, "-R", repo_label],
                label="snapshot-delete")


def vykar_delete_repo(vykar_bin: str, config_path: str,
                      repo_label: str) -> subprocess.CompletedProcess:
    return _run(vykar_bin, config_path,
                ["delete", "-R", repo_label, "--yes-delete-this-repo"],
                label="delete-repo")
