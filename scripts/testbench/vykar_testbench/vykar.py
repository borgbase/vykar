"""Subprocess wrappers for vykar CLI commands and verification helpers.

Merges and replaces:
- scripts/scenarios/scenario_runner/vykar.py
- scripts/scenarios/scenario_runner/verify.py
- drop_caches() from scripts/lib/common.sh and scripts/benchmarks/benchmark_runner/host.py
"""

import os
import platform
import re
import shutil
import subprocess
import sys


_SNAPSHOT_RE = re.compile(r"Snapshot created: (\S+)")


def _env_with_passphrase() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("VYKAR_PASSPHRASE", "123")
    return env


def _is_missing_repo_error(stderr: str) -> bool:
    return "no repository found at" in stderr.lower()


def run_vykar(
    vykar_bin: str,
    config_path: str,
    args: list[str],
    *,
    timeout: int = 120,
    label: str = "",
    allow_missing_repo: bool = False,
    passphrase: str | None = None,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    """Run a vykar CLI command."""
    cmd = [vykar_bin, "--config", config_path] + args
    env = _env_with_passphrase()
    if passphrase is not None:
        env["VYKAR_PASSPHRASE"] = passphrase

    result = subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        env=env,
        timeout=timeout,
    )
    missing_repo = allow_missing_repo and _is_missing_repo_error(result.stderr)
    if result.returncode != 0 and not missing_repo:
        print(
            f"[vykar {label or ' '.join(args)}] FAILED (rc={result.returncode})",
            file=sys.stderr,
        )
        if result.stderr:
            print(result.stderr[-2000:], file=sys.stderr)
    if missing_repo:
        return subprocess.CompletedProcess(
            args=result.args,
            returncode=0,
            stdout=result.stdout,
            stderr=result.stderr,
        )
    return result


def vykar_init(vykar_bin: str, config_path: str, repo_label: str) -> subprocess.CompletedProcess:
    return run_vykar(vykar_bin, config_path, ["init", "-R", repo_label], label="init")


def vykar_backup(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
    snapshot_label: str = "",
) -> tuple[subprocess.CompletedProcess, str | None]:
    """Run backup, return (result, snapshot_id or None)."""
    args = ["backup", "-R", repo_label]
    result = run_vykar(vykar_bin, config_path, args, timeout=3600, label="backup")
    snapshot_id = None
    if result.returncode == 0:
        snapshot_id = extract_snapshot_id(result.stdout)
    return result, snapshot_id


def vykar_list(vykar_bin: str, config_path: str, repo_label: str) -> subprocess.CompletedProcess:
    return run_vykar(vykar_bin, config_path, ["list", "-R", repo_label, "--last", "100"], label="list")


def vykar_restore(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
    snapshot_id: str,
    target_dir: str,
) -> subprocess.CompletedProcess:
    return run_vykar(
        vykar_bin,
        config_path,
        ["restore", "-R", repo_label, snapshot_id, target_dir],
        timeout=3600,
        label="restore",
    )


def vykar_check(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
    *,
    verify_data: bool = False,
) -> subprocess.CompletedProcess:
    args = ["check", "-R", repo_label]
    if verify_data:
        args.append("--verify-data")
    return run_vykar(vykar_bin, config_path, args, timeout=3600, label="check")


def vykar_prune(vykar_bin: str, config_path: str, repo_label: str) -> subprocess.CompletedProcess:
    return run_vykar(vykar_bin, config_path, ["prune", "-R", repo_label], label="prune")


def vykar_compact(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
    *,
    threshold: str = "0",
) -> subprocess.CompletedProcess:
    return run_vykar(
        vykar_bin,
        config_path,
        ["compact", "-R", repo_label, "--threshold", threshold],
        label="compact",
    )


def vykar_snapshot_delete(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
    snapshot_id: str,
) -> subprocess.CompletedProcess:
    return run_vykar(
        vykar_bin,
        config_path,
        ["snapshot", "delete", snapshot_id, "-R", repo_label],
        label="snapshot-delete",
    )


def vykar_delete_repo(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
) -> subprocess.CompletedProcess:
    return run_vykar(
        vykar_bin,
        config_path,
        ["delete", "-R", repo_label, "--yes-delete-this-repo"],
        label="delete-repo",
        allow_missing_repo=True,
    )


def vykar_break_lock(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
) -> subprocess.CompletedProcess:
    return run_vykar(vykar_bin, config_path, ["break-lock", "-R", repo_label], label="break-lock")


def extract_snapshot_id(stdout: str) -> str | None:
    """Extract snapshot ID from vykar backup stdout."""
    m = _SNAPSHOT_RE.search(stdout)
    return m.group(1) if m else None


def verify_restore(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
    snapshot_id: str,
    corpus_dir: str,
    work_dir: str,
) -> tuple[bool, str]:
    """Restore a snapshot and diff against the corpus.

    Returns (passed, detail_message).
    """
    restore_dir = os.path.join(work_dir, "restore")
    if os.path.exists(restore_dir):
        shutil.rmtree(restore_dir)
    os.makedirs(restore_dir, exist_ok=True)

    result = vykar_restore(vykar_bin, config_path, repo_label, snapshot_id, restore_dir)
    if result.returncode != 0:
        shutil.rmtree(restore_dir, ignore_errors=True)
        return False, f"restore failed (rc={result.returncode}): {result.stderr[-500:]}"

    diff_args = ["diff", "-qr"]
    if platform.system() != "Darwin":
        diff_args.append("--no-dereference")
    diff_args += [corpus_dir, restore_dir]

    diff_result = subprocess.run(diff_args, capture_output=True, text=True)

    shutil.rmtree(restore_dir, ignore_errors=True)

    if diff_result.returncode == 0:
        return True, "restore matches corpus"
    else:
        lines = diff_result.stdout.strip().splitlines()
        summary = "\n".join(lines[:20])
        if len(lines) > 20:
            summary += f"\n... and {len(lines) - 20} more differences"
        return False, f"diff mismatch:\n{summary}"


def drop_caches() -> None:
    """Drop OS page/dentry/inode caches. Requires Linux + passwordless sudo.

    Silently skips on macOS or if sudo is unavailable.
    """
    subprocess.run(["sync"], check=False)
    if not os.path.exists("/proc/sys/vm/drop_caches"):
        return
    if (
        subprocess.run(
            ["sudo", "-n", "true"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        != 0
    ):
        return
    subprocess.run(
        ["sudo", "-n", "tee", "/proc/sys/vm/drop_caches"],
        input="3\n",
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
