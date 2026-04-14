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
SHORT_TIMEOUT_SECONDS = 120
LONG_TIMEOUT_SECONDS = 3600


def timeout_for_args(args: list[str]) -> int:
    """Return the default timeout for a vykar subcommand."""
    if not args:
        return SHORT_TIMEOUT_SECONDS

    command = args[0]
    if command in {"backup", "restore", "check", "compact", "prune", "delete"}:
        return LONG_TIMEOUT_SECONDS
    return SHORT_TIMEOUT_SECONDS


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
    timeout: int | None = None,
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
    resolved_timeout = timeout if timeout is not None else timeout_for_args(args)

    try:
        result = subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            env=env,
            timeout=resolved_timeout,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        timeout_msg = (
            f"Command timed out after {resolved_timeout} seconds: {' '.join(cmd)}"
        )
        if stderr:
            stderr = f"{stderr}\n{timeout_msg}"
        else:
            stderr = timeout_msg
        result = subprocess.CompletedProcess(
            args=cmd,
            returncode=124,
            stdout=stdout,
            stderr=stderr,
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
    result = run_vykar(vykar_bin, config_path, args, label="backup")
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
    return run_vykar(vykar_bin, config_path, args, label="check")


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


def _build_diff_args(corpus_dir: str, restore_dir: str) -> list[str]:
    diff_args = ["diff", "-qr"]
    if platform.system() != "Darwin":
        diff_args.append("--no-dereference")
    diff_args += [corpus_dir, restore_dir]
    return diff_args


def _run_diff(corpus_dir: str, restore_dir: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        _build_diff_args(corpus_dir, restore_dir),
        capture_output=True,
        text=True,
    )


def _diff_output(diff_result: subprocess.CompletedProcess) -> str:
    output = diff_result.stdout.strip()
    if output:
        return output
    error_output = diff_result.stderr.strip()
    if error_output:
        return error_output
    return f"diff exited with rc={diff_result.returncode} and produced no output"


def verify_restore(
    vykar_bin: str,
    config_path: str,
    repo_label: str,
    snapshot_id: str,
    corpus_dir: str,
    work_dir: str,
) -> tuple[bool, str, bool]:
    """Restore a snapshot and diff against the corpus.

    Returns (passed, detail_message, stop_scenario).
    """
    restore_dir = os.path.join(work_dir, "restore")
    if os.path.exists(restore_dir):
        shutil.rmtree(restore_dir)
    os.makedirs(restore_dir, exist_ok=True)

    result = vykar_restore(vykar_bin, config_path, repo_label, snapshot_id, restore_dir)
    if result.returncode != 0:
        shutil.rmtree(restore_dir, ignore_errors=True)
        return False, f"restore failed (rc={result.returncode}): {result.stderr[-500:]}", False

    diff_result = _run_diff(corpus_dir, restore_dir)
    if diff_result.returncode != 0:
        retry_result = _run_diff(corpus_dir, restore_dir)
    else:
        retry_result = None

    shutil.rmtree(restore_dir, ignore_errors=True)

    if diff_result.returncode == 0:
        return True, "restore matches corpus", False
    if retry_result is not None and retry_result.returncode == 0:
        return True, "restore matches corpus after diff retry", False
    failed_result = retry_result if retry_result is not None else diff_result
    return False, f"diff mismatch after retry:\n{_diff_output(failed_result)}", True


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
