"""Autonomous vykar stress test.

Python rewrite of scripts/stress.sh with the same CLI flags.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

from . import config as cfg
from . import vykar as vykar_cmd


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)


def _die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def _run_vykar_logged(
    vykar_bin: str,
    config_path: str,
    args: list[str],
    *,
    env: dict[str, str],
    log_file: str,
    time_v: bool = False,
    timeout: int = 120,
) -> tuple[int, str]:
    """Run vykar, capturing output to log_file. Returns (rc, log_file)."""
    cmd = [vykar_bin, "--config", config_path] + args
    if time_v:
        time_file = log_file.replace(".log", ".timev")
        full_cmd = ["/usr/bin/time", "-v", "-o", time_file] + cmd
    else:
        full_cmd = cmd

    with open(log_file, "w") as f:
        result = subprocess.run(
            full_cmd,
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )

    return result.returncode, log_file


def _extract_snapshot_id_from_file(log_file: str) -> str | None:
    """Parse snapshot ID from backup log output."""
    with open(log_file) as f:
        content = f.read()
    return vykar_cmd.extract_snapshot_id(content)


def _verify_restore_matches(corpus_dir: str, restored_dir: str) -> bool:
    """Diff corpus against restored directory."""
    result = subprocess.run(
        ["diff", "-qr", corpus_dir, restored_dir],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="stress",
        description="Autonomous vykar stress test against a corpus dataset.",
    )
    parser.add_argument("--iterations", type=int, default=1000, help="loop count (default: 1000)")
    parser.add_argument(
        "--check-every",
        type=int,
        default=50,
        help="run 'check' every N iterations; 0 disables (default: 50)",
    )
    parser.add_argument(
        "--verify-data-every",
        type=int,
        default=0,
        help="run 'check --verify-data' every N iters; 0 disables (default: 0)",
    )
    parser.add_argument(
        "--backend",
        choices=["local", "rest", "s3", "sftp"],
        default="local",
        help="storage backend (default: local)",
    )
    parser.add_argument(
        "--drop-caches",
        action="store_true",
        help="drop OS file caches before backup and restore",
    )
    parser.add_argument(
        "--time-v",
        action="store_true",
        help="capture /usr/bin/time -v per vykar step into logs",
    )

    args = parser.parse_args()

    if args.time_v and not os.path.isfile("/usr/bin/time"):
        _die("/usr/bin/time is required when --time-v is enabled")

    defaults = cfg.load_defaults()

    vykar_bin = shutil.which("vykar")
    if not vykar_bin:
        _die("vykar binary not found on PATH")
    vykar_bin = os.path.abspath(vykar_bin)

    corpus_dir = os.path.abspath(defaults.corpus_local)
    if not os.path.isdir(corpus_dir):
        _die(f"corpus directory not found: {corpus_dir}")
    if not os.listdir(corpus_dir):
        _die(f"corpus is empty: {corpus_dir}")

    repo_label = f"stress-{args.backend}"

    # Work directories
    stress_root = os.environ.get(
        "STRESS_ROOT",
        os.path.join(defaults.runtime_root, "stress", args.backend),
    )
    work_dir = os.path.join(stress_root, "work")
    repo_dir = os.path.join(work_dir, "repository")
    restore_dir = os.path.join(work_dir, "restore")
    runtime_dir = os.path.join(work_dir, "runtime")
    config_path = os.path.join(work_dir, "vykar.stress.yaml")
    log_dir = os.path.join(work_dir, "logs")
    home_dir = os.path.join(runtime_dir, "home")
    xdg_cache_dir = os.path.join(runtime_dir, "xdg-cache")

    # State tracking
    run_ok = False
    current_iter = 0
    current_step = "startup"
    current_snapshot = ""

    def cleanup() -> None:
        if args.backend == "local" and os.path.exists(repo_dir):
            shutil.rmtree(repo_dir, ignore_errors=True)
        for d in [restore_dir, runtime_dir]:
            if os.path.exists(d):
                shutil.rmtree(d, ignore_errors=True)
        if os.path.exists(config_path):
            os.unlink(config_path)
        if args.time_v and os.path.isdir(log_dir):
            _log(f"Preserving logs (--time-v enabled): {log_dir}")
        elif os.path.exists(log_dir):
            shutil.rmtree(log_dir, ignore_errors=True)

    def print_failure_context(exit_code: int) -> None:
        print("Failure context:", file=sys.stderr)
        print(f"  exit_code:      {exit_code}", file=sys.stderr)
        print(f"  iteration:      {current_iter}", file=sys.stderr)
        print(f"  step:           {current_step}", file=sys.stderr)
        print(f"  snapshot:       {current_snapshot or '<none>'}", file=sys.stderr)
        print(f"  repository_url: {repo_url}", file=sys.stderr)
        if args.backend == "local":
            print(f"  repository_dir: {repo_dir}", file=sys.stderr)

    # Clean ephemeral state
    cleanup()
    for d in [restore_dir, log_dir, home_dir, xdg_cache_dir]:
        os.makedirs(d, exist_ok=True)
    if args.backend == "local":
        os.makedirs(repo_dir, exist_ok=True)

    # Resolve repo URL — honor REPO_URL for any backend; default local to
    # the ephemeral work/repository dir (not the shared scenario repo).
    env_repo_url = os.environ.get("REPO_URL")
    if env_repo_url:
        repo_url = env_repo_url
    elif args.backend == "local":
        repo_url = repo_dir
    else:
        repo_url = cfg.resolve_repo_url(args.backend, repo_label, defaults)

    # Backend setup
    if args.backend == "s3":
        cfg.reset_minio(defaults)
        cfg.ensure_backend_ready("s3", repo_url, defaults)

    # Write config
    cfg.write_vykar_config(
        config_path,
        backend=args.backend,
        repo_label=repo_label,
        corpus_path=corpus_dir,
        repo_url=repo_url,
        defaults=defaults,
    )

    # Build env
    env = os.environ.copy()
    env["VYKAR_PASSPHRASE"] = defaults.passphrase
    env["HOME"] = home_dir
    env["XDG_CACHE_HOME"] = xdg_cache_dir

    _log(f"Stress backend={args.backend} repo_url={repo_url}")

    try:
        # Delete + init repo
        _log("Deleting repository before init")
        current_step = "delete-repo"
        rc, _ = _run_vykar_logged(
            vykar_bin, config_path,
            ["delete", "-R", repo_label, "--yes-delete-this-repo"],
            env=env,
            log_file=os.path.join(log_dir, "iter-000000-delete-repo.log"),
        )
        # Tolerate failure (repo may not exist)

        _log("Initializing repository")
        current_step = "init"
        rc, _ = _run_vykar_logged(
            vykar_bin, config_path,
            ["init", "-R", repo_label],
            env=env,
            log_file=os.path.join(log_dir, "iter-000000-init.log"),
            time_v=args.time_v,
        )
        if rc != 0:
            _die("init failed")

        _log(f"Starting stress run iterations={args.iterations}")

        # Counters
        backups = lists = restores = deletes = compacts = prunes = break_locks = checks = verify_checks = 0
        start_ts = time.time()

        for i in range(1, args.iterations + 1):
            current_iter = i
            current_snapshot = ""
            iter_prefix = os.path.join(log_dir, f"iter-{i:06d}")

            # Optional cache drop pre-backup
            if args.drop_caches:
                _log(f"[{i}/{args.iterations}] drop caches (pre-backup)")
                current_step = "drop-caches-pre-backup"
                vykar_cmd.drop_caches()

            # Break-lock for remote backends
            if args.backend in ("rest", "s3", "sftp"):
                _log(f"[{i}/{args.iterations}] break-lock")
                current_step = "break-lock"
                rc, _ = _run_vykar_logged(
                    vykar_bin, config_path,
                    ["break-lock", "-R", repo_label],
                    env=env,
                    log_file=f"{iter_prefix}-break-lock.log",
                    time_v=args.time_v,
                )
                if rc != 0:
                    _die(f"break-lock failed at iteration {i}")
                break_locks += 1

            # Backup
            _log(f"[{i}/{args.iterations}] backup")
            current_step = "backup"
            backup_args = ["backup", "-R", repo_label]
            if args.backend == "rest":
                backup_args.extend(["--connections", "6"])
            rc, backup_log = _run_vykar_logged(
                vykar_bin, config_path,
                backup_args,
                env=env,
                log_file=f"{iter_prefix}-backup.log",
                time_v=args.time_v,
                timeout=3600,
            )
            if rc != 0:
                _die(f"backup failed at iteration {i}")
            snapshot = _extract_snapshot_id_from_file(backup_log)
            if not snapshot:
                _die(f"failed to parse snapshot ID at iteration {i}")
            current_snapshot = snapshot
            backups += 1

            # List
            _log(f"[{i}/{args.iterations}] list (snapshot {snapshot})")
            current_step = "list"
            rc, list_log = _run_vykar_logged(
                vykar_bin, config_path,
                ["list", "-R", repo_label, "--last", "20"],
                env=env,
                log_file=f"{iter_prefix}-list.log",
                time_v=args.time_v,
            )
            if rc != 0:
                _die(f"list failed at iteration {i}")
            with open(list_log) as f:
                if snapshot not in f.read():
                    _die(f"snapshot '{snapshot}' missing from list output at iteration {i}")
            lists += 1

            # Restore
            restore_target = os.path.join(restore_dir, "current")
            if os.path.exists(restore_target):
                shutil.rmtree(restore_target)
            os.makedirs(restore_target, exist_ok=True)

            if args.drop_caches:
                _log(f"[{i}/{args.iterations}] drop caches (pre-restore)")
                current_step = "drop-caches-pre-restore"
                vykar_cmd.drop_caches()

            _log(f"[{i}/{args.iterations}] restore")
            current_step = "restore"
            rc, _ = _run_vykar_logged(
                vykar_bin, config_path,
                ["restore", "-R", repo_label, snapshot, restore_target],
                env=env,
                log_file=f"{iter_prefix}-restore.log",
                time_v=args.time_v,
                timeout=3600,
            )
            if rc != 0:
                _die(f"restore failed at iteration {i}")

            # Verify
            _log(f"[{i}/{args.iterations}] verify")
            current_step = "verify"
            if not _verify_restore_matches(corpus_dir, restore_target):
                _die(f"restore verification failed at iteration {i}")
            restores += 1

            # Delete snapshot
            _log(f"[{i}/{args.iterations}] delete")
            current_step = "delete"
            rc, _ = _run_vykar_logged(
                vykar_bin, config_path,
                ["snapshot", "delete", snapshot, "-R", repo_label],
                env=env,
                log_file=f"{iter_prefix}-delete.log",
                time_v=args.time_v,
            )
            if rc != 0:
                _die(f"snapshot delete failed at iteration {i}")
            deletes += 1

            # Compact
            _log(f"[{i}/{args.iterations}] compact")
            current_step = "compact"
            rc, _ = _run_vykar_logged(
                vykar_bin, config_path,
                ["compact", "-R", repo_label, "--threshold", "0"],
                env=env,
                log_file=f"{iter_prefix}-compact.log",
                time_v=args.time_v,
            )
            if rc != 0:
                _die(f"compact failed at iteration {i}")
            compacts += 1

            # Prune
            _log(f"[{i}/{args.iterations}] prune")
            current_step = "prune"
            rc, _ = _run_vykar_logged(
                vykar_bin, config_path,
                ["prune", "-R", repo_label],
                env=env,
                log_file=f"{iter_prefix}-prune.log",
                time_v=args.time_v,
            )
            if rc != 0:
                _die(f"prune failed at iteration {i}")
            prunes += 1

            # Periodic check
            if args.check_every > 0 and i % args.check_every == 0:
                _log(f"[{i}/{args.iterations}] check")
                current_step = "check"
                rc, _ = _run_vykar_logged(
                    vykar_bin, config_path,
                    ["check", "-R", repo_label],
                    env=env,
                    log_file=f"{iter_prefix}-check.log",
                    time_v=args.time_v,
                    timeout=3600,
                )
                if rc != 0:
                    _die(f"check failed at iteration {i}")
                checks += 1

            # Periodic check --verify-data
            if args.verify_data_every > 0 and i % args.verify_data_every == 0:
                _log(f"[{i}/{args.iterations}] check --verify-data")
                current_step = "check-verify-data"
                rc, _ = _run_vykar_logged(
                    vykar_bin, config_path,
                    ["check", "-R", repo_label, "--verify-data"],
                    env=env,
                    log_file=f"{iter_prefix}-check-data.log",
                    time_v=args.time_v,
                    timeout=3600,
                )
                if rc != 0:
                    _die(f"check --verify-data failed at iteration {i}")
                verify_checks += 1

            elapsed = int(time.time() - start_ts)
            _log(f"[{i}/{args.iterations}] done ({elapsed}s elapsed)")

        elapsed = int(time.time() - start_ts)
        _log("Stress run complete")
        print("Summary:")
        print(f"  iterations:          {args.iterations}")
        print(f"  backups:             {backups}")
        print(f"  lists:               {lists}")
        print(f"  restores:            {restores}")
        print(f"  deletes:             {deletes}")
        print(f"  compacts:            {compacts}")
        print(f"  prunes:              {prunes}")
        print(f"  break-lock:          {break_locks}")
        print(f"  check:               {checks}")
        print(f"  check --verify-data: {verify_checks}")
        print(f"  elapsed_sec:         {elapsed}")

        current_step = "complete"
        run_ok = True

    except SystemExit:
        raise
    except Exception as exc:
        print_failure_context(1)
        _log(f"Run failed: {exc}")
        _log("Preserving artifacts for debugging")
        sys.exit(1)
    finally:
        if run_ok:
            cleanup()
        else:
            if current_step != "complete":
                print_failure_context(1)
                _log("Run failed; preserving artifacts for debugging")
