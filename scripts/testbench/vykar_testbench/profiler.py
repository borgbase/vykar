"""Profiling orchestration for vykar CLI.

Python rewrite of scripts/profile.sh with the same CLI flags.
"""

import argparse
import glob
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

from . import config as cfg
from . import vykar as vykar_cmd


def _log(msg: str) -> None:
    print(msg, flush=True)


def _die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def _need(cmd: str) -> None:
    if shutil.which(cmd) is None:
        _die(f"missing required command: {cmd}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="profile",
        description="Build vykar with profiling profile and run heaptrack/perf profiling.",
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=["backup", "restore", "compact", "prune", "check"],
        help="profiling mode",
    )
    parser.add_argument(
        "--backend",
        choices=["local", "rest", "s3"],
        default="local",
        help="storage backend (default: local)",
    )
    parser.add_argument("--source", default=None, help="source path to back up (default: $CORPUS_LOCAL)")
    parser.add_argument(
        "--profiler",
        choices=["heaptrack", "perf", "both"],
        default="heaptrack",
        help="profiler to use (default: heaptrack)",
    )
    parser.add_argument("--skip-build", action="store_true", help="skip cargo build")
    parser.add_argument("--no-drop-caches", action="store_true", help="do not drop caches before profiling")
    parser.add_argument("--dry-run", action="store_true", help="enable dry-run for compact/prune")
    parser.add_argument("--verify-data", action="store_true", help="include --verify-data for check")
    parser.add_argument(
        "--compact-threshold",
        default="10",
        help="compact threshold percentage (default: 10)",
    )

    args = parser.parse_args()

    defaults = cfg.load_defaults()
    source_path = args.source or defaults.corpus_local
    if not os.path.isdir(source_path):
        _die(f"source not found: {source_path}")

    seed_source_path = os.path.join(source_path, "snapshot-1")
    if args.mode in ("backup", "compact") and not os.path.isdir(seed_source_path):
        _die(f"seed source not found: {seed_source_path}")

    # Repo root for cargo build
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Walk up from vykar_testbench/ -> testbench/ -> scripts/ -> repo_root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))

    # Tool checks
    _need("cargo")
    _need("perl")
    _need("git")
    if args.profiler in ("heaptrack", "both"):
        _need("heaptrack")
        _need("heaptrack_print")
    if args.profiler in ("perf", "both"):
        _need("perf")

    # Constants
    repo_label = f"profile-{args.backend}"
    snapshot_label = "corpus-profile"
    perf_events = os.environ.get("PERF_EVENTS", "")
    perf_record_freq = 99
    cost_type = "peak"

    # Output layout
    out_root = os.path.join(defaults.runtime_root, "heaptrack", "reports")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_root = os.path.join(out_root, stamp)
    run_dir = os.path.join(run_root, args.mode)
    os.makedirs(run_dir, exist_ok=True)

    heaptrack_data_template = os.path.join(run_dir, f"heaptrack.vykar.{stamp}.%p.gz")
    analysis_txt = os.path.join(run_dir, "heaptrack.analysis.txt")
    stacks_txt = os.path.join(run_dir, "heaptrack.stacks.txt")
    flamegraph_svg = os.path.join(run_dir, "heaptrack.flamegraph.svg")
    perf_stat_txt = os.path.join(run_dir, "perf.stat.txt")
    perf_stdout_txt = os.path.join(run_dir, "perf.stdout.txt")
    perf_data = os.path.join(run_dir, "perf.data")
    perf_record_txt = os.path.join(run_dir, "perf.record.txt")
    perf_report_txt = os.path.join(run_dir, "perf.report.txt")
    profile_log = os.path.join(run_dir, "profile.log")
    setup_log = os.path.join(run_dir, "setup.log")
    drop_caches_log = os.path.join(run_dir, "drop-caches.log")
    meta_txt = os.path.join(run_dir, "meta.txt")

    vykar_bin = os.path.join(repo_root, "target", "profiling", "vykar")
    cleanup_dirs: list[str] = []
    setup_steps: list[str] = []
    heaptrack_file = ""

    config_generated = False
    config_path = os.environ.get("VYKAR_CONFIG", "")
    repo_url = ""

    # Generate config if not explicitly provided
    if not config_path:
        config_generated = True
        config_path = os.path.join(run_dir, "vykar.profile.yaml")
        work_dir = os.path.join(run_dir, "work")
        os.makedirs(work_dir, exist_ok=True)

        if args.backend == "local":
            profile_repo_dir = os.path.join(work_dir, "repository")
            os.makedirs(profile_repo_dir, exist_ok=True)
            repo_url = profile_repo_dir
            cleanup_dirs.append(profile_repo_dir)
        elif args.backend == "rest":
            repo_url = defaults.rest_url
        elif args.backend == "s3":
            repo_url = f"s3+http://127.0.0.1:9000/vykar-profile/{repo_label}"
            cfg.reset_minio(defaults)
            cfg.ensure_backend_ready("s3", repo_url, defaults)

        cfg.write_vykar_config(
            config_path,
            backend=args.backend,
            repo_label=repo_label,
            corpus_path=source_path,
            repo_url=repo_url,
            defaults=defaults,
        )
        os.environ["VYKAR_PASSPHRASE"] = defaults.passphrase
    else:
        if not os.path.isfile(config_path):
            _die(f"config not found: {config_path}")

    env = os.environ.copy()
    env.setdefault("VYKAR_PASSPHRASE", defaults.passphrase)

    def cleanup_on_exit() -> None:
        for d in cleanup_dirs:
            if d and os.path.exists(d):
                shutil.rmtree(d, ignore_errors=True)

    # Build
    if not args.skip_build:
        _log("[1/6] Building vykar-cli with profiling profile...")
        subprocess.run(
            ["cargo", "build", "-p", "vykar-cli", "--profile", "profiling"],
            cwd=repo_root,
            check=True,
        )
    else:
        _log("[1/6] Skipping build (--skip-build)")

    if not os.path.isfile(vykar_bin):
        cleanup_on_exit()
        _die(f"built binary not found: {vykar_bin}")

    # Setup
    _log(f"[2/6] Running setup for mode: {args.mode}")
    with open(setup_log, "w") as f:
        f.write("")

    def run_setup_cmd(cmd: list[str], desc: str) -> None:
        setup_steps.append(desc)
        with open(setup_log, "a") as f:
            f.write(f"[setup] {desc}\n")
            result = subprocess.run(cmd, env=env, stdout=f, stderr=subprocess.STDOUT, check=False)
            if result.returncode != 0:
                cleanup_on_exit()
                _die(f"setup failed: {desc}")

    def setup_reset_and_init() -> None:
        setup_steps.append(f"delete+init repo ({repo_label}, backend={args.backend})")
        # Delete (tolerate failure)
        subprocess.run(
            [vykar_bin, "--config", config_path, "delete", "-R", repo_label, "--yes-delete-this-repo"],
            env=env,
            capture_output=True,
            check=False,
        )
        run_setup_cmd(
            [vykar_bin, "--config", config_path, "init", "-R", repo_label],
            f"init repo ({repo_label})",
        )

    def setup_backup(label: str, src: str) -> None:
        run_setup_cmd(
            [vykar_bin, "--config", config_path, "backup", "-R", repo_label, "-l", label, src],
            f"backup ({src}, label={label})",
        )

    # Build profile command based on mode
    profile_cmd: list[str] = []
    restore_dest = ""

    if args.mode == "backup":
        setup_reset_and_init()
        setup_backup(snapshot_label, seed_source_path)
        profile_cmd = [vykar_bin, "--config", config_path, "backup", "-R", repo_label, "-l", snapshot_label, source_path]

    elif args.mode == "restore":
        setup_reset_and_init()
        setup_backup(snapshot_label, source_path)
        restore_dest = os.path.join(run_dir, "restore-target")
        if os.path.exists(restore_dest):
            shutil.rmtree(restore_dest)
        os.makedirs(restore_dest, exist_ok=True)
        cleanup_dirs.append(restore_dest)
        setup_steps.append(f"prepare restore destination ({restore_dest})")
        profile_cmd = [vykar_bin, "--config", config_path, "restore", "-R", repo_label, "latest", restore_dest]

    elif args.mode == "compact":
        seed_label = f"{snapshot_label}-seed"
        setup_reset_and_init()
        setup_backup(seed_label, seed_source_path)
        setup_backup(snapshot_label, source_path)
        # Resolve and delete seed snapshot
        result = subprocess.run(
            [vykar_bin, "--config", config_path, "list", "-R", repo_label, "-S", seed_label, "--last", "1"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        lines = result.stdout.strip().splitlines()
        if len(lines) < 2:
            cleanup_on_exit()
            _die("could not resolve seed snapshot for compact setup")
        seed_snapshot = lines[1].split()[0]
        _log(f"[setup] Deleting seed snapshot: {seed_snapshot}")
        setup_steps.append(f"delete seed snapshot ({seed_snapshot})")
        run_setup_cmd(
            [vykar_bin, "--config", config_path, "snapshot", "delete", seed_snapshot, "-R", repo_label],
            f"delete seed snapshot ({seed_snapshot})",
        )
        profile_cmd = [vykar_bin, "--config", config_path, "compact", "-R", repo_label, "--threshold", args.compact_threshold]
        if args.dry_run:
            profile_cmd.append("-n")

    elif args.mode == "prune":
        setup_reset_and_init()
        setup_backup(snapshot_label, source_path)
        profile_cmd = [vykar_bin, "--config", config_path, "prune", "-R", repo_label]
        if args.dry_run:
            profile_cmd.append("-n")

    elif args.mode == "check":
        setup_reset_and_init()
        setup_backup(snapshot_label, source_path)
        profile_cmd = [vykar_bin, "--config", config_path, "check", "-R", repo_label]
        if args.verify_data:
            profile_cmd.append("--verify-data")

    # Drop caches
    _log("[3/6] Dropping caches before measured command...")
    setup_steps.append("drop_caches before profile")
    if not args.no_drop_caches:
        vykar_cmd.drop_caches()
        with open(drop_caches_log, "w") as f:
            f.write("caches dropped\n")
    else:
        with open(drop_caches_log, "w") as f:
            f.write("skipped: drop_caches (--no-drop-caches)\n")

    # Resolve heaptrack data file
    def resolve_heaptrack_data_file() -> str:
        patterns = [
            os.path.join(run_dir, f"heaptrack.vykar.{stamp}.*.gz.zst"),
            os.path.join(run_dir, f"heaptrack.vykar.{stamp}.*.gz"),
        ]
        for pattern in patterns:
            files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
            if files:
                return files[0]
        _die(f"could not find heaptrack output in: {run_dir}")
        return ""  # unreachable

    # Profiling functions
    def run_heaptrack_profile() -> None:
        _log(f"[4/6] Running heaptrack for mode: {args.mode}")
        if args.mode == "restore" and restore_dest:
            if os.path.exists(restore_dest):
                shutil.rmtree(restore_dest)
            os.makedirs(restore_dest, exist_ok=True)
        with open(profile_log, "w") as f:
            subprocess.run(
                ["heaptrack", "-o", heaptrack_data_template] + profile_cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                check=True,
            )

    def run_perf_profile() -> None:
        _log(f"[4/6] Running perf stat for mode: {args.mode}")
        if args.mode == "restore" and restore_dest:
            if os.path.exists(restore_dest):
                shutil.rmtree(restore_dest)
            os.makedirs(restore_dest, exist_ok=True)

        perf_stat_cmd = ["perf", "stat", "-d", "-r", "1"]
        if perf_events:
            perf_stat_cmd.extend(["-e", perf_events])
        perf_stat_cmd.extend(["--"] + profile_cmd)

        with open(perf_stdout_txt, "w") as out, open(perf_stat_txt, "w") as err:
            subprocess.run(perf_stat_cmd, stdout=out, stderr=err, check=False)
        _log(f"[4/6] perf stat complete -> {perf_stat_txt}")

        # Second run: restore needs clean target
        if args.mode == "restore" and restore_dest:
            if os.path.exists(restore_dest):
                shutil.rmtree(restore_dest)
            os.makedirs(restore_dest, exist_ok=True)

        _log(f"[4/6] Running perf record for mode: {args.mode}")
        with open(perf_record_txt, "w") as f:
            subprocess.run(
                ["perf", "record", "-F", str(perf_record_freq), "-g", "--output", perf_data] + profile_cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                check=False,
            )
        _log(f"[4/6] perf record complete -> {perf_data}")

        _log("[5/6] Generating perf report...")
        with open(perf_report_txt, "w") as f:
            subprocess.run(
                ["perf", "report", "--stdio", "--input", perf_data],
                stdout=f,
                stderr=subprocess.STDOUT,
                check=False,
            )

    def render_heaptrack_reports() -> None:
        nonlocal heaptrack_file
        _log("[5/6] Generating text analysis and stacks...")
        heaptrack_file = resolve_heaptrack_data_file()
        with open(analysis_txt, "w") as f:
            subprocess.run(["heaptrack_print", "-f", heaptrack_file], stdout=f, check=True)
        subprocess.run(
            ["heaptrack_print", "-f", heaptrack_file, "--flamegraph-cost-type", cost_type, "-F", stacks_txt],
            stdout=subprocess.DEVNULL,
            check=True,
        )

        flamegraph_pl = shutil.which("flamegraph.pl")
        if not flamegraph_pl:
            fg_dir = "/tmp/FlameGraph"
            fg_script = os.path.join(fg_dir, "flamegraph.pl")
            if not os.path.isfile(fg_script):
                if os.path.exists(fg_dir):
                    shutil.rmtree(fg_dir)
                subprocess.run(
                    ["git", "clone", "--depth", "1", "https://github.com/brendangregg/FlameGraph.git", fg_dir],
                    capture_output=True,
                    check=True,
                )
            flamegraph_pl = fg_script

        _log("[6/6] Rendering flamegraph SVG...")
        with open(stacks_txt) as stacks_in, open(flamegraph_svg, "w") as svg_out:
            subprocess.run(
                [
                    "perl",
                    flamegraph_pl,
                    "--title",
                    f"heaptrack: {cost_type} (vykar-cli profiling, mode={args.mode})",
                    "--colors",
                    "mem",
                    "--countname",
                    cost_type,
                ],
                stdin=stacks_in,
                stdout=svg_out,
                check=True,
            )

    try:
        if args.profiler == "heaptrack":
            run_heaptrack_profile()
            render_heaptrack_reports()
        elif args.profiler == "perf":
            run_perf_profile()
        else:  # both
            run_heaptrack_profile()
            render_heaptrack_reports()
            if not args.no_drop_caches:
                _log("[7/7] Dropping caches before perf run...")
                setup_steps.append("drop_caches before perf profile")
                vykar_cmd.drop_caches()
            run_perf_profile()
    finally:
        cleanup_on_exit()

    # Meta
    with open(meta_txt, "w") as f:
        f.write(f"mode={args.mode}\n")
        f.write(f"backend={args.backend}\n")
        f.write(f"timestamp_utc={datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n")
        f.write(f"vykar_bin={vykar_bin}\n")
        f.write(f"config={config_path}\n")
        f.write(f"config_generated={1 if config_generated else 0}\n")
        f.write(f"repo={repo_label}\n")
        f.write(f"source={source_path}\n")
        f.write(f"profiler={args.profiler}\n")
        f.write(f"skip_build={1 if args.skip_build else 0}\n")
        f.write(f"drop_caches={0 if args.no_drop_caches else 1}\n")
        f.write(f"profile_cmd={' '.join(profile_cmd)}\n")
        f.write("setup_steps<<EOF\n")
        for step in setup_steps:
            f.write(f"{step}\n")
        f.write("EOF\n")
        if heaptrack_file:
            f.write(f"heaptrack_data={heaptrack_file}\n")
        if args.profiler in ("perf", "both"):
            f.write(f"perf_stat_txt={perf_stat_txt}\n")
            f.write(f"perf_data={perf_data}\n")

    # Summary
    print()
    print("Run complete. Outputs:")
    print(f"  mode:      {args.mode}")
    print(f"  backend:   {args.backend}")
    print(f"  profiler:  {args.profiler}")
    print(f"  run_dir:   {run_dir}")
    if heaptrack_file:
        print(f"  flamegraph: {flamegraph_svg}")
    if args.profiler in ("perf", "both"):
        print(f"  perf_stat: {perf_stat_txt}")
        print(f"  perf_data: {perf_data}")
    print(f"  meta:      {meta_txt}")
