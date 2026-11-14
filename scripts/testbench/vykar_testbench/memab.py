"""Interleaved A/B peak-RSS comparison of `vykar backup` across binaries.

Peak RSS on a single host drifts between sweeps and is bimodal within one, so
a candidate may only be compared against a baseline measured in the same
sweep. This tool runs every variant once per round, rotates the order each
round, and restores an identical repository + cache snapshot before every
run, then reports per-variant medians and round-paired deltas.

Workloads:
- ``full``: the repo holds a seed backup of ``<source>/snapshot-1``; the
  measured command backs up the whole ``<source>`` (mostly new data).
- ``incremental``: the repo already holds a backup of the whole ``<source>``;
  the measured command backs it up again (nothing changed).
"""

import argparse
import fcntl
import os
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field

import yaml

from . import config as cfg
from . import vykar as vykar_cmd

LOCK_PATH = "/tmp/vykar-mem-ab.lock"
REPO_LABEL = "mem-ab"
SNAPSHOT_LABEL = "corpus"
WORKLOADS = ("full", "incremental")


@dataclass
class Variant:
    name: str
    binary: str
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class RunResult:
    workload: str
    round: int
    variant: str
    maxrss_kb: int
    wall_s: float
    user_s: float
    sys_s: float
    returncode: int


def _log(msg: str) -> None:
    print(msg, flush=True)


def _die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def parse_variants(specs: list[str], env_specs: list[str]) -> list[Variant]:
    """Parse ``NAME=BINARY`` and ``NAME:KEY=VALUE`` command-line specs."""
    variants: list[Variant] = []
    for spec in specs:
        name, sep, binary = spec.partition("=")
        if not sep or not name or not binary:
            raise ValueError(f"--variant must be NAME=BINARY, got: {spec}")
        if any(v.name == name for v in variants):
            raise ValueError(f"duplicate variant name: {name}")
        variants.append(Variant(name=name, binary=os.path.abspath(os.path.expanduser(binary))))
    if len(variants) < 1:
        raise ValueError("at least one --variant is required")
    for spec in env_specs:
        name, sep, assignment = spec.partition(":")
        key, eq, value = assignment.partition("=")
        if not sep or not eq or not key:
            raise ValueError(f"--variant-env must be NAME:KEY=VALUE, got: {spec}")
        matches = [v for v in variants if v.name == name]
        if not matches:
            raise ValueError(f"--variant-env names unknown variant: {name}")
        matches[0].env[key] = value
    return variants


def round_order(variants: list[Variant], round_index: int) -> list[Variant]:
    """Rotate the variant order so each one runs first equally often."""
    shift = round_index % len(variants)
    return variants[shift:] + variants[:shift]


def run_measured(argv: list[str], env: dict[str, str], log_path: str) -> tuple[int, int, float, float, float]:
    """Run `argv`, returning (returncode, maxrss_kb, wall_s, user_s, sys_s).

    Uses `wait4` so the figures cover exactly this child, not earlier ones.
    """
    with open(log_path, "w") as log:
        start = time.monotonic()
        proc = subprocess.Popen(argv, env=env, stdout=log, stderr=subprocess.STDOUT)
        _, status, usage = os.wait4(proc.pid, 0)
        wall = time.monotonic() - start
    proc.returncode = os.waitstatus_to_exitcode(status)
    return proc.returncode, usage.ru_maxrss, wall, usage.ru_utime, usage.ru_stime


def summarize(results: list[RunResult], variants: list[str]) -> str:
    """Render per-workload tables: per-variant medians and paired deltas vs the first variant."""
    lines: list[str] = []
    mib = 1024.0
    for workload in WORKLOADS:
        rows = [r for r in results if r.workload == workload and r.returncode == 0]
        if not rows:
            continue
        lines.append(f"== {workload} ==")
        lines.append(
            f"{'variant':<16} {'n':>2} {'rss med':>9} {'rss min':>9} {'rss max':>9} "
            f"{'wall med':>9} {'cpu med':>8} {'d rss med':>10} {'lower':>6} {'d wall':>7}"
        )
        base_by_round = {r.round: r for r in rows if r.variant == variants[0]}
        for name in variants:
            mine = [r for r in rows if r.variant == name]
            if not mine:
                continue
            rss = [r.maxrss_kb / mib for r in mine]
            wall = [r.wall_s for r in mine]
            cpu = [r.user_s + r.sys_s for r in mine]
            paired = [(r, base_by_round[r.round]) for r in mine if r.round in base_by_round]
            if name == variants[0] or not paired:
                delta, lower, dwall = "-", "-", "-"
            else:
                deltas = [(r.maxrss_kb - b.maxrss_kb) / mib for r, b in paired]
                delta = f"{statistics.median(deltas):+.1f}"
                lower = f"{sum(1 for d in deltas if d < 0)}/{len(deltas)}"
                dwall = f"{statistics.median([(r.wall_s / b.wall_s - 1) * 100 for r, b in paired]):+.1f}%"
            lines.append(
                f"{name:<16} {len(mine):>2} {statistics.median(rss):>9.1f} {min(rss):>9.1f} {max(rss):>9.1f} "
                f"{statistics.median(wall):>9.2f} {statistics.median(cpu):>8.1f} {delta:>10} {lower:>6} {dwall:>7}"
            )
        lines.append("")
    lines.append("rss in MiB, wall/cpu in seconds; d = round-paired median delta vs the first variant")
    return "\n".join(lines)


def _copy_tree(src: str, dst: str) -> None:
    if os.path.exists(dst):
        shutil.rmtree(dst)
    subprocess.run(["cp", "-a", "--reflink=auto", src, dst], check=True)


def _write_config(path: str, repo_dir: str, cache_dir: str, source: str, args: argparse.Namespace) -> None:
    defaults = cfg.load_defaults()
    cfg.write_vykar_config(path, backend="local", repo_label=REPO_LABEL, corpus_path=source, repo_url=repo_dir, defaults=defaults)
    with open(path) as f:
        data = yaml.safe_load(f)
    data["cache_dir"] = cache_dir
    limits: dict = {}
    if args.threads is not None:
        limits["threads"] = args.threads
    if args.connections is not None:
        limits["connections"] = args.connections
    if limits:
        data["limits"] = limits
    with open(path, "w") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mem-ab",
        description="Interleaved A/B peak-RSS comparison of `vykar backup` between binaries.",
    )
    parser.add_argument("--variant", action="append", default=[], metavar="NAME=BINARY",
                        help="a vykar binary to measure; repeat. The first is the baseline.")
    parser.add_argument("--variant-env", action="append", default=[], metavar="NAME:KEY=VALUE",
                        help="extra environment variable for one variant; repeat")
    parser.add_argument("--workload", choices=[*WORKLOADS, "both"], default="both")
    parser.add_argument("--rounds", type=int, default=8, help="runs per variant per workload (default: 8)")
    parser.add_argument("--source", default=None, help="corpus with a snapshot-1 subdirectory (default: $CORPUS_LOCAL)")
    parser.add_argument("--threads", type=int, default=None, help="limits.threads (default: vykar's auto)")
    parser.add_argument("--connections", type=int, default=None, help="limits.connections (default: vykar's default)")
    parser.add_argument("--work-dir", default=None, help="scratch directory for repos (default: /mnt/repos/mem-ab)")
    parser.add_argument("--out-dir", default=None, help="where results are written (default: $RUNTIME_ROOT/mem-ab/<stamp>)")
    parser.add_argument("--drop-caches", action="store_true", help="drop the page cache before every measured run")
    parser.add_argument("--keep", action="store_true", help="keep the work directory afterwards")
    args = parser.parse_args()

    try:
        variants = parse_variants(args.variant, args.variant_env)
    except ValueError as exc:
        _die(str(exc))
    for v in variants:
        if not os.path.isfile(v.binary):
            _die(f"binary not found for variant {v.name}: {v.binary}")
    if args.rounds < 1:
        _die("--rounds must be >= 1")

    defaults = cfg.load_defaults()
    source = os.path.abspath(os.path.expanduser(args.source or defaults.corpus_local))
    seed = os.path.join(source, "snapshot-1")
    if not os.path.isdir(seed):
        _die(f"seed source not found: {seed}")

    work = args.work_dir or os.path.join(defaults.repo_root, "mem-ab")
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.out_dir or os.path.join(defaults.runtime_root, "mem-ab", stamp)
    os.makedirs(out_dir, exist_ok=True)
    workloads = list(WORKLOADS) if args.workload == "both" else [args.workload]

    # One sweep at a time per host: concurrent sweeps would perturb each other.
    lock = open(LOCK_PATH, "w")
    _log(f"waiting for {LOCK_PATH} ...")
    fcntl.flock(lock, fcntl.LOCK_EX)

    if os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(work)
    run_dir = os.path.join(work, "run")
    repo_dir = os.path.join(run_dir, "repo")
    cache_dir = os.path.join(run_dir, "cache")
    config_path = os.path.join(work, "vykar.yaml")
    _write_config(config_path, repo_dir, cache_dir, source, args)

    base_env = os.environ.copy()
    base_env["VYKAR_PASSPHRASE"] = defaults.passphrase
    setup_bin = variants[0].binary

    def setup(argv: list[str], desc: str) -> None:
        _log(f"[setup] {desc}")
        with open(os.path.join(out_dir, "setup.log"), "a") as log:
            log.write(f"[setup] {desc}\n")
            log.flush()
            rc = subprocess.run([setup_bin, "--config", config_path, *argv], env=base_env,
                                stdout=log, stderr=subprocess.STDOUT, check=False).returncode
        if rc != 0:
            _die(f"setup failed ({desc}); see {out_dir}/setup.log")

    # Templates are created at the run path and snapshotted, so every measured
    # run sees the repository and its caches at the path they were created at.
    os.makedirs(repo_dir)
    os.makedirs(cache_dir)
    setup(["init", "-R", REPO_LABEL], "init repo")
    setup(["backup", "-R", REPO_LABEL, "-l", SNAPSHOT_LABEL, seed], "seed backup (snapshot-1)")
    templates = {"full": os.path.join(work, "template-full")}
    _copy_tree(run_dir, templates["full"])
    if "incremental" in workloads:
        setup(["backup", "-R", REPO_LABEL, "-l", SNAPSHOT_LABEL, source], "full backup (for incremental template)")
        templates["incremental"] = os.path.join(work, "template-incremental")
        _copy_tree(run_dir, templates["incremental"])

    measured = ["--config", config_path, "backup", "-R", REPO_LABEL, "-l", SNAPSHOT_LABEL, source]
    results: list[RunResult] = []
    tsv_path = os.path.join(out_dir, "results.tsv")
    with open(tsv_path, "w") as tsv:
        tsv.write("workload\tround\tvariant\tmaxrss_kb\twall_s\tuser_s\tsys_s\treturncode\n")
        for workload in workloads:
            for rnd in range(args.rounds):
                for variant in round_order(variants, rnd):
                    _copy_tree(templates[workload], run_dir)
                    subprocess.run(["sync"], check=False)
                    if args.drop_caches:
                        vykar_cmd.drop_caches()
                    env = {**base_env, **variant.env}
                    log_path = os.path.join(out_dir, f"{workload}.r{rnd}.{variant.name}.log")
                    rc, rss, wall, user, sys_s = run_measured([variant.binary, *measured], env, log_path)
                    result = RunResult(workload, rnd, variant.name, rss, wall, user, sys_s, rc)
                    results.append(result)
                    tsv.write(f"{workload}\t{rnd}\t{variant.name}\t{rss}\t{wall:.3f}\t{user:.3f}\t{sys_s:.3f}\t{rc}\n")
                    tsv.flush()
                    status = "" if rc == 0 else f"  FAILED rc={rc}"
                    _log(f"[{workload} r{rnd}] {variant.name:<16} rss={rss / 1024:.1f} MiB wall={wall:.2f}s "
                         f"cpu={user + sys_s:.1f}s{status}")

    if not args.keep:
        shutil.rmtree(work, ignore_errors=True)

    summary = summarize(results, [v.name for v in variants])
    with open(os.path.join(out_dir, "summary.txt"), "w") as f:
        f.write(summary + "\n")
    print()
    print(summary)
    print(f"\nresults: {tsv_path}")
    if any(r.returncode != 0 for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
