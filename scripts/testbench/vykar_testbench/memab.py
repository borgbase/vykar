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
- ``restore``: the repo holds one backup of the whole ``<source>``; the
  measured command restores ``latest`` into an empty destination directory.
  Restore does not modify the repository, so the repo is seeded once (and can
  be reused across sweeps with ``--reuse-work-dir``); only the cache directory
  is reset from a template, and the destination is recreated empty before and
  deleted after every run.

Besides ``ru_maxrss``, every run polls ``/proc/<pid>/status`` and records the
peaks of RssAnon / RssFile / RssShmem plus a per-run timeline file, since the
total alone cannot tell heap from file-backed (mmap'd) pages.
"""

import argparse
import fcntl
import json
import os
import shutil
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

import yaml

from . import config as cfg
from . import vykar as vykar_cmd

LOCK_PATH = "/tmp/vykar-mem-ab.lock"
REPO_LABEL = "mem-ab"
SNAPSHOT_LABEL = "corpus"
BACKUP_WORKLOADS = ("full", "incremental")
WORKLOADS = (*BACKUP_WORKLOADS, "restore")
RESTORE_SEED_MARKER = "restore-seed.json"
# /proc/<pid>/status fields sampled by the poller, all in kB except Threads.
STATUS_FIELDS = ("VmRSS", "RssAnon", "RssFile", "RssShmem", "VmHWM", "Threads")
POLL_INTERVAL_S = 0.05


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
    # Peaks seen by the /proc poller (kB); 0 when no sample was taken.
    peak_vmrss_kb: int = 0
    peak_anon_kb: int = 0
    peak_file_kb: int = 0
    peak_shmem_kb: int = 0
    peak_threads: int = 0


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


def parse_proc_status(text: str) -> dict[str, int]:
    """Extract `STATUS_FIELDS` from a /proc/<pid>/status dump (kB / counts)."""
    out: dict[str, int] = {}
    for line in text.splitlines():
        key, sep, rest = line.partition(":")
        if sep and key in STATUS_FIELDS:
            parts = rest.split()
            if parts:
                out[key] = int(parts[0])
    return out


def peak_of(samples: list[dict[str, int]]) -> dict[str, int]:
    """Per-field maximum over poller samples (fields may be missing in some)."""
    peaks: dict[str, int] = {}
    for sample in samples:
        for key, value in sample.items():
            if key != "t_ms" and value > peaks.get(key, 0):
                peaks[key] = value
    return peaks


class ProcPoller:
    """Sample /proc/<pid>/status every `interval` seconds into a timeline TSV.

    Peaks are only as fine as the interval; ru_maxrss stays the exact total.
    """

    def __init__(self, pid: int, timeline_path: str | None, interval: float = POLL_INTERVAL_S) -> None:
        self.pid = pid
        self.interval = interval
        self.timeline_path = timeline_path
        self.samples: list[dict[str, int]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._start = time.monotonic()
        self._thread.start()

    def _run(self) -> None:
        path = f"/proc/{self.pid}/status"
        while not self._stop.is_set():
            try:
                with open(path) as f:
                    sample = parse_proc_status(f.read())
            except OSError:
                break
            if not sample:  # zombie: the status file no longer carries memory fields
                break
            sample["t_ms"] = int((time.monotonic() - self._start) * 1000)
            self.samples.append(sample)
            self._stop.wait(self.interval)

    def stop(self) -> dict[str, int]:
        self._stop.set()
        self._thread.join()
        if self.timeline_path:
            with open(self.timeline_path, "w") as f:
                f.write("t_ms\t" + "\t".join(STATUS_FIELDS) + "\n")
                for s in self.samples:
                    f.write(f"{s['t_ms']}\t" + "\t".join(str(s.get(k, "")) for k in STATUS_FIELDS) + "\n")
        return peak_of(self.samples)


def run_measured(
    argv: list[str], env: dict[str, str], log_path: str, timeline_path: str | None = None
) -> tuple[int, int, float, float, float, dict[str, int]]:
    """Run `argv`, returning (returncode, maxrss_kb, wall_s, user_s, sys_s, poller peaks).

    Uses `wait4` so the figures cover exactly this child, not earlier ones.
    The poller reads the still-unreaped child, so it never races a reused pid.
    """
    with open(log_path, "w") as log:
        start = time.monotonic()
        proc = subprocess.Popen(argv, env=env, stdout=log, stderr=subprocess.STDOUT)
        poller = ProcPoller(proc.pid, timeline_path)
        poller.start()
        # Wait for exit without reaping, so the poller can never read a
        # recycled pid; then reap with wait4 for the rusage.
        os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT)
        peaks = poller.stop()
        _, status, usage = os.wait4(proc.pid, 0)
        wall = time.monotonic() - start
    proc.returncode = os.waitstatus_to_exitcode(status)
    return proc.returncode, usage.ru_maxrss, wall, usage.ru_utime, usage.ru_stime, peaks


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
            f"{'wall med':>9} {'cpu med':>8} {'d rss med':>10} {'lower':>6} {'d wall':>7} "
            f"{'anon med':>9} {'file med':>9}"
        )
        base_by_round = {r.round: r for r in rows if r.variant == variants[0]}
        for name in variants:
            mine = [r for r in rows if r.variant == name]
            if not mine:
                continue
            rss = [r.maxrss_kb / mib for r in mine]
            wall = [r.wall_s for r in mine]
            cpu = [r.user_s + r.sys_s for r in mine]
            anon = [r.peak_anon_kb / mib for r in mine]
            filed = [(r.peak_file_kb + r.peak_shmem_kb) / mib for r in mine]
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
                f"{statistics.median(wall):>9.2f} {statistics.median(cpu):>8.1f} {delta:>10} {lower:>6} {dwall:>7} "
                f"{statistics.median(anon):>9.1f} {statistics.median(filed):>9.1f}"
            )
        lines.append("")
    lines.append("rss in MiB, wall/cpu in seconds; d = round-paired median delta vs the first variant")
    lines.append("anon/file = peak RssAnon and peak RssFile+RssShmem from the /proc poller (sampled, MiB)")
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


def selected_workloads(choice: str) -> list[str]:
    """`both` keeps meaning the two backup workloads; `restore` is opt-in."""
    return list(BACKUP_WORKLOADS) if choice == "both" else [choice]


def check_restore_dest(dest: str, protected: list[str]) -> None:
    """Refuse a restore destination that overlaps the corpus or the work dir.

    The destination is deleted before and after every run, so it must not be
    (inside) a protected directory, nor contain one.
    """
    d = os.path.realpath(dest)
    if d == "/":
        raise ValueError("restore destination must not be /")
    for p in protected:
        p = os.path.realpath(p)
        if d == p or d.startswith(p.rstrip("/") + "/") or p.startswith(d.rstrip("/") + "/"):
            raise ValueError(f"restore destination {dest} overlaps {p}")


def free_bytes(path: str) -> int:
    """Free bytes on the filesystem holding `path` (or its nearest existing parent)."""
    while not os.path.exists(path):
        path = os.path.dirname(path)
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


def _device(path: str) -> int:
    while not os.path.exists(path):
        path = os.path.dirname(path)
    return os.stat(path).st_dev


def space_shortfall(needs: dict[str, int], margin: float = 1.1,
                    free=free_bytes, device=_device) -> str:
    """Return a message if the paths' filesystems lack room for `needs` (bytes), else "".

    Needs of paths on the same filesystem add up.
    """
    by_dev: dict[int, tuple[list[str], int]] = {}
    for path, need in needs.items():
        paths, total = by_dev.get(device(path), ([], 0))
        by_dev[device(path)] = ([*paths, path], total + need)
    problems = []
    for paths, total in by_dev.values():
        have = free(paths[0])
        if have < total * margin:
            problems.append(f"{', '.join(paths)}: need ~{total * margin / 2**30:.1f} GiB, have {have / 2**30:.1f} GiB")
    return "; ".join(problems)


def tree_bytes(path: str) -> int:
    out = subprocess.run(["du", "-sb", path], check=True, capture_output=True, text=True).stdout
    return int(out.split()[0])


def _reset_dir(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mem-ab",
        description="Interleaved A/B peak-RSS comparison of `vykar backup` / `vykar restore` between binaries.",
    )
    parser.add_argument("--variant", action="append", default=[], metavar="NAME=BINARY",
                        help="a vykar binary to measure; repeat. The first is the baseline.")
    parser.add_argument("--variant-env", action="append", default=[], metavar="NAME:KEY=VALUE",
                        help="extra environment variable for one variant; repeat")
    parser.add_argument("--workload", choices=[*WORKLOADS, "both"], default="both",
                        help="both = full + incremental (backup); restore must be selected explicitly")
    parser.add_argument("--rounds", type=int, default=8, help="runs per variant per workload (default: 8)")
    parser.add_argument("--source", default=None, help="corpus with a snapshot-1 subdirectory (default: $CORPUS_LOCAL)")
    parser.add_argument("--threads", type=int, default=None, help="limits.threads (default: vykar's auto)")
    parser.add_argument("--connections", type=int, default=None,
                        help="limits.connections (default: vykar's default; also the restore reader thread count)")
    parser.add_argument("--work-dir", default=None, help="scratch directory for repos (default: /mnt/repos/mem-ab)")
    parser.add_argument("--out-dir", default=None, help="where results are written (default: $RUNTIME_ROOT/mem-ab/<stamp>)")
    parser.add_argument("--drop-caches", action="store_true", help="drop the page cache before every measured run")
    parser.add_argument("--keep", action="store_true", help="keep the work directory afterwards")
    parser.add_argument("--reuse-work-dir", action="store_true",
                        help="restore only: reuse a work dir seeded by an earlier sweep (seed it if absent); implies --keep")
    parser.add_argument("--restore-dest", default=None,
                        help="restore only: destination directory, recreated per run "
                             "(default: $RUNTIME_ROOT/mem-ab-restore-dest, i.e. a different disk than /mnt/repos)")
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
    workloads = selected_workloads(args.workload)
    restore_mode = workloads == ["restore"]
    if args.reuse_work_dir and not restore_mode:
        _die("--reuse-work-dir is only supported with --workload restore")
    keep = args.keep or args.reuse_work_dir

    defaults = cfg.load_defaults()
    source = os.path.abspath(os.path.expanduser(args.source or defaults.corpus_local))
    seed = os.path.join(source, "snapshot-1")
    if not restore_mode and not os.path.isdir(seed):
        _die(f"seed source not found: {seed}")
    if not os.path.isdir(source):
        _die(f"source not found: {source}")

    work = os.path.abspath(args.work_dir or os.path.join(defaults.repo_root, "mem-ab"))
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.out_dir or os.path.join(defaults.runtime_root, "mem-ab", stamp)
    os.makedirs(out_dir, exist_ok=True)
    restore_dest = os.path.abspath(os.path.expanduser(
        args.restore_dest or os.path.join(defaults.runtime_root, "mem-ab-restore-dest")))
    if restore_mode:
        try:
            check_restore_dest(restore_dest, [source, work, out_dir])
        except ValueError as exc:
            _die(str(exc))

    # One sweep at a time per host: concurrent sweeps would perturb each other.
    lock = open(LOCK_PATH, "w")
    _log(f"waiting for {LOCK_PATH} ...")
    fcntl.flock(lock, fcntl.LOCK_EX)

    run_dir = os.path.join(work, "run")
    repo_dir = os.path.join(run_dir, "repo")
    cache_dir = os.path.join(run_dir, "cache")
    config_path = os.path.join(work, "vykar.yaml")
    marker_path = os.path.join(work, RESTORE_SEED_MARKER)
    reuse = False
    if args.reuse_work_dir and os.path.isfile(marker_path):
        with open(marker_path) as f:
            marker = json.load(f)
        if marker.get("source") != source:
            _die(f"{work} was seeded from {marker.get('source')}, not {source}")
        reuse = True
        _log(f"[setup] reusing seeded work dir {work} (seeded {marker.get('seeded_at')})")
    else:
        if os.path.exists(work):
            shutil.rmtree(work)
        os.makedirs(work)
    # Rewritten every sweep so --threads/--connections apply to a reused repo too.
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

    templates: dict[str, str] = {}
    if restore_mode:
        # Restore leaves the repo alone but may touch the local cache, so only
        # the (small) cache directory is reset from a template before each run.
        templates["restore"] = os.path.join(work, "template-restore-cache")
        if not reuse:
            source_bytes = tree_bytes(source)
            # Repo ~ corpus size worst case (incompressible), destination = corpus size.
            shortfall = space_shortfall({work: source_bytes, restore_dest: source_bytes})
            if shortfall:
                _die(f"not enough free space: {shortfall}")
            os.makedirs(repo_dir)
            os.makedirs(cache_dir)
            setup(["init", "-R", REPO_LABEL], "init repo")
            setup(["backup", "-R", REPO_LABEL, "-l", SNAPSHOT_LABEL, source], "full backup (restore seed)")
            _copy_tree(cache_dir, templates["restore"])
            with open(marker_path, "w") as f:
                json.dump({"source": source, "source_bytes": source_bytes, "seed_binary": setup_bin,
                           "seeded_at": stamp}, f, indent=2)
        with open(marker_path) as f:
            source_bytes = json.load(f)["source_bytes"]
        shutil.rmtree(restore_dest, ignore_errors=True)  # leftover of an aborted sweep
        shortfall = space_shortfall({restore_dest: source_bytes})
        if shortfall:
            _die(f"not enough free space: {shortfall}")
        measured = ["--config", config_path, "restore", "-R", REPO_LABEL, "latest", restore_dest]
    else:
        # Templates are created at the run path and snapshotted, so every measured
        # run sees the repository and its caches at the path they were created at.
        os.makedirs(repo_dir)
        os.makedirs(cache_dir)
        setup(["init", "-R", REPO_LABEL], "init repo")
        setup(["backup", "-R", REPO_LABEL, "-l", SNAPSHOT_LABEL, seed], "seed backup (snapshot-1)")
        templates["full"] = os.path.join(work, "template-full")
        _copy_tree(run_dir, templates["full"])
        if "incremental" in workloads:
            setup(["backup", "-R", REPO_LABEL, "-l", SNAPSHOT_LABEL, source], "full backup (for incremental template)")
            templates["incremental"] = os.path.join(work, "template-incremental")
            _copy_tree(run_dir, templates["incremental"])
        measured = ["--config", config_path, "backup", "-R", REPO_LABEL, "-l", SNAPSHOT_LABEL, source]

    def prepare(workload: str) -> None:
        if workload == "restore":
            _copy_tree(templates["restore"], cache_dir)
            _reset_dir(restore_dest)
        else:
            _copy_tree(templates[workload], run_dir)

    results: list[RunResult] = []
    tsv_path = os.path.join(out_dir, "results.tsv")
    try:
        with open(tsv_path, "w") as tsv:
            tsv.write("workload\tround\tvariant\tmaxrss_kb\twall_s\tuser_s\tsys_s\treturncode\t"
                      "peak_vmrss_kb\tpeak_anon_kb\tpeak_file_kb\tpeak_shmem_kb\tpeak_threads\n")
            for workload in workloads:
                for rnd in range(args.rounds):
                    for variant in round_order(variants, rnd):
                        prepare(workload)
                        subprocess.run(["sync"], check=False)
                        if args.drop_caches:
                            vykar_cmd.drop_caches()
                        env = {**base_env, **variant.env}
                        stem = os.path.join(out_dir, f"{workload}.r{rnd}.{variant.name}")
                        rc, rss, wall, user, sys_s, peaks = run_measured(
                            [variant.binary, *measured], env, stem + ".log", stem + ".timeline.tsv")
                        if workload == "restore":
                            shutil.rmtree(restore_dest, ignore_errors=True)
                        result = RunResult(workload, rnd, variant.name, rss, wall, user, sys_s, rc,
                                           peaks.get("VmRSS", 0), peaks.get("RssAnon", 0), peaks.get("RssFile", 0),
                                           peaks.get("RssShmem", 0), peaks.get("Threads", 0))
                        results.append(result)
                        tsv.write(f"{workload}\t{rnd}\t{variant.name}\t{rss}\t{wall:.3f}\t{user:.3f}\t{sys_s:.3f}\t{rc}\t"
                                  f"{result.peak_vmrss_kb}\t{result.peak_anon_kb}\t{result.peak_file_kb}\t"
                                  f"{result.peak_shmem_kb}\t{result.peak_threads}\n")
                        tsv.flush()
                        status = "" if rc == 0 else f"  FAILED rc={rc}"
                        _log(f"[{workload} r{rnd}] {variant.name:<16} rss={rss / 1024:.1f} MiB "
                             f"anon={result.peak_anon_kb / 1024:.1f} file={result.peak_file_kb / 1024:.1f} "
                             f"wall={wall:.2f}s cpu={user + sys_s:.1f}s{status}")
    finally:
        if restore_mode:
            shutil.rmtree(restore_dest, ignore_errors=True)

    if not keep:
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
