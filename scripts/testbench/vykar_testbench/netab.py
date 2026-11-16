"""Interleaved A/B benchmark of `vykar backup` over a shaped loopback link.

Generalises the out-of-tree S3-only harness (``s3bench.py``) to every network
backend: ``s3`` (MinIO), ``rest`` (vykar-server), ``sftp`` (sshd), plus
``local`` as an unshaped reference.

Per sweep:
- takes the host-wide benchmark locks (see ``LOCK_PATHS``), so it never runs
  concurrently with ``mem-ab`` or ``s3bench.py``;
- shapes only the backend's TCP port on ``lo`` with netem (prio qdisc, 4 bands,
  u32 filters on sport/dport into band 4; delay applies per direction, so
  RTT = 2 x ``--delay-ms``) and puts the previous ``lo`` state back on exit
  (also on error / Ctrl-C / SIGTERM) unless ``--keep-netem`` is given;
- per run creates a fresh repository, runs ``vykar init`` and a timed
  ``vykar backup`` (optionally a second, no-change one), and deletes the
  repository afterwards.

Per run it records wall, user/sys CPU and max RSS (wait4), peak RssAnon and
thread count (/proc poller), and samples the shaped band's byte counter every
100 ms for upload-rate stability (mean / p10 / p50 / p90 / max / cv / idle
fraction over 250 ms buckets). Binaries carrying the measurement-only http
trace patch write per-request timings to ``$VYKAR_HTTP_TRACE``; a non-empty
trace is summarised, an empty or missing one is ignored (SFTP never has one).
"""

import argparse
import contextlib
import fcntl
import ipaddress
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from . import config as cfg
from . import memab

# net-ab measures the same shared resources as both s3bench.py (netem, MinIO,
# CPU) and mem-ab (CPU, /mnt/repos), so it holds both of their locks. They each
# take only their own, and net-ab always takes them in this order, so no
# deadlock is possible.
LOCK_PATHS = ("/tmp/vykar-s3bench.lock", memab.LOCK_PATH)
REPO_LABEL = "net-ab"
SNAPSHOT_LABEL = "corpus"
BACKENDS = ("s3", "rest", "sftp", "local")
STEPS = ("full", "incr")

TC = "/sbin/tc"
DEV = "lo"
SHAPED_CLASS = "1:4"  # band 4 of the prio qdisc; the default priomap only uses bands 1-3
LEAF_HANDLE = "40:"
NETEM_LIMIT = 200000  # packets; the netem default (1000) drops at high BDP
POLL_INTERVAL_S = 0.1
BUCKET_MS = 250  # bandwidth bucket for the stability stats
DEFAULT_PORTS = {"s3+http": 80, "s3+https": 443, "s3": 443, "http": 80, "https": 443, "sftp": 22}
# Below this, a socket that pins its buffer with SO_RCVBUF/SO_SNDBUF (MinIO
# does) is window-capped at about value / RTT per connection.
SOCKET_BUFFER_MIN = 4 * 2**20
REST_STASH_MAX_BYTES = 16 * 2**20


def _log(msg: str) -> None:
    print(msg, flush=True)


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr, flush=True)


def _die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Variants
# ---------------------------------------------------------------------------


@dataclass
class NetVariant:
    name: str
    binary: str
    env: dict[str, str]
    connections: int | None
    threads: int | None


def parse_net_variants(
    specs: list[str],
    env_specs: list[str],
    conn_specs: list[str],
    thread_specs: list[str],
    connections: int | None,
    threads: int | None,
) -> list[NetVariant]:
    """`memab.parse_variants` plus per-variant ``NAME=N`` limits overrides."""
    base = memab.parse_variants(specs, env_specs)
    out = [NetVariant(v.name, v.binary, v.env, connections, threads) for v in base]
    by_name = {v.name: v for v in out}
    for flag, attr, items in (("--variant-conn", "connections", conn_specs), ("--variant-threads", "threads", thread_specs)):
        for spec in items:
            name, sep, n = spec.partition("=")
            if not sep or name not in by_name:
                raise ValueError(f"{flag} must be NAME=N for a known variant, got: {spec}")
            try:
                value = int(n)
            except ValueError:
                raise ValueError(f"{flag} value must be an integer, got: {spec}") from None
            if value < 1:
                raise ValueError(f"{flag} value must be >= 1, got: {spec}")
            setattr(by_name[name], attr, value)
    return out


# ---------------------------------------------------------------------------
# Endpoint / port derivation
# ---------------------------------------------------------------------------


def backend_endpoint(backend: str, url: str) -> tuple[str, int] | None:
    """(host, port) a backend's repository URL talks to; None for `local`."""
    if backend == "local":
        return None
    parsed = urllib.parse.urlparse(url)
    if not parsed.hostname:
        raise ValueError(f"no host in {backend} URL: {url}")
    try:
        port = parsed.port
    except ValueError:
        raise ValueError(f"invalid port in {backend} URL: {url}") from None
    if port is None:
        port = DEFAULT_PORTS.get(parsed.scheme)
    if port is None:
        raise ValueError(f"cannot derive a port from {backend} URL: {url}")
    return parsed.hostname, port


def is_ipv4_loopback(host: str) -> bool:
    """The u32 filters match IPv4 only and netem shapes `lo` only."""
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return addr.version == 4 and addr.is_loopback


# ---------------------------------------------------------------------------
# tc / netem
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Shaping:
    """Shaping of `ports` on lo. delay_ms == 0 and no rate = count-only leaf (pfifo)."""

    ports: tuple[int, ...]
    delay_ms: float = 0.0
    rate: str | None = None
    limit: int = NETEM_LIMIT

    def __post_init__(self) -> None:
        # tc prints rates in its own case ("1Gbit" for "1gbit"); compare case-insensitively.
        if self.rate is not None:
            object.__setattr__(self, "rate", self.rate.lower())

    @property
    def shapes(self) -> bool:
        return self.delay_ms > 0 or self.rate is not None


def tc_teardown_command() -> list[str]:
    return ["qdisc", "del", "dev", DEV, "root"]


def tc_setup_commands(sh: Shaping) -> list[list[str]]:
    """tc argv lists (without the `sudo tc` prefix) that build `sh` on a bare lo."""
    cmds = [["qdisc", "add", "dev", DEV, "root", "handle", "1:", "prio", "bands", "4"]]
    leaf = ["qdisc", "add", "dev", DEV, "parent", SHAPED_CLASS, "handle", LEAF_HANDLE]
    if sh.shapes:
        leaf += ["netem", "delay", f"{sh.delay_ms:g}ms", "limit", str(sh.limit)]
        if sh.rate:
            leaf += ["rate", sh.rate]
    else:
        leaf += ["pfifo", "limit", str(sh.limit)]
    cmds.append(leaf)
    for port in sh.ports:
        for direction in ("dport", "sport"):
            cmds.append([
                "filter", "add", "dev", DEV, "protocol", "ip", "parent", "1:0", "prio", "1", "u32",
                "match", "ip", "protocol", "6", "0xff", "match", "ip", direction, str(port), "0xffff",
                "flowid", SHAPED_CLASS,
            ])
    return cmds


_DELAY_UNITS = {"us": 0.001, "ms": 1.0, "s": 1000.0}


def _parse_delay_ms(text: str) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(us|ms|s)", text)
    if not m:
        raise ValueError(f"unrecognised netem delay: {text}")
    return float(m.group(1)) * _DELAY_UNITS[m.group(2)]


def parse_lo_state(qdisc_text: str, filter_text: str) -> Shaping | None:
    """Recognise the lo state from `tc qdisc show` / `tc filter show` output.

    Returns None for an unshaped lo (noqueue / nothing), a `Shaping` for a
    setup in the layout `tc_setup_commands` (and netem.sh) builds, and raises
    ValueError for anything else, which net-ab then refuses to touch.
    """
    qdiscs = [line.split() for line in qdisc_text.splitlines() if line.startswith("qdisc ")]
    if all(q[1] == "noqueue" for q in qdiscs):
        return None
    roots = [q for q in qdiscs if "root" in q]
    if len(roots) != 1 or roots[0][1:3] != ["prio", "1:"] or "bands" not in roots[0] \
            or roots[0][roots[0].index("bands") + 1] != "4":
        raise ValueError(f"unrecognised root qdisc on {DEV}: {' '.join(roots[0]) if roots else qdisc_text!r}")
    children = [q for q in qdiscs if "root" not in q]
    if len(children) != 1 or children[0][2] != LEAF_HANDLE or children[0][3:5] != ["parent", SHAPED_CLASS]:
        raise ValueError(f"unrecognised child qdiscs on {DEV}: {[' '.join(c) for c in children]}")
    leaf = children[0]
    kind, opts = leaf[1], leaf[5:]
    if len(opts) % 2:
        raise ValueError(f"unrecognised leaf qdisc options: {' '.join(leaf)}")
    pairs = dict(zip(opts[::2], opts[1::2]))
    allowed = {"netem": {"limit", "delay", "rate", "seed"}, "pfifo": {"limit"}}.get(kind)
    if allowed is None or set(pairs) - allowed:
        raise ValueError(f"unrecognised leaf qdisc: {' '.join(leaf)}")
    limit = int(str(pairs.get("limit", NETEM_LIMIT)).rstrip("p"))  # pfifo prints "200000p"
    delay_ms = _parse_delay_ms(pairs["delay"]) if "delay" in pairs else 0.0
    rate = pairs.get("rate")
    if kind == "netem" and delay_ms == 0 and rate is None:
        raise ValueError(f"netem leaf without delay or rate is not a layout net-ab builds: {' '.join(leaf)}")

    ports: set[int] = set()
    current: list[str] | None = None
    entry_matches: list[tuple[int, int, int]] = []

    def close_entry() -> None:
        if current is None:
            return
        if "flowid" not in current or current[current.index("flowid") + 1] != SHAPED_CLASS:
            raise ValueError(f"unrecognised filter: {' '.join(current)}")
        if (0x60000, 0xFF0000, 8) not in entry_matches or len(entry_matches) != 2:
            raise ValueError(f"unrecognised filter matches: {entry_matches}")
        for value, mask, off in entry_matches:
            if off != 20:
                continue
            if mask == 0x0000FFFF:
                ports.add(value & 0xFFFF)
            elif mask == 0xFFFF0000:
                ports.add(value >> 16)
            else:
                raise ValueError(f"unrecognised port match mask: {mask:08x}")

    for line in filter_text.splitlines():
        words = line.split()
        if not words:
            continue
        if words[0] == "filter":
            close_entry()
            # Only "fh 800::NNN" lines are actual match entries; the others are
            # the u32 hash-table headers.
            current = words if "fh" in words and "::" in words[words.index("fh") + 1] else None
            entry_matches = []
        elif words[0] == "match" and current is not None:
            vm, at, off = words[1], words[2], words[3]
            value, _, mask = vm.partition("/")
            if at != "at":
                raise ValueError(f"unrecognised filter line: {line}")
            entry_matches.append((int(value, 16), int(mask, 16), int(off)))
    close_entry()
    return Shaping(tuple(sorted(ports)), delay_ms, rate, limit)


def _tc_show(*what: str) -> str:
    return subprocess.run([TC, *what, "show", "dev", DEV], check=True, capture_output=True, text=True).stdout


def read_lo_state() -> Shaping | None:
    return parse_lo_state(_tc_show("qdisc"), _tc_show("filter"))


def _sudo_tc(args: list[str], check: bool = True) -> None:
    r = subprocess.run(["sudo", "-n", TC, *args], capture_output=True, text=True, check=False)
    if check and r.returncode != 0:
        raise RuntimeError(f"tc {' '.join(args)} failed: {r.stderr.strip()}")


def apply_lo_state(state: Shaping | None) -> None:
    """Replace whatever is on lo by `state` (None = the kernel default)."""
    _sudo_tc(tc_teardown_command(), check=False)  # fails harmlessly when lo is bare
    if state is not None:
        for cmd in tc_setup_commands(state):
            _sudo_tc(cmd)
    got = read_lo_state()
    if got != state:
        raise RuntimeError(f"lo state after apply is {got}, expected {state}")


def describe_state(state: Shaping | None) -> str:
    if state is None:
        return f"{DEV}: no qdisc (unshaped)"
    what = f"delay {state.delay_ms:g} ms each way" if state.delay_ms else "no delay (count-only)"
    if state.rate:
        what += f", rate {state.rate}"
    return f"{DEV}: tcp ports {','.join(map(str, state.ports)) or '-'}: {what}"


def leaf_bytes() -> int | None:
    """Byte counter of the shaped band's leaf qdisc (both directions, our port only)."""
    r = subprocess.run([TC, "-s", "-j", "qdisc", "show", "dev", DEV], capture_output=True, text=True, check=False)
    try:
        for q in json.loads(r.stdout):
            if q.get("handle") == LEAF_HANDLE and q.get("parent") == SHAPED_CLASS:
                return int(q["bytes"])
    except (ValueError, KeyError, TypeError):
        pass
    return None


# ---------------------------------------------------------------------------
# Socket buffer sanity
# ---------------------------------------------------------------------------


def _read_sysctl(name: str) -> list[int] | None:
    try:
        with open("/proc/sys/" + name.replace(".", "/")) as f:
            return [int(x) for x in f.read().split()]
    except (OSError, ValueError):
        return None


def window_warnings(values: dict[str, int | None], rtt_ms: float, threshold: int = SOCKET_BUFFER_MIN) -> list[str]:
    """Warnings for socket buffer limits that would window-cap a high-BDP link."""
    if rtt_ms <= 0:
        return []
    out = []
    for name, value in values.items():
        if value is not None and value < threshold:
            cap = value / (rtt_ms / 1000) / 2**20
            out.append(f"{name}={value} is below {threshold}: a socket limited by it is window-capped "
                       f"at ~{cap:.1f} MiB/s per connection at {rtt_ms:g} ms RTT")
    return out


def socket_buffer_limits() -> dict[str, int | None]:
    def last(name: str) -> int | None:
        v = _read_sysctl(name)
        return v[-1] if v else None

    return {
        "net.core.rmem_max": last("net.core.rmem_max"),
        "net.core.wmem_max": last("net.core.wmem_max"),
        "net.ipv4.tcp_rmem[max]": last("net.ipv4.tcp_rmem"),
        "net.ipv4.tcp_wmem[max]": last("net.ipv4.tcp_wmem"),
    }


# ---------------------------------------------------------------------------
# Measurements
# ---------------------------------------------------------------------------


def bw_stats(
    samples: list[tuple[float, int]], bucket_ms: int = BUCKET_MS
) -> tuple[dict[str, float], list[tuple[float, float]]]:
    """Bucket a (t_s, cumulative bytes) counter into MiB/s windows; stats over the active span.

    A bucket closes at the first sample at least `bucket_ms` after it opened,
    so buckets are slightly longer than `bucket_ms`; the returned series holds
    (bucket start t_s, MiB/s) pairs.

    The active span runs from the first to the last bucket above 1 MiB/s, so
    the idle start (process startup, repo open) and tail do not dilute it.
    """
    if len(samples) < 3:
        return {}, []
    timed: list[tuple[float, float]] = []
    t_start, b_start = samples[0]
    nxt = t_start + bucket_ms / 1000
    for t, b in samples[1:]:
        if t >= nxt:
            timed.append((t_start, (b - b_start) / (t - t_start) / 2**20))
            t_start, b_start = t, b
            nxt = t + bucket_ms / 1000
    if not timed:
        return {}, []
    series = [v for _, v in timed]
    active = [i for i, v in enumerate(series) if v > 1.0]
    core = series[active[0]:active[-1] + 1] if active else series
    mean = statistics.fmean(core)
    srt = sorted(core)

    def pct(p: float) -> float:
        return srt[min(len(srt) - 1, int(p * len(srt)))]

    return {
        "mean_mibs": mean,
        "p10_mibs": pct(0.10),
        "p50_mibs": pct(0.50),
        "p90_mibs": pct(0.90),
        "max_mibs": srt[-1],
        "cv": (statistics.pstdev(core) / mean) if mean else 0.0,
        "idle_frac": sum(1 for v in core if v < 0.1 * srt[-1]) / len(core),
    }, timed


def trace_summary(path: str) -> dict[str, dict[str, float]]:
    """Aggregate a $VYKAR_HTTP_TRACE file by op class; {} when missing or empty.

    Line format: start_epoch_us, dur_us, thread, attempt, ok, op (tab-separated).
    """
    try:
        if os.path.getsize(path) == 0:
            return {}
    except OSError:
        return {}
    agg: dict[str, list[float]] = {}
    with open(path) as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) < 6:
                continue
            try:
                dur = int(p[1]) / 1e6
            except ValueError:
                continue
            w = p[5].split()
            if not w:
                continue
            verb, key = w[0], (w[1] if len(w) > 1 else "")
            cls = verb
            if "part" in w:
                cls = "PUT-part"
            elif verb == "PUT" and key.startswith("packs/"):
                cls = "PUT-pack-single"
            elif verb in ("PUT", "GET", "HEAD", "DELETE", "LIST"):
                cls = f"{verb}-{key.split('/')[0]}"
            a = agg.setdefault(cls, [0, 0.0, 0.0])
            a[0] += 1
            a[1] += dur
            a[2] = max(a[2], dur)
    return {k: {"n": int(v[0]), "sum_s": round(v[1], 3), "max_s": round(v[2], 3)} for k, v in agg.items()}


def _median(rows: list[dict], key: str) -> float | None:
    vals = [r[key] for r in rows if r.get(key) is not None]
    return statistics.median(vals) if vals else None


def _fmt(value: float | None, width: int, prec: int) -> str:
    return f"{'-':>{width}}" if value is None else f"{value:>{width}.{prec}f}"


def summarize(results: list[dict], variants: list[str]) -> str:
    """Per-step tables: per-variant medians and round-paired deltas vs the first variant.

    Failed runs (rc != 0) are excluded. d_wall%/d_rss% are medians of the
    per-round ratios against the baseline's run in the same round; `faster`
    counts rounds where the variant's wall time beat the baseline's.
    """
    lines: list[str] = []
    for step in STEPS:
        rows = [r for r in results if r["step"] == step and r["rc"] == 0]
        if not rows:
            continue
        lines.append(f"== {step} ==")
        lines.append(f"{'variant':<16}{'n':>3}{'wall_s':>9}{'cpu_s':>8}{'rss_MiB':>9}{'anon_MiB':>9}"
                     f"{'bw_mean':>9}{'bw_p10':>8}{'bw_max':>8}{'cv':>6}{'idle':>6}"
                     f"{'d_wall%':>9}{'faster':>7}{'d_rss%':>8}")
        base_by_round = {r["round"]: r for r in rows if r["variant"] == variants[0]}
        for name in variants:
            mine = [r for r in rows if r["variant"] == name]
            if not mine:
                continue
            for r in mine:
                r.setdefault("cpu_s", r["user_s"] + r["sys_s"])
            paired = [(r, base_by_round[r["round"]]) for r in mine if r["round"] in base_by_round]
            if name == variants[0] or not paired:
                dwall, faster, drss = f"{'-':>9}", f"{'-':>7}", f"{'-':>8}"
            else:
                dw = [100 * (r["wall_s"] / b["wall_s"] - 1) for r, b in paired]
                dr = [100 * (r["maxrss_mib"] / b["maxrss_mib"] - 1) for r, b in paired]
                dwall = f"{statistics.median(dw):>+9.1f}"
                faster = f"{f'{sum(1 for d in dw if d < 0)}/{len(dw)}':>7}"
                drss = f"{statistics.median(dr):>+8.1f}"
            lines.append(
                f"{name:<16}{len(mine):>3}{_fmt(_median(mine, 'wall_s'), 9, 2)}{_fmt(_median(mine, 'cpu_s'), 8, 1)}"
                f"{_fmt(_median(mine, 'maxrss_mib'), 9, 1)}{_fmt(_median(mine, 'peak_anon_mib'), 9, 1)}"
                f"{_fmt(_median(mine, 'mean_mibs'), 9, 1)}{_fmt(_median(mine, 'p10_mibs'), 8, 1)}"
                f"{_fmt(_median(mine, 'max_mibs'), 8, 1)}{_fmt(_median(mine, 'cv'), 6, 2)}"
                f"{_fmt(_median(mine, 'idle_frac'), 6, 2)}{dwall}{faster}{drss}"
            )
        lines.append("")
    lines.append("wall/cpu in s, rss/anon in MiB (ru_maxrss / polled peak RssAnon); "
                 "bw_* in MiB/s over 250 ms buckets of the shaped port's byte counter (both directions)")
    lines.append("d_wall%/d_rss% = median round-paired delta vs the first variant; faster = rounds with lower wall")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Backends: fresh repo per run, cleaned up afterwards
# ---------------------------------------------------------------------------


def _checked_child(parent: str, name: str) -> str:
    """parent/name, refusing empty or path-like components (guards every rmtree)."""
    if not parent or not os.path.isabs(parent) or os.path.realpath(parent) == "/":
        raise ValueError(f"refusing unsafe parent directory: {parent!r}")
    if not name or "/" in name or name in (".", ".."):
        raise ValueError(f"refusing unsafe path component: {name!r}")
    return os.path.join(parent, name)


def _du_bytes(path: str) -> int:
    r = subprocess.run(["du", "-sb", path], capture_output=True, text=True, check=False)
    parts = r.stdout.split()
    return int(parts[0]) if r.returncode == 0 and parts else 0


class Backend:
    name = ""

    def __init__(self, run_id: str, defaults: cfg.Defaults, args: argparse.Namespace) -> None:
        self.run_id = run_id
        self.defaults = defaults
        self.args = args

    def endpoint_url(self) -> str:
        """URL whose host/port net-ab shapes (any tag)."""
        return self.repo_url("probe")

    def repo_url(self, tag: str) -> str:
        raise NotImplementedError

    def preflight(self, out_dir: str) -> None:
        """Check the service is up before anything is changed."""

    def prepare(self, tag: str) -> None:
        """Called before `vykar init` for run `tag`."""

    def repo_bytes(self, tag: str) -> int:
        return 0

    def cleanup(self, tag: str, vykar_argv: list[str], env: dict[str, str], log_path: str) -> None:
        raise NotImplementedError

    def finish(self) -> None:
        """Called once at the end of the sweep (after every cleanup)."""


def _vykar_delete(vykar_argv: list[str], env: dict[str, str], log_path: str) -> int:
    with open(log_path, "a") as log:
        return subprocess.run([*vykar_argv, "delete", "-R", REPO_LABEL, "--yes-delete-this-repo"], env=env,
                              stdout=log, stderr=subprocess.STDOUT, check=False).returncode


class LocalBackend(Backend):
    """Repos under <work-dir>/<run-id>/<tag>.repo (the run dir is removed at the end)."""

    name = "local"

    def __init__(self, run_id: str, defaults: cfg.Defaults, args: argparse.Namespace, run_dir: str) -> None:
        super().__init__(run_id, defaults, args)
        self.run_dir = run_dir

    def _path(self, tag: str) -> str:
        return _checked_child(self.run_dir, f"{tag}.repo")

    def repo_url(self, tag: str) -> str:
        return self._path(tag)

    def prepare(self, tag: str) -> None:
        os.makedirs(self._path(tag))

    def repo_bytes(self, tag: str) -> int:
        return _du_bytes(self._path(tag))

    def cleanup(self, tag: str, vykar_argv: list[str], env: dict[str, str], log_path: str) -> None:
        shutil.rmtree(self._path(tag), ignore_errors=True)


class S3Backend(Backend):
    """Repos at s3+http://<endpoint>/<bucket>/net-ab/<run-id>/<tag>.

    The endpoint comes from `config.resolve_repo_url("s3", ...)`. The bucket is
    created if missing and removed again at the end if net-ab created it and
    it is empty. Cleanup deletes exactly the objects under the run's prefix.
    """

    name = "s3"

    def __init__(self, run_id: str, defaults: cfg.Defaults, args: argparse.Namespace) -> None:
        super().__init__(run_id, defaults, args)
        ref = urllib.parse.urlparse(cfg.resolve_repo_url("s3", REPO_LABEL, defaults))
        self.scheme, self.netloc = ref.scheme, ref.netloc
        self.bucket = args.s3_bucket
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket):
            raise ValueError(f"invalid S3 bucket name: {self.bucket!r}")
        self.created_bucket = False
        self._client = None

    def _prefix(self, tag: str) -> str:
        for part in (self.run_id, tag):
            if not part or "/" in part:
                raise ValueError(f"refusing unsafe S3 prefix component: {part!r}")
        return f"{REPO_LABEL}/{self.run_id}/{tag}"

    def repo_url(self, tag: str) -> str:
        return f"{self.scheme}://{self.netloc}/{self.bucket}/{self._prefix(tag)}"

    def client(self):
        if self._client is None:
            from minio import Minio

            self._client = Minio(self.netloc, access_key=self.defaults.s3_access_key,
                                 secret_key=self.defaults.s3_secret_key,
                                 secure=self.scheme == "s3+https" or self.scheme == "s3",
                                 region=self.defaults.s3_region)
        return self._client

    def preflight(self, out_dir: str) -> None:
        scheme = "https" if self.scheme in ("s3", "s3+https") else "http"
        try:
            urllib.request.urlopen(f"{scheme}://{self.netloc}/minio/health/live", timeout=10).close()
        except OSError as exc:
            raise RuntimeError(f"S3 endpoint {self.netloc} is not healthy: {exc}") from exc
        if not self.client().bucket_exists(self.bucket):
            self.client().make_bucket(self.bucket)
            self.created_bucket = True
            _log(f"[s3] created bucket {self.bucket} (removed again at the end if empty)")

    def _objects(self, tag: str):
        return self.client().list_objects(self.bucket, prefix=self._prefix(tag) + "/", recursive=True)

    def repo_bytes(self, tag: str) -> int:
        return sum(o.size or 0 for o in self._objects(tag))

    def cleanup(self, tag: str, vykar_argv: list[str], env: dict[str, str], log_path: str) -> None:
        from minio.deleteobjects import DeleteObject

        names = [o.object_name for o in self._objects(tag)]
        errors = list(self.client().remove_objects(self.bucket, [DeleteObject(n) for n in names]))
        for e in errors:
            _warn(f"[s3] could not delete {e.name}: {e.message}")
        left = sum(1 for _ in self._objects(tag))
        if left:
            _warn(f"[s3] {left} objects left under {self.bucket}/{self._prefix(tag)}/")

    def finish(self) -> None:
        if self.created_bucket:
            if any(True for _ in self.client().list_objects(self.bucket, recursive=True)):
                _warn(f"[s3] bucket {self.bucket} is not empty; leaving it")
            else:
                self.client().remove_bucket(self.bucket)
                _log(f"[s3] removed bucket {self.bucket}")


class RestBackend(Backend):
    """vykar-server serves ONE repository at its root, so every run reuses it.

    The server must be empty at the start: a repository already there is
    either refused (default) or, with --rest-stash-existing, downloaded into
    the out dir before the sweep and uploaded again at the end. Cleanup is
    `vykar delete` (as the other testbench tools do) plus an HTTP DELETE of
    anything the run left behind — safe because the server held nothing else.
    """

    name = "rest"

    def __init__(self, run_id: str, defaults: cfg.Defaults, args: argparse.Namespace) -> None:
        super().__init__(run_id, defaults, args)
        self.base = defaults.rest_url.rstrip("/")
        self.token = defaults.rest_token
        self.stash_dir: str | None = None
        self.stashed: list[str] = []
        self.original: set[str] = set()

    def repo_url(self, tag: str) -> str:
        return self.defaults.rest_url

    def _request(self, method: str, key: str, data: bytes | None = None, query: str = "") -> tuple[int, bytes]:
        url = f"{self.base}/{urllib.parse.quote(key, safe='/')}" + (f"?{query}" if query else "")
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Authorization": f"Bearer {self.token}"})
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()

    def list_sized(self) -> list[dict]:
        status, body = self._request("GET", "", query="list&sized")
        if status != 200:
            raise RuntimeError(f"REST list failed: HTTP {status} {body[:200]!r}")
        return json.loads(body)

    def preflight(self, out_dir: str) -> None:
        try:
            urllib.request.urlopen(f"{self.base}/health", timeout=10).close()
        except OSError as exc:
            raise RuntimeError(f"REST server {self.base} is not healthy: {exc}") from exc
        keys = self.list_sized()
        if not keys:
            return
        if not self.args.rest_stash_existing:
            raise RuntimeError(
                f"the REST server at {self.base} already holds {len(keys)} objects (a repository?). vykar-server "
                "serves a single repository and net-ab needs it; re-run with --rest-stash-existing to save it under "
                "the out dir and put it back afterwards, or empty the server yourself")
        total = sum(k["size"] for k in keys)
        if any(k["key"].startswith("packs/") for k in keys) or total > REST_STASH_MAX_BYTES:
            raise RuntimeError(f"refusing to stash the existing REST repository ({len(keys)} objects, {total} bytes, "
                               "packs present or too large); empty the server yourself")
        self.stash_dir = os.path.join(out_dir, "rest-stash")
        for k in keys:
            status, body = self._request("GET", k["key"])
            if status != 200:
                raise RuntimeError(f"REST stash: GET {k['key']} -> HTTP {status}")
            path = os.path.join(self.stash_dir, k["key"])
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(body)
        with open(os.path.join(out_dir, "rest-stash.json"), "w") as f:
            json.dump(keys, f, indent=1)
        # Delete only after every object is safely on disk.
        self.original = {k["key"] for k in keys}
        for k in keys:
            status, _ = self._request("DELETE", k["key"])
            if status not in (200, 204, 404):
                raise RuntimeError(f"REST stash: DELETE {k['key']} -> HTTP {status}")
            self.stashed.append(k["key"])
        _log(f"[rest] stashed the existing repository ({len(keys)} objects, {total} bytes) in {self.stash_dir}")

    def repo_bytes(self, tag: str) -> int:
        return sum(k["size"] for k in self.list_sized())

    def cleanup(self, tag: str, vykar_argv: list[str], env: dict[str, str], log_path: str) -> None:
        _vykar_delete(vykar_argv, env, log_path)
        for k in self.list_sized():
            self._request("DELETE", k["key"])
        left = self.list_sized()
        if left:
            raise RuntimeError(f"REST server still holds {len(left)} objects after cleanup of {tag}")

    def finish(self) -> None:
        if not self.stashed or not self.stash_dir:
            return
        # Objects of the original repository that were never deleted (an
        # interrupted stash) may still be there; anything else is foreign.
        foreign = [k["key"] for k in self.list_sized() if k["key"] not in self.original]
        if foreign:
            _warn(f"[rest] server holds {len(foreign)} unexpected objects; NOT restoring the stashed repository "
                  f"(kept in {self.stash_dir})")
            return
        if "config" in self.stashed:
            # Recreate the server-side layout `vykar init` produced (keys/,
            # snapshots/, locks/, packs/00..ff; sessions/ appears on first
            # use): `vykar delete` removed the empty directories and a plain
            # PUT only creates the parents of the keys it writes.
            for key, query in (("", "init"), ("sessions", "mkdir")):
                status, body = self._request("POST", key, data=b"", query=query)
                if status not in (200, 201, 204):
                    _warn(f"[rest] POST {key}?{query} -> HTTP {status} {body[:200]!r}")
        for key in self.stashed:
            with open(os.path.join(self.stash_dir, key), "rb") as f:
                status, body = self._request("PUT", key, data=f.read())
            if status not in (200, 201, 204):
                _warn(f"[rest] restoring {key} failed: HTTP {status} {body[:200]!r} (stash kept in {self.stash_dir})")
                return
        _log(f"[rest] restored the stashed repository ({len(self.stashed)} objects)")


class SftpBackend(Backend):
    """Repos at sftp://user@host:port<base>/net-ab-sftp/<run-id>/<tag>.

    When the SFTP host is loopback and the base dir exists locally (the usual
    testbench setup: same filesystem, no chroot), the run directory is created
    and removed locally — a `vykar delete` over a 40 ms SFTP link takes
    minutes, one round trip per object and directory. Otherwise cleanup is
    `vykar delete`, as the other testbench tools do. The per-run and
    net-ab-sftp parents are rmdir'ed at the end when empty and ours.
    """

    name = "sftp"
    SUBDIR = "net-ab-sftp"

    def __init__(self, run_id: str, defaults: cfg.Defaults, args: argparse.Namespace) -> None:
        super().__init__(run_id, defaults, args)
        self.base = defaults.sftp_base_dir.rstrip("/")
        if not self.base.startswith("/"):
            raise ValueError(f"SFTP_BASE_DIR must be absolute, got {defaults.sftp_base_dir!r}")
        self.parent = _checked_child(self.base, self.SUBDIR)
        self.run_dir = _checked_child(self.parent, run_id)
        self.local = is_ipv4_loopback(defaults.sftp_host) and os.path.isdir(self.base)
        self.created: list[str] = []

    def _path(self, tag: str) -> str:
        return _checked_child(self.run_dir, tag)

    def repo_url(self, tag: str) -> str:
        d = self.defaults
        return f"sftp://{d.sftp_user}@{d.sftp_host}:{d.sftp_port}{self._path(tag)}"

    def prepare(self, tag: str) -> None:
        if not self.local:
            return
        for d in (self.parent, self.run_dir):
            if not os.path.isdir(d):
                os.mkdir(d)
                self.created.append(d)
        os.mkdir(self._path(tag))  # fails if it exists: the directory must be ours

    def repo_bytes(self, tag: str) -> int:
        return _du_bytes(self._path(tag)) if self.local else 0

    def cleanup(self, tag: str, vykar_argv: list[str], env: dict[str, str], log_path: str) -> None:
        if self.local:
            shutil.rmtree(self._path(tag), ignore_errors=True)
        else:
            _vykar_delete(vykar_argv, env, log_path)

    def finish(self) -> None:
        for d in reversed(self.created):
            with contextlib.suppress(OSError):
                os.rmdir(d)  # only succeeds when empty


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _raise_exit(signum: int, _frame) -> None:
    raise SystemExit(128 + signum)


@contextlib.contextmanager
def _shielded():
    """Ignore SIGINT/SIGTERM/SIGHUP while cleanup runs, so a second Ctrl-C cannot cut it short."""
    sigs = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    old = {s: signal.signal(s, signal.SIG_IGN) for s in sigs}
    try:
        yield
    finally:
        for s, h in old.items():
            signal.signal(s, h)


def make_backend(name: str, run_id: str, defaults: cfg.Defaults, args: argparse.Namespace, run_dir: str) -> Backend:
    if name == "local":
        return LocalBackend(run_id, defaults, args, run_dir)
    if name == "s3":
        return S3Backend(run_id, defaults, args)
    if name == "rest":
        return RestBackend(run_id, defaults, args)
    if name == "sftp":
        return SftpBackend(run_id, defaults, args)
    raise ValueError(f"unknown backend: {name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="net-ab",
        description="Interleaved A/B benchmark of `vykar backup` against s3/rest/sftp over a netem-shaped loopback "
                    "port (local = unshaped reference).",
    )
    parser.add_argument("--variant", action="append", default=[], metavar="NAME=BINARY",
                        help="a vykar binary to measure; repeat. The first is the baseline.")
    parser.add_argument("--variant-conn", action="append", default=[], metavar="NAME=N",
                        help="per-variant limits.connections; repeat")
    parser.add_argument("--variant-threads", action="append", default=[], metavar="NAME=N",
                        help="per-variant limits.threads; repeat")
    parser.add_argument("--variant-env", action="append", default=[], metavar="NAME:KEY=VALUE",
                        help="extra environment variable for one variant; repeat")
    parser.add_argument("--rounds", type=int, default=3, help="runs per variant (default: 3)")
    parser.add_argument("--backend", choices=BACKENDS, default="s3")
    parser.add_argument("--source", default=None, help="directory to back up (default: $CORPUS_LOCAL/snapshot-1)")
    parser.add_argument("--connections", type=int, default=None, help="limits.connections (default: vykar's)")
    parser.add_argument("--threads", type=int, default=None, help="limits.threads (default: vykar's auto)")
    parser.add_argument("--incremental", action="store_true", help="also time a no-change second backup")
    parser.add_argument("--delay-ms", type=float, default=20,
                        help="one-way delay on the backend port (RTT = 2x; default 20). 0 = no delay; the port is "
                             "still routed through a count-only pfifo so bandwidth is measured")
    parser.add_argument("--rate", default=None, help="optional netem rate, e.g. 200mbit")
    parser.add_argument("--keep-netem", action="store_true",
                        help="leave the shaping in place on exit instead of restoring the previous lo state")
    parser.add_argument("--out", default=None, help="results directory (default: $RUNTIME_ROOT/net-ab/<stamp>)")
    parser.add_argument("--work-dir", default=None,
                        help="scratch dir for caches and local repos (default: <REPO_ROOT>/net-ab)")
    parser.add_argument("--s3-bucket", default="vykar-net-ab", help="bucket for s3 repos (default: vykar-net-ab)")
    parser.add_argument("--rest-stash-existing", action="store_true",
                        help="rest only: save a repository already on the server, and put it back afterwards")
    args = parser.parse_args()

    try:
        variants = parse_net_variants(args.variant, args.variant_env, args.variant_conn, args.variant_threads,
                                      args.connections, args.threads)
    except ValueError as exc:
        _die(str(exc))
    for v in variants:
        if not (os.path.isfile(v.binary) and os.access(v.binary, os.X_OK)):
            _die(f"not an executable for variant {v.name}: {v.binary}")
    if args.rounds < 1:
        _die("--rounds must be >= 1")
    if args.delay_ms < 0:
        _die("--delay-ms must be >= 0")

    defaults = cfg.load_defaults()
    source = os.path.abspath(os.path.expanduser(args.source or os.path.join(defaults.corpus_local, "snapshot-1")))
    if not os.path.isdir(source):
        _die(f"source not found: {source}")
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    run_id = f"{stamp}-{os.getpid()}"
    out_dir = os.path.abspath(os.path.expanduser(args.out or os.path.join(defaults.runtime_root, "net-ab", stamp)))
    os.makedirs(out_dir, exist_ok=True)
    work = os.path.abspath(os.path.expanduser(args.work_dir or os.path.join(defaults.repo_root, "net-ab")))
    run_dir = _checked_child(work, run_id)

    try:
        backend = make_backend(args.backend, run_id, defaults, args, run_dir)
        endpoint = backend_endpoint(args.backend, backend.endpoint_url())
    except ValueError as exc:
        _die(str(exc))
    want: Shaping | None = None
    if endpoint is not None:
        host, port = endpoint
        if not is_ipv4_loopback(host):
            _die(f"{args.backend} endpoint {host}:{port} is not an IPv4 loopback address; net-ab only shapes {DEV}")
        want = Shaping((port,), args.delay_ms, args.rate)
    elif args.delay_ms or args.rate:
        _log("[netem] local backend: no network, no shaping")

    # Measurements share netem, MinIO and the CPU with s3bench.py and mem-ab.
    locks = []
    for path in LOCK_PATHS:
        lock = open(path, "a")
        _log(f"waiting for {path} ...")
        fcntl.flock(lock, fcntl.LOCK_EX)
        locks.append(lock)
    _log(f"locks acquired; out={out_dir}")

    signal.signal(signal.SIGTERM, _raise_exit)
    signal.signal(signal.SIGHUP, _raise_exit)

    work_created = not os.path.exists(work)
    os.makedirs(run_dir)  # fails if it exists: everything under it is ours
    previous: Shaping | None = None
    netem_touched = False
    results: list[dict] = []
    try:
        if want is not None:
            try:
                previous = read_lo_state()
            except ValueError as exc:
                _die(f"refusing to modify {DEV}: {exc}")
            _log(f"[netem] previous state: {describe_state(previous)}")
            if previous != want:
                netem_touched = True
                apply_lo_state(want)
            _log(f"[netem] now: {describe_state(want)}")
            if leaf_bytes() is None:
                _die("shaped band byte counter not readable")
            rtt = 2 * args.delay_ms
            for w in window_warnings(socket_buffer_limits(), rtt):
                _warn(w)
            if args.backend == "sftp" and rtt > 0:
                _log("[netem] note: SFTP upload throughput is also bounded by the server's SSH channel window "
                     "(OpenSSH: 2 MiB) / RTT per connection")

        backend.preflight(out_dir)
        with open(os.path.join(out_dir, "sweep.json"), "w") as f:
            json.dump({"backend": args.backend, "source": source, "rounds": args.rounds, "delay_ms": args.delay_ms,
                       "rate": args.rate, "endpoint": endpoint, "previous_lo_state": describe_state(previous),
                       "variants": [vars(v) for v in variants], "run_id": run_id}, f, indent=1, default=str)

        base_env = os.environ.copy()
        base_env["VYKAR_PASSPHRASE"] = defaults.passphrase
        base_env.pop("VYKAR_HTTP_TRACE", None)
        shaped = want is not None
        for rnd in range(1, args.rounds + 1):
            for v in memab.round_order(variants, rnd - 1):
                tag = f"r{rnd}.{v.name}"
                prefix = os.path.join(out_dir, tag)
                cache = _checked_child(run_dir, "cache")
                shutil.rmtree(cache, ignore_errors=True)
                os.makedirs(cache)
                conf = prefix + ".yaml"
                memab.write_bench_config(conf, backend=args.backend, repo_label=REPO_LABEL, repo_url=backend.repo_url(tag),
                                         cache_dir=cache, source=source, threads=v.threads,
                                         connections=v.connections, defaults=defaults)
                env = {**base_env, **v.env}
                vykar_argv = [v.binary, "--config", conf]
                backend.prepare(tag)
                try:
                    with open(prefix + ".init.log", "w") as log:
                        rc = subprocess.run([*vykar_argv, "init", "-R", REPO_LABEL], env=env, stdout=log,
                                            stderr=subprocess.STDOUT, check=False).returncode
                    if rc != 0:
                        raise RuntimeError(f"init failed, see {prefix}.init.log")
                    subprocess.run(["sync"], check=False)
                    steps = [("full", prefix)] + ([("incr", prefix + ".incr")] if args.incremental else [])
                    for step, pfx in steps:
                        trace = pfx + ".http.tsv"
                        run_env = {**env, "VYKAR_HTTP_TRACE": trace}
                        samples: list[dict[str, int]] = []
                        net_before = leaf_bytes() if shaped else None
                        rc, rss_kb, wall, user, sys_s, peaks = memab.run_measured(
                            [*vykar_argv, "backup", "-R", REPO_LABEL, "-l", SNAPSHOT_LABEL, source],
                            run_env, pfx + ".log", pfx + ".timeline.tsv", interval=POLL_INTERVAL_S,
                            extra=(lambda: {"net_bytes": leaf_bytes() or 0}) if shaped else None,
                            extra_fields=("net_bytes",) if shaped else (), samples_out=samples)
                        net_after = leaf_bytes() if shaped else None
                        r: dict = {
                            "round": rnd, "variant": v.name, "step": step, "rc": rc, "wall_s": round(wall, 2),
                            "user_s": round(user, 2), "sys_s": round(sys_s, 2), "cpu_s": round(user + sys_s, 2),
                            "maxrss_mib": round(rss_kb / 1024, 1),
                            "peak_anon_mib": round(peaks.get("RssAnon", 0) / 1024, 1),
                            "peak_threads": peaks.get("Threads", 0),
                            "connections": v.connections, "threads": v.threads,
                        }
                        if shaped:
                            stats, series = bw_stats([(s["t_ms"] / 1000, s["net_bytes"]) for s in samples])
                            r.update({k: round(val, 3) for k, val in stats.items()})
                            if net_before is not None and net_after is not None:
                                r["net_mib"] = round((net_after - net_before) / 2**20, 1)
                                r["avg_mibs"] = round(r["net_mib"] / wall, 1) if wall else None
                            if series:
                                with open(pfx + ".bw.tsv", "w") as f:
                                    f.write("t_s\tmibs\n" + "".join(f"{t:.2f}\t{val:.1f}\n" for t, val in series))
                        r["http"] = trace_summary(trace)
                        if not r["http"] and os.path.exists(trace) and os.path.getsize(trace) == 0:
                            os.unlink(trace)
                        if step == "full":
                            r["repo_mib"] = round(backend.repo_bytes(tag) / 2**20, 1)
                        results.append(r)
                        _log(f"{tag} {step}: wall={r['wall_s']}s cpu={r['user_s']}+{r['sys_s']}s "
                             f"rss={r['maxrss_mib']}MiB anon={r['peak_anon_mib']} "
                             f"bw mean={r.get('mean_mibs', '-')} p10={r.get('p10_mibs', '-')} "
                             f"max={r.get('max_mibs', '-')} cv={r.get('cv', '-')} idle={r.get('idle_frac', '-')} "
                             f"avg={r.get('avg_mibs', '-')} rc={rc}")
                        if rc != 0:
                            _log(f"  !! non-zero exit, see {pfx}.log")
                finally:
                    with _shielded():
                        try:
                            backend.cleanup(tag, vykar_argv, env, prefix + ".cleanup.log")
                        finally:
                            with open(os.path.join(out_dir, "results.json"), "w") as f:
                                json.dump(results, f, indent=1)
    except RuntimeError as exc:
        _die(str(exc))
    finally:
        with _shielded():
            try:
                backend.finish()
            except Exception as exc:  # noqa: BLE001 - must not mask the netem restore
                _warn(f"backend cleanup at exit failed: {exc}")
            shutil.rmtree(run_dir, ignore_errors=True)
            if work_created:
                with contextlib.suppress(OSError):
                    os.rmdir(work)
            if netem_touched and not args.keep_netem:
                try:
                    apply_lo_state(previous)
                    _log(f"[netem] restored: {describe_state(previous)}")
                except Exception as exc:  # noqa: BLE001
                    _warn(f"could not restore {DEV}: {exc}; previous state was {describe_state(previous)}")
            elif want is not None:
                _log(f"[netem] left as: {describe_state(want)}")
            for lock in locks:
                fcntl.flock(lock, fcntl.LOCK_UN)

    summary = summarize(results, [v.name for v in variants])
    with open(os.path.join(out_dir, "summary.txt"), "w") as f:
        f.write(summary + "\n")
    print()
    print(summary)
    print(f"\nresults: {out_dir}/results.json")
    if any(r["rc"] != 0 for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
