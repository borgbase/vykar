"""Run the put-if-absent probe against loopback storage targets.

Builds (or takes) `crates/vykar-storage/examples/put_if_absent_probe.rs`, runs
it once per target, keeps each target's JSON report, and writes one Markdown
table with a row per target, ready to copy into backends.md.

Targets:
- ``local``: a fresh directory under ``$TMPDIR``.
- ``sftp-exec``: a fresh directory under ``$TMPDIR``, spoken to through a local
  ``sftp-server`` binary over its stdio (no SSH).
- ``s3``: an S3 URL (default the testbench MinIO); the bucket is created first.
  Credentials go to the probe's environment, never its argv.
- ``sftp``: a real SSH server, only with ``--sftp-url``.

S3 and SFTP targets must be on a loopback host unless ``--allow-remote`` is
given; the check runs before anything is built or started.
"""

import argparse
import datetime
import ipaddress
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from . import config as cfg

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_S3_URL = "s3+http://127.0.0.1:9000/vykar-probe/runs"
TARGETS = ("local", "sftp-exec", "s3", "sftp")
DEFAULT_TARGETS = "local,sftp-exec,s3"
SFTP_SERVERS = (
    "/usr/lib/openssh/sftp-server",
    "/usr/libexec/sftp-server",
    "/usr/lib/ssh/sftp-server",
)
BUILD_CMD = [
    "cargo",
    "build",
    "--release",
    "-p",
    "vykar-storage",
    "--features",
    "backend-sftp",
    "--example",
    "put_if_absent_probe",
]
VERDICTS = {
    0: "works",
    1: "ignores_condition",
    2: "unsupported",
    3: "inconclusive",
    4: "works, cleanup incomplete",
    5: "stale_listing",
}


def url_host(url: str) -> str:
    """The host of an s3://, s3+http:// or sftp:// URL, lower-case, no port."""
    normalized = url.replace("s3+http://", "http://", 1).replace("s3://", "https://", 1)
    return (urlparse(normalized).hostname or "").lower()


def is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def remote_targets(targets: list[str], s3_url: str, sftp_url: str | None) -> list[str]:
    """The S3/SFTP targets whose host is not loopback, as "target (host)"."""
    out = []
    for target, url in (("s3", s3_url), ("sftp", sftp_url)):
        if target in targets and url is not None and not is_loopback(url_host(url)):
            out.append(f"{target} ({url_host(url) or url})")
    return out


def default_probe_bin() -> Path:
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", REPO_ROOT / "target"))
    return target_dir / "release" / "examples" / "put_if_absent_probe"


def find_sftp_server() -> str | None:
    return next((p for p in SFTP_SERVERS if os.path.exists(p)), None)


def global_args(args: argparse.Namespace, label: str, json_path: Path) -> list[str]:
    return [
        "--label",
        label,
        "--json",
        str(json_path),
        "--race-writers",
        str(args.race_writers),
        "--race-rounds",
        str(args.race_rounds),
        "--list-rounds",
        str(args.list_rounds),
    ]


def target_command(
    target: str,
    args: argparse.Namespace,
    defaults: cfg.Defaults,
    scratch: Path,
) -> tuple[list[str], dict[str, str]]:
    """The probe's target arguments and extra environment for `target`."""
    if target == "local":
        root = Path(tempfile.mkdtemp(prefix="probe-local-", dir=scratch))
        return ["local", str(root)], {}
    if target == "sftp-exec":
        server = args.sftp_server or find_sftp_server()
        if server is None:
            raise RuntimeError("sftp-exec: no sftp-server found; pass --sftp-server")
        root = Path(tempfile.mkdtemp(prefix="probe-sftp-exec-", dir=scratch))
        return ["sftp-exec", str(root), "--", server], {}
    if target == "s3":
        argv = ["s3", args.s3_url, "--region", defaults.s3_region]
        if args.s3_url.startswith("s3+http://"):
            argv.append("--allow-insecure-http")
        env = {
            "AWS_ACCESS_KEY_ID": defaults.s3_access_key,
            "AWS_SECRET_ACCESS_KEY": defaults.s3_secret_key,
        }
        return argv, env
    if target == "sftp":
        argv = ["sftp", args.sftp_url]
        if defaults.sftp_key:
            argv += ["--key", defaults.sftp_key]
        if defaults.sftp_known_hosts:
            argv += ["--known-hosts", defaults.sftp_known_hosts]
        return argv, {}
    raise ValueError(f"unknown target: {target}")


def combined_markdown(outputs: list[str]) -> str:
    """One header and one row per probe stdout (header lines, then the row)."""
    header: list[str] = []
    rows: list[str] = []
    for out in outputs:
        lines = [line for line in out.splitlines() if line.startswith("|")]
        if len(lines) < 3:
            continue
        if not header:
            header = lines[:2]
        rows.append(lines[-1])
    if not header:
        return ""
    return "\n".join(header + rows) + "\n"


def aggregate_exit(codes: list[int]) -> int:
    """0 when every target exited 0, else the first non-zero code."""
    return next((c for c in codes if c != 0), 0)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="probe", description=__doc__.splitlines()[0])
    p.add_argument("--targets", default=DEFAULT_TARGETS, help=f"comma-separated, of {', '.join(TARGETS)}")
    p.add_argument("--out", help="output directory (default <runtime_root>/probe-runs)")
    p.add_argument("--label", default="", help="text prefixed to each target's label")
    p.add_argument("--race-writers", type=int, default=8)
    p.add_argument("--race-rounds", type=int, default=10)
    p.add_argument("--list-rounds", type=int, default=10)
    p.add_argument("--probe-bin", help="use this probe binary instead of building it")
    p.add_argument("--s3-url", default=DEFAULT_S3_URL)
    p.add_argument("--sftp-url", help="sftp://user@host[:port]/path (enables the sftp target)")
    p.add_argument("--sftp-server", help="sftp-server binary for sftp-exec")
    p.add_argument("--allow-remote", action="store_true", help="allow non-loopback S3/SFTP hosts")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    targets = [t.strip() for t in args.targets.split(",") if t.strip()]
    unknown = [t for t in targets if t not in TARGETS]
    if unknown:
        print(f"probe: unknown target(s): {', '.join(unknown)}", file=sys.stderr)
        return 2
    if "sftp" in targets and not args.sftp_url:
        print("probe: the sftp target needs --sftp-url", file=sys.stderr)
        return 2
    remote = remote_targets(targets, args.s3_url, args.sftp_url)
    if remote and not args.allow_remote:
        print(
            f"probe: refusing non-loopback target(s) {', '.join(remote)}; pass --allow-remote",
            file=sys.stderr,
        )
        return 2

    defaults = cfg.load_defaults()
    out_dir = Path(args.out or os.path.join(defaults.runtime_root, "probe-runs"))
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.probe_bin:
        probe_bin = Path(args.probe_bin)
    else:
        subprocess.run(BUILD_CMD, cwd=REPO_ROOT, check=True)
        probe_bin = default_probe_bin()

    if "s3" in targets:
        cfg.ensure_backend_ready("s3", args.s3_url, defaults)

    day = datetime.date.today().isoformat()
    scratch = Path(tempfile.mkdtemp(prefix="vykar-probe-"))
    outputs: list[str] = []
    codes: list[int] = []
    for target in targets:
        label = f"{args.label} {target}".strip()
        json_path = out_dir / f"{day}-{target}.json"
        target_argv, extra_env = target_command(target, args, defaults, scratch)
        cmd = [str(probe_bin), *global_args(args, label, json_path), *target_argv]
        result = subprocess.run(
            cmd,
            env={**os.environ, **extra_env},
            capture_output=True,
            text=True,
        )
        if result.stderr:
            sys.stderr.write(result.stderr)
        outputs.append(result.stdout)
        codes.append(result.returncode)
        verdict = VERDICTS.get(result.returncode, "error")
        print(f"probe {target}: {verdict} (exit {result.returncode})", file=sys.stderr)

    shutil.rmtree(scratch, ignore_errors=True)
    markdown = combined_markdown(outputs)
    md_path = out_dir / f"{day}-probe.md"
    md_path.write_text(markdown)
    sys.stdout.write(markdown)
    print(f"probe: wrote {md_path}", file=sys.stderr)
    return aggregate_exit(codes)


if __name__ == "__main__":
    sys.exit(main())
