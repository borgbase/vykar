#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import sys
from typing import Dict, List


OPS: List[str] = [
    "vykar_backup",
    "vykar_restore",
    "restic_backup",
    "restic_restore",
    "rustic_backup",
    "rustic_restore",
    "borg_backup",
    "borg_restore",
    "kopia_backup",
    "kopia_restore",
]

TOOLS: List[str] = ["vykar", "restic", "rustic", "borg", "kopia"]

TIMEV_FIELDS = {
    "elapsed": "Elapsed (wall clock) time (h:mm:ss or m:ss)",
    "cpu_pct": "Percent of CPU this job got",
    "user_s": "User time (seconds)",
    "sys_s": "System time (seconds)",
    "max_rss_kb": "Maximum resident set size (kbytes)",
    "voluntary_ctx_switches": "Voluntary context switches",
    "involuntary_ctx_switches": "Involuntary context switches",
    "fs_in": "File system inputs",
    "fs_out": "File system outputs",
    "exit_status": "Exit status",
}


def parse_kv_file(path: pathlib.Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        if ": " not in line:
            continue
        k, v = line.split(": ", 1)
        out[k.strip()] = v.strip()
    return out


def parse_timev(path: pathlib.Path) -> Dict[str, str]:
    raw = parse_kv_file(path)
    out: Dict[str, str] = {}
    for key, label in TIMEV_FIELDS.items():
        out[key] = raw.get(label, "NA")
    return out


def parse_repo_sizes(path: pathlib.Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    tool_map = {
        "bench-vykar": "vykar",
        "bench-restic": "restic",
        "bench-rustic": "rustic",
        "bench-borg": "borg",
        "bench-kopia": "kopia",
    }
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        size = parts[0]
        repo_path = parts[1]
        for suffix, tool in tool_map.items():
            if repo_path.endswith(suffix):
                out[tool] = size
                break
    return out


def parse_size_to_bytes(s: str) -> int | None:
    if not s or s == "NA":
        return None
    clean = s.strip()
    if clean.isdigit():
        return int(clean)

    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)([KMGTP]?)(?:i?B?)?$", clean, re.IGNORECASE)
    if not match:
        return None
    value = float(match.group(1))
    suffix = match.group(2).upper()
    scale = {
        "": 1,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
        "P": 1024**5,
    }.get(suffix)
    if scale is None:
        return None
    return int(value * scale)


def parse_elapsed_seconds(s: str) -> float | None:
    if not s or s == "NA":
        return None
    parts = s.split(":")
    try:
        if len(parts) == 2:
            mins = int(parts[0])
            secs = float(parts[1])
            return mins * 60.0 + secs
        if len(parts) == 3:
            hours = int(parts[0])
            mins = int(parts[1])
            secs = float(parts[2])
            return hours * 3600.0 + mins * 60.0 + secs
    except ValueError:
        return None
    return None


def parse_float(s: str) -> float | None:
    if not s or s == "NA":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_int(s: str) -> int | None:
    if not s or s == "NA":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_cpu_pct_value(s: str) -> float | None:
    if not s or s == "NA":
        return None
    clean = s.strip().replace("%", "")
    return parse_float(clean)


def mean_std(values: List[float | None]) -> tuple[float | None, float | None]:
    clean = [v for v in values if v is not None]
    if not clean:
        return None, None
    mean = float(statistics.fmean(clean))
    if len(clean) == 1:
        return mean, 0.0
    return mean, float(statistics.stdev(clean))


def parse_run_rc(timev_path: pathlib.Path) -> int | None:
    rc_file = pathlib.Path(str(timev_path).replace(".timev.txt", ".rc"))
    if not rc_file.exists():
        return None
    try:
        return int(rc_file.read_text(errors="replace").strip())
    except ValueError:
        return None


def parse_run_repo_size_bytes(timev_path: pathlib.Path) -> int | None:
    repo_size_file = pathlib.Path(str(timev_path).replace(".timev.txt", ".repo-size-bytes.txt"))
    if not repo_size_file.exists():
        return None
    return parse_int(repo_size_file.read_text(errors="replace").strip())


def list_timev_files(op_dir: pathlib.Path) -> List[pathlib.Path]:
    runs_dir = op_dir / "runs"
    if runs_dir.is_dir():
        return sorted(runs_dir.glob("run-*.timev.txt"))
    single = op_dir / "timev.txt"
    if single.exists():
        return [single]
    return []


def fmt_cpu_pct(v: float | None) -> str:
    if v is None:
        return "NA"
    return f"{v:.1f}%"


def _dataset_path_from_meta(meta: pathlib.Path) -> str | None:
    if not meta.exists():
        return None
    dataset: str | None = None
    benchmark_root: str | None = None
    benchmark_dataset: str | None = None
    for line in meta.read_text(errors="replace").splitlines():
        if line.startswith("dataset_benchmark="):
            benchmark_root = line.split("=", 1)[1].strip()
            break
        if line.startswith("dataset_snapshot_2="):
            benchmark_dataset = line.split("=", 1)[1].strip()
            continue
        if line.startswith("dataset="):
            dataset = line.split("=", 1)[1].strip()
    dataset_path = benchmark_root or benchmark_dataset or dataset
    return dataset_path


def find_dataset_path(root: pathlib.Path) -> str | None:
    dataset_path: str | None = None
    for op in OPS:
        dataset_path = _dataset_path_from_meta(root / f"profile.{op}" / "meta.txt")
        if dataset_path:
            break
    return dataset_path


def get_dataset_bytes(root: pathlib.Path) -> int | None:
    dataset_path = find_dataset_path(root)
    if not dataset_path:
        return None
    try:
        out = subprocess.check_output(["du", "-sb", dataset_path], text=True).strip()
        return int(out.split()[0])
    except Exception:
        return None


def get_dataset_file_count(root: pathlib.Path) -> int | None:
    dataset_path = find_dataset_path(root)
    if not dataset_path:
        return None
    try:
        total = 0
        for _root, _dirs, files in os.walk(dataset_path):
            total += len(files)
        return total
    except Exception:
        return None


def infer_run_timestamp_utc(root: pathlib.Path) -> str | None:
    m = re.match(r"^(\d{8}T\d{6}Z)$", root.name)
    if m:
        try:
            dt = datetime.datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=datetime.timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass

    for op in OPS:
        meta = root / f"profile.{op}" / "meta.txt"
        if not meta.exists():
            continue
        for line in meta.read_text(errors="replace").splitlines():
            if line.startswith("timestamp_utc="):
                ts = line.split("=", 1)[1].strip()
                if ts:
                    return ts
    return None


def build_records(root: pathlib.Path) -> tuple[List[dict], int | None, int | None]:
    repo_sizes = parse_repo_sizes(root / "repo-sizes.txt")
    repo_sizes_bytes = {tool: parse_size_to_bytes(size) for tool, size in repo_sizes.items()}
    dataset_bytes = get_dataset_bytes(root)
    dataset_files = get_dataset_file_count(root)
    dataset_mib = (dataset_bytes / (1024.0 * 1024.0)) if dataset_bytes else None

    records: List[dict] = []
    for op in OPS:
        tool, phase = op.split("_", 1)
        op_dir = root / f"profile.{op}"
        run_files = list_timev_files(op_dir)
        run_metrics: List[dict] = []
        for run_file in run_files:
            t = parse_timev(run_file)
            duration_s = parse_elapsed_seconds(t.get("elapsed", "NA"))
            throughput_mib_s: float | None = None
            user_s = parse_float(t.get("user_s", "NA"))
            sys_s = parse_float(t.get("sys_s", "NA"))
            if dataset_mib is not None and duration_s and duration_s > 0:
                throughput_mib_s = dataset_mib / duration_s
            exit_status = parse_int(t.get("exit_status", "NA"))
            if exit_status is None:
                exit_status = parse_run_rc(run_file)
            run_metrics.append(
                {
                    "duration_s": duration_s,
                    "throughput_mib_s": throughput_mib_s,
                    "cpu_pct_value": parse_cpu_pct_value(t.get("cpu_pct", "NA")),
                    "user_s": user_s,
                    "sys_s": sys_s,
                    "cpu_s": user_s + sys_s if user_s is not None and sys_s is not None else None,
                    "maxrss_kb": parse_int(t.get("max_rss_kb", "NA")),
                    "voluntary_ctx_switches": parse_int(t.get("voluntary_ctx_switches", "NA")),
                    "involuntary_ctx_switches": parse_int(t.get("involuntary_ctx_switches", "NA")),
                    "fs_in": parse_int(t.get("fs_in", "NA")),
                    "fs_out": parse_int(t.get("fs_out", "NA")),
                    "repo_size_bytes": parse_run_repo_size_bytes(run_file),
                    "exit_status": exit_status,
                }
            )

        duration_s, duration_s_std = mean_std([r["duration_s"] for r in run_metrics])
        throughput_mib_s, throughput_mib_s_std = mean_std([r["throughput_mib_s"] for r in run_metrics])
        cpu_pct_value, cpu_pct_std = mean_std([r["cpu_pct_value"] for r in run_metrics])
        user_s, user_s_std = mean_std([r["user_s"] for r in run_metrics])
        sys_s, sys_s_std = mean_std([r["sys_s"] for r in run_metrics])
        cpu_s, cpu_s_std = mean_std([r["cpu_s"] for r in run_metrics])
        maxrss_kb, maxrss_kb_std = mean_std(
            [float(r["maxrss_kb"]) if r["maxrss_kb"] is not None else None for r in run_metrics]
        )
        voluntary_ctx_switches, voluntary_ctx_switches_std = mean_std(
            [
                float(r["voluntary_ctx_switches"]) if r["voluntary_ctx_switches"] is not None else None
                for r in run_metrics
            ]
        )
        involuntary_ctx_switches, involuntary_ctx_switches_std = mean_std(
            [
                float(r["involuntary_ctx_switches"]) if r["involuntary_ctx_switches"] is not None else None
                for r in run_metrics
            ]
        )
        fs_in, fs_in_std = mean_std([float(r["fs_in"]) if r["fs_in"] is not None else None for r in run_metrics])
        fs_out, fs_out_std = mean_std(
            [float(r["fs_out"]) if r["fs_out"] is not None else None for r in run_metrics]
        )
        repo_size_bytes, repo_size_bytes_std = mean_std(
            [float(r["repo_size_bytes"]) if r["repo_size_bytes"] is not None else None for r in run_metrics]
        )
        if repo_size_bytes is None:
            fallback_bytes = repo_sizes_bytes.get(tool)
            if fallback_bytes is not None:
                repo_size_bytes = float(fallback_bytes)
                repo_size_bytes_std = 0.0
        run_count = len(run_metrics)
        failed_runs = 0
        nonzero_exits: List[int] = []
        for r in run_metrics:
            ex = r["exit_status"]
            if ex is None or ex != 0:
                failed_runs += 1
            if ex not in (None, 0):
                nonzero_exits.append(ex)
        if run_count == 0:
            exit_status = None
        elif nonzero_exits:
            exit_status = max(nonzero_exits)
        elif failed_runs > 0:
            exit_status = 1
        else:
            exit_status = 0
        records.append(
            {
                "op": op,
                "tool": tool,
                "phase": phase,
                "duration_s": duration_s,
                "duration_s_std": duration_s_std,
                "throughput_mib_s": throughput_mib_s,
                "throughput_mib_s_std": throughput_mib_s_std,
                "cpu_pct_raw": fmt_cpu_pct(cpu_pct_value),
                "cpu_pct_value": cpu_pct_value,
                "cpu_pct_std": cpu_pct_std,
                "user_s": user_s,
                "user_s_std": user_s_std,
                "sys_s": sys_s,
                "sys_s_std": sys_s_std,
                "cpu_s": cpu_s,
                "cpu_s_std": cpu_s_std,
                "maxrss_kb": maxrss_kb,
                "maxrss_kb_std": maxrss_kb_std,
                "voluntary_ctx_switches": voluntary_ctx_switches,
                "voluntary_ctx_switches_std": voluntary_ctx_switches_std,
                "involuntary_ctx_switches": involuntary_ctx_switches,
                "involuntary_ctx_switches_std": involuntary_ctx_switches_std,
                "fs_in": fs_in,
                "fs_in_std": fs_in_std,
                "fs_out": fs_out,
                "fs_out_std": fs_out_std,
                "repo_size_bytes": repo_size_bytes,
                "repo_size_bytes_std": repo_size_bytes_std,
                "repo_size": repo_sizes.get(tool, "NA"),
                "run_count": run_count,
                "failed_runs": failed_runs,
                "exit_status": exit_status,
            }
        )
    return records, dataset_bytes, dataset_files


def _record_has_data(record: dict | None) -> bool:
    if not record:
        return False
    return int(record.get("run_count", 0) or 0) > 0


def merge_records(
    current_records: List[dict],
    backfill_records: List[dict],
    mode: str,
    selected_tool: str | None = None,
) -> List[dict]:
    current_map = {r["op"]: r for r in current_records}
    backfill_map = {r["op"]: r for r in backfill_records}
    merged: List[dict] = []

    for op in OPS:
        current = current_map.get(op)
        backfill = backfill_map.get(op)
        chosen = current
        tool = op.split("_", 1)[0]

        if mode == "nonselected":
            if selected_tool is None:
                raise ValueError("selected_tool is required for nonselected backfill mode")
            if tool != selected_tool and not _record_has_data(current) and backfill is not None:
                chosen = backfill
        elif mode == "missing":
            if not _record_has_data(current) and backfill is not None:
                chosen = backfill
        else:
            raise ValueError(f"unsupported backfill mode: {mode}")

        if chosen is not None:
            merged.append(chosen)

    return merged


def apply_backfill_records(
    records: List[dict],
    backfill_roots: List[str],
    backfill_mode: str,
    selected_tool_arg: str,
) -> tuple[List[dict], List[str], str]:
    if not backfill_roots:
        return records, [], selected_tool_arg

    selected_tool: str | None = None
    if backfill_mode == "nonselected":
        if selected_tool_arg not in TOOLS:
            raise ValueError(f"--selected-tool must be one of: {', '.join(TOOLS)}")
        selected_tool = selected_tool_arg

    applied_roots: List[str] = []
    merged = records
    for root_str in backfill_roots:
        backfill_root = pathlib.Path(root_str).expanduser()
        if not backfill_root.is_dir():
            raise ValueError(f"backfill root dir not found: {backfill_root}")
        backfill_records, _backfill_dataset_bytes, _backfill_dataset_files = build_records(backfill_root)
        merged = merge_records(merged, backfill_records, backfill_mode, selected_tool)
        applied_roots.append(str(backfill_root))

    return merged, applied_roots, selected_tool_arg


def fmt_float(v: float | None, digits: int) -> str:
    if v is None:
        return "NA"
    return f"{v:.{digits}f}"


def fmt_int(v: int | None) -> str:
    if v is None:
        return "NA"
    return str(v)


def records_tsv(records: List[dict]) -> str:
    lines = [
        (
            "op\truns\tfailed_runs\tduration_s\tduration_s_std\tthroughput_mib_s\tthroughput_mib_s_std\tcpu%"
            "\tcpu_pct_std\tuser_s\tuser_s_std\tsys_s\tsys_s_std\tmaxrss_kb\tmaxrss_kb_std\tfs_in\tfs_in_std"
            "\tfs_out\tfs_out_std\tvol_ctx_sw\tvol_ctx_sw_std\tinvol_ctx_sw\tinvol_ctx_sw_std"
            "\trepo_size_bytes\trepo_size_bytes_std\trepo_size\texit"
        )
    ]
    for r in records:
        lines.append(
            "\t".join(
                [
                    r["op"],
                    fmt_int(r["run_count"]),
                    fmt_int(r["failed_runs"]),
                    fmt_float(r["duration_s"], 2),
                    fmt_float(r["duration_s_std"], 2),
                    fmt_float(r["throughput_mib_s"], 1),
                    fmt_float(r["throughput_mib_s_std"], 1),
                    r["cpu_pct_raw"],
                    fmt_float(r["cpu_pct_std"], 1),
                    fmt_float(r["user_s"], 2),
                    fmt_float(r["user_s_std"], 2),
                    fmt_float(r["sys_s"], 2),
                    fmt_float(r["sys_s_std"], 2),
                    fmt_float(r["maxrss_kb"], 0),
                    fmt_float(r["maxrss_kb_std"], 0),
                    fmt_float(r["fs_in"], 0),
                    fmt_float(r["fs_in_std"], 0),
                    fmt_float(r["fs_out"], 0),
                    fmt_float(r["fs_out_std"], 0),
                    fmt_float(r["voluntary_ctx_switches"], 0),
                    fmt_float(r["voluntary_ctx_switches_std"], 0),
                    fmt_float(r["involuntary_ctx_switches"], 0),
                    fmt_float(r["involuntary_ctx_switches_std"], 0),
                    fmt_float(r["repo_size_bytes"], 0),
                    fmt_float(r["repo_size_bytes_std"], 0),
                    r["repo_size"],
                    fmt_int(r["exit_status"]),
                ]
            )
        )
    return "\n".join(lines) + "\n"


def records_markdown(records: List[dict]) -> str:
    lines = [
        (
            "| op | runs | failed_runs | duration_s | duration_s_std | throughput_mib_s | throughput_mib_s_std | "
            "cpu% | cpu_pct_std | user_s | user_s_std | sys_s | sys_s_std | maxrss_kb | maxrss_kb_std | fs_in | "
            "fs_in_std | fs_out | fs_out_std | vol_ctx_sw | vol_ctx_sw_std | invol_ctx_sw | invol_ctx_sw_std | "
            "repo_size_bytes | repo_size_bytes_std | repo_size | exit |"
        ),
        "|---|" + "---:|" * 26,
    ]
    for r in records:
        lines.append(
            "| "
            + " | ".join(
                [
                    r["op"],
                    fmt_int(r["run_count"]),
                    fmt_int(r["failed_runs"]),
                    fmt_float(r["duration_s"], 2),
                    fmt_float(r["duration_s_std"], 2),
                    fmt_float(r["throughput_mib_s"], 1),
                    fmt_float(r["throughput_mib_s_std"], 1),
                    r["cpu_pct_raw"],
                    fmt_float(r["cpu_pct_std"], 1),
                    fmt_float(r["user_s"], 2),
                    fmt_float(r["user_s_std"], 2),
                    fmt_float(r["sys_s"], 2),
                    fmt_float(r["sys_s_std"], 2),
                    fmt_float(r["maxrss_kb"], 0),
                    fmt_float(r["maxrss_kb_std"], 0),
                    fmt_float(r["fs_in"], 0),
                    fmt_float(r["fs_in_std"], 0),
                    fmt_float(r["fs_out"], 0),
                    fmt_float(r["fs_out_std"], 0),
                    fmt_float(r["voluntary_ctx_switches"], 0),
                    fmt_float(r["voluntary_ctx_switches_std"], 0),
                    fmt_float(r["involuntary_ctx_switches"], 0),
                    fmt_float(r["involuntary_ctx_switches_std"], 0),
                    fmt_float(r["repo_size_bytes"], 0),
                    fmt_float(r["repo_size_bytes_std"], 0),
                    r["repo_size"],
                    fmt_int(r["exit_status"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def write_summary_outputs(
    out_dir: pathlib.Path,
    records: List[dict],
    dataset_bytes: int | None,
    dataset_files: int | None,
    summary_meta: dict | None = None,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.tsv").write_text(records_tsv(records))
    (out_dir / "summary.md").write_text(records_markdown(records))
    payload = {
        "dataset_bytes": dataset_bytes,
        "dataset_files": dataset_files,
        "records": records,
    }
    if summary_meta:
        payload.update(summary_meta)
    (out_dir / "summary.json").write_text(json.dumps(payload, indent=2))


def print_summary(root: pathlib.Path, records: List[dict], dataset_bytes: int | None, dataset_files: int | None) -> None:
    print(f"root: {root}")
    if dataset_bytes is not None:
        print(f"dataset_bytes: {dataset_bytes}")
    if dataset_files is not None:
        print(f"dataset_files: {dataset_files}")
    print(
        (
            "op\truns\tfailed_runs\tduration_s\tduration_s_std\tthroughput_mib_s\tthroughput_mib_s_std\tcpu%"
            "\tcpu_pct_std\tuser_s\tuser_s_std\tsys_s\tsys_s_std\tmaxrss_kb\tmaxrss_kb_std\tfs_in\tfs_in_std"
            "\tfs_out\tfs_out_std\tvol_ctx_sw\tvol_ctx_sw_std\tinvol_ctx_sw\tinvol_ctx_sw_std"
            "\trepo_size_bytes\trepo_size_bytes_std\trepo_size\texit"
        )
    )
    for r in records:
        print(
            "\t".join(
                [
                    r["op"],
                    fmt_int(r["run_count"]),
                    fmt_int(r["failed_runs"]),
                    fmt_float(r["duration_s"], 2),
                    fmt_float(r["duration_s_std"], 2),
                    fmt_float(r["throughput_mib_s"], 1),
                    fmt_float(r["throughput_mib_s_std"], 1),
                    r["cpu_pct_raw"],
                    fmt_float(r["cpu_pct_std"], 1),
                    fmt_float(r["user_s"], 2),
                    fmt_float(r["user_s_std"], 2),
                    fmt_float(r["sys_s"], 2),
                    fmt_float(r["sys_s_std"], 2),
                    fmt_float(r["maxrss_kb"], 0),
                    fmt_float(r["maxrss_kb_std"], 0),
                    fmt_float(r["fs_in"], 0),
                    fmt_float(r["fs_in_std"], 0),
                    fmt_float(r["fs_out"], 0),
                    fmt_float(r["fs_out_std"], 0),
                    fmt_float(r["voluntary_ctx_switches"], 0),
                    fmt_float(r["voluntary_ctx_switches_std"], 0),
                    fmt_float(r["involuntary_ctx_switches"], 0),
                    fmt_float(r["involuntary_ctx_switches_std"], 0),
                    fmt_float(r["repo_size_bytes"], 0),
                    fmt_float(r["repo_size_bytes_std"], 0),
                    r["repo_size"],
                    fmt_int(r["exit_status"]),
                ]
            )
        )


def _chart_values(records: List[dict], metric: str) -> dict:
    by_tool_phase: dict = {(r["tool"], r["phase"]): r for r in records}
    backup: List[float] = []
    restore: List[float] = []
    for tool in TOOLS:
        b = by_tool_phase.get((tool, "backup"))
        r = by_tool_phase.get((tool, "restore"))
        bv = b.get(metric) if b else None
        rv = r.get(metric) if r else None
        backup.append(float(bv) if bv is not None else math.nan)
        restore.append(float(rv) if rv is not None else math.nan)
    return {"backup": backup, "restore": restore}


def generate_chart_with_deps(
    root: pathlib.Path,
    out_file: pathlib.Path,
    records: List[dict],
    no_uv_bootstrap: bool = False,
    backfill_roots: List[str] | None = None,
    backfill_mode: str = "nonselected",
    selected_tool: str = "",
) -> int:
    try:
        import matplotlib.gridspec as gridspec
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.patches import Patch
        from matplotlib.ticker import FuncFormatter
    except ModuleNotFoundError as e:
        if no_uv_bootstrap:
            print(f"chart dependencies missing: {e}", file=sys.stderr)
            return 2
        uv = shutil.which("uv")
        if not uv:
            print("chart dependencies missing and 'uv' is not installed", file=sys.stderr)
            return 2
        cmd = [
            uv,
            "run",
            "--with",
            "numpy>=2.0,<3",
            "--with",
            "matplotlib>=3.9,<4",
            "python3",
            str(pathlib.Path(__file__).resolve()),
            "chart",
            str(root),
            "--chart-file",
            str(out_file),
            "--no-uv-bootstrap",
        ]
        for backfill_root in backfill_roots or []:
            cmd.extend(["--backfill-root", backfill_root])
        if backfill_roots:
            cmd.extend(["--backfill-mode", backfill_mode])
            if selected_tool:
                cmd.extend(["--selected-tool", selected_tool])
        return subprocess.call(cmd)

    tools_display = ["Vykar", "Restic", "Rustic", "Borg", "Kopia"]
    duration = _chart_values(records, "duration_s")
    cpu_seconds = _chart_values(records, "cpu_s")
    user_seconds = _chart_values(records, "user_s")
    sys_seconds = _chart_values(records, "sys_s")
    memory = _chart_values(records, "maxrss_kb")
    repo_size_gb = _chart_values(records, "repo_size_bytes")
    memory["backup"] = [v / 1024.0 if not math.isnan(v) else v for v in memory["backup"]]
    memory["restore"] = [v / 1024.0 if not math.isnan(v) else v for v in memory["restore"]]
    repo_size_gb["backup"] = [v / 1_000_000_000.0 if not math.isnan(v) else v for v in repo_size_gb["backup"]]
    repo_size_gb["restore"] = [v / 1_000_000_000.0 if not math.isnan(v) else v for v in repo_size_gb["restore"]]

    # Match the original benchmark chart style.
    VYKAR = "#fb8c00"
    VYKAR_LIGHT = "#ffb74d"
    OTHER = "#546e7a"
    OTHER_LIGHT = "#78909c"
    BG = "#1a2327"

    plt.rcParams.update(
        {
            "font.family": "monospace",
            "figure.facecolor": BG,
            "axes.facecolor": BG,
            "text.color": "#eceff1",
            "axes.labelcolor": "#b0bec5",
            "xtick.color": "#90a4ae",
            "ytick.color": "#90a4ae",
        }
    )

    x = np.arange(len(TOOLS))
    w = 0.32

    backup_colors = [VYKAR if t == "Vykar" else OTHER for t in tools_display]
    restore_colors = [VYKAR_LIGHT if t == "Vykar" else OTHER_LIGHT for t in tools_display]
    backup_top_colors = ["#f57c00" if t == "Vykar" else "#455a64" for t in tools_display]
    restore_top_colors = ["#ffa726" if t == "Vykar" else "#607d8b" for t in tools_display]

    def style_axis(
        ax,
        title: str,
        higher_is_better: bool,
        qualifier_override: str | None = None,
        show_arrow: bool = True,
    ) -> None:
        ax.set_xticks(x)
        ax.set_xticklabels(tools_display, fontsize=10)
        for label in ax.get_xticklabels():
            if "Vykar" in label.get_text():
                label.set_color(VYKAR)
                label.set_fontweight("bold")
        arrow = "↑" if higher_is_better else "↓"
        qualifier = qualifier_override if qualifier_override is not None else (
            "higher is better" if higher_is_better else "lower is better"
        )
        if show_arrow:
            subtitle = f"{arrow} {qualifier}"
        else:
            subtitle = qualifier
        ax.set_title(f"{title}  ({subtitle})", fontsize=10, color="#cfd8dc", pad=10)
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#2e3d44", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#2a2a40")
        ax.spines["bottom"].set_color("#2a2a40")

    def draw_standard_panel(
        ax,
        title: str,
        vals: dict,
        higher_is_better: bool,
        use_k_labels: bool = False,
        label_decimals: int | None = None,
        qualifier_override: str | None = None,
        show_arrow: bool = True,
    ) -> None:
        backup_vals = np.array(vals["backup"], dtype=float)
        restore_vals = np.array(vals["restore"], dtype=float)
        bars1 = ax.bar(x - w / 2, backup_vals, w, color=backup_colors, zorder=3)
        bars2 = ax.bar(x + w / 2, restore_vals, w, color=restore_colors, zorder=3)

        finite = np.concatenate([backup_vals[np.isfinite(backup_vals)], restore_vals[np.isfinite(restore_vals)]])
        ymax = float(finite.max()) if finite.size else 1.0
        ax.set_ylim(0, ymax * 1.18)

        for bars in (bars1, bars2):
            for bar in bars:
                h = float(bar.get_height())
                if not np.isfinite(h):
                    continue
                if label_decimals is not None:
                    label = f"{h:.{label_decimals}f}"
                elif use_k_labels:
                    label = f"{h / 1000.0:.1f}k"
                else:
                    label = f"{h:.0f}" if h >= 10 else f"{h:.1f}"
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    h + ymax * 0.01,
                    label,
                    ha="center",
                    va="bottom",
                    fontsize=7.5,
                    color="#b0bec5",
                    fontweight="bold",
                )

        if use_k_labels:
            ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _pos: f"{y / 1000.0:.0f}k"))

        style_axis(
            ax,
            title,
            higher_is_better,
            qualifier_override=qualifier_override,
            show_arrow=show_arrow,
        )

    def draw_cpu_seconds_stacked_panel(ax) -> None:
        user_backup = np.array(user_seconds["backup"], dtype=float)
        user_restore = np.array(user_seconds["restore"], dtype=float)
        sys_backup = np.array(sys_seconds["backup"], dtype=float)
        sys_restore = np.array(sys_seconds["restore"], dtype=float)
        total_backup = np.array(cpu_seconds["backup"], dtype=float)
        total_restore = np.array(cpu_seconds["restore"], dtype=float)

        bars_user_backup = ax.bar(x - w / 2, user_backup, w, color=backup_colors, zorder=3)
        bars_user_restore = ax.bar(x + w / 2, user_restore, w, color=restore_colors, zorder=3)
        ax.bar(x - w / 2, sys_backup, w, bottom=user_backup, color=backup_top_colors, zorder=3)
        ax.bar(x + w / 2, sys_restore, w, bottom=user_restore, color=restore_top_colors, zorder=3)

        finite = np.concatenate([total_backup[np.isfinite(total_backup)], total_restore[np.isfinite(total_restore)]])
        ymax = float(finite.max()) if finite.size else 1.0
        ax.set_ylim(0, ymax * 1.20)

        for bars, totals in ((bars_user_backup, total_backup), (bars_user_restore, total_restore)):
            for i, bar in enumerate(bars):
                h = float(totals[i])
                if not np.isfinite(h):
                    continue
                label = f"{h:.0f}" if h >= 10 else f"{h:.1f}"
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    h + ymax * 0.012,
                    label,
                    ha="center",
                    va="bottom",
                    fontsize=7.5,
                    color="#b0bec5",
                    fontweight="bold",
                )

        ax.text(
            0.01,
            0.98,
            "bottom: user_s\ntop: sys_s",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=8.0,
            color="#90a4ae",
        )
        style_axis(ax, "CPU Seconds (s)", False)

    def draw_memory_broken_panel(ax_top, ax_bot, vals: dict) -> None:
        backup_vals = np.array(vals["backup"], dtype=float)
        restore_vals = np.array(vals["restore"], dtype=float)

        for ax in (ax_top, ax_bot):
            ax.bar(x - w / 2, backup_vals, w, color=backup_colors, zorder=3)
            ax.bar(x + w / 2, restore_vals, w, color=restore_colors, zorder=3)

        finite = np.concatenate([backup_vals[np.isfinite(backup_vals)], restore_vals[np.isfinite(restore_vals)]])
        max_val = float(finite.max()) if finite.size else 1.0
        lower_cap = 1100.0
        upper_min = max(lower_cap * 1.2, max_val * 0.9)
        upper_max = max(upper_min + 200.0, max_val * 1.06)

        ax_bot.set_ylim(0, lower_cap)
        ax_top.set_ylim(upper_min, upper_max)

        ax_top.spines["bottom"].set_visible(False)
        ax_bot.spines["top"].set_visible(False)
        ax_top.tick_params(bottom=False, labelbottom=False)

        zigzag_n = 12
        zx = np.linspace(-0.6, len(tools_display) - 0.4, zigzag_n * 2 + 1)
        zy_amp_b = (ax_bot.get_ylim()[1] - ax_bot.get_ylim()[0]) * 0.018
        zy_bot = [ax_bot.get_ylim()[1] + (zy_amp_b if i % 2 == 0 else -zy_amp_b) for i in range(len(zx))]
        ax_bot.plot(zx, zy_bot, color="#78909c", linewidth=0.7, clip_on=False, zorder=10)

        zy_amp_t = (ax_top.get_ylim()[1] - ax_top.get_ylim()[0]) * 0.018
        zy_top = [ax_top.get_ylim()[0] + (zy_amp_t if i % 2 == 0 else -zy_amp_t) for i in range(len(zx))]
        ax_top.plot(zx, zy_top, color="#78909c", linewidth=0.7, clip_on=False, zorder=10)

        for i in range(len(tools_display)):
            for values, x_off in ((backup_vals, -w / 2), (restore_vals, w / 2)):
                v = float(values[i])
                if not np.isfinite(v):
                    continue
                if v > lower_cap:
                    label_y = max(v, upper_min) + (upper_max - upper_min) * 0.03
                    ax_top.text(
                        x[i] + x_off,
                        label_y,
                        f"{v:.0f}",
                        ha="center",
                        va="bottom",
                        fontsize=7.5,
                        color="#ff6666",
                        fontweight="bold",
                        clip_on=False,
                    )
                else:
                    ax_bot.text(
                        x[i] + x_off,
                        v + lower_cap * 0.012,
                        f"{v:.0f}",
                        ha="center",
                        va="bottom",
                        fontsize=7.5,
                        color="#b0bec5",
                        fontweight="bold",
                    )

        ax_bot.set_xticks(x)
        ax_bot.set_xticklabels(tools_display, fontsize=10)
        for label in ax_bot.get_xticklabels():
            if "Vykar" in label.get_text():
                label.set_color(VYKAR)
                label.set_fontweight("bold")
        ax_top.set_xticks([])
        ax_top.set_title("Peak Memory (MB)  (↓ lower is better)", fontsize=10, color="#cfd8dc", pad=10)

        for ax in (ax_top, ax_bot):
            ax.set_axisbelow(True)
            ax.grid(axis="y", color="#2e3d44", linewidth=0.5)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.spines["left"].set_color("#2a2a40")
            ax.spines["bottom"].set_color("#2a2a40")
        ax_top.spines["bottom"].set_visible(False)
        ax_bot.spines["top"].set_visible(False)

    def draw_ctx_switches_broken_panel(ax_top, ax_bot, vals: dict) -> None:
        backup_vals = np.array(vals["backup"], dtype=float)
        restore_vals = np.array(vals["restore"], dtype=float)

        for ax in (ax_top, ax_bot):
            ax.bar(x - w / 2, backup_vals, w, color=backup_colors, zorder=3)
            ax.bar(x + w / 2, restore_vals, w, color=restore_colors, zorder=3)

        finite = np.concatenate([backup_vals[np.isfinite(backup_vals)], restore_vals[np.isfinite(restore_vals)]])
        if not finite.size:
            max_val = 1.0
            lower_cap = 1.0
            upper_min = 1.0
            upper_max = 2.0
        else:
            sorted_vals = np.sort(finite)
            max_val = float(sorted_vals[-1])
            second_max = float(sorted_vals[-2]) if sorted_vals.size > 1 else max_val
            lower_cap = max(second_max * 1.15, max_val * 0.18)
            lower_cap = min(lower_cap, max_val * 0.75)
            upper_min = max(lower_cap * 1.10, max_val * 0.88)
            upper_max = max(upper_min + max_val * 0.06, max_val * 1.06)

        ax_bot.set_ylim(0, lower_cap)
        ax_top.set_ylim(upper_min, upper_max)

        ax_top.spines["bottom"].set_visible(False)
        ax_bot.spines["top"].set_visible(False)
        ax_top.tick_params(bottom=False, labelbottom=False)

        zigzag_n = 12
        zx = np.linspace(-0.6, len(tools_display) - 0.4, zigzag_n * 2 + 1)
        zy_amp_b = (ax_bot.get_ylim()[1] - ax_bot.get_ylim()[0]) * 0.018
        zy_bot = [ax_bot.get_ylim()[1] + (zy_amp_b if i % 2 == 0 else -zy_amp_b) for i in range(len(zx))]
        ax_bot.plot(zx, zy_bot, color="#78909c", linewidth=0.7, clip_on=False, zorder=10)

        zy_amp_t = (ax_top.get_ylim()[1] - ax_top.get_ylim()[0]) * 0.018
        zy_top = [ax_top.get_ylim()[0] + (zy_amp_t if i % 2 == 0 else -zy_amp_t) for i in range(len(zx))]
        ax_top.plot(zx, zy_top, color="#78909c", linewidth=0.7, clip_on=False, zorder=10)

        for i in range(len(tools_display)):
            for values, x_off in ((backup_vals, -w / 2), (restore_vals, w / 2)):
                v = float(values[i])
                if not np.isfinite(v):
                    continue
                label = f"{int(round(v / 1000.0))}k"
                if v > lower_cap:
                    ax_top.text(
                        x[i] + x_off,
                        v + (upper_max - upper_min) * 0.03,
                        label,
                        ha="center",
                        va="bottom",
                        fontsize=7.5,
                        color="#ff6666",
                        fontweight="bold",
                    )
                else:
                    ax_bot.text(
                        x[i] + x_off,
                        v + lower_cap * 0.012,
                        label,
                        ha="center",
                        va="bottom",
                        fontsize=7.5,
                        color="#b0bec5",
                        fontweight="bold",
                    )

        ax_bot.set_xticks(x)
        ax_bot.set_xticklabels(tools_display, fontsize=10)
        for label in ax_bot.get_xticklabels():
            if "Vykar" in label.get_text():
                label.set_color(VYKAR)
                label.set_fontweight("bold")
        ax_top.set_xticks([])
        ax_top.set_title("I/O Wait Events  (↓ lower is better)", fontsize=10, color="#cfd8dc", pad=10)

        for ax in (ax_top, ax_bot):
            ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _pos: f"{int(round(y / 1000.0))}k"))
            ax.set_axisbelow(True)
            ax.grid(axis="y", color="#2e3d44", linewidth=0.5)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.spines["left"].set_color("#2a2a40")
            ax.spines["bottom"].set_color("#2a2a40")
        ax_top.spines["bottom"].set_visible(False)
        ax_bot.spines["top"].set_visible(False)

    dataset_bytes = get_dataset_bytes(root)
    dataset_files = get_dataset_file_count(root)
    dataset_gib = (dataset_bytes / (1024.0 ** 3)) if dataset_bytes else None
    if dataset_files is None:
        files_label = None
    elif dataset_files >= 1_000_000:
        files_label = f"{dataset_files / 1_000_000.0:.1f}M"
    elif dataset_files >= 1_000:
        files_label = f"{dataset_files / 1_000.0:.0f}k"
    else:
        files_label = str(dataset_files)
    if dataset_gib is not None and files_label is not None:
        dataset_label = f"{dataset_gib:.0f} GiB / {files_label} files"
    elif dataset_gib is not None:
        dataset_label = f"{dataset_gib:.0f} GiB dataset"
    else:
        dataset_label = "dataset"

    fig = plt.figure(figsize=(12.5, 8.5))
    fig.suptitle(
        f"Backup Tool Benchmark  ·  {dataset_label}",
        fontsize=16,
        fontweight="bold",
        color="#e8e8f0",
        y=0.97,
    )

    outer = gridspec.GridSpec(
        2, 2, figure=fig, hspace=0.38, wspace=0.28, left=0.06, right=0.97, top=0.90, bottom=0.09
    )
    draw_standard_panel(fig.add_subplot(outer[0, 0]), "Duration (s)", duration, False)
    inner = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=outer[0, 1], height_ratios=[1, 3], hspace=0.08)
    draw_memory_broken_panel(fig.add_subplot(inner[0]), fig.add_subplot(inner[1]), memory)
    draw_cpu_seconds_stacked_panel(fig.add_subplot(outer[1, 0]))
    draw_standard_panel(
        fig.add_subplot(outer[1, 1]),
        "Repo Size (GB)",
        repo_size_gb,
        False,
        label_decimals=1,
        qualifier_override="equivalent zstd compression",
        show_arrow=False,
    )

    legend_elements = [
        Patch(facecolor=VYKAR, label="Vykar backup"),
        Patch(facecolor=VYKAR_LIGHT, label="Vykar restore"),
        Patch(facecolor=OTHER, label="Others backup"),
        Patch(facecolor=OTHER_LIGHT, label="Others restore"),
    ]
    fig.legend(
        handles=legend_elements,
        loc="lower center",
        ncol=4,
        frameon=True,
        fontsize=9,
        fancybox=False,
        edgecolor="#4a5d66",
        facecolor="#2c3940",
        labelcolor="#c8c8d4",
        bbox_to_anchor=(0.5, 0.005),
    )

    run_timestamp_utc = infer_run_timestamp_utc(root)
    if run_timestamp_utc:
        fig.text(
            0.995,
            0.003,
            f"run: {run_timestamp_utc}",
            ha="right",
            va="bottom",
            fontsize=6.5,
            color="#78909c",
        )

    out_file.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_file, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"chart: {out_file}")
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.root).expanduser()
    if not root.is_dir():
        print(f"root dir not found: {root}", file=sys.stderr)
        return 2
    records, dataset_bytes, dataset_files = build_records(root)
    print_summary(root, records, dataset_bytes, dataset_files)
    if args.write_files:
        out_dir = pathlib.Path(args.out_dir).expanduser()
        write_summary_outputs(out_dir, records, dataset_bytes, dataset_files)
        print(f"summary files: {out_dir}")
    return 0


def cmd_chart(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.root).expanduser()
    if not root.is_dir():
        print(f"root dir not found: {root}", file=sys.stderr)
        return 2
    out_file = pathlib.Path(args.chart_file).expanduser()
    records, _dataset_bytes, _dataset_files = build_records(root)
    try:
        records, _applied_backfill_roots, _selected_tool = apply_backfill_records(
            records, args.backfill_root, args.backfill_mode, args.selected_tool
        )
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2

    return generate_chart_with_deps(
        root,
        out_file,
        records,
        no_uv_bootstrap=args.no_uv_bootstrap,
        backfill_roots=args.backfill_root,
        backfill_mode=args.backfill_mode,
        selected_tool=args.selected_tool,
    )


def cmd_all(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.root).expanduser()
    if not root.is_dir():
        print(f"root dir not found: {root}", file=sys.stderr)
        return 2

    out_dir = pathlib.Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    chart_file = pathlib.Path(args.chart_file).expanduser()

    records, dataset_bytes, dataset_files = build_records(root)
    summary_meta: dict = {}
    if args.backfill_root:
        try:
            records, applied_backfill_roots, selected_tool_arg = apply_backfill_records(
                records, args.backfill_root, args.backfill_mode, args.selected_tool
            )
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 2
        summary_meta["backfill_roots"] = applied_backfill_roots
        summary_meta["selected_tool"] = selected_tool_arg
        summary_meta["backfill_mode"] = args.backfill_mode

    print_summary(root, records, dataset_bytes, dataset_files)
    write_summary_outputs(out_dir, records, dataset_bytes, dataset_files, summary_meta=summary_meta)
    print(f"summary files: {out_dir}")

    rc = generate_chart_with_deps(
        root,
        chart_file,
        records,
        no_uv_bootstrap=False,
        backfill_roots=args.backfill_root,
        backfill_mode=args.backfill_mode,
        selected_tool=args.selected_tool,
    )
    if rc != 0:
        (out_dir / "chart_error.txt").write_text("failed to generate chart\n")
        return rc
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Benchmark summary/chart reporter")
    sub = p.add_subparsers(dest="command", required=True)

    ps = sub.add_parser("summary", help="print and optionally write summary tables")
    ps.add_argument("root", help="benchmark root directory")
    ps.add_argument("--out-dir", default="", help="output dir for summary files")
    ps.add_argument("--write-files", action="store_true", help="write summary.{md,tsv,json}")
    ps.set_defaults(func=cmd_summary)

    pc = sub.add_parser("chart", help="generate chart PNG")
    pc.add_argument("root", help="benchmark root directory")
    pc.add_argument(
        "--chart-file",
        default="",
        help="output chart path (default: <root>/reports/benchmark.summary.png)",
    )
    pc.add_argument(
        "--backfill-root",
        action="append",
        default=[],
        help="previous benchmark root used for backfill (repeatable; newest first)",
    )
    pc.add_argument(
        "--backfill-mode",
        default="nonselected",
        choices=["nonselected", "missing"],
        help="record backfill policy",
    )
    pc.add_argument("--selected-tool", default="", help="tool selected in current run")
    pc.add_argument("--no-uv-bootstrap", action="store_true", help=argparse.SUPPRESS)
    pc.set_defaults(func=cmd_chart)

    pa = sub.add_parser("all", help="summary + chart")
    pa.add_argument("root", help="benchmark root directory")
    pa.add_argument("--out-dir", default="", help="output dir for report files")
    pa.add_argument(
        "--chart-file",
        default="",
        help="output chart path (default: <out-dir>/benchmark.summary.png)",
    )
    pa.add_argument(
        "--backfill-root",
        action="append",
        default=[],
        help="previous benchmark root used for backfill (repeatable; newest first)",
    )
    pa.add_argument(
        "--backfill-mode",
        default="nonselected",
        choices=["nonselected", "missing"],
        help="record backfill policy",
    )
    pa.add_argument("--selected-tool", default="", help="tool selected in current run")
    pa.set_defaults(func=cmd_all)

    return p


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    root = pathlib.Path(getattr(args, "root", ".")).expanduser()
    default_out_dir = root / "reports"
    if hasattr(args, "out_dir") and not args.out_dir:
        args.out_dir = str(default_out_dir)
    if hasattr(args, "chart_file") and not args.chart_file:
        if hasattr(args, "out_dir") and args.out_dir:
            args.chart_file = str(pathlib.Path(args.out_dir).expanduser() / "benchmark.summary.png")
        else:
            args.chart_file = str(default_out_dir / "benchmark.summary.png")

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
