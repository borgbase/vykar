"""Scenario runner: YAML-driven end-to-end testing for vykar.

Merges and replaces:
- scripts/scenarios/scenario_runner/cli.py
- scripts/scenarios/scenario_runner/runner.py
- scripts/scenarios/scenario_runner/report.py
"""

import argparse
import json
import os
import random
import shutil
import statistics
import sys
import time
from datetime import datetime, timezone

import yaml

from . import config as cfg
from . import corpus
from . import vykar as vykar_cmd
from .corpus import CorpusDependencyError


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

_GIB = 1024 ** 3


def _compute_timing_stats(samples: list[float]) -> dict | None:
    if not samples:
        return None
    stdev = 0.0 if len(samples) == 1 else statistics.stdev(samples)
    return {
        "count": len(samples),
        "mean_sec": statistics.mean(samples),
        "stdev_sec": stdev,
        "min_sec": min(samples),
        "max_sec": max(samples),
    }


def _compute_normalized_stats(samples: list[float]) -> dict | None:
    if not samples:
        return None
    stdev = 0.0 if len(samples) == 1 else statistics.stdev(samples)
    mean_value = statistics.mean(samples)
    return {
        "normalized_sample_count": len(samples),
        "mean_sec_per_gib": mean_value,
        "stdev_sec_per_gib": stdev,
        "min_sec_per_gib": min(samples),
        "max_sec_per_gib": max(samples),
        "mean_gib_per_sec": 0.0 if mean_value == 0 else 1.0 / mean_value,
    }


def _phase_normalized_metric(phase: dict) -> tuple[str, float] | None:
    action = phase.get("action", "")
    duration = phase.get("duration_sec", 0.0)
    if duration <= 0:
        return None
    if action in {"backup", "verify"}:
        corpus_bytes = phase.get("corpus_bytes_at_phase_start", 0)
        if corpus_bytes <= 0:
            return None
        return "corpus_bytes_at_phase_start", duration / (corpus_bytes / _GIB)
    if action == "churn":
        added_bytes = phase.get("stats", {}).get("added_bytes", 0)
        if added_bytes <= 0:
            return None
        return "added_bytes", duration / (added_bytes / _GIB)
    return None


def build_summary(summaries: list[dict]) -> dict:
    """Build aggregate timing summary for runs and phases."""
    passed_runs = [s for s in summaries if s.get("passed", False)]
    run_samples = [s.get("duration_sec", 0.0) for s in passed_runs]
    run_totals = _compute_timing_stats(run_samples)
    if run_totals is None:
        run_totals = {
            "count": 0,
            "excluded_failed_runs": len(summaries),
            "mean_sec": 0.0,
            "stdev_sec": 0.0,
            "min_sec": 0.0,
            "max_sec": 0.0,
        }
    else:
        run_totals["excluded_failed_runs"] = len(summaries) - len(passed_runs)

    phase_buckets: dict[str, dict] = {}
    phase_order: list[str] = []
    for run in summaries:
        for phase in run.get("phases", []):
            action = phase.get("action", "")
            label = phase.get("label", "")
            key = action if not label else f"{action} ({label})"
            if key not in phase_buckets:
                phase_buckets[key] = {
                    "key": key,
                    "action": action,
                    "label": label,
                    "samples": [],
                    "normalized_samples": [],
                    "normalization": None,
                    "count": 0,
                    "passed_count": 0,
                    "failed_count": 0,
                }
                phase_order.append(key)
            bucket = phase_buckets[key]
            bucket["count"] += 1
            if phase.get("passed", False):
                bucket["passed_count"] += 1
                bucket["samples"].append(phase.get("duration_sec", 0.0))
                normalized = _phase_normalized_metric(phase)
                if normalized is not None:
                    normalization, value = normalized
                    bucket["normalization"] = normalization
                    bucket["normalized_samples"].append(value)
            else:
                bucket["failed_count"] += 1

    phases = []
    for key in phase_order:
        bucket = phase_buckets[key]
        stats = _compute_timing_stats(bucket.pop("samples"))
        normalized_stats = _compute_normalized_stats(bucket.pop("normalized_samples"))
        if stats is None:
            stats = {"mean_sec": 0.0, "stdev_sec": 0.0, "min_sec": 0.0, "max_sec": 0.0}
        if normalized_stats is None:
            normalized_stats = {
                "normalized_sample_count": 0,
                "mean_sec_per_gib": 0.0,
                "stdev_sec_per_gib": 0.0,
                "min_sec_per_gib": 0.0,
                "max_sec_per_gib": 0.0,
                "mean_gib_per_sec": 0.0,
            }
        phases.append({**bucket, **stats, **normalized_stats})

    return {
        "run_totals": run_totals,
        "note": "Runs reused a shared corpus; compare backup/verify/churn by normalized data size metrics.",
        "phases": phases,
    }


def write_run_summary(run_dir: str, phases: list[dict]) -> str:
    """Write summary.json for a single run. Returns path to the file."""
    os.makedirs(run_dir, exist_ok=True)
    path = os.path.join(run_dir, "summary.json")
    with open(path, "w") as f:
        json.dump({"phases": phases}, f, indent=2)
    return path


def write_aggregate_report(output_dir: str, summaries: list[dict]) -> str:
    """Write report.json aggregating all run summaries. Returns path."""
    path = os.path.join(output_dir, "report.json")
    total = len(summaries)
    passed = sum(1 for s in summaries if s.get("passed", False))
    summary = build_summary(summaries)
    with open(path, "w") as f:
        json.dump(
            {
                "total_runs": total,
                "passed": passed,
                "failed": total - passed,
                "summary": summary,
                "runs": summaries,
            },
            f,
            indent=2,
        )
    return path


def print_summary(summaries: list[dict]) -> None:
    """Print human-readable summary table to stdout."""
    total = len(summaries)
    passed = sum(1 for s in summaries if s.get("passed", False))
    failed = total - passed

    print(f"\n{'='*60}")
    print(f"  Scenario Results: {passed}/{total} runs passed")
    print(f"{'='*60}")

    summary = build_summary(summaries)
    run_totals = summary["run_totals"]
    print("  Performance Summary")
    print(
        "  total run: "
        f"samples={run_totals['count']} "
        f"avg={run_totals['mean_sec']:.2f}s "
        f"stdev={run_totals['stdev_sec']:.2f}s "
        f"min={run_totals['min_sec']:.2f}s "
        f"max={run_totals['max_sec']:.2f}s"
    )
    print(f"  note: {summary['note']}")
    for phase in summary["phases"]:
        if phase["action"] in {"backup", "verify", "churn"} and phase["normalized_sample_count"] > 0:
            unit = "sec/GiB-added" if phase["normalization"] == "added_bytes" else "sec/GiB"
            print(
                f"  {phase['key']}: "
                f"samples={phase['normalized_sample_count']} "
                f"avg={phase['mean_sec_per_gib']:.2f} {unit} "
                f"stdev={phase['stdev_sec_per_gib']:.2f} "
                f"min={phase['min_sec_per_gib']:.2f} "
                f"max={phase['max_sec_per_gib']:.2f} "
                f"(avg {phase['mean_gib_per_sec']:.2f} GiB/s)"
            )
        else:
            print(
                f"  {phase['key']}: "
                f"samples={phase['passed_count']} "
                f"avg={phase['mean_sec']:.2f}s "
                f"stdev={phase['stdev_sec']:.2f}s "
                f"min={phase['min_sec']:.2f}s "
                f"max={phase['max_sec']:.2f}s"
            )

    failed_runs = [s for s in summaries if not s.get("passed")]
    if failed_runs:
        print("  Failed Runs")
        for s in failed_runs:
            run_id = s.get("run_id", "?")
            phases_ok = s.get("phases_passed", 0)
            phases_total = s.get("phases_total", 0)
            print(f"  Run {run_id:>3}: phases={phases_ok}/{phases_total}")
            for p in s.get("failed_phases", []):
                print(f"           -> {p}")

    print(f"{'='*60}")
    if failed:
        print(f"  FAILED: {failed} run(s)")
    else:
        print(f"  All {total} run(s) passed")
    print()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _log(msg: str) -> None:
    print(f"[scenario] {msg}", file=sys.stderr, flush=True)


def _summarize_list_output(stdout: str) -> str:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines:
        return "no snapshots listed"
    return f"captured {len(lines)} list lines"


def _run_phase(phase: dict, *, ctx: dict) -> dict:
    """Execute a single phase. Returns a result dict."""
    action = phase["action"]
    label = phase.get("label", "")
    t0 = time.monotonic()
    result = {"action": action, "label": label, "passed": False, "detail": ""}

    vykar_bin = ctx["vykar_bin"]
    config_path = ctx["config_path"]
    repo_label = ctx["repo_label"]

    if action in {"backup", "verify", "churn"}:
        result["corpus_bytes_at_phase_start"] = corpus.dir_size_bytes(ctx["corpus_dir"])

    if action == "init":
        r = vykar_cmd.vykar_init(vykar_bin, config_path, repo_label)
        result["passed"] = r.returncode == 0
        result["detail"] = r.stderr[-500:] if r.returncode != 0 else "ok"

    elif action == "backup":
        snap_label = label or f"snap-{int(time.time())}"
        r, snap_id = vykar_cmd.vykar_backup(vykar_bin, config_path, repo_label, snap_label)
        result["passed"] = r.returncode == 0 and snap_id is not None
        if snap_id:
            ctx["latest_snapshot"] = snap_id
            result["detail"] = f"snapshot={snap_id}"
        else:
            result["detail"] = r.stderr[-500:] if r.returncode != 0 else "no snapshot ID parsed"

    elif action == "verify":
        snap_id = ctx.get("latest_snapshot")
        if not snap_id:
            result["detail"] = "no snapshot to verify"
        else:
            passed, detail, stop_scenario = vykar_cmd.verify_restore(
                vykar_bin, config_path, repo_label, snap_id, ctx["corpus_dir"], ctx["work_dir"]
            )
            result["passed"] = passed
            result["detail"] = detail
            if stop_scenario:
                result["stop_scenario"] = True

    elif action == "check":
        r = vykar_cmd.vykar_check(vykar_bin, config_path, repo_label)
        result["passed"] = r.returncode == 0
        result["detail"] = r.stderr[-500:] if r.returncode != 0 else "ok"

    elif action == "list":
        r = vykar_cmd.vykar_list(vykar_bin, config_path, repo_label)
        result["passed"] = r.returncode == 0
        if r.returncode == 0:
            result["detail"] = _summarize_list_output(r.stdout)
            result["output"] = r.stdout
        else:
            result["detail"] = r.stderr[-500:]

    elif action == "prune":
        r = vykar_cmd.vykar_prune(vykar_bin, config_path, repo_label)
        result["passed"] = r.returncode == 0
        result["detail"] = r.stderr[-500:] if r.returncode != 0 else "ok"

    elif action == "compact":
        r = vykar_cmd.vykar_compact(vykar_bin, config_path, repo_label)
        result["passed"] = r.returncode == 0
        result["detail"] = r.stderr[-500:] if r.returncode != 0 else "ok"

    elif action == "delete_snapshot":
        snap_id = ctx.get("latest_snapshot")
        if not snap_id:
            result["detail"] = "no snapshot to delete"
        else:
            r = vykar_cmd.vykar_snapshot_delete(vykar_bin, config_path, repo_label, snap_id)
            result["passed"] = r.returncode == 0
            result["detail"] = r.stderr[-500:] if r.returncode != 0 else f"deleted {snap_id}"

    elif action == "churn":
        churn_cfg = ctx["scenario"].get("churn", {})
        stats = corpus.apply_churn(
            ctx["corpus_dir"],
            ctx["corpus_config"],
            churn_cfg,
            ctx["initial_corpus_bytes"],
            ctx["rng"],
        )
        result["passed"] = True
        result["stats"] = stats
        result["detail"] = (
            f"added={stats['added']} deleted={stats['deleted']} "
            f"modified={stats['modified']} dirs={stats['dirs_added']} "
            f"skipped_files={stats['skipped_add_files']} skipped_dirs={stats['skipped_add_dirs']} "
            f"size={stats['total_bytes_after'] / 1024**2:.1f}/{stats['max_allowed_bytes'] / 1024**2:.1f} MiB"
        )

    elif action == "cleanup":
        vykar_cmd.vykar_delete_repo(vykar_bin, config_path, repo_label)
        result["passed"] = True
        result["detail"] = "repo deleted"

    else:
        result["detail"] = f"unknown action: {action}"

    result["duration_sec"] = round(time.monotonic() - t0, 2)
    return result


def run_scenario(
    scenario: dict,
    *,
    backend: str,
    runs: int,
    output_dir: str,
    vykar_bin: str,
    seed: int,
) -> bool:
    """Run a complete scenario. Returns True if all runs passed."""
    name = scenario.get("name", "unnamed")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    work_dir = os.path.join(output_dir, name, timestamp)
    os.makedirs(work_dir, exist_ok=True)

    repo_label = f"scenario-{name}"
    corpus_config = scenario.get("corpus", {"size_gib": 0.1})
    phases = scenario.get("phases", [])

    all_summaries = []
    all_passed = True
    corpus_dir = os.path.join(work_dir, "corpus")
    config_path = os.path.join(work_dir, "config.yaml")

    corpus.validate_corpus_mix(corpus_config)

    _log("Preparing corpus")
    if os.path.exists(corpus_dir):
        shutil.rmtree(corpus_dir)
    os.makedirs(corpus_dir, exist_ok=True)

    corpus_stats = corpus.generate_corpus(corpus_dir, corpus_config, random.Random(seed))
    _log(
        f"  corpus: {corpus_stats['file_count']} files, "
        f"{corpus_stats['total_bytes'] / 1024**2:.1f} MiB"
    )

    repo_url = cfg.write_vykar_config(
        config_path, backend=backend, repo_label=repo_label, corpus_path=corpus_dir
    )
    cfg.ensure_backend_ready(backend, repo_url)
    _log(f"  repo: {repo_url}")

    for run_idx in range(1, runs + 1):
        run_rng = random.Random(seed + run_idx)
        run_id = f"{run_idx:03d}"
        run_dir = os.path.join(work_dir, f"run-{run_id}")
        os.makedirs(run_dir, exist_ok=True)
        _log(f"Run {run_id}/{runs:03d}: using existing corpus")

        ctx = {
            "vykar_bin": vykar_bin,
            "config_path": config_path,
            "repo_label": repo_label,
            "corpus_dir": corpus_dir,
            "work_dir": work_dir,
            "scenario": scenario,
            "corpus_config": corpus_config,
            "initial_corpus_bytes": corpus_stats["total_bytes"],
            "rng": run_rng,
            "latest_snapshot": None,
        }

        phase_results = []
        run_passed = True
        stop_scenario = False
        t0 = time.monotonic()

        try:
            vykar_cmd.vykar_delete_repo(vykar_bin, config_path, repo_label)

            for i, phase in enumerate(phases):
                action = phase["action"]
                label = phase.get("label", "")
                phase_desc = f"{action}" + (f" ({label})" if label else "")
                _log(f"  [{run_id}] phase {i+1}/{len(phases)}: {phase_desc}")

                pr = _run_phase(phase, ctx=ctx)
                phase_results.append(pr)

                status = "ok" if pr["passed"] else "FAILED"
                _log(f"  [{run_id}]   -> {status} ({pr['duration_sec']}s) {pr['detail'][:120]}")

                if not pr["passed"]:
                    run_passed = False
                    if pr.get("stop_scenario"):
                        stop_scenario = True
                        _log(f"  [{run_id}] repeated verify diff mismatch; full diff follows")
                        print(pr["detail"], file=sys.stderr, flush=True)
                    break

        except KeyboardInterrupt:
            _log(f"  [{run_id}] interrupted — cleaning up")
            run_passed = False
        finally:
            if not run_passed:
                try:
                    vykar_cmd.vykar_delete_repo(vykar_bin, config_path, repo_label)
                except Exception:
                    pass

        duration = round(time.monotonic() - t0, 2)
        failed_phases = [
            f"{p['action']}({p.get('label','')}): {p['detail'][:80]}"
            for p in phase_results
            if not p["passed"]
        ]

        run_summary = {
            "run_id": run_id,
            "passed": run_passed,
            "duration_sec": duration,
            "phases_passed": sum(1 for p in phase_results if p["passed"]),
            "phases_total": len(phase_results),
            "failed_phases": failed_phases,
            "phases": phase_results,
        }
        all_summaries.append(run_summary)
        write_run_summary(run_dir, phase_results)

        if not run_passed:
            all_passed = False

        _log(f"  Run {run_id}: {'PASS' if run_passed else 'FAIL'} ({duration}s)")
        if stop_scenario:
            _log(f"Stopping scenario after run {run_id} due to repeated verify diff mismatch")
            break

    corpus_dir = os.path.join(work_dir, "corpus")
    if os.path.exists(corpus_dir):
        shutil.rmtree(corpus_dir, ignore_errors=True)

    write_aggregate_report(work_dir, all_summaries)
    print_summary(all_summaries)

    _log(f"Report: {os.path.join(work_dir, 'report.json')}")
    return all_passed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="scenario",
        description="YAML-driven scenario testing for vykar",
    )
    parser.add_argument("scenario", help="path to YAML scenario file")
    parser.add_argument(
        "--backend",
        choices=["local", "rest", "s3", "sftp"],
        default=None,
        help="storage backend (default: all)",
    )
    parser.add_argument("--runs", type=int, default=1, help="number of runs (default: 1)")
    parser.add_argument("--output-dir", default="./output", help="output directory (default: ./output)")
    parser.add_argument("--vykar-bin", default=None, help="path to vykar binary (default: from PATH)")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed (default: random)")
    parser.add_argument(
        "--corpus-gb",
        type=float,
        default=None,
        help="override corpus size in GiB (default: scenario YAML)",
    )

    args = parser.parse_args()

    if args.corpus_gb is not None and args.corpus_gb <= 0:
        parser.error("--corpus-gb must be greater than 0")

    vykar_bin = args.vykar_bin
    if vykar_bin is None:
        vykar_bin = shutil.which("vykar")
        if vykar_bin is None:
            print("error: vykar binary not found on PATH; use --vykar-bin", file=sys.stderr)
            sys.exit(1)

    seed = args.seed if args.seed is not None else random.randint(0, 2**31)

    with open(args.scenario) as f:
        scenario = yaml.safe_load(f)
    if scenario is None:
        scenario = {}

    if args.corpus_gb is not None:
        corpus_cfg = scenario.setdefault("corpus", {})
        corpus_cfg["size_gib"] = args.corpus_gb

    backends = [args.backend] if args.backend else ["local", "rest", "s3", "sftp"]
    all_passed = True

    try:
        for backend in backends:
            if len(backends) > 1:
                print(f"\n{'='*60}", file=sys.stderr)
                print(f"  Backend: {backend}", file=sys.stderr)
                print(f"{'='*60}", file=sys.stderr, flush=True)

            passed = run_scenario(
                scenario,
                backend=backend,
                runs=args.runs,
                output_dir=args.output_dir,
                vykar_bin=vykar_bin,
                seed=seed,
            )
            if not passed:
                all_passed = False
    except CorpusDependencyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)

    sys.exit(0 if all_passed else 1)
