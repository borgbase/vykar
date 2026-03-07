"""JSON/stdout summary output."""

import json
import os
import sys


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
    with open(path, "w") as f:
        json.dump({
            "total_runs": total,
            "passed": passed,
            "failed": total - passed,
            "runs": summaries,
        }, f, indent=2)
    return path


def print_summary(summaries: list[dict]) -> None:
    """Print human-readable summary table to stdout."""
    total = len(summaries)
    passed = sum(1 for s in summaries if s.get("passed", False))
    failed = total - passed

    print(f"\n{'='*60}")
    print(f"  Scenario Results: {passed}/{total} runs passed")
    print(f"{'='*60}")

    for s in summaries:
        status = "PASS" if s.get("passed") else "FAIL"
        run_id = s.get("run_id", "?")
        duration = s.get("duration_sec", 0)
        phases_ok = s.get("phases_passed", 0)
        phases_total = s.get("phases_total", 0)
        print(f"  Run {run_id:>3}: [{status}]  phases={phases_ok}/{phases_total}  duration={duration:.1f}s")

        if not s.get("passed"):
            for p in s.get("failed_phases", []):
                print(f"           -> {p}")

    print(f"{'='*60}")
    if failed:
        print(f"  FAILED: {failed} run(s)")
    else:
        print(f"  All {total} run(s) passed")
    print()
