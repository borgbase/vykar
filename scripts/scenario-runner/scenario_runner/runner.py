"""Scenario orchestration loop."""

import os
import random
import shutil
import sys
import time
from datetime import datetime, timezone

from . import config as cfg
from . import corpus
from . import report
from . import verify
from . import vykar as vykar_cmd


def _log(msg: str) -> None:
    print(f"[scenario] {msg}", file=sys.stderr, flush=True)


def _run_phase(phase: dict, *, ctx: dict) -> dict:
    """Execute a single phase. Returns a result dict."""
    action = phase["action"]
    label = phase.get("label", "")
    t0 = time.monotonic()
    result = {"action": action, "label": label, "passed": False, "detail": ""}

    vykar_bin = ctx["vykar_bin"]
    config_path = ctx["config_path"]
    repo_label = ctx["repo_label"]

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
            passed, detail = verify.verify_snapshot(
                vykar_bin, config_path, repo_label, snap_id,
                ctx["corpus_dir"], ctx["work_dir"])
            result["passed"] = passed
            result["detail"] = detail

    elif action == "check":
        r = vykar_cmd.vykar_check(vykar_bin, config_path, repo_label)
        result["passed"] = r.returncode == 0
        result["detail"] = r.stderr[-500:] if r.returncode != 0 else "ok"

    elif action == "list":
        r = vykar_cmd.vykar_list(vykar_bin, config_path, repo_label)
        result["passed"] = r.returncode == 0
        result["detail"] = r.stdout[:1000] if r.returncode == 0 else r.stderr[-500:]

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
        stats = corpus.apply_churn(ctx["corpus_dir"], ctx["corpus_config"], churn_cfg, ctx["rng"])
        result["passed"] = True
        result["detail"] = (f"added={stats['added']} deleted={stats['deleted']} "
                            f"modified={stats['modified']} dirs={stats['dirs_added']}")

    elif action == "cleanup":
        vykar_cmd.vykar_delete_repo(vykar_bin, config_path, repo_label)
        result["passed"] = True
        result["detail"] = "repo deleted"

    else:
        result["detail"] = f"unknown action: {action}"

    result["duration_sec"] = round(time.monotonic() - t0, 2)
    return result


def run_scenario(scenario: dict, *, backend: str, runs: int,
                 output_dir: str, vykar_bin: str, seed: int) -> bool:
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

    for run_idx in range(1, runs + 1):
        run_rng = random.Random(seed + run_idx)
        run_id = f"{run_idx:03d}"
        run_dir = os.path.join(work_dir, f"run-{run_id}")
        os.makedirs(run_dir, exist_ok=True)

        corpus_dir = os.path.join(work_dir, "corpus")
        config_path = os.path.join(work_dir, "config.yaml")

        _log(f"Run {run_id}/{runs:03d}: generating corpus")

        # Clean corpus from previous run
        if os.path.exists(corpus_dir):
            shutil.rmtree(corpus_dir)
        os.makedirs(corpus_dir, exist_ok=True)

        corpus_stats = corpus.generate_corpus(corpus_dir, corpus_config, run_rng)
        _log(f"  corpus: {corpus_stats['file_count']} files, "
             f"{corpus_stats['total_bytes'] / 1024**2:.1f} MiB")

        repo_url = cfg.write_vykar_config(
            config_path, backend=backend, repo_label=repo_label, corpus_path=corpus_dir)
        _log(f"  repo: {repo_url}")

        ctx = {
            "vykar_bin": vykar_bin,
            "config_path": config_path,
            "repo_label": repo_label,
            "corpus_dir": corpus_dir,
            "work_dir": work_dir,
            "scenario": scenario,
            "corpus_config": corpus_config,
            "rng": run_rng,
            "latest_snapshot": None,
        }

        phase_results = []
        run_passed = True
        t0 = time.monotonic()

        try:
            # Clean start: delete any prior repo
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
                    break

        except KeyboardInterrupt:
            _log(f"  [{run_id}] interrupted — cleaning up")
            run_passed = False
        finally:
            # Best-effort cleanup of repo on interruption
            if not run_passed:
                try:
                    vykar_cmd.vykar_delete_repo(vykar_bin, config_path, repo_label)
                except Exception:
                    pass

        duration = round(time.monotonic() - t0, 2)
        failed_phases = [f"{p['action']}({p.get('label','')}): {p['detail'][:80]}"
                         for p in phase_results if not p["passed"]]

        run_summary = {
            "run_id": run_id,
            "passed": run_passed,
            "duration_sec": duration,
            "phases_passed": sum(1 for p in phase_results if p["passed"]),
            "phases_total": len(phase_results),
            "failed_phases": failed_phases,
        }
        all_summaries.append(run_summary)
        report.write_run_summary(run_dir, phase_results)

        if not run_passed:
            all_passed = False

        _log(f"  Run {run_id}: {'PASS' if run_passed else 'FAIL'} ({duration}s)")

    # Clean up corpus after all runs
    corpus_dir = os.path.join(work_dir, "corpus")
    if os.path.exists(corpus_dir):
        shutil.rmtree(corpus_dir, ignore_errors=True)

    report.write_aggregate_report(work_dir, all_summaries)
    report.print_summary(all_summaries)

    _log(f"Report: {os.path.join(work_dir, 'report.json')}")
    return all_passed
