"""Restore + diff verification."""

import os
import platform
import shutil
import subprocess
import sys

from . import vykar as vykar_cmd


def verify_snapshot(vykar_bin: str, config_path: str, repo_label: str,
                    snapshot_id: str, corpus_dir: str, work_dir: str) -> tuple[bool, str]:
    """Restore a snapshot and diff against the corpus.

    Returns (passed, detail_message).
    """
    restore_dir = os.path.join(work_dir, "restore")
    if os.path.exists(restore_dir):
        shutil.rmtree(restore_dir)
    os.makedirs(restore_dir, exist_ok=True)

    result = vykar_cmd.vykar_restore(vykar_bin, config_path, repo_label, snapshot_id, restore_dir)
    if result.returncode != 0:
        shutil.rmtree(restore_dir, ignore_errors=True)
        return False, f"restore failed (rc={result.returncode}): {result.stderr[-500:]}"

    # diff -qr: report only differing files
    diff_args = ["diff", "-qr"]
    if platform.system() != "Darwin":
        diff_args.append("--no-dereference")
    diff_args += [corpus_dir, restore_dir]

    diff_result = subprocess.run(diff_args, capture_output=True, text=True)

    shutil.rmtree(restore_dir, ignore_errors=True)

    if diff_result.returncode == 0:
        return True, "restore matches corpus"
    else:
        lines = diff_result.stdout.strip().splitlines()
        summary = "\n".join(lines[:20])
        if len(lines) > 20:
            summary += f"\n... and {len(lines) - 20} more differences"
        return False, f"diff mismatch:\n{summary}"
