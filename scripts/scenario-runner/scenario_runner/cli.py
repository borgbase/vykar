"""Argument parsing and entry point."""

import argparse
import random
import shutil
import sys

import yaml

from .runner import run_scenario


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="scenario-runner",
        description="YAML-driven scenario testing for vykar",
    )
    parser.add_argument("scenario", help="path to YAML scenario file")
    parser.add_argument("--backend", choices=["local", "rest", "s3", "sftp"],
                        default=None, help="storage backend (default: all)")
    parser.add_argument("--runs", type=int, default=1,
                        help="number of runs (default: 1)")
    parser.add_argument("--output-dir", default="./output",
                        help="output directory (default: ./output)")
    parser.add_argument("--vykar-bin", default=None,
                        help="path to vykar binary (default: from PATH)")
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed (default: random)")

    args = parser.parse_args()

    # Resolve vykar binary
    vykar_bin = args.vykar_bin
    if vykar_bin is None:
        vykar_bin = shutil.which("vykar")
        if vykar_bin is None:
            print("error: vykar binary not found on PATH; use --vykar-bin", file=sys.stderr)
            sys.exit(1)

    seed = args.seed if args.seed is not None else random.randint(0, 2**31)

    with open(args.scenario) as f:
        scenario = yaml.safe_load(f)

    backends = [args.backend] if args.backend else ["local", "rest", "s3", "sftp"]
    all_passed = True

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

    sys.exit(0 if all_passed else 1)
