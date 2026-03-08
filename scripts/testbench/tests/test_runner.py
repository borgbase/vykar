import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from vykar_testbench import corpus
from vykar_testbench import scenarios
from vykar_testbench import benchmarks


class ScenarioRunnerTests(unittest.TestCase):
    def test_list_phase_captures_output_without_echoing_header_in_detail(self) -> None:
        list_stdout = (
            "ID        Host                       Source   Label   Date\n"
            "abc123    host                       corpus   corpus  2025-03-07\n"
        )
        completed = subprocess.CompletedProcess(
            args=["vykar", "list"],
            returncode=0,
            stdout=list_stdout,
            stderr="",
        )
        ctx = {
            "vykar_bin": "vykar",
            "config_path": "/tmp/config.yaml",
            "repo_label": "scenario-simple",
        }

        with mock.patch("vykar_testbench.scenarios.vykar_cmd.vykar_list", return_value=completed):
            result = scenarios._run_phase({"action": "list"}, ctx=ctx)

        self.assertTrue(result["passed"])
        self.assertEqual(result["detail"], "captured 2 list lines")
        self.assertEqual(result["output"], list_stdout)

    def test_run_scenario_reuses_single_corpus_across_runs(self) -> None:
        scenario = {
            "name": "reuse-corpus",
            "corpus": {"size_gib": 0.1},
            "phases": [{"action": "init"}, {"action": "cleanup"}],
        }

        phase_results = [
            {"action": "init", "label": "", "passed": True, "detail": "ok", "duration_sec": 0.01},
            {"action": "cleanup", "label": "", "passed": True, "detail": "repo deleted", "duration_sec": 0.01},
        ]

        with tempfile.TemporaryDirectory() as output_dir:
            with mock.patch(
                "vykar_testbench.scenarios.corpus.validate_corpus_mix"
            ) as validate_corpus_mix, mock.patch(
                "vykar_testbench.scenarios.corpus.generate_corpus",
                return_value={"file_count": 1, "total_bytes": 1024},
            ) as generate_corpus, mock.patch(
                "vykar_testbench.scenarios.cfg.write_vykar_config",
                return_value="/mnt/repos/scenario-repo",
            ) as write_config, mock.patch(
                "vykar_testbench.scenarios.cfg.ensure_backend_ready"
            ) as ensure_backend_ready, mock.patch(
                "vykar_testbench.scenarios.vykar_cmd.vykar_delete_repo"
            ), mock.patch(
                "vykar_testbench.scenarios._run_phase",
                side_effect=phase_results * 2,
            ), mock.patch(
                "vykar_testbench.scenarios.write_run_summary"
            ), mock.patch(
                "vykar_testbench.scenarios.write_aggregate_report"
            ), mock.patch(
                "vykar_testbench.scenarios.print_summary"
            ):
                passed = scenarios.run_scenario(
                    scenario,
                    backend="local",
                    runs=2,
                    output_dir=output_dir,
                    vykar_bin="vykar",
                    seed=123,
                )

        self.assertTrue(passed)
        validate_corpus_mix.assert_called_once_with({"size_gib": 0.1})
        self.assertEqual(generate_corpus.call_count, 1)
        self.assertEqual(write_config.call_count, 1)
        ensure_backend_ready.assert_called_once_with("local", "/mnt/repos/scenario-repo")

    def test_run_scenario_validates_corpus_before_setup(self) -> None:
        scenario = {
            "name": "broken-docx",
            "corpus": {"mix": [{"type": "docx", "weight": 1, "file_size": "1kb"}]},
            "phases": [{"action": "init"}],
        }

        with tempfile.TemporaryDirectory() as output_dir:
            with mock.patch(
                "vykar_testbench.scenarios.corpus.validate_corpus_mix",
                side_effect=corpus.CorpusDependencyError("corpus type 'docx' is unavailable"),
            ) as validate_corpus_mix, mock.patch(
                "vykar_testbench.scenarios.corpus.generate_corpus"
            ) as generate_corpus, mock.patch(
                "vykar_testbench.scenarios.cfg.write_vykar_config"
            ) as write_config, mock.patch(
                "vykar_testbench.scenarios.cfg.ensure_backend_ready"
            ) as ensure_backend_ready:
                with self.assertRaisesRegex(corpus.CorpusDependencyError, "docx"):
                    scenarios.run_scenario(
                        scenario,
                        backend="local",
                        runs=1,
                        output_dir=output_dir,
                        vykar_bin="vykar",
                        seed=123,
                    )

        validate_corpus_mix.assert_called_once_with(scenario["corpus"])
        generate_corpus.assert_not_called()
        write_config.assert_not_called()
        ensure_backend_ready.assert_not_called()

    def test_churn_phase_reports_cap_stats(self) -> None:
        ctx = {
            "vykar_bin": "vykar",
            "config_path": "/tmp/config.yaml",
            "repo_label": "scenario-simple",
            "corpus_dir": "/tmp/corpus",
            "work_dir": "/tmp/work",
            "scenario": {"churn": {}},
            "corpus_config": {"size_gib": 0.1},
            "initial_corpus_bytes": 1024,
            "rng": mock.Mock(),
        }

        with mock.patch("vykar_testbench.scenarios.corpus.apply_churn", return_value={
            "added": 1,
            "deleted": 2,
            "modified": 3,
            "dirs_added": 4,
            "skipped_add_files": 5,
            "skipped_add_dirs": 6,
            "total_bytes_before": 100,
            "total_bytes_after": 200,
            "max_allowed_bytes": 400,
        }) as apply_churn:
            result = scenarios._run_phase({"action": "churn"}, ctx=ctx)

        self.assertTrue(result["passed"])
        self.assertIn("skipped_files=5", result["detail"])
        self.assertIn("skipped_dirs=6", result["detail"])
        self.assertEqual(result["stats"]["max_allowed_bytes"], 400)
        apply_churn.assert_called_once_with("/tmp/corpus", {"size_gib": 0.1}, {}, 1024, ctx["rng"])


class BenchmarkRunnerTests(unittest.TestCase):
    def test_previous_run_roots_are_sorted_newest_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_root = Path(tmpdir)
            base = runtime_root / "benchmarks"
            (base / "20260101T000000Z").mkdir(parents=True)
            (base / "20260103T000000Z").mkdir(parents=True)
            (base / "20260102T000000Z").mkdir(parents=True)
            dataset = runtime_root / "dataset"
            (dataset / "snapshot-1").mkdir(parents=True)
            (dataset / "snapshot-2").mkdir(parents=True)
            with mock.patch.dict("os.environ", {"RUNTIME_ROOT": str(runtime_root), "REPO_ROOT": str(runtime_root / "repos")}):
                with mock.patch("vykar_testbench.benchmarks.ensure_required_commands"):
                    cfg = benchmarks.build_config(runs=1, tool="vykar", dataset=str(dataset))
            object.__setattr__(cfg, "out_root", base / "20260104T000000Z")
            roots = benchmarks.list_previous_run_roots(cfg)
        # Resolve paths to handle macOS /var -> /private/var symlink
        resolved_roots = [str(Path(r).resolve()) for r in roots]
        expected = [str((base / d).resolve()) for d in ("20260103T000000Z", "20260102T000000Z", "20260101T000000Z")]
        self.assertEqual(resolved_roots, expected)

    def test_run_benchmarks_generates_report_arguments_for_single_tool(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dataset = root / "dataset"
            (dataset / "snapshot-1").mkdir(parents=True)
            (dataset / "snapshot-2").mkdir(parents=True)
            runtime_root = root / "runtime"
            (runtime_root / "benchmarks" / "20260101T000000Z").mkdir(parents=True)
            with mock.patch.dict("os.environ", {"RUNTIME_ROOT": str(runtime_root), "REPO_ROOT": str(root / "repos")}):
                with mock.patch("vykar_testbench.benchmarks.ensure_required_commands"):
                    cfg = benchmarks.build_config(runs=1, tool="vykar", dataset=str(dataset))
            with mock.patch("vykar_testbench.benchmarks.write_vykar_config"), \
                    mock.patch("vykar_testbench.benchmarks.build_storage_settle_targets", return_value=([], [])), \
                    mock.patch("vykar_testbench.benchmarks._timed_run", return_value=0), \
                    mock.patch("vykar_testbench.benchmarks.cleanup_repo_for_tool", return_value=0), \
                    mock.patch("vykar_testbench.benchmarks.cleanup_restore_for_tool"), \
                    mock.patch("vykar_testbench.benchmarks._collect_repo_sizes"), \
                    mock.patch("vykar_testbench.benchmarks._write_tool_stats"), \
                    mock.patch("vykar_testbench.benchmarks.report.main", return_value=0) as report_main, \
                    mock.patch("vykar_testbench.benchmarks._cleanup_transient_dirs"):
                rc = benchmarks.run_benchmarks(cfg)

        self.assertEqual(rc, 0)
        report_args = report_main.call_args.args[0]
        self.assertIn("--backfill-root", report_args)
        self.assertIn("--selected-tool", report_args)


if __name__ == "__main__":
    unittest.main()
