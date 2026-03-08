import io
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest import mock

from vykar_testbench import scenarios
from vykar_testbench import corpus


class ScenarioCliTests(unittest.TestCase):
    def _write_scenario(self, content: str) -> str:
        tmp = tempfile.NamedTemporaryFile("w", delete=False, suffix=".yaml")
        tmp.write(content)
        tmp.flush()
        tmp.close()
        self.addCleanup(lambda: __import__("os").unlink(tmp.name))
        return tmp.name

    def test_corpus_gb_overrides_yaml_value(self) -> None:
        scenario_path = self._write_scenario("name: test\ncorpus:\n  size_gib: 25\n")

        with mock.patch("vykar_testbench.scenarios.shutil.which", return_value="/usr/bin/vykar"), \
                mock.patch("vykar_testbench.scenarios.run_scenario", return_value=True) as run_scenario, \
                mock.patch("sys.argv", [
                    "scenario",
                    scenario_path,
                    "--backend", "local",
                    "--corpus-gb", "3.5",
                ]):
            with self.assertRaises(SystemExit) as exc:
                scenarios.main()

        self.assertEqual(exc.exception.code, 0)
        scenario = run_scenario.call_args.args[0]
        self.assertEqual(scenario["corpus"]["size_gib"], 3.5)

    def test_corpus_gb_defaults_to_yaml_value(self) -> None:
        scenario_path = self._write_scenario("name: test\ncorpus:\n  size_gib: 25\n")

        with mock.patch("vykar_testbench.scenarios.shutil.which", return_value="/usr/bin/vykar"), \
                mock.patch("vykar_testbench.scenarios.run_scenario", return_value=True) as run_scenario, \
                mock.patch("sys.argv", [
                    "scenario",
                    scenario_path,
                    "--backend", "local",
                ]):
            with self.assertRaises(SystemExit) as exc:
                scenarios.main()

        self.assertEqual(exc.exception.code, 0)
        scenario = run_scenario.call_args.args[0]
        self.assertEqual(scenario["corpus"]["size_gib"], 25)

    def test_corpus_gb_creates_corpus_section_when_missing(self) -> None:
        scenario_path = self._write_scenario("name: test\n")

        with mock.patch("vykar_testbench.scenarios.shutil.which", return_value="/usr/bin/vykar"), \
                mock.patch("vykar_testbench.scenarios.run_scenario", return_value=True) as run_scenario, \
                mock.patch("sys.argv", [
                    "scenario",
                    scenario_path,
                    "--backend", "local",
                    "--corpus-gb", "2",
                ]):
            with self.assertRaises(SystemExit) as exc:
                scenarios.main()

        self.assertEqual(exc.exception.code, 0)
        scenario = run_scenario.call_args.args[0]
        self.assertEqual(scenario["corpus"]["size_gib"], 2.0)

    def test_corpus_gb_rejects_non_positive_values(self) -> None:
        scenario_path = self._write_scenario("name: test\n")

        for value in ("0", "-1"):
            with self.subTest(value=value):
                stderr = io.StringIO()
                with mock.patch("sys.argv", [
                    "scenario",
                    scenario_path,
                    "--corpus-gb", value,
                ]), redirect_stderr(stderr):
                    with self.assertRaises(SystemExit) as exc:
                        scenarios.main()

                self.assertEqual(exc.exception.code, 2)
                self.assertIn("--corpus-gb must be greater than 0", stderr.getvalue())

    def test_corpus_dependency_error_is_reported_without_traceback(self) -> None:
        scenario_path = self._write_scenario("name: test\n")
        stderr = io.StringIO()

        with mock.patch("vykar_testbench.scenarios.shutil.which", return_value="/usr/bin/vykar"), \
                mock.patch(
                    "vykar_testbench.scenarios.run_scenario",
                    side_effect=corpus.CorpusDependencyError("corpus type 'docx' is unavailable"),
                ) as run_scenario, \
                mock.patch("sys.argv", ["scenario", scenario_path]), \
                redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                scenarios.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertEqual(run_scenario.call_count, 1)
        output = stderr.getvalue()
        self.assertIn("error: corpus type 'docx' is unavailable", output)
        self.assertNotIn("Traceback", output)


class BenchmarkCliTests(unittest.TestCase):
    def test_runs_is_required(self) -> None:
        from vykar_testbench import benchmarks
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                benchmarks.build_parser().parse_args([])
        self.assertEqual(exc.exception.code, 2)
        self.assertIn("--runs", stderr.getvalue())

    def test_default_dataset_is_corpus_local(self) -> None:
        from vykar_testbench.benchmarks import default_dataset_dir
        from pathlib import Path
        with mock.patch("pathlib.Path.home", return_value=Path("/tmp/home")):
            self.assertEqual(default_dataset_dir(), Path("/tmp/home/corpus-local"))

    def test_main_reports_config_errors(self) -> None:
        from vykar_testbench import benchmarks
        stderr = io.StringIO()
        with mock.patch("vykar_testbench.benchmarks.build_config", side_effect=benchmarks.ConfigError("bad config")), \
                mock.patch("sys.argv", ["benchmark", "--runs", "1"]), \
                redirect_stderr(stderr):
            rc = benchmarks.main()
        self.assertEqual(rc, 2)
        self.assertIn("bad config", stderr.getvalue())

    def test_build_config_validates_expected_dataset_shape(self) -> None:
        from vykar_testbench.benchmarks import build_config
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = Path(tmpdir)
            (dataset / "snapshot-1").mkdir()
            (dataset / "snapshot-2").mkdir()
            with mock.patch("vykar_testbench.benchmarks.ensure_required_commands"):
                cfg = build_config(runs=1, tool="vykar", dataset=str(dataset))
        self.assertEqual(cfg.dataset_dir, dataset.resolve())
        self.assertEqual(cfg.selected_tools, ("vykar",))


if __name__ == "__main__":
    unittest.main()
