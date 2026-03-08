import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout

from vykar_testbench import scenarios


class ScenarioReportTests(unittest.TestCase):
    def test_build_summary_aggregates_runs_and_phases(self) -> None:
        summaries = [
            {
                "run_id": "001",
                "passed": True,
                "duration_sec": 10.0,
                "phases": [
                    {"action": "init", "label": "", "passed": True, "duration_sec": 1.0},
                    {
                        "action": "backup",
                        "label": "baseline",
                        "passed": True,
                        "duration_sec": 4.0,
                        "corpus_bytes_at_phase_start": 2 * (1024 ** 3),
                    },
                ],
            },
            {
                "run_id": "002",
                "passed": False,
                "duration_sec": 13.0,
                "phases": [
                    {"action": "init", "label": "", "passed": True, "duration_sec": 2.0},
                    {"action": "backup", "label": "baseline", "passed": False, "duration_sec": 5.0},
                ],
            },
            {
                "run_id": "003",
                "passed": True,
                "duration_sec": 14.0,
                "phases": [
                    {"action": "init", "label": "", "passed": True, "duration_sec": 3.0},
                    {
                        "action": "backup",
                        "label": "baseline",
                        "passed": True,
                        "duration_sec": 6.0,
                        "corpus_bytes_at_phase_start": 3 * (1024 ** 3),
                    },
                ],
            },
        ]

        summary = scenarios.build_summary(summaries)

        self.assertEqual(summary["run_totals"]["count"], 2)
        self.assertEqual(summary["run_totals"]["excluded_failed_runs"], 1)
        self.assertAlmostEqual(summary["run_totals"]["mean_sec"], 12.0)
        self.assertAlmostEqual(summary["run_totals"]["stdev_sec"], 2.8284271247461903)

        self.assertEqual([phase["key"] for phase in summary["phases"]], ["init", "backup (baseline)"])
        self.assertEqual(summary["phases"][0]["passed_count"], 3)
        self.assertEqual(summary["phases"][0]["failed_count"], 0)
        self.assertAlmostEqual(summary["phases"][0]["mean_sec"], 2.0)
        self.assertEqual(summary["phases"][1]["passed_count"], 2)
        self.assertEqual(summary["phases"][1]["failed_count"], 1)
        self.assertAlmostEqual(summary["phases"][1]["mean_sec"], 5.0)
        self.assertEqual(summary["phases"][1]["normalization"], "corpus_bytes_at_phase_start")
        self.assertAlmostEqual(summary["phases"][1]["mean_sec_per_gib"], 2.0)

    def test_write_aggregate_report_includes_summary_top_level(self) -> None:
        summaries = [
            {
                "run_id": "001",
                "passed": True,
                "duration_sec": 1.5,
                "phases_passed": 1,
                "phases_total": 1,
                "failed_phases": [],
                "phases": [{"action": "init", "label": "", "passed": True, "duration_sec": 1.5}],
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = scenarios.write_aggregate_report(tmpdir, summaries)
            with open(path) as f:
                payload = json.load(f)

        self.assertEqual(list(payload.keys()), ["total_runs", "passed", "failed", "summary", "runs"])
        self.assertEqual(payload["summary"]["run_totals"]["count"], 1)
        self.assertEqual(payload["summary"]["phases"][0]["key"], "init")
        self.assertIn("note", payload["summary"])

    def test_print_summary_shows_aggregates_not_per_run_durations(self) -> None:
        summaries = [
            {
                "run_id": "001",
                "passed": True,
                "duration_sec": 2.0,
                "phases_passed": 2,
                "phases_total": 2,
                "failed_phases": [],
                "phases": [
                    {"action": "init", "label": "", "passed": True, "duration_sec": 0.5},
                    {
                        "action": "backup",
                        "label": "baseline",
                        "passed": True,
                        "duration_sec": 1.5,
                        "corpus_bytes_at_phase_start": 1 * (1024 ** 3),
                    },
                    {
                        "action": "churn",
                        "label": "",
                        "passed": True,
                        "duration_sec": 4.0,
                        "corpus_bytes_at_phase_start": 2 * (1024 ** 3),
                        "stats": {"added_bytes": 2 * (1024 ** 3)},
                    },
                ],
            },
            {
                "run_id": "002",
                "passed": False,
                "duration_sec": 3.0,
                "phases_passed": 1,
                "phases_total": 2,
                "failed_phases": ["list(): timeout"],
                "phases": [
                    {"action": "init", "label": "", "passed": True, "duration_sec": 0.6},
                    {"action": "backup", "label": "baseline", "passed": False, "duration_sec": 2.4},
                ],
            },
        ]

        out = io.StringIO()
        with redirect_stdout(out):
            scenarios.print_summary(summaries)

        printed = out.getvalue()
        self.assertIn("Performance Summary", printed)
        self.assertIn("note: Runs reused a shared corpus", printed)
        self.assertIn("total run: samples=1 avg=2.00s stdev=0.00s min=2.00s max=2.00s", printed)
        self.assertIn("init: samples=2 avg=0.55s", printed)
        self.assertIn("backup (baseline): samples=1 avg=1.50 sec/GiB", printed)
        self.assertIn("churn: samples=1 avg=2.00 sec/GiB-added", printed)
        self.assertIn("Failed Runs", printed)
        self.assertIn("Run 002: phases=1/2", printed)
        self.assertNotIn("duration=2.0s", printed)


if __name__ == "__main__":
    unittest.main()
