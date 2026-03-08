import tempfile
import unittest
from pathlib import Path
from unittest import mock

from vykar_testbench import bench_report as report


class BenchReportTests(unittest.TestCase):
    def test_merge_records_prefers_backfill_for_missing_nonselected_tool(self) -> None:
        current = [
            {"op": "vykar_backup", "run_count": 1},
            {"op": "restic_backup", "run_count": 0},
        ]
        backfill = [
            {"op": "restic_backup", "run_count": 3},
        ]
        merged = report.merge_records(current, backfill, "nonselected", selected_tool="vykar")
        merged_map = {item["op"]: item for item in merged}
        self.assertEqual(merged_map["vykar_backup"]["run_count"], 1)
        self.assertEqual(merged_map["restic_backup"]["run_count"], 3)

    def test_main_defaults_chart_path_under_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "reports").mkdir()
            with mock.patch("vykar_testbench.bench_report.build_records", return_value=([], None, None)), \
                    mock.patch("vykar_testbench.bench_report.generate_chart_with_deps", return_value=0) as generate_chart:
                rc = report.main(["chart", str(root)])
            out_file = generate_chart.call_args.args[1]
            self.assertEqual(out_file, root / "reports" / "benchmark.summary.png")
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
