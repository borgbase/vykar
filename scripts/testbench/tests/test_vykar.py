import subprocess
import unittest
from unittest import mock

from vykar_testbench import vykar


class VykarWrapperTests(unittest.TestCase):
    def test_delete_repo_tolerates_missing_repository(self) -> None:
        missing_repo = subprocess.CompletedProcess(
            args=["vykar", "delete"],
            returncode=1,
            stdout="",
            stderr="Error: no repository found at '/mnt/repos/scenario-repo'\n",
        )

        with mock.patch("vykar_testbench.vykar.subprocess.run", return_value=missing_repo):
            result = vykar.vykar_delete_repo("vykar", "/tmp/config.yaml", "scenario-simple")

        self.assertEqual(result.returncode, 0)

    def test_backup_ignores_runner_phase_label_for_configured_sources(self) -> None:
        backup_result = subprocess.CompletedProcess(
            args=["vykar", "backup"],
            returncode=0,
            stdout="Snapshot created: snap-123\n",
            stderr="",
        )

        with mock.patch("vykar_testbench.vykar.subprocess.run", return_value=backup_result) as run:
            result, snapshot_id = vykar.vykar_backup(
                "vykar",
                "/tmp/config.yaml",
                "scenario-simple",
                snapshot_label="baseline",
            )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(snapshot_id, "snap-123")
        self.assertEqual(
            run.call_args.args[0],
            ["vykar", "--config", "/tmp/config.yaml", "backup", "-R", "scenario-simple"],
        )


if __name__ == "__main__":
    unittest.main()
