import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from vykar_testbench import probe

HEADER = "| Target | Transport |\n|---|---|"


def completed(cmd, code=0, row="| x | y |"):
    return subprocess.CompletedProcess(args=cmd, returncode=code, stdout=f"{HEADER}\n{row}\n", stderr="")


class ProbeTests(unittest.TestCase):
    def run_main(self, argv, codes=None):
        """Run main() with subprocess.run mocked; returns (rc, calls, out_dir)."""
        codes = list(codes or [])
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append((cmd, kwargs))
            if cmd[0] == "cargo":
                return subprocess.CompletedProcess(args=cmd, returncode=0)
            code = codes.pop(0) if codes else 0
            return completed(cmd, code, row=f"| {cmd[cmd.index('--label') + 1]} | t |")

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name) / "out"
        env = {"AWS_ACCESS_KEY_ID": "AKTESTKEY", "AWS_SECRET_ACCESS_KEY": "SKTESTSECRET", "TMPDIR": tmp.name}
        with (
            mock.patch.dict("os.environ", env),
            mock.patch("vykar_testbench.probe.subprocess.run", side_effect=fake_run),
            mock.patch("vykar_testbench.probe.cfg.ensure_backend_ready") as ready,
            mock.patch("vykar_testbench.probe.find_sftp_server", return_value="/usr/lib/openssh/sftp-server"),
            mock.patch("sys.stderr"),
            mock.patch("sys.stdout"),
        ):
            rc = probe.main([*argv, "--out", str(out)])
        self.ready = ready
        return rc, calls, out

    def test_argv_per_target(self):
        rc, calls, out = self.run_main(["--probe-bin", "/bin/probe", "--label", "L"])
        self.assertEqual(rc, 0)
        cmds = [c for c, _ in calls]
        self.assertEqual(len(cmds), 3)
        local, sftp_exec, s3 = cmds
        for cmd, target in zip(cmds, ["local", "sftp-exec", "s3"]):
            self.assertEqual(cmd[0], "/bin/probe")
            self.assertEqual(cmd[cmd.index("--label") + 1], f"L {target}")
            self.assertTrue(cmd[cmd.index("--json") + 1].endswith(f"-{target}.json"))
            self.assertIn("--list-rounds", cmd)
        self.assertEqual(local[-2], "local")
        self.assertEqual(sftp_exec[-4], "sftp-exec")
        self.assertEqual(sftp_exec[-2:], ["--", "/usr/lib/openssh/sftp-server"])
        i = s3.index("s3")
        self.assertEqual(s3[i + 1], probe.DEFAULT_S3_URL)
        self.assertIn("--allow-insecure-http", s3)
        self.ready.assert_called_once()

    def test_s3_credentials_in_env_not_argv(self):
        _, calls, _ = self.run_main(["--probe-bin", "/bin/probe", "--targets", "s3"])
        cmd, kwargs = calls[0]
        self.assertNotIn("AKTESTKEY", " ".join(cmd))
        self.assertNotIn("SKTESTSECRET", " ".join(cmd))
        self.assertEqual(kwargs["env"]["AWS_ACCESS_KEY_ID"], "AKTESTKEY")
        self.assertEqual(kwargs["env"]["AWS_SECRET_ACCESS_KEY"], "SKTESTSECRET")

    def test_non_loopback_refused_without_allow_remote(self):
        cases = [
            ["--targets", "s3", "--s3-url", "s3://s3.example.com/b/r"],
            ["--targets", "s3", "--s3-url", "s3+http://10.0.0.1:9000/b/r"],
            ["--targets", "sftp", "--sftp-url", "sftp://u@host.example/p"],
        ]
        for argv in cases:
            with (
                mock.patch("vykar_testbench.probe.subprocess.run") as run,
                mock.patch("vykar_testbench.probe.cfg.ensure_backend_ready") as ready,
                mock.patch("vykar_testbench.probe.cfg.load_defaults") as defaults,
                mock.patch("sys.stderr"),
            ):
                rc = probe.main(argv)
            self.assertNotEqual(rc, 0, argv)
            run.assert_not_called()
            ready.assert_not_called()
            defaults.assert_not_called()

    def test_loopback_hosts(self):
        for host in ["127.0.0.1", "127.1.2.3", "::1", "localhost"]:
            self.assertTrue(probe.is_loopback(host), host)
        for host in ["10.0.0.1", "example.com", "", "localhost.example.com"]:
            self.assertFalse(probe.is_loopback(host), host)
        self.assertEqual(probe.url_host("s3+http://127.0.0.1:9100/b/r"), "127.0.0.1")
        self.assertEqual(probe.url_host("sftp://u@[::1]:22/p"), "::1")

    def test_combined_markdown_has_one_header_and_a_row_per_target(self):
        rc, _, out = self.run_main(["--probe-bin", "/bin/probe"])
        self.assertEqual(rc, 0)
        md = next(out.glob("*-probe.md")).read_text().splitlines()
        self.assertEqual(md[:2], HEADER.splitlines())
        self.assertEqual(len(md), 2 + 3)
        self.assertEqual(sum(1 for line in md if line.startswith("| Target")), 1)

    def test_exit_code_aggregation(self):
        rc, _, _ = self.run_main(["--probe-bin", "/bin/probe"], codes=[0, 5, 3])
        self.assertEqual(rc, 5)
        self.assertEqual(probe.aggregate_exit([0, 0, 0]), 0)
        self.assertEqual(probe.aggregate_exit([0, 3, 1]), 3)

    def test_probe_bin_skips_the_build(self):
        _, calls, _ = self.run_main(["--probe-bin", "/bin/probe"])
        self.assertFalse(any(c[0] == "cargo" for c, _ in calls))
        _, calls, _ = self.run_main([])
        self.assertEqual(calls[0][0][0], "cargo")
        self.assertEqual(calls[0][0], probe.BUILD_CMD)


if __name__ == "__main__":
    unittest.main()
