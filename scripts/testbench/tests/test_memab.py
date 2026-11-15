import unittest

from vykar_testbench import memab


class ParseVariantsTests(unittest.TestCase):
    def test_parses_binaries_and_env(self) -> None:
        variants = memab.parse_variants(["base=/bin/a", "cand=/bin/b"], ["cand:KNOB=1"])
        self.assertEqual([v.name for v in variants], ["base", "cand"])
        self.assertEqual(variants[0].binary, "/bin/a")
        self.assertEqual(variants[0].env, {})
        self.assertEqual(variants[1].env, {"KNOB": "1"})

    def test_rejects_malformed_specs(self) -> None:
        for specs, env in ([["nobinary"], []], [["a=/x", "a=/y"], []], [["a=/x"], ["b:K=V"]], [["a=/x"], ["a:KV"]], [[], []]):
            with self.assertRaises(ValueError):
                memab.parse_variants(specs, env)


class RoundOrderTests(unittest.TestCase):
    def test_every_variant_runs_first_equally_often(self) -> None:
        variants = [memab.Variant(n, "/bin/x") for n in "abc"]
        firsts = [memab.round_order(variants, r)[0].name for r in range(6)]
        self.assertEqual(firsts, ["a", "b", "c", "a", "b", "c"])
        self.assertEqual(sorted(v.name for v in memab.round_order(variants, 4)), ["a", "b", "c"])


class SummarizeTests(unittest.TestCase):
    def test_reports_paired_delta_against_first_variant(self) -> None:
        mib = 1024
        results = [
            memab.RunResult("full", 0, "base", 400 * mib, 10.0, 8.0, 1.0, 0),
            memab.RunResult("full", 0, "cand", 300 * mib, 11.0, 8.0, 1.0, 0),
            memab.RunResult("full", 1, "base", 500 * mib, 10.0, 8.0, 1.0, 0),
            memab.RunResult("full", 1, "cand", 320 * mib, 10.0, 8.0, 1.0, 0),
            # Failed runs are excluded from the statistics.
            memab.RunResult("full", 2, "cand", 9999 * mib, 1.0, 1.0, 1.0, 1),
        ]
        text = memab.summarize(results, ["base", "cand"])
        cand = next(line for line in text.splitlines() if line.startswith("cand"))
        fields = cand.split()
        self.assertEqual(fields[1], "2")  # n
        self.assertEqual(fields[2], "310.0")  # median rss
        self.assertEqual(fields[7], "-140.0")  # paired median delta
        self.assertEqual(fields[8], "2/2")  # rounds lower than baseline
        self.assertNotIn("incremental", text)


class WorkloadSelectionTests(unittest.TestCase):
    def test_both_means_the_backup_workloads(self) -> None:
        self.assertEqual(memab.selected_workloads("both"), ["full", "incremental"])
        self.assertEqual(memab.selected_workloads("restore"), ["restore"])


class ProcStatusTests(unittest.TestCase):
    STATUS = (
        "Name:\tvykar\nVmHWM:\t  300000 kB\nVmRSS:\t  250000 kB\nRssAnon:\t  200000 kB\n"
        "RssFile:\t   49000 kB\nRssShmem:\t    1000 kB\nVmSwap:\t       0 kB\nThreads:\t5\n"
    )

    def test_parses_sampled_fields_only(self) -> None:
        self.assertEqual(
            memab.parse_proc_status(self.STATUS),
            {"VmHWM": 300000, "VmRSS": 250000, "RssAnon": 200000, "RssFile": 49000, "RssShmem": 1000, "Threads": 5},
        )
        # A zombie's status has no memory fields.
        self.assertEqual(memab.parse_proc_status("Name:\tvykar\nState:\tZ (zombie)\n"), {})

    def test_peak_is_per_field_max_ignoring_time(self) -> None:
        samples = [{"t_ms": 0, "RssAnon": 5, "RssFile": 9}, {"t_ms": 50, "RssAnon": 7}, {"t_ms": 100, "RssAnon": 6, "RssFile": 1}]
        self.assertEqual(memab.peak_of(samples), {"RssAnon": 7, "RssFile": 9})


class RestoreDestTests(unittest.TestCase):
    def test_rejects_overlap_with_protected_dirs(self) -> None:
        for dest in ("/data/corpus", "/data/corpus/sub", "/data", "/"):
            with self.assertRaises(ValueError):
                memab.check_restore_dest(dest, ["/data/corpus", "/mnt/repos/w"])
        memab.check_restore_dest("/data/corpus-dest", ["/data/corpus", "/mnt/repos/w"])


class SpaceShortfallTests(unittest.TestCase):
    def test_needs_on_one_filesystem_add_up(self) -> None:
        free = {"a": 100, "b": 100}
        dev = {"/x/1": "a", "/x/2": "a", "/y": "b"}
        kw = dict(margin=1.0, free=lambda p: free[dev[p]], device=lambda p: dev[p])
        self.assertEqual(memab.space_shortfall({"/x/1": 60, "/y": 60}, **kw), "")
        msg = memab.space_shortfall({"/x/1": 60, "/x/2": 60}, **kw)
        self.assertIn("/x/1, /x/2", msg)


if __name__ == "__main__":
    unittest.main()
