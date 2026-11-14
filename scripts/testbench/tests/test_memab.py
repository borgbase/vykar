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


if __name__ == "__main__":
    unittest.main()
