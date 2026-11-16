import os
import tempfile
import unittest

from vykar_testbench import netab

# Captured from `tc qdisc show dev lo` / `tc filter show dev lo` with netem.sh on 20 (port 9000).
QDISC_NETEM = (
    "qdisc prio 1: root refcnt 2 bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1\n"
    "qdisc netem 40: parent 1:4 limit 200000 delay 20ms seed 16587641499975997493\n"
)
FILTER_9000 = (
    "filter parent 1: protocol ip pref 1 u32 chain 0 \n"
    "filter parent 1: protocol ip pref 1 u32 chain 0 fh 800: ht divisor 1 \n"
    "filter parent 1: protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 flowid 1:4 not_in_hw \n"
    "  match 00060000/00ff0000 at 8\n"
    "  match 00002328/0000ffff at 20\n"
    "filter parent 1: protocol ip pref 1 u32 chain 0 fh 800::801 order 2049 key ht 800 bkt 0 flowid 1:4 not_in_hw \n"
    "  match 00060000/00ff0000 at 8\n"
    "  match 23280000/ffff0000 at 20\n"
)


class EndpointTests(unittest.TestCase):
    def test_ports_from_urls(self) -> None:
        cases = {
            ("s3", "s3+http://127.0.0.1:9000/bucket/net-ab/x"): ("127.0.0.1", 9000),
            ("s3", "s3+http://127.0.0.1/bucket/x"): ("127.0.0.1", 80),
            ("s3", "s3://127.0.0.1/bucket/x"): ("127.0.0.1", 443),
            ("rest", "http://127.0.0.1:8585"): ("127.0.0.1", 8585),
            ("rest", "https://127.0.0.1"): ("127.0.0.1", 443),
            ("sftp", "sftp://manu@127.0.0.1:2222/mnt/repos/x"): ("127.0.0.1", 2222),
            ("sftp", "sftp://manu@127.0.0.1/mnt/repos/x"): ("127.0.0.1", 22),
        }
        for (backend, url), want in cases.items():
            self.assertEqual(netab.backend_endpoint(backend, url), want, url)
        self.assertIsNone(netab.backend_endpoint("local", "/mnt/repos/x"))

    def test_rejects_unparseable_urls(self) -> None:
        for backend, url in (("rest", "http://127.0.0.1:notaport"), ("rest", "ftp://127.0.0.1/x"), ("s3", "/no/host")):
            with self.assertRaises(ValueError):
                netab.backend_endpoint(backend, url)

    def test_only_ipv4_loopback_is_shapeable(self) -> None:
        self.assertTrue(netab.is_ipv4_loopback("127.0.0.1"))
        self.assertTrue(netab.is_ipv4_loopback("127.1.2.3"))
        for host in ("::1", "localhost", "10.0.0.1", ""):
            self.assertFalse(netab.is_ipv4_loopback(host), host)


class TcCommandTests(unittest.TestCase):
    def test_netem_setup_matches_netem_sh(self) -> None:
        cmds = netab.tc_setup_commands(netab.Shaping((9000,), 20))
        self.assertEqual(cmds[0], ["qdisc", "add", "dev", "lo", "root", "handle", "1:", "prio", "bands", "4"])
        self.assertEqual(cmds[1], ["qdisc", "add", "dev", "lo", "parent", "1:4", "handle", "40:",
                                   "netem", "delay", "20ms", "limit", "200000"])
        self.assertEqual(len(cmds), 4)
        for cmd, direction in zip(cmds[2:], ("dport", "sport")):
            self.assertEqual(cmd[:4], ["filter", "add", "dev", "lo"])
            self.assertIn("u32", cmd)
            i = cmd.index(direction)
            self.assertEqual(cmd[i + 1:i + 3], ["9000", "0xffff"])
            self.assertEqual(cmd[-2:], ["flowid", "1:4"])
            self.assertEqual(cmd[cmd.index("protocol", 6) + 1:cmd.index("protocol", 6) + 3], ["6", "0xff"])

    def test_rate_and_fractional_delay(self) -> None:
        leaf = netab.tc_setup_commands(netab.Shaping((8585,), 2.5, "200mbit"))[1]
        self.assertEqual(leaf[leaf.index("netem"):], ["netem", "delay", "2.5ms", "limit", "200000", "rate", "200mbit"])

    def test_zero_delay_is_count_only(self) -> None:
        sh = netab.Shaping((22,), 0)
        self.assertFalse(sh.shapes)
        leaf = netab.tc_setup_commands(sh)[1]
        self.assertNotIn("netem", leaf)
        self.assertEqual(leaf[-3:], ["pfifo", "limit", "200000"])

    def test_teardown(self) -> None:
        self.assertEqual(netab.tc_teardown_command(), ["qdisc", "del", "dev", "lo", "root"])


class LoStateTests(unittest.TestCase):
    def test_bare_lo_is_none(self) -> None:
        self.assertIsNone(netab.parse_lo_state("qdisc noqueue 0: root refcnt 2 \n", ""))
        self.assertIsNone(netab.parse_lo_state("", ""))

    def test_parses_netem_sh_layout(self) -> None:
        self.assertEqual(netab.parse_lo_state(QDISC_NETEM, FILTER_9000), netab.Shaping((9000,), 20.0, None))

    def test_rate_compares_case_insensitively(self) -> None:
        self.assertEqual(netab.Shaping((8585,), 20, "1gbit"), netab.Shaping((8585,), 20.0, "1Gbit"))

    def test_parses_rate_and_pfifo(self) -> None:
        q = QDISC_NETEM.replace("delay 20ms", "delay 1.5ms rate 200Mbit")
        self.assertEqual(netab.parse_lo_state(q, FILTER_9000), netab.Shaping((9000,), 1.5, "200Mbit"))
        q = QDISC_NETEM.splitlines()[0] + "\nqdisc pfifo 40: parent 1:4 limit 200000p\n"
        self.assertEqual(netab.parse_lo_state(q, FILTER_9000), netab.Shaping((9000,), 0.0, None))

    def test_round_trips_setup_layout(self) -> None:
        # What tc would print for our own setup must parse back to the same Shaping.
        sh = netab.Shaping((8585,), 20)
        port = f"{8585:04x}"
        flt = FILTER_9000.replace("00002328/", f"0000{port}/").replace("23280000/", f"{port}0000/")
        self.assertEqual(netab.parse_lo_state(QDISC_NETEM, flt), sh)

    def test_refuses_foreign_setups(self) -> None:
        bad = [
            ("qdisc fq_codel 0: root refcnt 2 limit 10240p\n", ""),
            (QDISC_NETEM.replace("bands 4", "bands 3"), FILTER_9000),
            (QDISC_NETEM.replace("delay 20ms", "delay 20ms  5ms"), FILTER_9000),
            (QDISC_NETEM.replace("delay 20ms", "delay 20ms loss 1%"), FILTER_9000),
            (QDISC_NETEM + "qdisc netem 30: parent 1:3 limit 1000 delay 1ms\n", FILTER_9000),
            (QDISC_NETEM, FILTER_9000.replace("flowid 1:4", "flowid 1:3", 1)),
            (QDISC_NETEM, FILTER_9000.replace("00060000/00ff0000 at 8", "00110000/00ff0000 at 8", 1)),
        ]
        for q, f in bad:
            with self.assertRaises(ValueError, msg=q + f):
                netab.parse_lo_state(q, f)


class WindowWarningTests(unittest.TestCase):
    def test_warns_only_below_threshold_with_latency(self) -> None:
        vals = {"net.core.rmem_max": 212992, "net.core.wmem_max": 4194304, "net.ipv4.tcp_rmem[max]": None}
        warns = netab.window_warnings(vals, rtt_ms=40)
        self.assertEqual(len(warns), 1)
        self.assertIn("net.core.rmem_max=212992", warns[0])
        self.assertIn("~5.1 MiB/s", warns[0])
        self.assertEqual(netab.window_warnings(vals, rtt_ms=0), [])
        self.assertEqual(netab.window_warnings({"net.core.rmem_max": 4194304}, rtt_ms=40), [])


class BandwidthStatsTests(unittest.TestCase):
    def test_constant_rate(self) -> None:
        mib = 2**20
        samples = [(i * 0.1, int(i * 0.1 * 100 * mib)) for i in range(51)]  # 100 MiB/s for 5 s
        stats, series = netab.bw_stats(samples)
        self.assertEqual(len(series), 16)
        self.assertAlmostEqual(series[1][0], 0.3)  # buckets close on the first sample >= 250 ms
        self.assertAlmostEqual(series[1][1], 100.0, places=3)
        self.assertAlmostEqual(stats["mean_mibs"], 100.0, places=3)
        self.assertAlmostEqual(stats["p10_mibs"], 100.0, places=3)
        self.assertAlmostEqual(stats["cv"], 0.0, places=6)
        self.assertEqual(stats["idle_frac"], 0.0)

    def test_idle_head_and_tail_are_trimmed_but_gaps_count(self) -> None:
        mib = 2**20
        rates = [0, 0, 50, 50, 0, 50, 50, 0, 0]  # per 0.3 s step, MiB/s
        t, b, samples = 0.0, 0, [(0.0, 0)]
        for r in rates:
            t += 0.3
            b += int(r * 0.3 * mib)
            samples.append((t, b))
        stats, series = netab.bw_stats(samples)
        self.assertEqual(len(series), len(rates))
        self.assertAlmostEqual(stats["mean_mibs"], 40.0, places=3)  # active span 50,50,0,50,50
        self.assertAlmostEqual(stats["idle_frac"], 0.2)
        self.assertAlmostEqual(stats["max_mibs"], 50.0, places=3)
        self.assertGreater(stats["cv"], 0)

    def test_too_few_samples(self) -> None:
        self.assertEqual(netab.bw_stats([(0.0, 0), (0.1, 5)]), ({}, []))


class TraceSummaryTests(unittest.TestCase):
    def test_missing_and_empty_trace_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "t.tsv")
            self.assertEqual(netab.trace_summary(path), {})
            open(path, "w").close()
            self.assertEqual(netab.trace_summary(path), {})

    def test_classifies_ops(self) -> None:
        lines = [
            "1\t1000000\tt1\t0\t1\tPUT packs/ab/cd",
            "2\t3000000\tt1\t0\t1\tPUT packs/ab/ef",
            "3\t500000\tt2\t0\t1\tPUT packs/ab/cd part 1",
            "4\t200000\tt2\t0\t1\tGET index",
            "5\t100000\tt2\t0\t1\tHEAD snapshots/x",
            "garbage",
        ]
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "t.tsv")
            with open(path, "w") as f:
                f.write("\n".join(lines) + "\n")
            s = netab.trace_summary(path)
        self.assertEqual(s["PUT-pack-single"], {"n": 2, "sum_s": 4.0, "max_s": 3.0})
        self.assertEqual(s["PUT-part"]["n"], 1)
        self.assertEqual(s["GET-index"]["n"], 1)
        self.assertEqual(s["HEAD-snapshots"]["n"], 1)


def _row(rnd: int, variant: str, wall: float, rss: float, step: str = "full", rc: int = 0, **kw) -> dict:
    return {"round": rnd, "variant": variant, "step": step, "rc": rc, "wall_s": wall, "user_s": 1.0,
            "sys_s": 0.5, "maxrss_mib": rss, "peak_anon_mib": rss / 2, **kw}


class SummarizeTests(unittest.TestCase):
    def test_paired_deltas_against_first_variant(self) -> None:
        results = [
            _row(1, "base", 100.0, 800.0, mean_mibs=50.0),
            _row(1, "cand", 80.0, 880.0, mean_mibs=60.0),
            _row(2, "base", 120.0, 800.0, mean_mibs=40.0),
            _row(2, "cand", 132.0, 800.0, mean_mibs=40.0),
            _row(3, "base", 110.0, 800.0, mean_mibs=45.0),
            _row(3, "cand", 99.0, 840.0, mean_mibs=50.0),
            _row(4, "cand", 1.0, 1.0, rc=1),  # failed: excluded
        ]
        text = netab.summarize(results, ["base", "cand"])
        lines = text.splitlines()
        self.assertEqual(lines[0], "== full ==")
        base = next(line for line in lines if line.startswith("base")).split()
        cand = next(line for line in lines if line.startswith("cand")).split()
        self.assertEqual(base[1], "3")
        self.assertEqual(base[-3:], ["-", "-", "-"])
        self.assertEqual(cand[1], "3")
        self.assertEqual(cand[2], "99.00")  # median wall
        self.assertEqual(cand[6], "50.0")  # median bw_mean
        self.assertEqual(cand[-3:], ["-10.0", "2/3", "+5.0"])  # d_wall% (-20,+10,-10), faster, d_rss% (10,0,5)
        self.assertNotIn("== incr ==", text)

    def test_missing_bandwidth_renders_dashes_and_incr_table(self) -> None:
        results = [_row(1, "base", 10.0, 100.0), _row(1, "base", 2.0, 90.0, step="incr")]
        text = netab.summarize(results, ["base"])
        self.assertIn("== incr ==", text)
        base = next(line for line in text.splitlines() if line.startswith("base")).split()
        self.assertEqual(base[6:11], ["-", "-", "-", "-", "-"])


class VariantTests(unittest.TestCase):
    def test_per_variant_limits(self) -> None:
        vs = netab.parse_net_variants(["base=/bin/a", "cand=/bin/b"], ["cand:K=V"], ["cand=8"], ["base=2"], 4, None)
        self.assertEqual([(v.name, v.connections, v.threads, v.env) for v in vs],
                         [("base", 4, 2, {}), ("cand", 8, None, {"K": "V"})])

    def test_rejects_bad_limit_specs(self) -> None:
        for conn in (["nope=2"], ["cand"], ["cand=x"], ["cand=0"]):
            with self.assertRaises(ValueError):
                netab.parse_net_variants(["cand=/bin/b"], [], conn, [], None, None)


class SafePathTests(unittest.TestCase):
    def test_checked_child(self) -> None:
        self.assertEqual(netab._checked_child("/mnt/repos/net-ab", "run1"), "/mnt/repos/net-ab/run1")
        for parent, name in (("", "x"), ("/", "x"), ("rel", "x"), ("/mnt", ""), ("/mnt", ".."), ("/mnt", "a/b")):
            with self.assertRaises(ValueError):
                netab._checked_child(parent, name)


if __name__ == "__main__":
    unittest.main()
