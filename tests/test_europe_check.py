import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import europe_check as ec  # noqa: E402


class TestEuropeCheck(unittest.TestCase):
    LINE = "1.2.3.4:443#US-120ms-5.00MB/s-fast-V4"
    KEY = "1.2.3.4:443#US"

    def test_parse_endpoint(self):
        self.assertEqual(ec.parse_endpoint(self.LINE), (self.KEY, "1.2.3.4", 443))
        self.assertIsNone(ec.parse_endpoint("broken"))

    def test_history_promotes_after_stable_runs(self):
        history = {}
        result = {
            self.KEY: {"ok": True, "ms": 42.5, "error": None, "line": self.LINE}
        }
        for i in range(3):
            history = ec.update_history(
                history, result, window=12, now=f"2026-09-24T00:0{i}:00Z"
            )
        lines = ec.select_lines(
            result,
            history,
            stable=True,
            min_samples=3,
            min_success_pct=80,
            min_streak=2,
            limit=0,
        )
        self.assertEqual(len(lines), 1)
        self.assertIn("-42ms-", lines[0])

    def test_failure_breaks_streak_and_excludes(self):
        previous = {
            "proxies": {
                self.KEY: {
                    "samples": [1, 1, 1],
                    "success_pct": 100,
                    "streak": 3,
                    "fail_streak": 0,
                    "ms": 50,
                }
            }
        }
        failed = {
            self.KEY: {
                "ok": False,
                "ms": None,
                "error": "TimeoutError",
                "line": self.LINE,
            }
        }
        history = ec.update_history(
            previous, failed, window=12, now="2026-09-24T01:00:00Z"
        )
        state = history["proxies"][self.KEY]
        self.assertEqual(state["streak"], 0)
        self.assertEqual(state["fail_streak"], 1)
        self.assertEqual(
            ec.select_lines(
                failed,
                history,
                stable=True,
                min_samples=3,
                min_success_pct=80,
                min_streak=2,
                limit=0,
            ),
            [],
        )

    def test_ranking_prefers_success_then_latency(self):
        line2 = "2.2.2.2:443#DE-20ms-2.00MB/s-mid-V4"
        results = {
            self.KEY: {"ok": True, "ms": 80, "line": self.LINE},
            "2.2.2.2:443#DE": {"ok": True, "ms": 30, "line": line2},
        }
        history = {
            "proxies": {
                self.KEY: {"samples": [1, 1, 1], "success_pct": 100, "streak": 3},
                "2.2.2.2:443#DE": {
                    "samples": [1, 1, 1], "success_pct": 100, "streak": 3
                },
            }
        }
        lines = ec.select_lines(
            results,
            history,
            stable=True,
            min_samples=3,
            min_success_pct=80,
            min_streak=2,
            limit=1,
        )
        self.assertTrue(lines[0].startswith("2.2.2.2:443#"))

    def test_speed_filter_does_not_use_latency_cutoff(self):
        results = {
            self.KEY: {"ok": True, "ms": 80, "line": self.LINE},
            "2.2.2.2:443#DE": {
                "ok": True,
                "ms": 180,
                "line": "2.2.2.2:443#DE-20ms-8.00MB/s-fast-V4",
            },
            "3.3.3.3:443#FR": {
                "ok": True,
                "ms": 60,
                "line": "3.3.3.3:443#FR-20ms-2.00MB/s-mid-V4",
            },
        }
        ec.classify_results(results, min_speed_mbps=5)
        self.assertTrue(results[self.KEY]["qualified"])
        self.assertTrue(results["2.2.2.2:443#DE"]["qualified"])
        self.assertFalse(results["3.3.3.3:443#FR"]["qualified"])
        self.assertEqual(
            results["3.3.3.3:443#FR"]["reject_reason"], "speed"
        )

    def test_history_tracks_qualification_not_bare_reachability(self):
        result = {
            self.KEY: {
                "ok": True,
                "qualified": False,
                "ms": 250,
                "error": None,
                "line": self.LINE,
            }
        }
        history = ec.update_history(
            {}, result, window=12, now="2026-09-24T00:00:00Z"
        )
        self.assertEqual(history["proxies"][self.KEY]["samples"], [0])
        self.assertEqual(history["proxies"][self.KEY]["streak"], 0)


if __name__ == "__main__":
    unittest.main()
