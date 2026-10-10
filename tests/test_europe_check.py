import sys
import tempfile
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
            self.KEY: {
                "ok": True, "ms": 80, "speed_mbps": 6.0, "line": self.LINE
            },
            "2.2.2.2:443#DE": {
                "ok": True,
                "ms": 180,
                "speed_mbps": 8.0,
                "line": "2.2.2.2:443#DE-20ms-8.00MB/s-fast-V4",
            },
            "3.3.3.3:443#FR": {
                "ok": True,
                "ms": 60,
                "speed_mbps": 2.0,
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

    def test_classification_uses_live_speed_not_source_annotation(self):
        results = {
            self.KEY: {
                "ok": True,
                "reachable": True,
                "speed_mbps": 1.0,
                "line": "1.2.3.4:443#US-120ms-100.00MB/s",
            }
        }
        ec.classify_results(results, min_speed_mbps=5)
        self.assertFalse(results[self.KEY]["qualified"])
        self.assertEqual(results[self.KEY]["source_speed_mbps"], 100.0)

    def test_history_tracks_speed_percentiles_and_availability(self):
        results = {
            self.KEY: {
                "ok": True,
                "reachable": True,
                "qualified": True,
                "ms": 42.0,
                "speed_mbps": 8.0,
            }
        }
        history = {}
        for i in range(6):
            history = ec.update_history(
                history, results, window=12,
                now=f"2026-09-24T00:0{i}:00Z",
            )
        state = history["proxies"][self.KEY]
        self.assertEqual(state["success_pct"], 100)
        self.assertEqual(state["reachable_pct"], 100)
        self.assertEqual(state["sample_count"], 6)
        self.assertEqual(state["speed_sample_count"], 6)
        self.assertEqual(state["median_speed_mbps"], 8.0)
        self.assertEqual(state["speed_spread_pct"], 0.0)

    def test_history_resets_after_long_collection_gap(self):
        previous = {
            "ts": "2026-09-24T00:00:00Z",
            "proxies": {
                self.KEY: {
                    "samples": [1] * 6,
                    "speed_samples": [8.0] * 6,
                    "latency_samples": [40.0] * 6,
                    "streak": 6,
                }
            },
        }
        result = {
            self.KEY: {
                "ok": True, "qualified": True, "reachable": True,
                "speed_mbps": 8.0, "ms": 40.0,
            }
        }
        history = ec.update_history(
            previous, result, window=12, now="2026-10-09T00:00:00Z"
        )
        state = history["proxies"][self.KEY]
        self.assertEqual(state["samples"], [1])
        self.assertEqual(state["speed_sample_count"], 1)
        self.assertEqual(state["streak"], 1)

    def test_quality_and_fast_profiles_use_rolling_median(self):
        results = {
            self.KEY: {
                "ok": True, "qualified": True, "ms": 40,
                "speed_mbps": 8.0,
                "line": "1.2.3.4:443#US-120ms-100.00MB/s",
            },
            "2.2.2.2:443#DE": {
                "ok": True, "qualified": True, "ms": 60,
                "speed_mbps": 12.0,
                "line": "2.2.2.2:443#DE-130ms-100.00MB/s",
            },
        }
        history = {}
        for i in range(6):
            round_results = {
                key: {
                    **value,
                    "speed_mbps": 8.0 if key == self.KEY else 12.0,
                }
                for key, value in results.items()
            }
            history = ec.update_history(
                history, round_results, window=12,
                now=f"2026-09-24T00:0{i}:00Z",
            )
        quality = ec.select_lines(
            results, history, stable=False, min_samples=3,
            min_success_pct=80, min_streak=2, limit=0, profile="quality",
        )
        fast = ec.select_lines(
            results, history, stable=False, min_samples=3,
            min_success_pct=80, min_streak=2, limit=0, profile="fast",
        )
        self.assertEqual(len(quality), 2)
        measured = next(line for line in quality if line.startswith("1.2.3.4"))
        self.assertIn("8.00MB/s", measured)
        self.assertEqual(len(fast), 1)
        self.assertTrue(fast[0].startswith("2.2.2.2:"))

        legacy_history = {"proxies": {
            key: {**state, "speed_sample_count": 1}
            for key, state in history["proxies"].items()
        }}
        self.assertEqual(
            ec.select_lines(
                results, legacy_history, stable=False, min_samples=3,
                min_success_pct=80, min_streak=2, limit=0, profile="quality",
            ),
            [],
        )

    def test_quality_output_is_preserved_during_warmup_and_removed_when_mature_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "all_eu_quality.txt"
            path.write_text("stale-node\n", encoding="utf-8")

            ec.write_quality_list(path, [], history_ready=False)
            self.assertEqual(path.read_text(encoding="utf-8"), "stale-node\n")

            ec.write_quality_list(path, [], history_ready=True)
            self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()
