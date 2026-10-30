import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import europe_check as ec  # noqa: E402


class TestEuropeCheck(unittest.TestCase):
    LINE = "1.2.3.4:443#US-120ms-5.00MB/s-fast-V4"
    KEY = "1.2.3.4:443#US"

    def test_parse_endpoint(self):
        self.assertEqual(ec.parse_endpoint(self.LINE), (self.KEY, "1.2.3.4", 443))
        self.assertIsNone(ec.parse_endpoint("broken"))

    def test_country_long_test_threshold_is_three_mbps(self):
        self.assertEqual(ec.DEFAULT_MIN_SPEED_MBPS, 2.0)
        self.assertEqual(ec.DEFAULT_LONG_MIN_SPEED_MBPS, 3.0)

    def test_long_scheduler_rotates_only_mature_speed_candidates(self):
        now = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
        fresh = now.isoformat()
        mature = {
            "samples": [1] * 6,
            "speed_samples": [4.0] * 6,
            "speed_sample_times": [fresh] * 6,
            "speed_sample_count": 6,
            "success_pct": 100,
            "streak": 6,
            "median_speed_mbps": 4.0,
            "speed_spread_pct": 0.1,
            "last_available": True,
            "reachable_pct": 100,
            "availability_streak": 6,
            "last_probe_at": fresh,
            "last_speed_at": fresh,
        }
        history = {"proxies": {
            "untested": dict(mature),
            "recent": {**mature, "last_long_at": (now - timedelta(hours=1)).isoformat()},
            "slow": {**mature, "median_speed_mbps": 2.9},
        }}
        results = {key: {"available": True} for key in history["proxies"]}
        selected = ec.select_long_keys(
            results, history, budget=10, now=now, interval_hours=2,
        )
        self.assertEqual(selected, ["untested"])
        self.assertEqual(ec.select_long_keys(
            results, history, budget=0, now=now,
        ), [])

    def test_long_history_records_only_scheduled_attempts_and_breaks_streak(self):
        start = datetime(2026, 10, 20, 10, tzinfo=timezone.utc)
        first = start.isoformat()
        second = (start + timedelta(hours=2)).isoformat()
        history = {"proxies": {"node": {
            "long_samples": [1], "long_sample_times": [first], "long_streak": 1,
            "last_long_at": first,
        }}}
        ec.update_long_history(history, {"node": {
            "long_tested": True, "long_ok": True, "long_checked_at": second,
            "long_requests": 60, "long_duration_seconds": 60.0,
        }}, now=second)
        state = history["proxies"]["node"]
        self.assertEqual(state["long_samples"], [1, 1])
        self.assertEqual(state["long_streak"], 2)
        third = (start + timedelta(hours=4)).isoformat()
        ec.update_long_history(history, {"node": {
            "long_tested": True, "long_ok": False, "long_checked_at": third,
            "long_requests": 19, "long_duration_seconds": 22.0,
            "long_error": "ConnectionResetError",
        }}, now=third)
        self.assertEqual(state["long_samples"], [1, 1, 0])
        self.assertEqual(state["long_streak"], 0)
        self.assertFalse(state["last_long_ok"])
        self.assertEqual(state["long_error"], "ConnectionResetError")

    def test_long_connection_reuses_one_tls_stream_for_the_full_duration(self):
        class Reader:
            async def readuntil(self, _separator):
                return b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n"

            async def readexactly(self, size):
                return b"x" * size

        class Writer:
            def __init__(self):
                self.requests = []
                self.closed = False

            def write(self, request):
                self.requests.append(request)

            async def drain(self):
                return None

            def close(self):
                self.closed = True

            async def wait_closed(self):
                return None

        reader, writer = Reader(), Writer()
        connector = AsyncMock(return_value=(reader, writer))
        with patch.object(ec.asyncio, "open_connection", connector):
            ok, requests, elapsed, error = asyncio.run(ec.measure_long_connection(
                "1.2.3.4", 443, host=ec.DEFAULT_SPEED_HOST,
                path=ec.DEFAULT_LONG_PATH, duration=0.03, interval=0.005,
                timeout=0.1, ctx=ec.tls_context(),
            ))
        self.assertTrue(ok, error)
        self.assertGreaterEqual(elapsed, 0.03)
        self.assertEqual(requests, len(writer.requests))
        self.assertGreaterEqual(requests, 4)
        self.assertTrue(all(b"Connection: keep-alive" in req for req in writer.requests))
        connector.assert_awaited_once()
        self.assertTrue(writer.closed)

    def test_opt_in_catchup_skips_mature_nodes_but_keeps_failures_and_cooldown(self):
        now = datetime(2026, 10, 15, 5, tzinfo=timezone.utc)
        old = "2026-10-15T01:00:00Z"
        recent = "2026-10-15T04:30:00Z"
        results = {key: {"available": True} for key in ("mature", "failed", "cooling", "stale")}
        history = {"proxies": {
            "mature": {"speed_samples": [3.0] * 6, "speed_sample_times": [old] * 6, "last_speed_at": old},
            "failed": {"speed_samples": [3.0] * 5 + [None], "speed_sample_times": [old] * 6, "last_speed_at": old},
            "cooling": {"speed_samples": [None], "speed_sample_times": [recent], "last_speed_at": recent},
            "stale": {"speed_samples": [3.0] * 6, "speed_sample_times": ["2026-09-01T00:00:00Z"] * 6,
                      "last_speed_at": "2026-09-01T00:00:00Z"},
        }}
        self.assertEqual(set(ec.select_speed_keys(results, history, budget=10, now=now)), {"mature", "failed", "stale"})
        self.assertEqual(set(ec.select_speed_keys(results, history, budget=10, now=now, target_samples=6)), {"failed", "stale"})

    def test_history_promotes_after_stable_runs(self):
        history = {}
        result = {
            self.KEY: {"ok": True, "ms": 42.5, "speed_mbps": 5.0, "error": None, "line": self.LINE}
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
                self.KEY: {"samples": [1, 1, 1], "success_pct": 100, "streak": 3,
                           "reachable_pct": 100, "availability_streak": 3},
                "2.2.2.2:443#DE": {
                    "samples": [1, 1, 1], "success_pct": 100, "streak": 3,
                    "reachable_pct": 100, "availability_streak": 3,
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

    def test_history_requalifies_retained_speeds_after_threshold_change(self):
        previous = {
            "ts": "2026-09-24T00:00:00Z",
            "proxies": {
                self.KEY: {
                    "samples": [0, 0, 1],
                    "speed_samples": [1.5, 3.0, 6.0],
                    "latency_samples": [40.0, 40.0, 40.0],
                    "streak": 1,
                }
            },
        }
        result = {
            self.KEY: {
                "ok": True, "qualified": True, "reachable": True,
                "speed_mbps": 3.0, "ms": 40.0,
            }
        }
        history = ec.update_history(
            previous, result, window=12, now="2026-09-24T01:00:00Z"
        )
        state = history["proxies"][self.KEY]
        self.assertEqual(state["samples"], [0, 1, 1, 1])
        self.assertEqual(state["success_pct"], 75)
        self.assertEqual(state["streak"], 3)

    def test_history_expires_samples_older_than_retention(self):
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

    def test_default_source_is_full_pool(self):
        self.assertEqual(ec.DEFAULT_SOURCE, ROOT / "data" / "valid" / "all.txt")
        self.assertEqual(ec.DEFAULT_SPEED_BUDGET, 5400)
        workflow = (ROOT / ".github/workflows/europe-check.yml").read_text(encoding="utf-8")
        self.assertIn("--source data/valid/all.txt", workflow)
        self.assertIn(f"--speed-budget {ec.DEFAULT_SPEED_BUDGET}", workflow)

    def test_unscheduled_speed_keeps_history_without_extending_speed_age(self):
        history = {}
        for i in range(6):
            history = ec.update_history(history, {self.KEY: {
                "ok": True, "speed_mbps": 8.0, "ms": 40,
            }}, window=12, now=f"2026-09-24T00:0{i}:00Z")
        prior = history["proxies"][self.KEY]
        history = ec.update_history(history, {self.KEY: {
            "available": True, "speed_tested": False, "ms": 40,
        }}, window=12, now="2026-09-25T02:00:00Z")
        state = history["proxies"][self.KEY]
        self.assertEqual(state["speed_samples"], prior["speed_samples"])
        self.assertEqual(state["speed_sample_times"], prior["speed_sample_times"])
        self.assertEqual(state["last_speed_at"], prior["last_speed_at"])
        self.assertEqual(state["success_pct"], 100)
        self.assertEqual(state["streak"], 6)
        # A long HTTP gap resets only availability streak, not speed history.
        self.assertEqual(state["availability_samples"], [1])

    def test_partial_run_retains_other_nodes_until_retention_expires(self):
        previous = {"ts": "2026-09-24T00:00:00Z", "proxies": {
            self.KEY: {"samples": [1], "speed_samples": [8.0]},
        }}
        history = ec.update_history(previous, {}, window=12, now="2026-09-25T00:00:00Z")
        self.assertIn(self.KEY, history["proxies"])
        self.assertEqual(history["proxies"][self.KEY]["last_speed_at"], previous["ts"])
        expired = ec.update_history(history, {}, window=12, now="2026-10-10T00:00:00Z")
        self.assertNotIn(self.KEY, expired["proxies"])

    def test_actual_failed_download_records_one_failure(self):
        history = ec.update_history({}, {self.KEY: {
            "available": True, "speed_tested": True, "speed_mbps": None,
        }}, window=12, now="2026-09-24T00:00:00Z")
        state = history["proxies"][self.KEY]
        self.assertEqual(state["speed_samples"], [None])
        self.assertEqual(state["samples"], [0])
        self.assertEqual(state["availability_samples"], [1])

    def test_history_keeps_actual_measurement_times_not_publication_time(self):
        probe_at = "2026-09-24T00:00:00Z"
        speed_at = "2026-09-24T00:15:00Z"
        history = ec.update_history({}, {self.KEY: {
            "available": True, "speed_tested": True, "speed_mbps": 8.0,
            "probed_at": probe_at, "speed_checked_at": speed_at,
        }}, window=12, now="2026-09-24T00:25:00Z")
        state = history["proxies"][self.KEY]
        self.assertEqual(state["last_probe_at"], probe_at)
        self.assertEqual(state["last_check"], probe_at)
        self.assertEqual(state["last_speed_at"], speed_at)
        self.assertEqual(state["speed_sample_times"], [speed_at])
        self.assertEqual(state["last_ok"], speed_at)

    def test_sample_retention_is_per_measurement_not_top_level_timestamp(self):
        previous = {"ts": "2026-09-26T00:00:00Z", "proxies": {self.KEY: {
            "last_probe_at": "2026-09-26T00:00:00Z",
            "last_speed_at": "2026-09-25T00:00:00Z",
            "speed_samples": [8.0] * 6,
            "speed_sample_times": ["2026-09-01T00:00:00Z"] * 5 + ["2026-09-25T00:00:00Z"],
        }}}
        history = ec.update_history(previous, {self.KEY: {
            "available": True, "speed_tested": False,
        }}, window=12, now="2026-09-26T02:00:00Z")
        state = history["proxies"][self.KEY]
        self.assertEqual(state["speed_samples"], [8.0])
        self.assertEqual(state["speed_sample_count"], 1)
        self.assertEqual(state["last_speed_at"], "2026-09-25T00:00:00Z")

    def test_current_classification_uses_per_node_speed_freshness(self):
        now = datetime(2026, 9, 26, tzinfo=timezone.utc)
        states = {
            "fresh": {"last_speed_at": "2026-09-25T00:00:00Z", "last_speed_mbps": 8},
            "stale": {"last_speed_at": "2026-09-24T00:00:00Z", "last_speed_mbps": 8},
            "failed_http": {"last_speed_at": "2026-09-25T00:00:00Z", "last_speed_mbps": 8},
        }
        results = {key: {"available": key != "failed_http", "speed_tested": False} for key in states}
        ec.classify_results(results, min_speed_mbps=2, history={"proxies": states}, now=now)
        self.assertTrue(results["fresh"]["qualified"])
        self.assertFalse(results["stale"]["qualified"])
        self.assertFalse(results["failed_http"]["qualified"])

    def test_speed_scheduler_prioritizes_unmeasured_then_oldest_and_skips_unavailable(self):
        now = datetime(2026, 9, 26, tzinfo=timezone.utc)
        results = {key: {"available": True} for key in ("new", "old", "recent", "dead", "due")}
        results["dead"]["available"] = False
        history = {"proxies": {
            key: {"last_speed_at": stamp, "speed_samples": [8]} for key, stamp in (
                ("old", "2026-09-24T00:00:00Z"),
                ("recent", "2026-09-25T23:00:00Z"),
                ("due", "2026-09-25T22:00:00Z"),
            )
        }}
        self.assertEqual(ec.select_speed_keys(results, history, budget=2, now=now), ["new", "old"])
        self.assertEqual(ec.select_speed_keys(results, history, budget=10, now=now), ["new", "old", "due"])
        self.assertEqual(ec.select_speed_keys(results, history, budget=0, now=now), [])

    def test_daily_rotation_covers_entire_21532_node_pool(self):
        now = datetime(2026, 9, 26, tzinfo=timezone.utc)
        results = {f"node-{i}": {"available": True} for i in range(21532)}
        history = {"proxies": {}}
        seen = set()
        for turn in range(12):
            at = now + timedelta(hours=2 * turn)
            keys = ec.select_speed_keys(results, history, budget=1800, now=at)
            self.assertLessEqual(len(keys), 1800)
            seen.update(keys)
            for key in keys:
                history["proxies"][key] = {"last_speed_at": at.isoformat(), "speed_samples": [8]}
        self.assertEqual(seen, set(results))

    def test_http_probe_does_not_measure_speed_and_http_failure_is_not_available(self):
        reader = AsyncMock()
        writer = AsyncMock()
        writer.write = lambda _data: None
        writer.close = lambda: None
        reader.readuntil.return_value = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n"
        reader.read.return_value = b"x"
        with patch.object(ec.asyncio, "open_connection", AsyncMock(return_value=(reader, writer))), \
             patch.object(ec, "measure_speed", AsyncMock()) as speed:
            args = dict(sni=ec.DEFAULT_SNI, path=ec.DEFAULT_PATH, timeout=3, ctx=ec.tls_context())
            self.assertTrue(asyncio.run(ec.probe_once("1.2.3.4", 443, **args))[0])
            reader.readuntil.return_value = b"HTTP/1.1 403 Forbidden\r\n\r\n"
            self.assertFalse(asyncio.run(ec.probe_once("1.2.3.4", 443, **args))[0])
            speed.assert_not_awaited()

    def test_availability_concurrency_is_bounded_and_speed_is_not_scheduled(self):
        active = peak = 0

        async def fake_probe(*_args, **_kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return True, 30.0, None, None

        lines = [f"10.0.0.{i}:443#US" for i in range(1, 26)]
        with patch.object(ec, "probe_once", fake_probe):
            results = asyncio.run(ec.probe_all(
                lines, workers=3, timeout=3, retries=0, sni=ec.DEFAULT_SNI, path=ec.DEFAULT_PATH,
            ))
        self.assertEqual(peak, 3)
        self.assertEqual(len(results), len(lines))
        self.assertTrue(all(state["available"] and not state["speed_tested"] for state in results.values()))


class TestEuropeRun(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.source = root / "all.txt"
        self.history = root / "europe.json"
        self.outputs = [root / f"{name}.txt" for name in ("current", "stable", "quality", "fast")]
        self.lines = [f"10.0.0.{i}:443#US-100ms-100.00MB/s" for i in range(1, 26)]
        self.source.write_text("\n".join(self.lines), encoding="utf-8")
        self.history.write_text('{"previous":true}\n', encoding="utf-8")
        for path in self.outputs:
            path.write_text("keep-me\n", encoding="utf-8")
        self.argv = ["--source", str(self.source), "--history", str(self.history)]
        for flag, path in zip(("--current-out", "--stable-out", "--quality-out", "--fast-out"), self.outputs):
            self.argv.extend([flag, str(path)])

    def results(self, *, available=True):
        return {ec.line_to_key(line): {"line": line, "ok": available, "available": available,
                "reachable": available, "speed_tested": False, "ms": 50} for line in self.lines}

    def assert_preserved(self):
        self.assertEqual(self.history.read_text(encoding="utf-8"), '{"previous":true}\n')
        for path in self.outputs:
            self.assertEqual(path.read_text(encoding="utf-8"), "keep-me\n")

    def test_whole_http_failure_preserves_history_and_outputs(self):
        with patch.object(ec, "probe_all", AsyncMock(return_value=self.results(available=False))), \
             patch.object(ec, "probe_speeds", AsyncMock()) as speeds:
            self.assertEqual(ec.main(self.argv), 2)
            speeds.assert_not_awaited()
        self.assert_preserved()

    def test_whole_speed_failure_preserves_history_and_outputs(self):
        with patch.object(ec, "probe_all", AsyncMock(return_value=self.results())), \
             patch.object(ec, "measure_speed", AsyncMock(return_value=None)):
            self.assertEqual(ec.main(self.argv), 2)
        self.assert_preserved()

    def test_timeout_preserves_history_and_outputs(self):
        async def never_complete(*_args, **_kwargs):
            await asyncio.Event().wait()
        with patch.object(ec, "probe_all", never_complete):
            self.assertEqual(ec.main(self.argv + ["--run-timeout", "0.01"]), 2)
        self.assert_preserved()

    def test_timeout_during_speed_phase_preserves_history_and_outputs(self):
        async def never_complete(*_args, **_kwargs):
            await asyncio.Event().wait()
        with patch.object(ec, "probe_all", AsyncMock(return_value=self.results())), \
             patch.object(ec, "measure_speed", never_complete):
            self.assertEqual(ec.main(self.argv + ["--run-timeout", "0.01"]), 2)
        self.assert_preserved()

    def test_incomplete_run_preserves_history_and_outputs(self):
        with patch.object(ec, "probe_all", AsyncMock(return_value={})):
            self.assertEqual(ec.main(self.argv), 2)
        self.assert_preserved()

    def test_mature_unqualified_measurements_remove_stale_stable_list(self):
        now = "2026-09-26T02:00:00Z"
        previous = {}
        for hour in range(3):
            previous = ec.update_history(previous, {
                key: {**state, "speed_tested": True, "speed_mbps": 1.0}
                for key, state in self.results().items()
            }, window=12, now=f"2026-09-26T0{hour}:00:00Z")
        self.history.write_text(json.dumps(previous), encoding="utf-8")
        with patch.object(ec, "probe_all", AsyncMock(return_value=self.results())), \
             patch.object(ec, "utc_now", return_value=now):
            self.assertEqual(ec.main(self.argv + ["--speed-budget", "0"]), 0)
        self.assertFalse(self.outputs[1].exists())

    def test_full_pool_checked_each_run_and_speed_budget_rotates(self):
        now = datetime(2026, 9, 26, tzinfo=timezone.utc)
        self.history.unlink()
        seen = set()
        for turn in range(3):
            stamp = (now + timedelta(hours=2 * turn)).isoformat()
            with patch.object(ec, "probe_all", AsyncMock(return_value=self.results())) as availability, \
                 patch.object(ec, "measure_speed", AsyncMock(return_value=8.0)) as speed, \
                 patch.object(ec, "utc_now", return_value=stamp):
                self.assertEqual(ec.main(self.argv + ["--speed-budget", "10", "--output-limit", "2"]), 0)
                self.assertEqual(availability.await_args.args[0], self.lines)
                self.assertEqual(speed.await_count, 10)
            history = json.loads(self.history.read_text(encoding="utf-8"))
            self.assertEqual(history["coverage"]["availability_tested"], 25)
            self.assertEqual(len(history["proxies"]), 25)
            self.assertEqual(len(self.outputs[0].read_text().splitlines()), 2)
            for key, state in history["proxies"].items():
                self.assertEqual(len(state["availability_samples"]), turn + 1)
                if state["speed_samples"]:
                    seen.add(key)
                    self.assertEqual(state["success_pct"], 100)
        self.assertEqual(seen, {ec.line_to_key(line) for line in self.lines})


if __name__ == "__main__":
    unittest.main()
