import sys
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import europe_catchup as catchup


class TestEuropeCatchup(unittest.TestCase):
    def setUp(self):
        self.now = catchup.START + timedelta(hours=1)
        self.report = {
            "health_ts": "2026-10-15T03:40:00Z",
            "available_or_unknown_below_6_valid_samples": 100,
            "deficient_nodes_due_now": 100,
        }
        self.status = {"status": "built", "health_ts": self.report["health_ts"]}
        self.europe = [self.make_run()]
        self.good = [self.make_run(event="workflow_run")]

    def make_run(self, **fields):
        return {"createdAt": "2026-10-15T03:41:00Z", "status": "completed",
                "conclusion": "success", "event": "workflow_dispatch", "url": "https://example.test/run", **fields}

    def decision(self, **overrides):
        return catchup.decide(overrides.get("report", self.report), overrides.get("status", self.status),
                              overrides.get("europe", self.europe), overrides.get("good", self.good),
                              now=overrides.get("now", self.now))["action"]

    def test_dispatch_when_deficient_nodes_are_due(self):
        self.assertEqual(self.decision(), "europe")

    def test_execute_is_rejected_outside_intended_github_runner(self):
        for env in ({}, {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": "wrong/repo"}):
            with patch.dict("os.environ", env, clear=True):
                with self.assertRaises(RuntimeError):
                    catchup.main(["--execute"])

    def test_current_europe_or_good_prevents_duplicate(self):
        for field in ("europe", "good"):
            for status in catchup.ACTIVE:
                self.assertEqual(self.decision(**{field: [self.make_run(status=status, conclusion="")]}), "wait")

    def test_old_jobless_queue_does_not_block_today(self):
        old = self.make_run(createdAt="2026-10-07T00:00:00Z", status="queued", conclusion="")
        self.assertEqual(self.decision(europe=[old] + self.europe), "europe")

    def test_failed_runs_stop_without_retries(self):
        for conclusion in ("failure", "timed_out", "cancelled", "action_required"):
            self.assertEqual(self.decision(europe=[self.make_run(conclusion=conclusion)]), "fail")
            self.assertEqual(self.decision(good=[self.make_run(conclusion=conclusion)]), "fail")

    def test_cooldown_does_not_launch_empty_batch(self):
        self.report["deficient_nodes_due_now"] = 0
        self.assertEqual(self.decision(), "wait")

    def test_nearby_cooldowns_are_combined_but_tail_batch_is_not_starved(self):
        self.report["deficient_nodes_due_now"] = 10
        self.report["next_deficient_node_due_at"] = (self.now + timedelta(minutes=3)).isoformat()
        self.assertEqual(self.decision(), "wait")
        self.report["next_deficient_node_due_at"] = (self.now + timedelta(hours=1)).isoformat()
        self.assertEqual(self.decision(), "europe")
        self.report["deficient_nodes_due_now"] = 5400
        self.report["next_deficient_node_due_at"] = (self.now + timedelta(minutes=1)).isoformat()
        self.report["available_or_unknown_below_6_valid_samples"] = 20000
        self.assertEqual(self.decision(), "europe")

    def test_complete_only_after_matching_good_build(self):
        self.report["available_or_unknown_below_6_valid_samples"] = 0
        self.assertEqual(self.decision(), "stop")
        self.assertEqual(self.decision(status={"status": "built", "health_ts": "old"}), "build")
        self.assertEqual(self.decision(good=[]), "fail")

    def test_good_repair_is_allowed_only_once_per_measurement(self):
        self.assertEqual(self.decision(status={}), "build")
        self.assertEqual(self.decision(status={}, good=[self.make_run()]), "fail")

    def test_before_window_and_after_deadline_never_dispatch(self):
        self.assertEqual(self.decision(now=catchup.START - timedelta(seconds=1)), "wait")
        self.assertEqual(self.decision(now=catchup.DEADLINE), "stop")
        self.assertEqual(self.decision(now=catchup.LAST_START), "stop")

    def test_manual_cap_includes_already_started_batches(self):
        self.assertEqual(self.decision(europe=self.europe * catchup.MAX_MANUAL_BATCHES), "stop")
        self.assertEqual(self.decision(europe=self.europe * (catchup.MAX_MANUAL_BATCHES - 1)), "europe")

    def test_scheduled_runs_do_not_use_manual_cap(self):
        self.assertEqual(self.decision(europe=[self.make_run(event="workflow_run")] * 40), "europe")

    def test_deadline_does_not_launch_a_repair(self):
        self.assertEqual(self.decision(status={}, now=catchup.DEADLINE), "stop")

    def test_real_coverage_counts_failed_attempts_and_cooldown(self):
        stamp = self.now.isoformat()
        a, b = "1.2.3.4:443#US", "2.3.4.5:443#DE"
        report = catchup.coverage(f"{a}\n{b}\n{a}\n", {
            "ts": stamp, "schema_version": 2,
            "proxies": {
                a: {"last_probe_at": stamp, "last_available": True, "last_speed_at": stamp,
                    "speed_samples": [3.0, None], "speed_sample_times": [stamp, stamp]},
                b: {"last_probe_at": stamp, "last_available": False},
            },
        }, f"{a}\n{a}\n", now=self.now)
        self.assertEqual(report["candidates"], 2)
        self.assertEqual(report["available"], 1)
        self.assertEqual(report["unavailable"], 1)
        self.assertEqual(report["availability_tested_today"], 2)
        self.assertEqual(report["today_speed_attempts_current_candidates"], 2)
        self.assertEqual(report["today_speed_succeeded_current_candidates"], 1)
        self.assertEqual(report["today_speed_failed_current_candidates"], 1)
        self.assertEqual(report["available_or_unknown_below_6_valid_samples"], 1)
        self.assertEqual(report["deficient_nodes_due_now"], 0)
        self.assertEqual(report["all_good_unique_nodes"], 1)

    def test_stale_samples_do_not_count_and_unknown_candidates_are_due(self):
        a, b = "1.2.3.4:443#US", "2.3.4.5:443#DE"
        report = catchup.coverage(f"{a}\n{b}\n", {
            "ts": self.now.isoformat(), "proxies": {
                a: {"last_available": True, "last_probe_at": self.now.isoformat(),
                    "speed_samples": [5.0] * 6, "speed_sample_times": ["2026-09-01T00:00:00Z"] * 6,
                    "last_speed_at": "2026-09-01T00:00:00Z"},
            },
        }, "", now=self.now)
        self.assertEqual(report["available_or_unknown_below_6_valid_samples"], 2)
        self.assertEqual(report["deficient_nodes_due_now"], 2)
        self.assertEqual(report["unknown"], 1)


if __name__ == "__main__":
    unittest.main()
