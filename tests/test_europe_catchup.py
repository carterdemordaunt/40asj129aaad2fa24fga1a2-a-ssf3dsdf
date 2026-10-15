import io
import json
import sys
import tempfile
import unittest
from contextlib import ExitStack
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
        self.assertEqual(report["available_not_speed_tested_today"], 0)
        self.assertEqual(report["minimum_missing_valid_samples_available_or_unknown"], 5)
        self.assertEqual(report["minimum_additional_full_5400_batches"], 1)

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

    def test_six_valid_samples_are_not_the_same_as_good(self):
        key = "1.2.3.4:443#US"
        stamp = self.now.isoformat()
        report = catchup.coverage(key, {
            "ts": stamp, "schema_version": 2, "proxies": {
                key: {"last_available": True, "last_probe_at": stamp, "last_speed_at": stamp,
                      "speed_samples": [1.0] * 6, "speed_sample_times": [stamp] * 6},
            },
        }, "", now=self.now)
        self.assertEqual(report["available_with_6_valid_samples"], 1)
        self.assertEqual(report["available_sample_mature_but_not_good"], 1)
        self.assertEqual(report["available_or_unknown_below_6_valid_samples"], 0)
        self.assertEqual(report["policy_qualified_now"], 0)

    def test_old_valid_samples_count_without_requiring_six_today(self):
        key = "1.2.3.4:443#US"
        stamp = self.now.isoformat()
        old = (self.now - timedelta(days=1)).isoformat()
        report = catchup.coverage(key, {
            "ts": stamp, "schema_version": 2, "proxies": {
                key: {"last_available": True, "last_probe_at": stamp, "last_speed_at": old,
                      "speed_samples": [3.0] * 6, "speed_sample_times": [old] * 6},
            },
        }, "", now=self.now)
        self.assertEqual(report["available_with_6_valid_samples"], 1)
        self.assertEqual(report["available_not_speed_tested_today"], 1)
        self.assertEqual(report["today_speed_attempts_current_candidates"], 0)
        self.assertEqual(report["minimum_missing_valid_samples_available_or_unknown"], 0)

    def test_reach_six_estimate_respects_last_batch_start_not_publication_deadline(self):
        key = "1.2.3.4:443#US"
        now = catchup.LAST_START - timedelta(hours=1)
        for offset, expected in ((-1, 0), (0, 1), (15, 1)):
            stamp = (catchup.LAST_START - timedelta(hours=2) + timedelta(minutes=offset)).isoformat()
            report = catchup.coverage(key, {
                "ts": now.isoformat(), "proxies": {
                    key: {"last_available": True, "last_probe_at": now.isoformat(), "last_speed_at": stamp,
                          "speed_samples": [3.0] * 5, "speed_sample_times": [stamp] * 5},
                },
            }, "", now=now)
            self.assertEqual(report["optimistically_cannot_reach_6_by_deadline"], expected)

    def test_terminal_report_distinguishes_partial_from_complete(self):
        with tempfile.TemporaryDirectory() as folder:
            summary = Path(folder) / "summary.md"
            output = Path(folder) / "output.txt"
            with patch.dict("os.environ", {"GITHUB_STEP_SUMMARY": str(summary), "GITHUB_OUTPUT": str(output)}):
                with patch("builtins.print"):
                    catchup.publish({"action": "wait", "reason": "cooldown"}, self.report, [])
                    catchup.publish({"action": "stop", "reason": "deadline"}, self.report, [])
                text = summary.read_text(encoding="utf-8")
                self.assertEqual(text.count("## Europe catch-up"), 1)
                self.assertIn('"outcome": "partial"', text)
                self.assertTrue(output.read_text().endswith("stop=true\n"))
                complete = {**self.report, "available_or_unknown_below_6_valid_samples": 0,
                            "good_status": self.status}
                with patch("builtins.print"):
                    catchup.publish({"action": "stop", "reason": "finished"}, complete, [])
                self.assertIn('"outcome": "complete"', summary.read_text(encoding="utf-8"))

    def test_watch_cannot_run_as_local_scheduler(self):
        with patch("sys.stderr", new=io.StringIO()):
            with self.assertRaises(SystemExit):
                catchup.main(["--watch"])
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(RuntimeError):
                catchup.main(["--execute", "--watch"])

    def test_cloud_workflow_has_bounded_serial_jobs_not_event_recursion(self):
        workflow = (Path(__file__).resolve().parent.parent / ".github/workflows/europe-catchup.yml").read_text()
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("workflow_run:", workflow)
        self.assertNotIn("schedule:", workflow)
        self.assertIn("max-parallel: 1", workflow)
        self.assertIn("slice: [1, 2, 3]", workflow)
        self.assertIn("timeout-minutes: 330", workflow)
        self.assertIn("--execute --watch", workflow)


class TestCoordinatorRuntime(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.now = catchup.START + timedelta(hours=1)
        self.clock = stack.enter_context(patch.object(catchup, "datetime"))
        self.clock.now.return_value = self.now
        self.europe = [{"databaseId": 10, "createdAt": "2026-10-15T03:20:00Z", "status": "completed",
                        "conclusion": "success", "event": "workflow_dispatch"}]
        self.good = [{"databaseId": 20, "createdAt": "2026-10-15T03:41:00Z", "status": "completed",
                      "conclusion": "success", "event": "workflow_run"}]
        self.report = {"health_ts": "2026-10-15T03:40:00Z", "available_or_unknown_below_6_valid_samples": 100,
                       "deficient_nodes_due_now": 100,
                       "good_status": {"status": "built", "health_ts": "2026-10-15T03:40:00Z"}}
        self.load_report_helper = catchup.load_report
        self.loader = stack.enter_context(patch.object(catchup, "load_report", return_value=self.report))
        self.runs = stack.enter_context(patch.object(catchup, "gh_runs", side_effect=self.list_runs))
        self.dispatch_helper = catchup.dispatch
        self.dispatch = stack.enter_context(patch.object(catchup, "dispatch"))

    def list_runs(self, workflow):
        return self.europe if workflow == "europe-check.yml" else self.good

    def poll(self, pending=None):
        return catchup.run_iteration(execute=True, refresh=True, pending=pending)

    def test_dispatch_once_even_when_run_visibility_is_delayed(self):
        plan, _report, _runs, pending = self.poll()
        self.assertEqual(plan["action"], "europe")
        self.assertEqual(pending["workflow"], "europe-check.yml")
        plan, _report, _runs, _pending = self.poll(pending)
        self.assertEqual(plan["action"], "wait")
        self.dispatch.assert_called_once_with("europe-check.yml")
        self.clock.now.return_value = self.now + catchup.DISPATCH_GRACE
        plan, _report, _runs, _pending = self.poll(pending)
        self.assertEqual(plan["action"], "fail")
        self.dispatch.assert_called_once()

    def test_visible_queued_run_clears_pending_and_blocks_duplicate(self):
        _plan, _report, _runs, pending = self.poll()
        self.europe = [{**self.europe[0], "databaseId": 11, "status": "queued", "conclusion": ""}] + self.europe
        plan, _report, _runs, pending = self.poll(pending)
        self.assertEqual(plan["action"], "wait")
        self.assertIsNone(pending)
        self.dispatch.assert_called_once()

    def test_good_rebuild_then_europe_does_not_require_workflow_run_event(self):
        self.report["good_status"] = {}
        plan, _report, _runs, pending = self.poll()
        self.assertEqual(plan["action"], "build")
        self.good = [{**self.good[0], "databaseId": 21, "event": "workflow_dispatch",
                      "createdAt": self.now.isoformat()}] + self.good
        self.report["good_status"] = {"status": "built", "health_ts": self.report["health_ts"]}
        plan, _report, _runs, _pending = self.poll(pending)
        self.assertEqual(plan["action"], "europe")
        self.assertEqual([call.args[0] for call in self.dispatch.call_args_list],
                         ["build-good.yml", "europe-check.yml"])

    def test_action_changes_during_recheck_prevent_dispatch(self):
        self.runs.side_effect = [self.europe, self.good, self.europe + [{"databaseId": 11}]]
        plan, _report, _runs, _pending = self.poll()
        self.assertEqual(plan["action"], "wait")
        self.dispatch.assert_not_called()

    def test_stale_published_history_is_not_used_to_start_another_batch(self):
        self.europe[0]["createdAt"] = "2026-10-15T04:00:00Z"
        plan, _report, _runs, _pending = self.poll()
        self.assertEqual(plan["action"], "fail")
        self.dispatch.assert_not_called()

    def test_stale_history_cannot_be_reported_as_complete(self):
        self.europe[0]["createdAt"] = "2026-10-15T04:00:00Z"
        self.report["available_or_unknown_below_6_valid_samples"] = 0
        plan, report, _runs, _pending = self.poll()
        self.assertEqual(plan["action"], "fail")
        self.assertFalse(report["latest_successful_europe_run_published"])
        self.dispatch.assert_not_called()

    def test_deadline_is_rechecked_after_network_calls(self):
        self.clock.now.side_effect = [self.now, catchup.DEADLINE]
        plan, _report, _runs, _pending = self.poll()
        self.assertEqual(plan["action"], "stop")
        self.dispatch.assert_not_called()

    def test_watch_refreshes_repeatedly_and_yields_before_hosted_job_limit(self):
        result = ({"action": "wait", "reason": "cooldown"}, self.report, [], None)
        with patch.object(catchup, "run_iteration", return_value=result) as poll:
            with patch.object(catchup, "publish") as publish:
                with patch.object(catchup.time, "monotonic", side_effect=[0, 0, 18000]):
                    with patch.object(catchup.time, "sleep") as sleep:
                        self.assertEqual(catchup.watch(poll_seconds=60, session_seconds=18000), 0)
        self.assertEqual(poll.call_count, 2)
        self.assertTrue(all(call.kwargs["refresh"] for call in poll.call_args_list))
        self.assertEqual(publish.call_args.args[0]["action"], "yield")
        sleep.assert_called_once_with(60)

    def test_watch_does_not_lose_unconfirmed_dispatch_at_slice_boundary(self):
        pending = {"workflow": "europe-check.yml"}
        result = ({"action": "wait", "reason": "pending"}, self.report, [], pending)
        observed = ({"action": "wait", "reason": "running"}, self.report, [], None)
        with patch.object(catchup, "run_iteration", side_effect=[result, observed]):
            with patch.object(catchup, "publish"), patch.object(catchup.time, "sleep") as sleep:
                with patch.object(catchup.time, "monotonic", side_effect=[0, 18000, 18001]):
                    self.assertEqual(catchup.watch(poll_seconds=60, session_seconds=18000), 0)
        sleep.assert_called_once()

    def test_failure_stops_watching_without_retry(self):
        result = ({"action": "fail", "reason": "child failed"}, self.report, [], None)
        with patch.object(catchup, "run_iteration", return_value=result) as poll:
            with patch.object(catchup, "publish"), patch.object(catchup.time, "sleep") as sleep:
                self.assertEqual(catchup.watch(poll_seconds=60, session_seconds=18000), 1)
        poll.assert_called_once()
        sleep.assert_not_called()

    def test_refreshed_files_come_from_one_pinned_commit(self):
        history = {"ts": self.report["health_ts"], "proxies": {}}
        files = {"valid/all.txt": "source", "valid/all_good.txt": "good",
                 "quality/europe.json": json.dumps(history), "quality/good_status.json": "{}",
                 "quality/good_meta.json": "{}"}

        def git_output(command, **_kwargs):
            if command[1] == "rev-parse":
                return "pinned-commit\n"
            self.assertTrue(command[2].startswith("pinned-commit:data/"))
            return files[command[2].split(":data/", 1)[1]].encode()

        with patch.object(catchup.subprocess, "run") as run:
            with patch.object(catchup.subprocess, "check_output", side_effect=git_output):
                with patch.object(catchup, "coverage", return_value={"health_ts": history["ts"]}) as coverage:
                    report = self.load_report_helper(now=self.now, refresh=True)
        self.assertEqual(run.call_args.args[0],
                         ["git", "fetch", "--no-tags", "origin", "content:refs/remotes/origin/content"])
        coverage.assert_called_once_with("source", history, "good", now=self.now)
        self.assertEqual(report["good_status"], {})

    def test_dispatch_is_scoped_to_exact_repo_main_and_opt_in_target(self):
        with patch.object(catchup.subprocess, "run") as run:
            self.dispatch_helper("europe-check.yml")
            command = ["gh", "workflow", "run", "europe-check.yml", "--repo", catchup.REPO,
                       "--ref", "main", "--field", "speed_target_samples=6"]
            run.assert_called_once_with(command, check=True, timeout=45)
            run.reset_mock()
            self.dispatch_helper("build-good.yml")
            run.assert_called_once_with(["gh", "workflow", "run", "build-good.yml", "--repo", catchup.REPO,
                                         "--ref", "main"], check=True, timeout=45)


if __name__ == "__main__":
    unittest.main()
