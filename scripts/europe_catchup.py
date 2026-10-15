#!/usr/bin/env python3
"""Bounded, GitHub-only coordinator for the 2026-10-15 sampling catch-up."""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from build_good import HEALTH_MIN_SAMPLES, build_health_map, is_healthy
from common import DATA_DIR, line_to_key
from europe_check import select_speed_keys
from europe_health import SAMPLE_MAX_AGE_DAYS, is_fresh, parse_timestamp

REPO = "carterdemordaunt/40asj129aaad2fa24fga1a2-a-ssf3dsdf"
START = parse_timestamp("2026-10-15T03:11:21Z")
DAY_START = parse_timestamp("2026-10-14T16:00:00Z")
DEADLINE = parse_timestamp("2026-10-15T15:30:00Z")
LAST_START = DEADLINE - timedelta(minutes=30)
MAX_MANUAL_BATCHES = 28
ACTIVE = {"queued", "in_progress", "requested", "waiting", "pending"}
TERMINAL = {"stop", "fail"}
DISPATCH_GRACE = timedelta(minutes=5)


def recent_runs(runs: list[dict]) -> list[dict]:
    # The jobless October 7 queue record is not part of this operation.
    return [run for run in runs if (parse_timestamp(run.get("createdAt")) or DAY_START) >= START]


def coverage(source: str, history: dict, good_text: str, *, now: datetime) -> dict:
    keys = {key for line in source.splitlines() if (key := line_to_key(line))}
    states = history.get("proxies", {})
    if not keys or not isinstance(states, dict) or parse_timestamp(history.get("ts")) is None:
        raise ValueError("missing candidate pool or valid Europe history")
    health = build_health_map(history)
    candidates, missing, histogram = {}, set(), Counter()
    cooling_until = []
    tested_today = unavailable = unknown = too_late = attempts = succeeded = healthy = 0
    missing_samples = untested_today = no_valid = mature = mature_rejected = 0
    for key in keys:
        state = states.get(key, {})
        if not isinstance(state, dict):
            raise ValueError(f"invalid history for {key}")
        probe_at = parse_timestamp(state.get("last_probe_at"))
        tested_today += bool(probe_at and DAY_START <= probe_at <= now)
        available = state.get("last_available") is True
        unknown += not bool(probe_at)
        unavailable += bool(probe_at) and not available
        speeds = state.get("speed_samples") or []
        times = state.get("speed_sample_times") or [state.get("last_speed_at")] * len(speeds)
        valid = 0
        for speed, stamp in zip(speeds, times):
            at = parse_timestamp(stamp)
            ok = isinstance(speed, (int, float)) and math.isfinite(speed) and speed > 0
            valid += ok and is_fresh(stamp, SAMPLE_MAX_AGE_DAYS * 24, now=now)
            if at and DAY_START <= at <= now:
                attempts += 1
                succeeded += ok
        histogram[str(min(valid, HEALTH_MIN_SAMPLES))] += 1
        qualified = is_healthy(key, health, now=now)
        healthy += qualified
        if available:
            last_speed = parse_timestamp(state.get("last_speed_at"))
            untested_today += last_speed is None or last_speed < DAY_START
            no_valid += valid == 0
            mature += valid >= HEALTH_MIN_SAMPLES
            mature_rejected += valid >= HEALTH_MIN_SAMPLES and not qualified
        candidates[key] = {"available": available or not probe_at}
        if candidates[key]["available"] and valid < HEALTH_MIN_SAMPLES:
            missing.add(key)
            missing_samples += HEALTH_MIN_SAMPLES - valid
            last = parse_timestamp(state.get("last_speed_at"))
            first_due = max(now, last + timedelta(hours=2)) if last else now
            if first_due > now:
                cooling_until.append(first_due)
            earliest = first_due + timedelta(hours=2 * (HEALTH_MIN_SAMPLES - valid - 1))
            too_late += earliest >= LAST_START
    due = select_speed_keys(candidates, history, budget=len(keys), now=now)
    good_keys = {key for line in good_text.splitlines() if (key := line_to_key(line))}
    return {
        "health_ts": history["ts"],
        "candidates": len(keys),
        "availability_tested_today": tested_today,
        "available": sum(state.get("last_available") is True for key, state in states.items() if key in keys),
        "unavailable": unavailable,
        "unknown": unknown,
        "available_not_speed_tested_today": untested_today,
        "available_without_valid_speed_samples": no_valid,
        "available_with_6_valid_samples": mature,
        "available_sample_mature_but_not_good": mature_rejected,
        "valid_sample_histogram_6_means_at_least_6": dict(sorted(histogram.items())),
        "available_or_unknown_below_6_valid_samples": len(missing),
        "minimum_missing_valid_samples_available_or_unknown": missing_samples,
        "minimum_additional_full_5400_batches": math.ceil(missing_samples / 5400),
        "deficient_nodes_due_now": sum(key in missing for key in due),
        "all_nodes_due_now": len(due),
        "next_deficient_node_due_at": min(cooling_until).isoformat() if cooling_until else None,
        "optimistically_cannot_reach_6_by_deadline": too_late,
        "today_speed_attempts_current_candidates": attempts,
        "today_speed_succeeded_current_candidates": succeeded,
        "today_speed_failed_current_candidates": attempts - succeeded,
        "policy_qualified_now": healthy,
        "all_good_unique_nodes": len(good_keys),
        "latest_batch": history.get("coverage", {}),
    }


def decide(report: dict, status: dict, europe: list[dict], good: list[dict], *, now: datetime) -> dict:
    europe, good = recent_runs(europe), recent_runs(good)

    def result(action: str, reason: str) -> dict:
        return {"action": action, "reason": reason}

    if now < START:
        return result("wait", "The one-day catch-up window has not started")
    failed = [run for run in europe + good if run.get("status") == "completed"
              and run.get("conclusion") not in ("success", "skipped", "neutral")]
    if failed:
        return result("fail", f"A catch-up/quality run failed; no blind retries: {failed[0].get('url')}")
    if now >= DEADLINE:
        return result("stop", "23:30 Beijing deadline reached; report partial coverage honestly")
    if any(run.get("status") in ACTIVE for run in europe):
        return result("wait", "A current Europe run is active; do not dispatch another")
    if any(run.get("status") in ACTIVE for run in good):
        return result("wait", "Wait for the current good build before the next batch")
    if status.get("status") != "built" or status.get("health_ts") != report["health_ts"]:
        at = parse_timestamp(report["health_ts"])
        repairs = [run for run in good if run.get("event") == "workflow_dispatch"
                   and parse_timestamp(run["createdAt"]) >= at]
        if repairs:
            return result("fail", "One manual good rebuild already ran without publishing matching health_ts")
        return result("build", "Rebuild good once for the latest completed Europe measurement")
    if not any(run.get("conclusion") == "success" for run in good):
        return result("fail", "No successful good Action found for this catch-up")
    if report["available_or_unknown_below_6_valid_samples"] == 0:
        return result("stop", "All currently available candidates have six valid samples; latest good is built")
    if now >= LAST_START:
        return result("stop", "Stop new batches at 23:00 Beijing, reserving 30 minutes for publishing")
    manual_count = sum(run.get("event") == "workflow_dispatch" for run in europe)
    if manual_count >= MAX_MANUAL_BATCHES:
        return result("stop", f"Reached the {MAX_MANUAL_BATCHES}-batch safety cap; remaining deficits are reported")
    if report["deficient_nodes_due_now"] == 0:
        return result("wait", "Deficient nodes are cooling down; keep the two-hour sampling interval")
    next_due = parse_timestamp(report.get("next_deficient_node_due_at"))
    if (report["deficient_nodes_due_now"] < min(5400, report["available_or_unknown_below_6_valid_samples"])
            and next_due and now < next_due <= now + timedelta(minutes=10)):
        return result("wait", "More deficient nodes are due within ten minutes; combine downloads into an efficient batch")
    return result("europe", f"Continue oldest-first coverage; {manual_count}/{MAX_MANUAL_BATCHES} manual batches used")


def gh_runs(workflow: str) -> list[dict]:
    output = subprocess.check_output([
        "gh", "run", "list", "--repo", REPO, "--workflow", workflow,
        "--created", ">=2026-10-15T03:11:21Z", "--limit", "100",
        "--json", "databaseId,status,conclusion,createdAt,event,url",
    ], text=True, encoding="utf-8", timeout=45)
    runs = json.loads(output)
    if len(runs) >= 100:
        raise ValueError("run query reached its safety limit; refusing an incomplete dispatch count")
    return runs


def publish(plan: dict, report: dict, runs: list[dict]) -> None:
    complete = (report.get("available_or_unknown_below_6_valid_samples") == 0
                and report.get("good_status", {}).get("status") == "built"
                and report.get("good_status", {}).get("health_ts") == report.get("health_ts")
                and report.get("latest_successful_europe_run_published", True))
    outcome = ("failed" if plan["action"] == "fail" else
               "complete" if plan["action"] == "stop" and complete else
               "partial" if plan["action"] == "stop" else "running")
    payload = {**plan, "outcome": outcome, "coverage": report, "recent_actions": runs[:8]}
    rendered = json.dumps(payload, indent=2, ensure_ascii=True)
    print(rendered)
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "w", encoding="utf-8") as stream:
            stream.write("## Europe catch-up: 2026-10-15\n\n")
            stream.write(f"**{plan['action']}**: {plan['reason']}\n\n```json\n{rendered}\n```\n")
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(f"stop={'true' if plan['action'] in TERMINAL else 'false'}\n")


def load_report(*, now: datetime, refresh: bool) -> dict:
    if refresh:
        # Read one fetched commit, not the checkout from hours before this poll.
        subprocess.run(["git", "fetch", "--no-tags", "origin", "content:refs/remotes/origin/content"],
                       check=True, timeout=60)
        revision = subprocess.check_output(["git", "rev-parse", "origin/content"], text=True, timeout=15).strip()

        def read(path: str) -> str:
            return subprocess.check_output(["git", "show", f"{revision}:data/{path}"],
                                           timeout=30).decode("utf-8-sig")
    else:
        def read(path: str) -> str:
            return (DATA_DIR / path).read_text(encoding="utf-8-sig")

    report = coverage(read("valid/all.txt"), json.loads(read("quality/europe.json")),
                      read("valid/all_good.txt"), now=now)
    report["good_status"] = json.loads(read("quality/good_status.json"))
    report["good_meta"] = json.loads(read("quality/good_meta.json"))
    return report


def dispatch(workflow: str) -> None:
    command = ["gh", "workflow", "run", workflow, "--repo", REPO, "--ref", "main"]
    if workflow == "europe-check.yml":
        command += ["--field", "speed_target_samples=6"]
    subprocess.run(command, check=True, timeout=45)


def run_iteration(*, execute: bool, refresh: bool, pending: dict | None = None) -> tuple:
    europe, good = gh_runs("europe-check.yml"), gh_runs("build-good.yml")
    now = datetime.now(timezone.utc)
    report = load_report(now=now, refresh=refresh)
    status = report["good_status"]
    plan = decide(report, status, europe, good, now=now)
    completed = [run for run in recent_runs(europe) if run.get("conclusion") == "success"]
    report["latest_successful_europe_run_published"] = not completed or (
        max(parse_timestamp(run["createdAt"]) for run in completed) <= parse_timestamp(report["health_ts"]))
    if (plan["action"] != "fail" and now < DEADLINE
            and not report["latest_successful_europe_run_published"]):
        plan = {"action": "fail", "reason": "Published Europe history is older than a successful run; refusing stale decisions"}
    if pending and plan["action"] not in TERMINAL:
        runs = europe if pending["workflow"] == "europe-check.yml" else good
        observed = [run for run in runs if run.get("event") == "workflow_dispatch"
                    and run.get("databaseId") not in pending["known_ids"]]
        if observed:
            pending = None
        elif now - pending["sent_at"] >= DISPATCH_GRACE:
            plan = {"action": "fail", "reason": "Dispatched Action did not appear within five minutes; no duplicate retry"}
        else:
            plan = {"action": "wait", "reason": "Wait for the dispatched Action to appear; do not dispatch twice"}
    if plan["action"] in ("europe", "build") and execute:
        # Recheck both lists, including completed runs, before using the snapshot.
        if gh_runs("europe-check.yml") != europe or gh_runs("build-good.yml") != good:
            plan = {"action": "wait", "reason": "Action state changed during this poll; refresh before dispatch"}
        else:
            checked_at = datetime.now(timezone.utc)
            current = decide(report, status, europe, good, now=checked_at)
            if current["action"] != plan["action"]:
                plan = current
            else:
                workflow = "europe-check.yml" if plan["action"] == "europe" else "build-good.yml"
                runs = europe if plan["action"] == "europe" else good
                dispatch(workflow)
                pending = {"workflow": workflow, "sent_at": checked_at,
                           "known_ids": {run["databaseId"] for run in runs}}
    return plan, report, europe + good, pending


def watch(*, poll_seconds: float, session_seconds: float) -> int:
    started = time.monotonic()
    pending = None
    while True:
        plan, report, runs, pending = run_iteration(execute=True, refresh=True, pending=pending)
        publish(plan, report, runs)
        if plan["action"] in TERMINAL:
            return int(plan["action"] == "fail")
        if time.monotonic() - started >= session_seconds and pending is None:
            publish({"action": "yield", "reason": "Continue in the next serial GitHub job before the hosted-job time limit"},
                    report, runs)
            return 0
        remaining = (DEADLINE - datetime.now(timezone.utc)).total_seconds()
        time.sleep(max(1, min(poll_seconds, remaining)))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true", help="Dispatch only inside this repository's GitHub Action")
    ap.add_argument("--watch", action="store_true", help="Keep coordinating inside a bounded GitHub job")
    ap.add_argument("--poll-seconds", type=float, default=60)
    ap.add_argument("--session-seconds", type=float, default=5 * 60 * 60)
    args = ap.parse_args(argv)
    if args.execute and (os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REPOSITORY") != REPO):
        raise RuntimeError("Execution is restricted to the intended GitHub repository")
    if args.watch and not args.execute:
        ap.error("--watch requires --execute inside the intended GitHub repository")
    if not 15 <= args.poll_seconds <= 60 or not 0 < args.session_seconds <= 5 * 60 * 60:
        ap.error("poll interval must be 15-60 seconds and each session at most five hours")
    if args.watch:
        return watch(poll_seconds=args.poll_seconds, session_seconds=args.session_seconds)
    plan, report, runs, _pending = run_iteration(execute=args.execute, refresh=args.execute)
    publish(plan, report, runs)
    return int(plan["action"] == "fail")


if __name__ == "__main__":
    raise SystemExit(main())
