#!/usr/bin/env python3
"""Bounded, GitHub-only coordinator for the 2026-10-15 sampling catch-up."""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from build_good import HEALTH_MIN_SAMPLES, build_health_map, is_healthy
from common import DATA_DIR, line_to_key, read_json
from europe_check import select_speed_keys
from europe_health import SAMPLE_MAX_AGE_DAYS, is_fresh, parse_timestamp

REPO = "carterdemordaunt/40asj129aaad2fa24fga1a2-a-ssf3dsdf"
START = parse_timestamp("2026-10-15T03:11:21Z")
DAY_START = parse_timestamp("2026-10-14T16:00:00Z")
DEADLINE = parse_timestamp("2026-10-15T15:30:00Z")
LAST_START = DEADLINE - timedelta(minutes=30)
MAX_MANUAL_BATCHES = 28
ACTIVE = {"queued", "in_progress", "requested", "waiting", "pending"}


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
        healthy += is_healthy(key, health, now=now)
        candidates[key] = {"available": available or not probe_at}
        if candidates[key]["available"] and valid < HEALTH_MIN_SAMPLES:
            missing.add(key)
            last = parse_timestamp(state.get("last_speed_at"))
            first_due = max(now, last + timedelta(hours=2)) if last else now
            if first_due > now:
                cooling_until.append(first_due)
            earliest = first_due + timedelta(hours=2 * (HEALTH_MIN_SAMPLES - valid - 1))
            too_late += earliest > DEADLINE
    due = select_speed_keys(candidates, history, budget=len(keys), now=now)
    good_keys = {key for line in good_text.splitlines() if (key := line_to_key(line))}
    return {
        "health_ts": history["ts"],
        "candidates": len(keys),
        "availability_tested_today": tested_today,
        "available": sum(state.get("last_available") is True for key, state in states.items() if key in keys),
        "unavailable": unavailable,
        "unknown": unknown,
        "valid_sample_histogram_6_means_at_least_6": dict(sorted(histogram.items())),
        "available_or_unknown_below_6_valid_samples": len(missing),
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
    payload = {**plan, "coverage": report, "recent_actions": runs[:8]}
    rendered = json.dumps(payload, indent=2, ensure_ascii=True)
    print(rendered)
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as stream:
            stream.write("## Europe catch-up: 2026-10-15\n\n")
            stream.write(f"**{plan['action']}**: {plan['reason']}\n\n```json\n{rendered}\n```\n")
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(f"stop={'true' if plan['action'] in ('stop', 'fail') else 'false'}\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true", help="Dispatch only inside this repository's GitHub Action")
    args = ap.parse_args(argv)
    if args.execute and (os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REPOSITORY") != REPO):
        raise RuntimeError("Execution is restricted to the intended GitHub repository")
    now = datetime.now(timezone.utc)
    source = (DATA_DIR / "valid" / "all.txt").read_text(encoding="utf-8")
    good_file = DATA_DIR / "valid" / "all_good.txt"
    report = coverage(source, read_json(DATA_DIR / "quality" / "europe.json"),
                      good_file.read_text(encoding="utf-8") if good_file.exists() else "", now=now)
    status = read_json(DATA_DIR / "quality" / "good_status.json")
    report["good_status"] = status
    report["good_meta"] = read_json(DATA_DIR / "quality" / "good_meta.json")
    europe, good = gh_runs("europe-check.yml"), gh_runs("build-good.yml")
    plan = decide(report, status, europe, good, now=now)
    publish(plan, report, europe + good)
    if plan["action"] in ("europe", "build") and args.execute:
        # Recheck just before dispatch to close the checkout/API polling race.
        current = decide(report, status, gh_runs("europe-check.yml"), gh_runs("build-good.yml"),
                         now=datetime.now(timezone.utc))
        if current["action"] != plan["action"]:
            publish(current, report, [])
            return int(current["action"] == "fail")
        workflow = "europe-check.yml" if plan["action"] == "europe" else "build-good.yml"
        command = ["gh", "workflow", "run", workflow, "--repo", REPO, "--ref", "main"]
        if plan["action"] == "europe":
            command += ["--field", "speed_target_samples=6"]
        subprocess.run(command, check=True, timeout=45)
    return int(plan["action"] == "fail")


if __name__ == "__main__":
    raise SystemExit(main())
