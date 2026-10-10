#!/usr/bin/env python3
"""Build GitHub-runner proxy lists for European-server use.

The public workflow runs on GitHub-hosted ``ubuntu-latest``.  It performs a
real TLS + HTTP GET through every candidate and keeps a short rolling history,
producing:

* ``data/valid/all_eu.txt`` — currently reachable from the GitHub runner and
  within the configured speed limit;
* ``data/valid/all_eu_stable.txt`` — currently qualified, qualified in at
  least ``--min-success-pct`` percent of the rolling samples, and qualified
  for at least ``--min-streak`` consecutive runs.

Standard GitHub-hosted runner geography is not guaranteed.  The measured
latency is retained for ranking, but is not used as a hard admission limit.

The default candidate pool is ``all_ltd_verified.txt``.  It is deliberately
small (fastest-per-country candidates that already passed the full-chain
validator), so GitHub Actions can recheck it frequently without probing the
entire public pool.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import re
import ssl
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    DATA_DIR,
    line_to_key,
    parse_line,
    read_json,
    rewrite_latency,
    write_json,
    write_text_if_changed,
)
from validate_proxies import SPEED_WARMUP_BYTES, speed_download  # noqa: E402

DEFAULT_SOURCE = DATA_DIR / "valid" / "all_ltd_verified.txt"
DEFAULT_HISTORY = DATA_DIR / "quality" / "europe.json"
DEFAULT_CURRENT = DATA_DIR / "valid" / "all_eu.txt"
DEFAULT_STABLE = DATA_DIR / "valid" / "all_eu_stable.txt"

DEFAULT_SNI = "cdnjs.cloudflare.com"
DEFAULT_PATH = "/ajax/libs/three.js/r128/three.js"
DEFAULT_TIMEOUT = 6.0
DEFAULT_WORKERS = 120
DEFAULT_HISTORY_WINDOW = 12
DEFAULT_HISTORY_MAX_GAP_HOURS = 12
DEFAULT_MIN_SAMPLES = 3
DEFAULT_MIN_SUCCESS_PCT = 80
DEFAULT_MIN_STREAK = 2
DEFAULT_MIN_SPEED_MBPS = 5.0
DEFAULT_QUALITY_MIN_SAMPLES = 6
DEFAULT_QUALITY_MIN_SUCCESS_PCT = 90
DEFAULT_QUALITY_MAX_SPEED_SPREAD = 0.5
DEFAULT_FAST_MEDIAN_SPEED_MBPS = 10.0
DEFAULT_SPEED_HOST = "speed.cloudflare.com"
DEFAULT_SPEED_PATH = "/__down?bytes=5000000"
DEFAULT_SPEED_BYTES = 5 * 1024 * 1024
DEFAULT_SPEED_TIMEOUT = 12

SPEED_TOKEN_RE = re.compile(r"-(\d+(?:\.\d+)?)MB/s(?:-|$)")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_endpoint(line: str) -> tuple[str, str, int] | None:
    parsed = parse_line(line)
    if not parsed:
        return None
    key, ip, port, _cc, _note = parsed
    try:
        return key, ip, int(port)
    except (TypeError, ValueError):
        return None


def source_speed(line: str) -> float:
    """Return the existing GitHub validator download speed in MB/s."""
    match = SPEED_TOKEN_RE.search(line)
    return float(match.group(1)) if match else 0.0


def classify_results(
    results: dict[str, dict], *, min_speed_mbps: float
) -> None:
    """Mark each result as reachable and sufficiently fast on this runner."""
    for result in results.values():
        reachable = bool(result.get("reachable", result.get("ok")))
        speed = result.get("speed_mbps")
        speed = float(speed) if isinstance(speed, (int, float)) else 0.0
        qualified = (
            bool(result.get("ok"))
            and (min_speed_mbps <= 0 or speed >= min_speed_mbps)
        )
        result["reachable"] = reachable
        result["qualified"] = qualified
        result["speed_mbps"] = speed or None
        result["source_speed_mbps"] = source_speed(result.get("line") or "")
        if reachable and not qualified:
            result["reject_reason"] = (
                "speed" if result.get("ok") else result.get("error") or "probe"
            )


def tls_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def probe_once(
    ip: str,
    port: int,
    *,
    sni: str,
    path: str,
    timeout: float,
    ctx: ssl.SSLContext,
) -> tuple[bool, float | None, float | None, str | None]:
    """Require a usable HTTP response and measure a sustained download sample."""
    writer: asyncio.StreamWriter | None = None
    started = time.monotonic()
    ms: float | None = None
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(
                ip,
                port,
                ssl=ctx,
                server_hostname=sni,
                limit=64 * 1024,
            )
            ms = round((time.monotonic() - started) * 1000, 1)
            request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {sni}\r\n"
                "User-Agent: CF-Proxy-Europe-Check/1.0\r\n"
                "Accept: */*\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            writer.write(request)
            await writer.drain()
            head = await reader.readuntil(b"\r\n\r\n")
            status_line = head.split(b"\r\n", 1)[0].decode(
                "ascii", errors="replace"
            )
            parts = status_line.split()
            status = int(parts[1]) if len(parts) >= 2 else 0
            if not 200 <= status < 400:
                return False, ms, None, f"http_{status or 'invalid'}"
            body = await reader.read(1)
            if not body:
                return False, ms, None, "empty_body"
    except Exception as exc:  # network failures are expected probe results
        return False, ms, None, type(exc).__name__
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass

    speed = await measure_speed(
        ip, port, host=DEFAULT_SPEED_HOST, path=DEFAULT_SPEED_PATH,
        timeout=DEFAULT_SPEED_TIMEOUT, cap_bytes=DEFAULT_SPEED_BYTES, ctx=ctx,
    )
    if speed is None:
        return False, ms, None, "speed_failed"
    return True, ms, speed, None


async def measure_speed(
    ip: str,
    port: int,
    *,
    host: str,
    path: str,
    timeout: float,
    cap_bytes: int,
    ctx: ssl.SSLContext,
) -> float | None:
    """Measure one fresh-connection download, excluding TCP slow-start bytes."""
    writer: asyncio.StreamWriter | None = None
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(
                ip, port, ssl=ctx, server_hostname=host, limit=64 * 1024
            )
            return await speed_download(
                reader, writer, host, path, cap_bytes, timeout,
                warmup_bytes=min(SPEED_WARMUP_BYTES, cap_bytes // 5),
            )
    except Exception:
        return None
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass


async def probe_all(
    lines: list[str],
    *,
    workers: int,
    timeout: float,
    retries: int,
    sni: str,
    path: str,
) -> dict[str, dict]:
    sem = asyncio.Semaphore(max(1, workers))
    ctx = tls_context()
    results: dict[str, dict] = {}

    async def work(line: str) -> None:
        endpoint = parse_endpoint(line)
        if not endpoint:
            return
        key, ip, port = endpoint
        ok, ms, speed, error = False, None, None, "not_run"
        for attempt in range(max(0, retries) + 1):
            async with sem:
                ok, ms, speed, error = await probe_once(
                    ip,
                    port,
                    sni=sni,
                    path=path,
                    timeout=timeout,
                    ctx=ctx,
                )
            if ok:
                break
            if attempt < retries:
                await asyncio.sleep(0.15 * (attempt + 1))
        results[key] = {
            "ok": ok,
            "reachable": ms is not None,
            "ms": ms,
            "speed_mbps": speed,
            "error": error,
            "line": line,
        }

    tasks = [asyncio.create_task(work(line)) for line in lines]
    total = len(tasks)
    for done, task in enumerate(asyncio.as_completed(tasks), 1):
        await task
        if done % 50 == 0 or done == total:
            print(f"Europe probe: {done}/{total}")
    return results


def update_history(
    previous: dict,
    results: dict[str, dict],
    *,
    window: int,
    now: str,
) -> dict:
    old = previous.get("proxies", {}) if isinstance(previous, dict) else {}
    previous_ts = previous.get("ts") if isinstance(previous, dict) else None
    try:
        previous_at = datetime.fromisoformat(previous_ts.replace("Z", "+00:00"))
        current_at = datetime.fromisoformat(now.replace("Z", "+00:00"))
        if previous_at.tzinfo is None:
            previous_at = previous_at.replace(tzinfo=timezone.utc)
        if current_at.tzinfo is None:
            current_at = current_at.replace(tzinfo=timezone.utc)
        gap = current_at - previous_at
        if gap < timedelta(0) or gap > timedelta(hours=DEFAULT_HISTORY_MAX_GAP_HOURS):
            old = {}
    except (AttributeError, TypeError, ValueError):
        old = {}
    if not isinstance(old, dict):
        old = {}
    proxies: dict[str, dict] = {}
    for key, result in results.items():
        prior = old.get(key, {}) if isinstance(old.get(key), dict) else {}
        keep = max(0, window - 1)
        prior_samples = prior.get("samples")
        prior_samples = prior_samples if isinstance(prior_samples, list) else []
        prior_availability = prior.get("availability_samples")
        prior_availability = (
            prior_availability if isinstance(prior_availability, list) else []
        )
        prior_speeds = prior.get("speed_samples")
        prior_speeds = prior_speeds if isinstance(prior_speeds, list) else []
        prior_latencies = prior.get("latency_samples")
        prior_latencies = prior_latencies if isinstance(prior_latencies, list) else []
        samples = [int(bool(x)) for x in prior_samples][-keep:] if keep else []
        availability_samples = [
            int(bool(x)) for x in prior_availability
        ][-keep:] if keep else []
        speed_samples = [
            float(x) if isinstance(x, (int, float)) and x > 0 else None
            for x in prior_speeds
        ][-keep:] if keep else []
        latency_samples = [
            float(x) if isinstance(x, (int, float)) and x > 0 else None
            for x in prior_latencies
        ][-keep:] if keep else []
        ok = bool(result.get("qualified", result.get("ok")))
        reachable = bool(result.get("reachable", result.get("ok")))
        samples.append(int(ok))
        availability_samples.append(int(reachable))
        speed = result.get("speed_mbps")
        speed_samples.append(
            float(speed) if isinstance(speed, (int, float)) and speed > 0 else None
        )
        latency = result.get("ms")
        latency_samples.append(
            float(latency) if isinstance(latency, (int, float)) and latency > 0 else None
        )
        streak = int(prior.get("streak") or 0) + 1 if ok else 0
        fail_streak = int(prior.get("fail_streak") or 0) + 1 if not ok else 0
        pct = round(sum(samples) * 100 / len(samples)) if samples else 0
        reachable_pct = (
            round(sum(availability_samples) * 100 / len(availability_samples))
            if availability_samples else 0
        )
        valid_speeds = [x for x in speed_samples if x is not None]
        q25 = percentile(valid_speeds, 0.25)
        q75 = percentile(valid_speeds, 0.75)
        median_speed = statistics.median(valid_speeds) if valid_speeds else None
        spread = (
            round((q75 - q25) / median_speed, 3)
            if median_speed and q25 is not None and q75 is not None
            else None
        )
        entry = {
            "samples": samples,
            "availability_samples": availability_samples,
            "speed_samples": speed_samples,
            "latency_samples": latency_samples,
            "sample_count": len(samples),
            "speed_sample_count": len(valid_speeds),
            "success_pct": pct,
            "reachable_pct": reachable_pct,
            "median_speed_mbps": round(median_speed, 2) if median_speed is not None else None,
            "p25_speed_mbps": round(q25, 2) if q25 is not None else None,
            "p75_speed_mbps": round(q75, 2) if q75 is not None else None,
            "speed_spread_pct": spread,
            "streak": streak,
            "fail_streak": fail_streak,
            "last_check": now,
            "last_ok": now if ok else prior.get("last_ok"),
            "ms": result.get("ms") if ok else prior.get("ms"),
            "error": None if ok else result.get("error"),
        }
        proxies[key] = entry
    return {
        "ts": now,
        "vantage": "github-actions/ubuntu-latest",
        "window": window,
        "proxies": proxies,
    }


def percentile(values: list[float], fraction: float) -> float | None:
    """Linearly interpolated percentile for small rolling samples."""
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * fraction
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (pos - lower)


def select_lines(
    results: dict[str, dict],
    history: dict,
    *,
    stable: bool,
    min_samples: int,
    min_success_pct: int,
    min_streak: int,
    limit: int,
    profile: str | None = None,
    quality_min_samples: int = DEFAULT_QUALITY_MIN_SAMPLES,
    quality_min_success_pct: int = DEFAULT_QUALITY_MIN_SUCCESS_PCT,
    quality_max_speed_spread: float = DEFAULT_QUALITY_MAX_SPEED_SPREAD,
    quality_min_speed_mbps: float = DEFAULT_MIN_SPEED_MBPS,
    fast_median_speed_mbps: float = DEFAULT_FAST_MEDIAN_SPEED_MBPS,
) -> list[str]:
    states = history.get("proxies", {}) if isinstance(history, dict) else {}
    if not isinstance(states, dict):
        states = {}
    selected: list[tuple[tuple, str]] = []
    for key, result in results.items():
        if not result.get("qualified", result.get("ok")):
            continue
        state = states.get(key, {})
        samples = state.get("samples", [])
        if profile in ("quality", "fast") and not (
            len(samples) >= quality_min_samples
            and int(state.get("speed_sample_count") or 0) >= quality_min_samples
            and int(state.get("success_pct") or 0) >= quality_min_success_pct
            and int(state.get("streak") or 0) >= min_streak
            and float(state.get("median_speed_mbps") or 0) >= quality_min_speed_mbps
            and state.get("speed_spread_pct") is not None
            and float(state["speed_spread_pct"]) <= quality_max_speed_spread
        ):
            continue
        if profile == "fast" and float(state.get("median_speed_mbps") or 0) < fast_median_speed_mbps:
            continue
        if stable and not (
            len(samples) >= min_samples
            and int(state.get("success_pct") or 0) >= min_success_pct
            and int(state.get("streak") or 0) >= min_streak
        ):
            continue
        speed = (
            state.get("median_speed_mbps")
            if profile in ("quality", "fast")
            else result.get("speed_mbps")
        )
        line = rewrite_latency(result["line"], result.get("ms"))
        line = rewrite_speed(line, speed)
        if profile in ("quality", "fast"):
            rank = (
                -int(state.get("success_pct") or 0),
                -float(state.get("median_speed_mbps") or 0),
                float(state.get("speed_spread_pct") or 0),
                float(result.get("ms") or 1_000_000),
                key,
            )
        else:
            rank = (
                -int(state.get("success_pct") or 0),
                -int(state.get("streak") or 0),
                float(result.get("ms") or 1_000_000),
                -float(result.get("speed_mbps") or source_speed(line)),
                key,
            )
        selected.append((rank, line))
    selected.sort(key=lambda item: item[0])
    lines = [line for _rank, line in selected]
    return lines[:limit] if limit > 0 else lines


def rewrite_speed(line: str, speed: float | int | None) -> str:
    """Replace the inline speed with the current or rolling measured sample."""
    if not isinstance(speed, (int, float)) or speed <= 0:
        return line
    return SPEED_TOKEN_RE.sub(f"-{speed:.2f}MB/s", line, count=1)


def write_list(path: Path, lines: list[str]) -> None:
    if lines:
        write_text_if_changed(path, "\n".join(lines) + "\n")


def write_quality_list(path: Path, lines: list[str], *, history_ready: bool) -> None:
    """Keep warm-up outputs, but remove stale quality lists once history is mature."""
    if lines:
        write_text_if_changed(path, "\n".join(lines) + "\n")
    elif history_ready and path.exists():
        path.unlink()


async def run(args: argparse.Namespace) -> int:
    if not args.source.exists():
        print(f"source not found: {args.source}", file=sys.stderr)
        return 1
    lines = [
        line for line in args.source.read_text(encoding="utf-8").splitlines()
        if line.strip() and line_to_key(line)
    ]
    if args.limit_candidates > 0:
        lines = lines[: args.limit_candidates]
    if not lines:
        print("source contains no parseable candidates", file=sys.stderr)
        return 1

    results = await probe_all(
        lines,
        workers=args.workers,
        timeout=args.timeout,
        retries=args.retries,
        sni=args.sni,
        path=args.path,
    )
    ok_count = sum(bool(v.get("ok")) for v in results.values())
    ratio = ok_count / max(1, len(results))
    reachable_count = sum(bool(v.get("reachable")) for v in results.values())
    print(
        f"Europe probe result: reachable={reachable_count}/{len(results)} "
        f"qualified={ok_count}/{len(results)} ({ratio:.1%})"
    )
    # A DNS/routing/SNI outage must not poison the rolling history or wipe a
    # previously useful subscription.
    if len(results) >= 20 and ratio < args.min_run_success_pct / 100:
        print(
            "aborting: run-wide success ratio is below the safety floor; "
            "history and outputs were left untouched",
            file=sys.stderr,
        )
        return 2

    classify_results(
        results,
        min_speed_mbps=args.min_speed_mbps,
    )
    qualified_count = sum(bool(v.get("qualified")) for v in results.values())
    print(
        f"GitHub candidate filter: qualified={qualified_count}/{len(results)} "
        f"(speed>={args.min_speed_mbps:g}MB/s; latency is ranking-only)"
    )

    now = utc_now()
    history = update_history(
        read_json(args.history),
        results,
        window=args.history_window,
        now=now,
    )
    current = select_lines(
        results,
        history,
        stable=False,
        min_samples=args.min_samples,
        min_success_pct=args.min_success_pct,
        min_streak=args.min_streak,
        limit=args.output_limit,
    )
    stable = select_lines(
        results,
        history,
        stable=True,
        min_samples=args.min_samples,
        min_success_pct=args.min_success_pct,
        min_streak=args.min_streak,
        limit=args.output_limit,
    )
    quality = select_lines(
        results,
        history,
        stable=False,
        min_samples=args.min_samples,
        min_success_pct=args.min_success_pct,
        min_streak=args.min_streak,
        limit=args.output_limit,
        profile="quality",
        quality_min_samples=args.quality_min_samples,
        quality_min_success_pct=args.quality_min_success_pct,
        quality_max_speed_spread=args.quality_max_speed_spread,
        quality_min_speed_mbps=args.min_speed_mbps,
        fast_median_speed_mbps=args.fast_median_speed_mbps,
    )
    fast = select_lines(
        results,
        history,
        stable=False,
        min_samples=args.min_samples,
        min_success_pct=args.min_success_pct,
        min_streak=args.min_streak,
        limit=args.output_limit,
        profile="fast",
        quality_min_samples=args.quality_min_samples,
        quality_min_success_pct=args.quality_min_success_pct,
        quality_max_speed_spread=args.quality_max_speed_spread,
        quality_min_speed_mbps=args.min_speed_mbps,
        fast_median_speed_mbps=args.fast_median_speed_mbps,
    )
    write_json(args.history, history)
    write_list(args.current_out, current)
    # During the first few runs there are intentionally not enough samples.
    # Do not delete an existing stable list merely because history was reset.
    if stable or not args.stable_out.exists():
        write_list(args.stable_out, stable)
    quality_history_ready = any(
        len(state.get("samples", [])) >= args.quality_min_samples
        and int(state.get("speed_sample_count") or 0) >= args.quality_min_samples
        for state in history.get("proxies", {}).values()
        if isinstance(state, dict)
    )
    if quality or quality_history_ready or not args.quality_out.exists():
        write_quality_list(
            args.quality_out, quality, history_ready=quality_history_ready
        )
    if fast or quality_history_ready or not args.fast_out.exists():
        write_quality_list(args.fast_out, fast, history_ready=quality_history_ready)
    print(
        f"Wrote Europe lists: current={len(current)} stable={len(stable)} "
        f"quality={len(quality)} fast={len(fast)} "
        f"history={args.history}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    ap.add_argument("--current-out", type=Path, default=DEFAULT_CURRENT)
    ap.add_argument("--stable-out", type=Path, default=DEFAULT_STABLE)
    ap.add_argument(
        "--quality-out", type=Path,
        default=DATA_DIR / "valid" / "all_eu_quality.txt",
    )
    ap.add_argument(
        "--fast-out", type=Path,
        default=DATA_DIR / "valid" / "all_eu_fast.txt",
    )
    ap.add_argument("--sni", default=DEFAULT_SNI)
    ap.add_argument("--path", default=DEFAULT_PATH)
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--history-window", type=int, default=DEFAULT_HISTORY_WINDOW)
    ap.add_argument("--min-samples", type=int, default=DEFAULT_MIN_SAMPLES)
    ap.add_argument(
        "--min-success-pct", type=int, default=DEFAULT_MIN_SUCCESS_PCT
    )
    ap.add_argument("--min-streak", type=int, default=DEFAULT_MIN_STREAK)
    ap.add_argument(
        "--quality-min-samples", type=int,
        default=DEFAULT_QUALITY_MIN_SAMPLES,
    )
    ap.add_argument(
        "--quality-min-success-pct", type=int,
        default=DEFAULT_QUALITY_MIN_SUCCESS_PCT,
    )
    ap.add_argument(
        "--quality-max-speed-spread", type=float,
        default=DEFAULT_QUALITY_MAX_SPEED_SPREAD,
    )
    ap.add_argument(
        "--fast-median-speed-mbps", type=float,
        default=DEFAULT_FAST_MEDIAN_SPEED_MBPS,
    )
    ap.add_argument(
        "--min-speed-mbps",
        type=float,
        default=DEFAULT_MIN_SPEED_MBPS,
        help="Minimum current GitHub-runner download speed in MB/s "
             "(0 = unlimited; default: %(default)s)",
    )
    ap.add_argument(
        "--min-run-success-pct",
        type=int,
        default=5,
        help="Abort a suspicious whole-run collapse below this percentage",
    )
    ap.add_argument(
        "--limit-candidates",
        type=int,
        default=0,
        help="Probe at most this many input lines (0 = all)",
    )
    ap.add_argument(
        "--output-limit",
        type=int,
        default=500,
        help="Maximum lines in each Europe subscription (0 = unlimited)",
    )
    args = ap.parse_args(argv)
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
