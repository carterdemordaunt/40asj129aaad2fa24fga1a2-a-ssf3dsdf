#!/usr/bin/env python3
"""Build GitHub-runner proxy lists for European-server use.

The public workflow runs on GitHub-hosted ``ubuntu-latest``.  It performs a
real TLS + HTTP GET through every candidate and keeps a short rolling history,
producing:

* ``data/valid/all_eu.txt`` — HTTP-available now, with a speed measurement
  within 36 hours meeting the configured minimum;
* ``data/valid/all_eu_stable.txt`` — currently qualified, qualified in at
  least ``--min-success-pct`` percent of the rolling samples, and qualified
  for at least ``--min-streak`` consecutive runs.

Standard GitHub-hosted runner geography is not guaranteed.  The measured
latency is retained for ranking, but is not used as a hard admission limit.

Every run checks HTTP availability for the entire ``all.txt`` pool. Sustained
downloads use a separate, oldest-first budget so coverage is not restricted to
the fastest-per-country pool. Unscheduled downloads are not failed samples.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
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
from europe_health import (  # noqa: E402
    PROBE_MAX_AGE_HOURS,
    SPEED_MAX_AGE_HOURS,
    SAMPLE_MAX_AGE_DAYS,
    is_fresh,
    parse_timestamp,
)

DEFAULT_SOURCE = DATA_DIR / "valid" / "all.txt"
DEFAULT_HISTORY = DATA_DIR / "quality" / "europe.json"
DEFAULT_CURRENT = DATA_DIR / "valid" / "all_eu.txt"
DEFAULT_STABLE = DATA_DIR / "valid" / "all_eu_stable.txt"

DEFAULT_SNI = "cdnjs.cloudflare.com"
DEFAULT_PATH = "/ajax/libs/three.js/r128/three.js"
DEFAULT_TIMEOUT = 3.0
DEFAULT_WORKERS = 400
DEFAULT_HISTORY_WINDOW = 12
DEFAULT_SPEED_BUDGET = 5400
DEFAULT_SPEED_WORKERS = 20
DEFAULT_SPEED_INTERVAL_HOURS = 2
DEFAULT_MIN_SAMPLES = 3
DEFAULT_MIN_SUCCESS_PCT = 80
DEFAULT_MIN_STREAK = 2
DEFAULT_MIN_SPEED_MBPS = 2.0
DEFAULT_QUALITY_MIN_SAMPLES = 6
DEFAULT_QUALITY_MIN_SUCCESS_PCT = 90
DEFAULT_QUALITY_MAX_SPEED_SPREAD = 0.5
DEFAULT_FAST_MEDIAN_SPEED_MBPS = 10.0
DEFAULT_SPEED_HOST = "speed.cloudflare.com"
DEFAULT_SPEED_PATH = "/__down?bytes=5000000"
DEFAULT_SPEED_BYTES = 5 * 1024 * 1024
DEFAULT_SPEED_TIMEOUT = 12
DEFAULT_LONG_BUDGET = 200
DEFAULT_LONG_WORKERS = 50
DEFAULT_LONG_DURATION = 60.0
DEFAULT_LONG_INTERVAL = 1.0
DEFAULT_LONG_TIMEOUT = 8.0
DEFAULT_LONG_PATH = "/__down?bytes=1"
DEFAULT_LONG_MIN_SPEED_MBPS = 3.0
LONG_HISTORY_WINDOW = 6

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
    results: dict[str, dict], *, min_speed_mbps: float,
    history: dict | None = None, now: datetime | None = None,
) -> None:
    """Require current HTTP availability plus a fresh, measured speed."""
    now = now or datetime.now(timezone.utc)
    for key, result in results.items():
        reachable = bool(result.get("available", result.get("ok")))
        state = (history or {}).get("proxies", {}).get(key, {})
        speed = state.get("last_speed_mbps") if history is not None else result.get("speed_mbps")
        speed = float(speed) if isinstance(speed, (int, float)) else 0.0
        qualified = (
            reachable
            and (min_speed_mbps <= 0 or (
                speed >= min_speed_mbps and (
                    history is None or is_fresh(
                        state.get("last_speed_at"), SPEED_MAX_AGE_HOURS, now=now
                    )
                )
            ))
        )
        result["reachable"] = reachable
        result["qualified"] = qualified
        result["effective_speed_mbps"] = speed or None
        result["source_speed_mbps"] = source_speed(result.get("line") or "")
        if reachable and not qualified:
            result["reject_reason"] = "speed"


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
    """Check TLS, HTTP status and first byte without a sustained download."""
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
                ssl_shutdown_timeout=1,
            )
            ms = round((time.monotonic() - started) * 1000, 1)
            request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {sni}\r\n"
                "User-Agent: CF-Proxy-Europe-Check/1.0\r\n"
                "Accept: */*\r\n"
                "Range: bytes=0-0\r\n"
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
            ms = round((time.monotonic() - started) * 1000, 1)
            return True, ms, None, None
    except Exception as exc:  # network failures are expected probe results
        return False, ms, None, type(exc).__name__
    finally:
        if writer is not None:
            try:
                writer.close()
                async with asyncio.timeout(1):
                    await writer.wait_closed()
            except (OSError, TimeoutError):
                pass


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
                ip, port, ssl=ctx, server_hostname=host, limit=64 * 1024,
                ssl_shutdown_timeout=1,
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
                async with asyncio.timeout(1):
                    await writer.wait_closed()
            except (OSError, TimeoutError):
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
            "available": ok,
            "reachable": ok,
            "speed_tested": False,
            "probed_at": utc_now(),
            "ms": ms,
            "speed_mbps": speed,
            "error": error,
            "line": line,
        }

    tasks = [asyncio.create_task(work(line)) for line in lines]
    total = len(tasks)
    for done, task in enumerate(asyncio.as_completed(tasks), 1):
        await task
        if done % 500 == 0 or done == total:
            print(f"Europe probe: {done}/{total}")
    return results


def select_speed_keys(
    results: dict[str, dict], previous: dict, *, budget: int,
    now: datetime, interval_hours: float = DEFAULT_SPEED_INTERVAL_HOURS,
    target_samples: int = 0,
) -> list[str]:
    """Oldest first; hashed ties avoid bias towards source order/country."""
    old = previous.get("proxies", {})
    old = old if isinstance(old, dict) else {}
    due = []
    for key, result in results.items():
        if not result.get("available", result.get("ok")):
            continue
        state = old.get(key)
        state = state if isinstance(state, dict) else {}
        if target_samples:
            speeds = state.get("speed_samples") or []
            stamps = state.get("speed_sample_times") or [state.get("last_speed_at")] * len(speeds)
            valid_count = sum(
                isinstance(speed, (int, float)) and math.isfinite(speed) and speed > 0
                and is_fresh(stamp, SAMPLE_MAX_AGE_DAYS * 24, now=now)
                for speed, stamp in zip(speeds, stamps)
            )
            if valid_count >= target_samples:
                continue
        stamp = state.get("last_speed_at", state.get("last_check", previous.get("ts")))
        if not state.get("speed_samples"):
            stamp = None
        checked = parse_timestamp(stamp)
        if checked is not None and timedelta(0) <= now - checked < timedelta(hours=interval_hours):
            continue
        age_order = checked.timestamp() if checked is not None and checked <= now else float("-inf")
        due.append((age_order, hashlib.sha256(key.encode("utf-8")).digest(), key))
    due.sort()
    return [key for _age, _tie, key in due[:max(0, budget)]]


async def probe_speeds(results: dict[str, dict], keys: list[str], *, workers: int) -> None:
    sem = asyncio.Semaphore(max(1, workers))
    ctx = tls_context()

    async def work(key: str) -> None:
        result = results[key]
        _key, ip, port = parse_endpoint(result["line"])
        async with sem:
            speed = await measure_speed(
                ip, port, host=DEFAULT_SPEED_HOST, path=DEFAULT_SPEED_PATH,
                timeout=DEFAULT_SPEED_TIMEOUT, cap_bytes=DEFAULT_SPEED_BYTES, ctx=ctx,
            )
        result["speed_tested"] = True
        result["speed_checked_at"] = utc_now()
        result["speed_mbps"] = speed
        result["speed_error"] = None if speed is not None else "speed_failed"

    tasks = [asyncio.create_task(work(key)) for key in keys]
    for done, task in enumerate(asyncio.as_completed(tasks), 1):
        await task
        if done % 200 == 0 or done == len(tasks):
            print(f"Europe speed: {done}/{len(tasks)}")


async def measure_long_connection(
    ip: str,
    port: int,
    *,
    host: str,
    path: str,
    duration: float,
    interval: float,
    timeout: float,
    ctx: ssl.SSLContext,
) -> tuple[bool, int, float, str | None]:
    """Exercise one TLS connection with periodic HTTP exchanges for duration."""
    writer: asyncio.StreamWriter | None = None
    started = time.monotonic()
    connected_at: float | None = None
    requests = 0
    error = None
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(
                ip, port, ssl=ctx, server_hostname=host, limit=64 * 1024,
                ssl_shutdown_timeout=1,
            )
        connected_at = time.monotonic()
        deadline = connected_at + duration
        next_request = connected_at
        while time.monotonic() < deadline:
            async with asyncio.timeout(timeout):
                writer.write((
                    f"GET {path} HTTP/1.1\r\n"
                    f"Host: {host}\r\n"
                    "User-Agent: CF-Proxy-Europe-Long-Check/1.0\r\n"
                    "Accept: */*\r\n"
                    "Connection: keep-alive\r\n\r\n"
                ).encode("ascii"))
                await writer.drain()
                head = await reader.readuntil(b"\r\n\r\n")
                rows = head.decode("iso-8859-1").split("\r\n")
                status = rows[0].split()
                if len(status) < 2 or status[1] != "200":
                    error = f"http_{status[1] if len(status) > 1 else 'invalid'}"
                    break
                headers = {}
                for row in rows[1:]:
                    if ":" in row:
                        name, value = row.split(":", 1)
                        headers[name.strip().lower()] = value.strip().lower()
                if headers.get("connection") == "close":
                    error = "keepalive_closed"
                    break
                try:
                    body_size = int(headers.get("content-length", ""))
                except ValueError:
                    error = "missing_content_length"
                    break
                if body_size != 1:
                    error = "unexpected_content_length"
                    break
                body = await reader.readexactly(body_size)
                if len(body) != 1:
                    error = "incomplete_body"
                    break
                requests += 1
            next_request += interval
            delay = next_request - time.monotonic()
            if delay > 0:
                await asyncio.sleep(min(delay, max(0.0, deadline - time.monotonic())))
    except Exception as exc:
        error = type(exc).__name__
    finally:
        elapsed = time.monotonic() - (connected_at if connected_at is not None else started)
        if writer is not None:
            try:
                writer.close()
                async with asyncio.timeout(1):
                    await writer.wait_closed()
            except (OSError, TimeoutError):
                pass
    expected_requests = max(2, int(duration / interval * 0.8))
    if error is None and requests < expected_requests:
        error = "long_stream_too_sparse"
    return error is None and elapsed >= duration, requests, round(elapsed, 2), error


def select_long_keys(
    results: dict[str, dict], history: dict, *, budget: int, now: datetime,
    min_speed_mbps: float = DEFAULT_LONG_MIN_SPEED_MBPS,
    interval_hours: float = 2.0,
) -> list[str]:
    """Rotate long-connection tests across otherwise-good speed candidates."""
    states = history.get("proxies", {}) if isinstance(history, dict) else {}
    if not isinstance(states, dict):
        return []
    due = []
    for key, result in results.items():
        if not result.get("available", result.get("ok")):
            continue
        state = states.get(key)
        if not isinstance(state, dict):
            continue
        samples = state.get("samples")
        speed_values = state.get("speed_samples")
        speed_times = state.get("speed_sample_times")
        if not isinstance(speed_values, list):
            speed_values = []
        if not isinstance(speed_times, list) or len(speed_times) != len(speed_values):
            speed_times = [state.get("last_speed_at")] * len(speed_values)
        strict_speed_samples = [
            int(
                isinstance(speed, (int, float))
                and math.isfinite(speed)
                and speed >= min_speed_mbps
            )
            for speed, stamp in zip(speed_values, speed_times)
            if is_fresh(stamp, SAMPLE_MAX_AGE_DAYS * 24, now=now)
        ]
        try:
            eligible = (
                isinstance(samples, list)
                and len(samples) >= DEFAULT_QUALITY_MIN_SAMPLES
                and int(state.get("speed_sample_count") or 0) >= DEFAULT_QUALITY_MIN_SAMPLES
                and float(state.get("success_pct") or 0) >= DEFAULT_QUALITY_MIN_SUCCESS_PCT
                and int(state.get("streak") or 0) >= DEFAULT_MIN_STREAK
                and len(strict_speed_samples) >= DEFAULT_QUALITY_MIN_SAMPLES
                and sum(strict_speed_samples) * 100 / len(strict_speed_samples)
                >= DEFAULT_QUALITY_MIN_SUCCESS_PCT
                and trailing_count(strict_speed_samples, 1) >= DEFAULT_MIN_STREAK
                and float(state.get("median_speed_mbps") or 0) >= min_speed_mbps
                and float(state["speed_spread_pct"]) <= DEFAULT_QUALITY_MAX_SPEED_SPREAD
                and bool(state.get("last_available"))
                and float(state.get("reachable_pct") or 0) >= DEFAULT_QUALITY_MIN_SUCCESS_PCT
                and int(state.get("availability_streak") or 0) >= DEFAULT_MIN_STREAK
                and is_fresh(state.get("last_probe_at"), PROBE_MAX_AGE_HOURS, now=now)
                and is_fresh(state.get("last_speed_at"), SPEED_MAX_AGE_HOURS, now=now)
            )
        except (KeyError, TypeError, ValueError):
            eligible = False
        if not eligible:
            continue
        checked = parse_timestamp(state.get("last_long_at"))
        if checked is not None and timedelta(0) <= now - checked < timedelta(hours=interval_hours):
            continue
        age_order = checked.timestamp() if checked is not None and checked <= now else float("-inf")
        due.append((age_order, hashlib.sha256(key.encode("utf-8")).digest(), key))
    due.sort()
    return [key for _age, _tie, key in due[:max(0, budget)]]


async def probe_long_connections(
    results: dict[str, dict], keys: list[str], *, workers: int,
    duration: float = DEFAULT_LONG_DURATION,
    interval: float = DEFAULT_LONG_INTERVAL,
    timeout: float = DEFAULT_LONG_TIMEOUT,
) -> None:
    sem = asyncio.Semaphore(max(1, workers))
    ctx = tls_context()

    async def work(key: str) -> None:
        result = results[key]
        _key, ip, port = parse_endpoint(result["line"])
        async with sem:
            ok, requests, elapsed, error = await measure_long_connection(
                ip, port, host=DEFAULT_SPEED_HOST, path=DEFAULT_LONG_PATH,
                duration=duration, interval=interval, timeout=timeout, ctx=ctx,
            )
        result["long_tested"] = True
        result["long_checked_at"] = utc_now()
        result["long_ok"] = ok
        result["long_requests"] = requests
        result["long_duration_seconds"] = elapsed
        result["long_error"] = error

    tasks = [asyncio.create_task(work(key)) for key in keys]
    for done, task in enumerate(asyncio.as_completed(tasks), 1):
        await task
        if done % 50 == 0 or done == len(tasks):
            print(f"Europe long connection: {done}/{len(tasks)}")


def update_history(
    previous: dict,
    results: dict[str, dict],
    *,
    window: int,
    now: str,
    min_speed_mbps: float = DEFAULT_MIN_SPEED_MBPS,
) -> dict:
    old = previous.get("proxies", {}) if isinstance(previous, dict) else {}
    if not isinstance(old, dict):
        old = {}
    current_at = parse_timestamp(now)
    if current_at is None:
        raise ValueError("invalid history timestamp")
    # Migrate old snapshots once, without refreshing the age of old speeds.
    proxies = {}
    for key, entry in old.items():
        if not isinstance(entry, dict):
            continue
        prior = dict(entry)
        prior.setdefault("last_probe_at", prior.get("last_check", previous.get("ts")))
        prior.setdefault("last_speed_at", prior.get("last_check", previous.get("ts")))
        prior.setdefault("last_speed_mbps", (prior.get("speed_samples") or [None])[-1])
        prior.setdefault("last_available", bool((
            prior.get("availability_samples") or prior.get("samples") or [0]
        )[-1]))
        if is_fresh(prior.get("last_probe_at"), SAMPLE_MAX_AGE_DAYS * 24, now=current_at):
            proxies[key] = prior
    for key, result in results.items():
        prior = proxies.get(key, {})
        window = max(1, window)
        availability_samples = list(prior.get("availability_samples") or [])
        latency_samples = list(prior.get("latency_samples") or [])
        if not is_fresh(prior.get("last_probe_at"), PROBE_MAX_AGE_HOURS, now=current_at):
            availability_samples = []
            latency_samples = []
        available = bool(result.get("available", result.get("reachable", result.get("ok"))))
        availability_samples = (availability_samples + [int(available)])[-window:]
        latency = result.get("ms")
        latency_samples = (latency_samples + [
            float(latency) if isinstance(latency, (int, float)) and latency > 0 else None
        ])[-window:]

        old_speeds = prior.get("speed_samples") or []
        old_times = prior.get("speed_sample_times")
        if not isinstance(old_times, list) or len(old_times) != len(old_speeds):
            old_times = [prior.get("last_speed_at")] * len(old_speeds)
        retained = [
            (float(speed) if isinstance(speed, (int, float)) and math.isfinite(speed) and speed > 0 else None, stamp)
            for speed, stamp in zip(old_speeds, old_times)
            if is_fresh(stamp, SAMPLE_MAX_AGE_DAYS * 24, now=current_at)
        ]
        speed_tested = result.get("speed_tested", True)
        speed_checked_at = result.get("speed_checked_at", now)
        if speed_tested:
            speed = result.get("speed_mbps")
            retained.append((
                float(speed) if isinstance(speed, (int, float)) and math.isfinite(speed) and speed > 0 else None,
                speed_checked_at,
            ))
        retained = retained[-window:]
        speed_samples = [speed for speed, _stamp in retained]
        speed_sample_times = [stamp for _speed, stamp in retained]
        # Only actual speed attempts enter this denominator; a skipped speed
        # measurement never becomes a failure or increases the success streak.
        samples = [int(speed is not None and speed >= min_speed_mbps) for speed in speed_samples]
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
            "speed_sample_times": speed_sample_times,
            "latency_samples": latency_samples,
            "sample_count": len(samples),
            "speed_sample_count": len(valid_speeds),
            "success_pct": pct,
            "reachable_pct": reachable_pct,
            "median_speed_mbps": round(median_speed, 2) if median_speed is not None else None,
            "p25_speed_mbps": round(q25, 2) if q25 is not None else None,
            "p75_speed_mbps": round(q75, 2) if q75 is not None else None,
            "speed_spread_pct": spread,
            "streak": trailing_count(samples, 1),
            "fail_streak": trailing_count(samples, 0),
            "availability_streak": trailing_count(availability_samples, 1),
            "last_available": available,
            "last_probe_at": result.get("probed_at", now),
            "last_speed_at": speed_checked_at if speed_tested else prior.get("last_speed_at"),
            "last_speed_mbps": speed_samples[-1] if speed_samples else None,
            "last_check": result.get("probed_at", now),
            "last_ok": speed_checked_at if speed_tested and samples[-1] else prior.get("last_ok"),
            "ms": result.get("ms") if available else prior.get("ms"),
            "error": None if available else result.get("error"),
            "speed_error": result.get("speed_error") if speed_tested else prior.get("speed_error"),
            "long_samples": list(prior.get("long_samples") or []),
            "long_sample_times": list(prior.get("long_sample_times") or []),
            "long_streak": int(prior.get("long_streak") or 0),
            "last_long_at": prior.get("last_long_at"),
            "last_long_ok": prior.get("last_long_ok"),
            "last_long_requests": prior.get("last_long_requests"),
            "last_long_duration_seconds": prior.get("last_long_duration_seconds"),
            "long_error": prior.get("long_error"),
        }
        proxies[key] = entry
    return {
        "schema_version": 2,
        "ts": now,
        "vantage": "github-actions/ubuntu-latest",
        "window": window,
        "proxies": proxies,
    }


def update_long_history(
    history: dict, results: dict[str, dict], *, now: str,
    window: int = LONG_HISTORY_WINDOW,
) -> None:
    """Append only scheduled long tests; untested nodes keep their history."""
    current_at = parse_timestamp(now)
    if current_at is None:
        raise ValueError("invalid long-history timestamp")
    states = history.get("proxies", {})
    for key, state in states.items():
        if not isinstance(state, dict):
            continue
        samples = list(state.get("long_samples") or [])
        stamps = state.get("long_sample_times")
        if not isinstance(stamps, list) or len(stamps) != len(samples):
            stamps = [state.get("last_long_at")] * len(samples)
        retained = [
            (int(sample), stamp)
            for sample, stamp in zip(samples, stamps)
            if sample in (0, 1)
            and is_fresh(stamp, SAMPLE_MAX_AGE_DAYS * 24, now=current_at)
        ]
        result = results.get(key, {})
        if result.get("long_tested"):
            retained.append((int(bool(result.get("long_ok"))), result.get("long_checked_at", now)))
        retained = retained[-max(1, window):]
        state["long_samples"] = [sample for sample, _stamp in retained]
        state["long_sample_times"] = [stamp for _sample, stamp in retained]
        state["long_streak"] = trailing_count(state["long_samples"], 1)
        if result.get("long_tested"):
            state["last_long_at"] = result.get("long_checked_at", now)
            state["last_long_ok"] = bool(result.get("long_ok"))
            state["last_long_requests"] = result.get("long_requests")
            state["last_long_duration_seconds"] = result.get("long_duration_seconds")
            state["long_error"] = result.get("long_error")
        elif retained:
            state["last_long_at"] = retained[-1][1]
            state["last_long_ok"] = bool(retained[-1][0])
            state["long_error"] = state.get("long_error")


def trailing_count(samples: list, value: int) -> int:
    count = 0
    for sample in reversed(samples):
        if sample != value:
            break
        count += 1
    return count


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
            and int(state.get("reachable_pct") or 0) >= quality_min_success_pct
            and int(state.get("availability_streak") or 0) >= min_streak
        ):
            continue
        if profile == "fast" and float(state.get("median_speed_mbps") or 0) < fast_median_speed_mbps:
            continue
        if stable and not (
            len(samples) >= min_samples
            and int(state.get("success_pct") or 0) >= min_success_pct
            and int(state.get("streak") or 0) >= min_streak
            and int(state.get("reachable_pct") or 0) >= min_success_pct
            and int(state.get("availability_streak") or 0) >= min_streak
        ):
            continue
        speed = (
            state.get("median_speed_mbps")
            if profile in ("quality", "fast")
            else result.get("effective_speed_mbps", result.get("speed_mbps"))
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
                -float(speed or 0),
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
    else:
        path.unlink(missing_ok=True)


def write_quality_list(path: Path, lines: list[str], *, history_ready: bool) -> None:
    """Keep warm-up outputs, but remove stale quality lists once history is mature."""
    if lines:
        write_text_if_changed(path, "\n".join(lines) + "\n")
    elif history_ready and path.exists():
        path.unlink()


async def run(args: argparse.Namespace) -> int:
    started = time.monotonic()
    if not args.source.exists():
        print(f"source not found: {args.source}", file=sys.stderr)
        return 1
    lines = [
        line for line in args.source.read_text(encoding="utf-8").splitlines()
        if line.strip() and line_to_key(line)
    ]
    lines = list({line_to_key(line): line for line in lines}.values())
    if args.limit_candidates > 0:
        lines = lines[: args.limit_candidates]
    if not lines:
        print("source contains no parseable candidates", file=sys.stderr)
        return 1

    previous = read_json(args.history)
    results = await probe_all(
        lines,
        workers=args.workers,
        timeout=args.timeout,
        retries=args.retries,
        sni=args.sni,
        path=args.path,
    )
    if len(results) != len(lines):
        print("::error::Incomplete availability run; history and outputs left untouched", file=sys.stderr)
        return 2
    ok_count = sum(bool(v.get("available")) for v in results.values())
    ratio = ok_count / max(1, len(results))
    reachable_count = sum(bool(v.get("reachable")) for v in results.values())
    print(
        f"Europe probe result: reachable={reachable_count}/{len(results)} "
        f"HTTP-available={ok_count}/{len(results)} ({ratio:.1%})"
    )
    # A DNS/routing/SNI outage must not poison the rolling history or wipe a
    # previously useful subscription.
    if len(results) >= 20 and ratio < args.min_run_success_pct / 100:
        print(
            "::error::aborting: run-wide HTTP success ratio is below the safety floor; "
            "history and outputs were left untouched",
            file=sys.stderr,
        )
        return 2

    speed_keys = select_speed_keys(
        results, previous, budget=args.speed_budget,
        now=parse_timestamp(utc_now()), interval_hours=args.speed_interval_hours,
        target_samples=args.speed_target_samples,
    )
    availability_seconds = round(time.monotonic() - started, 2)
    print(
        f"Full-pool availability checked; rotating downloads={len(speed_keys)} "
        f"(budget={args.speed_budget}, speed workers={args.speed_workers})"
    )
    speed_started = time.monotonic()
    await probe_speeds(results, speed_keys, workers=args.speed_workers)
    speed_seconds = round(time.monotonic() - speed_started, 2)
    speed_ok = sum(results[key].get("speed_mbps") is not None for key in speed_keys)
    if len(speed_keys) >= 20 and speed_ok / len(speed_keys) < args.min_run_success_pct / 100:
        print("::error::Whole-run download failure; history and outputs left untouched", file=sys.stderr)
        return 2

    now = utc_now()
    current_at = parse_timestamp(now)
    history = update_history(
        previous,
        results,
        window=args.history_window,
        now=now,
        min_speed_mbps=args.min_speed_mbps,
    )
    long_keys = select_long_keys(
        results, history, budget=args.long_budget, now=current_at,
        interval_hours=args.long_interval_hours,
    )
    print(
        f"Qualifying candidates selected for long-connection checks={len(long_keys)} "
        f"(budget={args.long_budget}, duration={args.long_duration}s, "
        f"workers={args.long_workers})"
    )
    long_started = time.monotonic()
    await probe_long_connections(
        results, long_keys, workers=args.long_workers,
        duration=args.long_duration, interval=args.long_interval_seconds,
        timeout=args.long_timeout,
    )
    long_seconds = round(time.monotonic() - long_started, 2)
    now = utc_now()
    current_at = parse_timestamp(now)
    update_long_history(history, results, now=now)
    classify_results(results, min_speed_mbps=args.min_speed_mbps, history=history, now=current_at)
    states = history["proxies"]
    coverage = {
        "candidates": len(lines),
        "availability_tested": len(results),
        "availability_seconds": availability_seconds,
        "available": ok_count,
        "speed_tested": len(speed_keys),
        "speed_seconds": speed_seconds,
        "speed_succeeded": speed_ok,
        "long_tested": len(long_keys),
        "long_succeeded": sum(bool(results[key].get("long_ok")) for key in long_keys),
        "long_seconds": long_seconds,
        "speed_fresh": sum(is_fresh(
            states[key].get("last_speed_at"), SPEED_MAX_AGE_HOURS, now=current_at
        ) for key in results),
        "speed_unmeasured": sum(not states[key].get("last_speed_at") for key in results),
        "qualified": sum(bool(v.get("qualified")) for v in results.values()),
    }
    coverage["speed_stale"] = len(results) - coverage["speed_fresh"] - coverage["speed_unmeasured"]
    history["coverage"] = coverage
    print(f"Europe coverage: {coverage}")
    if args.speed_budget and math.ceil(ok_count / args.speed_budget) * 2 > SPEED_MAX_AGE_HOURS:
        print("::warning::Speed budget cannot cover the available pool within 36h at a 2h cadence")
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
    coverage.update(current=len(current), stable=len(stable), quality=len(quality), fast=len(fast))
    write_json(args.history, history)
    write_list(args.current_out, current)
    stable_history_ready = any(
        len(state.get("samples", [])) >= args.min_samples
        and len(state.get("availability_samples") or []) >= args.min_streak
        and is_fresh(state.get("last_speed_at"), SPEED_MAX_AGE_HOURS, now=current_at)
        for key, state in states.items() if key in results
    )
    write_quality_list(args.stable_out, stable, history_ready=stable_history_ready)
    quality_history_ready = any(
        len(state.get("samples", [])) >= args.quality_min_samples
        and int(state.get("speed_sample_count") or 0) >= args.quality_min_samples
        and len(state.get("availability_samples") or []) >= args.min_streak
        and is_fresh(state.get("last_speed_at"), SPEED_MAX_AGE_HOURS, now=current_at)
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
    ap.add_argument("--speed-budget", type=int, default=DEFAULT_SPEED_BUDGET,
                    help="Maximum downloads per run (0 = availability only)")
    ap.add_argument("--speed-workers", type=int, default=DEFAULT_SPEED_WORKERS)
    ap.add_argument("--speed-target-samples", type=int, default=0,
                    help="Opt-in catch-up: skip downloads with this many valid recent samples (0 = normal rotation)")
    ap.add_argument("--speed-interval-hours", type=float, default=DEFAULT_SPEED_INTERVAL_HOURS,
                    help="Do not repeat a speed measurement inside this interval")
    ap.add_argument("--long-budget", type=int, default=DEFAULT_LONG_BUDGET,
                    help="Maximum long-connection tests per run")
    ap.add_argument("--long-workers", type=int, default=DEFAULT_LONG_WORKERS)
    ap.add_argument("--long-duration", type=float, default=DEFAULT_LONG_DURATION,
                    help="Seconds to keep each TLS connection active")
    ap.add_argument("--long-interval-seconds", type=float, default=DEFAULT_LONG_INTERVAL,
                    help="Seconds between keep-alive exchanges on each connection")
    ap.add_argument("--long-timeout", type=float, default=DEFAULT_LONG_TIMEOUT,
                    help="Per-connection/request timeout for long tests")
    ap.add_argument("--long-interval-hours", type=float, default=2.0,
                    help="Do not repeat a long test inside this interval")
    ap.add_argument("--run-timeout", type=float, default=25 * 60,
                    help="Total seconds before aborting without publishing partial results")
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
    if (args.speed_budget < 0 or args.speed_target_samples < 0
            or args.speed_interval_hours < 0 or args.history_window < 1
            or args.long_budget < 0 or args.long_interval_hours < 0):
        ap.error("budgets/intervals must be nonnegative and history window must be positive")
    if min(args.workers, args.speed_workers, args.long_workers, args.timeout,
           args.run_timeout, args.long_duration, args.long_interval_seconds,
           args.long_timeout) <= 0:
        ap.error("workers and timeouts must be positive")
    if args.long_interval_seconds >= args.long_duration:
        ap.error("long interval must be shorter than the long connection duration")
    if not 0 <= args.min_run_success_pct <= 100:
        ap.error("min-run-success-pct must be between 0 and 100")

    async def bounded_run() -> int:
        try:
            async with asyncio.timeout(args.run_timeout):
                return await run(args)
        except TimeoutError:
            print("::error::Europe run timed out; history and outputs left untouched", file=sys.stderr)
            return 2

    try:
        return asyncio.run(bounded_run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
