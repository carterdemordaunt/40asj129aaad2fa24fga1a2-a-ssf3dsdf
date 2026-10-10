#!/usr/bin/env python3
"""Refresh reputation signals and rebuild reputation-ranked proxy lists."""

import argparse
import asyncio
import ipaddress
import os
import sys
import time
from collections import Counter
from pathlib import Path

from common import (
    DEFAULT_SOURCE,
    EXIT_FAMILY_FILE,
    EXTERNAL_CHECK_FILE,
    IPINFO_FILE,
    MIN_REP_COVERAGE,
    QUALITY_DIR,
    QUALITY_META_FILE,
    REP_CACHE_TTL,
    REPUTATION_FILE,
    keyed_json,
    now_ts,
    parse_ltd_line,
    read_json,
    write_json,
)
from quality_check import (
    annotate_valid_files,
    build_annotations,
    build_ipinfo_map,
    build_reputation_map,
    read_fresh_deep_speed,
    write_reputation_files,
)
from quality_probe import batch_ipapi
from quality_reputation import (
    DEFAULT_REP_SOURCES,
    REPUTATION_WEIGHTS,
    lookup_all_risk,
    norm_asn,
    unavailable_reputation_sources,
)


def _valid_ip(value) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return None
    return value


def build_refresh_results(
    source_text: str,
    ipinfo: dict,
    external_checks: dict,
    exit_families: dict,
) -> dict[str, dict]:
    """Build current proxy records with the best known exit IP per key."""
    results = {}
    for line in source_text.splitlines():
        parsed = parse_ltd_line(line)
        if not parsed:
            continue
        key, entry_ip, _port, cc = parsed
        previous = ipinfo.get(key) or {}
        external = external_checks.get(key) or {}
        family = exit_families.get(key) or {}
        external_ip = (external.get("exit_geo") or {}).get("ip")
        candidates = (
            (external_ip, "trace"),
            (family.get("exit_v4"), "exit_family"),
            (family.get("exit_v6"), "exit_family"),
            (previous.get("exit_ip"), "ipinfo_cache"),
            (entry_ip, "proxy"),
        )
        selected = next(
            (
                (valid, source)
                for candidate, source in candidates
                if (valid := _valid_ip(candidate)) is not None
            ),
            None,
        )
        if selected is None:
            continue
        exit_ip, exit_source = selected
        result = {
            "key": key,
            "ip": entry_ip,
            "cc": cc,
            "exit_ip": exit_ip,
            "exit_ip_source": exit_source,
        }
        if external:
            result["external_check"] = external
        results[key] = result
    return results


def _set_github_output(name: str, value: str) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as output:
            output.write(f"{name}={value}\n")


def _merge_ipinfo(results: dict, refreshed: dict, previous: dict) -> dict:
    merged = {}
    for key, result in results.items():
        item = dict(previous.get(key) or {})
        for field, value in (refreshed.get(key) or {}).items():
            if value is not None:
                item[field] = value
        item["exit_ip"] = result["exit_ip"]
        item["exit_ip_source"] = result["exit_ip_source"]
        merged[key] = item
    return merged


async def refresh(args: argparse.Namespace) -> int:
    source_text = args.source.read_text(encoding="utf-8")
    previous_ipinfo = read_json(IPINFO_FILE).get("proxies", {})
    external_checks = read_json(EXTERNAL_CHECK_FILE).get("proxies", {})
    exit_families = read_json(EXIT_FAMILY_FILE).get("proxies", {})
    results = build_refresh_results(
        source_text, previous_ipinfo, external_checks, exit_families
    )
    if not results:
        print(f"No valid proxy entries in {args.source}", file=sys.stderr)
        return 1

    ips = list(dict.fromkeys(item["exit_ip"] for item in results.values()))
    deadline = time.monotonic() + args.time_budget if args.time_budget > 0 else None
    geo = await batch_ipapi(ips, deadline=deadline)
    asn_map = {
        ip: asn
        for ip, item in geo.items()
        if (asn := norm_asn(item.get("asn") or item.get("as")))
    }

    unavailable = unavailable_reputation_sources(DEFAULT_REP_SOURCES)
    sources = [name for name in DEFAULT_REP_SOURCES if name not in unavailable]
    lookup_args = argparse.Namespace(
        reputation_sources=sources,
        reputation_weights=REPUTATION_WEIGHTS,
        rep_cache_ttl=args.rep_cache_ttl,
        no_rep_cache=False,
        getipintel_email="",
    )
    risk_data = await lookup_all_risk(
        ips, lookup_args, asn_map, deadline=deadline
    )
    rep_map = build_reputation_map(
        results,
        risk_data,
        REPUTATION_WEIGHTS,
        deep_speed=read_fresh_deep_speed(),
        geo=geo,
    )
    coverage = len(rep_map) / len(results) if results else 0.0
    degraded = (
        not rep_map
        or (len(results) >= 100 and coverage < MIN_REP_COVERAGE)
    )
    published = bool(rep_map) and not degraded
    generated = now_ts()
    exit_sources = Counter(
        item["exit_ip_source"] for item in results.values()
    )
    refresh_meta = {
        "ts": generated,
        "proxy_count": len(results),
        "unique_exit_ips": len(ips),
        "geo_checked": len(geo),
        "risk_signals": len(risk_data),
        "reputation_checked": len(rep_map),
        "reputation_coverage": round(coverage, 4),
        "reputation_published": published,
        "reputation_degraded": degraded,
        "sources": sources,
        "unavailable_sources": unavailable,
        "exit_ip_sources": dict(sorted(exit_sources.items())),
    }

    quality_meta = read_json(QUALITY_META_FILE)
    quality_meta.update({
        "reputation_ts": generated,
        "reputation_total": len(results),
        "reputation_checked": len(rep_map),
        "reputation_coverage": round(coverage, 4),
        "reputation_degraded": degraded,
        "reputation_published": published,
    })
    write_json(QUALITY_META_FILE, quality_meta)
    write_json(QUALITY_DIR / "reputation_refresh.json", refresh_meta)
    _set_github_output("published", str(published).lower())

    print(
        f"Reputation refresh: {len(rep_map)}/{len(results)} scored "
        f"({coverage:.1%}), geo={len(geo)}/{len(ips)}, "
        f"sources={len(sources)}, published={published}"
    )
    if degraded:
        print(
            "Reputation coverage is below the publish threshold; "
            "keeping the previous reputation snapshot",
            file=sys.stderr,
        )
        return 0

    annotations = build_annotations(results, rep_map)
    write_reputation_files(source_text, annotations, rep_map)
    annotate_valid_files(annotations)
    refreshed_ipinfo = build_ipinfo_map(
        results, geo, {}, risk_data, REPUTATION_WEIGHTS
    )
    write_json(
        IPINFO_FILE,
        keyed_json(_merge_ipinfo(results, refreshed_ipinfo, previous_ipinfo)),
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, default=DEFAULT_SOURCE,
        help="Validated proxy list (default: data/valid/all.txt)",
    )
    parser.add_argument(
        "--rep-cache-ttl", type=int, default=REP_CACHE_TTL,
        help="Per-IP source cache TTL in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--time-budget", type=int, default=5400,
        help="Stop opening network requests after this many seconds",
    )
    return asyncio.run(refresh(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
