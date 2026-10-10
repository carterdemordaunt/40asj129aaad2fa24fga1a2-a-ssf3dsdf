"""Tests for the scheduled reputation refresh workflow."""

import argparse
import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import refresh_reputation as rr


class TestBuildRefreshResults(unittest.TestCase):
    def test_prefers_trace_then_exit_family_then_cached_then_entry_ip(self):
        source = (
            "1.2.3.4:443#US\n"
            "2.3.4.5:443#JP\n"
            "3.4.5.6:443#DE\n"
            "4.5.6.7:443#FR\n"
        )
        results = rr.build_refresh_results(
            source,
            {"3.4.5.6:443#DE": {"exit_ip": "8.8.4.4"}},
            {"1.2.3.4:443#US": {"exit_geo": {"ip": "9.9.9.9"}}},
            {
                "1.2.3.4:443#US": {"exit_v4": "8.8.8.8"},
                "2.3.4.5:443#JP": {
                    "exit_v4": "1.1.1.1",
                    "exit_v6": "2606:4700:4700::1111",
                },
            },
        )

        self.assertEqual(
            (results["1.2.3.4:443#US"]["exit_ip"],
             results["1.2.3.4:443#US"]["exit_ip_source"]),
            ("9.9.9.9", "trace"),
        )
        self.assertEqual(
            (results["2.3.4.5:443#JP"]["exit_ip"],
             results["2.3.4.5:443#JP"]["exit_ip_source"]),
            ("1.1.1.1", "exit_family"),
        )
        self.assertEqual(
            (results["3.4.5.6:443#DE"]["exit_ip"],
             results["3.4.5.6:443#DE"]["exit_ip_source"]),
            ("8.8.4.4", "ipinfo_cache"),
        )
        self.assertEqual(
            (results["4.5.6.7:443#FR"]["exit_ip"],
             results["4.5.6.7:443#FR"]["exit_ip_source"]),
            ("4.5.6.7", "proxy"),
        )

    def test_ignores_rows_without_any_valid_ip_candidate(self):
        results = rr.build_refresh_results(
            "not-an-ip:443#US\n",
            {},
            {},
            {},
        )
        self.assertEqual(results, {})


class TestRefreshCoverageGuard(unittest.TestCase):
    def test_degraded_refresh_preserves_reputation_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "all.txt"
            quality = root / "quality"
            quality.mkdir()
            source.write_text(
                "".join(
                    f"198.51.100.{i}:443#US-50ms\n"
                    for i in range(1, 101)
                ),
                encoding="utf-8",
            )
            rep_path = quality / "reputation.json"
            previous_snapshot = '{"proxies":{"old": {"score": 90}}}\n'
            rep_path.write_text(previous_snapshot, encoding="utf-8")
            (quality / "quality_meta.json").write_text(
                '{"total":100}', encoding="utf-8"
            )

            results = rr.build_refresh_results(
                source.read_text(encoding="utf-8"), {}, {}, {}
            )
            partial_map = {
                key: {"score": 90, "risk": "low"}
                for key in list(results)[:10]
            }
            args = argparse.Namespace(
                source=source,
                time_budget=0,
                rep_cache_ttl=604800,
            )
            with (
                patch.multiple(
                    rr,
                    IPINFO_FILE=quality / "ipinfo.json",
                    EXTERNAL_CHECK_FILE=quality / "external_check.json",
                    EXIT_FAMILY_FILE=quality / "exit_family.json",
                    QUALITY_META_FILE=quality / "quality_meta.json",
                    QUALITY_DIR=quality,
                    REPUTATION_FILE=rep_path,
                    DEFAULT_REP_SOURCES=("ip-api",),
                    batch_ipapi=AsyncMock(return_value={}),
                    unavailable_reputation_sources=lambda _sources: [],
                    lookup_all_risk=AsyncMock(return_value={}),
                    build_reputation_map=lambda *_args, **_kwargs: partial_map,
                    read_fresh_deep_speed=lambda: {},
                    write_reputation_files=unittest.mock.Mock(),
                    annotate_valid_files=unittest.mock.Mock(),
                    build_annotations=unittest.mock.Mock(),
                    build_ipinfo_map=unittest.mock.Mock(),
                    now_ts=lambda: "2026-10-10T00:00:00Z",
                )
            ):
                status = asyncio.run(rr.refresh(args))

            self.assertEqual(status, 0)
            self.assertEqual(rep_path.read_text(encoding="utf-8"), previous_snapshot)
            meta = json.loads(
                (quality / "quality_meta.json").read_text(encoding="utf-8")
            )
            self.assertEqual(meta["reputation_checked"], 10)
            self.assertEqual(meta["reputation_total"], 100)
            self.assertEqual(meta["reputation_coverage"], 0.1)
            self.assertTrue(meta["reputation_degraded"])
            self.assertFalse(meta["reputation_published"])


if __name__ == "__main__":
    unittest.main()
