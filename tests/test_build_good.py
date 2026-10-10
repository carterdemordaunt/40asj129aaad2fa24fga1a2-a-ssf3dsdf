"""Tests for build_good.py scoring, filtering and output layout."""

import json
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import build_good as bg


def healthy_map(text: str, *, median_speed_mbps: float = 8.0) -> dict[str, dict]:
    state = {
        "samples": [1] * 6,
        "speed_sample_count": 6,
        "success_pct": 100,
        "streak": 6,
        "median_speed_mbps": median_speed_mbps,
        "speed_spread_pct": 0.1,
    }
    return {
        key: dict(state)
        for line in text.splitlines()
        if (key := bg.line_to_key(line))
    }


class TestParseMetrics(unittest.TestCase):
    def test_latency_and_speed(self):
        ms, mbps = bg.parse_metrics("1.2.3.4:443#US-130ms-1.86MB/s-CF-99")
        self.assertEqual(ms, 130)
        self.assertAlmostEqual(mbps, 1.86)

    def test_missing_metrics(self):
        self.assertEqual(bg.parse_metrics("1.2.3.4:443#US"), (None, None))

    def test_estimated_speed_ignored(self):
        # ≈ 大陆估算 token 不算实测速度——不进入综合分（R78 同族 ≈ 拒绝）。
        # 延迟仍取（大陆视角展示），仅速度轴置 None。
        self.assertEqual(
            bg.parse_metrics("1.2.3.4:443#US-130ms-≈1.86MB/s"),
            (130, None),
        )
        self.assertEqual(
            bg.parse_metrics("1.2.3.4:443#US-≈99MB/s"), (None, None)
        )

    def test_latency_only(self):
        self.assertEqual(bg.parse_metrics("1.2.3.4:443#US-80ms"), (80, None))


class TestScores(unittest.TestCase):
    def test_latency_score_boundaries(self):
        self.assertEqual(bg.latency_score(None), 0.0)
        self.assertEqual(bg.latency_score(50), 100.0)
        self.assertEqual(bg.latency_score(100), 100.0)
        self.assertEqual(bg.latency_score(1500), 0.0)
        self.assertEqual(bg.latency_score(2000), 0.0)

    def test_latency_score_linear(self):
        # midpoint between 100 and 1500 -> 50
        self.assertAlmostEqual(bg.latency_score(800), 50.0)

    def test_speed_score(self):
        self.assertEqual(bg.speed_score(None), 0.0)
        self.assertEqual(bg.speed_score(0.0), 0.0)
        self.assertEqual(bg.speed_score(2.0), 100.0)
        self.assertEqual(bg.speed_score(1.0), 50.0)
        self.assertEqual(bg.speed_score(33.0), 100.0)

    def test_composite_reputation_weighted(self):
        # 0.6*100 + 0.2*100 + 0.2*100 = 100
        self.assertEqual(bg.composite_score(100, 80, 5.0), 100)
        # reputation only, no metrics: 0.6*100 = 60
        self.assertEqual(bg.composite_score(100, None, None), 60)
        # 0.6*80 + 0.2*50 + 0.2*0 = 58
        self.assertEqual(bg.composite_score(80, 800, None), 58)

    def test_composite_without_reputation_renormalizes_other_weights(self):
        self.assertEqual(bg.composite_score(None, 100, 5.0), 100)
        self.assertEqual(bg.composite_score(None, 1500, None), 0)


class TestMaps(unittest.TestCase):
    def test_build_rep_map(self):
        data = {"proxies": {
            "1.2.3.4:443#US": {"score": 88, "risk": "low"},
            "5.6.7.8:443#JP": {"risk": "medium"},
            "6.6.6.6:443#DE": "garbage",
        }}
        m = bg.build_rep_map(data)
        self.assertEqual(m, {
            "1.2.3.4:443#US": {"score": 88, "risk": "low"},
            "5.6.7.8:443#JP": {"score": None, "risk": "medium"},
        })

    def test_build_china_set(self):
        """CN 池 = 当期全可达集（清单保持完整，不按延迟精简）。"""
        data = {"proxies": {
            "1.2.3.4:443#US": {"verdict": "reachable", "ms": 218},
            "5.6.7.8:443#JP": {"verdict": "reachable", "ms": 1},   # 噪声 ms 不剔除
            "0.0.0.1:443#US": {"verdict": "reachable"},            # 无数值 ms 也保留
            "7.7.7.7:443#US": {"verdict": "unreachable"},
            "6.6.6.6:443#DE": {"verdict": "uncertain"},
        }}
        self.assertEqual(bg.build_china_set(data),
                         {"1.2.3.4:443#US", "5.6.7.8:443#JP", "0.0.0.1:443#US"})

    def test_build_china_set_no_verdict(self):
        """verdict 缺失视为非可达（CN 池只收当期可达）。"""
        data = {"proxies": {
            "3.3.3.3:443#US": {"ms": 20},
        }}
        self.assertEqual(bg.build_china_set(data), set())

    def test_build_cn_ms_map_prefers_trusted_l2(self):
        """延迟图优先可信大陆探测，L3 复核源的 1ms 噪声不得覆盖真实 L2 读数。"""
        data = {"proxies": {
            # cn14 1ms vs cn20 234ms → 取 234（大陆视角）
            "1.1.1.1:443#US": {"verdict": "reachable", "ms": 1, "sources": {
                "cn20": {"status": "ok", "ms": 234.0},
                "cn14": {"status": "ok", "ms": 1}}},
            # 无大陆探测，回退合并 ms
            "2.2.2.2:443#US": {"verdict": "reachable", "ms": 42, "sources": {
                "cn30": {"status": "ok", "ms": 42}}},
        }}
        m = bg.build_cn_ms_map(data)
        self.assertEqual(m, {"1.1.1.1:443#US": 234.0, "2.2.2.2:443#US": 42})

    def test_to_cn_view_rewrites_mainland_latency_and_speed(self):
        """CN 视图：ms 用大陆实测、速度用 ≈XMB/s，噪声/无读数键诚实处理。"""
        lines = [
            "7.7.7.7:443#US→US-6ms-116.99MB/s-RES-fast-V4-CN-100-U16",
            "8.8.8.8:443#JP→JP-30ms-1.00MB/s-RES-mid-V4-CN-99-U16",
        ]
        cn_ms = {"7.7.7.7:443#US": 234.0, "8.8.8.8:443#JP": 35.0}
        out = bg.to_cn_view(lines, cn_ms)
        self.assertIn("7.7.7.7:443#US→US-234ms-", out[0])     # 大陆 234 非海外 6
        self.assertIn("≈", out[0])                            # 速度标记 ≈ 估算
        self.assertIn("8.8.8.8:443#JP→JP-35ms-", out[1])
        self.assertIn("≈", out[1])
        # 无 cn_ms → 原样
        self.assertEqual(bg.to_cn_view(lines, None), lines)

    def test_to_cn_view_key_missing_keeps_latency_drops_speed(self):
        """可达但无大陆读数的键：延迟保留海外值（validate write_variant 规则），
        速度 token 删除而非冒充大陆值。"""
        lines = [
            "9.9.9.9:443#US→US-24ms-8.00MB/s-RES-fast-V4-CN-98-U100",
        ]
        cn_ms = {"OTHER:443#US": 1.0}  # 键缺失
        out = bg.to_cn_view(lines, cn_ms)
        self.assertIn("-24ms-", out[0])            # 海外延迟回退保留
        self.assertNotIn("MB/s", out[0])           # 速度删除
        self.assertNotIn("≈", out[0])

    def test_is_cn_reachable_current_only(self):
        china = {"1.2.3.4:443#US"}
        # judged reachable this run
        self.assertTrue(bg.is_cn_reachable(
            "1.2.3.4:443#US", "1.2.3.4:443#US-80ms", china))
        # historical -CN annotation, absent from current verdicts → 不再兜底
        self.assertFalse(bg.is_cn_reachable(
            "5.6.7.8:443#JP", "5.6.7.8:443#JP-80ms-CN-V6", china))
        # neither
        self.assertFalse(bg.is_cn_reachable(
            "6.6.6.6:443#DE", "6.6.6.6:443#DE-80ms-V6", china))
        self.assertFalse(bg.is_cn_reachable(None, "", china))


class TestTopSlice(unittest.TestCase):
    def setUp(self):
        self.z1 = "203.0.0.1:443#DE-40ms"
        self.z2 = "204.0.0.1:443#DE-40ms"

    def _mk(self, mbps_values, no_speed_at_end=()):
        lines = []
        for i, mb in enumerate(mbps_values):
            lines.append(f"p{i}.0.0.1:443#US-40ms-{mb}MB/s")
        lines.extend(no_speed_at_end)
        return lines

    def test_line_mbps(self):
        self.assertEqual(bg._line_mbps("1.2.3.4:443#US-80ms-1.86MB/s"), 1.86)
        self.assertEqual(bg._line_mbps("1.2.3.4:443#US-80ms"), -1.0)
        self.assertEqual(bg._line_mbps("1.2.3.4:443#US-80ms-≈2MB/s"), -1.0)

    def test_top_slice_skips_missing_speed(self):
        lines = self._mk([10, 30, 20, 40, 50, 60, 70, 80],
                         no_speed_at_end=[self.z1, self.z2])
        picked, thr = bg.top_slice(lines)
        self.assertEqual(thr, 60.0)      # vals=10..80 升序, idx max(0,int(8*.75)-1)=5
        self.assertEqual(len(picked), 3)  # 60/70/80
        self.assertTrue(all(self.z1 not in m for m in picked))
        self.assertTrue(all(self.z2 not in m for m in picked))

    def test_top_slice_picks_top_fraction_with_ties(self):
        lines = self._mk([10, 20, 30, 40, 50, 60, 70, 70.0])
        picked, thr = bg.top_slice(lines)
        self.assertEqual(thr, 60.0)
        self.assertEqual(len(picked), 3)  # 60,70,70 → 并列同保留

    def test_top_slice_insufficient_samples(self):
        lines = self._mk([10, 20, 30, 40, 50, 60, 70])
        picked, thr = bg.top_slice(lines)  # 7 < min_samples=8
        self.assertEqual((picked, thr), ([], None))


class TestFilterRank(unittest.TestCase):
    LINES = (
        "9.9.9.9:443#US-500ms-2.00MB/s-CN-90\n"
        "1.1.1.1:443#US-100ms-5.00MB/s-CN-90\n"
        "5.5.5.5:443#JP-60ms-10.0MB/s-95\n"          # not CN reachable
        "2.2.2.2:443#HK-50ms-8.00MB/s-CN-40\n"       # rep below 80
        "3.3.3.3:443#SG-70ms-1.00MB/s-CN-99-risky\n" # risk high below
    )

    def setUp(self):
        self.china = {"9.9.9.9:443#US", "1.1.1.1:443#US",
                      "2.2.2.2:443#HK", "3.3.3.3:443#SG"}
        self.rep = {
            "9.9.9.9:443#US": {"score": 90, "risk": "low"},
            "1.1.1.1:443#US": {"score": 90, "risk": "low"},
            "2.2.2.2:443#HK": {"score": 40, "risk": "low"},
            "3.3.3.3:443#SG": {"score": 99, "risk": "high"},
            "5.5.5.5:443#JP": {"score": 95, "risk": "low"},
        }

    def test_filters_and_orders(self):
        out = bg.filter_rank(self.LINES, self.china, self.rep)
        # 1.1.1.1: 0.6*90+0.2*100+0.2*100=94; 9.9.9.9: 54+20+8=82;
        # Reputation and CN reachability are informational only; all lines pass.
        self.assertEqual(
            [l.split("#")[0] for l in out],
            ["5.5.5.5:443", "1.1.1.1:443", "3.3.3.3:443", "9.9.9.9:443", "2.2.2.2:443"],
        )

    def test_rep_score_threshold_boundary(self):
        lines = (
            "8.0.0.1:443#US-100ms-CN-80\n"
            "8.0.0.2:443#US-100ms-CN-79\n"
        )
        china = {f"8.0.0.{i}:443#US" for i in (1, 2)}
        rep = {
            "8.0.0.1:443#US": {"score": 80, "risk": "low"},
            "8.0.0.2:443#US": {"score": 79, "risk": "low"},
        }
        out = bg.filter_rank(lines, china, rep)
        self.assertEqual([l.split(":")[0] for l in out], ["8.0.0.1", "8.0.0.2"])

        strict = bg.filter_rank(
            lines, china, rep, min_rep_score=bg.HEALTHY_GOOD_MIN_REP_SCORE
        )
        self.assertEqual(len(strict), 2)

    def test_health_gate_requires_mature_fast_stable_history(self):
        line = "8.0.0.1:443#US-100ms-8.00MB/s-CN-90"
        key = "8.0.0.1:443#US"
        china = {key}
        rep = {key: {"score": 90, "risk": "low"}}
        healthy = {
            "samples": [1] * 6,
            "speed_sample_count": 6,
            "success_pct": 100,
            "streak": 6,
            "median_speed_mbps": 8.0,
            "speed_spread_pct": 0.1,
        }
        self.assertEqual(
            bg.filter_rank(line, china, rep, health_map={key: healthy}), [line]
        )
        unstable = {**healthy, "success_pct": 80}
        self.assertEqual(
            bg.filter_rank(line, china, rep, health_map={key: unstable}), []
        )
        slow = {**healthy, "median_speed_mbps": 1.99}
        self.assertEqual(bg.filter_rank(line, china, rep, health_map={key: slow}), [])
        missing_speed_history = {**healthy, "speed_sample_count": 1}
        self.assertEqual(
            bg.filter_rank(
                line, china, rep, health_map={key: missing_speed_history}
            ),
            [],
        )

    def test_missing_reputation_skips_only_reputation_checks(self):
        lines = (
            "8.0.0.1:443#US-100ms-8.00MB/s\n"
            "8.0.0.2:443#US-100ms-8.00MB/s\n"
            "8.0.0.3:443#US-100ms-8.00MB/s\n"
            "8.0.0.4:443#US-100ms-8.00MB/s\n"
        )
        keys = [f"8.0.0.{i}:443#US" for i in range(1, 5)]
        healthy = {
            "samples": [1] * 6,
            "speed_sample_count": 6,
            "success_pct": 100,
            "streak": 6,
            "median_speed_mbps": 8.0,
            "speed_spread_pct": 0.1,
        }
        health = {key: healthy for key in keys}
        health[keys[1]] = {**healthy, "median_speed_mbps": 4.9}
        rep = {
            keys[2]: {"score": 40, "risk": "low"},
            keys[3]: {"score": None, "risk": "high"},
        }

        out = bg.filter_rank(
            lines, set(keys), rep, health_map=health,
            min_rep_score=bg.HEALTHY_GOOD_MIN_REP_SCORE,
        )

        self.assertEqual(set(bg.line_to_key(line) for line in out), set(keys))

    def test_health_history_rejects_stale_or_missing_timestamp(self):
        now = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
        self.assertTrue(bg.health_history_fresh({"ts": "2026-10-09T10:00:00Z"}, now=now))
        self.assertFalse(bg.health_history_fresh({"ts": "2026-10-09T01:00:00Z"}, now=now))
        self.assertFalse(bg.health_history_fresh({}, now=now))

    def test_tie_breaks_by_latency_then_key(self):
        lines = (
            "9.0.0.2:443#US-300ms-CN-90\n"
            "9.0.0.1:443#US-300ms-CN-90\n"
            "9.0.0.3:443#US-200ms-CN-90\n"
        )
        china = {f"9.0.0.{i}:443#US" for i in (1, 2, 3)}
        rep = {f"9.0.0.{i}:443#US": {"score": 90, "risk": "low"}
               for i in (1, 2, 3)}
        out = bg.filter_rank(lines, china, rep)
        self.assertEqual(
            [l.split(":")[0] for l in out],
            ["9.0.0.3", "9.0.0.1", "9.0.0.2"],
        )

    def test_historical_cn_token_rejected(self):
        lines = "7.7.7.7:443#DE-120ms-3.00MB/s-CN-V6-88\n"
        # CN reachability is no longer a good-list requirement.
        out = bg.filter_rank(lines, set(),
                             {"7.7.7.7:443#DE": {"score": 88, "risk": "low"}})
        self.assertEqual(len(out), 1)

    def test_lines_kept_verbatim(self):
        line = "1.1.1.1:443#🇺🇸US-100ms-5.00MB/s-CN-V6-GPT-90"
        out = bg.filter_rank(line, {"1.1.1.1:443#US"},
                             {"1.1.1.1:443#US": {"score": 90, "risk": "low"}})
        self.assertEqual(out, [line])

    def test_empty_inputs(self):
        self.assertEqual(bg.filter_rank("", set(), {}), [])

    def test_europe_metrics_are_kept_for_ranking(self):
        lines = (
            "1.0.0.1:443#US-50ms-5.00MB/s-CN-90\n"    # 海外快，大陆慢
            "2.0.0.2:443#US-900ms-5.00MB/s-CN-90\n"   # 海外慢，大陆快
        )
        china = {"1.0.0.1:443#US", "2.0.0.2:443#US"}
        rep = {
            "1.0.0.1:443#US": {"score": 90, "risk": "low"},
            "2.0.0.2:443#US": {"score": 90, "risk": "low"},
        }
        # 无 cn_ms：按行内海外延迟排序，1.0.0.1 在前
        out = bg.filter_rank(lines, china, rep)
        self.assertEqual([l.split(":")[0] for l in out], ["1.0.0.1", "2.0.0.2"])
        # CN measurements are ignored; Europe inline latency remains authoritative.
        cn_ms = {"1.0.0.1:443#US": 800.0, "2.0.0.2:443#US": 60.0}
        out = bg.filter_rank(lines, china, rep, cn_ms)
        self.assertEqual([l.split(":")[0] for l in out], ["1.0.0.1", "2.0.0.2"])


class TestWriteGoodFiles(unittest.TestCase):
    POOL = (
        "1.1.1.1:443#US-100ms-5.00MB/s-CN-90\n"
        "2.2.2.2:443#US-400ms-1.00MB/s-CN-70\n"
        "5.5.5.5:443#JP-60ms-9.00MB/s-95\n"
    )
    CHINA = {"1.1.1.1:443#US", "2.2.2.2:443#US"}
    REP = {
        "1.1.1.1:443#US": {"score": 90, "risk": "low"},
        "2.2.2.2:443#US": {"score": 85, "risk": "low"},
    }

    def test_layout_and_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            (valid / "countries" / "US").mkdir(parents=True)
            (valid / "sets" / "hot").mkdir(parents=True)
            (valid / "countries" / "XX").mkdir()   # no all.txt -> skipped
            (valid / "all.txt").write_text(self.POOL, encoding="utf-8")
            (valid / "countries" / "US" / "all.txt").write_text(
                self.POOL, encoding="utf-8")
            (valid / "sets" / "hot" / "all.txt").write_text(
                self.POOL, encoding="utf-8")

            stats = bg.write_good_files(
                valid, self.CHINA, self.REP, health_map=healthy_map(self.POOL)
            )
            self.assertEqual(stats["all_good"], 3)
            self.assertEqual(stats["countries/US"], 3)
            self.assertEqual(stats["sets/hot"], 3)
            self.assertNotIn("countries/XX", stats)

            good = (valid / "all_good.txt").read_text(encoding="utf-8")
            self.assertEqual(good.splitlines()[0].split("#")[0], "5.5.5.5:443")
            self.assertEqual(len(good.splitlines()), 3)
            self.assertTrue((valid / "countries" / "US" / "good.txt").exists())
            self.assertTrue((valid / "sets" / "hot" / "good.txt").exists())

    def test_good_ltd_from_ltd_pools(self):
        """good_ltd = 同套 good 标准在 ltd.txt 限量池上筛选（每国最快优质子集）。"""
        ltd_pool = (
            "1.1.1.1:443#US-100ms-5.00MB/s-CN-90\n"   # reachable + rep90 -> in
            "5.5.5.5:443#JP-60ms-9.00MB/s-95\n"       # 非 CN -> drop
            "7.7.7.7:443#US-200ms-0.50MB/s-CN-55\n"   # rep<80 -> drop
        )
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            (valid / "countries" / "US").mkdir(parents=True)
            (valid / "sets" / "hot").mkdir(parents=True)
            (valid / "all_ltd.txt").write_text(ltd_pool, encoding="utf-8")
            (valid / "countries" / "US" / "ltd.txt").write_text(
                ltd_pool, encoding="utf-8")
            (valid / "sets" / "hot" / "ltd.txt").write_text(
                ltd_pool, encoding="utf-8")

            stats = bg.write_good_files(
                valid, self.CHINA, self.REP, health_map=healthy_map(ltd_pool)
            )
            self.assertEqual(stats["all_good_ltd"], 3)
            self.assertEqual(stats["countries/US_ltd"], 3)
            self.assertEqual(stats["sets/hot_ltd"], 3)
            self.assertNotIn("countries/XX", stats)

            for rel in ("all_good_ltd.txt", "countries/US/good_ltd.txt",
                        "sets/hot/good_ltd.txt"):
                body = (valid / rel).read_text(encoding="utf-8")
                self.assertEqual(len(body.splitlines()), 3)

    def test_good_ltd_stale_files_cleaned(self):
        """good_ltd 全部清空时清理上轮残留（基清单及其变体）。"""
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            valid.mkdir(parents=True)
            for name in ("all_good_ltd.txt", "all_good_ltd_verified.txt",
                         "all_good_ltd_stable.txt"):
                (valid / name).write_text("stale\n", encoding="utf-8")
            # 无 ltd 池 -> 不产生且清理旧文件
            bg.write_good_files(valid, set(), {})
            self.assertFalse((valid / "all_good_ltd.txt").exists())

            # 有 ltd 池时无需 CN 可达条件。
            (valid / "all_ltd.txt").write_text(
                "5.5.5.5:443#JP-60ms-9.00MB/s-95\n", encoding="utf-8")
            bg.write_good_files(
                valid,
                self.CHINA,
                self.REP,
                health_map=healthy_map(
                    "5.5.5.5:443#JP-60ms-9.00MB/s-95\n"
                ),
            )
            self.assertTrue((valid / "all_good_ltd.txt").exists())

    def test_verified_stable_variants(self):
        import common

        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            valid.mkdir(parents=True)
            (valid / "all.txt").write_text(self.POOL, encoding="utf-8")
            orig = common.SPEED_FILE, common.CHINA_FILE
            common.SPEED_FILE = Path(tmp) / "speed.json"
            common.CHINA_FILE = Path(tmp) / "china.json"
            try:
                common.SPEED_FILE.write_text(
                    json.dumps({"proxies": {"1.1.1.1:443#US": {}}}),
                    encoding="utf-8",
                )
                common.CHINA_FILE.write_text(
                    json.dumps(
                        {
                            "proxies": {
                                "2.2.2.2:443#US": {
                                    "verdict": "reachable",
                                    "streak": 2,
                                }
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                bg.write_good_files(
                    valid,
                    self.CHINA,
                    self.REP,
                    health_map=healthy_map(self.POOL),
                )
            finally:
                common.SPEED_FILE, common.CHINA_FILE = orig

            ver = (valid / "all_good_verified.txt").read_text(encoding="utf-8")
            self.assertEqual(
                [l.split("#")[0] for l in ver.splitlines()], ["1.1.1.1:443"]
            )
            sta = (valid / "all_good_stable.txt").read_text(encoding="utf-8")
            self.assertEqual(
                [l.split("#")[0] for l in sta.splitlines()], ["2.2.2.2:443"]
            )

    def test_tier_variants_and_dirs(self):
        """good_<tier> 变体 + tiers/<tier>/ 细分目录（全局/国家/集合三视图）。"""
        pool = (
            "1.1.1.1:443#US-100ms-5.00MB/s-CN-fast-90\n"
            "2.2.2.2:443#US-400ms-1.00MB/s-CN-slow-85\n"
            "3.3.3.3:443#JP-80ms-4.00MB/s-CN-mid-90\n"
        )
        china = {"1.1.1.1:443#US", "2.2.2.2:443#US", "3.3.3.3:443#JP"}
        rep = {k: {"score": 90, "risk": "low"} for k in
               ("1.1.1.1:443#US", "2.2.2.2:443#US", "3.3.3.3:443#JP")}
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            (valid / "countries" / "US").mkdir(parents=True)
            (valid / "sets" / "hot").mkdir(parents=True)
            (valid / "all.txt").write_text(pool, encoding="utf-8")
            (valid / "countries" / "US" / "all.txt").write_text(pool, encoding="utf-8")
            (valid / "sets" / "hot" / "all.txt").write_text(pool, encoding="utf-8")

            stats = bg.write_good_files(
                valid, china, rep, health_map=healthy_map(pool)
            )

            # 扁平变体：按档位 token 分桶
            fast = (valid / "all_good_fast.txt").read_text(encoding="utf-8")
            self.assertEqual(fast.splitlines()[0].split("#")[0], "1.1.1.1:443")
            slow = (valid / "all_good_slow.txt").read_text(encoding="utf-8")
            self.assertEqual(slow.splitlines()[0].split("#")[0], "2.2.2.2:443")
            self.assertIn("tiers/fast", stats)

            # 细分目录：tiers/<t>/{all,<CC>,sets/<name>}.txt 内容与扁平一致
            tdir = valid / "tiers"
            self.assertEqual(
                (tdir / "fast" / "US.txt").read_text(encoding="utf-8"), fast)
            self.assertEqual(
                (tdir / "slow" / "US.txt").read_text(encoding="utf-8"), slow)
            mid_all = (tdir / "mid" / "all.txt").read_text(encoding="utf-8")
            self.assertEqual(mid_all.splitlines()[0].split("#")[0], "3.3.3.3:443")
            self.assertTrue((tdir / "mid" / "sets" / "hot.txt").exists())
            self.assertTrue((tdir / "slow" / "US.txt").exists())
            self.assertFalse((tdir / "slow" / "JP.txt").exists())  # JP 无 slow 行

    def test_tier_dir_removed_when_empty(self):
        """池中不再有某档位 → 对应 tiers 子目录内容被清理。"""
        pool_fast = "1.1.1.1:443#US-100ms-5.00MB/s-CN-fast-90\n"
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            valid.mkdir(parents=True)
            (valid / "all.txt").write_text(pool_fast, encoding="utf-8")
            bg.write_good_files(
                valid,
                {"1.1.1.1:443#US"},
                {"1.1.1.1:443#US": {"score": 90, "risk": "low"}},
                health_map=healthy_map(pool_fast),
            )
            stale = valid / "tiers" / "slow" / "all.txt"
            stale.parent.mkdir(parents=True)
            stale.write_text("9.9.9.9:80#US\n", encoding="utf-8")
            bg.write_good_files(
                valid,
                {"1.1.1.1:443#US"},
                {"1.1.1.1:443#US": {"score": 90, "risk": "low"}},
                health_map=healthy_map(pool_fast),
            )
            self.assertFalse(stale.exists())

    def test_note_tier_helper(self):
        import common
        self.assertEqual(common.note_tier("1.1.1.1:80#US-x-fast"), "fast")
        self.assertIsNone(common.note_tier("1.1.1.1:80#US-x"))

    def test_europe_view_applied_to_all_good_outputs(self):
        """good 输出保留欧洲实测延迟和速度，不依赖 CN 快照。"""
        pool = (
            "1.1.1.1:443#US-100ms-5.00MB/s-CN-fast-90\n"
            "2.2.2.2:443#US-400ms-1.00MB/s-CN-slow-85\n"
        )
        china = {"1.1.1.1:443#US", "2.2.2.2:443#US"}
        rep = {k: {"score": 90, "risk": "low"} for k in china}
        cn_ms = {"1.1.1.1:443#US": 234.0, "2.2.2.2:443#US": 35.0}
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            (valid / "countries" / "US").mkdir(parents=True)
            (valid / "sets" / "hot").mkdir(parents=True)
            (valid / "all.txt").write_text(pool, encoding="utf-8")
            (valid / "countries" / "US" / "all.txt").write_text(pool, encoding="utf-8")
            (valid / "sets" / "hot" / "all.txt").write_text(pool, encoding="utf-8")

            bg.write_good_files(
                valid,
                china,
                rep,
                cn_ms=cn_ms,
                health_map=healthy_map(pool),
            )

            for rel in ("all_good.txt", "countries/US/good.txt",
                        "sets/hot/good.txt"):
                body = (valid / rel).read_text(encoding="utf-8")
                lines = body.splitlines()
                self.assertIn("US-100ms-", lines[0])
                self.assertNotIn("≈", lines[0])
                self.assertIn("US-400ms-", lines[1])
                self.assertNotIn("≈", lines[1])
            fast = (valid / "all_good_fast.txt").read_text(encoding="utf-8")
            self.assertNotIn("≈", fast)
            ver = (valid / "all_good_verified.txt")  # 无 speed.json 数据 → 不生成
            self.assertFalse(ver.exists())

    def test_all_good_and_country_good_require_europe_health_history(self):
        lines = (
            "1.1.1.1:443#US-100ms-8.00MB/s-CN-fast-90\n"
            "2.2.2.2:443#US-120ms-8.00MB/s-CN-fast-90\n"
        )
        keys = {"1.1.1.1:443#US", "2.2.2.2:443#US"}
        healthy = {
            "samples": [1] * 6,
            "speed_sample_count": 6,
            "success_pct": 100,
            "streak": 6,
            "median_speed_mbps": 8.0,
            "speed_spread_pct": 0.1,
        }
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            country = valid / "countries" / "US"
            country.mkdir(parents=True)
            (valid / "all.txt").write_text(lines, encoding="utf-8")
            (country / "all.txt").write_text(lines, encoding="utf-8")

            bg.write_good_files(
                valid,
                keys,
                {key: {"score": 90, "risk": "low"} for key in keys},
                health_map={
                    "1.1.1.1:443#US": healthy,
                    "2.2.2.2:443#US": {**healthy, "success_pct": 80},
                },
            )

            output = (country / "good.txt").read_text(encoding="utf-8")
            self.assertEqual(len(output.splitlines()), 1)
            self.assertTrue(output.startswith("1.1.1.1:443#US-"))
            root_output = (valid / "all_good.txt").read_text(encoding="utf-8")
            self.assertEqual(root_output, output)
            self.assertEqual((valid / "all.txt").read_text(encoding="utf-8"), lines)

    def test_idempotent_rewrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            valid.mkdir(parents=True)
            (valid / "all.txt").write_text(self.POOL, encoding="utf-8")
            bg.write_good_files(
                valid, self.CHINA, self.REP, health_map=healthy_map(self.POOL)
            )
            first = (valid / "all_good.txt").read_bytes()
            mtime = (valid / "all_good.txt").stat().st_mtime_ns
            bg.write_good_files(
                valid, self.CHINA, self.REP, health_map=healthy_map(self.POOL)
            )
            self.assertEqual((valid / "all_good.txt").read_bytes(), first)
            self.assertEqual(
                (valid / "all_good.txt").stat().st_mtime_ns, mtime)

    def test_missing_quality_data_yields_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "valid"
            valid.mkdir(parents=True)
            (valid / "all.txt").write_text(self.POOL, encoding="utf-8")
            stats = bg.write_good_files(valid, set(), {})
            self.assertEqual(stats["all_good"], 0)
            # 空清单不落盘（不写 0 字节文件），避免仓库堆积空壳订阅文件
            self.assertFalse((valid / "all_good.txt").exists())

    def test_write_good_file_empty_cleans_up(self):
        """write_good_file：空清单清理残留、不写 0 字节文件；非空正常写。"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stale = root / "good.txt"
            stale.write_text("9.9.9.9:80#US\n", encoding="utf-8")
            self.assertEqual(bg.write_good_file(stale, []), 0)
            self.assertFalse(stale.exists())
            fresh = root / "good.txt"
            self.assertEqual(bg.write_good_file(fresh, []), 0)
            self.assertFalse(fresh.exists())
            real = root / "good.txt"
            self.assertEqual(
                bg.write_good_file(real, ["1.1.1.1:443#US-80ms-CN-90"]), 1)
            self.assertEqual(real.read_text(encoding="utf-8"),
                             "1.1.1.1:443#US-80ms-CN-90\n")

    def test_main_stamps_good_meta(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            (data_dir / "valid").mkdir(parents=True)
            (data_dir / "quality").mkdir(parents=True)
            rc = bg.main(["--data-dir", str(data_dir)])
            self.assertEqual(rc, 0)
            meta = json.loads(
                (data_dir / "quality" / "good_meta.json").read_text()
            )
            self.assertIn("file_count", meta)
            self.assertIn("proxy_count", meta)
            self.assertIsInstance(meta["ts"], str)  # ISO 时间戳已落盘
            self.assertGreater(len(meta["ts"]), 10)

    def test_main_rebuilds_outputs_on_reputation_coverage_collapse(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            valid = data_dir / "valid"
            quality = data_dir / "quality"
            valid.mkdir(parents=True)
            quality.mkdir(parents=True)
            lines = [
                f"10.0.0.{i}:443#US-50ms-5.00MB/s-CN-90"
                for i in range(1, 101)
            ]
            (valid / "all.txt").write_text("\n".join(lines) + "\n")
            stale = valid / "all_good.txt"
            stale.write_text("keep-me\n")
            rc = bg.main(["--data-dir", str(data_dir)])
            self.assertEqual(rc, 0)
            self.assertFalse(stale.exists())

    def test_main_rebuilds_outputs_when_quality_meta_marks_degraded_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            valid = data_dir / "valid"
            quality = data_dir / "quality"
            valid.mkdir(parents=True)
            quality.mkdir(parents=True)
            lines = [
                f"10.0.0.{i}:443#US-50ms-5.00MB/s-CN-90"
                for i in range(1, 101)
            ]
            (valid / "all.txt").write_text("\n".join(lines) + "\n")
            # The file itself looks complete, but metadata says this run could
            # not publish its partial reputation snapshot.
            (quality / "reputation.json").write_text(json.dumps({
                "proxies": {
                    f"10.0.0.{i}:443#US": {"score": 90, "risk": "low"}
                    for i in range(1, 101)
                }
            }))
            (quality / "quality_meta.json").write_text(json.dumps({
                "total": 100,
                "reputation_checked": 1,
                "reputation_degraded": True,
            }))
            stale = valid / "all_good.txt"
            stale.write_text("keep-me\n")

            rc = bg.main(["--data-dir", str(data_dir)])

            self.assertEqual(rc, 0)
            self.assertFalse(stale.exists())

    def test_main_includes_unscored_proxy_when_europe_health_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            valid = data_dir / "valid"
            quality = data_dir / "quality"
            country = valid / "countries" / "US"
            country.mkdir(parents=True)
            quality.mkdir(parents=True)
            line = "10.0.0.1:443#US-50ms-8.00MB/s\n"
            (valid / "all.txt").write_text(line, encoding="utf-8")
            (country / "all.txt").write_text(line, encoding="utf-8")
            key = "10.0.0.1:443#US"
            (quality / "china.json").write_text(json.dumps({
                "proxies": {key: {"verdict": "reachable"}}
            }), encoding="utf-8")
            (quality / "europe.json").write_text(json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "proxies": {key: {
                    "samples": [1] * 6,
                    "speed_sample_count": 6,
                    "success_pct": 100,
                    "streak": 6,
                    "median_speed_mbps": 8.0,
                    "speed_spread_pct": 0.1,
                }},
            }), encoding="utf-8")

            rc = bg.main(["--data-dir", str(data_dir)])

            self.assertEqual(rc, 0)
            self.assertTrue((valid / "all_good.txt").exists())
            self.assertEqual((country / "good.txt").read_text(), line)

    def test_main_clears_all_good_and_country_good_without_europe_health(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            valid = data_dir / "valid"
            quality = data_dir / "quality"
            country = valid / "countries" / "US"
            country.mkdir(parents=True)
            quality.mkdir(parents=True)
            line = "10.0.0.1:443#US-50ms-8.00MB/s-CN-90\n"
            (valid / "all.txt").write_text(line, encoding="utf-8")
            (country / "all.txt").write_text(line, encoding="utf-8")
            (country / "good.txt").write_text("unverified-old-node\n")
            (quality / "china.json").write_text(json.dumps({
                "proxies": {
                    "10.0.0.1:443#US": {"verdict": "reachable"}
                }
            }))
            (quality / "reputation.json").write_text(json.dumps({
                "proxies": {
                    "10.0.0.1:443#US": {"score": 90, "risk": "low"}
                }
            }))

            rc = bg.main(["--data-dir", str(data_dir)])

            self.assertEqual(rc, 0)
            self.assertFalse((valid / "all_good.txt").exists())
            self.assertFalse((country / "good.txt").exists())

    def test_main_recovers_deleted_outputs_from_inline_scores(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            valid = data_dir / "valid"
            quality = data_dir / "quality"
            valid.mkdir(parents=True)
            quality.mkdir(parents=True)
            lines = [
                f"10.0.0.{i}:443#US-50ms-5.00MB/s-CN-90"
                for i in range(1, 101)
            ]
            (valid / "all.txt").write_text("\n".join(lines) + "\n")
            (quality / "china.json").write_text(json.dumps({
                "proxies": {
                    f"10.0.0.{i}:443#US": {"verdict": "reachable"}
                    for i in range(1, 101)
                }
            }))
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            (quality / "europe.json").write_text(json.dumps({
                "ts": now,
                "proxies": {
                    f"10.0.0.{i}:443#US": {
                        "samples": [1] * 6,
                        "speed_sample_count": 6,
                        "success_pct": 100,
                        "streak": 6,
                        "median_speed_mbps": 5.0,
                        "speed_spread_pct": 0.1,
                    }
                    for i in range(1, 101)
                },
            }))
            rc = bg.main(["--data-dir", str(data_dir)])
            self.assertEqual(rc, 0)
            good = valid / "all_good.txt"
            self.assertTrue(good.exists())
            self.assertEqual(len(good.read_text().splitlines()), 100)


class TestCommittedCnViewInvariant(unittest.TestCase):
    """数据护栏：good 输出保留欧洲实测速率，CN 专用清单仍保持 CN 视图。

    覆盖 CN 专用清单表面（只允许 ``≈XMB/s`` 估算或无速度 token）：

    - ``premium`` 家族（build_premium 及其全部变体）；
    - ``all_cn*.txt``（china_check 的大陆清单）。
    - ``countries/*/cn*.txt`` 与 ``sets/*/cn*.txt``（R299 补入：此前
      仅前三块有锁，子目录 CN 视图同类泄漏将无声入库；实证零泄漏）。

    曾出现陈旧清单把海外实测 ``-XMB/s`` 直接提交进 per-country/set 的
    good/premium 文件（大陆用户误读为大陆速度）。此测试在 CI 里直接扫描
    仓库内已提交数据，出现任何纯 ``-XMB/s`` 即失败，防止问题复发。
    """

    ROOT = Path(__file__).resolve().parent.parent
    _PLAIN_SPEED = re.compile(r"-\d+(?:\.\d+)?MB/s")

    def _cn_view_files(self):
        valid = self.ROOT / "data" / "valid"
        if not valid.is_dir():
            return []
        out = []
        for path in valid.rglob("*.txt"):
            if path.name.startswith("premium"):
                out.append(path)
                continue
            # 根级 flatten 家族（all_good*.txt / all_premium*.txt）——以 good/
            # premium 前缀命名的判断抓不到它们，须显式纳入，否则护栏对
            # 主清单文件失联
            if path.name.startswith("all_premium"):
                out.append(path)
                continue
            if path.name.startswith("all_cn"):
                out.append(path)
                continue
            # 子目录 CN 视图（countries/*/cn*.txt、sets/*/cn*.txt）同属
            # CN 视图语义（≈XMB/s 或无速度），R299 纳入护栏。按目录位置
            # 判断，避免把 tiers/*/sets/cn_common.txt 这类合法 good 集合
            # 镜像误当成 CN 视图。
            relative_parts = path.relative_to(valid).parts
            if (
                len(relative_parts) >= 3
                and relative_parts[0] in ("countries", "sets")
                and path.name.startswith("cn")
            ):
                out.append(path)
                continue
            if "tiers" in path.parts and "premium" in path.name:
                out.append(path)
        return out

    @unittest.skipUnless(
        (Path(__file__).resolve().parent.parent / "data" / "valid").is_dir(),
        "repo data dir not present",
    )
    def test_no_plain_overseas_speed_in_good_premium(self):
        offenders = []
        total = 0
        for path in self._cn_view_files():
            for line in path.read_text(encoding="utf-8").splitlines():
                total += 1
                if self._PLAIN_SPEED.search(line):
                    offenders.append((str(path.relative_to(self.ROOT)), line))
        if offenders:
            first = offenders[0]
            self.fail(
                f"CN 专用视图混入 {len(offenders)} 条海外实测速度（应为 ≈ 估算）："
                f"{first[0]} {first[1][:80]}"
            )
        # 全家族非空（含关键基准文件），防止护栏失联
        self.assertGreater(total, 0)


if __name__ == "__main__":
    unittest.main()
