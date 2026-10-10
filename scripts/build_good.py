#!/usr/bin/env python3
"""Build comprehensive-best (综合最优) ``good.txt`` lists from quality data.

Filters the annotated valid pools to proxies that satisfy Europe-runner policy.
Every good list requires stable and fast history:
   at least six recent speed samples, >=90% qualified runs, median >=2 MB/s,
   and interquartile speed spread <=50%

Reputation is optional ranking metadata only. It never rejects a line. When
present it contributes to a composite ranking score::

    score = round(0.6 * rep + 0.2 * latency_score + 0.2 * speed_score)

When reputation is absent, the remaining latency and speed weights are
renormalized to 50% each, so unknown reputation is neither a penalty nor a
bonus. China reachability is deliberately not a good-list requirement.
where ``latency_score`` maps <=100ms to 100 and >=1500ms to 0 linearly
(missing latency counts 0), and ``speed_score = min(MB/s / 2, 1) * 100``
(missing speed counts 0). Ties break by latency asc then key asc.

Latency and speed are the Europe-runner measurements retained in each input
line.

Good outputs retain the Europe-runner latency and speed annotations verbatim;
legacy CN-view helpers remain available for older consumers.

Outputs are the Europe-viewed annotated lines:

- ``data/valid/all_good.txt``            (global policy group)
- ``data/valid/countries/<CC>/good.txt`` (per-country groups)
- ``data/valid/sets/<name>/good.txt``    (country-set groups)
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (
    _rewrite_cn_speed,
    CHINA_FILE,
    DATA_DIR,
    EXIT_FAMILY_FILE,
    LATENCY_RE,
    REPUTATION_FILE,
    SPEED_RE,
    cn_fastest_ms,
    line_to_key,
    load_china_stable_keys,
    load_speed_keys,
    load_uptime_keys,
    note_score,
    note_tier,
    parse_line,
    read_json,
    rewrite_latency,
    write_json,
    write_text_if_changed,
)

LATENCY_BEST_MS = 100
LATENCY_WORST_MS = 1500
SPEED_FULL_MBPS = 2.0

MIN_REP_SCORE = 80
HEALTHY_GOOD_MIN_REP_SCORE = 85
HEALTH_MIN_SAMPLES = 6
HEALTH_MIN_SUCCESS_PCT = 90
HEALTH_MIN_STREAK = 2
HEALTH_MIN_SPEED_MBPS = 2.0
HEALTH_MAX_SPEED_SPREAD = 0.5
HEALTH_MAX_AGE_HOURS = 8

WEIGHT_REP = 0.6
WEIGHT_LATENCY = 0.2
WEIGHT_SPEED = 0.2


def parse_metrics(line: str) -> tuple[int | None, float | None]:
    """Extract ``(latency_ms, speed_mbps)`` from an annotated line.

    ``≈XMB/s``（大陆估算 token）不算实测速度——mbps 返回 ``None``，
    不进入综合分 speed 轴（与 ``_line_mbps``/``speed_tier``/``_parse_speed``
    同族 ≈ 拒绝，见 R77/R78）。当前生产输入均为 all.txt 系（无 ≈），
    此为防御性收紧。
    """
    lat_match = LATENCY_RE.search(line)
    speed_match = SPEED_RE.search(line)
    ms = int(lat_match.group(1)) if lat_match else None
    if speed_match is None:
        return ms, None
    if speed_match.start() > 0 and line[speed_match.start() - 1] == "≈":
        return ms, None
    return ms, float(speed_match.group(1))


def latency_score(ms: int | None) -> float:
    """Linear map: <=100ms -> 100, >=1500ms -> 0; missing -> 0."""
    if ms is None:
        return 0.0
    if ms <= LATENCY_BEST_MS:
        return 100.0
    if ms >= LATENCY_WORST_MS:
        return 0.0
    span = LATENCY_WORST_MS - LATENCY_BEST_MS
    return (LATENCY_WORST_MS - ms) / span * 100.0


def speed_score(mbps: float | None) -> float:
    """``min(mbps / 2, 1) * 100``; missing -> 0."""
    if mbps is None:
        return 0.0
    return min(mbps / SPEED_FULL_MBPS, 1.0) * 100.0


def composite_score(rep: int | None, ms: int | None, mbps: float | None) -> int:
    """Rank by rep/latency/speed, renormalizing when reputation is absent."""
    if rep is None:
        remaining_weight = WEIGHT_LATENCY + WEIGHT_SPEED
        if remaining_weight <= 0:
            return 0
        return round(
            (WEIGHT_LATENCY * latency_score(ms)
             + WEIGHT_SPEED * speed_score(mbps))
            / remaining_weight
        )
    return round(
        WEIGHT_REP * rep
        + WEIGHT_LATENCY * latency_score(ms)
        + WEIGHT_SPEED * speed_score(mbps)
    )


def build_rep_map(data: dict) -> dict[str, dict]:
    """``reputation.json`` -> per-key score/risk records, including risk-only."""
    result: dict[str, dict] = {}
    for key, entry in data.get("proxies", {}).items():
        if not isinstance(entry, dict):
            continue
        score = entry.get("score")
        risk = entry.get("risk", "")
        if score is None and not risk:
            continue
        try:
            parsed_score = int(score) if score is not None else None
        except (TypeError, ValueError):
            parsed_score = None
        result[key] = {"score": parsed_score, "risk": risk}
    return result


def build_inline_rep_map(text: str) -> dict[str, dict]:
    """Build a last-known reputation map from validated-list annotations.

    ``annotate_classify.py`` deliberately retains an existing score token when
    a later reputation run has no result for that key.  This makes the token a
    useful cache during a provider outage.  Fresh ``reputation.json`` entries
    remain authoritative and overwrite these fallback records in
    :func:`merge_rep_maps`.
    """
    result: dict[str, dict] = {}
    for line in text.splitlines():
        key = line_to_key(line)
        score = note_score(line)
        if not key or score is None:
            continue
        result[key] = {
            "score": score,
            "risk": "high" if score < 30 else ("medium" if score < 75 else "low"),
            "source": "inline-cache",
        }
    return result


def merge_rep_maps(
    inline_map: dict[str, dict], current_map: dict[str, dict]
) -> dict[str, dict]:
    """Merge maps, keeping inline score fallback beside current risk data."""
    merged = dict(inline_map)
    for key, current in current_map.items():
        previous = merged.get(key) or {}
        merged[key] = {**previous, **current}
        if current.get("score") is None and previous.get("score") is not None:
            merged[key]["score"] = previous["score"]
    return merged


def build_china_set(data: dict) -> set[str]:
    """``china.json`` -> 当期全可达集。

    CN good-tier 与 all_cn.txt 口径一致：清单保持完整（全可达集，≥1 万），
    不按延迟门槛精简；延迟/速度语义交给 cn_fastest_ms —— 每行展示大陆视角
    实测读数而非海外 TLS 值。
    """
    result: set[str] = set()
    for key, entry in data.get("proxies", {}).items():
        if isinstance(entry, dict) and entry.get("verdict") == "reachable":
            result.add(key)
    return result


def build_cn_ms_map(data: dict) -> dict[str, float]:
    """``china.json`` -> ``{key: 大陆实测 ms}``（最快运营商优先，同 all_cn.txt）。"""
    result: dict[str, float] = {}
    for key, entry in data.get("proxies", {}).items():
        ms = cn_fastest_ms(entry)
        if ms is not None:
            result[key] = ms
    return result


def build_health_map(data: dict) -> dict[str, dict]:
    """Return rolling runner measurements keyed by normalized proxy key."""
    result: dict[str, dict] = {}
    for key, entry in data.get("proxies", {}).items():
        if isinstance(entry, dict):
            result[key] = entry
    return result


def is_healthy(key: str, health_map: dict[str, dict] | None) -> bool:
    """Require mature, reliable, fast and low-variance rolling measurements."""
    if health_map is None:
        return True
    health = health_map.get(key)
    if not isinstance(health, dict):
        return False
    samples = health.get("samples")
    try:
        success_pct = float(health.get("success_pct") or 0)
        streak = int(health.get("streak") or 0)
        speed_sample_count = int(health.get("speed_sample_count") or 0)
        median_speed = float(health.get("median_speed_mbps") or 0)
        spread = float(health["speed_spread_pct"])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(
        isinstance(samples, list)
        and len(samples) >= HEALTH_MIN_SAMPLES
        and speed_sample_count >= HEALTH_MIN_SAMPLES
        and success_pct >= HEALTH_MIN_SUCCESS_PCT
        and streak >= HEALTH_MIN_STREAK
        and median_speed >= HEALTH_MIN_SPEED_MBPS
        and spread <= HEALTH_MAX_SPEED_SPREAD
    )


def health_history_fresh(data: dict, *, now: datetime | None = None) -> bool:
    """Reject absent or stale health snapshots before they can rewrite good."""
    stamp = data.get("ts") if isinstance(data, dict) else None
    if not isinstance(stamp, str):
        return False
    try:
        checked_at = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return False
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    age = now - checked_at.astimezone(timezone.utc)
    return timedelta(0) <= age <= timedelta(hours=HEALTH_MAX_AGE_HOURS)


def is_cn_reachable(key: str | None, line: str, china_set: set[str]) -> bool:
    """CN-reachable per repo convention: judged ``reachable`` this run only.

    不在当期 `china.json` 可达集内的行即使历史带 ``-CN`` 也不收——
    与 all_cn.txt 同策略（过期 -CN 不再兜底），消除失效标志残留。
    参数 ``line`` 保留以兼容调用方签名。
    """
    return key in china_set


def to_cn_view(lines: list[str], cn_ms: dict | None) -> list[str]:
    """CN 视图：行内 ms 换成大陆实测（可信探测优先），速度换 ``≈XMB/s`` 估算。

    与 all_cn.txt 同口径（common.cn_fastest_ms / _rewrite_cn_speed），
    保证"每个 CN 文件用大陆延迟和速度"；无读数键速度 token 删除而非冒充。
    ``cn_ms`` 为空时原样返回。
    """
    if not cn_ms:
        return lines
    return [
        _rewrite_cn_speed(rewrite_latency(ln, cn_ms.get(k)), cn_ms)
        if (k := line_to_key(ln)) else ln
        for ln in lines
    ]


def filter_rank(
    text: str,
    china_set: set[str],
    rep_map: dict[str, dict],
    cn_ms: dict[str, float] | None = None,
    health_map: dict[str, dict] | None = None,
    min_rep_score: int = MIN_REP_SCORE,
) -> list[str]:
    """Filter pool lines by entry criteria and rank by composite score.

    Lines failing the criteria are dropped; survivors keep their annotated
    form verbatim, ordered by ``(score desc, latency asc, key asc)``.
    Latency uses the Europe-runner TLS latency parsed from each line.
    """
    ranked: list[tuple[int, int, str, str]] = []
    for line in text.splitlines():
        if not line:
            continue
        key = line_to_key(line)
        if not key:
            continue
        if not is_healthy(key, health_map):
            continue
        rep = rep_map.get(key)
        rep_score = rep.get("score") if rep else None
        overseas_ms, mbps = parse_metrics(line)
        ms = overseas_ms
        score = composite_score(rep_score, ms, mbps)
        ranked.append((score, ms if ms is not None else LATENCY_WORST_MS, key, line))
    ranked.sort(key=lambda item: (-item[0], item[1], item[2]))
    return [line for _s, _ms, _k, line in ranked]


TIER_TOKENS = ("fast", "mid", "slow")


def _line_mbps(line: str) -> float:
    m = SPEED_RE.search(line)
    if not m:
        return -1.0
    if m.start() > 0 and line[m.start() - 1] == "≈":
        return -1.0
    return float(m.group(1))


def top_slice(lines: list[str], frac: float = 0.25,
              min_samples: int = 8) -> tuple[list[str], float | None]:
    """按**组内**实测速度取前 ``frac`` 分位的行，返回 ``(行列表, 阈值MB/s)``。

    动机：不同国家/机房供给差距巨大，全局档位（≥5MB/s=fast）会让弱供给
    国家全军覆没——某国最快的线也可能被标 slow。``good_top.txt`` 保证每个
    组都暴露自己的最快一档（相对最优），与绝对档位互补。
    有效速度样本少于 ``min_samples`` 时视为无统计意义，返回空。
    """
    vals = sorted(v for v in map(_line_mbps, lines) if v >= 0)
    if len(vals) < min_samples:
        return [], None
    thr = vals[max(0, int(len(vals) * (1 - frac)) - 1)]
    picked = [ln for ln in lines if _line_mbps(ln) >= thr]
    return picked, thr


def write_good_file(path: Path, lines: list[str]) -> int:
    """Write ``lines`` to ``path``; empty list cleans up instead of leaving a
    0-byte file (consistent with "空清单不落盘并清理残留")."""
    if not lines:
        path.unlink(missing_ok=True)
        return 0
    write_text_if_changed(path, "\n".join(lines) + "\n")
    return len(lines)


def exit_identity(
    key: str, family_proxies: dict
) -> str:
    """出口身份：实测出口 IP 优先（exit_v4/exit_v6），否则入口 /24。

    CF 中转代理的入口恒为 CF 边缘——同农场节点往往共享 /24，用入口
    网段兜底分组仍能聚合同源节点；有实测出口时按出口精确去重。
    """
    fam = family_proxies.get(key, {}) or {}
    ident = fam.get("exit_v4") or fam.get("exit_v6")
    if isinstance(ident, str) and ident:
        return f"exit/{ident}"
    ip = key.split("#", 1)[0].rsplit(":", 1)[0]
    parts = ip.split(".")
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        return f"ip24/{'.'.join(parts[:3])}.0/24"
    return f"ip/{ip}"


def build_diverse_lines(
    pool_text: str, family_proxies: dict, rep_map: dict
) -> list[str]:
    """每出口身份只保留综合分最高的一条，返回按分数降序的行列表。

    避免整池被同一农场/同一出口连坐（shared_exit 聚簇）：消费方拿到
    ``all_diverse.txt`` 即得到出口层面互不重复的最大覆盖组合。
    """
    groups: dict[str, list[tuple[int, str]]] = {}
    for ln in pool_text.splitlines():
        parsed = parse_line(ln)
        if not parsed:
            continue
        key = parsed[0]
        rep = rep_map.get(key, {})
        ms, mbps = parse_metrics(ln)
        score = composite_score(int(rep.get("score") or 0), ms, mbps)
        ident = exit_identity(key, family_proxies)
        groups.setdefault(ident, []).append((score, ln))
    ranked: list[tuple[int, str]] = []
    for cands in groups.values():
        best = max(cands, key=lambda t: (t[0], t[1]))
        ranked.append(best)
    ranked.sort(key=lambda t: -t[0])
    return [ln for _s, ln in ranked]


def write_good_files(
    valid_dir: Path,
    china_set: set[str],
    rep_map: dict[str, dict],
    cn_ms: dict[str, float] | None = None,
    health_map: dict[str, dict] | None = None,
) -> dict[str, int]:
    """Write all_good.txt + per-country/set good.txt; return per-file counts.

    每份 good 清单同步产出：

    - ``_verified``（speed.json 全链路验证）与 ``_stable``（china.json
      streak≥2 且 flip≤1 跨轮稳定）可靠性变体；
    - ``good_<tier>.txt`` 速度档变体（fast/mid/slow，来自行备注档位 token）
      ——不同国家实测速度天然分层，档位文件让消费者按带宽需求直达；
    - 同内容镜像进细分目录 ``data/valid/tiers/<tier>/``：
      全局组写 ``all.txt``，国家组写 ``<CC>.txt``，集合组写
      ``sets/<name>.txt``——目录导航式消费入口（空档位整目录跳过）。

    对每目录 ``ltd.txt`` 限量池额外产出 ``good_ltd``（每国最快的优质子集）：
    同套 good 标准在该池上筛选（欧洲健康达标），
    派生 ``_verified`` 与 ``_stable`` 变体，空清单不落盘并清理上轮残留。
    """
    # ``all*.txt`` are unrestricted candidate pools. Every derived ``good``
    # output requires a mature Europe history, including root all_good files.
    health_map = health_map or {}
    stats: dict[str, int] = {}
    speed_keys = load_speed_keys()
    stable_keys = load_china_stable_keys()
    uptime_keys = load_uptime_keys()
    tier_lines: dict[str, list[tuple[str, Path]]] = {t: [] for t in TIER_TOKENS}

    def emit(base: Path, lines: list[str], tier_name: str | None = None) -> int:
        raw = lines
        n = write_good_file(base, lines)
        for suffix, keys in (
            ("_verified", speed_keys),
            ("_stable", stable_keys),
            ("_uptime", uptime_keys),
        ):
            vpath = base.with_name(f"{base.stem}{suffix}.txt")
            vlines = [ln for ln in lines if (k := line_to_key(ln)) and k in keys]
            if vlines:
                write_text_if_changed(vpath, "\n".join(vlines) + "\n")
            elif vpath.exists():
                vpath.unlink()
        # 组内相对最优：good_top.txt（前 25% 分位），按欧洲实测速率遴选。
        tlines, thr = top_slice(raw)
        tpath = base.with_name(f"{base.stem}_top.txt")
        if tlines:
            write_text_if_changed(
                tpath, "\n".join(tlines) + "\n"
            )
        elif tpath.exists():
            tpath.unlink()
        if tier_name is not None and tlines:
            stats[f"top:{tier_name}"] = len(tlines)
        # 速度档变体：<base_stem>_<tier>.txt（与所在目录同级的扁平入口）
        for tier in TIER_TOKENS:
            tlines = [ln for ln in lines if note_tier(ln) == tier]
            tpath = base.with_name(f"{base.stem}_{tier}.txt")
            if tlines:
                write_text_if_changed(tpath, "\n".join(tlines) + "\n")
            elif tpath.exists():
                tpath.unlink()
            if tier_name is not None:
                tier_lines[tier].append(("\n".join(tlines) + "\n" if tlines else "",
                                         tier_name))
        return n

    all_pool = valid_dir / "all.txt"
    if all_pool.exists():
        stats["all_good"] = emit(
            valid_dir / "all_good.txt",
            filter_rank(
                all_pool.read_text(encoding="utf-8"),
                china_set,
                rep_map,
                health_map=health_map,
            ),
            tier_name="all",
        )

    # good_ltd：对同目录 ltd.txt 限量池按同套标准筛出每国最快的优质子集
    def emit_ltd(base: Path, lines: list[str]) -> int:
        n = write_good_file(base, lines) if lines else 0
        if not lines:
            base.unlink(missing_ok=True)
        for suffix, keys in (
            ("_verified", speed_keys),
            ("_stable", stable_keys),
        ):
            vpath = base.with_name(f"{base.stem}{suffix}.txt")
            vlines = [ln for ln in lines if (k := line_to_key(ln)) and k in keys]
            if vlines:
                write_text_if_changed(vpath, "\n".join(vlines) + "\n")
            elif vpath.exists():
                vpath.unlink()
        return n

    def rank_ltd(pool: Path) -> list[str]:
        if not pool.exists():
            return []
        return filter_rank(
            pool.read_text(encoding="utf-8"), china_set, rep_map,
            health_map=health_map,
        )

    stats["all_good_ltd"] = emit_ltd(
        valid_dir / "all_good_ltd.txt",
        rank_ltd(valid_dir / "all_ltd.txt"),
    )

    for sub in ("countries", "sets"):
        root = valid_dir / sub
        if not root.is_dir():
            continue
        for group_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            name = f"{sub}/{group_dir.name}"
            rel = (f"sets/{group_dir.name}" if sub == "sets"
                   else f"{group_dir.name}")
            pool = group_dir / "all.txt"
            if pool.exists():
                stats[name] = emit(
                    group_dir / "good.txt",
                    filter_rank(
                        pool.read_text(encoding="utf-8"), china_set, rep_map,
                        health_map=health_map,
                    ),
                    tier_name=rel,
                )
            stats[f"{sub}/{group_dir.name}_ltd"] = emit_ltd(
                group_dir / "good_ltd.txt", rank_ltd(group_dir / "ltd.txt")
            )

    # 细分目录：tiers/<tier>/{all.txt,<CC>.txt,sets/<name>.txt}
    # 先清理陈旧产物（组消失/档位清空后残留），再写当前代内容

    tiers_root = valid_dir / "tiers"
    for tier, parts in tier_lines.items():
        content_by_rel = {rel: body for body, rel in parts if body}
        tdir = tiers_root / tier
        if tdir.is_dir():
            keep = {f"{rel}.txt" for rel in content_by_rel}
            for old in tdir.rglob("*.txt"):
                if old.relative_to(tdir).as_posix() not in keep:
                    old.unlink()
            for dead in sorted(tdir.rglob("*"), reverse=True):
                if dead.is_dir() and not any(dead.iterdir()):
                    dead.rmdir()
            if not any(tdir.iterdir()):
                tdir.rmdir()
        if not content_by_rel:
            continue
        total = sum(body.count("\n") for body in content_by_rel.values())
        stats[f"tiers/{tier}"] = total
        for rel, body in content_by_rel.items():
            write_text_if_changed(tiers_root / tier / f"{rel}.txt", body)
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help="data/ root (default: repo-root/data)",
    )
    args = ap.parse_args(argv)
    valid_dir = args.data_dir / "valid"
    quality_dir = args.data_dir / "quality"

    china_set = build_china_set(read_json(quality_dir / CHINA_FILE.name))
    cn_ms = build_cn_ms_map(read_json(quality_dir / CHINA_FILE.name))
    current_rep_map = build_rep_map(
        read_json(quality_dir / REPUTATION_FILE.name)
    )
    health_data = read_json(quality_dir / "europe.json")
    health_map = build_health_map(health_data)
    if not health_history_fresh(health_data):
        print(
            "Europe health history is missing or stale; all good lists "
            "will require new qualifying samples",
            file=sys.stderr,
        )
        health_map = {}
    all_pool = valid_dir / "all.txt"
    all_pool_text = (
        all_pool.read_text(encoding="utf-8") if all_pool.exists() else ""
    )
    inline_rep_map = build_inline_rep_map(all_pool_text)
    rep_map = merge_rep_maps(inline_rep_map, current_rep_map)
    print(
        f"Maps: cn={len(china_set)} cn_ms={len(cn_ms)} "
        f"rep={len(rep_map)} (current={len(current_rep_map)} "
        f"inline-fallback={len(set(inline_rep_map) - set(current_rep_map))})"
    )

    stats = write_good_files(valid_dir, china_set, rep_map, cn_ms, health_map)

    # 出口多样性视图：每出口身份一条，按综合分降序
    if all_pool.exists():
        family_proxies = read_json(
            quality_dir / EXIT_FAMILY_FILE.name
        ).get("proxies", {})
        diverse = build_diverse_lines(
            all_pool_text, family_proxies, rep_map
        )
        stats["all_diverse"] = write_good_file(valid_dir / "all_diverse.txt", diverse)
    # proxy_count = 全部 good 清单（全局/国家/集合 × 各可靠性/档位变体 × ltd）
    # 的行数合计——同一节点会同时出现在多种视图里（非去重节点数）。
    # 仅作状态/时效展示，无下游消费其数值（health_alert 只看文件龄）。
    total = sum(stats.values())
    for name in sorted(stats):
        print(f"  {name}.txt: {stats[name]}")
    write_json(
        quality_dir / "good_meta.json",
        {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "file_count": len(stats),
            "proxy_count": total,
        },
    )
    print(f"Done: {len(stats)} files, {total} proxies")
    return 0


if __name__ == "__main__":
    sys.exit(main())
