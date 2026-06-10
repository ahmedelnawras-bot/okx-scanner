"""
Capital Intelligence Layer - unified dashboard v1.6

Builds a single shadow-only dashboard from:
- CapitalAuctionResult / capital_report summary
- optional Symbol Memory summaries
- optional Capital Regret summaries

Safety:
- No trading side effects.
- Does NOT place orders.
- Does NOT reject trades.
- Does NOT touch OKX, TP/SL, Recovery, BLOCK, or main.py.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

try:
    from .capital_report import build_capital_summary
except Exception:  # allows direct script import during local testing
    from capital_report import build_capital_summary


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _fmt(value: object, digits: int = 2) -> str:
    return f"{_safe_float(value, 0.0):.{digits}f}"


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "to_dict"):
        try:
            out = value.to_dict()
            return dict(out or {}) if isinstance(out, dict) else {}
        except Exception:
            return {}
    return {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple) or isinstance(value, set):
        return list(value)
    return [value]


def _extract_rows(auction_result: Any) -> list[dict[str, Any]]:
    if auction_result is None:
        return []
    if isinstance(auction_result, dict):
        rows = auction_result.get("bids") or []
        return [dict(row or {}) for row in rows if isinstance(row, dict)]
    bids = getattr(auction_result, "bids", []) or []
    rows: list[dict[str, Any]] = []
    for bid in bids:
        if hasattr(bid, "to_dict"):
            try:
                row = bid.to_dict()
                if isinstance(row, dict):
                    rows.append(dict(row))
            except Exception:
                continue
        elif isinstance(bid, dict):
            rows.append(dict(bid))
    return rows


def _component_map(row: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    components = row.get("components") or []
    if isinstance(components, dict):
        for name, value in components.items():
            if isinstance(value, dict):
                out[str(name)] = _safe_float(value.get("points"), 0.0)
            else:
                out[str(name)] = _safe_float(value, 0.0)
        return out
    for comp in _as_list(components):
        if not isinstance(comp, dict):
            continue
        name = str(comp.get("name") or "").strip()
        if not name:
            continue
        out[name] = _safe_float(comp.get("points"), 0.0)
    return out


def _top_driver_names(row: dict[str, Any], limit: int = 3) -> list[str]:
    comps = _component_map(row)
    ranked = sorted(comps.items(), key=lambda item: item[1], reverse=True)
    drivers: list[str] = []
    for name, points in ranked:
        if points <= 0:
            continue
        clean = name.replace("_", " ").title()
        drivers.append(f"{clean} {points:.1f}")
        if len(drivers) >= max(1, int(limit or 3)):
            break
    return drivers


def _component_averages(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        for name, points in _component_map(row).items():
            buckets[name].append(float(points or 0.0))
    out: list[dict[str, Any]] = []
    for name, values in buckets.items():
        out.append({
            "name": name,
            "count": len(values),
            "avg_points": round(sum(values) / max(1, len(values)), 2),
            "max_points": round(max(values), 2) if values else 0.0,
        })
    out.sort(key=lambda item: (item["avg_points"], item["count"]), reverse=True)
    return out


def _confidence_grade(avg_bid: float, a_plus_count: int, a_count: int, total: int) -> tuple[str, int, str]:
    total = max(0, int(total or 0))
    if total <= 0:
        return "NO_DATA", 0, "no_candidates"
    quality_ratio = (float(a_plus_count + a_count) / max(1.0, float(total))) * 100.0
    score = 0.0
    score += min(60.0, max(0.0, (float(avg_bid) / 100.0) * 60.0))
    score += min(40.0, quality_ratio * 0.70)
    score_int = int(round(max(0.0, min(100.0, score))))
    if score_int >= 85:
        return "HIGH", score_int, "strong_bid_quality"
    if score_int >= 70:
        return "GOOD", score_int, "healthy_bid_quality"
    if score_int >= 55:
        return "MEDIUM", score_int, "mixed_bid_quality"
    return "LOW", score_int, "weak_or_sparse_bid_quality"


def _normalize_memory_rows(symbol_memory: Any, limit: int = 8) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if symbol_memory is None:
        return rows

    if isinstance(symbol_memory, dict):
        # Supports either {symbol: stats} or {"symbols": [...]} shapes.
        if isinstance(symbol_memory.get("symbols"), list):
            source = symbol_memory.get("symbols") or []
        elif isinstance(symbol_memory.get("rows"), list):
            source = symbol_memory.get("rows") or []
        else:
            source = []
            for symbol, stats in symbol_memory.items():
                if isinstance(stats, dict):
                    row = dict(stats)
                    row.setdefault("symbol", symbol)
                    source.append(row)
    else:
        source = _as_list(symbol_memory)

    for item in source:
        if hasattr(item, "to_dict"):
            item = item.to_dict()
        if isinstance(item, dict):
            rows.append(dict(item))

    def _memory_rank(row: dict[str, Any]) -> float:
        return (
            _safe_float(row.get("symbol_memory_points") or row.get("memory_points") or row.get("bonus"), 0.0) * 10.0
            + _safe_float(row.get("win_rate") or row.get("win_rate_pct"), 0.0)
            + _safe_float(row.get("tp2_rate") or row.get("tp2_rate_pct"), 0.0) * 0.50
            - _safe_float(row.get("sl_rate") or row.get("sl_rate_pct"), 0.0) * 0.35
        )

    rows.sort(key=_memory_rank, reverse=True)
    return rows[:max(1, int(limit or 8))]


def _normalize_regret_summary(regret_summary: Any) -> dict[str, Any]:
    row = _as_dict(regret_summary)
    if not row:
        return {}
    # Keep flexible because capital_regret.py may evolve.
    decisions = _safe_int(row.get("decisions") or row.get("total_decisions") or row.get("total"), 0)
    false_rejections = _safe_int(row.get("false_rejections") or row.get("false_rejection_count"), 0)
    false_acceptance = _safe_int(row.get("false_acceptance") or row.get("false_acceptances") or row.get("false_acceptance_count"), 0)
    correct = _safe_int(row.get("correct") or row.get("correct_decisions"), 0)
    accuracy = _safe_float(row.get("accuracy") or row.get("accuracy_pct"), 0.0)
    if accuracy <= 0 and decisions > 0:
        accuracy = (correct / max(1, decisions)) * 100.0
    return {
        **row,
        "decisions": decisions,
        "false_rejections": false_rejections,
        "false_acceptance": false_acceptance,
        "correct": correct,
        "accuracy_pct": round(accuracy, 2),
    }


def build_capital_dashboard_summary(
    auction_result: Any = None,
    symbol_memory: Any = None,
    regret_summary: Any = None,
    *,
    top_n: int = 8,
) -> dict[str, Any]:
    """Build one JSON-like dashboard summary for the Capital Brain.

    This is report-only and safe to call from tests, scripts, or future main.py
    shadow integration.
    """
    scan_summary = build_capital_summary(auction_result, top_n=top_n)
    rows = _extract_rows(auction_result)
    classes = dict(scan_summary.get("classes") or {}) if isinstance(scan_summary, dict) else {}
    avg_bid = _safe_float(scan_summary.get("average_bid") if isinstance(scan_summary, dict) else 0.0, 0.0)
    total = _safe_int(scan_summary.get("candidates") if isinstance(scan_summary, dict) else len(rows), len(rows))
    a_plus = _safe_int(classes.get("A+"), 0)
    a_count = _safe_int(classes.get("A"), 0)
    conf_label, conf_score, conf_reason = _confidence_grade(avg_bid, a_plus, a_count, total)

    memory_rows = _normalize_memory_rows(symbol_memory, limit=top_n)
    regret = _normalize_regret_summary(regret_summary)
    components = _component_averages(rows)

    return {
        "ok": True,
        "model": "capital_dashboard_v1_6_shadow",
        "mode": "shadow",
        "scan": scan_summary,
        "component_averages": components,
        "confidence": {
            "label": conf_label,
            "score": conf_score,
            "reason": conf_reason,
            "a_plus_count": a_plus,
            "a_count": a_count,
            "quality_count": a_plus + a_count,
            "total_candidates": total,
        },
        "symbol_memory_top": memory_rows,
        "regret": regret,
        "safety": {
            "shadow_only": True,
            "execution_impact": False,
            "main_py_required": False,
        },
    }


def build_capital_dashboard_text(
    auction_result: Any = None,
    symbol_memory: Any = None,
    regret_summary: Any = None,
    *,
    top_n: int = 8,
) -> str:
    summary = build_capital_dashboard_summary(
        auction_result=auction_result,
        symbol_memory=symbol_memory,
        regret_summary=regret_summary,
        top_n=top_n,
    )
    scan = dict(summary.get("scan") or {})
    confidence = dict(summary.get("confidence") or {})
    classes = dict(scan.get("classes") or {})

    lines: list[str] = [
        "🧠 Capital Intelligence Dashboard — V1.6",
        "━━━━━━━━━━━━",
        f"Mode: <b>SHADOW</b>",
        f"Confidence: <b>{confidence.get('label', 'NO_DATA')}</b> | Score: <code>{confidence.get('score', 0)}/100</code>",
        f"Reason: <code>{confidence.get('reason', '-')}</code>",
        "",
        "💰 Current Bid Quality",
        f"Candidates: <code>{_safe_int(scan.get('candidates'), 0)}</code>",
        f"Strategy: <code>{_safe_int(scan.get('strategy_candidates'), 0)}</code> | Operational/Recovered: <code>{_safe_int(scan.get('operational_candidates'), 0)}</code>",
        f"Average Bid: <code>{_fmt(scan.get('average_bid'), 2)}</code>",
        "Classes: " + (", ".join(f"{k}={v}" for k, v in sorted(classes.items())) if classes else "-"),
    ]

    top_rows = _as_list(scan.get("top") or [])[:max(1, int(top_n or 8))]
    if top_rows:
        lines.extend(["", "🏆 Best Opportunities"])
        for row in top_rows:
            if not isinstance(row, dict):
                continue
            rank = _safe_int(row.get("rank"), 0)
            selected = "✅" if row.get("advisory_selected") else "⏳"
            symbol = str(row.get("symbol") or "-")
            setup = str(row.get("setup_type") or "-")
            bid = _fmt(row.get("bid_score"), 2)
            cls = str(row.get("trade_class") or "C")
            drivers = _top_driver_names(row, limit=3)
            driver_text = " | ".join(drivers) if drivers else "drivers unavailable"
            lines.append(f"{rank}. {selected} <b>{symbol}</b> | {setup} | Bid <code>{bid}</code> | {cls}")
            lines.append(f"   ↳ {driver_text}")

    setup_summary = _as_list(scan.get("setup_summary") or [])
    if setup_summary:
        lines.extend(["", "🔥 Best Setups by Avg Bid"])
        for item in setup_summary[:5]:
            if isinstance(item, dict):
                lines.append(f"• {item.get('setup_type', '-')}: avg <code>{item.get('avg_bid', 0)}</code> | n={item.get('count', 0)}")

    components = _as_list(summary.get("component_averages") or [])
    if components:
        lines.extend(["", "🧩 Component Averages"])
        for item in components[:7]:
            if isinstance(item, dict):
                name = str(item.get("name") or "-").replace("_", " ").title()
                lines.append(f"• {name}: avg <code>{_fmt(item.get('avg_points'), 2)}</code> | max <code>{_fmt(item.get('max_points'), 2)}</code>")

    memory_rows = _as_list(summary.get("symbol_memory_top") or [])
    if memory_rows:
        lines.extend(["", "🧬 Symbol Memory — Top"])
        for row in memory_rows[:5]:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "-")
            points = _fmt(row.get("symbol_memory_points") or row.get("memory_points") or row.get("bonus"), 2)
            win = _fmt(row.get("win_rate") or row.get("win_rate_pct"), 1)
            tp2 = _fmt(row.get("tp2_rate") or row.get("tp2_rate_pct"), 1)
            sl = _fmt(row.get("sl_rate") or row.get("sl_rate_pct"), 1)
            lines.append(f"• <b>{symbol}</b> | mem <code>{points}</code> | WR {win}% | TP2 {tp2}% | SL {sl}%")

    regret = dict(summary.get("regret") or {})
    if regret:
        lines.extend(["", "😢 Capital Regret"])
        lines.append(f"Decisions: <code>{_safe_int(regret.get('decisions'), 0)}</code>")
        lines.append(f"Accuracy: <code>{_fmt(regret.get('accuracy_pct'), 2)}%</code>")
        lines.append(f"False Rejections: <code>{_safe_int(regret.get('false_rejections'), 0)}</code>")
        lines.append(f"False Acceptances: <code>{_safe_int(regret.get('false_acceptance'), 0)}</code>")

        miss = regret.get("most_expensive_miss") or regret.get("highest_missed")
        if isinstance(miss, dict):
            lines.append(f"Most Expensive Miss: <b>{miss.get('symbol', '-')}</b> | Bid <code>{_fmt(miss.get('bid_score') or miss.get('bid'), 2)}</code>")
        worst = regret.get("worst_selected") or regret.get("worst_acceptance")
        if isinstance(worst, dict):
            lines.append(f"Worst Selected: <b>{worst.get('symbol', '-')}</b> | Bid <code>{_fmt(worst.get('bid_score') or worst.get('bid'), 2)}</code>")

    family_summary = dict(scan.get("family_summary") or {})
    operational = dict(family_summary.get("operational") or {})
    if int(operational.get("count", 0) or 0) > 0:
        lines.extend(["", "🛠️ Operational / Recovered Rows"])
        lines.append(f"Count: <code>{_safe_int(operational.get('count'), 0)}</code> | Avg Bid <code>{_fmt(operational.get('average_bid'), 2)}</code>")
        lines.append("Separated from strategy-quality analytics.")

    lines.extend([
        "",
        "Safety: Shadow only — no execution impact.",
    ])
    return "\n".join(lines)


__all__ = [
    "build_capital_dashboard_summary",
    "build_capital_dashboard_text",
]
