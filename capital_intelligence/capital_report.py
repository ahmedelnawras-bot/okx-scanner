"""
Capital Intelligence Layer - reporting v2

Builds compact text/JSON reports from CapitalAuctionResult.
No trading side effects.

V2 additions:
- Preserves the public API from v1:
  - build_capital_summary()
  - build_capital_report_text()
- Adds component averages so new intelligence layers are visible:
  setup_synergy, market_mode_awareness, symbol_memory, etc.
- Adds compact per-candidate drivers/reasons.
- Adds A+/A quality ratio and a lightweight Capital Confidence label.

Safety:
- Reporting only.
- Does not place orders.
- Does not reject trades.
- Does not change score, TP, SL, OKX, Recovery, BLOCK, or main.py.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

try:
    from .capital_models import CapitalAuctionResult, CapitalBid
except Exception:
    from capital_models import CapitalAuctionResult, CapitalBid


def _fmt(value: object) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return "0.00"


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _as_rows(result: CapitalAuctionResult | dict | None) -> list[dict[str, Any]]:
    if result is None:
        return []
    if isinstance(result, dict):
        bids = result.get("bids") or []
        return [dict(row or {}) for row in bids if isinstance(row, dict)]
    return [bid.to_dict() for bid in list(getattr(result, "bids", []) or [])]


def _components(row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = row.get("components") or []
    if not isinstance(raw, list):
        return []
    return [dict(item or {}) for item in raw if isinstance(item, dict)]


def _component_points(row: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for component in _components(row):
        name = str(component.get("name") or "").strip()
        if not name:
            continue
        out[name] = _safe_float(component.get("points"), 0.0)
    return out


def _component_reason(row: dict[str, Any], name: str) -> str:
    for component in _components(row):
        if str(component.get("name") or "") == name:
            return str(component.get("reason") or "").strip()
    return ""


def _top_drivers(row: dict[str, Any], limit: int = 3) -> list[str]:
    """Return compact positive drivers for a top candidate.

    This makes the shadow report explain *why* the Capital Brain liked a trade
    without dumping the full component tree.
    """
    components = _components(row)
    drivers: list[tuple[float, str]] = []
    for component in components:
        name = str(component.get("name") or "").strip()
        if not name:
            continue
        points = _safe_float(component.get("points"), 0.0)
        max_points = max(1.0, _safe_float(component.get("max_points"), 1.0))
        ratio = points / max_points
        reason = str(component.get("reason") or "").strip()

        # Prefer newer intelligence components even when their point cap is small.
        priority_boost = 0.0
        if name in {"setup_synergy", "market_mode_awareness", "symbol_memory"}:
            priority_boost = 0.15
        if ratio >= 0.70 or name in {"setup_synergy", "market_mode_awareness", "symbol_memory"}:
            label = name.replace("_", " ")
            if reason:
                label = f"{label}: {reason[:36]}"
            drivers.append((ratio + priority_boost, label))

    drivers.sort(key=lambda item: item[0], reverse=True)
    return [label for _, label in drivers[: max(1, int(limit or 3))]]


def _capital_confidence(avg_bid: float, classes: dict[str, int], candidates: int) -> dict[str, Any]:
    if candidates <= 0:
        return {"score": 0.0, "label": "NONE", "a_or_better_ratio": 0.0}

    a_plus = int(classes.get("A+", 0) or 0)
    a_count = int(classes.get("A", 0) or 0)
    b_count = int(classes.get("B", 0) or 0)
    quality_ratio = (a_plus + a_count) / max(1, candidates)
    investable_ratio = (a_plus + a_count + b_count) / max(1, candidates)

    # Lightweight scan confidence: not an execution signal, only a report label.
    score = (float(avg_bid) * 0.65) + (quality_ratio * 100.0 * 0.25) + (investable_ratio * 100.0 * 0.10)
    score = round(max(0.0, min(100.0, score)), 2)

    if score >= 80:
        label = "HIGH"
    elif score >= 65:
        label = "GOOD"
    elif score >= 50:
        label = "MEDIUM"
    else:
        label = "LOW"

    return {
        "score": score,
        "label": label,
        "a_or_better_ratio": round(quality_ratio * 100.0, 2),
        "b_or_better_ratio": round(investable_ratio * 100.0, 2),
    }



_OPERATIONAL_SETUP_MARKERS = (
    "okx_recovered",
    "recovered_position",
    "bot_order_restored",
    "restored_position",
    "manual_recovered",
    "duplicate_closed_by_okx_repair",
)

_STRATEGY_SETUP_MARKERS = (
    "wave_3",
    "higher_low",
    "retest_breakout",
    "vwap_reclaim",
    "support_bounce",
    "liquidity_sweep",
    "breakout_pullback",
    "compression_release",
    "sweep_reclaim",
    "trend_continuation",
)


def _trade_row_family(row: dict[str, Any]) -> str:
    """Classify report rows without changing trading decisions.

    strategy: real strategy/setup candidates.
    operational: OKX restore/recovery/sync artifacts that should not pollute
    setup-quality analytics. Unknown rows default to strategy because Capital
    auction candidates are normally strategy candidates.
    """
    setup = str(row.get("setup_type") or row.get("base_setup_type") or row.get("reporting_setup_type") or "").strip().lower()
    mode = str(row.get("market_mode") or row.get("exchange_sync_state") or row.get("execution_status") or "").strip().lower()
    source = str(row.get("trade_source") or row.get("tracking_bucket") or row.get("source") or "").strip().lower()
    joined = "|".join([setup, mode, source])
    if any(marker in joined for marker in _OPERATIONAL_SETUP_MARKERS):
        return "operational"
    if any(marker in setup for marker in _STRATEGY_SETUP_MARKERS):
        return "strategy"
    return "strategy"


def _family_summary(rows: list[dict[str, Any]], top_n: int = 8) -> dict[str, Any]:
    buckets = {"strategy": [], "operational": []}
    for row in rows:
        family = _trade_row_family(row)
        buckets.setdefault(family, []).append(row)

    out: dict[str, Any] = {}
    for family, items in buckets.items():
        total = sum(_safe_float(row.get("bid_score"), 0.0) for row in items)
        classes: dict[str, int] = defaultdict(int)
        setups: dict[str, list[float]] = defaultdict(list)
        for row in items:
            classes[str(row.get("trade_class") or "C")] += 1
            setups[str(row.get("setup_type") or "unknown")].append(_safe_float(row.get("bid_score"), 0.0))
        setup_summary = []
        for setup, scores in setups.items():
            setup_summary.append({
                "setup_type": setup,
                "count": len(scores),
                "avg_bid": round(sum(scores) / max(1, len(scores)), 2),
            })
        setup_summary.sort(key=lambda item: (item["avg_bid"], item["count"]), reverse=True)
        out[family] = {
            "count": len(items),
            "average_bid": round(total / max(1, len(items)), 2) if items else 0.0,
            "classes": dict(classes),
            "top": items[: max(1, int(top_n or 8))],
            "setup_summary": setup_summary,
        }
    return out


def build_capital_summary(result: CapitalAuctionResult | dict | None, top_n: int = 8) -> dict[str, Any]:
    rows = _as_rows(result)
    if result is None:
        return {"ok": False, "reason": "missing_result"}

    if not rows:
        return {
            "ok": True,
            "candidates": 0,
            "average_bid": 0.0,
            "classes": {},
            "top": [],
            "setup_summary": [],
            "component_summary": [],
            "family_summary": {"strategy": {"count": 0, "average_bid": 0.0, "classes": {}, "top": [], "setup_summary": []}, "operational": {"count": 0, "average_bid": 0.0, "classes": {}, "top": [], "setup_summary": []}},
            "strategy_candidates": 0,
            "operational_candidates": 0,
            "a_plus_summary": {"count": 0, "avg_bid": 0.0, "top_setups": []},
            "capital_confidence": {"score": 0.0, "label": "NONE", "a_or_better_ratio": 0.0},
        }

    total = sum(_safe_float(row.get("bid_score"), 0.0) for row in rows)
    classes: dict[str, int] = defaultdict(int)
    setups: dict[str, list[float]] = defaultdict(list)
    component_scores: dict[str, list[float]] = defaultdict(list)
    a_plus_setups: Counter[str] = Counter()
    a_plus_scores: list[float] = []

    for row in rows:
        trade_class = str(row.get("trade_class") or "C")
        setup = str(row.get("setup_type") or "unknown")
        bid_score = _safe_float(row.get("bid_score"), 0.0)
        classes[trade_class] += 1
        setups[setup].append(bid_score)
        if trade_class == "A+":
            a_plus_setups[setup] += 1
            a_plus_scores.append(bid_score)

        for component in _components(row):
            name = str(component.get("name") or "").strip()
            if not name:
                continue
            component_scores[name].append(_safe_float(component.get("points"), 0.0))

    setup_summary = []
    for setup, scores in setups.items():
        setup_summary.append({
            "setup_type": setup,
            "count": len(scores),
            "avg_bid": round(sum(scores) / max(1, len(scores)), 2),
        })
    setup_summary.sort(key=lambda item: (item["avg_bid"], item["count"]), reverse=True)

    component_summary = []
    for name, scores in component_scores.items():
        component_summary.append({
            "component": name,
            "count": len(scores),
            "avg_points": round(sum(scores) / max(1, len(scores)), 2),
            "max_seen": round(max(scores or [0.0]), 2),
        })
    component_summary.sort(key=lambda item: (item["avg_points"], item["count"]), reverse=True)

    avg_bid = round(total / max(1, len(rows)), 2)
    confidence = _capital_confidence(avg_bid, dict(classes), len(rows))
    family_summary = _family_summary(rows, top_n=top_n)

    return {
        "ok": True,
        "candidates": len(rows),
        "average_bid": avg_bid,
        "classes": dict(classes),
        "top": rows[: max(1, int(top_n or 8))],
        "setup_summary": setup_summary,
        "component_summary": component_summary,
        "family_summary": family_summary,
        "strategy_candidates": int((family_summary.get("strategy") or {}).get("count", 0) or 0),
        "operational_candidates": int((family_summary.get("operational") or {}).get("count", 0) or 0),
        "a_plus_summary": {
            "count": len(a_plus_scores),
            "avg_bid": round(sum(a_plus_scores) / max(1, len(a_plus_scores)), 2) if a_plus_scores else 0.0,
            "top_setups": [{"setup_type": setup, "count": count} for setup, count in a_plus_setups.most_common(5)],
        },
        "capital_confidence": confidence,
    }


def build_capital_report_text(result: CapitalAuctionResult | dict | None, top_n: int = 8) -> str:
    summary = build_capital_summary(result, top_n=top_n)
    if not summary.get("ok"):
        return "🧠 Capital Intelligence\nNo capital intelligence result available."

    confidence = dict(summary.get("capital_confidence") or {})
    lines = [
        "🧠 Capital Intelligence — Shadow Report V2",
        "━━━━━━━━━━━━",
        f"Candidates: {int(summary.get('candidates', 0) or 0)}",
        f"Average Bid: {_fmt(summary.get('average_bid', 0.0))}",
        "Classes: " + ", ".join(f"{k}={v}" for k, v in sorted((summary.get("classes") or {}).items())) if summary.get("classes") else "Classes: -",
        f"Confidence: {str(confidence.get('label') or 'NONE')} | Score {_fmt(confidence.get('score') or 0.0)} | A/A+ {_fmt(confidence.get('a_or_better_ratio') or 0.0)}%",
        f"Strategy Candidates: {int(summary.get('strategy_candidates', 0) or 0)} | Operational/Recovered: {int(summary.get('operational_candidates', 0) or 0)}",
        "",
        "🏆 Top Candidates",
    ]

    for row in summary.get("top", [])[:top_n]:
        rank = int(row.get("rank") or 0)
        symbol = str(row.get("symbol") or "-")
        setup = str(row.get("setup_type") or "-")
        bid = _fmt(row.get("bid_score") or 0.0)
        cls = str(row.get("trade_class") or "C")
        selected = "✅" if row.get("advisory_selected") else "⏳"
        lines.append(f"{rank}. {selected} {symbol} | {setup} | Bid {bid} | {cls}")

        drivers = _top_drivers(row, limit=3)
        if drivers:
            lines.append("   ↳ " + " | ".join(drivers))

    a_plus = dict(summary.get("a_plus_summary") or {})
    if int(a_plus.get("count", 0) or 0) > 0:
        lines.extend([
            "",
            "🌟 A+ Quality",
            f"Count: {int(a_plus.get('count', 0) or 0)} | Avg Bid: {_fmt(a_plus.get('avg_bid') or 0.0)}",
        ])
        top_setups = a_plus.get("top_setups") or []
        if top_setups:
            lines.append("Drivers: " + ", ".join(f"{item.get('setup_type')}={item.get('count')}" for item in top_setups[:4]))

    component_summary = summary.get("component_summary") or []
    if component_summary:
        lines.extend(["", "🧩 Component Averages"])
        # Keep report compact but include the new intelligence components when present.
        preferred_order = [
            "setup_quality",
            "setup_synergy",
            "market_mode_awareness",
            "symbol_memory",
            "mtf_strength",
            "price_action",
            "nour_stability",
            "resistance_distance",
            "context_quality",
        ]
        by_name = {str(item.get("component")): item for item in component_summary}
        shown = []
        for name in preferred_order:
            item = by_name.get(name)
            if not item:
                continue
            lines.append(f"• {name}: avg {_fmt(item.get('avg_points') or 0.0)} | max {_fmt(item.get('max_seen') or 0.0)}")
            shown.append(name)
        for item in component_summary:
            name = str(item.get("component") or "")
            if name in shown:
                continue
            lines.append(f"• {name}: avg {_fmt(item.get('avg_points') or 0.0)} | max {_fmt(item.get('max_seen') or 0.0)}")
            if len(shown) >= 8:
                break
            shown.append(name)

    setup_summary = summary.get("setup_summary") or []
    if setup_summary:
        lines.extend(["", "📊 Setup Bid Average"])
        for item in setup_summary[:5]:
            lines.append(f"• {item.get('setup_type')}: avg {item.get('avg_bid')} | n={item.get('count')}")

    family_summary = dict(summary.get("family_summary") or {})
    operational = dict(family_summary.get("operational") or {})
    if int(operational.get("count", 0) or 0) > 0:
        lines.extend(["", "🛠️ Operational / Recovered Rows"])
        lines.append(f"Count: {int(operational.get('count', 0) or 0)} | Avg Bid: {_fmt(operational.get('average_bid') or 0.0)}")
        lines.append("Note: separated from strategy-quality analytics.")

    lines.extend(["", "Mode: Shadow only — no execution impact."])
    return "\n".join(lines)


__all__ = ["build_capital_summary", "build_capital_report_text"]
