"""
Capital Intelligence Layer - reporting v1

Builds compact text/JSON reports from CapitalAuctionResult.
No trading side effects.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

try:
    from .capital_models import CapitalAuctionResult, CapitalBid
except Exception:
    from capital_models import CapitalAuctionResult, CapitalBid


def _fmt(value: float) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return "0.00"


def build_capital_summary(result: CapitalAuctionResult | dict | None, top_n: int = 8) -> dict[str, Any]:
    if result is None:
        return {"ok": False, "reason": "missing_result"}
    if isinstance(result, dict):
        bids = result.get("bids") or []
        rows = bids if isinstance(bids, list) else []
    else:
        rows = [bid.to_dict() for bid in result.bids]

    if not rows:
        return {"ok": True, "candidates": 0, "average_bid": 0.0, "classes": {}, "top": []}

    total = sum(float(row.get("bid_score") or 0.0) for row in rows)
    classes: dict[str, int] = defaultdict(int)
    setups: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        classes[str(row.get("trade_class") or "C")] += 1
        setups[str(row.get("setup_type") or "unknown")].append(float(row.get("bid_score") or 0.0))

    setup_summary = []
    for setup, scores in setups.items():
        setup_summary.append({"setup_type": setup, "count": len(scores), "avg_bid": round(sum(scores) / max(1, len(scores)), 2)})
    setup_summary.sort(key=lambda item: (item["avg_bid"], item["count"]), reverse=True)

    return {
        "ok": True,
        "candidates": len(rows),
        "average_bid": round(total / max(1, len(rows)), 2),
        "classes": dict(classes),
        "top": rows[:max(1, int(top_n or 8))],
        "setup_summary": setup_summary,
    }


def build_capital_report_text(result: CapitalAuctionResult | dict | None, top_n: int = 8) -> str:
    summary = build_capital_summary(result, top_n=top_n)
    if not summary.get("ok"):
        return "🧠 Capital Intelligence\nNo capital intelligence result available."

    lines = [
        "🧠 Capital Intelligence — Shadow Report",
        "━━━━━━━━━━━━",
        f"Candidates: {int(summary.get('candidates', 0) or 0)}",
        f"Average Bid: {_fmt(summary.get('average_bid', 0.0))}",
        "Classes: " + ", ".join(f"{k}={v}" for k, v in sorted((summary.get("classes") or {}).items())) if summary.get("classes") else "Classes: -",
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

    setup_summary = summary.get("setup_summary") or []
    if setup_summary:
        lines.extend(["", "📊 Setup Bid Average"])
        for item in setup_summary[:5]:
            lines.append(f"• {item.get('setup_type')}: avg {item.get('avg_bid')} | n={item.get('count')}")

    lines.extend(["", "Mode: Shadow only — no execution impact."])
    return "\n".join(lines)


__all__ = ["build_capital_summary", "build_capital_report_text"]
