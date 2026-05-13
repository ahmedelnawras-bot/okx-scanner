from __future__ import annotations

from collections import Counter

from tracking.models import TrackedTrade
from utils.constants import LEVERAGE_NOTE_AR
from reporting.report_format import (
    SEP,
    append_trade_cards,
    behavior_summary_lines,
    open_trades,
    quick_stats_lines,
    trade_effective_pnl,
    wallet_impact_lines,
)


def build_execution_report(
    execution_results: list[dict],
    trades: list[TrackedTrade] | None = None,
    title: str = "🚀 تقرير أداء التنفيذ",
) -> str:
    trades = trades or []
    accepted = [r for r in execution_results if r.get("status") == "accepted_preview"]
    pending = [r for r in execution_results if r.get("status") == "pending_pullback_preview"]
    candidate_only = [r for r in execution_results if r.get("status") == "candidate_only"]
    rejected = [r for r in execution_results if str(r.get("status", "")).startswith("rejected")]
    block = [r for r in execution_results if r.get("path") == "block_exception"]
    recovery = [r for r in execution_results if r.get("path") == "recovery"]
    strong = [r for r in execution_results if r.get("path") == "elite_or_whitelist"]
    whitelist = [r for r in execution_results if r.get("path") == "whitelist"]
    total = len(execution_results)
    acc_rate = (len(accepted) / max(1, total)) * 100 if total else 0.0

    opened = open_trades(trades)
    winners = sorted([t for t in opened if trade_effective_pnl(t) >= 0], key=trade_effective_pnl, reverse=True)
    losers = sorted([t for t in opened if trade_effective_pnl(t) < 0], key=trade_effective_pnl)

    lines: list[str] = [title, "📅 Since Start", SEP, LEVERAGE_NOTE_AR, ""]
    lines.extend([
        "📊 <b>Quick Stats</b>",
        f"• Candidates: {total}",
        f"• Open: {len(opened)}",
        f"• Closed: {len([t for t in trades if t.status not in {'open', 'tp1_partial', 'tp2_partial', 'runner'}])}",
        f"🏆 Accept Rate: <b>{acc_rate:.1f}%</b>",
        f"✅ Accepted: {len(accepted)} | ⏳ Pending: {len(pending)}",
        f"⚠️ Candidate Only: {len(candidate_only)} | ❌ Rejected: {len(rejected)}",
        f"🛣 Whitelist: {len(whitelist)} | Strong: {len(strong)} | Recovery: {len(recovery)} | Block: {len(block)}",
    ])

    if trades:
        lines.extend([SEP, *wallet_impact_lines(trades, title="Wallet Impact")])
        lines.extend([SEP, *behavior_summary_lines(trades, label="Execution Behavior Summary")])
        lines.extend([SEP, "📂 <b>Open Trades</b>"])
        lines.append(f"🟢 Open Winners: {len(winners)} | 🔴 Open Losers: {len(losers)}")
        if opened:
            lines.append(f"⚡ Net Floating: {sum(trade_effective_pnl(t) for t in opened):+.2f}% Exposure")
        append_trade_cards(lines, "🟢 <b>Latest 3 Winners</b>", winners[:3], limit=3)
        append_trade_cards(lines, "🔴 <b>Latest 3 Losers</b>", losers[:3], limit=3)
    else:
        lines.extend([
            SEP,
            "💰 <b>Wallet Impact</b>",
            "📌 لا توجد صفقات تنفيذ مقبولة بعد.",
            "⚖️ Net Wallet Impact: <b>🟢 +0.00$</b>",
        ])

    if rejected:
        reason_counts = Counter(item.get("reason", "unknown") for item in rejected)
        lines.extend([SEP, "📉 <b>Top Rejections</b>"])
        for reason, count in reason_counts.most_common(5):
            lines.append(f"• {reason}: {count}")

    lines.extend([SEP, "💡 يعتمد على نظام إدارة 40/40/20"])
    return "\n".join(lines)
