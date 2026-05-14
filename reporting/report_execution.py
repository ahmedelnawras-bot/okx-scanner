from __future__ import annotations

from collections import Counter

from tracking.models import TrackedTrade
from utils.constants import LEVERAGE_NOTE_AR
from reporting.report_format import (
    SEP,
    append_trade_cards,
    behavior_summary_lines,
    closed_trades,
    open_trades,
    trade_effective_pnl,
    wallet_impact_lines,
)


def _is_accepted_status(status: str | None) -> bool:
    return str(status or "") in {"accepted_preview", "pending_pullback_preview", "executed", "open", "tp1", "tp2", "trailing"}


def _is_rejected_status(status: str | None) -> bool:
    text = str(status or "")
    return text.startswith("rejected") or text in {"candidate_only"}


def _closed_win_rate(trades: list[TrackedTrade]) -> str:
    closed = closed_trades(trades)
    if not closed:
        return "N/A"
    winners = [t for t in closed if trade_effective_pnl(t) > 0]
    losers = [t for t in closed if trade_effective_pnl(t) < 0]
    denom = len(winners) + len(losers)
    if denom <= 0:
        return "N/A"
    return f"{len(winners) / denom * 100.0:.1f}%"


def build_execution_report(
    execution_results: list[dict],
    trades: list[TrackedTrade] | None = None,
    title: str = "🚀 تقرير أداء التنفيذ",
) -> str:
    trades = trades or []
    accepted = [r for r in execution_results if _is_accepted_status(r.get("status"))]
    pending = [r for r in execution_results if r.get("status") == "pending_pullback_preview"]
    rejected = [r for r in execution_results if _is_rejected_status(r.get("status"))]
    checked = len(execution_results)
    block = [r for r in execution_results if r.get("path") == "block_exception"]
    recovery = [r for r in execution_results if r.get("path") == "recovery"]
    strong = [r for r in execution_results if r.get("path") == "elite_or_whitelist"]
    whitelist = [r for r in execution_results if r.get("path") == "whitelist"]
    acc_rate = (len(accepted) / max(1, checked)) * 100 if checked else 0.0

    opened = open_trades(trades)
    winners = sorted([t for t in opened if trade_effective_pnl(t) >= 0], key=trade_effective_pnl, reverse=True)
    losers = sorted([t for t in opened if trade_effective_pnl(t) < 0], key=trade_effective_pnl)

    lines: list[str] = [title, "📅 Since Start", SEP, LEVERAGE_NOTE_AR, ""]
    lines.extend([
        "📊 <b>Quick Stats</b>",
        f"• Execution Checked: {checked}",
        f"• Accepted Preview: {len(accepted)}",
        f"• Rejected After Check: {len(rejected)}",
        f"🎚 Accept Rate: <b>{acc_rate:.1f}%</b>",
        f"🏆 Closed Win Rate: <b>{_closed_win_rate(trades)}</b>",
        f"• Open Execution Trades: {len(opened)}",
        f"• Closed Execution Trades: {len(closed_trades(trades))}",
        f"⏳ Pending Pullback: {len(pending)}",
        f"🛣 Whitelist: {len(whitelist)} | Strong: {len(strong)} | Recovery: {len(recovery)} | Block: {len(block)}",
        "📌 ملاحظة: الصفقات المرفوضة محفوظة للتحليل ولا تُحسب كصفقات مفتوحة.",
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
        status_counts = Counter(item.get("status", "unknown") for item in rejected)
        lines.extend([SEP, "📉 <b>Rejected After Check — Top Reasons</b>"])
        for reason, count in reason_counts.most_common(7):
            lines.append(f"• {reason}: {count}")
        lines.append("📊 Rejection Types")
        for status, count in status_counts.most_common(5):
            lines.append(f"• {status}: {count}")

    lines.extend([SEP, "💡 إدارة الصفقات: Normal/Strong/Block 40/40/20 | Recovery 50/25/25"])
    return "\n".join(lines)
