from __future__ import annotations

from tracking.models import TrackedTrade
from utils.constants import LEVERAGE_NOTE_AR
from reporting.report_format import (
    SEP,
    behavior_summary_lines,
    open_trades,
    quick_stats_lines,
    trade_effective_pnl,
    append_trade_cards,
    wallet_impact_lines,
)


def build_general_report(trades: list[TrackedTrade], title: str = "📊 تقرير الصفقات العادية") -> str:
    opened = open_trades(trades)
    winners = sorted([t for t in opened if trade_effective_pnl(t) >= 0], key=trade_effective_pnl, reverse=True)
    losers = sorted([t for t in opened if trade_effective_pnl(t) < 0], key=trade_effective_pnl)
    closed_wins = sorted([t for t in trades if t.status not in {"open", "tp1_partial", "tp2_partial", "runner"} and trade_effective_pnl(t) > 0], key=trade_effective_pnl, reverse=True)
    closed_losses = sorted([t for t in trades if t.status not in {"open", "tp1_partial", "tp2_partial", "runner"} and trade_effective_pnl(t) < 0], key=trade_effective_pnl)

    lines: list[str] = [title, "📅 Since Start", SEP, LEVERAGE_NOTE_AR, ""]
    lines.extend(quick_stats_lines(trades, item_name="Signals"))
    lines.extend([SEP, *wallet_impact_lines(trades, title="Performance Impact")])
    lines.extend([SEP, *behavior_summary_lines(trades)])

    lines.extend([SEP, "📂 <b>Open Trades</b>"])
    lines.append(f"📈 Open Winners: {len(winners)} | 📉 Open Losers: {len(losers)}")
    if opened:
        lines.append(f"⚖️ Net Floating: {sum(trade_effective_pnl(t) for t in opened):+.2f}% Exposure")
    append_trade_cards(lines, "🟢 <b>Latest 3 Open Winners</b>", winners[:3], limit=3)
    append_trade_cards(lines, "🔴 <b>Latest 3 Open Losers</b>", losers[:3], limit=3)
    append_trade_cards(lines, "🏆 <b>Closed Wins — Top 3</b>", closed_wins[:3], limit=3)
    append_trade_cards(lines, "🛑 <b>Closed Losses — Worst 3</b>", closed_losses[:3], limit=3)

    lines.extend([SEP, "💡 يعتمد على نظام إدارة 40/40/20"])
    return "\n".join(lines)
