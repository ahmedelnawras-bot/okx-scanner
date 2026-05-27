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
    filter_trades_by_period,
    period_label,
)


def build_general_report(trades: list[TrackedTrade], title: str = "📊 تقرير الصفقات العادية", period: str = "since_start") -> str:
    # Normal reports are signal/performance reports only. No Wallet Impact for Normal.
    trades = filter_trades_by_period(trades or [], period)
    opened = open_trades(trades)
    winners = sorted([t for t in opened if trade_effective_pnl(t) >= 0], key=trade_effective_pnl, reverse=True)
    losers = sorted([t for t in opened if trade_effective_pnl(t) < 0], key=trade_effective_pnl)
    closed_wins = sorted([t for t in trades if t.is_closed and trade_effective_pnl(t) > 0], key=trade_effective_pnl, reverse=True)
    closed_losses = sorted([t for t in trades if t.is_closed and trade_effective_pnl(t) < 0], key=trade_effective_pnl)

    lines: list[str] = [title, f"📅 {period_label(period)}", SEP, LEVERAGE_NOTE_AR, ""]
    lines.extend(quick_stats_lines(trades, item_name="Signals"))
    lines.extend([SEP, *behavior_summary_lines(trades, label="Normal Behavior Summary")])

    lines.extend([SEP, "📂 <b>Open Trades</b>"])
    lines.append(f"📈 Open Winners: {len(winners)} | 📉 Open Losers: {len(losers)}")
    if opened:
        lines.append(f"⚖️ Total Floating PnL: {sum(trade_effective_pnl(t) for t in opened):+.2f}%")
    append_trade_cards(lines, "🟢 <b>Top 3 Open Winners</b>", winners[:3], limit=3)
    append_trade_cards(lines, "🔴 <b>Top 3 Open Losers</b>", losers[:3], limit=3)
    append_trade_cards(lines, "🏆 <b>Top 3 Closed Winners</b>", closed_wins[:3], limit=3)
    append_trade_cards(lines, "💀 <b>Top 3 Closed Losers</b>", closed_losses[:3], limit=3)

    lines.extend([SEP, "💡 Normal reports are signal/performance only — no Wallet Impact."])
    return "\n".join(lines)
