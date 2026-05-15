from __future__ import annotations

from tracking.models import TrackedTrade
from utils.constants import LEVERAGE_NOTE_AR
from reporting.report_format import (
    SEP,
    THIN_SEP,
    OPEN_STATUSES,
    append_trade_cards,
    open_trades,
    trade_effective_pnl,
    wallet_impact_lines,
)


def _is_execution_trade(t: TrackedTrade) -> bool:
    return bool(getattr(t, "execution_trade", False) or getattr(t, "tracking_bucket", "") == "execution")


def _filter_open(trades: list[TrackedTrade], execution_only: bool = False) -> list[TrackedTrade]:
    items = [t for t in trades if t.status in OPEN_STATUSES]
    if execution_only:
        items = [t for t in items if _is_execution_trade(t)]
    return items


def _avg_score(items: list[TrackedTrade]) -> float:
    return sum(float(t.score or 0.0) for t in items) / max(1, len(items))


def build_open_trades_report(
    trades: list[TrackedTrade],
    title: str = "📂 الصفقات العادية المفتوحة",
    execution_only: bool = False,
) -> str:
    opened = _filter_open(trades, execution_only=execution_only)

    if execution_only and not opened:
        rejected_count = sum(
            1 for t in trades
            if getattr(t, "execution_checked", False)
            and not getattr(t, "execution_trade", False)
            and str(getattr(t, "execution_status", "")).startswith("rejected")
        )
        lines = [
            title,
            "📅 Since Start",
            SEP,
            LEVERAGE_NOTE_AR,
            "",
            "📊 <b>Quick Stats</b>",
            "• Open Execution Trades: 0",
            "• Winners: 0 | Losers: 0",
            "🏆 Open Win Rate: 0.0%",
            "⚖️ Total Floating PnL: +0.00%",
            "",
            "لا توجد صفقات تنفيذ مفتوحة حاليًا.",
            "📌 الإشارات العادية المرفوضة من التنفيذ لا تُحسب هنا.",
        ]
        if rejected_count:
            lines.append(f"⚙️ Execution rejected checks: {rejected_count}")
        return "\n".join(lines)

    winners = sorted([t for t in opened if trade_effective_pnl(t) >= 0], key=trade_effective_pnl, reverse=True)
    losers = sorted([t for t in opened if trade_effective_pnl(t) < 0], key=trade_effective_pnl)
    protected = sum(1 for t in opened if t.protected_on_block)
    tightened = sum(1 for t in opened if t.trailing_tightened)
    tp1 = sum(1 for t in opened if t.tp1_hit)
    tp2 = sum(1 for t in opened if t.tp2_hit)
    runner = sum(1 for t in opened if t.runner_active or t.tp2_hit)
    danger = sum(1 for t in opened if trade_effective_pnl(t) < -1.0)
    floating = sum(trade_effective_pnl(t) for t in opened)
    wr = len(winners) / max(1, len(opened)) * 100.0 if opened else 0.0

    lines = [title, "📅 Since Start", SEP, LEVERAGE_NOTE_AR, ""]
    lines.extend([
        "📊 <b>Quick Stats</b>",
        f"• Open: {len(opened)}",
        f"• Winners: {len(winners)} | Losers: {len(losers)}",
        f"🏆 Open Win Rate: {wr:.1f}%",
        f"⚖️ Total Floating PnL: {floating:+.2f}%",
        f"⭐ Avg Score: {_avg_score(opened):.2f}",
    ])
    if execution_only:
        lines.extend([SEP, *wallet_impact_lines(opened, title="Wallet Impact")])
    lines.extend([
        SEP,
        "📊 <b>Trade Stages</b>",
        f"🎯 TP1 Hit: {tp1}",
        f"🏁 TP2 Active: {tp2}",
        f"🏃 Runner: {runner}",
        f"🔒 Protected: {protected}",
        f"🛠 Tightened: {tightened}",
        f"⚠️ Danger: {danger}",
    ])

    append_trade_cards(lines, "📈 <b>Open Winners — Top 5</b>", winners, limit=5)
    append_trade_cards(lines, "📉 <b>Open Losers — Top 5</b>", losers, limit=5)
    lines.extend([SEP, "💡 يعتمد على نظام إدارة 40/40/20"])
    return "\n".join(lines)
