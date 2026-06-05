from __future__ import annotations

from tracking.models import TrackedTrade
from reporting.report_format import (
    DEFAULT_MARGIN_PER_TRADE,
    SEP,
    open_trades,
    closed_trades,
    trade_money_pnl,
    filter_trades_by_period,
    period_label,
)


def _net_parts(trades: list[TrackedTrade], margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE) -> tuple[float, float, float]:
    closed = closed_trades(trades)
    opened = open_trades(trades)

    # Accurate wallet impact: sum each trade with its own stored margin.
    # margin_per_trade remains only as a fallback for legacy trades.
    closed_net = sum(
        trade_money_pnl(t, fallback_margin=margin_per_trade)
        for t in closed
    )
    open_net = sum(
        trade_money_pnl(t, fallback_margin=margin_per_trade)
        for t in opened
    )
    return closed_net, open_net, closed_net + open_net


def build_wallet_report(trades: list[TrackedTrade], starting_balance: float = 1000.0, margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE, title: str = "💼 Wallet Impact — Execution") -> str:
    # Wallet Impact is execution-only. Normal trades must not call this report.
    lines = [title, "📅 Snapshot", SEP, "⚡ Scope: Execution accepted/tracked trades only", f"📌 رأس المال: {starting_balance:.0f}$", ""]
    rows = [
        ("Since Start", "since_start"),
        ("Month", "month"),
        ("Last 7D", "last_7d"),
        ("Today", "today"),
        ("Last 1H", "last_1h"),
    ]
    lines.append("<code>Period       Closed$   Open$   Total$")
    for label, period in rows:
        subset = filter_trades_by_period(trades or [], period)
        closed_usd, open_usd, total_usd = _net_parts(subset, margin_per_trade)
        lines.append(f"{label:<11} {closed_usd:>+8.2f} {open_usd:>+7.2f} {total_usd:>+8.2f}")
    lines.append("</code>")
    lines.extend(["", "📌 المرفوضات وCandidate Only لا تدخل في Wallet Impact."])
    return "\n".join(lines)
