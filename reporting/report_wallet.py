from __future__ import annotations

from tracking.models import TrackedTrade
from reporting.report_format import SEP, money_from_exposure_pct, open_trades, closed_trades, trade_effective_pnl, filter_trades_by_period, period_label


def _net_parts(trades: list[TrackedTrade]) -> tuple[float, float, float]:
    closed = closed_trades(trades)
    opened = open_trades(trades)
    closed_net = sum(trade_effective_pnl(t) for t in closed)
    open_net = sum(trade_effective_pnl(t) for t in opened)
    return money_from_exposure_pct(closed_net), money_from_exposure_pct(open_net), money_from_exposure_pct(closed_net + open_net)


def build_wallet_report(trades: list[TrackedTrade], starting_balance: float = 1000.0, title: str = "💼 Wallet Impact — Execution") -> str:
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
        closed_usd, open_usd, total_usd = _net_parts(subset)
        lines.append(f"{label:<11} {closed_usd:>+8.2f} {open_usd:>+7.2f} {total_usd:>+8.2f}")
    lines.append("</code>")
    lines.extend(["", "📌 المرفوضات وCandidate Only لا تدخل في Wallet Impact."])
    return "\n".join(lines)
