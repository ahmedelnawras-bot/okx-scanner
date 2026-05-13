from __future__ import annotations

from tracking.models import TrackedTrade


def build_wallet_report(trades: list[TrackedTrade], starting_balance: float = 1000.0, title: str = "💼 Wallet Impact") -> str:
    closed = [t for t in trades if t.status in {"closed_loss", "breakeven_after_tp1", "trailing_hit", "closed_win"}]
    open_trades = [t for t in trades if t.status in {"open", "tp1_partial", "tp2_partial", "runner"}]
    gross_profit = sum(max(0.0, t.realized_pnl_pct) for t in closed)
    gross_loss = sum(min(0.0, t.realized_pnl_pct) for t in closed)
    floating_profit = sum(max(0.0, t.pnl_pct) for t in open_trades)
    floating_loss = sum(min(0.0, t.pnl_pct) for t in open_trades)
    locked = sum(max(0.0, t.realized_pnl_pct) for t in open_trades if t.tp1_hit)
    net_closed = gross_profit + gross_loss
    net_floating = floating_profit + floating_loss
    total_impact = net_closed + net_floating
    sign = "🟢" if total_impact >= 0 else "🔴"
    winners = sum(1 for t in open_trades if t.pnl_pct >= 0)
    losers = len(open_trades) - winners
    return "\n".join([
        title,
        "📅 Since Start / Snapshot",
        f"📌 Start Balance: {starting_balance:.0f}$",
        f"📈 Floating: {net_floating:+.2f}% | Winners: {winners} | Losers: {losers}",
        f"🔒 Locked: {locked:+.2f}% | Closed Net: {net_closed:+.2f}%",
        f"⚖️ Total Impact: **{sign} {total_impact:+.2f}%**",
        f"✅ Gross Profit: {gross_profit:+.2f}% | Gross Loss: {gross_loss:+.2f}%",
    ])
