from __future__ import annotations

from tracking.models import TrackedTrade
from utils.constants import LEVERAGE_NOTE_AR


def build_general_report(trades: list[TrackedTrade], title: str = "📊 التقرير العام") -> str:
    open_trades = [t for t in trades if t.status in {"open", "tp1_partial", "tp2_partial", "runner"}]
    wins = [t for t in trades if t.status in {"tp2_partial", "runner", "trailing_hit", "closed_win", "breakeven_after_tp1"}]
    losses = [t for t in trades if t.status == "closed_loss"]
    net_open = sum(t.pnl_pct for t in open_trades)
    realized = sum(t.realized_pnl_pct for t in trades)
    total_net = realized + net_open
    win_rate = (len(wins) / len(trades) * 100.0) if trades else 0.0

    return "\n".join([
        title,
        "━━━━━━━━━━━━",
        LEVERAGE_NOTE_AR,
        f"📌 Quick Stats | Trades: {len(trades)} | Open: {len(open_trades)}",
        f"✅ Win Rate: {win_rate:.1f}% | Wins: {len(wins)} | Losses: {len(losses)}",
        f"📈 Floating: {net_open:+.2f}% | 💰 Realized: {realized:+.2f}%",
        f"⚖️ Total Net: {total_net:+.2f}%",
    ])
