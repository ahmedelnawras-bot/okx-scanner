from __future__ import annotations

from tracking.models import TrackedTrade
from utils.constants import LEVERAGE_NOTE_AR


def _filter_open(trades: list[TrackedTrade], execution_only: bool = False) -> list[TrackedTrade]:
    items = [t for t in trades if t.status in {"open", "tp1_partial", "tp2_partial", "runner"}]
    if execution_only:
        items = [t for t in items if t.execution_setup_tags]
    return items


def _clean_setup_label(t: TrackedTrade) -> str:
    tags = ", ".join(t.execution_setup_tags[:2]) if t.execution_setup_tags else "normal"
    return f"{t.setup_type} | {tags}"


def _card_lines(t: TrackedTrade) -> list[str]:
    extras = []
    if t.protected_on_block:
        extras.append("🛡 Protected")
    if t.trailing_tightened:
        extras.append("🔧 Tightened")
    extra_tail = f" | {' | '.join(extras)}" if extras else ""
    return [
        f"• {t.symbol} | {t.pnl_pct:+.2f}%",
        f"⏱ {t.stage_label} | ⭐ Score: {t.score:.2f}{extra_tail}",
        f"🎯 TP1: {t.tp1:.6f} | 🏁 TP2: {t.tp2:.6f}",
        f"🛡 SL: {t.sl:.6f}",
        f"🧠 {_clean_setup_label(t)}",
    ]


def build_open_trades_report(
    trades: list[TrackedTrade],
    title: str = "📂 الصفقات المفتوحة",
    execution_only: bool = False,
) -> str:
    open_trades = _filter_open(trades, execution_only=execution_only)
    winners = sorted([t for t in open_trades if t.pnl_pct >= 0], key=lambda t: t.pnl_pct, reverse=True)
    losers = sorted([t for t in open_trades if t.pnl_pct < 0], key=lambda t: t.pnl_pct)
    protected = sum(1 for t in open_trades if t.protected_on_block)
    tightened = sum(1 for t in open_trades if t.trailing_tightened)
    tp1 = sum(1 for t in open_trades if t.tp1_hit)
    tp2 = sum(1 for t in open_trades if t.tp2_hit)
    runner = sum(1 for t in open_trades if t.runner_active or t.tp2_hit)
    floating = sum(t.pnl_pct for t in open_trades)
    avg_score = sum(t.score for t in open_trades) / max(1, len(open_trades))
    wr = (len(winners) / max(1, len(open_trades))) * 100 if open_trades else 0.0

    lines = [title, "━━━━━━━━━━━━", LEVERAGE_NOTE_AR]
    lines.append(f"📊 Open: {len(open_trades)} | Winners: {len(winners)} | Losers: {len(losers)}")
    lines.append(f"✅ Win Rate: {wr:.0f}% | ⚖️ Net Floating: {floating:+.2f}%")
    lines.append(f"🎯 TP1: {tp1} | 🏁 TP2: {tp2} | 🏃 Runner: {runner}")
    lines.append(f"🛡 Protected: {protected} | 🔧 Tightened: {tightened} | ⭐ Avg Score: {avg_score:.2f}")

    if winners:
        lines.append("📈 Open Winners — Top 5")
        for t in winners[:5]:
            lines.extend(_card_lines(t))
        if len(winners) > 5:
            lines.append(f"📂 +{len(winners)-5} more winning trades...")

    if losers:
        lines.append("📉 Open Losers — Top 5")
        for t in losers[:5]:
            lines.extend(_card_lines(t))
        if len(losers) > 5:
            lines.append(f"📂 +{len(losers)-5} more losing trades...")

    return "\n".join(lines)
