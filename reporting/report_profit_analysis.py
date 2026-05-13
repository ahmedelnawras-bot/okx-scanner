from __future__ import annotations

from collections import Counter
from tracking.models import TrackedTrade

PROFIT_STATUSES = {"tp1_partial", "tp2_partial", "runner", "trailing_hit", "closed_win", "breakeven_after_tp1"}


def build_profit_analysis_report(trades: list[TrackedTrade], title: str = "📈 تحليل أسباب الأرباح") -> str:
    profitable = [t for t in trades if t.status in PROFIT_STATUSES or t.realized_pnl_pct > 0 or t.pnl_pct > 0]
    total = len(trades)
    setup_counter = Counter(t.setup_type for t in profitable)
    warning_counter = Counter(w for t in profitable for w in t.warnings)
    avg_profit = sum(max(t.realized_pnl_pct, t.pnl_pct) for t in profitable) / max(1, len(profitable))
    lines = [title, "━━━━━━━━━━━━"]
    lines.append(f"📊 الصفقات داخل الفترة: {total}")
    lines.append(f"✅ الصفقات الرابحة داخل الفترة: {len(profitable)}")
    lines.append(f"💰 متوسط الربح: {avg_profit:+.2f}%")
    if setup_counter:
        lines.append("✅ أقوى Setups")
        for name, count in setup_counter.most_common(5):
            lines.append(f"• {name} — wins {count}")
    if warning_counter:
        lines.append("⚠️ Warnings ظهرت مع أرباح")
        for name, count in warning_counter.most_common(4):
            lines.append(f"• {name} — {count}")
    return "\n".join(lines)
