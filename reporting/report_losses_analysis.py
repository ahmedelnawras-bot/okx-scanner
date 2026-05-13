from __future__ import annotations

from collections import Counter
from tracking.models import TrackedTrade

LOSS_STATUSES = {"closed_loss"}


def build_losses_analysis_report(trades: list[TrackedTrade], title: str = "📉 تحليل أسباب الخسائر") -> str:
    losing = [t for t in trades if t.status in LOSS_STATUSES or (t.status == "open" and t.pnl_pct < 0)]
    total = len(trades)
    setup_counter = Counter(t.setup_type for t in losing)
    warning_counter = Counter(w for t in losing for w in t.warnings)
    avg_loss = sum(min(t.realized_pnl_pct if t.realized_pnl_pct else t.pnl_pct, 0.0) for t in losing) / max(1, len(losing))
    lines = [title, "━━━━━━━━━━━━"]
    lines.append(f"📊 الصفقات داخل الفترة: {total}")
    lines.append(f"❌ الصفقات الخاسرة داخل الفترة: {len(losing)}")
    lines.append(f"💸 متوسط الخسارة: {avg_loss:+.2f}%")
    if setup_counter:
        lines.append("⚠️ Setups تحتاج حذر")
        for name, count in setup_counter.most_common(5):
            lines.append(f"• {name} — losses {count}")
    if warning_counter:
        lines.append("🪫 Warnings مرتبطة بالخسائر")
        for name, count in warning_counter.most_common(4):
            lines.append(f"• {name} — {count}")
    return "\n".join(lines)
