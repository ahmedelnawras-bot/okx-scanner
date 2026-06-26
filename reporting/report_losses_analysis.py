from __future__ import annotations

from collections import Counter
from tracking.models import TrackedTrade
from reporting.report_format import SEP, score_range, trade_effective_pnl, closed_trades

LOSS_STATUSES = {"closed_loss"}


def _market_state_label(t: TrackedTrade) -> str:
    mode = str(t.market_mode or "UNKNOWN")
    if "BLOCK" in mode:
        return "🔴 Risk-Off"
    if "RECOVERY" in mode:
        return "🔵 Recovery"
    if "STRONG" in mode:
        return "🟡 Mixed"
    return "🟢 Normal / Bull"


def _entry_timing_label(t: TrackedTrade) -> str:
    warnings = " ".join(t.warnings or []).lower()
    if "late" in warnings or "chasing" in warnings:
        return "🔴 متأخر"
    if "extension" in warnings or "resistance" in warnings:
        return "🟡 متوسط (نص الحركة)"
    return "🟡 متوسط (نص الحركة)"


def build_losses_analysis_report(trades: list[TrackedTrade], title: str = "📉 تحليل أسباب خسائر التنفيذ") -> str:
    # تقييد التحليل بالصفقات المغلقة فقط. القديم كان يدخّل الصفقات المفتوحة
    # اللي floating بتاعها سالب، فيتضخّم عدد "Losing Trades" ويختلف عن عدد
    # الخاسرة في Win Rate (المحسوب على المغلقة). الاتساق مهم.
    closed = closed_trades(trades)
    losing = [t for t in closed if t.status in LOSS_STATUSES or trade_effective_pnl(t) < 0]
    setup_counter = Counter(t.setup_type for t in losing)
    market_counter = Counter(_market_state_label(t) for t in losing)
    timing_counter = Counter(_entry_timing_label(t) for t in losing)
    score_counter = Counter(score_range(float(t.score or 0)) for t in losing)
    warning_counter = Counter(w for t in losing for w in (t.warnings or []))
    close_counter = Counter(t.status for t in losing)
    avg_loss = sum(min(trade_effective_pnl(t), 0.0) for t in losing) / max(1, len(losing))
    direct_sl = sum(1 for t in losing if t.status == "closed_loss")
    tp1_rate = sum(1 for t in losing if t.tp1_hit) / max(1, len(losing)) * 100.0
    tp2_rate = sum(1 for t in losing if t.tp2_hit) / max(1, len(losing)) * 100.0

    lines = [title, "📅 Since Start", SEP]
    lines.extend([
        "📊 <b>Loss Summary</b>",
        f"• Losing Trades: {len(losing)}",
        f"📉 Avg Loser: {avg_loss:+.2f}%",
        f"🛑 Direct SL: {direct_sl}",
        f"🎯 TP1 Rate: {tp1_rate:.1f}%",
        f"🏁 TP2 Rate: {tp2_rate:.1f}%",
    ])
    sections = [
        ("🧠 <b>Weak Setups</b>", setup_counter),
        ("🌐 <b>Market States</b>", market_counter),
        ("⏱️ <b>Entry Timing Problems</b>", timing_counter),
        ("⭐ <b>Score Ranges</b>", score_counter),
        ("⚠️ <b>Repeated Warnings</b>", warning_counter),
        ("🏁 <b>Close Types</b>", close_counter),
    ]
    for heading, counter in sections:
        if counter:
            lines.extend([SEP, heading])
            for name, count in counter.most_common(5):
                lines.append(f"• {name} — {count}")
    lines.extend([SEP, "💡 <b>الخلاصة</b>"])
    if losing:
        worst_setup = setup_counter.most_common(1)[0][0] if setup_counter else "غير محدد"
        lines.append(f"أكبر ضغط خسائر حاليًا من {worst_setup}. راقب التوقيت والتحذيرات قبل توسيع التنفيذ.")
    else:
        lines.append("لا توجد خسائر كافية داخل الفترة الحالية.")
    return "\n".join(lines)
