from __future__ import annotations

from collections import Counter
from tracking.models import TrackedTrade
from reporting.report_format import SEP, filter_trades_by_period, period_label, score_range, trade_effective_pnl

PROFIT_STATUSES = {"tp1_partial", "tp2_partial", "runner", "trailing_hit", "closed_win", "breakeven_after_tp1"}


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
    if "pullback" in warnings:
        return "🟢 صحي"
    return "🟡 متوسط (نص الحركة)"


def build_profit_analysis_report(
    trades: list[TrackedTrade],
    title: str = "📈 تحليل أسباب أرباح التنفيذ",
    period: str = "since_start",
) -> str:
    trades = filter_trades_by_period(trades or [], period)
    profitable = [t for t in trades if t.status in PROFIT_STATUSES or trade_effective_pnl(t) > 0]
    setup_counter = Counter(t.setup_type for t in profitable)
    market_counter = Counter(_market_state_label(t) for t in profitable)
    timing_counter = Counter(_entry_timing_label(t) for t in profitable)
    score_counter = Counter(score_range(float(t.score or 0)) for t in profitable)
    reason_counter = Counter(w for t in profitable for w in (t.warnings or []))
    close_counter = Counter(t.status for t in profitable)
    avg_profit = sum(max(trade_effective_pnl(t), 0.0) for t in profitable) / max(1, len(profitable))
    tp1_only = sum(1 for t in profitable if t.tp1_hit and not t.tp2_hit)
    tp2_runner = sum(1 for t in profitable if t.tp2_hit or t.runner_active)

    lines = [title, f"📅 {period_label(period)}", SEP]
    lines.extend([
        "📊 <b>Profit Summary</b>",
        f"• Trades inside period: {len(trades)}",
        f"• Winners inside period: {len(profitable)}",
        f"📈 Avg Winner: {avg_profit:+.2f}%",
        f"🎯 TP1 Only: {tp1_only}",
        f"🏁 TP2 + Trail Win: {tp2_runner}",
    ])
    sections = [
        ("🧠 <b>Best Winning Setups</b>", setup_counter),
        ("🌐 <b>Best Market States</b>", market_counter),
        ("⏱️ <b>Entry Timing</b>", timing_counter),
        ("⭐ <b>Score Ranges</b>", score_counter),
        ("🧩 <b>Top Reasons</b>", reason_counter),
        ("🏁 <b>Close Types</b>", close_counter),
    ]
    for heading, counter in sections:
        if counter:
            lines.extend([SEP, heading])
            for name, count in counter.most_common(5):
                lines.append(f"• {name} — {count}")
    lines.extend([SEP, "💡 <b>الخلاصة</b>"])
    if profitable:
        best_setup = setup_counter.most_common(1)[0][0] if setup_counter else "غير محدد"
        lines.append(f"أفضل الأرباح جاءت غالبًا من {best_setup} مع الحفاظ على نموذج 40/40/20.")
    else:
        lines.append("لا توجد صفقات رابحة كافية داخل الفترة الحالية.")
    return "\n".join(lines)
