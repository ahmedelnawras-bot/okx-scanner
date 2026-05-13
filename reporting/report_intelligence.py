from __future__ import annotations

from collections import Counter, defaultdict
from tracking.models import TrackedTrade
from reporting.report_format import SEP, behavior_summary_lines, trade_effective_pnl


def build_intelligence_report(trades: list[TrackedTrade], title: str) -> str:
    setup_groups: dict[str, list[TrackedTrade]] = defaultdict(list)
    warning_counter = Counter()
    close_counter = Counter()
    for trade in trades:
        setup_groups[trade.setup_type].append(trade)
        for warning in trade.warnings or []:
            warning_counter[warning] += 1
        close_counter[trade.status] += 1

    focus = []
    caution = []
    for setup, items in setup_groups.items():
        wins = sum(1 for t in items if trade_effective_pnl(t) > 0)
        avg = sum(trade_effective_pnl(t) for t in items) / max(1, len(items))
        wr = wins / max(1, len(items)) * 100.0
        row = (setup, len(items), wr, avg)
        if wr >= 50.0 or avg > 0:
            focus.append(row)
        else:
            caution.append(row)
    focus.sort(key=lambda x: (x[2], x[3], x[1]), reverse=True)
    caution.sort(key=lambda x: (x[2], x[3], -x[1]))

    net = sum(trade_effective_pnl(t) for t in trades)
    winners = sum(1 for t in trades if trade_effective_pnl(t) > 0)
    losers = sum(1 for t in trades if trade_effective_pnl(t) < 0)
    win_rate = winners / max(1, winners + losers) * 100.0 if (winners or losers) else 0.0
    tp1 = sum(1 for t in trades if t.tp1_hit)
    tp2 = sum(1 for t in trades if t.tp2_hit)
    direct_sl = sum(1 for t in trades if t.status == "closed_loss")

    lines: list[str] = [title, "📅 Since Start", SEP]
    lines.extend([
        "📊 <b>Executive Summary</b>",
        f"• Trades: {len(trades)}",
        f"• Win Rate: {win_rate:.1f}%",
        f"• Net Impact: {net:+.2f}% Exposure",
        f"• Direct SL: {direct_sl}",
        f"• TP1 Rate: {tp1 / max(1, len(trades)) * 100:.1f}%",
        f"• TP2 Rate: {tp2 / max(1, len(trades)) * 100:.1f}%",
    ])

    lines.extend([SEP, "🎯 <b>Focus Setups</b>"])
    if focus:
        for setup, n, wr, avg in focus[:5]:
            lines.append(f"• {setup} — WR {wr:.0f}% | Avg {avg:+.2f}% | n={n}")
    else:
        lines.append("• لا توجد setup قوية كفاية حتى الآن.")

    lines.extend([SEP, "⚠️ <b>Watch Carefully</b>"])
    if caution:
        for setup, n, wr, avg in caution[:5]:
            lines.append(f"• {setup} — WR {wr:.0f}% | Avg {avg:+.2f}% | n={n}")
    elif warning_counter:
        for warning, count in warning_counter.most_common(5):
            lines.append(f"• {warning} — {count}")
    else:
        lines.append("• لا توجد تحذيرات كافية حتى الآن.")

    lines.extend([SEP, "🧪 <b>Recommended Tuning</b>"])
    if direct_sl > winners:
        lines.append("• راقب Direct SL قبل توسيع التنفيذ.")
        lines.append("• mid/late entries يفضل Pullback-first.")
    elif focus:
        lines.append("• حافظ على أقوى setups بدون توسيع عشوائي للـ whitelist.")
        lines.append("• راقب التحويل من TP1 إلى TP2 قبل زيادة المخاطرة.")
    else:
        lines.append("• العينة ما زالت صغيرة؛ لا تغيّر الفلاتر الأساسية الآن.")

    lines.extend([SEP, "💡 <b>Decision</b>"])
    if not trades:
        lines.append("لا توجد صفقات كافية لإصدار قرار ذكي.")
    elif net >= 0 and win_rate >= 45:
        lines.append("التنفيذ قابل للاستمرار، لكن يفضل تضييق انتقائي وليس فتح كامل.")
    else:
        lines.append("الأداء يحتاج مراقبة وتضييق جودة قبل أي توسع في التنفيذ.")
    return "\n".join(lines)
