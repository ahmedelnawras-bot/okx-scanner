from __future__ import annotations

from collections import Counter, defaultdict
from tracking.models import TrackedTrade


def _trade_effective_pnl(trade: TrackedTrade) -> float:
    if trade.status in {"closed_loss", "trailing_hit", "breakeven_after_tp1", "closed_win"}:
        return trade.realized_pnl_pct
    return trade.realized_pnl_pct + trade.runner_pnl_pct if trade.tp2_hit else max(trade.pnl_pct, trade.realized_pnl_pct)


def build_intelligence_report(trades: list[TrackedTrade], title: str) -> str:
    per_setup = defaultdict(list)
    filter_counts = Counter()
    exit_counts = Counter()
    for trade in trades:
        per_setup[trade.setup_type].append(trade)
        for warning in trade.warnings:
            filter_counts[warning] += 1
        if trade.tp2_hit:
            exit_counts['tp2_reached'] += 1
        elif trade.tp1_hit:
            exit_counts['tp1_reached'] += 1
        if trade.status == 'trailing_hit':
            exit_counts['trailing_exit'] += 1
        if trade.status == 'closed_loss':
            exit_counts['sl_exit'] += 1
    ranked = []
    caution = []
    for setup, items in per_setup.items():
        n = len(items)
        wins = sum(1 for t in items if _trade_effective_pnl(t) > 0)
        avg = sum(_trade_effective_pnl(t) for t in items) / max(1, n)
        wr = (wins / n) * 100 if n else 0.0
        entry = (setup, n, wr, avg)
        if wr >= 50 or avg > 0:
            ranked.append(entry)
        else:
            caution.append(entry)
    ranked.sort(key=lambda x: (x[2], x[3], x[1]), reverse=True)
    caution.sort(key=lambda x: (x[2], x[3], -x[1]))
    tp1_rate = (exit_counts['tp1_reached'] / max(1, len(trades))) * 100
    tp2_conv = (exit_counts['tp2_reached'] / max(1, exit_counts['tp1_reached'])) * 100 if exit_counts['tp1_reached'] else 0.0
    lines = [title, '━━━━━━━━━━━━', '✅ أقوى Setups']
    for setup, n, wr, avg in ranked[:4]:
        lines.append(f"• {setup} — WR {wr:.0f}% | Avg {avg:+.2f}% | n={n}")
    if caution:
        lines.append('⚠️ Setups تحتاج حذر')
        for setup, n, wr, avg in caution[:4]:
            lines.append(f"• {setup} — WR {wr:.0f}% | Avg {avg:+.2f}% | n={n}")
    if filter_counts:
        lines.append('🧱 Filters Review')
        for name, count in filter_counts.most_common(4):
            lines.append(f"• {name} — seen {count}")
    lines.append('🎯 Exit Quality')
    lines.append(f"• TP1 Rate — {tp1_rate:.0f}%")
    lines.append(f"• TP2 Conversion — {tp2_conv:.0f}%")
    lines.append(f"• Trailing Exit — {exit_counts['trailing_exit']}")
    if len(trades) < 8:
        lines.append('⚠️ العينة صغيرة — القرار غير مؤكد')
    else:
        suggestion = 'keep strongest whitelist / recovery flow active' if ranked else 'review filters and weak setups'
        lines.append('🧠 توصية مؤقتة')
        lines.append(f"• {suggestion}")
    return "\n".join(lines)
