from __future__ import annotations

from analysis.models import SignalCandidate
from utils.constants import MODE_COLOR_EMOJI, MODE_EXECUTION_HEADER_MAP, MODE_NORMAL_SIGNAL_HEADER_MAP


def build_signal_message(signal: SignalCandidate, execution_result: dict | None = None) -> str:
    mode_emoji = MODE_COLOR_EMOJI.get(signal.market_mode, "⚪")
    is_execution = execution_result and execution_result.get("status") in {"accepted_preview", "pending_pullback_preview"}
    header = MODE_EXECUTION_HEADER_MAP.get(signal.market_mode, f"🚀{mode_emoji} EXECUTION") if is_execution else MODE_NORMAL_SIGNAL_HEADER_MAP.get(signal.market_mode, f"📈 {mode_emoji} LONG SIGNAL")
    subtitle = "🔥 فرصة مرشحة للتنفيذ التجريبي" if is_execution else "📍 إشارة عادية — التنفيذ منفصل ويأتي لاحقًا فقط عند التأهل"
    badge = f"🚀 {mode_emoji} Execution Candidate" if is_execution else f"• {mode_emoji} Normal Signal"
    mode_hint = f"Mode Theme: {mode_emoji} {signal.market_mode}"

    entry_line = f"Market Entry: {signal.entry:.6f}" if signal.entry_timing == "market" else f"Pending Pullback: {signal.entry:.6f}"
    lines = [
        header,
        "━━━━━━━━━━━━",
        subtitle,
        f"{signal.symbol} | Score {signal.score}",
        entry_line,
        f"TP1: {signal.tp1:.6f} | TP2: {signal.tp2:.6f}",
        f"SL: {signal.sl:.6f}",
        badge,
        f"Setup: {signal.setup_type}",
        f"Market Mode: {mode_emoji} {signal.market_mode}",
        mode_hint,
        f"Entry Timing: {signal.entry_timing}",
    ]
    if signal.warnings:
        lines.append("Warnings: " + " | ".join(signal.warnings[:3]))
    if execution_result:
        lines.append(f"Execution: {execution_result.get('status')} | {execution_result.get('reason')}")
        slots = execution_result.get("slots")
        if slots:
            lines.append(f"📊 حد الصفقات: {slots['allowed']} | المفتوح المحسوب: {slots['counted']} | المتبقي: {slots['remaining']}")
    return "\n".join(lines)
