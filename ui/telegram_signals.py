from __future__ import annotations

from analysis.models import SignalCandidate
from utils.constants import (
    MODE_BLOCK_LONGS,
    MODE_COLOR_EMOJI,
    MODE_NORMAL_LONG,
    MODE_RECOVERY_LONG,
    MODE_STRONG_LONG_ONLY,
)

LIGHT_LINE = "┄┄┄┄┄┄┄┄"
EXEC_LINE = "════════════"


def _fmt_price(value: float | int | None) -> str:
    if value is None:
        return "-"
    try:
        value = float(value)
    except Exception:
        return str(value)
    if value >= 100:
        return f"{value:.2f}"
    if value >= 1:
        return f"{value:.4f}"
    return f"{value:.6f}"


def _clean_name(value: str | None) -> str:
    if not value:
        return "-"
    text = str(value).replace("_", " ").replace("|", " / ").strip()
    return " ".join(part.capitalize() if part.islower() else part for part in text.split())


def _execution_header(mode: str) -> str:
    emoji = MODE_COLOR_EMOJI.get(mode, "⚪")
    if mode == MODE_STRONG_LONG_ONLY:
        return f"🔥{emoji} STRONG EXECUTION"
    if mode == MODE_RECOVERY_LONG:
        return f"🔥{emoji} RECOVERY EXECUTION"
    if mode == MODE_BLOCK_LONGS:
        return f"🔥{emoji} BLOCK EXCEPTION"
    return f"🔥{emoji} EXECUTION"


def _normal_header(mode: str) -> str:
    # Keep normal alerts calm: no rocket, no oversized badge.
    return "📈 LONG SIGNAL"


def _execution_path(signal: SignalCandidate, execution_result: dict | None) -> str:
    if execution_result and execution_result.get("path"):
        return str(execution_result.get("path"))
    tags = signal.execution_setup_tags or []
    if any("recovery" in str(t).lower() for t in tags):
        return "recovery"
    if any("block" in str(t).lower() for t in tags):
        return "block_exception"
    if any("elite" in str(t).lower() for t in tags):
        return "elite"
    if tags:
        return "whitelist"
    return "normal_check"


def build_signal_message(signal: SignalCandidate, execution_result: dict | None = None) -> str:
    """Build the official compact Telegram signal message.

    UI-only formatting:
    - Normal signals stay calm and never look like execution failures.
    - Execution candidates keep a premium header with the current mode color.
    - If OKX orders are disabled, the message clearly says tracking/preview only.
    """
    mode_emoji = MODE_COLOR_EMOJI.get(signal.market_mode, "⚪")
    status = (execution_result or {}).get("status")
    reason = (execution_result or {}).get("reason")
    is_execution = status in {"accepted_preview", "pending_pullback_preview"}

    entry_label = "Market Entry" if signal.entry_timing == "market" else "Pullback Entry"
    setup_clean = _clean_name(signal.setup_type)
    tags_clean = ", ".join(_clean_name(t) for t in (signal.execution_setup_tags or [])[:4]) or setup_clean

    if is_execution:
        lines = [
            _execution_header(signal.market_mode),
            EXEC_LINE,
            "🔥 مرشحة للتنفيذ التجريبي",
            "🧠 Quality Filters: PASS",
            "⚡ Preview Ready",
            "",
            f"🪙 Symbol: {signal.symbol}",
            "⏱ TF: 15m",
            f"⭐ Score: {signal.score:.2f}",
            "",
            f"📍 {entry_label}: {_fmt_price(signal.entry)}",
            f"🎯 TP1: {_fmt_price(signal.tp1)}",
            f"🏁 TP2: {_fmt_price(signal.tp2)}",
            "🏃 Runner: 20% after TP2",
            f"🛡 SL: {_fmt_price(signal.sl)}",
            "",
            "┌─ 🚀 Tag Badge ─┐",
            f"Setup: {setup_clean}",
            f"Path: {_execution_path(signal, execution_result)}",
            f"Tags: {tags_clean}",
            "└───────────────┘",
            "",
            "📊 Trade Details",
            f"Setup: {setup_clean}",
            f"Entry Timing: {signal.entry_timing}",
            f"Current Wave: {signal.meta.get('wave', 'n/a')}",
            f"Volume State: {signal.meta.get('volume_state', 'n/a')}",
            f"1H Confirmation: {signal.meta.get('htf_confirmation', 'n/a')}",
            "",
            "🌐 Market",
            f"Mode: {mode_emoji} {signal.market_mode}",
            f"Theme: {mode_emoji} current mode color",
            "",
            "⚙️ Execution",
            f"Status: {status}",
            "🧪 OKX Paper Orders: OFF/controlled by Railway",
            "📌 Tracking / preview only unless OKX_PLACE_ORDERS=1",
        ]
        if signal.warnings:
            lines.extend(["", "⚠️ Notes", *[f"• {w}" for w in signal.warnings[:3]]])
        slots = (execution_result or {}).get("slots")
        if slots:
            lines.extend([
                "",
                f"📊 Slots: allowed {slots.get('allowed')} | open {slots.get('counted')} | remaining {slots.get('remaining')}",
            ])
        return "\n".join(lines)

    # Normal signal: calm style. Execution rejection is only a small check at the end.
    lines = [
        _normal_header(signal.market_mode),
        LIGHT_LINE,
        "📍 إشارة عادية — التنفيذ مسار منفصل عند التأهل فقط",
        "",
        f"🪙 Symbol: {signal.symbol}",
        "⏱ TF: 15m",
        f"⭐ Score: {signal.score:.2f}",
        "",
        f"📍 {entry_label}: {_fmt_price(signal.entry)}",
        f"🎯 TP1: {_fmt_price(signal.tp1)}",
        f"🏁 TP2: {_fmt_price(signal.tp2)}",
        "🏃 Runner: 20% after TP2",
        f"🛡 SL: {_fmt_price(signal.sl)}",
        "",
        "┌─ 🏷 Tag Badge ─┐",
        f"Setup: {setup_clean}",
        f"Context: {tags_clean}",
        "└───────────────┘",
        "",
        "📊 Trade Details",
        f"Setup: {setup_clean}",
        f"Entry Timing: {signal.entry_timing}",
        f"Current Wave: {signal.meta.get('wave', 'n/a')}",
        f"Volume State: {signal.meta.get('volume_state', 'n/a')}",
        f"1H Confirmation: {signal.meta.get('htf_confirmation', 'n/a')}",
        "",
        "🌐 Market",
        f"Mode: {mode_emoji} {signal.market_mode}",
        f"Theme: {mode_emoji} current mode color",
    ]

    if signal.warnings:
        lines.extend(["", "⚠️ Notes", *[f"• {w}" for w in signal.warnings[:3]]])

    if execution_result:
        # Do not make normal alerts look failed. It is only the execution check.
        lines.extend([
            "",
            "⚙️ Execution Check",
            f"Status: {status or 'not_candidate'}",
            f"Reason: {reason or 'normal_signal_only'}",
        ])

    return "\n".join(lines)
