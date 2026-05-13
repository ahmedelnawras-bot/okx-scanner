from __future__ import annotations

from analysis.market_modes import block_protection_status, MarketModeState
from utils.constants import *

LIGHT_LINE = "┄┄┄┄┄┄┄┄"
TITLE_LINE = "━━━━━━━━━━━━"


def _mix_action(mode: str, context: dict) -> str:
    mix = str(context.get("market_mix", "")).upper()
    if mode == MODE_BLOCK_LONGS:
        return "🚫 الضعيف ممنوع — استثناءات قوية فقط"
    if mode == MODE_RECOVERY_LONG:
        return "🔄 ارتداد سريع — Recovery path فقط"
    if "CHOPPY" in mix:
        return "⚠️ mtf_no يحتاج جودة أعلى"
    if "MIXED" in mix:
        return "⚠️ جودة أعلى قبل التنفيذ"
    return "✅ القواعد العادية فعالة"


def _signal_rules(mode: str, context: dict) -> list[str]:
    if mode == MODE_BLOCK_LONGS:
        return [
            "Normal longs: OFF",
            "Block exceptions: ON فقط للجودة العالية",
            "Recovery: ينتظر ارتداد واضح",
        ]
    if mode == MODE_RECOVERY_LONG:
        return [
            "Recovery signals: ON",
            "Max recovery trades/cycle: 3",
            "Normal strong routing لا يخنق الريكافر",
        ]
    if mode == MODE_STRONG_LONG_ONLY:
        return [
            "Weak normal signals: filtered",
            "Strong signals: ON",
            "Execution: whitelist / elite only",
        ]
    return [
        "Normal signals: ON",
        "Execution check: separate path",
        "Weak drift: execution-only",
    ]


def build_market_mode_sections(mode: str, context: dict, variant: str) -> str:
    """Unified market-mode message builder.

    Variants:
    - status: /mood
    - transition: mode change alert
    - reminder: periodic market reminder
    """
    mode_emoji = MODE_COLOR_EMOJI.get(mode, "⚪")
    lines: list[str] = []

    if variant == "transition":
        lines.append(f"{mode_emoji} Market Mode Update")
        lines.append(f"🔁 {context.get('old_mode', '?')} → {mode}")
    elif variant == "reminder":
        lines.append(f"{mode_emoji} Market Reminder #{context.get('reminder_count', 1)}")
        lines.append(f"⏱ {context.get('minutes_in_mode', 0)}m in {mode}")

    lines.append(MODE_TITLE_MAP.get(mode, f"{mode_emoji} Market Mode: {mode}"))
    lines.append(TITLE_LINE)
    lines.append(f"🧩 Mode Color: {mode_emoji} {mode}")

    market_mix = context.get("market_mix", "N/A")
    lines.extend([
        "",
        "🌗 Market Mix",
        f"Status: {market_mix}",
        f"Action: {_mix_action(mode, context)}",
        "",
        "🌐 Market State",
        str(context.get("market_state", "N/A")),
    ])

    if context.get("trigger"):
        lines.extend(["", "⚡ Trigger", str(context["trigger"])])

    lines.extend([
        "",
        "🧠 Mode Reason",
        str(context.get("mode_reason", "core market breadth decision")),
        "",
        "📌 Signal Rules",
        *[f"• {line}" for line in _signal_rules(mode, context)],
        "",
        "⚙️ Execution Notes",
        f"Execution candidates: {context.get('execution_notes', 'whitelist + quality gates')}",
        "OKX Paper Orders: controlled by Railway",
        "Live Trading: BLOCKED unless explicitly enabled",
    ])

    if mode == MODE_BLOCK_LONGS:
        current = context.get("protection_current", "LEVEL 1 — Monitor Only")
        next_label = context.get("protection_next", "Soft Protection")
        remaining = context.get("remaining_minutes", 0)
        lines.extend([
            "",
            "🛡 Protection Plan",
            "• Level 1 → Monitor only",
            "• Level 2 → Soft Protection",
            "• Level 3 → Defensive Protection",
            "",
            f"Protection: {current}",
        ])
        if remaining:
            lines.append(f"Next: {next_label} in ~{remaining}m")
        else:
            lines.append("Next: Max protection active")

    if mode == MODE_RECOVERY_LONG:
        lines.extend([
            "",
            "🪟 Recovery Window",
            "Duration: 90m",
            f"Remaining slots: {context.get('recovery_remaining', 3)}",
            "",
            "📌 Recovery Rules",
            "• Fast rebound only",
            "• Max 3 recovery trades/cycle",
            "• Special recovery path active",
        ])

    return "\n".join(lines)


def build_block_escalation_alert(state: MarketModeState, affected: int = 0, protected: int = 0, tightened: int = 0) -> str:
    protection = block_protection_status(state)
    level_title = "🛡 تفعيل حماية البلوك" if protection["level"] <= 2 else "🛡 تصعيد حماية البلوك"
    level_badge = (
        "🟠 المستوى 2 — حماية مرنة"
        if protection["level"] == 2
        else "🔴 المستوى 3 — حماية دفاعية"
        if protection["level"] >= 3
        else "🟡 المستوى 1 — مراقبة"
    )
    action = (
        "حماية الأرباح الحالية وتشديد trailing للـ runners"
        if protection["level"] == 2
        else "تشديد حماية الأرباح ومراقبة الـ runner بدقة"
        if protection["level"] >= 3
        else "مراقبة فقط بدون تعديل"
    )
    tail = (
        f"⏭ الحماية التالية: {protection['next']} خلال ~{protection['remaining_minutes']}m"
        if protection["remaining_minutes"]
        else "✅ أقصى مستوى حماية مفعل"
    )
    return "\n".join([
        level_title,
        LIGHT_LINE,
        level_badge,
        f"📊 الصفقات المتأثرة: {affected}",
        f"✅ الأرباح المحمية: {protected}",
        f"🔧 Runners تحت حماية مشددة: {tightened}",
        "⚪ الصفقات السلبية ما زالت على SL الأصلي",
        f"⚙️ الإجراء: {action}",
        tail,
    ])
