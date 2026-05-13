from __future__ import annotations

from analysis.market_modes import block_protection_status, MarketModeState
from utils.constants import *


def build_market_mode_sections(mode: str, context: dict, variant: str) -> str:
    mode_emoji = MODE_COLOR_EMOJI.get(mode, "⚪")
    lines: list[str] = []
    if variant == "transition":
        lines.append(f"{mode_emoji} Market Mode Transition")
        lines.append(f"🔁 {context.get('old_mode', '?')} → {mode}")
    elif variant == "reminder":
        lines.append(f"{mode_emoji} Market Reminder #{context.get('reminder_count', 1)}")
        lines.append(f"⏱ {context.get('minutes_in_mode', 0)}m in {mode}")
    lines.append(MODE_TITLE_MAP.get(mode, mode))
    lines.append("━━━━━━━━━━━━")
    lines.append(f"🎨 Theme: {mode_emoji} {mode}\n🧩 Mode Color Identity active")
    if context.get("market_mix"):
        lines.append(f"🌪 Market Mix: {context['market_mix']}")
    lines.append(f"📈 Market State: {context.get('market_state', 'N/A')}")
    if context.get("trigger"):
        lines.append(f"⚡ Trigger: {context['trigger']}")
    if context.get("mode_reason"):
        lines.append(f"🧠 Mode Reason: {context['mode_reason']}")
    lines.append(f"🎯 Signal Rules: {context.get('signal_rules', 'normal signal first → execution later')}")
    if context.get("requirements"):
        lines.append(f"✅ Requirements: {context['requirements']}")
    if context.get("execution_notes"):
        lines.append(f"⚙️ Execution Notes: {context['execution_notes']}")
    if mode == MODE_BLOCK_LONGS:
        current = context.get("protection_current", "LEVEL 1 — Monitor Only")
        next_label = context.get("protection_next", "Soft Protection")
        remaining = context.get("remaining_minutes", 0)
        lines.append("🛡 Protection Plan")
        lines.append(f"• Protection: {current}")
        lines.append(f"• Theme: {mode_emoji} block mode active")
        if remaining:
            lines.append(f"• {next_label} in ~{remaining}m")
        else:
            lines.append("• Max protection active")
    if mode == MODE_RECOVERY_LONG:
        lines.append("⚡ Recovery Window: 90m")
        lines.append(f"⚙️ Recovery Rules: max 3 trades / cycle | remaining {context.get('recovery_remaining', 3)}")
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
        level_badge,
        f"📊 الصفقات المتأثرة: {affected}",
        f"✅ الأرباح المحمية: {protected}",
        f"🔧 Runners تحت حماية مشددة: {tightened}",
        "⚪ الصفقات السلبية ما زالت على SL الأصلي",
        f"⚙️ الإجراء: {action}",
        tail,
    ])
