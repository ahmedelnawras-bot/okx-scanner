from __future__ import annotations

from analysis.market_modes import block_protection_status, MarketModeState
from utils.constants import *

LIGHT_LINE = "┄┄┄┄┄┄┄┄"
TITLE_LINE = "━━━━━━━━━━━━"


_MODE_AR_STATUS = {
    MODE_NORMAL_LONG: "السوق يسمح بإشارات Long عادية مع فلترة الجودة.",
    MODE_STRONG_LONG_ONLY: "السوق غير ممنوع، لكن يحتاج فرص أقوى فقط.",
    MODE_BLOCK_LONGS: "فتح Long جديد متوقف بسبب ضغط واضح في السوق.",
    MODE_RECOVERY_LONG: "ارتداد سريع بعد ضغط قوي، وفرص الريكفري فقط تحت المتابعة.",
}

_MODE_AR_REASON = {
    MODE_NORMAL_LONG: "اتساع السوق مقبول ولا يوجد ضغط كافي لإيقاف اللونج.",
    MODE_STRONG_LONG_ONLY: "السوق متذبذب أو غير واضح، لذلك يتم رفع جودة القبول.",
    MODE_BLOCK_LONGS: "نسبة الهبوط أو ضعف السوق أعلى من المسموح للونج العادي.",
    MODE_RECOVERY_LONG: "ظهر ارتداد سريع بعد ضغط، لكن السوق لم يرجع طبيعي بالكامل.",
}

_MODE_AR_EXECUTION = {
    MODE_NORMAL_LONG: "التنفيذ مسار منفصل بعد الإشارة، ويمر عبر whitelist + quality gates.",
    MODE_STRONG_LONG_ONLY: "التنفيذ متاح فقط للفرص الأقوى عبر whitelist / elite.",
    MODE_BLOCK_LONGS: "التنفيذ العادي متوقف؛ الاستثناءات القوية فقط هي المسموحة.",
    MODE_RECOVERY_LONG: "مسار الريكفري فعال بحد أقصى 3 فرص في الدورة.",
}


def _fmt_pct(value, decimals: int = 2) -> str:
    try:
        v = float(value)
    except Exception:
        return str(value or "0%")
    # Defensive display fix: if a tiny internal ratio sneaks in, show it as percent.
    # If value already looks like percent, keep it. This prevents duplicated ×100.
    if -1.0 < v < 1.0 and abs(v) > 0:
        return f"{v:.{decimals}f}%"
    return f"{v:.{decimals}f}%"


def _mode_color_identity(mode: str) -> str:
    emoji = MODE_COLOR_EMOJI.get(mode, "⚪")
    names = {
        MODE_NORMAL_LONG: "Normal",
        MODE_STRONG_LONG_ONLY: "Strong — light yellow",
        MODE_BLOCK_LONGS: "Block",
        MODE_RECOVERY_LONG: "Recovery",
    }
    return f"{emoji} {names.get(mode, mode)}"


def _mix_action(mode: str, context: dict) -> str:
    mix_label = str(context.get("mix_label", "") or context.get("market_mix", "")).upper()
    if mode == MODE_BLOCK_LONGS:
        return "🚫 الضعيف ممنوع — استثناءات قوية فقط"
    if mode == MODE_RECOVERY_LONG:
        return "🔄 ارتداد سريع — Recovery path فقط"
    if mode == MODE_STRONG_LONG_ONLY:
        return "⚠️ قبول أقوى فقط — لا يعتبر Block"
    if "CHOPPY" in mix_label or "MIXED" in mix_label:
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
            "Normal routing: محدود حتى استقرار السوق",
        ]
    if mode == MODE_STRONG_LONG_ONLY:
        return [
            "Weak normal signals: filtered",
            "Strong signals: ON",
            "Execution: whitelist / elite only",
        ]
    return [
        "Normal Signals: ON",
        "Execution Check: مسار منفصل بعد الإشارة",
        "Weak Drift: خاص بالتنفيذ فقط",
    ]


def _market_mix_lines(context: dict) -> list[str]:
    if any(k in context for k in ("strong_coins", "red_ratio", "avg15m")):
        return [
            f"• Strong Coins: {context.get('strong_coins', 0)}",
            f"• Red Ratio: {_fmt_pct(context.get('red_ratio', 0.0), 0)}",
            f"• Avg 15m Move: {_fmt_pct(context.get('avg15m', 0.0), 2)}",
            f"• BTC 1h MA5 Guard: {'⚠️ pressure' if context.get('hourly_ma5_pressure') else '✅ clear'} ({_fmt_pct(context.get('btc_1h_ma5_gap_pct', 0.0), 2)})",
            f"• Action: {_mix_action(str(context.get('mode', '')), context)}" if context.get("mode") else f"• Action: {context.get('action', '') or _mix_action('', context)}",
        ]
    return [
        f"Status: {context.get('market_mix', 'N/A')}",
        f"Action: {_mix_action('', context)}",
    ]



def _status_icon_pct(value: float, good_threshold: float = 0.10, bad_threshold: float = -0.25) -> str:
    try:
        v = float(value)
    except Exception:
        return "⚪"
    if v >= good_threshold:
        return "🟢"
    if v <= bad_threshold:
        return "🔴"
    return "🟡"


def _alts_status(red_ratio: float, avg15m: float) -> tuple[str, str]:
    if red_ratio >= 65 or avg15m <= -0.70:
        return "🔴", "ضعيف"
    if red_ratio <= 45 and avg15m >= 0.05:
        return "🟢", "جيد"
    return "🟡", "متذبذب"


def _mix_label(mode: str, context: dict) -> str:
    avg = float(context.get("avg15m", 0.0) or 0.0)
    red = float(context.get("red_ratio", 0.0) or 0.0)
    if mode == MODE_BLOCK_LONGS:
        return "RISK-OFF"
    if mode == MODE_RECOVERY_LONG:
        return "RECOVERY"
    if mode == MODE_STRONG_LONG_ONLY or red >= 55 or avg < -0.20:
        return "CHOPPY"
    return "CLEAR"


def _focus_lines(mode: str) -> list[str]:
    if mode == MODE_BLOCK_LONGS:
        return ["• no normal longs", "• block exceptions only", "• recovery watching"]
    if mode == MODE_RECOVERY_LONG:
        return ["• fast rebound only", "• recovery path active", "• max 3 recovery slots"]
    if mode == MODE_STRONG_LONG_ONLY:
        return ["• reclaim / retest / wave_3", "• whitelist / elite only"]
    return ["• normal signals allowed", "• execution check separate"]


def _execution_status_lines(mode: str, context: dict) -> list[str]:
    if mode == MODE_BLOCK_LONGS:
        return ["• Normal execution: OFF", "• Block exceptions: ON", "• Recovery: watching rebound"]
    if mode == MODE_RECOVERY_LONG:
        return ["• Recovery: ACTIVE", "• Normal execution: limited", "• Weak Drift: checked"]
    if mode == MODE_STRONG_LONG_ONLY:
        return ["• Whitelist/Elite: ACTIVE", "• Weak Drift: ON"]
    return ["• Whitelist: ACTIVE", "• Weak Drift: execution-only"]


def _build_compact_market_reminder(mode: str, context: dict) -> str:
    mode_emoji = MODE_COLOR_EMOJI.get(mode, "⚪")
    minutes = int(context.get("minutes_in_mode", 0) or 0)
    hours, mins = divmod(minutes, 60)
    duration = f"{hours}h {mins}m" if hours else f"{mins}m"
    avg = float(context.get("avg15m", 0.0) or 0.0)
    btc = float(context.get("btc15m", context.get("btc_change_15m", avg)) or 0.0)
    red = float(context.get("red_ratio", 0.0) or 0.0)
    strong = int(context.get("strong_coins", 0) or 0)
    scanned = int(context.get("scanned_pairs", context.get("sample_size", 200)) or 200)
    btc_icon = _status_icon_pct(btc, 0.10, -0.35)
    alts_icon, alts_label = _alts_status(red, avg)
    mix = _mix_label(mode, context)
    mix_action = (
        "🚫 longs blocked" if mode == MODE_BLOCK_LONGS else
        "🔄 rebound only" if mode == MODE_RECOVERY_LONG else
        "🚫 weak mtf_no filtered" if mode == MODE_STRONG_LONG_ONLY else
        "✅ normal rules active"
    )
    protection = context.get("protection_current") or ("Normal monitoring" if mode != MODE_BLOCK_LONGS else "LEVEL 1 — Monitor Only")
    remaining = context.get("remaining_minutes", 0)
    protection_tail = f"\n⏭ {context.get('protection_next', 'Next')} in ~{remaining}m" if remaining else ""

    open_winners = int(context.get("open_winners", 0) or 0)
    protected_runners = int(context.get("protected_runners", 0) or 0)
    danger_trades = int(context.get("danger_trades", 0) or 0)
    counted_open_positions = int(
        context.get(
            "counted_open_positions",
            context.get(
                "open_positions_count",
                open_winners + danger_trades + protected_runners,
            ),
        ) or 0
    )

    lines = [
        f"{mode_emoji} <b>Market Reminder #{context.get('reminder_count', 1)}</b>",
        f"⏱ {duration} in {mode}",
        TITLE_LINE,
        f"🌪 <b>Mix:</b> {mix} | {mix_action}",
        "",
        "📊 <b>Market Snapshot</b>",
        f"• BTC: {btc_icon} {btc:+.2f}%",
        f"• Alts: {alts_icon} {alts_label}",
        f"• Red: {red:.0f}% | Avg: {avg:+.2f}%",
        f"• Strong Setups: {strong} / {scanned}",
        "",
        "⚡ <b>Focus</b>",
        *_focus_lines(mode),
        "",
        "📂 <b>Open Positions</b>",
        f"• Counted Open: {counted_open_positions}",
        f"🟢 Winners: {open_winners} | 🟡 Protected Runners: {protected_runners} | 🔴 Danger: {danger_trades}",
        "",
        "📈 <b>Scan Summary</b>",
        f"• Signals: {context.get('signals_count', 0)}",
        f"• Exec Accepted: {context.get('exec_accepted', 0)}",
        f"• Rejects: {context.get('rejects_count', 0)}",
        "",
        "⚠️ <b>Top Reject</b>",
        str(context.get("top_reject", "n/a")),
        "",
        "🚀 <b>Execution</b>",
        *_execution_status_lines(mode, context),
        "",
        "🛡 <b>Protection</b>",
        f"{protection}{protection_tail}",
    ]
    return "\n".join(lines)


def build_market_mode_sections(mode: str, context: dict, variant: str) -> str:
    """Unified market-mode message builder.

    UI-only. v127 keeps the full /mood details but organizes the lower section
    and fixes Market Mix percent display.
    """
    context = dict(context or {})
    context.setdefault("mode", mode)
    if variant == "reminder":
        return _build_compact_market_reminder(mode, context)
    mode_emoji = MODE_COLOR_EMOJI.get(mode, "⚪")
    lines: list[str] = []

    if variant == "transition":
        lines.append(f"{mode_emoji} Market Mode Update")
        lines.append(f"🔁 {context.get('old_mode', '?')} → {mode}")
        lines.append(LIGHT_LINE)
    elif variant == "reminder":
        lines.append(f"{mode_emoji} Market Reminder #{context.get('reminder_count', 1)}")
        lines.append(f"⏱ {context.get('minutes_in_mode', 0)}m in {mode}")
        lines.append(LIGHT_LINE)

    lines.append(MODE_TITLE_MAP.get(mode, f"{mode_emoji} Market Mode: {mode}"))
    lines.append(TITLE_LINE if variant == "status" else LIGHT_LINE)
    lines.append(f"🧩 Mode Color: {_mode_color_identity(mode)}")

    lines.extend([
        "",
        "📌 الحالة العامة",
        _MODE_AR_STATUS.get(mode, "حالة السوق تحت المتابعة."),
        "",
        "🌗 Market Mix",
        *_market_mix_lines(context),
        "",
        "🌐 Market State",
        f"• Strong Coins: {context.get('strong_coins', 0)}",
        f"• Avg 15m: {_fmt_pct(context.get('avg15m', 0.0), 2)}",
        f"• Red Ratio: {_fmt_pct(context.get('red_ratio', 0.0), 0)}",
        f"• BTC 1h MA5: {'pressure' if context.get('hourly_ma5_pressure') else 'clear'} ({_fmt_pct(context.get('btc_1h_ma5_gap_pct', 0.0), 2)})",
    ])

    trigger = context.get("trigger")
    if trigger:
        lines.extend(["", "⚡ Trigger", str(trigger)])

    lines.extend([
        "",
        "🧠 سبب المود",
        _MODE_AR_REASON.get(mode, str(context.get("mode_reason", "core market breadth decision"))),
        f"Reason: {context.get('mode_reason', 'core market breadth decision')}",
        "",
        "📈 قواعد الإشارات",
        *[f"• {line}" for line in _signal_rules(mode, context)],
        "",
        "⚙️ التنفيذ",
        f"• الحالة: {_MODE_AR_EXECUTION.get(mode, 'التنفيذ يتبع إعدادات الجودة الحالية.')}",
        f"• Execution Candidates: {context.get('execution_notes', 'whitelist + quality gates')}",
        "• OKX Orders: حسب إعداد Railway الحالي",
        "• Live Trading: BLOCKED unless explicitly enabled",
    ])

    if mode == MODE_BLOCK_LONGS:
        current = context.get("protection_current", "LEVEL 1 — Monitor Only")
        next_label = context.get("protection_next", "Soft Protection")
        remaining = context.get("remaining_minutes", 0)
        lines.extend([
            "",
            "🛡 Protection Plan",
            "• Level 1 → مراقبة فقط",
            "• Level 2 → حماية أرباح",
            "• Level 3 → حماية دفاعية",
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
            "Max trades: 3 per cycle",
            "",
            "📌 Recovery Rules",
            "• Fast rebound only",
            "• مسار Recovery مستقل ولا يخنقه Strong routing",
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
