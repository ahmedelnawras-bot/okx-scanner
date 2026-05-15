from __future__ import annotations


def build_main_menu_layout() -> list[list[str]]:
    """Official main Telegram menu layout.

    v124: This is used as InlineKeyboardMarkup, not ReplyKeyboardMarkup,
    so it does not open the big persistent Telegram keyboard.
    """
    return [
        ["🚀 Execution", "📊 Normal Trades"],
        ["🧠🚀 Execution Intelligence", "🧠📊 Market Intelligence"],
        ["💼 Wallet Impact"],
        ["🧠 Diagnostics", "🤖 OKX Control"],
        ["⚙️ Admin", "📘 System Info"],
    ]


def build_main_reply_keyboard() -> dict:
    """Deprecated compatibility helper.

    Kept so older imports do not break, but /help should NOT use this anymore.
    Returning remove_keyboard prevents the old persistent keyboard from staying open.
    """
    return {"remove_keyboard": True}


def build_main_inline_keyboard() -> dict:
    """Inline buttons under /help dashboard, matching the old approved style."""
    return {
        "inline_keyboard": [
            [
                {"text": "🚀 Execution", "callback_data": "menu:execution"},
                {"text": "📊 Normal Trades", "callback_data": "menu:normal"},
            ],
            [
                {"text": "🧠🚀 Execution Intelligence", "callback_data": "cmd:/report_execution_intelligence"},
                {"text": "🧠📊 Market Intelligence", "callback_data": "cmd:/report_intelligence"},
            ],
            [
                {"text": "💼 Wallet Impact", "callback_data": "cmd:/report_execution_wallet"},
            ],
            [
                {"text": "🧠 Diagnostics", "callback_data": "menu:diagnostics"},
                {"text": "🤖 OKX Control", "callback_data": "menu:okx_control"},
            ],
            [
                {"text": "⚙️ Admin", "callback_data": "menu:admin"},
                {"text": "📘 System Info", "callback_data": "menu:system_info"},
            ],
        ]
    }


def _mode_line(mode: str | None = None) -> str:
    mode = mode or "NORMAL_LONG"
    if mode == "STRONG_LONG_ONLY":
        return "📈 Market Mode: 🟨 STRONG_LONG_ONLY"
    if mode == "BLOCK_LONGS":
        return "📈 Market Mode: 🔴 BLOCK_LONGS"
    if mode == "RECOVERY_LONG":
        return "📈 Market Mode: 🔵 RECOVERY_LONG"
    return "📈 Market Mode: 🟢 NORMAL_LONG"


def _system_status_block(
    mode: str | None = None,
    execution_enabled: bool = True,
    risk_enabled: bool = True,
    okx_orders: bool = False,
) -> list[str]:
    return [
        "🧭 System Status",
        "",
        _mode_line(mode),
        f"⚡ Execution Engine: {'ACTIVE' if execution_enabled else 'PAUSED'}",
        f"🛡 Risk Protection: {'ENABLED' if risk_enabled else 'DISABLED'}",
        f"🧪 OKX Orders: {'ON' if okx_orders else 'OFF'}",
        "",
    ]


def build_execution_help() -> str:
    lines = [
        "🚀 صفقات التنفيذ",
        "📘 /help_execution",
        "┄┄┄┄┄┄┄┄",
        "📊 التقرير العام",
        "/report_execution",
        "/report_execution_7d",
        "/report_execution_today",
        "/report_execution_1h",
        "",
        "📂 الصفقات المفتوحة",
        "/report_execution_open",
        "/report_execution_open_7d",
        "/report_execution_open_today",
        "/report_execution_open_1h",
        "",
        "📈 تحليل أسباب الأرباح",
        "/report_execution_profit_analysis",
        "/report_execution_profit_analysis_7d",
        "/report_execution_profit_analysis_today",
        "/report_execution_profit_analysis_1h",
        "",
        "📉 تحليل أسباب الخسائر",
        "/report_execution_losses_analysis",
        "/report_execution_losses_analysis_7d",
        "/report_execution_losses_analysis_today",
        "/report_execution_losses_analysis_1h",
        "",
        "💼 Wallet Impact",
        "/report_execution_wallet",
        "/report_execution_wallet_7d",
        "/report_execution_wallet_today",
        "/report_execution_wallet_1h",
        "",
        "🧠 ذكاء التنفيذ",
        "/report_execution_intelligence",
        "/report_execution_intelligence_7d",
        "/report_execution_intelligence_today",
        "/report_execution_intelligence_1h",
        "",
        "⚙️ تشخيص التنفيذ",
        "/report_execution_diagnostics",
        "/report_execution_diagnostics_7d",
        "/report_execution_diagnostics_today",
        "/report_execution_diagnostics_1h",
    ]
    return "\n".join(lines)


def build_normal_help() -> str:
    lines = [
        "📊 الصفقات العادية",
        "📘 /help_normal",
        "┄┄┄┄┄┄┄┄",
        "📊 التقرير العام",
        "/report_all",
        "/report_all_7d",
        "/report_all_today",
        "/report_all_1h",
        "",
        "📂 الصفقات المفتوحة",
        "/open_trades",
        "/open_trades_7d",
        "/open_trades_today",
        "/open_trades_1h",
        "",
        "📈 تحليل أسباب الأرباح",
        "/report_profit_analysis",
        "/report_profit_analysis_7d",
        "/report_profit_analysis_today",
        "/report_profit_analysis_1h",
        "",
        "📉 تحليل أسباب الخسائر",
        "/report_losses_analysis",
        "/report_losses_analysis_7d",
        "/report_losses_analysis_today",
        "/report_losses_analysis_1h",
        "",
        "🧠 Market Intelligence",
        "/report_intelligence",
        "/report_intelligence_7d",
        "/report_intelligence_today",
        "/report_intelligence_1h",
        "",
        "⚙️ تشخيص السوق",
        "/report_diagnostics",
        "/report_diagnostics_7d",
        "/report_diagnostics_today",
        "/report_diagnostics_1h",
    ]
    return "\n".join(lines)


def build_diagnostics_help() -> str:
    return "\n".join([
        "🧠 Diagnostics",
        "┄┄┄┄┄┄┄┄",
        "/diagnostics_help",
        "↳ شرح عملي لكل أوامر Diagnostics ومتى تستخدم كل أمر.",
        "/report_diagnostics",
        "↳ تشخيص آخر الإشارات والفلاتر بدون تغيير التنفيذ.",
        "/report_execution_diagnostics",
        "↳ تشخيص سبب قبول/رفض مرشحات التنفيذ.",
        "",
        "━━━━━━━━━━━━",
        "🧠 Technical Dataset / Live AI Data",
        "/help_technical_dataset",
        "↳ يعرض أوامر داتا الإشارات الحية.",
        "/tech_snapshot_status",
        "↳ حالة التسجيل وعدد السجلات حسب المود والنوع.",
        "/tech_snapshot_on",
        "↳ يشغل تسجيل snapshots من البوت الحي.",
        "/tech_snapshot_off",
        "↳ يوقف التسجيل فقط بدون حذف البيانات.",
        "/tech_snapshot_export",
        "↳ يعرض ملخص ومعاينة لمسار داتا Live AI.",
        "/tech_snapshot_export_file",
        "↳ يرسل ملف live snapshots مضغوط ZIP.",
        "/tech_snapshot_clear",
        "↳ يمسح داتا snapshots فقط ولا يلمس الصفقات.",
        "/gate_suggestions",
        "↳ قراءة مبدئية للبوابات بدون تطبيق أي قواعد.",
        "/gate_sim_normal",
        "↳ يحاكي بوابة NORMAL على replay + live ويرسل تقرير + JSON.",
        "/gate_sim_recovery",
        "↳ يحاكي بوابة RECOVERY على replay + live ويرسل تقرير + JSON.",
        "/gate_sim_strong",
        "↳ يحاكي بوابة STRONG على replay + live ويرسل تقرير + JSON.",
        "/gate_sim_block",
        "↳ يحاكي استثناءات BLOCK_LONGS على replay + live ويرسل تقرير + JSON.",
        "/gate_sim_all",
        "↳ ملخص سريع لمحاكاة كل البوابات ويرسل JSON شامل.",
        "",
        "━━━━━━━━━━━━",
        "🕰 Historical Replay Engine",
        "/help_historical_replay",
        "↳ يعرض أوامر محرك التاريخ فقط.",
        "/replay_start_30d",
        "↳ يشغل محاكاة آخر 30 يوم على بيانات OKX.",
        "/replay_start_45d",
        "↳ يشغل محاكاة آخر 45 يوم عند الحاجة.",
        "/replay_start_90d",
        "↳ يشغل محاكاة آخر 90 يوم؛ أثقل وأبطأ لكنه أدق للاختبار.",
        "/replay_status",
        "↳ يعرض حالة المحرك والتقدم والسجلات.",
        "/replay_stop",
        "↳ يوقف المحرك بأمان عند أقرب checkpoint.",
        "/replay_export",
        "↳ يعرض ملخص ومعاينة لداتا replay الخام.",
        "/replay_export_file",
        "↳ يرسل replay dataset مضغوط ZIP.",
        "/replay_summary",
        "↳ يعرض ملخص النتائج و TP/SL حسب الداتا المتاحة.",
        "/replay_clear",
        "↳ يمسح داتا Replay فقط ولا يلمس live snapshots.",
        "/compare_live_vs_replay",
        "↳ يقارن توزيع live snapshots مع replay.",
        "",
        "📌 هذه الأقسام للقياس والتحليل فقط ولا تغير التنفيذ.",
    ])


def build_diagnostics_commands_help() -> str:
    """Detailed diagnostics command reference. Kept as a function so /diagnostics_help can be routed directly."""
    return build_diagnostics_help()


def build_okx_control_help() -> str:
    return "\n".join([
        "🤖 OKX Control",
        "┄┄┄┄┄┄┄┄",
        "/status — حالة البوت والتنفيذ",
        "/mood — حالة السوق الحالية",
        "/report_execution_open — صفقات التنفيذ المفتوحة",
        "",
        "🧪 OKX Orders يتم التحكم فيه من Railway Variables.",
        "🔒 Live Trading يظل BLOCKED إلا لو تم تفعيله صراحة.",
    ])


def build_admin_help() -> str:
    return "\n".join([
        "⚙️ Admin",
        "┄┄┄┄┄┄┄┄",
        "/status — فحص سريع للبوت",
        "/mood — حالة المود",
        "/help — القائمة الرئيسية",
        "",
        "🧹 تنظيف بيانات الاختبار",
        "/soft_clean_preview — معاينة تنظيف آمن للقديم فقط",
        "/soft_clean_confirm — تنفيذ التنظيف الآمن",
        "/deep_clean_preview — معاينة مسح كامل لبيانات البوت",
        "/deep_clean_confirm — تنفيذ المسح الكامل ⚠️",
        "",
        "📌 استخدم Deep Clean فقط عند بدء baseline جديد للتجربة.",
    ])


def build_master_help(
    mode: str | None = None,
    execution_enabled: bool = True,
    risk_enabled: bool = True,
    okx_orders: bool = False,
) -> str:
    """Approved compact /help dashboard.

    v124: buttons are InlineKeyboardMarkup sent under this message only.
    This avoids the large persistent Reply Keyboard that looked cheap and stayed open.
    """
    lines = [
        "🤖 OKX Long Bot Dashboard",
        "┄┄┄┄┄┄┄┄",
        *_system_status_block(
            mode=mode,
            execution_enabled=execution_enabled,
            risk_enabled=risk_enabled,
            okx_orders=okx_orders,
        ),
        "📝 ملاحظات",
        "• الأزرار بالأسفل تفتح أقسام الأوامر مباشرة.",
        "• /open_trades يعرض كل الصفقات المتابعة.",
        "• /report_execution خاص بصفقات التنفيذ المرشحة.",
        "• أوامر OKX تعتمد على وضع التنفيذ الحالي.",
    ]
    return "\n".join(lines)
