from __future__ import annotations


def build_main_menu_layout() -> list[list[str]]:
    """Official main Telegram reply keyboard layout."""
    return [
        ["🚀 Execution", "📊 Normal Trades"],
        ["🧠🚀 Execution Intelligence", "🧠📊 Market Intelligence"],
        ["💼 Wallet Impact"],
        ["🧠 Diagnostics", "🤖 OKX Control"],
        ["⚙️ Admin", "📘 System Info"],
    ]


def build_main_reply_keyboard() -> dict:
    """Telegram ReplyKeyboardMarkup matching the approved dashboard layout."""
    return {
        "keyboard": [[{"text": item} for item in row] for row in build_main_menu_layout()],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
    }


def _mode_line(mode: str | None = None) -> str:
    mode = mode or "NORMAL_LONG"
    if mode == "STRONG_LONG_ONLY":
        return "📈 Market Mode: 🟡 STRONG_LONG_ONLY"
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
        "━━━━━━━━━━━━",
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
        "━━━━━━━━━━━━",
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


def build_master_help(
    mode: str | None = None,
    execution_enabled: bool = True,
    risk_enabled: bool = True,
    okx_orders: bool = False,
) -> str:
    """Approved compact /help dashboard.

    The visual button grid is supplied separately as Telegram ReplyKeyboardMarkup.
    This text stays short and status-focused, matching the old approved dashboard style.
    """
    lines = [
        "🤖 OKX Long Bot Dashboard",
        "━━━━━━━━━━━━",
        *_system_status_block(
            mode=mode,
            execution_enabled=execution_enabled,
            risk_enabled=risk_enabled,
            okx_orders=okx_orders,
        ),
        "━━━━━━━━━━━━",
        "📝 ملاحظات",
        "• الأزرار بالأسفل تفتح أقسام الأوامر مباشرة.",
        "• /open_trades يعرض كل الصفقات المتابعة.",
        "• /report_execution خاص بصفقات التنفيذ المرشحة.",
        "• أوامر OKX تعتمد على وضع التنفيذ الحالي.",
    ]
    return "\n".join(lines)
