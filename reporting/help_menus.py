from __future__ import annotations


def build_main_menu_layout() -> list[list[str]]:
    return [
        ["Execution", "Normal Trades"],
        ["Exec Intelligence", "Market Intelligence"],
        ["Wallet Impact", "Diagnostics"],
        ["OKX Control", "Admin"],
        ["System Info"],
    ]


def _system_status_block() -> list[str]:
    return [
        "🧭 System Status",
        "",
        "📈 Market Mode: 🟢 NORMAL_LONG | 🟨 STRONG_LONG_ONLY | 🔴 BLOCK_LONGS | 🔵 RECOVERY_LONG",
        "⚡ Execution Engine: ACTIVE",
        "🛡 Risk Protection: ENABLED",
        "🎨 Mode Colors: 🟢 Normal | 🟨 Strong (light yellow) | 🔴 Block | 🔵 Recovery",
        "",
    ]


def build_execution_help() -> str:
    lines = [
        "🚀 صفقات التنفيذ",
        "📘 /help_execution",
        "━━━━━━━━━━━━",
        * _system_status_block(),
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
        "📈 الصفقات العادية",
        "📘 /help_normal",
        "━━━━━━━━━━━━",
        *_system_status_block(),
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


def build_master_help() -> str:
    menu_rows = build_main_menu_layout()
    lines = [
        "🤖 OKX Long Bot Dashboard",
        "━━━━━━━━━━━━",
        "🧭 Main Buttons",
    ]
    for row in menu_rows:
        lines.append(" | ".join(row))
    lines.extend([
        "",
        "📘 Quick Access",
        "/help_execution",
        "/help_normal",
        "",
        "🧠 Notes",
        "• normal signal first",
        "• execution is a separate path",
        "• mode color stays visible in status / reminder / execution",
        "• execution candidate keeps execution style + current mode color",
        "• STRONG_LONG_ONLY uses light yellow identity in status / reminders / execution",
    ])
    return "\n".join(lines)
