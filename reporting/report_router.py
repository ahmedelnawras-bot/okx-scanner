from __future__ import annotations

from reporting.help_menus import build_execution_help, build_normal_help, build_master_help
from reporting.report_general import build_general_report
from reporting.report_open_trades import build_open_trades_report
from reporting.report_execution import build_execution_report
from reporting.report_wallet import build_wallet_report
from reporting.report_intelligence import build_intelligence_report
from reporting.report_diagnostics import build_diagnostics_report
from reporting.report_profit_analysis import build_profit_analysis_report
from reporting.report_losses_analysis import build_losses_analysis_report

PERIOD_LABELS = [
    ("", "Since Start"),
    ("_7d", "Last 7D"),
    ("_today", "Today"),
    ("_1h", "Last 1H"),
]


def _with_period_title(text: str, label: str) -> str:
    lines = text.splitlines()
    if len(lines) >= 2:
        return "\n".join([lines[0], f"📅 {label}", *lines[1:]])
    return f"{text}\n📅 {label}"


def build_report_bundle(trades, execution_results, signal_items):
    execution_trades = [t for t in trades if t.execution_setup_tags]
    return {
        "general": build_general_report(trades),
        "open_trades": build_open_trades_report(trades),
        "execution_open_trades": build_open_trades_report(
            trades, title="🚀📂 صفقات التنفيذ المفتوحة", execution_only=True
        ),
        "execution": build_execution_report(execution_results),
        "wallet": build_wallet_report(trades),
        "execution_wallet": build_wallet_report(execution_trades, title="💼 Wallet Impact — Execution"),
        "profit_analysis": build_profit_analysis_report(trades),
        "execution_profit_analysis": build_profit_analysis_report(execution_trades, title="📈 تحليل أسباب أرباح التنفيذ"),
        "losses_analysis": build_losses_analysis_report(trades),
        "execution_losses_analysis": build_losses_analysis_report(execution_trades, title="📉 تحليل أسباب خسائر التنفيذ"),
        "execution_intelligence": build_intelligence_report(execution_trades, "🧠🚀 Execution Intelligence"),
        "market_intelligence": build_intelligence_report(trades, "🧠📊 Market Intelligence"),
        "diagnostics": build_diagnostics_report(signal_items, execution_results),
    }


def build_command_outputs(trades, execution_results, signal_items):
    bundle = build_report_bundle(trades, execution_results, signal_items)
    commands = {}
    mappings = {
        "/report_execution": bundle["execution"],
        "/report_execution_open": bundle["execution_open_trades"],
        "/report_execution_wallet": bundle["execution_wallet"],
        "/report_execution_profit_analysis": bundle["execution_profit_analysis"],
        "/report_execution_losses_analysis": bundle["execution_losses_analysis"],
        "/report_execution_diagnostics": bundle["diagnostics"],
        "/report_all": bundle["general"],
        "/open_trades": bundle["open_trades"],
        "/report_profit_analysis": bundle["profit_analysis"],
        "/report_losses_analysis": bundle["losses_analysis"],
        "/report_intelligence": bundle["market_intelligence"],
        "/report_execution_intelligence": bundle["execution_intelligence"],
        "/report_diagnostics": bundle["diagnostics"],
    }
    for base, value in mappings.items():
        for suffix, label in PERIOD_LABELS:
            commands[f"{base}{suffix}"] = _with_period_title(value, label)
    commands["/help_execution"] = build_execution_help()
    commands["/help_normal"] = build_normal_help()
    commands["/help"] = build_master_help()
    return commands
