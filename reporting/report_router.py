from __future__ import annotations

from reporting.help_menus import build_execution_help, build_normal_help, build_master_help
from reporting.report_general import build_general_report
from reporting.report_open_trades import build_open_trades_report
from reporting.report_execution import build_execution_report
from reporting.report_wallet import build_wallet_report
from reporting.report_intelligence import build_intelligence_report, build_execution_intelligence_report
from reporting.report_diagnostics import build_diagnostics_report
from reporting.report_profit_analysis import build_profit_analysis_report
from reporting.report_losses_analysis import build_losses_analysis_report
from reporting.report_format import filter_trades_by_period, filter_checks_by_period

PERIODS = [
    ("", "since_start"),
    ("_month", "month"),
    ("_7d", "last_7d"),
    ("_today", "today"),
    ("_1h", "last_1h"),
]


def _execution_trades(trades):
    # Execution wallet/open/PnL must use accepted tracked execution trades only.
    return [t for t in trades if bool(getattr(t, "execution_trade", False) or getattr(t, "tracking_bucket", "") == "execution")]


def _normal_trades(trades):
    return [t for t in trades if not bool(getattr(t, "execution_trade", False) or getattr(t, "tracking_bucket", "") == "execution")]


def build_report_bundle(trades, execution_results, signal_items):
    execution_trades = _execution_trades(trades)
    normal_trades = _normal_trades(trades)
    return {
        "general": build_general_report(normal_trades, title="📊 تقرير الصفقات العادية"),
        "open_trades": build_open_trades_report(normal_trades, title="📂 الصفقات العادية المفتوحة"),
        "execution_open_trades": build_open_trades_report(
            execution_trades, title="🚀📂 صفقات التنفيذ المفتوحة", execution_only=True
        ),
        "execution": build_execution_report(execution_results, execution_trades, title="🚀 تقرير أداء التنفيذ"),
        "execution_wallet": build_wallet_report(execution_trades, title="💼 Wallet Impact — Execution"),
        "profit_analysis": build_profit_analysis_report(normal_trades, title="📈 تحليل أسباب الأرباح"),
        "execution_profit_analysis": build_profit_analysis_report(execution_trades, title="📈 تحليل أسباب أرباح التنفيذ"),
        "losses_analysis": build_losses_analysis_report(normal_trades, title="📉 تحليل أسباب الخسائر"),
        "execution_losses_analysis": build_losses_analysis_report(execution_trades, title="📉 تحليل أسباب خسائر التنفيذ"),
        "execution_intelligence": build_execution_intelligence_report(execution_trades, execution_results, "🧠🚀 ذكاء صفقات التنفيذ"),
        "market_intelligence": build_intelligence_report(normal_trades, "🧠📊 ذكاء الصفقات العادية"),
        "diagnostics": build_diagnostics_report(signal_items, execution_results),
    }


def build_command_outputs(trades, execution_results, signal_items):
    execution_trades = _execution_trades(trades)
    normal_trades = _normal_trades(trades)
    bundle = build_report_bundle(trades, execution_results, signal_items)
    commands = {
        "/report_execution_open": bundle["execution_open_trades"],
        "/report_execution_wallet": bundle["execution_wallet"],
        "/report_execution_profit_analysis": bundle["execution_profit_analysis"],
        "/report_execution_losses_analysis": bundle["execution_losses_analysis"],
        "/report_execution_diagnostics": bundle["diagnostics"],
        "/open_trades": bundle["open_trades"],
        "/report_profit_analysis": bundle["profit_analysis"],
        "/report_losses_analysis": bundle["losses_analysis"],
        "/report_intelligence": bundle["market_intelligence"],
        "/report_execution_intelligence": bundle["execution_intelligence"],
        "/report_diagnostics": bundle["diagnostics"],
    }

    for suffix, period in PERIODS:
        period_execution_trades = filter_trades_by_period(execution_trades, period)
        period_normal_trades = filter_trades_by_period(normal_trades, period)
        period_execution_results = filter_checks_by_period(execution_results, period)

        # Execution reports.
        commands[f"/report_execution{suffix}"] = build_execution_report(
            period_execution_results,
            period_execution_trades,
            title="🚀 تقرير أداء التنفيذ",
            period=period,
            table=period != "since_start",
        )
        commands[f"/report_execution_open{suffix}"] = build_open_trades_report(
            execution_trades,
            title="🚀📂 صفقات التنفيذ المفتوحة",
            execution_only=True,
            period=period,
        )
        commands[f"/report_execution_profit_analysis{suffix}"] = build_profit_analysis_report(
            execution_trades,
            title="📈 تحليل أسباب أرباح التنفيذ",
            period=period,
        )
        commands[f"/report_execution_losses_analysis{suffix}"] = build_losses_analysis_report(
            execution_trades,
            title="📉 تحليل أسباب خسائر التنفيذ",
            period=period,
        )
        commands[f"/report_execution_wallet{suffix}"] = build_wallet_report(
            execution_trades,
            title="💼 Wallet Impact — Execution",
            period=period,
        )
        commands[f"/report_execution_intelligence{suffix}"] = build_execution_intelligence_report(
            execution_trades,
            period_execution_results,
            "🧠🚀 ذكاء صفقات التنفيذ",
            period=period,
        )
        commands[f"/report_execution_diagnostics{suffix}"] = build_diagnostics_report(
            signal_items,
            period_execution_results,
            title="🧠 تشخيص التنفيذ",
            period=period,
        )

        # Normal reports have no Wallet Impact.
        commands[f"/report_all{suffix}"] = build_general_report(
            normal_trades,
            title="📊 تقرير الصفقات العادية",
            period=period,
        )
        commands[f"/open_trades{suffix}"] = build_open_trades_report(
            normal_trades,
            title="📂 الصفقات العادية المفتوحة",
            period=period,
        )
        commands[f"/report_profit_analysis{suffix}"] = build_profit_analysis_report(
            normal_trades,
            title="📈 تحليل أسباب الأرباح",
            period=period,
        )
        commands[f"/report_losses_analysis{suffix}"] = build_losses_analysis_report(
            normal_trades,
            title="📉 تحليل أسباب الخسائر",
            period=period,
        )
        commands[f"/report_intelligence{suffix}"] = build_intelligence_report(
            normal_trades,
            "🧠📊 ذكاء الصفقات العادية",
            period=period,
        )
        commands[f"/report_diagnostics{suffix}"] = build_diagnostics_report(
            signal_items,
            period_execution_results,
            title="🧠 تشخيص السوق والتنفيذ",
            period=period,
        )

    commands["/help_execution"] = build_execution_help()
    commands["/help_normal"] = build_normal_help()
    commands["/help"] = build_master_help()
    return commands
