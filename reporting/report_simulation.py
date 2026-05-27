from __future__ import annotations

import re
from typing import Any

from reporting.report_execution import build_execution_report
from reporting.report_open_trades import build_open_trades_report
from reporting.report_wallet import build_wallet_report
from reporting.report_profit_analysis import build_profit_analysis_report
from reporting.report_losses_analysis import build_losses_analysis_report
from reporting.report_intelligence import build_execution_intelligence_report
from reporting.report_diagnostics import build_diagnostics_report
from reporting.report_format import filter_checks_by_period, filter_trades_by_period


PERIODS = [
    ("", "since_start"),
    ("_month", "month"),
    ("_7d", "last_7d"),
    ("_today", "today"),
    ("_1h", "last_1h"),
]


def _simulation_header(text: str) -> str:
    return "🧪 Simulation Mode\n━━━━━━━━━━━━\n" + str(text or "").strip()


def _compact_tradingview_links(text: str) -> str:
    """Compact TradingView links only inside Simulation reports.

    We keep the actual URL in an HTML anchor so Telegram shows only TV.
    This module must not touch shared report_format.py.
    """
    value = str(text or "")
    url = r"https://www\.tradingview\.com/chart/\?symbol=[^\s<]+"

    # Original shared formatter:
    # 🔗 TradingView: https://...
    value = re.sub(rf"🔗\s*TradingView:\s*({url})", r'🔗 <a href="\1">TV</a>', value)

    # Some previous versions already changed label to TV but left the URL visible:
    # 🔗 TV: https://...
    value = re.sub(rf"🔗\s*TV:\s*({url})", r'🔗 <a href="\1">TV</a>', value)

    # Fallback if the icon is missing.
    value = re.sub(rf"(?m)^TV:\s*({url})", r'🔗 <a href="\1">TV</a>', value)

    return value


def _strip_inherited_execution_title(text: str) -> str:
    """Remove only the inherited execution title from Simulation reports.

    The report keeps the same execution section order and labels, but the visible
    title must not say "execution" after the Simulation header/top block.
    """
    lines = str(text or "").splitlines()
    cleaned: list[str] = []
    removed = False
    for line in lines:
        stripped = line.strip()
        if not removed and stripped in {"🚀 تقرير أداء التنفيذ", "🚀 تقرير الصفقات المرشحة — Execution"}:
            removed = True
            continue
        cleaned.append(line)
    return "\n".join(cleaned).strip()


def _polish_wallet_impact_rtl(text: str) -> str:
    """Simulation-only RTL polish for Wallet Impact.

    Avoid changing shared report_format.py. This only rewrites the capital line
    that Telegram Android renders badly when Arabic and USD are mixed together.
    """
    value = str(text or "")
    value = re.sub(
        r"(?m)^📌\s*رأس المال:\s*([^\n]+)$",
        r"📌 رأس المال\n<b>\1</b>",
        value,
    )
    return value


def _inject_top_block(execution_style_report: str, account_summary: str | None = None) -> str:
    """Add the Simulation-only top block, then leave the execution-style report intact.

    The user requirement is: Simulation report = Execution report style/order/terms,
    with only the upper Simulation balance/equity block added.
    """
    report = str(execution_style_report or "").strip()
    top = str(account_summary or "").strip()
    if not top:
        return report

    if "Simulation Daily Balance" in report and "Simulation Equity Curve" in report:
        return report

    # Keep the execution report header and Quick Stats order exactly as generated.
    return (top + "\n" + report).strip()


def _decorate(text: str, account_summary: str | None = None) -> str:
    value = _inject_top_block(text, account_summary)
    value = _strip_inherited_execution_title(value)
    value = _polish_wallet_impact_rtl(value)
    value = _compact_tradingview_links(value)
    return _simulation_header(value)


def _periodic_execution_style_reports(sim_checks: list[dict], sim_trades: list, account_summary: str | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for suffix, period in PERIODS:
        checks = filter_checks_by_period(sim_checks, period)
        trades = filter_trades_by_period(sim_trades, period)
        out[f"/report_simulation{suffix}"] = _decorate(
            build_execution_report(
                checks,
                trades,
                title="🧪 تقرير أداء المحاكاة",
                period=period,
                table=False,
            ),
            account_summary,
        )
        out[f"/report_simulation_open{suffix}"] = _decorate(
            build_open_trades_report(
                trades,
                title="🧪📂 صفقات المحاكاة المفتوحة",
                execution_only=True,
                period=period,
            ),
            account_summary,
        )
    return out


def build_simulation_command_outputs(
    result: dict[str, Any],
    *,
    account_summary: str | None = None,
    wallet_text: str | None = None,
    daily_balance_text: str | None = None,
) -> dict[str, str]:
    """Build Simulation reports in an isolated module.

    Uses only:
    - result["simulation_trades"]
    - result["simulation_execution_results"]
    - result["simulation_signal_items"]

    It deliberately mirrors execution report builders/terms/order without
    modifying execution, normal reports, or shared report_format.py.
    """
    sim_trades = list((result or {}).get("simulation_trades", []) or [])
    sim_checks = list((result or {}).get("simulation_execution_results", []) or [])
    sim_items = list((result or {}).get("simulation_signal_items", []) or [])

    out = _periodic_execution_style_reports(sim_checks, sim_trades, account_summary)

    # Same report families as execution, but under /report_simulation_*.
    out["/report_simulation_wallet"] = _decorate(
        build_wallet_report(sim_trades, title="💼 Wallet Impact — Simulation"),
        account_summary,
    )
    out["/report_simulation_profit_analysis"] = _decorate(
        build_profit_analysis_report(sim_trades, title="📈 تحليل أسباب أرباح المحاكاة"),
        account_summary,
    )
    out["/report_simulation_losses_analysis"] = _decorate(
        build_losses_analysis_report(sim_trades, title="📉 تحليل أسباب خسائر المحاكاة"),
        account_summary,
    )
    out["/report_simulation_intelligence"] = _decorate(
        build_execution_intelligence_report(sim_trades, sim_checks, "🧠🧪 ذكاء صفقات المحاكاة"),
        account_summary,
    )
    out["/report_simulation_diagnostics"] = _decorate(
        build_diagnostics_report(sim_items, sim_checks),
        account_summary,
    )

    # Period aliases for analysis reports. These builders filter by trade period only.
    for suffix, period in PERIODS[1:]:
        trades = filter_trades_by_period(sim_trades, period)
        checks = filter_checks_by_period(sim_checks, period)
        out[f"/report_simulation_profit_analysis{suffix}"] = _decorate(
            build_profit_analysis_report(trades, title="📈 تحليل أسباب أرباح المحاكاة"),
            account_summary,
        )
        out[f"/report_simulation_losses_analysis{suffix}"] = _decorate(
            build_losses_analysis_report(trades, title="📉 تحليل أسباب خسائر المحاكاة"),
            account_summary,
        )
        out[f"/report_simulation_intelligence{suffix}"] = _decorate(
            build_execution_intelligence_report(trades, checks, "🧠🧪 ذكاء صفقات المحاكاة"),
            account_summary,
        )
        out[f"/report_simulation_diagnostics{suffix}"] = _decorate(
            build_diagnostics_report(sim_items, checks),
            account_summary,
        )

    # Dedicated wallet/daily commands can keep their compact Simulation-only wallet panel.
    if wallet_text:
        out["/simulation_wallet"] = str(wallet_text)
        out["/report_simulation_wallet_panel"] = str(wallet_text)
    if daily_balance_text:
        out["/report_simulation_daily_balance"] = str(daily_balance_text)
        out["/simulation_daily_balance"] = str(daily_balance_text)

    if "/report_simulation_open" in out:
        out["/simulation_open"] = out["/report_simulation_open"]

    out["/simulation"] = "\n".join([
        "🧪 Simulation Mode",
        "━━━━━━━━━━━━",
        "Mirror كامل لوضع التداول.",
        "• نفس شروط الترشيح والتنفيذ",
        "• لا يرسل أوامر OKX Live",
        "• يفتح صفقات داخلية بمحفظة محاكاة",
        "",
        str(wallet_text or "").strip(),
    ]).strip()

    return out
