from __future__ import annotations

import re
from typing import Any

from reporting.report_router import build_report_bundle, build_command_outputs
from reporting.report_execution import build_execution_report


_RESERVED_COMMANDS = {
    "/help",
    "/start",
    "/status",
    "/mood",
    "/okx_control",
    "/help_execution",
    "/help_normal",
    "/diagnostics_help",
    "/help_diagnostics",
    "/bot_modes",
    "/modes",
    "/mode",
}


def _simulation_header(text: str) -> str:
    return "🧪 Simulation Mode\n━━━━━━━━━━━━\n" + str(text or "")


def _simulation_aliases_for_execution_command(command_key: str) -> list[str]:
    command_key = str(command_key or "").strip()
    if not command_key.startswith("/"):
        command_key = "/" + command_key

    aliases: list[str] = []
    if command_key == "/report_execution":
        aliases.append("/report_simulation")
    elif command_key.startswith("/report_execution_"):
        suffix = command_key[len("/report_execution_"):]
        aliases.append("/report_simulation_" + suffix)

    if command_key.startswith("/report_"):
        aliases.append(command_key.replace("/report_", "/report_simulation_", 1))

    return list(dict.fromkeys(a for a in aliases if a and a not in {"/report_simulation_execution"}))


def _compact_tradingview_links(text: str) -> str:
    """Compact TradingView display only in Simulation reports.

    Keeps shared report_format.py unchanged so Execution/Normal reports are not affected.
    """
    value = str(text or "")

    # Existing format from report_format.trade_card_lines:
    # 🔗 TradingView: https://www.tradingview.com/chart/?symbol=OKX:BTCUSDT.P
    pattern = re.compile(r"🔗 TradingView:\s*(https://www\.tradingview\.com/chart/\?symbol=[^\s<]+)")
    value = pattern.sub(r'🔗 <a href="\1">TV</a>', value)

    return value


def _inject_account_summary(text: str, account_summary: str | None = None) -> str:
    value = str(text or "").strip()
    block = str(account_summary or "").strip()
    if not block:
        return value

    if "Simulation Daily Balance" in value and "Simulation Equity Curve" in value:
        return value

    # Put the Simulation-only top block immediately before Wallet Impact,
    # leaving the execution report sections identical afterwards.
    for marker in ("💰 Wallet Impact", "💼 Wallet Impact", "Wallet Impact"):
        idx = value.find(marker)
        if idx >= 0:
            before = value[:idx].rstrip()
            after = value[idx:].lstrip()
            return (before + "\n\n" + block + "\n" + after).strip()

    return (block + "\n" + value).strip()


def _decorate(text: str, account_summary: str | None = None) -> str:
    return _simulation_header(_compact_tradingview_links(_inject_account_summary(text, account_summary)))


def build_simulation_command_outputs(
    result: dict[str, Any],
    *,
    account_summary: str | None = None,
    wallet_text: str | None = None,
    daily_balance_text: str | None = None,
) -> dict[str, str]:
    """Build Simulation report command outputs in an isolated report module.

    This intentionally mirrors Execution report formatting by feeding only
    simulation data into the existing report builders:
    - simulation_trades
    - simulation_execution_results
    - simulation_signal_items

    Execution and Normal reports are not modified.
    """
    sim_trades = list((result or {}).get("simulation_trades", []) or [])
    sim_checks = list((result or {}).get("simulation_execution_results", []) or [])
    sim_items = list((result or {}).get("simulation_signal_items", []) or [])

    try:
        reports = build_report_bundle(sim_trades, sim_checks, sim_items)
        commands = build_command_outputs(sim_trades, sim_checks, sim_items)
    except Exception as exc:
        print(f"⚠️ Simulation reports build failed: {exc}", flush=True)
        reports = {}
        commands = {}

    out: dict[str, str] = {}
    merged = {**reports, **commands}

    for key, value in merged.items():
        if not isinstance(value, str) or not value.strip():
            continue

        command_key = str(key)
        if not command_key.startswith("/"):
            command_key = "/" + command_key
        if command_key in _RESERVED_COMMANDS:
            continue

        for alias in _simulation_aliases_for_execution_command(command_key):
            if alias not in _RESERVED_COMMANDS:
                out[alias] = _decorate(value, account_summary)

        if command_key.startswith("/simulation_"):
            out[command_key] = _decorate(value, account_summary)

    if "/report_simulation" not in out:
        try:
            fallback_report = build_execution_report(
                sim_checks,
                sim_trades,
                title="🧪 تقرير أداء المحاكاة",
                period="since_start",
                table=False,
            )
        except Exception:
            fallback_report = (
                merged.get("/report_execution")
                or merged.get("report_execution")
                or merged.get("/report_all")
                or merged.get("report_all")
                or ""
            )
        if isinstance(fallback_report, str) and fallback_report.strip():
            out["/report_simulation"] = _decorate(fallback_report, account_summary)

    if wallet_text:
        out["/simulation_wallet"] = str(wallet_text)
        out["/report_simulation_wallet"] = str(wallet_text)
        out["/report_simulation_wallet_7d"] = str(wallet_text)
        out["/report_simulation_wallet_today"] = str(wallet_text)

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
