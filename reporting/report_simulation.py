from __future__ import annotations

import re
from typing import Any

from reporting.report_open_trades import build_open_trades_report
from reporting.report_wallet import build_wallet_report
from reporting.report_profit_analysis import build_profit_analysis_report
from reporting.report_losses_analysis import build_losses_analysis_report
from reporting.report_intelligence import build_execution_intelligence_report
from reporting.report_diagnostics import build_diagnostics_report
from reporting.report_format import (
    SEP,
    LEVERAGE_NOTE_AR,
    append_trade_cards,
    behavior_summary_lines,
    closed_trades,
    filter_checks_by_period,
    filter_trades_by_period,
    open_trades,
    period_label,
    trade_effective_pnl,
    trade_money_pnl,
)




# =========================================================
# Scope-isolated simulation accounting
# =========================================================
SIMULATION_SCOPE_MARKER = "simulation_wallet_truth_v1"

ACCEPTED_STATUSES = {
    "accepted_preview",
    "pending_pullback_preview",
    "executed",
    "open",
    "tp1",
    "tp2",
    "trailing",
}
REJECTED_EXTRA_STATUSES = {"candidate_only"}


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _is_accepted_status(status: str | None) -> bool:
    return str(status or "") in ACCEPTED_STATUSES


def _is_rejected_status(status: str | None) -> bool:
    text = str(status or "")
    return text.startswith("rejected") or text in REJECTED_EXTRA_STATUSES


def _accepted_gate_checks(execution_results: list[dict]) -> list[dict]:
    return [r for r in execution_results if _is_accepted_status(r.get("status"))]


def _rejected_checks(execution_results: list[dict]) -> list[dict]:
    return [r for r in execution_results if _is_rejected_status(r.get("status"))]


def _execution_path_counts(execution_results: list[dict]) -> dict[str, int]:
    return {
        "whitelist": sum(1 for r in execution_results if r.get("path") == "whitelist"),
        "strong": sum(1 for r in execution_results if r.get("path") == "elite_or_whitelist"),
        "recovery": sum(1 for r in execution_results if r.get("path") == "recovery"),
        "block": sum(1 for r in execution_results if r.get("path") == "block_exception"),
    }


def _closed_wr_parts(trades: list) -> tuple[int, int, float]:
    closed = closed_trades(trades)
    wins = [t for t in closed if trade_effective_pnl(t) > 0]
    losses = [t for t in closed if trade_effective_pnl(t) < 0]
    denom = len(wins) + len(losses)
    return len(wins), len(losses), (len(wins) / denom * 100.0 if denom else 0.0)


def _is_simulation_trade(t) -> bool:
    source = str(getattr(t, "trade_source", "") or "").strip().lower()
    bucket = str(getattr(t, "tracking_bucket", "") or "").strip().lower()
    # report_simulation receives simulation_trades from main, so legacy records in
    # this list are accepted unless explicitly marked execution.
    if source == "execution" or bucket == "execution" or bool(getattr(t, "execution_trade", False)):
        return False
    return True


def _simulation_scope_trades(trades: list | None) -> list:
    return [t for t in list(trades or []) if _is_simulation_trade(t)]


def _simulation_wallet_impact_lines(trades: list, *, account_summary: str | None = None, starting_balance: float = 1000.0) -> list[str]:
    opened = open_trades(trades)
    closed = closed_trades(trades)
    closed_profit_usd = sum(max(0.0, trade_money_pnl(t)) for t in closed)
    closed_loss_usd = sum(min(0.0, trade_money_pnl(t)) for t in closed)
    floating_profit_usd = sum(max(0.0, trade_money_pnl(t)) for t in opened)
    floating_loss_usd = sum(min(0.0, trade_money_pnl(t)) for t in opened)
    closed_profit = sum(max(0.0, trade_effective_pnl(t)) for t in closed)
    closed_loss = sum(min(0.0, trade_effective_pnl(t)) for t in closed)
    floating_profit = sum(max(0.0, trade_effective_pnl(t)) for t in opened)
    floating_loss = sum(min(0.0, trade_effective_pnl(t)) for t in opened)
    closed_net_usd = closed_profit_usd + closed_loss_usd
    floating_net_usd = floating_profit_usd + floating_loss_usd
    total_usd = closed_net_usd + floating_net_usd
    closed_net = closed_profit + closed_loss
    floating_net = floating_profit + floating_loss

    def money_icon(value: float) -> str:
        return ("🟢" if value >= 0 else "🔴") + f" {value:+.2f}$"

    return [
        "💰 <b>Wallet Impact</b>",
        f"🧱 Report Scope: <code>{SIMULATION_SCOPE_MARKER}</code>",
        f"📌 رأس المال\n<b>{float(starting_balance or 1000.0):.0f}$</b>",
        "",
        "✅ <b>الصفقات المغلقة</b>",
        "📈 الأرباح",
        f"{closed_profit_usd:+.2f}$ | {closed_profit:+.2f}% Realized PnL",
        "📉 الخسائر",
        f"{closed_loss_usd:+.2f}$ | {closed_loss:+.2f}% Realized PnL",
        "⚖️ الصافي",
        f"<b>{money_icon(closed_net_usd)} | {closed_net:+.2f}% Realized PnL</b>",
        "",
        "🔄 <b>الصفقات المفتوحة</b>",
        "📈 الأرباح العائمة",
        f"{floating_profit_usd:+.2f}$ | {floating_profit:+.2f}% Total Floating PnL",
        "📉 الخسائر العائمة",
        f"{floating_loss_usd:+.2f}$ | {floating_loss:+.2f}% Total Floating PnL",
        "⚖️ Total Floating PnL",
        f"<b>{money_icon(floating_net_usd)} | {floating_net:+.2f}% Total Floating PnL</b>",
        "",
        "💼 <b>التأثير الحالي على محفظة المحاكاة</b>",
        f"<b>{money_icon(total_usd)}</b>",
    ]


def _extract_sim_start_balance(account_summary: str | None, fallback: float = 1000.0) -> float:
    text = str(account_summary or "")
    m = re.search(r"Start Balance:\s*([0-9,.]+)", text)
    if m:
        return _safe_float(m.group(1).replace(',', ''), fallback)
    return fallback


def build_simulation_report(
    sim_checks: list[dict],
    sim_trades: list,
    *,
    title: str = "🧪 تقرير أداء المحاكاة",
    period: str = "since_start",
    account_summary: str | None = None,
) -> str:
    trades = filter_trades_by_period(_simulation_scope_trades(sim_trades), period)
    checks = filter_checks_by_period(sim_checks or [], period)
    accepted_checks = _accepted_gate_checks(checks)
    rejected_checks = _rejected_checks(checks)
    checked = len(checks)
    counts = _execution_path_counts(checks)
    acc_rate = (len(accepted_checks) / max(1, checked)) * 100 if checked else 0.0
    opened = open_trades(trades)
    closed = closed_trades(trades)
    win_count, loss_count, wr = _closed_wr_parts(trades)
    winners = sorted([t for t in opened if trade_effective_pnl(t) >= 0], key=trade_effective_pnl, reverse=True)
    losers = sorted([t for t in opened if trade_effective_pnl(t) < 0], key=trade_effective_pnl)
    closed_wins = sorted([t for t in closed if trade_effective_pnl(t) > 0], key=trade_effective_pnl, reverse=True)
    closed_losses = sorted([t for t in closed if trade_effective_pnl(t) < 0], key=trade_effective_pnl)
    start_balance = _extract_sim_start_balance(account_summary, 1000.0)

    lines: list[str] = [title, f"📅 {period_label(period)}", SEP, LEVERAGE_NOTE_AR, ""]
    lines.extend([
        "📊 <b>Quick Stats</b>",
        f"• Checked Candidates: {checked}",
        f"• Accepted After Gate: {len(accepted_checks)} | Accept Rate: {acc_rate:.1f}%",
        f"• Currently Open Tracked Trades: {len(opened)}",
        f"• Closed Tracked Trades: {len(closed)}",
        f"🏆 Win Rate: <b>{wr:.1f}%</b>",
        f"🟢 Winners: {win_count} | 🔴 Losers: {loss_count}",
        f"📌 Rejected After Check: {len(rejected_checks)} محفوظة للتحليل فقط ولا تُحسب كصفقات مفتوحة.",
        f"🛣 Whitelist: {counts['whitelist']} | Strong: {counts['strong']} | Recovery: {counts['recovery']} | Block: {counts['block']}",
    ])
    lines.extend([SEP, *_simulation_wallet_impact_lines(trades, account_summary=account_summary, starting_balance=start_balance)])
    behavior_lines = behavior_summary_lines(trades, label="Simulation Behavior Summary")
    lines.extend([SEP, *behavior_lines])
    lines.extend([SEP, "📂 <b>Open Trades</b>"])
    lines.append(f"🟢 Open Winners: {len(winners)} | 🔴 Open Losers: {len(losers)}")
    if opened:
        lines.append(f"⚡ Total Floating PnL: {sum(trade_effective_pnl(t) for t in opened):+.2f}%")
    append_trade_cards(lines, "🟢 <b>Top 3 Open Winners</b>", winners[:3], limit=3)
    append_trade_cards(lines, "🔴 <b>Top 3 Open Losers</b>", losers[:3], limit=3)
    append_trade_cards(lines, "🏆 <b>Top 3 Closed Winners</b>", closed_wins[:3], limit=3)
    append_trade_cards(lines, "💀 <b>Top 3 Closed Losers</b>", closed_losses[:3], limit=3)
    lines.extend([SEP, "💡 إدارة الصفقات: Simulation 30/50/20 | Recovery 50/25/25"])
    return "\n".join(lines)


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
            build_simulation_report(
                checks,
                trades,
                title="🧪 تقرير أداء المحاكاة",
                period=period,
                account_summary=account_summary,
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
