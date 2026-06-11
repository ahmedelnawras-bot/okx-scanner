from __future__ import annotations

import re
import copy
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

    # report_simulation receives result["simulation_trades"] from main.
    # Trust an explicit simulation source even when legacy mirror records still
    # carry execution-style fields like tracking_bucket="execution" or
    # execution_trade=True. Only reject records explicitly marked execution AND
    # not explicitly marked simulation.
    if source == "simulation":
        return True
    if source == "execution":
        return False
    if bucket == "simulation":
        return True
    if bucket == "execution":
        return False
    if bool(getattr(t, "execution_trade", False)):
        return False
    return True


def _simulation_scope_trades(trades: list | None) -> list:
    """Return Simulation trades normalized for report-only accounting.

    Important: result["simulation_trades"] is already the Simulation bucket from
    main.py. Some legacy simulation records were created by mirroring the
    execution path and may still carry fields like execution_trade=True or
    non-standard statuses. If we trust those flags here, the isolated Simulation
    report can show 0 open/closed trades while the Simulation wallet/top block
    correctly sees active trades.

    This function creates shallow report-only copies. It does not mutate Redis,
    lifecycle, execution, OKX, TP/SL, or the original trade objects.
    """
    out = []
    closed_statuses = {
        "closed", "stopped", "closed_loss", "closed_win", "expired",
        "trailing_hit", "breakeven_after_tp1", "duplicate_closed_by_okx_repair",
    }
    open_statuses = {
        "open", "tp1_hit", "tp2_hit", "tp1_partial", "tp2_partial",
        "runner", "runner_active", "protected_runner", "breakeven_runner",
        "partial_runner", "accepted_preview", "pending_pullback_preview",
    }

    for original in list(trades or []):
        # Trust explicit trade_source="simulation" first.
        # Legacy simulation mirror records may still carry execution-style fields
        # such as tracking_bucket="execution" or execution_trade=True. Those must
        # still be counted in Simulation reports because they came from
        # result["simulation_trades"]. Only reject records explicitly marked
        # execution when they are not explicitly marked simulation.
        source = str(getattr(original, "trade_source", "") or "").strip().lower()
        bucket = str(getattr(original, "tracking_bucket", "") or "").strip().lower()
        if source == "simulation":
            pass
        elif source == "execution":
            continue
        elif bucket == "execution":
            continue

        try:
            t = copy.copy(original)
        except Exception:
            t = original

        try:
            setattr(t, "trade_source", "simulation")
            setattr(t, "tracking_bucket", "simulation")
            setattr(t, "execution_trade", False)
        except Exception:
            pass

        status = str(getattr(t, "status", "") or "").strip().lower()
        is_closed = bool(getattr(t, "is_closed", False) or getattr(t, "closed_at", None) or status in closed_statuses)
        try:
            if is_closed:
                setattr(t, "is_closed", True)
                if status not in {"closed_loss", "closed_win", "breakeven_after_tp1", "trailing_hit"}:
                    realized = _safe_float(getattr(t, "realized_pnl_pct", 0.0), 0.0)
                    setattr(t, "status", "closed_win" if realized > 0 else "closed_loss")
            else:
                setattr(t, "is_closed", False)
                if status not in open_statuses:
                    setattr(t, "status", "open")
                elif status in {"tp1_hit", "tp1_partial"}:
                    setattr(t, "status", "tp1_partial")
                elif status in {"tp2_hit", "tp2_partial", "runner_active", "protected_runner", "partial_runner", "breakeven_runner"}:
                    setattr(t, "status", "runner")
        except Exception:
            pass

        out.append(t)
    return out


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
        return ("ðŸŸ¢" if value >= 0 else "ðŸ”´") + f" {value:+.2f}$"

    return [
        "ðŸ’° <b>Wallet Impact</b>",
        f"ðŸ§± Report Scope: <code>{SIMULATION_SCOPE_MARKER}</code>",
        f"ðŸ“Œ Ø±Ø£Ø³ Ø§Ù„Ù…Ø§Ù„\n<b>{float(starting_balance or 1000.0):.0f}$</b>",
        "",
        "âœ… <b>Ø§Ù„ØµÙÙ‚Ø§Øª Ø§Ù„Ù…ØºÙ„Ù‚Ø©</b>",
        "ðŸ“ˆ Ø§Ù„Ø£Ø±Ø¨Ø§Ø­",
        f"{closed_profit_usd:+.2f}$ | {closed_profit:+.2f}% Realized PnL",
        "ðŸ“‰ Ø§Ù„Ø®Ø³Ø§Ø¦Ø±",
        f"{closed_loss_usd:+.2f}$ | {closed_loss:+.2f}% Realized PnL",
        "âš–ï¸ Ø§Ù„ØµØ§ÙÙŠ",
        f"<b>{money_icon(closed_net_usd)} | {closed_net:+.2f}% Realized PnL</b>",
        "",
        "ðŸ”„ <b>Ø§Ù„ØµÙÙ‚Ø§Øª Ø§Ù„Ù…ÙØªÙˆØ­Ø©</b>",
        "ðŸ“ˆ Ø§Ù„Ø£Ø±Ø¨Ø§Ø­ Ø§Ù„Ø¹Ø§Ø¦Ù…Ø©",
        f"{floating_profit_usd:+.2f}$ | {floating_profit:+.2f}% Total Floating PnL",
        "ðŸ“‰ Ø§Ù„Ø®Ø³Ø§Ø¦Ø± Ø§Ù„Ø¹Ø§Ø¦Ù…Ø©",
        f"{floating_loss_usd:+.2f}$ | {floating_loss:+.2f}% Total Floating PnL",
        "âš–ï¸ Total Floating PnL",
        f"<b>{money_icon(floating_net_usd)} | {floating_net:+.2f}% Total Floating PnL</b>",
        "",
        "ðŸ’¼ <b>Ø§Ù„ØªØ£Ø«ÙŠØ± Ø§Ù„Ø­Ø§Ù„ÙŠ Ø¹Ù„Ù‰ Ù…Ø­ÙØ¸Ø© Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©</b>",
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
    title: str = "ðŸ§ª ØªÙ‚Ø±ÙŠØ± Ø£Ø¯Ø§Ø¡ Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©",
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

    lines: list[str] = [title, f"ðŸ“… {period_label(period)}", SEP, LEVERAGE_NOTE_AR, ""]
    lines.extend([
        "ðŸ“Š <b>Quick Stats</b>",
        f"â€¢ Checked Candidates: {checked}",
        f"â€¢ Accepted After Gate: {len(accepted_checks)} | Accept Rate: {acc_rate:.1f}%",
        f"â€¢ Currently Open Tracked Trades: {len(opened)}",
        f"â€¢ Closed Tracked Trades: {len(closed)}",
        f"ðŸ† Win Rate: <b>{wr:.1f}%</b>",
        f"ðŸŸ¢ Winners: {win_count} | ðŸ”´ Losers: {loss_count}",
        f"ðŸ“Œ Rejected After Check: {len(rejected_checks)} Ù…Ø­ÙÙˆØ¸Ø© Ù„Ù„ØªØ­Ù„ÙŠÙ„ ÙÙ‚Ø· ÙˆÙ„Ø§ ØªÙØ­Ø³Ø¨ ÙƒØµÙÙ‚Ø§Øª Ù…ÙØªÙˆØ­Ø©.",
        f"ðŸ›£ Whitelist: {counts['whitelist']} | Strong: {counts['strong']} | Recovery: {counts['recovery']} | Block: {counts['block']}",
    ])
    lines.extend([SEP, *_simulation_wallet_impact_lines(trades, account_summary=account_summary, starting_balance=start_balance)])
    behavior_lines = behavior_summary_lines(trades, label="Simulation Behavior Summary")
    lines.extend([SEP, *behavior_lines])
    lines.extend([SEP, "ðŸ“‚ <b>Open Trades</b>"])
    lines.append(f"ðŸŸ¢ Open Winners: {len(winners)} | ðŸ”´ Open Losers: {len(losers)}")
    if opened:
        lines.append(f"âš¡ Total Floating PnL: {sum(trade_effective_pnl(t) for t in opened):+.2f}%")
    append_trade_cards(lines, "ðŸŸ¢ <b>Top 3 Open Winners</b>", winners[:3], limit=3)
    append_trade_cards(lines, "ðŸ”´ <b>Top 3 Open Losers</b>", losers[:3], limit=3)
    append_trade_cards(lines, "ðŸ† <b>Top 3 Closed Winners</b>", closed_wins[:3], limit=3)
    append_trade_cards(lines, "ðŸ’€ <b>Top 3 Closed Losers</b>", closed_losses[:3], limit=3)
    lines.extend([SEP, "ðŸ’¡ Ø¥Ø¯Ø§Ø±Ø© Ø§Ù„ØµÙÙ‚Ø§Øª: Simulation 30/50/20 | Recovery 50/25/25"])
    return "\n".join(lines)


PERIODS = [
    ("", "since_start"),
    ("_month", "month"),
    ("_7d", "last_7d"),
    ("_today", "today"),
    ("_1h", "last_1h"),
]


def _simulation_header(text: str) -> str:
    return "ðŸ§ª Simulation Mode\nâ”â”â”â”â”â”â”â”â”â”â”â”\n" + str(text or "").strip()


def _compact_tradingview_links(text: str) -> str:
    """Compact TradingView links only inside Simulation reports.

    We keep the actual URL in an HTML anchor so Telegram shows only TV.
    This module must not touch shared report_format.py.
    """
    value = str(text or "")
    url = r"https://www\.tradingview\.com/chart/\?symbol=[^\s<]+"

    # Original shared formatter:
    # ðŸ”— TradingView: https://...
    value = re.sub(rf"ðŸ”—\s*TradingView:\s*({url})", r'ðŸ”— <a href="\1">TV</a>', value)

    # Some previous versions already changed label to TV but left the URL visible:
    # ðŸ”— TV: https://...
    value = re.sub(rf"ðŸ”—\s*TV:\s*({url})", r'ðŸ”— <a href="\1">TV</a>', value)

    # Fallback if the icon is missing.
    value = re.sub(rf"(?m)^TV:\s*({url})", r'ðŸ”— <a href="\1">TV</a>', value)

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
        if not removed and stripped in {"ðŸš€ ØªÙ‚Ø±ÙŠØ± Ø£Ø¯Ø§Ø¡ Ø§Ù„ØªÙ†ÙÙŠØ°", "ðŸš€ ØªÙ‚Ø±ÙŠØ± Ø§Ù„ØµÙÙ‚Ø§Øª Ø§Ù„Ù…Ø±Ø´Ø­Ø© â€” Execution"}:
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
        r"(?m)^ðŸ“Œ\s*Ø±Ø£Ø³ Ø§Ù„Ù…Ø§Ù„:\s*([^\n]+)$",
        r"ðŸ“Œ Ø±Ø£Ø³ Ø§Ù„Ù…Ø§Ù„\n<b>\1</b>",
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
                title="ðŸ§ª ØªÙ‚Ø±ÙŠØ± Ø£Ø¯Ø§Ø¡ Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©",
                period=period,
                account_summary=account_summary,
            ),
            account_summary,
        )
        out[f"/report_simulation_open{suffix}"] = _decorate(
            build_open_trades_report(
                trades,
                title="ðŸ§ªðŸ“‚ ØµÙÙ‚Ø§Øª Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø© Ø§Ù„Ù…ÙØªÙˆØ­Ø©",
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
    raw_sim_trades = list((result or {}).get("simulation_trades", []) or [])
    sim_trades = _simulation_scope_trades(raw_sim_trades)
    sim_checks = list((result or {}).get("simulation_execution_results", []) or [])
    sim_items = list((result or {}).get("simulation_signal_items", []) or [])

    out = _periodic_execution_style_reports(sim_checks, sim_trades, account_summary)

    # Same report families as execution, but under /report_simulation_*.
    # Dedicated isolated Simulation wallet impact. Do not call the shared
    # execution-style wallet report here because legacy simulation records may
    # carry execution-like flags/statuses.
    out["/report_simulation_wallet"] = _decorate(
        "\n".join(_simulation_wallet_impact_lines(_simulation_scope_trades(sim_trades), account_summary=account_summary, starting_balance=_extract_sim_start_balance(account_summary, 1000.0))),
        account_summary,
    )
    out["/report_simulation_profit_analysis"] = _decorate(
        build_profit_analysis_report(sim_trades, title="ðŸ“ˆ ØªØ­Ù„ÙŠÙ„ Ø£Ø³Ø¨Ø§Ø¨ Ø£Ø±Ø¨Ø§Ø­ Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©"),
        account_summary,
    )
    out["/report_simulation_losses_analysis"] = _decorate(
        build_losses_analysis_report(sim_trades, title="ðŸ“‰ ØªØ­Ù„ÙŠÙ„ Ø£Ø³Ø¨Ø§Ø¨ Ø®Ø³Ø§Ø¦Ø± Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©"),
        account_summary,
    )
    out["/report_simulation_intelligence"] = _decorate(
        build_execution_intelligence_report(sim_trades, sim_checks, "ðŸ§ ðŸ§ª Ø°ÙƒØ§Ø¡ ØµÙÙ‚Ø§Øª Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©"),
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
            build_profit_analysis_report(trades, title="ðŸ“ˆ ØªØ­Ù„ÙŠÙ„ Ø£Ø³Ø¨Ø§Ø¨ Ø£Ø±Ø¨Ø§Ø­ Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©"),
            account_summary,
        )
        out[f"/report_simulation_losses_analysis{suffix}"] = _decorate(
            build_losses_analysis_report(trades, title="ðŸ“‰ ØªØ­Ù„ÙŠÙ„ Ø£Ø³Ø¨Ø§Ø¨ Ø®Ø³Ø§Ø¦Ø± Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©"),
            account_summary,
        )
        out[f"/report_simulation_intelligence{suffix}"] = _decorate(
            build_execution_intelligence_report(trades, checks, "ðŸ§ ðŸ§ª Ø°ÙƒØ§Ø¡ ØµÙÙ‚Ø§Øª Ø§Ù„Ù…Ø­Ø§ÙƒØ§Ø©"),
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
        "ðŸ§ª Simulation Mode",
        "â”â”â”â”â”â”â”â”â”â”â”â”",
        "Mirror ÙƒØ§Ù…Ù„ Ù„ÙˆØ¶Ø¹ Ø§Ù„ØªØ¯Ø§ÙˆÙ„.",
        "â€¢ Ù†ÙØ³ Ø´Ø±ÙˆØ· Ø§Ù„ØªØ±Ø´ÙŠØ­ ÙˆØ§Ù„ØªÙ†ÙÙŠØ°",
        "â€¢ Ù„Ø§ ÙŠØ±Ø³Ù„ Ø£ÙˆØ§Ù…Ø± OKX Live",
        "â€¢ ÙŠÙØªØ­ ØµÙÙ‚Ø§Øª Ø¯Ø§Ø®Ù„ÙŠØ© Ø¨Ù…Ø­ÙØ¸Ø© Ù…Ø­Ø§ÙƒØ§Ø©",
        "",
        str(wallet_text or "").strip(),
    ]).strip()

    return out
