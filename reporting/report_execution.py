from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone

from tracking.models import TrackedTrade
from utils.constants import LEVERAGE_NOTE_AR
from reporting.report_format import (
    SEP,
    append_trade_cards,
    behavior_summary_lines,
    closed_trades,
    filter_checks_by_period,
    filter_trades_by_period,
    check_time,
    money_from_exposure_pct,
    open_trades,
    period_label,
    trade_activity_time,
    trade_effective_pnl,
    wallet_impact_lines,
)


def _is_accepted_status(status: str | None) -> bool:
    return str(status or "") in {"accepted_preview", "pending_pullback_preview", "executed", "open", "tp1", "tp2", "trailing"}


def _is_rejected_status(status: str | None) -> bool:
    text = str(status or "")
    return text.startswith("rejected") or text in {"candidate_only"}


def _closed_wr_parts(trades: list[TrackedTrade]) -> tuple[int, int, float]:
    closed = closed_trades(trades)
    wins = [t for t in closed if trade_effective_pnl(t) > 0]
    losses = [t for t in closed if trade_effective_pnl(t) < 0]
    denom = len(wins) + len(losses)
    return len(wins), len(losses), (len(wins) / denom * 100.0 if denom else 0.0)


def _trade_net_usd(trades: list[TrackedTrade]) -> float:
    return money_from_exposure_pct(sum(trade_effective_pnl(t) for t in trades))


def _execution_path_counts(execution_results: list[dict]) -> dict[str, int]:
    return {
        "whitelist": sum(1 for r in execution_results if r.get("path") == "whitelist"),
        "strong": sum(1 for r in execution_results if r.get("path") == "elite_or_whitelist"),
        "recovery": sum(1 for r in execution_results if r.get("path") == "recovery"),
        "block": sum(1 for r in execution_results if r.get("path") == "block_exception"),
    }


def _row_key_for_check(item: dict, table_period: str) -> str:
    dt = check_time(item) or datetime.now(timezone.utc)
    if table_period == "today":
        return dt.strftime("%H:00")
    return dt.strftime("%d-%m")


def _row_key_for_trade(t: TrackedTrade, table_period: str) -> str:
    dt = trade_activity_time(t) or datetime.now(timezone.utc)
    if table_period == "today":
        return dt.strftime("%H:00")
    return dt.strftime("%d-%m")


def build_execution_period_table(
    execution_results: list[dict],
    trades: list[TrackedTrade] | None = None,
    title: str = "🚀 تقرير الصفقات المرشحة للتنفيذ",
    period: str = "last_7d",
) -> str:
    """Old Excel-like candidates report.

    7D/month tables are grouped by day. Today is grouped by hour. Last 1H is a
    compact single-period snapshot. Wallet/Net values use execution accepted
    tracked trades only; rejected/candidate-only checks are excluded from PnL.
    """
    trades = filter_trades_by_period(trades or [], period)
    checks = filter_checks_by_period(execution_results or [], period)

    # 1H uses one summary row; week/month/today use period buckets.
    if period == "last_1h":
        keys = ["Last 1H"]
        check_buckets = {"Last 1H": checks}
        trade_buckets = {"Last 1H": trades}
    else:
        check_buckets = defaultdict(list)
        trade_buckets = defaultdict(list)
        for item in checks:
            check_buckets[_row_key_for_check(item, period)].append(item)
        for trade in trades:
            trade_buckets[_row_key_for_trade(trade, period)].append(trade)
        keys = sorted(set(check_buckets) | set(trade_buckets))

    lines = [title, f"📅 {period_label(period)}", SEP, LEVERAGE_NOTE_AR, ""]
    lines.append("<code>Period     Checked Accepted Rejected Open Closed WR    Net$")
    for key in keys:
        rows = check_buckets.get(key, [])
        row_trades = trade_buckets.get(key, [])
        checked = len(rows)
        accepted = sum(1 for r in rows if _is_accepted_status(r.get("status")))
        rejected = sum(1 for r in rows if _is_rejected_status(r.get("status")))
        opened = len(open_trades(row_trades))
        closed = len(closed_trades(row_trades))
        wins, losses, wr = _closed_wr_parts(row_trades)
        net = _trade_net_usd(row_trades)
        lines.append(f"{key:<10} {checked:>7} {accepted:>8} {rejected:>8} {opened:>4} {closed:>6} {wr:>4.0f}% {net:>+7.2f}")
    lines.append("</code>")

    total_rejected = sum(1 for r in checks if _is_rejected_status(r.get("status")))
    total_accepted = sum(1 for r in checks if _is_accepted_status(r.get("status")))
    total_checked = len(checks)
    acc_rate = total_accepted / max(1, total_checked) * 100.0 if total_checked else 0.0
    lines.extend([
        "",
        f"📊 Summary: Checked {total_checked} | Accepted {total_accepted} | Rejected {total_rejected} | Accept Rate {acc_rate:.1f}%",
        "📌 المرفوضات محفوظة للتحليل فقط ولا تدخل في Wallet/Open/Win Rate.",
    ])
    return "\n".join(lines)


def build_execution_report(
    execution_results: list[dict],
    trades: list[TrackedTrade] | None = None,
    title: str = "🚀 تقرير أداء التنفيذ",
    period: str = "since_start",
    table: bool = False,
) -> str:
    if table or period in {"month", "last_7d", "today", "last_1h"}:
        return build_execution_period_table(execution_results, trades, title="🚀 تقرير الصفقات المرشحة — Execution", period=period)

    trades = filter_trades_by_period(trades or [], period)
    execution_results = filter_checks_by_period(execution_results or [], period)
    accepted = [r for r in execution_results if _is_accepted_status(r.get("status"))]
    rejected = [r for r in execution_results if _is_rejected_status(r.get("status"))]
    checked = len(execution_results)
    counts = _execution_path_counts(execution_results)
    acc_rate = (len(accepted) / max(1, checked)) * 100 if checked else 0.0

    opened = open_trades(trades)
    closed = closed_trades(trades)
    win_count, loss_count, wr = _closed_wr_parts(trades)
    winners = sorted([t for t in opened if trade_effective_pnl(t) >= 0], key=trade_effective_pnl, reverse=True)
    losers = sorted([t for t in opened if trade_effective_pnl(t) < 0], key=trade_effective_pnl)

    lines: list[str] = [title, f"📅 {period_label(period)}", SEP, LEVERAGE_NOTE_AR, ""]
    lines.extend([
        "📊 <b>Quick Stats</b>",
        f"• Candidates: {checked}",
        f"• Open: {len(opened)}",
        f"• Closed: {len(closed)}",
        f"🏆 Win Rate: <b>{wr:.1f}%</b>",
        f"🟢 Winners: {win_count} | 🔴 Losers: {loss_count}",
        f"📌 Rejected After Check: {len(rejected)} محفوظة للتحليل ولا تُحسب كصفقات مفتوحة.",
        f"🛣 Whitelist: {counts['whitelist']} | Strong: {counts['strong']} | Recovery: {counts['recovery']} | Block: {counts['block']}",
    ])

    lines.extend([SEP, *wallet_impact_lines(trades, title="Wallet Impact")])
    lines.extend([SEP, *behavior_summary_lines(trades, label="Execution Behavior Summary")])
    lines.extend([SEP, "📂 <b>Open Trades</b>"])
    lines.append(f"🟢 Open Winners: {len(winners)} | 🔴 Open Losers: {len(losers)}")
    if opened:
        lines.append(f"⚡ Net Floating: {sum(trade_effective_pnl(t) for t in opened):+.2f}% Exposure")
    append_trade_cards(lines, "🟢 <b>Latest 3 Winners</b>", winners[:3], limit=3)
    append_trade_cards(lines, "🔴 <b>Latest 3 Losers</b>", losers[:3], limit=3)

    if rejected:
        reason_counts = Counter(item.get("reason", "unknown") for item in rejected)
        lines.extend([SEP, "📉 <b>Rejected After Check — Top Reasons</b>"])
        for reason, count in reason_counts.most_common(5):
            lines.append(f"• {reason}: {count}")

    lines.extend([SEP, "💡 إدارة الصفقات: Normal/Strong/Block 40/40/20 | Recovery 50/25/25"])
    return "\n".join(lines)
