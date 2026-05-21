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


def _is_accepted_status(status: str | None) -> bool:
    return str(status or "") in ACCEPTED_STATUSES


def _is_rejected_status(status: str | None) -> bool:
    text = str(status or "")
    return text.startswith("rejected") or text in REJECTED_EXTRA_STATUSES


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


def _accepted_gate_checks(execution_results: list[dict]) -> list[dict]:
    return [r for r in execution_results if _is_accepted_status(r.get("status"))]


def _rejected_checks(execution_results: list[dict]) -> list[dict]:
    return [r for r in execution_results if _is_rejected_status(r.get("status"))]


def build_execution_period_table(
    execution_results: list[dict],
    trades: list[TrackedTrade] | None = None,
    title: str = "🚀 تقرير الصفقات المرشحة للتنفيذ",
    period: str = "last_7d",
) -> str:
    """Excel-like execution snapshot.

    Checked / Accepted / Rejected come from execution checks.
    Open / Closed / Net come from tracked trades only.
    """
    trades = filter_trades_by_period(trades or [], period)
    checks = filter_checks_by_period(execution_results or [], period)

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
    lines.append("<code>Period     Checked GatePass Rejected Open Closed WR    Net$")
    for key in keys:
        rows = check_buckets.get(key, [])
        row_trades = trade_buckets.get(key, [])
        checked = len(rows)
        accepted = len(_accepted_gate_checks(rows))
        rejected = len(_rejected_checks(rows))
        opened = len(open_trades(row_trades))
        closed = len(closed_trades(row_trades))
        wins, losses, wr = _closed_wr_parts(row_trades)
        net = _trade_net_usd(row_trades)
        lines.append(f"{key:<10} {checked:>7} {accepted:>8} {rejected:>8} {opened:>4} {closed:>6} {wr:>4.0f}% {net:>+7.2f}")
    lines.append("</code>")

    total_rejected = len(_rejected_checks(checks))
    total_accepted = len(_accepted_gate_checks(checks))
    total_checked = len(checks)
    acc_rate = total_accepted / max(1, total_checked) * 100.0 if total_checked else 0.0
    lines.extend([
        "",
        f"📊 Summary: Checked {total_checked} | Gate Pass {total_accepted} | Rejected {total_rejected} | Accept Rate {acc_rate:.1f}%",
        "📌 Gate Pass = قبول بعد الفلاتر. Open/Closed = من الصفقات المتتبعة فقط.",
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
    if table:
        return build_execution_period_table(
            execution_results,
            trades,
            title="🚀 تقرير الصفقات المرشحة — Execution",
            period=period,
        )

    trades = filter_trades_by_period(trades or [], period)
    execution_results = filter_checks_by_period(execution_results or [], period)

    accepted_checks = _accepted_gate_checks(execution_results)
    rejected_checks = _rejected_checks(execution_results)
    checked = len(execution_results)
    counts = _execution_path_counts(execution_results)
    acc_rate = (len(accepted_checks) / max(1, checked)) * 100 if checked else 0.0

    opened = open_trades(trades)
    closed = closed_trades(trades)
    win_count, loss_count, wr = _closed_wr_parts(trades)
    winners = sorted([t for t in opened if trade_effective_pnl(t) >= 0], key=trade_effective_pnl, reverse=True)
    losers = sorted([t for t in opened if trade_effective_pnl(t) < 0], key=trade_effective_pnl)
    closed_wins = sorted([t for t in closed if trade_effective_pnl(t) > 0], key=trade_effective_pnl, reverse=True)
    closed_losses = sorted([t for t in closed if trade_effective_pnl(t) < 0], key=trade_effective_pnl)

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

    if accepted_checks and not opened and not closed:
        lines.extend([
            "",
            "⚠️ <b>ملاحظة تفسيرية</b>",
            "• يوجد قبول بعد الفلاتر في السجل، لكن لا توجد صفقات متتبعة مفتوحة/مغلقة حاليًا داخل نفس الفترة.",
            "• ده يعني إن أرقام Gate Pass و Rejected هي سجل checks، بينما Open/Closed تأتي فقط من trade tracking.",
        ])

    lines.extend([SEP, *wallet_impact_lines(trades, title="Wallet Impact")])
    lines.extend([SEP, *behavior_summary_lines(trades, label="Execution Behavior Summary")])
    lines.extend([SEP, "📂 <b>Open Trades</b>"])
    lines.append(f"🟢 Open Winners: {len(winners)} | 🔴 Open Losers: {len(losers)}")
    if opened:
        lines.append(f"⚡ Total Floating PnL: {sum(trade_effective_pnl(t) for t in opened):+.2f}%")
    append_trade_cards(lines, "🟢 <b>Top 3 Open Winners</b>", winners[:3], limit=3)
    append_trade_cards(lines, "🔴 <b>Top 3 Open Losers</b>", losers[:3], limit=3)
    append_trade_cards(lines, "🏆 <b>Top 3 Closed Winners</b>", closed_wins[:3], limit=3)
    append_trade_cards(lines, "💀 <b>Top 3 Closed Losers</b>", closed_losses[:3], limit=3)

    if rejected_checks:
        reason_counts = Counter(item.get("reason", "unknown") for item in rejected_checks)
        lines.extend([SEP, "📉 <b>Rejected After Check — Top Reasons</b>"])
        for reason, count in reason_counts.most_common(5):
            lines.append(f"• {reason}: {count}")
        lines.append("📌 هذه الأسباب تخص فترة التقرير، وليست بالضرورة الحالة الحالية للصفقات المفتوحة الآن.")

    lines.extend([SEP, "💡 إدارة الصفقات: Normal/Strong/Block 40/40/20 | Recovery 50/25/25"])
    return "\n".join(lines)
