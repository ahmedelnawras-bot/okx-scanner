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
    DEFAULT_MARGIN_PER_TRADE,
    money_from_exposure_pct,
    open_trades,
    period_label,
    trade_activity_time,
    trade_effective_pnl,
    trade_money_pnl,
    wallet_impact_lines,
)




# =========================================================
# Scope-isolated execution accounting
# =========================================================
# Execution reports must not borrow Simulation accounting rules.
# In live trading, the account balance shown to the user is the OKX-derived
# starting_balance passed by main.py. Redis tracked trades are used for analytics
# and open-trade diagnostics only.
EXECUTION_SCOPE_MARKER = "execution_scope_okx_truth_v1"
OPEN_EXECUTION_MIN_DISPLAY_PNL_PCT = -100.0
OPEN_EXECUTION_MAX_DISPLAY_PNL_PCT = 1500.0
CLOSED_EXECUTION_MIN_DISPLAY_PNL_PCT = -100.0
CLOSED_EXECUTION_MAX_DISPLAY_PNL_PCT = 1500.0


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _is_simulation_trade(t: TrackedTrade) -> bool:
    try:
        source = str(getattr(t, "trade_source", "") or "").strip().lower()
        bucket = str(getattr(t, "tracking_bucket", "") or "").strip().lower()
        return bool(source == "simulation" or bucket == "simulation")
    except Exception:
        return False


def _is_execution_trade(t: TrackedTrade) -> bool:
    """Execution report scope filter.

    - Explicit simulation records are always excluded.
    - Explicit execution records are included.
    - Legacy tracked records with no scope are kept for backward compatibility
      because older execution history may not carry trade_source yet.
    """
    if _is_simulation_trade(t):
        return False
    try:
        source = str(getattr(t, "trade_source", "") or "").strip().lower()
        bucket = str(getattr(t, "tracking_bucket", "") or "").strip().lower()
        if source == "execution" or bucket == "execution" or bool(getattr(t, "execution_trade", False)):
            return True
        if not source and not bucket:
            return True
    except Exception:
        return True
    return False


def _execution_scope_trades(trades: list[TrackedTrade] | None) -> list[TrackedTrade]:
    return [t for t in list(trades or []) if _is_execution_trade(t)]


def _trade_margin_usdt_local(t: TrackedTrade, fallback: float = DEFAULT_MARGIN_PER_TRADE) -> float:
    for attr in ("used_margin_usdt", "margin_usdt", "allocated_margin_usdt"):
        value = _safe_float(getattr(t, attr, 0.0), 0.0)
        if value > 0:
            return value
    return float(fallback or DEFAULT_MARGIN_PER_TRADE)


def _cap_open_execution_pnl_pct(value: float) -> tuple[float, bool]:
    raw = _safe_float(value, 0.0)
    capped = max(OPEN_EXECUTION_MIN_DISPLAY_PNL_PCT, min(OPEN_EXECUTION_MAX_DISPLAY_PNL_PCT, raw))
    return capped, abs(capped - raw) > 1e-9


def _cap_closed_execution_pnl_pct(value: float) -> tuple[float, bool]:
    raw = _safe_float(value, 0.0)
    capped = max(CLOSED_EXECUTION_MIN_DISPLAY_PNL_PCT, min(CLOSED_EXECUTION_MAX_DISPLAY_PNL_PCT, raw))
    return capped, abs(capped - raw) > 1e-9


def _execution_effective_pnl(t: TrackedTrade) -> tuple[float, bool]:
    """Display PnL for execution reports only.

    OKX balance is the financial truth. Redis tracked PnL is analytics.
    Therefore both open and closed execution records are capped for display so
    corrupted legacy Redis records cannot make reports show impossible wallet
    impacts such as +400k$ or -6000%. This does not mutate Redis, orders, TP/SL,
    lifecycle, or Simulation.
    """
    pct = trade_effective_pnl(t)
    if t in open_trades([t]):
        return _cap_open_execution_pnl_pct(pct)
    if t in closed_trades([t]) or bool(getattr(t, "is_closed", False)):
        return _cap_closed_execution_pnl_pct(pct)
    return pct, False


def _execution_money_pnl(t: TrackedTrade, *, fallback_margin: float = DEFAULT_MARGIN_PER_TRADE) -> tuple[float, bool]:
    pct, capped = _execution_effective_pnl(t)
    return (pct / 100.0) * _trade_margin_usdt_local(t, fallback=fallback_margin), capped


def _execution_wallet_impact_lines(
    trades: list[TrackedTrade],
    *,
    starting_balance: float = 0.0,
    margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE,
    title: str = "Wallet Impact",
) -> list[str]:
    opened = open_trades(trades)
    closed = closed_trades(trades)

    def pct_value(t):
        return _execution_effective_pnl(t)[0]

    closed_capped_symbols = []
    closed_profit = 0.0
    closed_loss = 0.0
    closed_profit_usd = 0.0
    closed_loss_usd = 0.0
    for t in closed:
        pct, capped = _execution_effective_pnl(t)
        if capped:
            closed_capped_symbols.append(str(getattr(t, "symbol", "?") or "?"))
        money = (pct / 100.0) * _trade_margin_usdt_local(t, fallback=margin_per_trade)
        if pct >= 0:
            closed_profit += pct
        else:
            closed_loss += pct
        if money >= 0:
            closed_profit_usd += money
        else:
            closed_loss_usd += money

    floating_profit = sum(max(0.0, pct_value(t)) for t in opened)
    floating_loss = sum(min(0.0, pct_value(t)) for t in opened)

    capped_symbols = []
    floating_profit_usd = 0.0
    floating_loss_usd = 0.0
    for t in opened:
        money, capped = _execution_money_pnl(t, fallback_margin=margin_per_trade)
        if capped:
            capped_symbols.append(str(getattr(t, "symbol", "?") or "?"))
        if money >= 0:
            floating_profit_usd += money
        else:
            floating_loss_usd += money

    closed_net_usd = closed_profit_usd + closed_loss_usd
    floating_net_usd = floating_profit_usd + floating_loss_usd
    total_usd = closed_net_usd + floating_net_usd
    closed_net = closed_profit + closed_loss
    floating_net = floating_profit + floating_loss

    def money_icon(value: float) -> str:
        return ("🟢" if value >= 0 else "🔴") + f" {value:+.2f}$"

    lines = [
        f"💰 <b>{title}</b>",
        f"🧱 Report Scope: <code>{EXECUTION_SCOPE_MARKER}</code>",
        f"📌 OKX Truth Balance: <b>{float(starting_balance or 0.0):.2f} USDT</b>",
        "📌 Tracked PnL below is analytics from execution records only.",
    ]
    if closed_capped_symbols:
        unique = list(dict.fromkeys(closed_capped_symbols))[:8]
        lines.append(f"🧯 Closed PnL sanity capped: <b>{len(closed_capped_symbols)}</b> trade(s) | {', '.join(unique)}")
    if capped_symbols:
        unique = list(dict.fromkeys(capped_symbols))[:8]
        lines.append(f"🧯 Open PnL sanity capped: <b>{len(capped_symbols)}</b> trade(s) | {', '.join(unique)}")

    lines.extend([
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
        "💼 <b>Tracked impact, not OKX balance</b>",
        f"<b>{money_icon(total_usd)}</b>",
    ])
    return lines


def _normalize_behavior_summary_lines_for_execution(
    summary_lines: list[str],
    opened: list[TrackedTrade],
    *,
    label: str = "Execution Behavior Summary",
) -> list[str]:
    fixed: list[str] = []
    open_total = sum(_execution_effective_pnl(t)[0] for t in opened or [])
    for line in list(summary_lines or []):
        text = str(line)
        if "Total Floating PnL:" in text:
            prefix = "⚡ " if text.lstrip().startswith("⚡") else ""
            text = f"{prefix}Total Floating PnL: {open_total:+.2f}%"
        fixed.append(text)
    return fixed



def _execution_behavior_summary_lines(
    trades: list[TrackedTrade],
    *,
    label: str = "Execution Behavior Summary",
) -> list[str]:
    """Execution-only behavior summary using capped/scope-safe PnL.

    The shared behavior_summary_lines() uses raw report_format.trade_effective_pnl().
    That is fine for clean data, but execution Redis can contain old corrupted
    history records. Wallet Impact already caps those records for display; this
    local summary must use the same capped PnL so Avg Winner/Avg Loser cannot
    show impossible values such as +1,995,682%.
    """
    scoped = _execution_scope_trades(list(trades or []))
    opened = open_trades(scoped)
    closed = closed_trades(scoped)

    closed_pnls = [_execution_effective_pnl(t)[0] for t in closed]
    winners = [v for v in closed_pnls if v > 0]
    losers = [v for v in closed_pnls if v < 0]
    avg_winner = sum(winners) / len(winners) if winners else 0.0
    avg_loser = sum(losers) / len(losers) if losers else 0.0

    total_trades = len(closed)
    tp1_hits = sum(1 for t in closed if bool(getattr(t, "tp1_hit", False)) or bool(getattr(t, "tp2_hit", False)))
    tp2_hits = sum(1 for t in closed if bool(getattr(t, "tp2_hit", False)))
    tp1_rate = (tp1_hits / total_trades * 100.0) if total_trades else 0.0
    tp2_rate = (tp2_hits / total_trades * 100.0) if total_trades else 0.0
    tp1_to_tp2 = (tp2_hits / tp1_hits * 100.0) if tp1_hits else 0.0

    trailing_exits = sum(1 for t in closed if str(getattr(t, "status", "") or "").lower() == "trailing_hit" or bool(getattr(t, "trailing_hit", False)))
    breakeven_exits = sum(1 for t in closed if str(getattr(t, "status", "") or "").lower() == "breakeven_after_tp1")
    direct_sl = sum(1 for t in closed if str(getattr(t, "status", "") or "").lower() == "closed_loss" and not bool(getattr(t, "tp1_hit", False)))
    trailing_rate = (trailing_exits / total_trades * 100.0) if total_trades else 0.0
    breakeven_rate = (breakeven_exits / total_trades * 100.0) if total_trades else 0.0
    direct_sl_rate = (direct_sl / total_trades * 100.0) if total_trades else 0.0

    floating_total = sum(_execution_effective_pnl(t)[0] for t in opened)
    rr_quality = "إيجابي ✅" if (avg_winner > abs(avg_loser) or floating_total >= 0) else "ضعيف ⚠️"

    capped_closed = sum(1 for t in closed if _execution_effective_pnl(t)[1])
    capped_open = sum(1 for t in opened if _execution_effective_pnl(t)[1])

    lines = [
        f"🧠 <b>{label}</b>",
        "📦 Model: Normal/Strong/Block 30/50/20 | Recovery 50/25/25",
        f"📈 Avg Winner: {avg_winner:+.2f}%",
        f"📉 Avg Loser: {avg_loser:+.2f}%",
        f"🎯 TP1 Rate: {tp1_rate:.1f}% | 🏁 TP2 Rate: {tp2_rate:.1f}%",
        f"🔁 TP1 → TP2: {tp1_to_tp2:.1f}%",
        f"🔄 Trailing Exit: {trailing_rate:.1f}%",
        f"🔒 Breakeven Exit: {breakeven_rate:.1f}%",
        f"🛑 Direct SL: {direct_sl_rate:.1f}%",
        f"⚡ Total Floating PnL: {floating_total:+.2f}%",
        f"💡 Risk / Reward Quality: {rr_quality}",
    ]
    if capped_closed or capped_open:
        lines.append(f"🧯 Summary sanity capped: closed={capped_closed} | open={capped_open}")
    return lines


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


def _trade_net_usd(trades: list[TrackedTrade], margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE) -> float:
    return sum((_execution_effective_pnl(t)[0] / 100.0) * _trade_margin_usdt_local(t, fallback=margin_per_trade) for t in trades)


def _fmt_price_local(value) -> str:
    v = _safe_float(value, 0.0)
    if v == 0:
        return "0"
    if abs(v) >= 1:
        return f"{v:.6f}".rstrip("0").rstrip(".")
    return f"{v:.10f}".rstrip("0").rstrip(".")


def _execution_trade_card_lines(t: TrackedTrade, *, fallback_margin: float = DEFAULT_MARGIN_PER_TRADE) -> list[str]:
    pct, capped = _execution_effective_pnl(t)
    pnl_name = "Realized PnL" if (bool(getattr(t, "is_closed", False)) or t in closed_trades([t])) else "Floating PnL"
    margin = _trade_margin_usdt_local(t, fallback=fallback_margin)
    impact = (pct / 100.0) * margin
    cap_note = " | 🧯 capped" if capped else ""
    return [
        f"• <b>{getattr(t, 'symbol', '-')}</b> | {pct:+.2f}% {pnl_name}{cap_note}",
        f"⚙️ Margin {margin:.2f}$ | Impact {impact:+.2f}$",
        f"🎯 Entry: {_fmt_price_local(getattr(t, 'entry', 0.0))}",
        f"🎯 TP1: {_fmt_price_local(getattr(t, 'tp1', 0.0))} | 🏁 TP2: {_fmt_price_local(getattr(t, 'tp2', 0.0))}",
        f"🛡 SL: {_fmt_price_local(getattr(t, 'sl', 0.0))}",
    ]


def _append_execution_trade_cards(lines: list[str], title: str, items: list[TrackedTrade], *, limit: int = 3, margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE) -> None:
    if not items:
        return
    lines.extend([SEP, title])
    for index, trade in enumerate(items[:limit]):
        if index:
            lines.append("┄┄┄┄┄┄┄┄")
        lines.extend(_execution_trade_card_lines(trade, fallback_margin=margin_per_trade))
    remaining = len(items) - limit
    if remaining > 0:
        lines.append(f"📂 +{remaining} more trades...")


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



def _looks_like_simulation_report_title(title: str | None) -> bool:
    text = str(title or "").lower()
    return bool("simulation" in text or "محاك" in text or "virtual" in text)


def _normalize_behavior_summary_lines_for_report(
    summary_lines: list[str],
    opened: list[TrackedTrade],
    *,
    label: str = "Execution Behavior Summary",
) -> list[str]:
    """Keep Behavior Summary consistent with the Open Trades section.

    The shared behavior_summary_lines() helper can be reused by execution and
    simulation reports. In some runtime paths it may print Total Floating PnL
    from a broader/stale trade set, while the Open Trades section correctly
    uses only currently-open tracked trades. This function is display-only: it
    does not mutate trades, lifecycle, TP/SL, OKX orders, or wallet accounting.
    """
    fixed: list[str] = []
    open_total = sum(trade_effective_pnl(t) for t in opened or [])
    for line in list(summary_lines or []):
        text = str(line)
        if "Execution Behavior Summary" in text and label != "Execution Behavior Summary":
            text = text.replace("Execution Behavior Summary", label)
        if "Total Floating PnL:" in text:
            prefix = "⚡ " if text.lstrip().startswith("⚡") else ""
            text = f"{prefix}Total Floating PnL: {open_total:+.2f}%"
        fixed.append(text)
    return fixed


def build_execution_period_table(
    execution_results: list[dict],
    trades: list[TrackedTrade] | None = None,
    title: str = "🚀 تقرير الصفقات المرشحة للتنفيذ",
    period: str = "last_7d",
    margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE,
) -> str:
    """Excel-like execution snapshot.

    Checked / Accepted / Rejected come from execution checks.
    Open / Closed / Net come from tracked trades only.
    """
    trades = filter_trades_by_period(_execution_scope_trades(trades or []), period)
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
        net = _trade_net_usd(row_trades, margin_per_trade)
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
    starting_balance: float = 1000.0,
    margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE,
) -> str:
    if table:
        return build_execution_period_table(
            execution_results,
            trades,
            title="🚀 تقرير الصفقات المرشحة — Execution",
            period=period,
            margin_per_trade=margin_per_trade,
        )

    trades = filter_trades_by_period(_execution_scope_trades(trades or []), period)
    execution_results = filter_checks_by_period(execution_results or [], period)

    accepted_checks = _accepted_gate_checks(execution_results)
    rejected_checks = _rejected_checks(execution_results)
    checked = len(execution_results)
    counts = _execution_path_counts(execution_results)
    acc_rate = (len(accepted_checks) / max(1, checked)) * 100 if checked else 0.0

    opened = open_trades(trades)
    closed = closed_trades(trades)
    win_count, loss_count, wr = _closed_wr_parts(trades)
    winners = sorted([t for t in opened if _execution_effective_pnl(t)[0] >= 0], key=lambda t: _execution_effective_pnl(t)[0], reverse=True)
    losers = sorted([t for t in opened if _execution_effective_pnl(t)[0] < 0], key=lambda t: _execution_effective_pnl(t)[0])
    closed_wins = sorted([t for t in closed if _execution_effective_pnl(t)[0] > 0], key=lambda t: _execution_effective_pnl(t)[0], reverse=True)
    closed_losses = sorted([t for t in closed if _execution_effective_pnl(t)[0] < 0], key=lambda t: _execution_effective_pnl(t)[0])

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

    behavior_label = "Simulation Behavior Summary" if _looks_like_simulation_report_title(title) else "Execution Behavior Summary"

    lines.extend([SEP, *_execution_wallet_impact_lines(
        trades,
        starting_balance=starting_balance,
        margin_per_trade=margin_per_trade,
        title="Wallet Impact",
    )])
    behavior_lines = _execution_behavior_summary_lines(trades, label=behavior_label)
    lines.extend([SEP, *behavior_lines])
    lines.extend([SEP, "📂 <b>Open Trades</b>"])
    lines.append(f"🟢 Open Winners: {len(winners)} | 🔴 Open Losers: {len(losers)}")
    if opened:
        lines.append(f"⚡ Total Floating PnL: {sum(_execution_effective_pnl(t)[0] for t in opened):+.2f}%")
    _append_execution_trade_cards(lines, "🟢 <b>Top 3 Open Winners</b>", winners[:3], limit=3, margin_per_trade=margin_per_trade)
    _append_execution_trade_cards(lines, "🔴 <b>Top 3 Open Losers</b>", losers[:3], limit=3, margin_per_trade=margin_per_trade)
    _append_execution_trade_cards(lines, "🏆 <b>Top 3 Closed Winners</b>", closed_wins[:3], limit=3, margin_per_trade=margin_per_trade)
    _append_execution_trade_cards(lines, "💀 <b>Top 3 Closed Losers</b>", closed_losses[:3], limit=3, margin_per_trade=margin_per_trade)

    if rejected_checks:
        reason_counts = Counter(item.get("reason", "unknown") for item in rejected_checks)
        lines.extend([SEP, "📉 <b>Rejected After Check — Top Reasons</b>"])
        for reason, count in reason_counts.most_common(5):
            lines.append(f"• {reason}: {count}")
        lines.append("📌 هذه الأسباب تخص فترة التقرير، وليست بالضرورة الحالة الحالية للصفقات المفتوحة الآن.")

    lines.extend([SEP, "💡 إدارة الصفقات: Normal/Strong/Block 30/50/20 | Recovery 50/25/25"])
    return "\n".join(lines)
