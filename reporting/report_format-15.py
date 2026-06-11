from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone, timedelta
from typing import Iterable

from tracking.models import TrackedTrade
from utils.constants import DEFAULT_LEVERAGE, LEVERAGE_NOTE_AR

SEP = "━━━━━━━━━━━━"
THIN_SEP = "┄┄┄┄┄┄┄┄"
DEFAULT_STARTING_BALANCE = 1000.0
DEFAULT_MARGIN_PER_TRADE = 35.0
DEFAULT_REPORT_LEVERAGE = float(DEFAULT_LEVERAGE or 1)

OPEN_STATUSES = {"open", "tp1_partial", "tp2_partial", "runner"}

CLOSED_STATUSES = {
    "closed_loss",
    "breakeven_after_tp1",
    "trailing_hit",
    "closed_win",
}

WIN_STATUSES = {
    "tp1_partial",
    "tp2_partial",
    "runner",
    "trailing_hit",
    "closed_win",
    "breakeven_after_tp1",
}

LOSS_STATUSES = {
    "closed_loss",
}


def leveraged_pct(
    raw_pct: float,
    leverage: float = DEFAULT_REPORT_LEVERAGE,
) -> float:
    """
    Convert a raw price move percentage into displayed leveraged performance.

    Tracking keeps raw price-move percentages for TP/SL logic.
    Reports and wallet impact must display leveraged performance.
    """

    return float(raw_pct or 0.0) * float(leverage or 1.0)


def money_from_exposure_pct(
    pct: float,
    margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE,
) -> float:
    """
    Return wallet impact in USD from a leveraged exposure percentage.

    Backward-compatible helper for places that still pass one aggregate
    margin. For accurate wallet reports, prefer trade_money_pnl() so each
    trade uses its own stored margin.
    """

    return (float(pct or 0.0) / 100.0) * margin_per_trade


def trade_margin_usdt(
    t: TrackedTrade,
    fallback: float = DEFAULT_MARGIN_PER_TRADE,
) -> float:
    """Return the margin that belonged to this specific trade.

    Critical Simulation rule:
    Simulation reports must use the same paper-wallet margin used by
    Simulation Daily Balance. Old simulation records can carry both
    used_margin_usdt and simulation_margin_usdt with different values; if the
    report prefers used_margin_usdt while the wallet snapshot prefers
    simulation_margin_usdt, Daily Balance and Wallet Impact diverge.

    Priority:
    - Simulation records: simulation_margin_usdt first.
    - Execution/live records: used_margin_usdt first.
    - Then legacy aliases and fallback.
    """

    try:
        trade_source = str(getattr(t, "trade_source", "") or "").strip().lower()
        bucket = str(getattr(t, "tracking_bucket", "") or "").strip().lower()
        is_simulation = bool(trade_source == "simulation" or bucket == "simulation")
    except Exception:
        is_simulation = False

    attrs = (
        ("simulation_margin_usdt", "used_margin_usdt", "margin_usdt", "allocated_margin_usdt")
        if is_simulation
        else ("used_margin_usdt", "simulation_margin_usdt", "margin_usdt", "allocated_margin_usdt")
    )

    for attr in attrs:
        try:
            value = float(getattr(t, attr, 0.0) or 0.0)
        except Exception:
            value = 0.0
        if value > 0:
            return value

    return float(fallback or DEFAULT_MARGIN_PER_TRADE)


def trade_money_pnl(
    t: TrackedTrade,
    *,
    fallback_margin: float = DEFAULT_MARGIN_PER_TRADE,
) -> float:
    """USD wallet impact for one trade using that trade's own margin.

    trade_effective_pnl() is already the leveraged/exposure percentage used
    by reports, so converting to money only needs the stored margin.
    """

    return (trade_effective_pnl(t) / 100.0) * trade_margin_usdt(
        t,
        fallback=fallback_margin,
    )


def trade_actual_leverage(t: TrackedTrade) -> float:
    """Return the actual/effective leverage saved on the trade, if available."""

    for attr in (
        "effective_leverage",
        "actual_leverage",
        "exchange_leverage",
        "leverage",
        "requested_leverage",
    ):
        try:
            value = float(getattr(t, attr, 0.0) or 0.0)
        except Exception:
            value = 0.0
        if value > 0:
            return value

    return 0.0


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _trade_report_leverage(t: TrackedTrade) -> float:
    """Per-trade leverage for report calculations.

    Old reports multiplied every trade by DEFAULT_REPORT_LEVERAGE. That is fine
    only when all trades use the same leverage. Simulation/execution records can
    store effective_leverage, actual_leverage, or simulation_leverage, so the
    report should use the trade's own leverage when present.
    """

    leverage = trade_actual_leverage(t)
    if leverage > 0:
        return leverage

    for attr in ("simulation_leverage", "default_leverage"):
        value = _safe_float(getattr(t, attr, 0.0), 0.0)
        if value > 0:
            return value

    return float(DEFAULT_REPORT_LEVERAGE or 1.0)


def _trade_price_raw_pnl(t: TrackedTrade) -> float | None:
    """Recalculate raw open-trade PnL from entry/current_price when possible.

    Stored pnl_pct / runner_pnl_pct can become stale after recovery, Redis
    restore, or report rebuilding. For an open trade, entry + current_price is
    the freshest source for floating PnL display. Closed trades still use their
    stored realized_pnl_pct.
    """

    entry = _safe_float(getattr(t, "entry", 0.0), 0.0)
    current = _safe_float(getattr(t, "current_price", 0.0), 0.0)

    if entry <= 0:
        return None

    if current <= 0:
        for attr in ("mark_price", "last_price", "close_price"):
            current = _safe_float(getattr(t, attr, 0.0), 0.0)
            if current > 0:
                break

    if current <= 0:
        return None

    return ((current - entry) / entry) * 100.0


def _current_or_stored_raw_pnl(t: TrackedTrade) -> float:
    recalculated = _trade_price_raw_pnl(t)
    if recalculated is not None:
        return recalculated
    return _safe_float(getattr(t, "pnl_pct", 0.0), 0.0)


def _raw_move_to_price(t: TrackedTrade, price: float) -> float | None:
    """Raw price-move pct from entry to a target price, if both are valid."""
    entry = _safe_float(getattr(t, "entry", 0.0), 0.0)
    px = _safe_float(price, 0.0)
    if entry <= 0 or px <= 0:
        return None
    return ((px - entry) / entry) * 100.0


def _planned_realized_raw_for_open_trade(t: TrackedTrade, stage: str) -> float | None:
    """Rebuild realized raw PnL for open partial/runner trades from Entry/TPs.

    For open trades, stored realized_pnl_pct can be polluted by old Redis state
    or previous report/lifecycle bugs. If TP prices are available, the report
    should deterministically reconstruct the closed portions from the trade plan:
    - TP1 portion = raw(entry→tp1) * tp1_close_pct
    - TP2 portion = raw(entry→tp2) * tp2_close_pct

    Closed trades still use stored realized_pnl_pct; this helper is only used
    for currently-open partial/runner records.
    """
    stage = str(stage or "").lower()
    tp1_raw = _raw_move_to_price(t, _safe_float(getattr(t, "tp1", 0.0), 0.0))
    if tp1_raw is None:
        return None

    tp1_close_pct = _tp_close_pct(t, "tp1_close_pct", 40.0)
    realized = tp1_raw * (tp1_close_pct / 100.0)

    if stage == "tp2":
        tp2_raw = _raw_move_to_price(t, _safe_float(getattr(t, "tp2", 0.0), 0.0))
        if tp2_raw is None:
            return None
        tp2_close_pct = _tp_close_pct(t, "tp2_close_pct", 40.0)
        realized += tp2_raw * (tp2_close_pct / 100.0)

    return realized


def _tp_close_pct(t: TrackedTrade, attr: str, fallback: float) -> float:
    value = _safe_float(getattr(t, attr, fallback), fallback)
    return max(0.0, min(100.0, value))


def trade_actuals_line(
    t: TrackedTrade,
    *,
    fallback_margin: float = DEFAULT_MARGIN_PER_TRADE,
) -> str:
    """Compact per-trade live/execution metrics for Telegram cards.

    Shows:
    - actual/effective leverage when known,
    - the trade's stored margin,
    - USD wallet impact using the same effective PnL as the report.
    """

    leverage = trade_actual_leverage(t)
    margin = trade_margin_usdt(t, fallback=fallback_margin)
    impact = trade_money_pnl(t, fallback_margin=fallback_margin)

    leverage_label = f"{leverage:.0f}x" if leverage > 0 else "-x"
    return f"⚙️ {leverage_label} | Margin {margin:.2f}$ | Impact {impact:+.2f}$"


def color_signed(value: float, unit: str = "%") -> str:
    icon = "🟢" if value >= 0 else "🔴"
    return f"{icon} {value:+.2f}{unit}"


def money_line(value: float) -> str:
    icon = "🟢" if value >= 0 else "🔴"
    return f"{icon} {value:+.2f}$"


def fmt_price(value: float | int | None) -> str:
    """Adaptive price formatter for very small-priced symbols like SATS.

    Prevents meaningful prices from being displayed as 0.000000 in reports.
    """
    if value is None:
        return "-"

    try:
        v = float(value)
    except Exception:
        return str(value)

    if v == 0:
        return "0"

    abs_v = abs(v)

    if abs_v >= 100:
        decimals = 2
    elif abs_v >= 1:
        decimals = 4
    elif abs_v >= 0.01:
        decimals = 6
    elif abs_v >= 0.0001:
        decimals = 8
    elif abs_v >= 0.000001:
        decimals = 10
    else:
        decimals = 12

    formatted = f"{v:.{decimals}f}"

    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")

    return formatted


def fmt_holding_duration(
    start: datetime | None,
    end: datetime | None = None,
) -> str:
    start = _ensure_aware(start)
    end = _ensure_aware(end) or datetime.now(timezone.utc)

    if start is None:
        return "unknown"

    seconds = max(0, int((end - start).total_seconds()))

    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60

    if days > 0:
        return f"{days}d {hours}h"

    if hours > 0:
        return f"{hours}h {minutes}m"

    return f"{minutes}m"


def trade_timer_label(t: TrackedTrade) -> str:
    if getattr(t, "is_closed", False):
        return (
            f"Closed after "
            f"{fmt_holding_duration(t.opened_at, t.closed_at or t.updated_at)}"
        )

    return f"Running {fmt_holding_duration(t.opened_at)}"


def open_trades(trades: Iterable[TrackedTrade]) -> list[TrackedTrade]:
    return [t for t in trades if t.status in OPEN_STATUSES]


def closed_trades(trades: Iterable[TrackedTrade]) -> list[TrackedTrade]:
    return [t for t in trades if t.status in CLOSED_STATUSES]


def trade_raw_effective_pnl(t: TrackedTrade) -> float:
    """
    Effective PnL using raw price-move percentages.

    Important:
    - Closed trades use stored realized_pnl_pct.
    - Open trades recalculate the current floating leg from entry/current_price.
      This prevents stale Redis pnl_pct values from poisoning reports.
    - Partial/runner trades keep realized_pnl_pct for filled portions and
      recalculate only the still-open remaining portion.
    """

    if t.status in CLOSED_STATUSES or bool(getattr(t, "is_closed", False)):
        return _safe_float(getattr(t, "realized_pnl_pct", 0.0), 0.0)

    current_raw = _current_or_stored_raw_pnl(t)
    stored_realized_raw = _safe_float(getattr(t, "realized_pnl_pct", 0.0), 0.0)

    if bool(getattr(t, "tp2_hit", False)):
        # For an open TP2 runner, never trust old stored realized_pnl_pct when
        # TP1/TP2 prices exist. Rebuild closed 30/50 (or configured) portions
        # from Entry/TP targets, then add the live runner leg from current price.
        planned_realized = _planned_realized_raw_for_open_trade(t, "tp2")
        realized_raw = planned_realized if planned_realized is not None else stored_realized_raw
        runner_pct = _tp_close_pct(t, "runner_close_pct", 20.0)
        return realized_raw + current_raw * (runner_pct / 100.0)

    if bool(getattr(t, "tp1_hit", False)):
        # Same protection for TP1 partial records: rebuild the realized TP1
        # portion from Entry/TP1 if possible, then add only the remaining open leg.
        planned_realized = _planned_realized_raw_for_open_trade(t, "tp1")
        realized_raw = planned_realized if planned_realized is not None else stored_realized_raw
        tp1_close_pct = _tp_close_pct(t, "tp1_close_pct", 40.0)
        remaining_pct = max(0.0, 100.0 - tp1_close_pct)
        return realized_raw + current_raw * (remaining_pct / 100.0)

    return current_raw


def trade_effective_pnl(t: TrackedTrade) -> float:
    """
    Displayed leveraged performance/exposure percentage.

    Uses the leverage stored on the trade when available instead of applying one
    global default leverage to every record.
    """

    return leveraged_pct(trade_raw_effective_pnl(t), _trade_report_leverage(t))


def trade_current_raw_pnl(t: TrackedTrade) -> float:
    return _current_or_stored_raw_pnl(t)


def trade_current_exposure_pnl(t: TrackedTrade) -> float:
    return leveraged_pct(trade_current_raw_pnl(t), _trade_report_leverage(t))


def trade_stage(t: TrackedTrade) -> str:
    return getattr(t, "stage_label", "OPEN") or "OPEN"


def trade_mode_label(t: TrackedTrade) -> str:
    """Return the market/execution mode label saved on the trade for display only."""
    mode = ""

    for attr in (
        "market_mode",
        "risk_mode",
        "entry_market_mode",
        "signal_market_mode",
        "mode",
    ):
        try:
            value = str(getattr(t, attr, "") or "").strip()
        except Exception:
            value = ""
        if value:
            mode = value
            break

    execution_path = str(getattr(t, "execution_path", "") or "").strip()

    if not mode:
        if execution_path == "recovery":
            mode = "RECOVERY_LONG"
        elif execution_path == "block_exception":
            mode = "BLOCK_EXCEPTION"
        else:
            mode = "UNKNOWN"

    # Keep the Telegram card short but informative.
    mode = mode.replace("MODE_", "").replace("_LONG_ONLY", "").replace("_LONG", "")

    if execution_path and execution_path not in {"general", "normal"}:
        path_label = execution_path.replace("_", " ").title()
        return f"{mode} / {path_label}"

    return mode


# =========================================================
# Setup Cleaning + Deduplication
# =========================================================

def clean_setup(t: TrackedTrade) -> str:
    raw = (
        (t.setup_type or "unknown")
        .replace("_", " ")
        .strip()
        .title()
    )

    tags = [
        str(x).replace("_", " ").strip().title()
        for x in (t.execution_setup_tags or [])
        if x
    ]

    # Remove duplicates while preserving order
    tags = list(dict.fromkeys(tags))

    # Prevent repeating raw setup inside tags
    tags = [x for x in tags if x != raw]

    if tags:
        return f"{raw} | {', '.join(tags[:2])}"

    return raw


# =========================================================
# TradingView URL Fix
# =========================================================

TV_SYMBOL_MAP = {
    # problematic symbols can be overridden here
    # "AT-USDT-SWAP": "OKX:ATUSDT.P",
}


def build_tv_symbol(symbol: str) -> str:
    raw = str(symbol or "").upper()

    if raw in TV_SYMBOL_MAP:
        return TV_SYMBOL_MAP[raw]

    if raw.endswith("-USDT-SWAP"):
        base = raw.replace("-USDT-SWAP", "")
        return f"OKX:{base}USDT.P"

    fallback = raw.replace("-", "")

    return f"OKX:{fallback}"


def tradingview_url(symbol: str) -> str:
    tv_symbol = build_tv_symbol(symbol)

    return (
        "https://www.tradingview.com/chart/"
        f"?symbol={tv_symbol}"
    )


# =========================================================
# Trade Cards
# =========================================================

def trade_card_lines(
    t: TrackedTrade,
    *,
    exposure_label: bool = True,
) -> list[str]:

    pnl = trade_effective_pnl(t)

    if exposure_label:
        pnl_name = (
            "Realized PnL"
            if getattr(t, "is_closed", False)
            else "Floating PnL"
        )

        pnl_label = f"{pnl:+.2f}% {pnl_name}"

    else:
        pnl_label = f"{pnl:+.2f}%"

    extra = []

    if getattr(t, "protected_runner", False):
        extra.append("🛡 Protected Runner")

    elif getattr(t, "protected_on_block", False):
        extra.append("🛡 Protected")

    if getattr(t, "trailing_tightened", False):
        extra.append("🔧 Tightened")

    extra_text = f" | {' | '.join(extra)}" if extra else ""

    return [
        f"• <b>{t.symbol}</b> | {pnl_label}",

        f"⏱️ {trade_timer_label(t)} | "
        f"{trade_stage(t)} | "
        f"🧭 {trade_mode_label(t)} | "
        f"⭐ {float(t.score or 0):.2f}"
        f"{extra_text}",

        trade_actuals_line(t),

        # =================================================
        # Actual Entry Price
        # =================================================
        f"🎯 Entry: {fmt_price(getattr(t, 'entry', 0.0))}",

        f"🎯 TP1: {fmt_price(getattr(t, 'tp1', 0.0))} | "
        f"🏁 TP2: {fmt_price(getattr(t, 'tp2', 0.0))}",

        f"📦 Close Plan: "
        f"{float(getattr(t, 'tp1_close_pct', 40.0) or 40.0):.0f}/"
        f"{float(getattr(t, 'tp2_close_pct', 40.0) or 40.0):.0f}/"
        f"{float(getattr(t, 'runner_close_pct', 20.0) or 20.0):.0f}",

        f"🛡 SL: {fmt_price(getattr(t, 'sl', 0.0))}",

        f"🧠 {clean_setup(t)}",

        f"🔗 TradingView: {tradingview_url(t.symbol)}",
    ]


def append_trade_cards(
    lines: list[str],
    title: str,
    items: list[TrackedTrade],
    limit: int = 3,
) -> None:
    if not items:
        return

    lines.extend([SEP, title])

    for index, trade in enumerate(items[:limit]):
        if index:
            lines.append(THIN_SEP)

        lines.extend(trade_card_lines(trade))

    remaining = len(items) - limit

    if remaining > 0:
        lines.append(f"📂 +{remaining} more trades...")


def behavior_summary_lines(
    trades: list[TrackedTrade],
    *,
    label: str = "Behavior Summary",
) -> list[str]:

    total = max(1, len(trades))

    winners = [t for t in trades if trade_effective_pnl(t) > 0]

    losers = [t for t in trades if trade_effective_pnl(t) < 0]

    tp1_count = sum(1 for t in trades if t.tp1_hit)

    tp2_count = sum(1 for t in trades if t.tp2_hit)

    trailing = sum(1 for t in trades if t.status == "trailing_hit")

    breakeven = sum(
        1 for t in trades if t.status == "breakeven_after_tp1"
    )

    direct_sl = sum(
        1 for t in trades if t.status == "closed_loss"
    )

    avg_winner = (
        sum(trade_effective_pnl(t) for t in winners)
        / max(1, len(winners))
    )

    avg_loser = (
        sum(trade_effective_pnl(t) for t in losers)
        / max(1, len(losers))
    )

    tp1_rate = tp1_count / total * 100.0

    tp2_rate = tp2_count / total * 100.0

    tp1_to_tp2 = (
        tp2_count / max(1, tp1_count) * 100.0
        if tp1_count
        else 0.0
    )

    floating = sum(
        trade_current_exposure_pnl(t)
        for t in open_trades(trades)
    )

    quality = (
        "إيجابي ✔️"
        if (avg_winner + avg_loser) >= 0
        else "يحتاج مراجعة ⚠️"
    )

    return [
        f"🧠 <b>{label}</b>",
        "📦 Model: Normal/Strong/Block 30/50/20 | Recovery 50/25/25",
        f"📈 Avg Winner: {avg_winner:+.2f}%",
        f"📉 Avg Loser: {avg_loser:+.2f}%",
        f"🎯 TP1 Rate: {tp1_rate:.1f}% | 🏁 TP2 Rate: {tp2_rate:.1f}%",
        f"🔁 TP1 → TP2: {tp1_to_tp2:.1f}%",
        f"🔄 Trailing Exit: {trailing / total * 100:.1f}%",
        f"🔒 Breakeven Exit: {breakeven / total * 100:.1f}%",
        f"🛑 Direct SL: {direct_sl / total * 100:.1f}%",
        f"⚡ Total Floating PnL: {floating:+.2f}%",
        f"💡 Risk / Reward Quality: {quality}",
    ]


def wallet_impact_lines(
    trades: list[TrackedTrade],
    *,
    starting_balance: float = DEFAULT_STARTING_BALANCE,
    margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE,
    title: str = "Wallet Impact",
) -> list[str]:

    opened = open_trades(trades)

    closed = closed_trades(trades)

    closed_profit = sum(
        max(0.0, trade_effective_pnl(t))
        for t in closed
    )

    closed_loss = sum(
        min(0.0, trade_effective_pnl(t))
        for t in closed
    )

    floating_profit = sum(
        max(0.0, trade_effective_pnl(t))
        for t in opened
    )

    floating_loss = sum(
        min(0.0, trade_effective_pnl(t))
        for t in opened
    )

    closed_net = closed_profit + closed_loss

    floating_net = floating_profit + floating_loss

    total = closed_net + floating_net

    closed_profit_usd = sum(
        max(0.0, trade_money_pnl(t, fallback_margin=margin_per_trade))
        for t in closed
    )

    closed_loss_usd = sum(
        min(0.0, trade_money_pnl(t, fallback_margin=margin_per_trade))
        for t in closed
    )

    floating_profit_usd = sum(
        max(0.0, trade_money_pnl(t, fallback_margin=margin_per_trade))
        for t in opened
    )

    floating_loss_usd = sum(
        min(0.0, trade_money_pnl(t, fallback_margin=margin_per_trade))
        for t in opened
    )

    closed_net_usd = closed_profit_usd + closed_loss_usd

    floating_net_usd = floating_profit_usd + floating_loss_usd

    total_usd = closed_net_usd + floating_net_usd

    return [
        f"💰 <b>{title}</b>",
        f"📌 رأس المال: {starting_balance:.0f}$",

        "",

        "✅ <b>الصفقات المغلقة</b>",

        "📈 الأرباح",

        f"{closed_profit_usd:+.2f}$ | "
        f"{closed_profit:+.2f}% Realized PnL",

        "📉 الخسائر",

        f"{closed_loss_usd:+.2f}$ | "
        f"{closed_loss:+.2f}% Realized PnL",

        "⚖️ الصافي",

        f"<b>{money_line(closed_net_usd)} | "
        f"{closed_net:+.2f}% Realized PnL</b>",

        "",

        "🔄 <b>الصفقات المفتوحة</b>",

        "📈 الأرباح العائمة",

        f"{floating_profit_usd:+.2f}$ | "
        f"{floating_profit:+.2f}% Total Floating PnL",

        "📉 الخسائر العائمة",

        f"{floating_loss_usd:+.2f}$ | "
        f"{floating_loss:+.2f}% Total Floating PnL",

        "⚖️ Total Floating PnL",

        f"<b>{money_line(floating_net_usd)} | "
        f"{floating_net:+.2f}% Total Floating PnL</b>",

        "",

        "💼 <b>التأثير الحالي على المحفظة</b>",

        f"<b>{money_line(total_usd)}</b>",
    ]


def quick_stats_lines(
    trades: list[TrackedTrade],
    *,
    label: str = "Quick Stats",
    item_name: str = "Trades",
) -> list[str]:

    opened = open_trades(trades)

    closed = closed_trades(trades)

    winners = [
        t for t in closed
        if trade_effective_pnl(t) > 0
    ]

    losers = [
        t for t in closed
        if trade_effective_pnl(t) < 0
    ]

    denom = len(winners) + len(losers)

    win_rate = (
        len(winners) / denom * 100.0
        if denom
        else 0.0
    )

    return [
        f"📊 <b>{label}</b>",
        f"• {item_name}: {len(trades)}",
        f"• Open: {len(opened)}",
        f"• Closed: {len(closed)}",
        f"🏆 Win Rate: <b>{win_rate:.1f}%</b>",
        f"🟢 Winners: {len(winners)} | 🔴 Losers: {len(losers)}",
    ]


def _ensure_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt


def parse_dt(value) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        return _ensure_aware(value)

    try:
        text = str(value)

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        dt = datetime.fromisoformat(text)

        return _ensure_aware(dt)

    except Exception:
        return None


def period_cutoff(
    period: str,
    now: datetime | None = None,
) -> datetime | None:

    now = now or datetime.now(timezone.utc)

    if period in ("last_1h", "1h"):
        return now - timedelta(hours=1)

    if period == "today":
        return now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    if period in ("last_7d", "7d"):
        return now - timedelta(days=7)

    if period in ("month", "last_30d", "30d"):
        return now - timedelta(days=30)

    return None


def trade_activity_time(t: TrackedTrade) -> datetime | None:
    return _ensure_aware(
        t.closed_at or t.updated_at or t.opened_at
    )


def filter_trades_by_period(
    trades: list[TrackedTrade],
    period: str,
) -> list[TrackedTrade]:

    cutoff = period_cutoff(period)

    if cutoff is None:
        return list(trades)

    return [
        t
        for t in trades
        if (
            trade_activity_time(t)
            or datetime.min.replace(tzinfo=timezone.utc)
        ) >= cutoff
    ]


def check_time(item: dict) -> datetime | None:
    for key in (
        "ts",
        "created_at",
        "time",
        "timestamp",
        "updated_at",
    ):
        dt = parse_dt(item.get(key))

        if dt is not None:
            return dt

    return None


def filter_checks_by_period(
    items: list[dict],
    period: str,
) -> list[dict]:

    cutoff = period_cutoff(period)

    if cutoff is None:
        return list(items)

    return [
        x
        for x in items
        if (
            check_time(x)
            or datetime.min.replace(tzinfo=timezone.utc)
        ) >= cutoff
    ]


def period_label(period: str) -> str:
    return {
        "since_start": "Since Start",
        "month": "Month",
        "last_7d": "Last 7D",
        "today": "Today",
        "last_1h": "Last 1H",
    }.get(period, period)


def score_range(score: float) -> str:
    if score >= 9:
        return "9+"

    if score >= 8:
        return "8.0-8.99"

    if score >= 7.5:
        return "7.5-7.99"

    if score >= 7:
        return "7.0-7.49"

    if score >= 6.5:
        return "6.5-6.99"

    return "<6.5"


def counter_lines(
    title: str,
    counter: Counter,
    limit: int = 5,
    suffix: str = "",
) -> list[str]:

    if not counter:
        return []

    lines = [title]

    for name, count in counter.most_common(limit):
        lines.append(f"• {name} — {count}{suffix}")

    return lines
