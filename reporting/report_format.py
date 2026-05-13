from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Iterable

from tracking.models import TrackedTrade
from utils.constants import LEVERAGE_NOTE_AR

SEP = "━━━━━━━━━━━━"
THIN_SEP = "┄┄┄┄┄┄┄┄"
DEFAULT_STARTING_BALANCE = 1000.0
DEFAULT_MARGIN_PER_TRADE = 35.0
OPEN_STATUSES = {"open", "tp1_partial", "tp2_partial", "runner"}
CLOSED_STATUSES = {"closed_loss", "breakeven_after_tp1", "trailing_hit", "closed_win"}
WIN_STATUSES = {"tp1_partial", "tp2_partial", "runner", "trailing_hit", "closed_win", "breakeven_after_tp1"}
LOSS_STATUSES = {"closed_loss"}


def money_from_exposure_pct(pct: float, margin_per_trade: float = DEFAULT_MARGIN_PER_TRADE) -> float:
    return (pct / 100.0) * margin_per_trade


def color_signed(value: float, unit: str = "%") -> str:
    icon = "🟢" if value >= 0 else "🔴"
    return f"{icon} {value:+.2f}{unit}"


def money_line(value: float) -> str:
    icon = "🟢" if value >= 0 else "🔴"
    return f"{icon} {value:+.2f}$"


def open_trades(trades: Iterable[TrackedTrade]) -> list[TrackedTrade]:
    return [t for t in trades if t.status in OPEN_STATUSES]


def closed_trades(trades: Iterable[TrackedTrade]) -> list[TrackedTrade]:
    return [t for t in trades if t.status in CLOSED_STATUSES]


def trade_effective_pnl(t: TrackedTrade) -> float:
    if t.status in CLOSED_STATUSES:
        return float(t.realized_pnl_pct or 0.0)
    if t.tp2_hit:
        return float(t.realized_pnl_pct or 0.0) + float(t.runner_pnl_pct or 0.0)
    if t.tp1_hit:
        return float(t.realized_pnl_pct or 0.0) + max(0.0, float(t.pnl_pct or 0.0)) * 0.20
    return float(t.pnl_pct or 0.0)


def trade_stage(t: TrackedTrade) -> str:
    return getattr(t, "stage_label", "OPEN") or "OPEN"


def clean_setup(t: TrackedTrade) -> str:
    raw = (t.setup_type or "unknown").replace("_", " ").strip().title()
    tags = [str(x).replace("_", " ").strip().title() for x in (t.execution_setup_tags or []) if x]
    if tags:
        return f"{raw} | {', '.join(tags[:2])}"
    return raw


def tradingview_url(symbol: str) -> str:
    tv = symbol.replace("-USDT-SWAP", "USDT").replace("-", "")
    return f"https://www.tradingview.com/chart/?symbol=OKX:{tv}"


def trade_card_lines(t: TrackedTrade, *, exposure_label: bool = True) -> list[str]:
    pnl = trade_effective_pnl(t)
    pnl_label = f"{pnl:+.2f}% Exposure" if exposure_label else f"{pnl:+.2f}%"
    extra = []
    if getattr(t, "protected_on_block", False):
        extra.append("🛡 Protected")
    if getattr(t, "trailing_tightened", False):
        extra.append("🔧 Tightened")
    extra_text = f" | {' | '.join(extra)}" if extra else ""
    return [
        f"• <b>{t.symbol}</b> | {pnl_label}",
        f"⏱️ {trade_stage(t)} | ⭐ {float(t.score or 0):.2f}{extra_text}",
        f"🎯 TP1: {float(t.tp1 or 0):.6f} | 🏁 TP2: {float(t.tp2 or 0):.6f}",
        f"🛡 SL: {float(t.sl or 0):.6f}",
        f"🧠 {clean_setup(t)}",
        f"🔗 TradingView: {tradingview_url(t.symbol)}",
    ]


def append_trade_cards(lines: list[str], title: str, items: list[TrackedTrade], limit: int = 3) -> None:
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


def behavior_summary_lines(trades: list[TrackedTrade], *, label: str = "Behavior Summary") -> list[str]:
    total = max(1, len(trades))
    winners = [t for t in trades if trade_effective_pnl(t) > 0]
    losers = [t for t in trades if trade_effective_pnl(t) < 0]
    tp1_count = sum(1 for t in trades if t.tp1_hit)
    tp2_count = sum(1 for t in trades if t.tp2_hit)
    trailing = sum(1 for t in trades if t.status == "trailing_hit")
    breakeven = sum(1 for t in trades if t.status == "breakeven_after_tp1")
    direct_sl = sum(1 for t in trades if t.status == "closed_loss")
    avg_winner = sum(trade_effective_pnl(t) for t in winners) / max(1, len(winners))
    avg_loser = sum(trade_effective_pnl(t) for t in losers) / max(1, len(losers))
    tp1_rate = tp1_count / total * 100.0
    tp2_rate = tp2_count / total * 100.0
    tp1_to_tp2 = tp2_count / max(1, tp1_count) * 100.0 if tp1_count else 0.0
    floating = sum(t.pnl_pct for t in open_trades(trades))
    quality = "إيجابي ✔️" if (avg_winner + avg_loser) >= 0 else "يحتاج مراجعة ⚠️"
    return [
        f"🧠 <b>{label}</b>",
        "📦 Model: 40/40/20",
        f"📈 Avg Winner: {avg_winner:+.2f}%",
        f"📉 Avg Loser: {avg_loser:+.2f}%",
        f"🎯 TP1 Rate: {tp1_rate:.1f}% | 🏁 TP2 Rate: {tp2_rate:.1f}%",
        f"🔁 TP1 → TP2: {tp1_to_tp2:.1f}%",
        f"🔄 Trailing Exit: {trailing / total * 100:.1f}%",
        f"🔒 Breakeven Exit: {breakeven / total * 100:.1f}%",
        f"🛑 Direct SL: {direct_sl / total * 100:.1f}%",
        f"⚡ Avg Floating Profit: {floating:+.2f}% Exposure",
        f"💡 Risk / Reward Quality: {quality}",
    ]


def wallet_impact_lines(trades: list[TrackedTrade], *, starting_balance: float = DEFAULT_STARTING_BALANCE, title: str = "Wallet Impact") -> list[str]:
    opened = open_trades(trades)
    closed = closed_trades(trades)
    closed_profit = sum(max(0.0, trade_effective_pnl(t)) for t in closed)
    closed_loss = sum(min(0.0, trade_effective_pnl(t)) for t in closed)
    floating_profit = sum(max(0.0, trade_effective_pnl(t)) for t in opened)
    floating_loss = sum(min(0.0, trade_effective_pnl(t)) for t in opened)
    closed_net = closed_profit + closed_loss
    floating_net = floating_profit + floating_loss
    total = closed_net + floating_net
    return [
        f"💰 <b>{title}</b>",
        f"📌 رأس المال: {starting_balance:.0f}$",
        "",
        "✅ <b>الصفقات المغلقة</b>",
        "📈 الأرباح",
        f"{money_from_exposure_pct(closed_profit):+.2f}$ | {closed_profit:+.2f}% Exposure",
        "📉 الخسائر",
        f"{money_from_exposure_pct(closed_loss):+.2f}$ | {closed_loss:+.2f}% Exposure",
        "⚖️ الصافي",
        f"<b>{money_line(money_from_exposure_pct(closed_net))} | {closed_net:+.2f}% Exposure</b>",
        "",
        "🔄 <b>الصفقات المفتوحة</b>",
        "📈 الأرباح العائمة",
        f"{money_from_exposure_pct(floating_profit):+.2f}$ | {floating_profit:+.2f}% Exposure",
        "📉 الخسائر العائمة",
        f"{money_from_exposure_pct(floating_loss):+.2f}$ | {floating_loss:+.2f}% Exposure",
        "⚖️ الصافي العائم",
        f"<b>{money_line(money_from_exposure_pct(floating_net))} | {floating_net:+.2f}% Exposure</b>",
        "",
        "💼 <b>التأثير الحالي على المحفظة</b>",
        f"<b>{money_line(money_from_exposure_pct(total))}</b>",
    ]


def quick_stats_lines(trades: list[TrackedTrade], *, label: str = "Quick Stats", item_name: str = "Trades") -> list[str]:
    opened = open_trades(trades)
    closed = closed_trades(trades)
    winners = [t for t in trades if trade_effective_pnl(t) > 0]
    losers = [t for t in trades if trade_effective_pnl(t) < 0]
    win_rate = len(winners) / max(1, len(winners) + len(losers)) * 100.0 if (winners or losers) else 0.0
    return [
        f"📊 <b>{label}</b>",
        f"• {item_name}: {len(trades)}",
        f"• Open: {len(opened)}",
        f"• Closed: {len(closed)}",
        f"🏆 Win Rate: <b>{win_rate:.1f}%</b>",
        f"🟢 Winners: {len(winners)} | 🔴 Losers: {len(losers)}",
    ]


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


def counter_lines(title: str, counter: Counter, limit: int = 5, suffix: str = "") -> list[str]:
    if not counter:
        return []
    lines = [title]
    for name, count in counter.most_common(limit):
        lines.append(f"• {name} — {count}{suffix}")
    return lines
