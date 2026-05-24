from __future__ import annotations

from analysis.models import SignalCandidate
from utils.constants import (
    DEFAULT_LEVERAGE,
    MODE_BLOCK_LONGS,
    MODE_COLOR_EMOJI,
    MODE_NORMAL_LONG,
    MODE_RECOVERY_LONG,
    MODE_STRONG_LONG_ONLY,
)

LIGHT_LINE = "┄┄┄┄┄┄┄┄"
EXEC_LINE = "════════════"


def _fmt_price(value: float | int | None) -> str:
    if value is None:
        return "-"
    try:
        value = float(value)
    except Exception:
        return str(value)
    if value >= 100:
        return f"{value:.2f}"
    if value >= 1:
        return f"{value:.4f}"
    return f"{value:.6f}"


def _clean_name(value: str | None) -> str:
    if not value:
        return "-"
    text = str(value).replace("_", " ").replace("|", " / ").strip()
    return " ".join(part.capitalize() if part.islower() else part for part in text.split())


def _mode_theme(mode: str) -> str:
    emoji = MODE_COLOR_EMOJI.get(mode, "⚪")
    names = {
        MODE_NORMAL_LONG: "Normal",
        MODE_STRONG_LONG_ONLY: "Strong",
        MODE_BLOCK_LONGS: "Block",
        MODE_RECOVERY_LONG: "Recovery",
    }
    return f"{emoji} {names.get(mode, mode)}"


def _execution_header(mode: str) -> str:
    emoji = MODE_COLOR_EMOJI.get(mode, "⚪")
    if mode == MODE_STRONG_LONG_ONLY:
        return f"🔥{emoji} STRONG EXECUTION"
    if mode == MODE_RECOVERY_LONG:
        return f"🔥{emoji} RECOVERY EXECUTION"
    if mode == MODE_BLOCK_LONGS:
        return f"🔥{emoji} BLOCK EXCEPTION"
    return f"🔥{emoji} EXECUTION"


def _normal_header(mode: str) -> str:
    # Calm normal alert. Mode color appears in the Market section, not in the header.
    return "📈 LONG SIGNAL"


def _execution_path(signal: SignalCandidate, execution_result: dict | None) -> str:
    if execution_result and execution_result.get("path"):
        return str(execution_result.get("path"))
    tags = signal.execution_setup_tags or []
    if any("recovery" in str(t).lower() for t in tags):
        return "recovery"
    if any("block" in str(t).lower() for t in tags):
        return "block_exception"
    if any("elite" in str(t).lower() for t in tags):
        return "elite"
    if tags:
        return "whitelist"
    return "normal_check"


def _managed_split_for_path(path: str) -> tuple[int, int, int]:
    if str(path or "").lower() == "recovery":
        return 50, 25, 25
    return 40, 40, 20


def _tradingview_symbol(symbol: str) -> str:
    """Convert OKX instrument id to a TradingView-friendly symbol.

    OKX Perpetual Futures format: OKX:BTCUSDT.P
    """
    raw = str(symbol or "").upper()
    if raw.endswith("-USDT-SWAP"):
        base = raw.replace("-USDT-SWAP", "USDT")
        return f"OKX:{base}.P"
    compact = raw.replace("-", "")
    return f"OKX:{compact}"


def build_tradingview_url(symbol: str) -> str:
    """بيبني الـ URL الكامل — نفس الـ URL المستخدم في الـ button."""
    return f"https://www.tradingview.com/chart/?symbol={_tradingview_symbol(symbol)}"


def build_tradingview_html_link(symbol: str) -> str:
    """HTML clickable link — يُستخدم في الـ track messages مع parse_mode=HTML.

    ✅ نفس format الـ button بالظبط — clickable وبيفتح TradingView.
    """
    tv_symbol = _tradingview_symbol(symbol)
    url = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
    return f'<a href="{url}">🔗 TradingView — {tv_symbol}</a>'


def build_tradingview_plain_link(symbol: str) -> str:
    """Plain text link للـ reports اللي مش HTML — يظهر كـ URL قابل للنقر في Telegram.

    ✅ Telegram بيعمل auto-link للـ URLs تلقائياً.
    """
    tv_symbol = _tradingview_symbol(symbol)
    url = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
    return f"🔗 TradingView ({tv_symbol})\n{url}"


def build_signal_buttons(signal: SignalCandidate) -> dict:
    """Unified inline buttons under every trade message.

    Track is a callback button like the old bot style.
    TradingView is a URL button with a unified link format.
    """
    return {
        "inline_keyboard": [[
            {"text": "📊 Track", "callback_data": f"track:{signal.symbol}"[:64]},
            {"text": "🔗 TradingView", "url": build_tradingview_url(signal.symbol)},
        ]]
    }


def _slot_status_snapshot(trade) -> tuple[bool, bool, bool]:
    counted_open = bool(getattr(trade, "counts_as_active_slot", False))
    daily_open = bool(getattr(trade, "counts_as_daily_open_risk", False))
    same_symbol_blocks = bool(getattr(trade, "blocks_same_symbol_reentry", counted_open))
    return counted_open, daily_open, same_symbol_blocks


def _preview_status_label(status: str) -> str:
    status = str(status or "").lower()
    if status == "pending_pullback_preview":
        return "🟡 Status: PULLBACK WAITING"
    return "🟢 Status: OPEN / ACTIVE"


def _profit_state(total_usd: float, protected: bool = False) -> str:
    if total_usd > 0.01:
        return "🟢 رابحة"
    if total_usd < -0.01:
        return "🔴 خاسرة"
    return "⚪ تعادل / Protected" if protected else "⚪ تعادل"


def _leveraged_pct(raw_pct: float) -> float:
    return float(raw_pct or 0.0) * float(DEFAULT_LEVERAGE or 1)


def _money_from_pct(pct: float, margin: float = 35.0) -> float:
    """USD impact from a displayed leveraged performance percentage."""
    return (float(pct or 0.0) / 100.0) * margin


def _trade_raw_effective_pnl_pct(trade) -> float:
    if getattr(trade, "tp2_hit", False):
        return float(getattr(trade, "realized_pnl_pct", 0.0) or 0.0) + float(getattr(trade, "runner_pnl_pct", 0.0) or 0.0)
    if getattr(trade, "tp1_hit", False):
        remaining_pct = max(0.0, 100.0 - float(getattr(trade, "tp1_close_pct", 40.0) or 40.0))
        return float(getattr(trade, "realized_pnl_pct", 0.0) or 0.0) + max(0.0, float(getattr(trade, "pnl_pct", 0.0) or 0.0)) * (remaining_pct / 100.0)
    return float(getattr(trade, "pnl_pct", 0.0) or 0.0)


def _trade_effective_pnl_pct(trade) -> float:
    return _leveraged_pct(_trade_raw_effective_pnl_pct(trade))


def _managed_plan_from_sources(signal: SignalCandidate, execution_result: dict | None = None, trade=None, order_result: dict | None = None) -> dict:
    execution_result = execution_result or {}
    order_result = order_result or {}

    plan = {}
    if isinstance(execution_result.get("managed_trade_plan"), dict):
        plan = dict(execution_result.get("managed_trade_plan") or {})
    elif trade is not None and isinstance(getattr(trade, "managed_trade_plan", None), dict):
        plan = dict(getattr(trade, "managed_trade_plan") or {})
    elif isinstance(order_result.get("managed_trade_plan"), dict):
        plan = dict(order_result.get("managed_trade_plan") or {})

    entry = plan.get("entry", signal.entry)
    sl = plan.get("sl", signal.sl)
    tp1 = plan.get("tp1", signal.tp1)
    tp2 = plan.get("tp2", signal.tp2)

    path = (
        plan.get("path")
        or execution_result.get("path")
        or getattr(trade, "execution_path", "")
        or _execution_path(signal, execution_result)
    )

    tp1_pct, tp2_pct, runner_pct = _managed_split_for_path(path)

    partials = plan.get("partials") or []
    if isinstance(partials, list):
        for partial in partials:
            name = str((partial or {}).get("name") or "").lower()
            close_pct = int(float((partial or {}).get("close_pct") or 0))
            if name == "tp1" and close_pct > 0:
                tp1_pct = close_pct
            elif name == "tp2" and close_pct > 0:
                tp2_pct = close_pct
            elif name == "runner" and close_pct > 0:
                runner_pct = close_pct

    entry_order_id = (
        order_result.get("entry_order_id")
        or order_result.get("order_id")
        or execution_result.get("entry_order_id")
        or getattr(trade, "entry_order_id", "")
    )
    tp1_order_id = (
        order_result.get("tp1_order_id")
        or execution_result.get("tp1_order_id")
        or getattr(trade, "tp1_order_id", "")
    )
    tp2_order_id = (
        order_result.get("tp2_order_id")
        or execution_result.get("tp2_order_id")
        or getattr(trade, "tp2_order_id", "")
    )

    sl_attached_on_entry = bool(
        order_result.get("sl_attached_on_entry")
        or execution_result.get("sl_attached_on_entry")
        or getattr(trade, "sl_attached_on_entry", False)
    )

    runner_requires_trailing = bool(
        order_result.get("runner_requires_trailing_after_tp2")
        or execution_result.get("runner_requires_trailing_after_tp2")
        or getattr(trade, "runner_requires_trailing_after_tp2", True)
    )

    return {
        "path": path,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp1_close_pct": tp1_pct,
        "tp2_close_pct": tp2_pct,
        "runner_close_pct": runner_pct,
        "entry_order_id": str(entry_order_id or ""),
        "tp1_order_id": str(tp1_order_id or ""),
        "tp2_order_id": str(tp2_order_id or ""),
        "sl_attached_on_entry": sl_attached_on_entry,
        "runner_requires_trailing_after_tp2": runner_requires_trailing,
    }


def build_execution_confirmation_message(
    signal: SignalCandidate,
    execution_result: dict | None = None,
    order_result: dict | None = None,
    trade=None,
) -> str:
    execution_result = execution_result or {}
    order_result = order_result or {}
    plan = _managed_plan_from_sources(signal, execution_result, trade=trade, order_result=order_result)

    exchange_reason = (
        order_result.get("reason")
        or execution_result.get("exchange_order_reason")
        or getattr(trade, "exchange_order_reason", "")
        or "accepted"
    )
    simulated = order_result.get("simulated")
    if simulated is None:
        simulated = execution_result.get("simulated")
    if simulated is None and trade is not None:
        simulated = getattr(trade, "simulated", None)

    lines = [
        "✅ OKX EXECUTION CONFIRMED",
        EXEC_LINE,
        f"💎 {signal.symbol}",
        f"🚀 Path: {_clean_name(plan.get('path'))}",
        f"📈 Mode: {_mode_theme(signal.market_mode)}",
        "",
        "📍 Entry Confirmed",
        f"• Entry: {_fmt_price(plan.get('entry'))}",
        f"• SL Attached On Entry: {'✅ Yes' if plan.get('sl_attached_on_entry') else '⚠️ No'}",
        f"• SL: {_fmt_price(plan.get('sl'))}",
        f"• Entry Order ID: {plan.get('entry_order_id') or '-'}",
        "",
        "🎯 Managed Exit Plan",
        f"• TP1: {_fmt_price(plan.get('tp1'))} | Close {int(plan.get('tp1_close_pct') or 40)}%",
        f"• TP2: {_fmt_price(plan.get('tp2'))} | Close {int(plan.get('tp2_close_pct') or 40)}%",
        f"• Runner: {int(plan.get('runner_close_pct') or 20)}%",
        f"• TP1 Order ID: {plan.get('tp1_order_id') or '-'}",
        f"• TP2 Order ID: {plan.get('tp2_order_id') or '-'}",
        f"• Runner After TP2: {'Trailing / protected' if plan.get('runner_requires_trailing_after_tp2') else 'Tracked only'}",
        "",
        "⚙️ Exchange",
        f"• Simulated: {simulated if simulated is not None else '-'}",
        f"• Result: {exchange_reason}",
        "",
        f"🔗 {build_tradingview_html_link(signal.symbol)}",
    ]
    return "\n".join(lines)


def build_execution_failure_message(
    signal: SignalCandidate,
    execution_result: dict | None = None,
    order_result: dict | None = None,
) -> str:
    execution_result = execution_result or {}
    order_result = order_result or {}
    exchange_reason = (
        order_result.get("reason")
        or order_result.get("error")
        or execution_result.get("exchange_order_reason")
        or "okx_execution_failed"
    )
    simulated = order_result.get("simulated")
    if simulated is None:
        simulated = execution_result.get("simulated")

    lines = [
        "⚠️ OKX EXECUTION FAILED",
        EXEC_LINE,
        f"💎 {signal.symbol}",
        f"🚀 Path: {_clean_name(_execution_path(signal, execution_result))}",
        "",
        "📍 Intended Plan",
        f"• Entry: {_fmt_price(signal.entry)}",
        f"• SL: {_fmt_price(signal.sl)}",
        f"• TP1: {_fmt_price(signal.tp1)}",
        f"• TP2: {_fmt_price(signal.tp2)}",
        "",
        "⚙️ Exchange",
        f"• Simulated: {simulated if simulated is not None else '-'}",
        f"• Result: {exchange_reason}",
        "",
        "📌 الصفقة لم تُفعّل كتريد مفتوح لأن تنفيذ OKX فشل.",
        "",
        f"🔗 {build_tradingview_html_link(signal.symbol)}",
    ]
    return "\n".join(lines)


def build_trade_track_message(trade) -> str:
    path = getattr(trade, "execution_path", "") or ("Execution" if getattr(trade, "execution_trade", False) else "Normal")
    mode = getattr(trade, "market_mode", "-")
    status = getattr(trade, "status", "open")
    protected = bool(getattr(trade, "protected_runner", False))
    title_status = "PROTECTED RUNNER" if protected else str(status).replace("_", " ").upper()
    total_pct = _trade_effective_pnl_pct(trade)
    total_usd = _money_from_pct(total_pct)
    locked_pct = _leveraged_pct(float(getattr(trade, "realized_pnl_pct", 0.0) or 0.0))
    locked_usd = _money_from_pct(locked_pct)
    floating_label = "Floating Runner" if getattr(trade, "tp2_hit", False) else "Floating PnL"
    raw_floating_pct = float(getattr(trade, "runner_pnl_pct", 0.0) or 0.0) if getattr(trade, "tp2_hit", False) else float(getattr(trade, "pnl_pct", 0.0) or 0.0)
    floating_pct = _leveraged_pct(raw_floating_pct)
    floating_usd = _money_from_pct(floating_pct)
    tp1_pct = float(getattr(trade, "tp1_close_pct", 40.0) or 40.0)
    tp2_pct = float(getattr(trade, "tp2_close_pct", 40.0) or 40.0)
    runner_pct = float(getattr(trade, "runner_close_pct", 20.0) or 20.0)
    symbol = getattr(trade, "symbol", "-")

    entry_price = float(getattr(trade, "entry", 0.0) or 0.0)
    current_price = float(getattr(trade, "current_price", 0.0) or entry_price)
    closed_at = getattr(trade, "closed_at", None)
    is_closed = bool(getattr(trade, "is_closed", False))
    counted_open, daily_open, same_symbol_blocks = _slot_status_snapshot(trade)

    price_change_pct = ((current_price - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
    price_change_icon = "🟢" if price_change_pct >= 0 else "🔴"
    price_change_str = f"{price_change_pct:+.2f}%"

    lines = [
        f"📊 Track — {symbol}",
        "━━━━━━━━━━━━",
        f"{'🔴 CLOSED' if is_closed else '🟢'} Status: {title_status}",
        f"🚀 Path: {_clean_name(path)}",
        f"📈 Mode: {mode}",
        f"⏱ TF: 15m | ⭐ Score: {float(getattr(trade, 'score', 0.0) or 0.0):.2f}",
        "",
        "📍 Position",
        f"• Entry Price: {_fmt_price(entry_price)}",
        f"• Current Price: {_fmt_price(current_price)} {price_change_icon} {price_change_str}",
        f"• Entry Type: {getattr(trade, 'entry_type', 'Market') if hasattr(trade, 'entry_type') else 'Market'}",
        *(
            [f"• Closed At: {closed_at.strftime('%H:%M %d/%m') if hasattr(closed_at, 'strftime') else str(closed_at)}"]
            if is_closed and closed_at else []
        ),
        "",
        f"💰 Current Result — {_profit_state(total_usd, protected)}",
        f"• Locked Profit: {locked_usd:+.2f}$",
        f"• {floating_label}: {floating_usd:+.2f}$",
        f"• Total Impact Now: {total_usd:+.2f}$",
        f"• 15x Performance: {total_pct:+.2f}% | {total_usd:+.2f}$",
        "",
        "🎯 Targets",
        f"• TP1: {_fmt_price(getattr(trade, 'tp1', 0.0))} | Close {tp1_pct:.0f}%",
        f"• TP2: {_fmt_price(getattr(trade, 'tp2', 0.0))} | Close {tp2_pct:.0f}%",
        f"• Runner: {runner_pct:.0f}%",
        f"• SL: {_fmt_price(getattr(trade, 'sl', 0.0))}",
        "",
        "📌 Stage",
        f"• TP1: {'✅ Hit' if getattr(trade, 'tp1_hit', False) else '⏳ Waiting'}",
        f"• TP2: {'✅ Hit' if getattr(trade, 'tp2_hit', False) else '⏳ Waiting'}",
        f"• Runner: {'🏃 Active' if getattr(trade, 'runner_active', False) else 'Not Active'}",
        f"• SL Moved: {'✅ Entry / Better' if getattr(trade, 'sl_moved_to_entry', False) else 'No'}",
        f"• Protected: {'✅ Yes' if protected else 'No'}",
        "",
        "🛡 Slot Status",
        f"• Counted Open Slot: {'Yes' if counted_open else 'No'}",
        f"• Daily Open Risk: {'Counted' if daily_open else 'Exempt'}",
        f"• Same Symbol Block: {'Yes' if same_symbol_blocks else 'No'}",
        f"• Protected Runner: {'Yes' if protected else 'No'}",
    ]

    entry_order_id = getattr(trade, "entry_order_id", "")
    tp1_order_id = getattr(trade, "tp1_order_id", "")
    tp2_order_id = getattr(trade, "tp2_order_id", "")
    sl_attached = bool(getattr(trade, "sl_attached_on_entry", False))
    exchange_state = getattr(trade, "exchange_sync_state", "")
    if entry_order_id or tp1_order_id or tp2_order_id or sl_attached or exchange_state:
        lines.extend([
            "",
            "🏦 Exchange State",
            f"• Entry Order ID: {entry_order_id or '-'}",
            f"• TP1 Order ID: {tp1_order_id or '-'}",
            f"• TP2 Order ID: {tp2_order_id or '-'}",
            f"• SL Attached On Entry: {'Yes' if sl_attached else 'No'}",
            f"• Sync State: {exchange_state or '-'}",
        ])

    lines.extend([
        "",
        "🧠 Setup",
        f"• {_clean_name(getattr(trade, 'setup_type', '-'))}",
        f"• Quality: {'PASS' if getattr(trade, 'execution_trade', False) else 'Normal Tracking'}",
        "",
        f"🔗 {build_tradingview_html_link(symbol)}",
    ])
    return "\n".join(lines)


def build_rejected_track_message(signal: SignalCandidate, execution_result: dict | None = None) -> str:
    execution_result = execution_result or {}
    reason = execution_result.get("reason") or "unknown"
    status = execution_result.get("status") or "candidate_only"
    gate = execution_result.get("gate") or {}
    return "\n".join([
        f"📊 Track — {signal.symbol}",
        "━━━━━━━━━━━━",
        "⚪ Status: EXECUTION CHECKED",
        "📍 Signal: Normal Signal",
        f"🚀 Execution: {str(status).replace('_', ' ').title()}",
        "",
        LIGHT_LINE,
        "❌ Rejection",
        f"• Reason: {reason}",
        f"• Category: {_clean_name(status)}",
        f"• Score: {signal.score:.2f}",
        f"• Required: {gate.get('min_score', 'by gate')}",
        f"• Setup: {_clean_name(signal.setup_type)}",
        "",
        "📌 ملاحظة: الصفقة محفوظة للتحليل ولا تُحسب كصفقة مفتوحة.",
        "",
        f"🔗 {build_tradingview_html_link(signal.symbol)}",
    ])


def build_track_message(signal: SignalCandidate, execution_result: dict | None = None, trade=None) -> str:
    if trade is not None:
        return build_trade_track_message(trade)
    status = (execution_result or {}).get("status") or "normal_signal_only"
    if status not in {"accepted_preview", "pending_pullback_preview", "executed", "open", "tp1", "tp2", "trailing"}:
        return build_rejected_track_message(signal, execution_result)

    reason = (execution_result or {}).get("reason") or "-"
    setup_clean = _clean_name(signal.setup_type)
    entry_label = "Market" if signal.entry_timing == "market" else "Pullback"
    path = _execution_path(signal, execution_result)
    tp1_pct, tp2_pct, runner_pct = _managed_split_for_path(path)
    entry_price = float(signal.entry or 0.0)

    current_price_line = (
        f"• Current Price: {_fmt_price(entry_price)} (waiting pullback trigger)"
        if status == "pending_pullback_preview"
        else f"• Current Price: {_fmt_price(entry_price)} (just opened)"
    )

    quality_label = "PREVIEW / WAITING PULLBACK" if status == "pending_pullback_preview" else "PASS"

    lines = [
        f"📊 Track — {signal.symbol}",
        "━━━━━━━━━━━━",
        _preview_status_label(status),
        f"🚀 Path: {_clean_name(path)}",
        f"📈 Mode: {signal.market_mode}",
        f"⏱ TF: 15m | ⭐ Score: {signal.score:.2f}",
        "",
        "📍 Position",
        f"• Entry Price: {_fmt_price(entry_price)}",
        current_price_line,
        f"• Entry Type: {entry_label}",
        "",
        f"💰 Current Result — ⚪ تعادل",
        "• Locked Profit: +0.00$",
        "• Floating PnL: +0.00$",
        "• Total Impact Now: +0.00$",
        "• 15x Performance: +0.00% | +0.00$",
        "",
        "🎯 Targets",
        f"• TP1: {_fmt_price(signal.tp1)} | Close {tp1_pct}%",
        f"• TP2: {_fmt_price(signal.tp2)} | Close {tp2_pct}%",
        f"• Runner: {runner_pct}%",
        f"• SL: {_fmt_price(signal.sl)}",
        "",
        "📌 Stage",
        "• TP1: ⏳ Waiting",
        "• TP2: ⏳ Waiting",
        "• Runner: Not Active",
        "• SL Moved: No",
        "• Protected: No",
        "",
        "🧠 Setup",
        f"• {setup_clean}",
        f"• Quality: {quality_label}",
        f"• Execution: {status}",
        f"• Reason: {reason}",
        "",
        f"🔗 {build_tradingview_html_link(signal.symbol)}",
    ]

    entry_order_id = (execution_result or {}).get("entry_order_id")
    if entry_order_id:
        lines.extend([
            "",
            "🏦 Exchange State",
            f"• Entry Order ID: {entry_order_id}",
            f"• TP1 Order ID: {(execution_result or {}).get('tp1_order_id') or '-'}",
            f"• TP2 Order ID: {(execution_result or {}).get('tp2_order_id') or '-'}",
            f"• SL Attached On Entry: {'Yes' if (execution_result or {}).get('sl_attached_on_entry') else 'No'}",
        ])

    return "\n".join(lines)



def _format_pa_line_from_signal(signal: SignalCandidate) -> str:
    """Compact PA evidence line for execution messages only.

    Display-only. Does not affect score, modes, Nour, or execution.
    """
    try:
        evidence = (getattr(signal, "meta", {}) or {}).get("smart_evidence") or {}
    except Exception:
        evidence = {}

    if not isinstance(evidence, dict) or not evidence.get("available"):
        return ""

    parts: list[str] = []

    if evidence.get("displacement_hint"):
        parts.append("Expansion✅")

    if evidence.get("auction_acceptance_hint"):
        parts.append("Acceptance✅")

    if evidence.get("failed_breakout_risk"):
        parts.append("WeakBreakout⚠️")

    if evidence.get("compression_release_hint"):
        parts.append("Compression✅")

    if evidence.get("sweep_reclaim_hint"):
        parts.append("Sweep✅")

    if not parts:
        return ""

    return "🧠 PA: " + " | ".join(parts)


def build_signal_message(signal: SignalCandidate, execution_result: dict | None = None) -> str:
    """Build Telegram signal message.

    IMPORTANT:
    - Normal signal layout is preserved exactly.
    - Only execution candidate layout is adjusted.
    - Track/TradingView buttons are still attached by main.py via build_signal_buttons().
    """
    mode_emoji = MODE_COLOR_EMOJI.get(signal.market_mode, "⚪")
    mode_theme = _mode_theme(signal.market_mode)
    status = (execution_result or {}).get("status")
    reason = (execution_result or {}).get("reason")
    is_execution = status in {"accepted_preview", "pending_pullback_preview"}

    entry_label = "Market Entry" if signal.entry_timing == "market" else "Pullback Entry"
    setup_clean = _clean_name(signal.setup_type)
    tags_clean = " | ".join(_clean_name(t) for t in (signal.execution_setup_tags or [])[:4]) or setup_clean
    is_pullback_preview = status == "pending_pullback_preview"
    path = _execution_path(signal, execution_result)
    tp1_pct, tp2_pct, runner_pct = _managed_split_for_path(path)

    if is_execution:
        lines = [
            _execution_header(signal.market_mode),
            EXEC_LINE,
            "🔥 مرشحة للتنفيذ التجريبي",
            "",
            f"<b>🚦 {signal.symbol} 🚦</b>",
            f"⭐ Score: {signal.score:.2f} | TF: 15m",
            "",
            f"📍 {entry_label}",
            f"• Price: {_fmt_price(signal.entry)}",
            f"🎯 TP1: {_fmt_price(signal.tp1)} | Close {tp1_pct}%",
            f"🏁 TP2: {_fmt_price(signal.tp2)} | Close {tp2_pct}%",
            f"🏃 Runner: {runner_pct}% after TP2",
            f"🛡 SL: {_fmt_price(signal.sl)}",
            "",
            "🟢 Quality Filters: PASS | ⚡ Preview Ready" if not is_pullback_preview else "🟢 Quality Filters: PASS | ⏳ Waiting Pullback",
        ]

        pa_line = _format_pa_line_from_signal(signal)
        if pa_line:
            lines.append(pa_line)

        lines.extend([
            "",
            "┌─ 🏷 Tag Badge ─┐",
            f"Setup: {setup_clean}",
            f"Path: {path}",
            f"Context: {tags_clean}",
            "└──────────────┘",
            "",
            "📊 Trade Details",
            f"Setup: {setup_clean}",
            f"Entry Timing: {signal.entry_timing}",
            f"Current Wave: {signal.meta.get('wave', 'n/a')}",
            f"Volume State: {signal.meta.get('volume_state', 'n/a')}",
            f"1H Confirmation: {signal.meta.get('htf_confirmation', 'n/a')}",
            "",
            "🌐 Market",
            f"Mode: {mode_emoji} {signal.market_mode}",
            f"Theme: {mode_theme}",
            "",
            "⚙️ Execution",
            f"Status: {status}",
            "🟡 Pending pullback does NOT place a market order yet" if is_pullback_preview else "• Awaiting OKX confirmation",
            "📌 Separate confirmation message will be sent after actual OKX execution" if not is_pullback_preview else "📌 Preview only — waiting pullback trigger before any execution",
        ])

        if reason:
            lines.append(f"• Reason: {_clean_name(str(reason))}")

        if signal.warnings:
            lines.extend(["", "⚠️ Notes", *[f"• {w}" for w in signal.warnings[:3]]])

        slots = (execution_result or {}).get("slots")
        if slots:
            lines.extend([
                "",
                f"📊 Slots: {slots.get('counted')} / {slots.get('allowed')} Open | {slots.get('remaining')} Remaining",
            ])

        return "\n".join(lines)

    # NORMAL BRANCH — restored/preserved exactly like the original style.
    lines = [
        _normal_header(signal.market_mode),
        LIGHT_LINE,
        "📍 إشارة عادية — التنفيذ مسار منفصل",
        "",
        f"💎 {signal.symbol}",
        f"⭐ Score: {signal.score:.2f} | TF: 15m",
        "",
        f"📍 {entry_label}",
        f"• Price: {_fmt_price(signal.entry)}",
        f"🎯 TP1: {_fmt_price(signal.tp1)}",
        f"🏁 TP2: {_fmt_price(signal.tp2)}",
        "🏃 Runner: 20% after TP2",
        f"🛡 SL: {_fmt_price(signal.sl)}",
        "",
        "┌─ 🏷 Tag Badge ─┐",
        f"Setup: {setup_clean}",
        f"Context: {tags_clean}",
        "└──────────────┘",
        "",
        "📊 Trade Details",
        f"Setup: {setup_clean}",
        f"Entry Timing: {signal.entry_timing}",
        f"Current Wave: {signal.meta.get('wave', 'n/a')}",
        f"Volume State: {signal.meta.get('volume_state', 'n/a')}",
        f"1H Confirmation: {signal.meta.get('htf_confirmation', 'n/a')}",
        "",
        "🌐 Market",
        f"Mode: {mode_emoji} {signal.market_mode}",
        f"Theme: {mode_theme}",
    ]

    if signal.warnings:
        lines.extend(["", "⚠️ Notes", *[f"• {w}" for w in signal.warnings[:3]]])

    if execution_result:
        lines.extend([
            "",
            "⚙️ Execution Check",
            f"Status: {status or 'not_candidate'}",
            f"Reason: {reason or 'normal_signal_only'}",
        ])

    return "\n".join(lines)

