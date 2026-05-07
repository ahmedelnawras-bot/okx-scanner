import time
import logging
import html

from execution.config import (
    TRADING_MODE,
    LIVE_EXECUTION_SETUP_WHITELIST_ENABLED,
    LIVE_EXECUTION_SETUP_WHITELIST,
    LIVE_EXECUTION_SETUP_KEYWORDS,
)
from execution.risk_manager import can_execute_trade
from execution.order_builder import build_order_preview
from execution.execution_state import (
    set_active_trade,
    register_order,
)

logger = logging.getLogger("okx-scanner")


EXECUTION_WHITELIST_KEYWORDS = (
    "vwap_reclaim",
    "retest_breakout_confirmed",
    "wave_3",
    "relative_strength_vs_btc",
)


def _candidate_blob(candidate: dict) -> str:
    diagnostics = candidate.get("diagnostics", {}) or {}
    values = [
        candidate.get("setup_type"),
        diagnostics.get("setup_type"),
        candidate.get("setup_type_base"),
        diagnostics.get("setup_type_base"),
        candidate.get("primary_extra_setup"),
        diagnostics.get("primary_extra_setup"),
    ]
    for key in ("extra_setup_names", "context_setups"):
        raw = candidate.get(key, diagnostics.get(key, []))
        if isinstance(raw, (list, tuple, set)):
            values.extend(list(raw))
        elif raw:
            values.append(raw)
    return "|".join(str(v) for v in values if v).lower()


def is_setup_allowed_for_execution(candidate: dict) -> dict:
    """Final execution whitelist defense. main.py should send only these candidates."""
    setup_type = str(candidate.get("setup_type", "") or "").strip()
    mode = str(candidate.get("current_mode") or candidate.get("market_mode") or "").upper()
    block_exception = bool(candidate.get("block_exception")) or mode == "BLOCK_LONGS"
    blob = _candidate_blob(candidate)
    allowed = block_exception or any(k in blob for k in EXECUTION_WHITELIST_KEYWORDS)
    return {
        "allowed": allowed,
        "reason": "execution_candidate_allowed" if allowed else "setup_not_whitelisted",
        "setup_type": setup_type,
    }


def _fmt(value) -> str:
    return html.escape(str(value if value is not None else ""))


def build_execution_preview_message(order: dict, setup_type: str, order_id: str) -> str:
    tp_plan = order.get("tp_plan", {}) or {}

    return "\n".join([
        "🧪 <b>تم قبول التنفيذ التجريبي</b>",
        "",
        f"🪙 <b>العملة:</b> {_fmt(order.get('symbol'))}",
        f"🧠 <b>Setup:</b> {_fmt(setup_type or '')}",
        f"🧭 <b>Mode:</b> {_fmt(TRADING_MODE)}",
        f"🆔 <b>Order ID:</b> {_fmt(order_id)}",
        "",
        f"⚙️ <b>Leverage:</b> {_fmt(order.get('leverage'))}x",
        f"📍 <b>Entry:</b> {_fmt(order.get('entry'))}",
        f"🛑 <b>SL:</b> {_fmt(order.get('sl'))}",
        f"🎯 <b>TP1:</b> {_fmt(order.get('tp1'))} | إغلاق {_fmt(tp_plan.get('tp1_close_pct'))}%",
        f"🎯 <b>TP2:</b> {_fmt(order.get('tp2'))} | إغلاق {_fmt(tp_plan.get('tp2_close_pct'))}%",
        f"🏃 <b>Trailing:</b> {_fmt(tp_plan.get('trailing_position_pct'))}% | {_fmt(tp_plan.get('trailing_pct'))}%",
        "",
        "🔒 <b>بعد TP1:</b> SL → Entry",
        "",
        "⚠️ <b>Simulation فقط - لم يتم إرسال أمر حقيقي إلى OKX</b>",
    ])


def build_pending_pullback_message(order: dict, setup_type: str, order_id: str) -> str:
    tp_plan = order.get("tp_plan", {}) or {}

    return "\n".join([
        "📌 <b>تم تسجيل تنفيذ معلق - انتظار Pullback</b>",
        "",
        f"🪙 <b>العملة:</b> {_fmt(order.get('symbol'))}",
        f"🧠 <b>Setup:</b> {_fmt(setup_type or '')}",
        f"🧭 <b>Mode:</b> {_fmt(TRADING_MODE)}",
        f"🆔 <b>Order ID:</b> {_fmt(order_id)}",
        "",
        f"📍 <b>Market Entry الحالي:</b> {_fmt(order.get('market_entry'))}",
        f"🎯 <b>منطقة Pullback:</b> {_fmt(order.get('pullback_low'))} → {_fmt(order.get('pullback_high'))}",
        f"📌 <b>دخول التنفيذ المخطط:</b> {_fmt(order.get('execution_entry'))}",
        "",
        f"🛑 <b>SL:</b> {_fmt(order.get('sl'))}",
        f"🎯 <b>TP1:</b> {_fmt(order.get('tp1'))} | إغلاق {_fmt(tp_plan.get('tp1_close_pct'))}%",
        f"🎯 <b>TP2:</b> {_fmt(order.get('tp2'))} | إغلاق {_fmt(tp_plan.get('tp2_close_pct'))}%",
        f"🏃 <b>Trailing:</b> {_fmt(tp_plan.get('trailing_position_pct'))}% | {_fmt(tp_plan.get('trailing_pct'))}%",
        "",
        "⏳ <b>لن يتم تنفيذ Market الآن.</b>",
        "✅ سيتم انتظار السعر داخل منطقة Pullback.",
        "",
        "⚠️ <b>Simulation فقط - لم يتم إرسال أمر حقيقي إلى OKX</b>",
    ])


def process_trade_candidate(redis_client, symbol: str, candidate: dict) -> dict:
    """
    المعالج الرئيسي للتنفيذ.
    حالياً preview فقط، ولا يرسل أي أوامر حقيقية إلى OKX.

    مهم:
    - لو الإشارة Market → accepted_preview
    - لو الإشارة Pullback → pending_pullback_preview
    """

    # 1) Setup whitelist gate
    setup_decision = is_setup_allowed_for_execution(candidate)

    if not setup_decision.get("allowed"):
        logger.info(
            f"⏭ Execution blocked | Setup not allowed | "
            f"{symbol} | {setup_decision.get('setup_type')}"
        )
        return {
            "status": "skipped",
            "reason": setup_decision.get("reason"),
            "setup_type": setup_decision.get("setup_type"),
        }

    # 2) Risk check
    decision = can_execute_trade(redis_client, symbol, candidate)

    if not decision.get("allowed"):
        return {
            "status": "rejected",
            "reason": decision.get("reason"),
            "setup_type": setup_decision.get("setup_type"),
        }

    # 3) Build order preview
    order = build_order_preview(symbol, candidate)

    # 4) Generate fake order id
    order_id = f"sim_{symbol}_{int(time.time())}"

    is_pullback = (
        order.get("entry_mode") == "pullback_pending"
        or order.get("status") == "pending_pullback_preview"
        or bool(order.get("has_pullback_plan"))
    )

    result_status = "pending_pullback_preview" if is_pullback else "accepted_preview"

    # 5) Save execution state
    state_payload = {
        "symbol": symbol,
        "order_id": order_id,
        "mode": TRADING_MODE,
        "setup_type": setup_decision.get("setup_type"),
        "status": result_status,
        "entry_mode": order.get("entry_mode", "market"),
        "entry": order.get("entry"),
        "execution_entry": order.get("execution_entry"),
        "market_entry": order.get("market_entry"),
        "pullback_low": order.get("pullback_low"),
        "pullback_high": order.get("pullback_high"),
        "pullback_entry": order.get("pullback_entry"),
        "sl": order.get("sl"),
        "tp1": order.get("tp1"),
        "tp2": order.get("tp2"),
        "created_ts": int(time.time()),
    }

    set_active_trade(
        redis_client,
        symbol,
        state_payload,
    )

    register_order(
        redis_client,
        order_id,
        {
            "symbol": symbol,
            "status": result_status,
            "setup_type": setup_decision.get("setup_type"),
            "entry_mode": order.get("entry_mode", "market"),
            "entry": order.get("entry"),
            "execution_entry": order.get("execution_entry"),
            "pullback_low": order.get("pullback_low"),
            "pullback_high": order.get("pullback_high"),
        },
    )

    if is_pullback:
        execution_message = build_pending_pullback_message(
            order,
            setup_decision.get("setup_type"),
            order_id,
        )
    else:
        execution_message = build_execution_preview_message(
            order,
            setup_decision.get("setup_type"),
            order_id,
        )

    return {
        "status": result_status,
        "order_id": order_id,
        "order": order,
        "setup_type": setup_decision.get("setup_type"),
        "entry_mode": order.get("entry_mode", "market"),
        "execution_message": execution_message,
    }
