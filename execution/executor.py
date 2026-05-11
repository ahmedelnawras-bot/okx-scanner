import time
import logging
import html

from execution.config import (
    TRADING_MODE,
    EXECUTION_SETUP_WHITELIST,
)
from execution.risk_manager import can_execute_trade
from execution.order_builder import build_order_preview
from execution.execution_state import (
    set_active_trade,
    register_order,
)

logger = logging.getLogger("okx-scanner")


EXECUTION_WHITELIST_KEYWORDS = tuple(EXECUTION_SETUP_WHITELIST)


def _normalize_execution_tag(value) -> str:
    try:
        tag = str(value or "").strip().lower()
        tag = tag.replace(" ", "_").replace("-", "_")
        while "__" in tag:
            tag = tag.replace("__", "_")
        return tag.strip("_|")
    except Exception:
        return ""


def _get_execution_setup_tags(candidate: dict) -> set:
    """Read only the canonical tags built by main.py."""
    raw_tags = (candidate or {}).get("execution_setup_tags") or []
    if isinstance(raw_tags, str):
        raw_tags = raw_tags.replace(",", "|").split("|")
    if not isinstance(raw_tags, (list, tuple, set)):
        raw_tags = []
    return {_normalize_execution_tag(tag) for tag in raw_tags if _normalize_execution_tag(tag)}


def is_setup_allowed_for_execution(candidate: dict) -> dict:
    """Final execution whitelist defense.

    Whitelist matching depends only on candidate['execution_setup_tags'].
    BLOCK_LONGS exceptions remain allowed as agreed.
    """
    setup_type = str(candidate.get("setup_type", "") or "").strip()
    mode = str(candidate.get("current_mode") or candidate.get("market_mode") or candidate.get("mode") or "").upper()
    block_exception = (
        bool(candidate.get("block_exception"))
        or bool(candidate.get("block_longs_execution_candidate"))
        or mode == "BLOCK_LONGS"
    )
    tags = _get_execution_setup_tags(candidate)
    whitelist = {_normalize_execution_tag(k) for k in EXECUTION_WHITELIST_KEYWORDS}
    allowed = block_exception or bool(tags & whitelist)
    return {
        "allowed": allowed,
        "reason": "execution_candidate_allowed" if allowed else "setup_not_whitelisted",
        "setup_type": setup_type,
        "execution_setup_tags": sorted(tags),
    }


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _round_like(value: float, reference):
    """Round adjusted TP using the visible precision of the original TP value."""
    try:
        ref_text = str(reference)
        if "." in ref_text:
            decimals = max(0, min(12, len(ref_text.rstrip("0").split(".", 1)[1])))
            return round(float(value), decimals)
        return round(float(value), 8)
    except Exception:
        return value


def _is_block_longs_candidate(candidate: dict) -> bool:
    mode = str((candidate or {}).get("current_mode") or (candidate or {}).get("market_mode") or (candidate or {}).get("mode") or "").upper()
    return (
        bool((candidate or {}).get("block_exception"))
        or bool((candidate or {}).get("block_longs_execution_candidate"))
        or mode == "BLOCK_LONGS"
    )


def _apply_block_longs_execution_tp1_adjustment(order: dict, candidate: dict) -> dict:
    """For BLOCK_LONGS execution only, bring TP1 25% closer while keeping signal/tracking TP unchanged."""
    try:
        if not _is_block_longs_candidate(candidate):
            return order
        entry = _safe_float(order.get("entry"), None)
        tp1 = _safe_float(order.get("tp1"), None)
        if entry is None or tp1 is None or tp1 <= entry:
            return order
        original_tp1 = order.get("tp1")
        adjusted_tp1 = entry + ((tp1 - entry) * 0.75)
        order["original_tp1"] = original_tp1
        order["tp1"] = _round_like(adjusted_tp1, original_tp1)
        order["block_longs_tp1_adjusted"] = True
        order["block_longs_tp1_factor"] = 0.75
        order["block_longs_note"] = "BLOCK_LONGS: TP1 adjusted to 75% of normal TP1 distance for faster risk reduction"
        return order
    except Exception:
        return order

def _fmt(value) -> str:
    return html.escape(str(value if value is not None else ""))




def _slot_lines(decision: dict) -> list:
    try:
        active = int(decision.get("active_count", 0) or 0)
        max_pos = int(decision.get("max_positions", 0) or 0)
        remaining = int(decision.get("remaining_slots", max(0, max_pos - active)) or 0)
        plan = decision.get("position_plan", {}) or {}
        margin = plan.get("margin_per_trade")
        capital = plan.get("max_capital_in_use")
        lines = [
            f"📊 <b>حد الصفقات:</b> {_fmt(max_pos)}",
            f"📂 <b>المفتوح المحسوب:</b> {_fmt(active)}",
            f"✅ <b>المتبقي:</b> {_fmt(remaining)}",
        ]
        if margin is not None and capital is not None:
            lines.append(f"💰 <b>Capital:</b> {_fmt(capital)}$ | <b>Margin/Trade:</b> {_fmt(margin)}$")
        return lines
    except Exception:
        return []


def build_execution_rejected_message(symbol: str, decision: dict, setup_type: str = "") -> str:
    reason = str((decision or {}).get("reason") or "")
    if reason == "max_positions_reached":
        reason_ar = "تم الوصول للحد الأقصى للصفقات"
    elif reason == "same_symbol_open":
        reason_ar = "توجد صفقة تنفيذ مفتوحة لنفس الزوج قبل TP2"
    else:
        reason_ar = html.escape(reason or "execution_rejected")
    return "\n".join([
        "⚠️ <b>Execution Candidate Rejected</b>",
        "━━━━━━━━━━━━",
        "",
        f"🪙 <b>{_fmt(symbol)}</b>",
        f"🧠 <b>Setup:</b> {_fmt(setup_type)}",
        f"❌ <b>السبب:</b> {reason_ar}",
        "",
        *_slot_lines(decision or {}),
        "",
        "ℹ️ TP2 protected runners لا تُحسب ضمن الحد.",
    ])

def build_execution_preview_message(order: dict, setup_type: str, order_id: str, decision: dict = None) -> str:
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
        (f"⚠️ <b>BLOCK_LONGS TP1:</b> 75% من TP1 العادي | الأصلي: {_fmt(order.get('original_tp1'))}" if order.get("block_longs_tp1_adjusted") else ""),
        f"🎯 <b>TP2:</b> {_fmt(order.get('tp2'))} | إغلاق {_fmt(tp_plan.get('tp2_close_pct'))}%",
        f"🏃 <b>Trailing:</b> {_fmt(tp_plan.get('trailing_position_pct'))}% | {_fmt(tp_plan.get('trailing_pct'))}%",
        "",
        "🔒 <b>TP1:</b> إغلاق 40% فقط — SL لا يتحرك",
        "🛡 <b>TP2:</b> إغلاق 40% + SL → TP1 + Trailing 20%",
        "",
        *(_slot_lines(decision or {})),
        "",
        "⚠️ <b>Simulation فقط - لم يتم إرسال أمر حقيقي إلى OKX</b>",
    ])


def build_pending_pullback_message(order: dict, setup_type: str, order_id: str, decision: dict = None) -> str:
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
        (f"⚠️ <b>BLOCK_LONGS TP1:</b> 75% من TP1 العادي | الأصلي: {_fmt(order.get('original_tp1'))}" if order.get("block_longs_tp1_adjusted") else ""),
        f"🎯 <b>TP2:</b> {_fmt(order.get('tp2'))} | إغلاق {_fmt(tp_plan.get('tp2_close_pct'))}%",
        f"🏃 <b>Trailing:</b> {_fmt(tp_plan.get('trailing_position_pct'))}% | {_fmt(tp_plan.get('trailing_pct'))}%",
        "",
        "⏳ <b>لن يتم تنفيذ Market الآن.</b>",
        "✅ سيتم انتظار السعر داخل منطقة Pullback.",
        "🔒 <b>TP1:</b> إغلاق 40% فقط — SL لا يتحرك",
        "🛡 <b>TP2:</b> إغلاق 40% + SL → TP1 + Trailing 20%",
        "",
        *(_slot_lines(decision or {})),
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
        rejection_message = build_execution_rejected_message(symbol, decision, setup_decision.get("setup_type"))
        return {
            "status": "rejected",
            "reason": decision.get("reason"),
            "setup_type": setup_decision.get("setup_type"),
            "active_count": decision.get("active_count"),
            "max_positions": decision.get("max_positions"),
            "remaining_slots": decision.get("remaining_slots"),
            "position_plan": decision.get("position_plan"),
            "execution_message": rejection_message,
        }

    # 3) Build order preview
    order = build_order_preview(symbol, candidate)
    order = _apply_block_longs_execution_tp1_adjustment(order, candidate)

    # 4) Generate fake order id
    order_id = f"sim_{symbol}_{int(time.time())}"

    is_pullback = (
        order.get("entry_mode") == "pullback_pending"
        or order.get("status") == "pending_pullback_preview"
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
        "original_tp1": order.get("original_tp1"),
        "block_longs_tp1_adjusted": order.get("block_longs_tp1_adjusted", False),
        "tp2": order.get("tp2"),
        "tp1_close_pct": order.get("tp_plan", {}).get("tp1_close_pct"),
        "tp2_close_pct": order.get("tp_plan", {}).get("tp2_close_pct"),
        "trailing_pct": order.get("tp_plan", {}).get("trailing_pct"),
        "trailing_position_pct": order.get("tp_plan", {}).get("trailing_position_pct"),
        "slot_active_count": decision.get("active_count"),
        "slot_max_positions": decision.get("max_positions"),
        "slot_remaining": decision.get("remaining_slots"),
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
            "tp1": order.get("tp1"),
            "original_tp1": order.get("original_tp1"),
            "block_longs_tp1_adjusted": order.get("block_longs_tp1_adjusted", False),
            "pullback_low": order.get("pullback_low"),
            "pullback_high": order.get("pullback_high"),
        },
    )

    if is_pullback:
        execution_message = build_pending_pullback_message(
            order,
            setup_decision.get("setup_type"),
            order_id,
            decision,
        )
    else:
        execution_message = build_execution_preview_message(
            order,
            setup_decision.get("setup_type"),
            order_id,
            decision,
        )

    return {
        "status": result_status,
        "order_id": order_id,
        "order": order,
        "setup_type": setup_decision.get("setup_type"),
        "entry_mode": order.get("entry_mode", "market"),
        "execution_message": execution_message,
        "active_count": decision.get("active_count"),
        "max_positions": decision.get("max_positions"),
        "remaining_slots": decision.get("remaining_slots"),
        "position_plan": decision.get("position_plan"),
    }
