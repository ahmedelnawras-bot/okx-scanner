from __future__ import annotations

from typing import Any

from analysis.models import SignalCandidate
from analysis.execution_candidate import decide_execution_candidate

from utils.constants import (
    MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
    MAX_RECOVERY_TRADES_PER_CYCLE,
)

from .risk_manager import evaluate_execution_risk
from .order_builder import build_preview_order


# ─────────────────────────────────────────
# Same-symbol active protection
# يمنع إعادة الدخول لنفس العملة
# طالما الصفقة القديمة ما زالت تمنع re-entry
#
# بعد TP2:
# - الصفقة قد تظل موجودة للـ runner tracking
# - لكنها لا تمنع same-symbol re-entry
# ─────────────────────────────────────────
ACTIVE_BLOCKING_STATUSES = {
    "open",
    "tp1_hit",
    "runner_active",
    "breakeven_runner",
    "protected_runner",
    "partial_runner",
}

NON_BLOCKING_FINAL_STATUSES = {
    "closed",
    "stopped",
    "closed_loss",
    "closed_win",
    "expired",
    "tp2_hit",
    "take_profit_2_hit",
    "trailing_hit",
    "breakeven_after_tp1",
}


def _get_execution_score(signal: SignalCandidate) -> float:
    """
    v129 execution architecture
    ───────────────────────────
    IMPORTANT:
    execution/risk MUST use boost_score
    NOT display_score.

    signal.score = UI display only.
    """
    meta = signal.meta or {}

    boost_score = meta.get("boost_score")
    if boost_score is not None:
        try:
            return float(boost_score)
        except Exception:
            pass

    # emergency fallback only
    return float(signal.score)


def _trade_reached_tp2(trade) -> bool:
    return bool(
        getattr(trade, "tp2_hit", False)
        or getattr(trade, "take_profit_2_hit", False)
        or str(getattr(trade, "status", "")).lower() in {"tp2_hit", "take_profit_2_hit"}
    )


def _trade_is_closed(trade) -> bool:
    return bool(getattr(trade, "is_closed", False))


def _trade_blocks_same_symbol_reentry(trade) -> bool:
    if bool(getattr(trade, "same_symbol_block_exempt", False)):
        return False

    explicit_flag = getattr(trade, "blocks_same_symbol_reentry", None)
    if explicit_flag is not None:
        try:
            return bool(explicit_flag)
        except Exception:
            pass

    if _trade_is_closed(trade):
        return False

    if _trade_reached_tp2(trade):
        return False

    trade_status = str(getattr(trade, "status", "")).lower()

    if trade_status in NON_BLOCKING_FINAL_STATUSES:
        return False

    if trade_status in ACTIVE_BLOCKING_STATUSES:
        return True

    # fallback protection:
    # أي صفقة غير مغلقة ولم تصل TP2
    # تعتبر ما زالت مانعة لإعادة الدخول
    return True


def _same_symbol_trade_is_active(trade, signal: SignalCandidate) -> tuple[bool, str]:
    trade_symbol = getattr(
        trade,
        "symbol",
        None,
    )

    if trade_symbol != signal.symbol:
        return False, ""

    if not _trade_blocks_same_symbol_reentry(trade):
        return False, ""

    trade_status = str(
        getattr(trade, "status", "")
    ).lower()

    return True, trade_status or "active_unfinished_trade"


def _managed_target_model(signal: SignalCandidate, path: str) -> tuple[str, float, float, float]:
    normalized_path = str(path or "").strip().lower()
    if normalized_path == "recovery":
        return "recovery_50_25_25", 50.0, 25.0, 25.0
    return "standard_40_40_20", 40.0, 40.0, 20.0


def _build_managed_trade_preview(signal: SignalCandidate, path: str) -> dict[str, Any]:
    target_model, tp1_pct, tp2_pct, runner_pct = _managed_target_model(signal, path)
    entry = float(getattr(signal, "entry", 0.0) or 0.0)
    sl = float(getattr(signal, "sl", 0.0) or 0.0)
    tp1 = float(getattr(signal, "tp1", 0.0) or 0.0)
    tp2 = float(getattr(signal, "tp2", 0.0) or 0.0)

    def _portion(exit_price: float, pct: float) -> dict[str, Any]:
        return {
            "price": exit_price,
            "close_pct": pct,
            "close_fraction": round(pct / 100.0, 6),
        }

    return {
        "target_model": target_model,
        "entry": {
            "symbol": getattr(signal, "symbol", ""),
            "price": entry,
            "entry_mode": "pullback_pending" if getattr(signal, "entry_timing", "") == "pullback" else "market",
        },
        "stop_loss": {
            "price": sl,
            "attach_on_entry": sl > 0,
            "ord_px": "-1" if sl > 0 else "",
        },
        "tp1": _portion(tp1, tp1_pct),
        "tp2": _portion(tp2, tp2_pct),
        "runner": {
            "close_pct": runner_pct,
            "close_fraction": round(runner_pct / 100.0, 6),
            "requires_trailing_after_tp2": True,
            "requires_block_sl_sync": True,
        },
        "block_protection": {
            "enabled": True,
            "amend_live_stop_loss": True,
        },
    }


def _base_response(
    *,
    signal: SignalCandidate,
    gate: dict[str, Any],
    risk: dict[str, Any] | None = None,
    status: str,
    reason: str,
    slot_scope: str = "",
    order: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution_score = _get_execution_score(signal)
    path = gate.get("path") if isinstance(gate, dict) else ""
    managed_trade_plan = _build_managed_trade_preview(signal, str(path or ""))
    target_model, tp1_pct, tp2_pct, runner_pct = _managed_target_model(signal, str(path or ""))

    payload: dict[str, Any] = {
        "status": status,
        "reason": reason,
        "path": path,
        "slot_scope": slot_scope,
        "gate": gate,
        "nour_filter_name": gate.get("nour_filter_name") if isinstance(gate, dict) else None,
        "nour_filter_passed": gate.get("nour_filter_passed") if isinstance(gate, dict) else None,
        "nour_filter_reason": gate.get("nour_filter_reason") if isinstance(gate, dict) else None,
        "execution_score": round(execution_score, 2),
        "display_score": round(float(signal.score), 2),
        "target_model": target_model,
        "tp1_close_pct": tp1_pct,
        "tp2_close_pct": tp2_pct,
        "runner_close_pct": runner_pct,
        "managed_trade_plan": managed_trade_plan,
        "sl_attached_on_entry": bool((managed_trade_plan.get("stop_loss") or {}).get("attach_on_entry")),
        "runner_requires_trailing_after_tp2": bool((managed_trade_plan.get("runner") or {}).get("requires_trailing_after_tp2")),
        "tp_split_expected": True,
        "exchange_sync_state": (
            "queued_for_submission"
            if status == "accepted_preview"
            else "awaiting_pullback"
            if status == "pending_pullback_preview"
            else "not_submitted"
        ),
        "exchange_order_ok": False,
        "exchange_order_reason": "not_submitted",
        "entry_order_id": "",
        "entry_client_order_id": "",
        "entry_order_payload": {},
        "sl_attached_payload": [],
        "live_stop_loss_px": float(getattr(signal, "sl", 0.0) or 0.0),
        "tp_split_ok": False,
        "tp_split_reason": "not_submitted",
        "tp1_order_id": "",
        "tp2_order_id": "",
        "tp1_client_order_id": "",
        "tp2_client_order_id": "",
        "runner_expected_size": "",
        "runner_algo_id": "",
        "runner_algo_client_order_id": "",
        "last_exchange_error": "",
    }

    if order is not None:
        payload["order"] = order

    if isinstance(risk, dict):
        payload["slots"] = risk.get("slots")
        payload["drawdown_level"] = risk.get("drawdown_level")
        payload["drawdown_pct"] = risk.get("drawdown_pct")

    return payload


def process_trade_candidate(
    signal: SignalCandidate,
    open_trades: list | None = None,
    current_open_positions: int = 0,
    max_open_positions: int = 10,
    min_execution_score: float = 6.6,
    recovery_slots_remaining: int | None = None,
    block_open_positions: int = 0,
    max_block_positions: int = MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
    recovery_open_positions: int = 0,
    max_recovery_positions: int = MAX_RECOVERY_TRADES_PER_CYCLE,
    drawdown_status=None,
) -> dict:

    # ─────────────────────────────────────────
    # Execution intelligence gate
    # IMPORTANT:
    # Nour filter + execution quality
    # must stay inside execution layer
    # NOT scoring architecture
    # ─────────────────────────────────────────
    gate = decide_execution_candidate(
        signal,
        recovery_slots_remaining=recovery_slots_remaining,
    )

    # ─────────────────────────────────────────
    # Candidate rejected by execution gate
    # ─────────────────────────────────────────
    if not gate["allowed"]:
        status = "candidate_only"

        if gate["reason"] in {
            "late_risky_execution_context",
            "weak_drift_execution_block",
            "recovery_quality_not_confirmed",
            "velocity_instability",
            "expansion_exhaustion",
            "pa_structure_weak",
            "pa_weak_breakout_danger",
        }:
            status = "rejected_quality"

        if gate["reason"] in {
            "recovery_cycle_full",
        }:
            status = "rejected_limit"

        return _base_response(
            signal=signal,
            gate=gate,
            status=status,
            reason=gate["reason"],
        )

    # ─────────────────────────────────────────
    # Same symbol protection
    # يمنع إعادة الدخول لنفس العملة
    # قبل انتهاء مرحلة المنع الفعلية
    #
    # بعد TP2 لا يوجد block على نفس العملة
    # حتى لو ظل runner tracking قائمًا
    # ─────────────────────────────────────────
    open_trades = open_trades or []

    for trade in open_trades:
        is_active, trade_status = _same_symbol_trade_is_active(
            trade,
            signal,
        )

        if is_active:
            return _base_response(
                signal=signal,
                gate=gate,
                status="rejected_same_symbol",
                reason="same_symbol_active_trade",
            ) | {
                "existing_trade_status": trade_status,
            }

    # ─────────────────────────────────────────
    # IMPORTANT:
    # execution layer uses boost_score
    # NOT display_score
    # ─────────────────────────────────────────
    execution_score = _get_execution_score(signal)

    path = str(gate.get("path") or "")

    # ─────────────────────────────────────────
    # BLOCK exception routing
    # ─────────────────────────────────────────
    if path == "block_exception":
        risk = evaluate_execution_risk(
            score=execution_score,
            max_open_positions=max_block_positions,
            current_open_positions=block_open_positions,
            min_execution_score=min_execution_score,
            drawdown_status=drawdown_status,
        )
        slot_scope = "block_exception"

    # ─────────────────────────────────────────
    # RECOVERY routing
    # ─────────────────────────────────────────
    elif path == "recovery":
        risk = evaluate_execution_risk(
            score=execution_score,
            max_open_positions=max_recovery_positions,
            current_open_positions=recovery_open_positions,
            min_execution_score=min_execution_score,
            drawdown_status=drawdown_status,
        )
        slot_scope = "recovery"

    # ─────────────────────────────────────────
    # NORMAL / STRONG routing
    # ─────────────────────────────────────────
    else:
        risk = evaluate_execution_risk(
            score=execution_score,
            max_open_positions=max_open_positions,
            current_open_positions=current_open_positions,
            min_execution_score=min_execution_score,
            drawdown_status=drawdown_status,
        )
        slot_scope = "general"

    # ─────────────────────────────────────────
    # Risk rejected
    # ─────────────────────────────────────────
    if not risk["allowed"]:
        return _base_response(
            signal=signal,
            gate=gate,
            risk=risk,
            status=(
                "rejected_limit"
                if risk["reason"] == "max_positions_reached"
                else "rejected_risk"
            ),
            reason=risk["reason"],
            slot_scope=slot_scope,
        )

    # ─────────────────────────────────────────
    # Final acceptance state
    # ─────────────────────────────────────────
    status = (
        "pending_pullback_preview"
        if gate["pending_pullback"]
        else "accepted_preview"
    )

    order = build_preview_order(signal)

    return _base_response(
        signal=signal,
        gate=gate,
        risk=risk,
        status=status,
        reason=gate["reason"],
        slot_scope=slot_scope,
        order=order,
    )
