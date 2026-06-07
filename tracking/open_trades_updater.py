from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .lifecycle import update_trade_with_price
from .models import TrackedTrade


def _safe_setattr(obj: Any, name: str, value: Any) -> None:
    try:
        setattr(obj, name, value)
    except Exception:
        pass


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _call_if_exists(obj: Any, method_name: str, *args, **kwargs) -> Any:
    method = getattr(obj, method_name, None)
    if callable(method):
        try:
            return method(*args, **kwargs)
        except Exception as exc:
            return {"ok": False, "reason": f"{method_name}_exception: {exc}"}
    return None


def _entry_identifiers(trade: TrackedTrade) -> tuple[str, str]:
    return (
        str(getattr(trade, "entry_order_id", "") or ""),
        str(getattr(trade, "entry_client_order_id", "") or ""),
    )


def _parse_order_state(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""

    row = payload.get("order") or payload.get("data") or payload.get("response") or payload
    if isinstance(row, list):
        row = row[0] if row else {}
    if not isinstance(row, dict):
        return ""

    state = row.get("state") or row.get("ordState") or row.get("status") or row.get("fill_state")
    return str(state or "").strip().lower()


def _parse_fill_summary(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else payload
    if not isinstance(summary, dict):
        return {}

    return {
        "ok": bool(summary.get("ok", True)),
        "filled": bool(summary.get("filled", False)),
        "partial": bool(summary.get("partial", False)),
        "fill_ratio": _safe_float(summary.get("fill_ratio"), 0.0),
        "filled_size": _safe_float(summary.get("filled_size"), 0.0),
        "order_size": _safe_float(summary.get("order_size"), 0.0),
        "avg_fill_px": _safe_float(summary.get("avg_fill_px"), 0.0),
        "state": str(summary.get("state") or "").strip().lower(),
        "reason": str(summary.get("reason") or ""),
    }


def _sync_entry_fill_state(trade: TrackedTrade, okx_client: Any) -> TrackedTrade:
    inst_id = str(getattr(trade, "symbol", "") or "")
    ord_id, cl_ord_id = _entry_identifiers(trade)
    if not inst_id or not (ord_id or cl_ord_id):
        return trade

    fill_summary = _call_if_exists(
        okx_client,
        "get_order_fill_summary",
        inst_id=inst_id,
        ord_id=ord_id or None,
        cl_ord_id=cl_ord_id or None,
    )
    parsed = _parse_fill_summary(fill_summary)
    if not parsed:
        return trade

    _safe_setattr(trade, "entry_fill_ratio", parsed.get("fill_ratio", 0.0))
    _safe_setattr(trade, "entry_filled_size", parsed.get("filled_size", 0.0))
    _safe_setattr(trade, "entry_order_size", parsed.get("order_size", 0.0))
    _safe_setattr(trade, "entry_avg_fill_px", parsed.get("avg_fill_px", 0.0))
    _safe_setattr(trade, "entry_fill_state", parsed.get("state", ""))
    _safe_setattr(trade, "last_exchange_sync_at", datetime.now(timezone.utc))

    avg_fill_px = parsed.get("avg_fill_px", 0.0)
    if avg_fill_px > 0:
        # Keep tracked entry aligned with the actual executed entry when available.
        _safe_setattr(trade, "entry", avg_fill_px)
        if _safe_float(getattr(trade, "current_price", 0.0), 0.0) <= 0:
            _safe_setattr(trade, "current_price", avg_fill_px)
        if _safe_float(getattr(trade, "highest_price", 0.0), 0.0) <= 0:
            _safe_setattr(trade, "highest_price", avg_fill_px)

    if parsed.get("filled"):
        _safe_setattr(trade, "exchange_sync_state", "entry_filled")
        if str(getattr(trade, "execution_status", "") or "").strip().lower() == "accepted_preview":
            _safe_setattr(trade, "execution_status", "executed")
    elif parsed.get("partial"):
        _safe_setattr(trade, "exchange_sync_state", "entry_partial_fill")
    elif parsed.get("state"):
        _safe_setattr(trade, "exchange_sync_state", f"entry_{parsed.get('state')}")

    return trade


def _sync_tp_order_state(trade: TrackedTrade, okx_client: Any) -> TrackedTrade:
    inst_id = str(getattr(trade, "symbol", "") or "")
    if not inst_id:
        return trade

    tp_states: dict[str, str] = {}
    for label, attr in (("tp1", "tp1_order_id"), ("tp2", "tp2_order_id")):
        ord_id = str(getattr(trade, attr, "") or "")
        if not ord_id:
            continue
        details = _call_if_exists(okx_client, "get_order_details", inst_id=inst_id, ord_id=ord_id)
        state = _parse_order_state(details)
        if state:
            tp_states[label] = state
            _safe_setattr(trade, f"{label}_exchange_state", state)

    if tp_states:
        _safe_setattr(trade, "last_exchange_sync_at", datetime.now(timezone.utc))

    return trade


def _sync_live_stop_snapshot(trade: TrackedTrade, okx_client: Any) -> TrackedTrade:
    inst_id = str(getattr(trade, "symbol", "") or "")
    ord_id, _ = _entry_identifiers(trade)
    if not inst_id or not ord_id:
        return trade

    snapshot = _call_if_exists(okx_client, "get_attached_stop_from_order", inst_id=inst_id, ord_id=ord_id)
    if not isinstance(snapshot, dict):
        return trade

    current_sl = _safe_float(snapshot.get("slTriggerPx") or snapshot.get("trigger_px"), 0.0)
    if current_sl > 0:
        _safe_setattr(trade, "live_stop_loss_px", current_sl)
        _safe_setattr(trade, "last_exchange_sync_at", datetime.now(timezone.utc))

    attach_algo_id = snapshot.get("attachAlgoId") or snapshot.get("algoId")
    if attach_algo_id:
        _safe_setattr(trade, "sl_attach_algo_id", str(attach_algo_id))

    return trade


def _sync_stop_loss_to_exchange(trade: TrackedTrade, okx_client: Any) -> TrackedTrade:
    inst_id = str(getattr(trade, "symbol", "") or "")
    ord_id, _ = _entry_identifiers(trade)
    if not inst_id or not ord_id:
        return trade

    desired_sl = max(
        _safe_float(getattr(trade, "protected_sl", 0.0), 0.0),
        _safe_float(getattr(trade, "sl", 0.0), 0.0),
    )
    live_sl = _safe_float(getattr(trade, "live_stop_loss_px", 0.0), 0.0)

    if desired_sl <= 0:
        return trade
    if live_sl > 0 and desired_sl <= live_sl + 1e-12:
        return trade

    amend_result = _call_if_exists(
        okx_client,
        "sync_attached_stop_loss",
        inst_id=inst_id,
        ord_id=ord_id,
        desired_sl_trigger_px=desired_sl,
        current_sl_trigger_px=live_sl if live_sl > 0 else None,
        attach_algo_id=str(getattr(trade, "sl_attach_algo_id", "") or "") or None,
    )
    if not isinstance(amend_result, dict):
        return trade

    _safe_setattr(trade, "sl_sync_result", amend_result)
    _safe_setattr(trade, "last_exchange_sync_at", datetime.now(timezone.utc))

    if bool(amend_result.get("ok")):
        _safe_setattr(trade, "live_stop_loss_px", desired_sl)
        _safe_setattr(trade, "exchange_sync_state", "sl_synced")
    elif amend_result.get("reason"):
        _safe_setattr(trade, "exchange_sync_state", f"sl_sync_failed:{amend_result.get('reason')}")

    return trade


def _is_recovered_trade(trade: TrackedTrade) -> bool:
    """Return True for trades imported from OKX without original order IDs."""
    market_mode = str(getattr(trade, "market_mode", "") or "").upper()
    setup_type = str(getattr(trade, "setup_type", "") or "").lower()
    exchange_sync = str(getattr(trade, "exchange_sync_state", "") or "").lower()
    return bool(
        "RECOVERED" in market_mode
        or "RESTORED" in market_mode
        or "recovered" in setup_type
        or "restored" in setup_type
        or "recovered" in exchange_sync
        or "restored" in exchange_sync
    )


def _sync_recovered_trade_from_position(trade: TrackedTrade, okx_client: Any) -> TrackedTrade:
    """Sync price + margin for recovered/restored trades that have no order IDs.

    These trades were imported from live OKX positions so they have no
    entry_order_id. We sync directly from the positions endpoint instead.
    """
    inst_id = str(getattr(trade, "symbol", "") or "")
    if not inst_id or okx_client is None:
        return trade

    try:
        positions_result = _call_if_exists(okx_client, "get_positions", inst_type="SWAP") or {}
        rows = positions_result.get("rows") or positions_result.get("data") or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_inst = str(row.get("instId") or "").strip().upper()
            if row_inst != inst_id.upper():
                continue

            # سعر الـ mark الحالي
            for px_key in ("markPx", "last", "lastPx", "idxPx"):
                mark_px = _safe_float(row.get(px_key), 0.0)
                if mark_px > 0:
                    _safe_setattr(trade, "current_price", mark_px)
                    break

            # entry price من OKX
            for avg_key in ("avgPx", "avgPxUsd", "openAvgPx"):
                avg_px = _safe_float(row.get(avg_key), 0.0)
                if avg_px > 0:
                    _safe_setattr(trade, "entry", avg_px)
                    break

            # margin الحالي
            for margin_key in ("margin", "imr", "initialMargin", "marginUsd"):
                margin = _safe_float(row.get(margin_key), 0.0)
                if margin > 0:
                    _safe_setattr(trade, "used_margin_usdt", margin)
                    _safe_setattr(trade, "margin_usdt", margin)
                    break

            # leverage
            lever = _safe_float(row.get("lever") or row.get("leverage"), 0.0)
            if lever > 0:
                _safe_setattr(trade, "effective_leverage", lever)

            _safe_setattr(trade, "exchange_sync_state", "position_synced")
            _safe_setattr(trade, "last_exchange_sync_at", datetime.now(timezone.utc))
            print(
                f"RECOVERED_TRADE_SYNC | {inst_id} | "
                f"markPx={getattr(trade, 'current_price', 0)} | "
                f"entry={getattr(trade, 'entry', 0)}",
                flush=True,
            )
            break
    except Exception as exc:
        print(f"⚠️ RECOVERED_TRADE_SYNC_FAILED | {inst_id} | {exc}", flush=True)

    return trade


def _sync_trade_with_exchange(trade: TrackedTrade, okx_client: Any) -> TrackedTrade:
    if okx_client is None:
        return trade

    # ✅ FIX: الـ recovered/restored trades مش عندهم entry_order_id
    # بنسنحهم من الـ positions endpoint مباشرة بدل ما نتجاهلهم
    if _is_recovered_trade(trade):
        return _sync_recovered_trade_from_position(trade, okx_client)

    trade = _sync_entry_fill_state(trade, okx_client)
    trade = _sync_tp_order_state(trade, okx_client)
    trade = _sync_live_stop_snapshot(trade, okx_client)
    return trade


def update_open_trades(
    trades: list[TrackedTrade],
    price_map: dict[str, float],
    protection_level: int = 0,
    okx_client: Any | None = None,
    sync_exchange: bool = False,
    sync_exchange_stop: bool = False,
) -> list[TrackedTrade]:
    """Update tracked trades with price and optional read-only exchange reconciliation.

    Backward compatible:
    - existing callers can still use update_open_trades(trades, price_map, protection_level)

    Read-only behavior when sync_exchange=True and okx_client is provided:
    - sync entry fill snapshot from OKX
    - sync TP1/TP2 order states snapshot
    - read currently attached stop loss from OKX

    Optional write-back remains disabled by default and only runs when
    sync_exchange_stop=True. The main worker can use that only during
    BLOCK protection scans to push a tighter SL to OKX once per full scan.
    """
    updated: list[TrackedTrade] = []
    for trade in trades:
        current_price = float(price_map.get(trade.symbol, trade.current_price or trade.entry))

        if sync_exchange and okx_client is not None:
            trade = _sync_trade_with_exchange(trade, okx_client)

        trade = update_trade_with_price(trade, current_price, protection_level=protection_level)

        if sync_exchange and sync_exchange_stop and okx_client is not None:
            trade = _sync_stop_loss_to_exchange(trade, okx_client)

        updated.append(trade)
    return updated
