from __future__ import annotations

import threading
from datetime import datetime, timezone
from statistics import mean
from typing import Any

from analysis.market_modes import MarketModeState, MarketSnapshot, decide_market_mode, register_recovery_trade
from analysis.models import PairCandidate
from analysis.scoring import build_signal_candidate
from execution.execution_processor import process_trade_candidate
from utils.constants import MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE, MAX_RECOVERY_TRADES_PER_CYCLE

from .dataset_writer import append_records
from .okx_loader import (
    HistoricalCandle,
    fetch_historical_candles,
    fetch_swap_tickers,
    select_top_usdt_swap_symbols,
    timeframe_to_ms,
)
from .outcome import evaluate_trade_outcome
from .state import DATA_KEY, LOCAL_DATA_PATH, append_log, clear_stop, get_status, set_status, stop_requested, utc_now_iso

_WORKER_LOCK = threading.Lock()
_WORKER_THREAD: threading.Thread | None = None


def _safe_pct(open_: float, close: float) -> float:
    try:
        return ((float(close) / float(open_)) - 1.0) * 100.0 if float(open_) > 0 else 0.0
    except Exception:
        return 0.0


def _volume_avg(candles: list[HistoricalCandle], idx: int, lookback: int = 20) -> float:
    start = max(0, idx - int(lookback))
    vals = [float(c.quote_volume or c.volume or 0.0) for c in candles[start:idx] if float(c.quote_volume or c.volume or 0.0) > 0]
    return mean(vals) if vals else 0.0


def _pair_from_candle(symbol: str, candles: list[HistoricalCandle], idx: int, btc_change: float = 0.0) -> PairCandidate | None:
    candle = candles[idx]
    change_15m = _safe_pct(candle.open, candle.close)
    quote_volume = float(candle.quote_volume or candle.volume or 0.0)
    avg_vol = _volume_avg(candles, idx, lookback=20)
    vol_ratio = (quote_volume / avg_vol) if avg_vol > 0 else 1.0

    # Lightweight historical feature proxy. The actual old scoring/candidate
    # builder is still used downstream; these tags emulate the ranked-pair input
    # that live scanning receives from ticker/pair selection.
    tags: list[str] = []
    if quote_volume >= 100_000 or vol_ratio >= 1.1:
        tags.append("liquid")
    if change_15m >= 0.28:
        tags.append("momentum")
    if change_15m >= 0.65 and vol_ratio >= 1.15:
        tags.append("breakout")
    if change_15m >= btc_change + 0.20 and change_15m > 0:
        tags.append("rs_btc")
    if change_15m < -0.35 and vol_ratio >= 0.9:
        tags.append("rebound")
    if change_15m >= 1.8 and vol_ratio < 1.05:
        tags.append("near_resistance")
    if symbol.startswith(("BTC-", "ETH-", "SOL-", "XRP-", "DOGE-", "BNB-", "AVAX-", "LINK-")):
        tags.append("major")

    score_hint = 5.85
    score_hint += min(max(change_15m, -1.0), 2.2) * 0.85
    score_hint += min(max(vol_ratio - 1.0, 0.0), 2.5) * 0.38
    if "breakout" in tags:
        score_hint += 0.45
    if "rs_btc" in tags:
        score_hint += 0.35
    if "major" in tags:
        score_hint += 0.10
    rebound_hint = 0.45 if "rebound" in tags else 0.0

    if candle.close <= 0:
        return None
    pair = PairCandidate(
        symbol=symbol,
        last_price=float(candle.close),
        change_pct=round(change_15m, 4),
        turnover_usdt=quote_volume,
        score_hint=round(score_hint, 4),
        rebound_hint=round(rebound_hint, 4),
        tags=tags,
    )
    try:
        setattr(pair, "btc_bounce_pct", btc_change)
    except Exception:
        pass
    return pair


def _market_snapshot_at(symbol_candles: dict[str, list[HistoricalCandle]], idx: int) -> MarketSnapshot:
    changes: list[float] = []
    btc_change = 0.0
    for symbol, candles in symbol_candles.items():
        if idx >= len(candles):
            continue
        c = candles[idx]
        change = _safe_pct(c.open, c.close)
        changes.append(change)
        if symbol.startswith("BTC-"):
            btc_change = change
    if not changes:
        return MarketSnapshot()
    avg_change = sum(changes) / len(changes)
    red_ratio = sum(1 for x in changes if x < 0) / len(changes)
    strong_count = sum(1 for x in changes if x >= 0.40)
    if btc_change == 0.0:
        btc_change = avg_change
    return MarketSnapshot(
        btc_change_15m=btc_change,
        red_ratio_15m=red_ratio,
        avg_change_15m=avg_change,
        strong_coins_count=strong_count,
        fast_rebound=bool(avg_change > 0.20 and strong_count >= 6 and red_ratio <= 0.58 and btc_change > -0.40),
        btc_reclaim=bool(btc_change > -0.15),
        breadth_improving=bool(red_ratio <= 0.62 and avg_change > -0.55),
    )


def _record_from_signal(run_id: str, ts: int, signal: Any, execution: dict, snapshot: MarketSnapshot, outcome: dict) -> dict[str, Any]:
    status = str(execution.get("status") or "")
    gate = execution.get("gate") or {}
    reason = str(execution.get("reason") or gate.get("reason") or "")
    quality_candidate = bool(gate.get("allowed") or status in {"accepted_preview", "pending_pullback_preview", "rejected_limit"})
    execution_candidate = status in {"accepted_preview", "pending_pullback_preview"}
    blocked_by_limit = status == "rejected_limit" or reason in {"max_positions_reached", "recovery_cycle_full"}
    meta = getattr(signal, "meta", {}) or {}
    entry = float(getattr(signal, "entry", 0.0) or 0.0)
    sl = float(getattr(signal, "sl", 0.0) or 0.0)
    tp1 = float(getattr(signal, "tp1", 0.0) or 0.0)
    tp2 = float(getattr(signal, "tp2", 0.0) or 0.0)
    risk = max(entry - sl, 0.0)
    iso_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    return {
        "event": "historical_replay_signal",
        "schema_version": 2,
        "dataset_source": "historical_replay",
        "replay_run_id": run_id,
        "time": iso_time,
        "signal_id": f"{run_id}|{getattr(signal, 'symbol', '')}|{ts}|{getattr(signal, 'market_mode', '')}|{entry}",
        "symbol": getattr(signal, "symbol", ""),
        "mode": getattr(signal, "market_mode", ""),
        "signal_level": "candidate" if quality_candidate else "normal",
        "normal_signal": True,
        "quality_candidate": quality_candidate,
        "execution_candidate": execution_candidate,
        "blocked_by_limit": blocked_by_limit,
        "block_reason": reason if blocked_by_limit else "",
        "execution_status": status,
        "execution_path": execution.get("path") or gate.get("path") or "",
        "legacy_gate_passed": bool(gate.get("allowed", False)),
        "legacy_gate_reason": gate.get("reason") or reason,
        "trade_plan": {
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp1_pct": ((tp1 / entry) - 1.0) * 100.0 if entry else 0.0,
            "tp2_pct": ((tp2 / entry) - 1.0) * 100.0 if entry else 0.0,
            "sl_pct": ((sl / entry) - 1.0) * 100.0 if entry else 0.0,
            "rr1": ((tp1 - entry) / risk) if risk else 0.0,
            "rr2": ((tp2 - entry) / risk) if risk else 0.0,
        },
        "features": {
            "score": float(getattr(signal, "score", 0.0) or 0.0),
            "effective_score": float(meta.get("effective_score") or getattr(signal, "score", 0.0) or 0.0),
            "raw_score": float(meta.get("raw_score") or getattr(signal, "score", 0.0) or 0.0),
            "setup_type": getattr(signal, "setup_type", ""),
            "entry_timing": getattr(signal, "entry_timing", ""),
            "setup_tags": list(getattr(signal, "execution_setup_tags", []) or []),
            "pair_tags": list(meta.get("pair_tags", []) or []),
            "turnover_usdt": float(meta.get("turnover_usdt") or 0.0),
            "change_pct": float(meta.get("change_pct") or 0.0),
            "vol_ratio": float(meta.get("vol_ratio") or 1.0),
            "mtf_confirmed": bool(meta.get("mtf_confirmed")),
            "breakout": bool(meta.get("breakout")),
            "pre_breakout": bool(meta.get("pre_breakout")),
            "breakout_quality": meta.get("breakout_quality") or "",
            "setup_weight": float(meta.get("setup_weight") or 0.0),
            "resistance_warning": meta.get("resistance_warning") or "",
            "btc_bounce_pct": float(meta.get("btc_bounce_pct") or 0.0),
            "symbol_bounce_pct": float(meta.get("symbol_bounce_pct") or 0.0),
            "bounce_ratio_vs_btc": float(meta.get("bounce_ratio_vs_btc") or 0.0),
            "recovery_relative_bounce": bool(meta.get("recovery_relative_bounce")),
        },
        "market_context": {
            "mode": getattr(signal, "market_mode", ""),
            "btc_change_15m": float(snapshot.btc_change_15m or 0.0),
            "avg_change_15m": float(snapshot.avg_change_15m or 0.0),
            "red_ratio_15m": float(snapshot.red_ratio_15m or 0.0),
            "strong_coins_count": int(snapshot.strong_coins_count or 0),
        },
        "outcome": outcome,
    }


def _clear_output(redis_client: Any | None = None) -> None:
    try:
        if redis_client is not None:
            redis_client.delete(DATA_KEY)
    except Exception:
        pass
    try:
        if LOCAL_DATA_PATH.exists():
            LOCAL_DATA_PATH.unlink()
    except Exception:
        pass


def _run_replay_worker(days: int, symbols_limit: int, timeframe: str, redis_client: Any | None = None, settings: Any | None = None) -> None:
    base_url = str(getattr(settings, "okx_base_url", "https://www.okx.com") or "https://www.okx.com")
    timeout = int(getattr(settings, "request_timeout", 15) or 15)
    min_normal_score = float(getattr(settings, "min_normal_score", 6.2) or 6.2)
    min_strong_score = float(getattr(settings, "min_strong_score", 7.5) or 7.5)
    min_execution_score = float(getattr(settings, "min_execution_score", 6.6) or 6.6)
    max_open_positions = int(getattr(settings, "max_execution_positions", 10) or 10)

    days = max(1, int(days))
    tf_ms = timeframe_to_ms(timeframe)
    expected_candles = max(1, int((days * 86_400_000) / max(1, tf_ms)))
    min_required_candles = max(120, min(expected_candles - 96, int(expected_candles * 0.65)))

    run_id = f"replay_{days}d_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    _clear_output(redis_client)
    clear_stop(redis_client)
    status = get_status(redis_client)
    status.update({
        "running": True,
        "state": "loading_symbols",
        "run_id": run_id,
        "days": days,
        "symbols_limit": int(symbols_limit),
        "timeframe": timeframe,
        "expected_candles_per_symbol": expected_candles,
        "min_required_candles": min_required_candles,
        "current_symbol": "",
        "current_symbol_candles": 0,
        "started_at": utc_now_iso(),
        "completed_at": "",
        "progress_pct": 0.0,
        "symbols_total": int(symbols_limit),
        "symbols_done": 0,
        "records": 0,
        "normal": 0,
        "quality_candidates": 0,
        "execution_candidates": 0,
        "blocked_by_limits": 0,
        "message": "Loading OKX symbols for historical replay.",
    })
    set_status(status, redis_client)
    append_log(f"Replay heavy runner initialized: {run_id}", redis_client)

    try:
        tickers = fetch_swap_tickers(base_url, timeout=timeout)
        symbols = select_top_usdt_swap_symbols(tickers, limit=symbols_limit)
        status.update({"symbols_total": len(symbols), "state": "loading_candles", "message": f"Loading historical candles for {len(symbols)} symbols."})
        set_status(status, redis_client)

        symbol_candles: dict[str, list[HistoricalCandle]] = {}
        for i, symbol in enumerate(symbols, start=1):
            candles: list[HistoricalCandle] = []
            if stop_requested(redis_client):
                status.update({"running": False, "state": "stopped", "completed_at": utc_now_iso(), "message": "Stopped while loading candles."})
                set_status(status, redis_client)
                return
            try:
                status.update({"current_symbol": symbol, "current_symbol_candles": 0, "message": f"Loading candles: {i}/{len(symbols)} {symbol}"})
                set_status(status, redis_client)
                candles = fetch_historical_candles(base_url, symbol, bar=timeframe, days=days, timeout=timeout)
                if len(candles) >= min_required_candles:
                    symbol_candles[symbol] = candles
                    append_log(f"Loaded {len(candles)}/{expected_candles} candles for {symbol}", redis_client)
                else:
                    append_log(f"Skipped {symbol}: only {len(candles)}/{expected_candles} candles (< {min_required_candles})", redis_client)
                status.update({"current_symbol": symbol, "current_symbol_candles": len(candles)})
            except Exception as exc:
                append_log(f"Failed loading {symbol}: {exc}", redis_client)
            status.update({
                "symbols_done": i,
                "progress_pct": round((i / max(1, len(symbols))) * 35.0, 2),
                "current_symbol": symbol,
                "current_symbol_candles": len(candles),
                "message": f"Loading candles: {i}/{len(symbols)} | {symbol} | {len(candles)}/{expected_candles}",
            })
            set_status(status, redis_client)

        if not symbol_candles:
            status.update({"running": False, "state": "error", "completed_at": utc_now_iso(), "message": "No historical candles loaded from OKX."})
            set_status(status, redis_client)
            return

        min_len = min(len(c) for c in symbol_candles.values())
        start_idx = 30
        end_idx = max(start_idx, min_len - 97)  # keep 24h future for outcome
        total_steps = max(1, end_idx - start_idx)
        state = MarketModeState()
        counts = {"records": 0, "normal": 0, "quality_candidates": 0, "execution_candidates": 0, "blocked_by_limits": 0}
        status.update({
            "current_symbol": "",
            "current_symbol_candles": 0,
            "state": "running",
            "running": True,
            "symbols_done": len(symbol_candles),
            "symbols_total": len(symbol_candles),
            "message": f"Replaying {len(symbol_candles)} symbols over {total_steps} candles.",
        })
        set_status(status, redis_client)

        for step_no, idx in enumerate(range(start_idx, end_idx), start=1):
            if stop_requested(redis_client):
                status.update({"running": False, "state": "stopped", "completed_at": utc_now_iso(), "message": "Replay stopped by user."})
                set_status({**status, **counts}, redis_client)
                append_log("Replay stopped by user.", redis_client)
                return

            snapshot = _market_snapshot_at(symbol_candles, idx)
            state = decide_market_mode(snapshot, previous=state)
            btc_change = float(snapshot.btc_change_15m or 0.0)
            slot_counts = {"general": 0, "recovery": 0, "block_exception": 0}
            recovery_remaining = MAX_RECOVERY_TRADES_PER_CYCLE
            batch: list[dict[str, Any]] = []

            # Score all symbols at this candle, then process strongest first like a scan.
            pairs: list[PairCandidate] = []
            for symbol, candles in symbol_candles.items():
                if idx >= len(candles):
                    continue
                pair = _pair_from_candle(symbol, candles, idx, btc_change=btc_change)
                if pair is not None:
                    pairs.append(pair)
            pairs.sort(key=lambda p: (p.score_hint + p.rebound_hint, p.turnover_usdt), reverse=True)

            for pair in pairs:
                signal = build_signal_candidate(pair, state.mode, min_normal_score, min_strong_score)
                if not signal:
                    continue
                exec_result = process_trade_candidate(
                    signal,
                    current_open_positions=slot_counts.get("general", 0),
                    max_open_positions=max_open_positions,
                    min_execution_score=min_execution_score,
                    recovery_slots_remaining=recovery_remaining if state.mode == "RECOVERY_LONG" else None,
                    block_open_positions=slot_counts.get("block_exception", 0),
                    max_block_positions=MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
                    recovery_open_positions=slot_counts.get("recovery", 0),
                    max_recovery_positions=MAX_RECOVERY_TRADES_PER_CYCLE,
                )
                if exec_result.get("status") in {"accepted_preview", "pending_pullback_preview"}:
                    path = str(exec_result.get("path") or "general")
                    if path == "block_exception":
                        slot_counts["block_exception"] += 1
                    elif path == "recovery":
                        slot_counts["recovery"] += 1
                        recovery_remaining = max(0, MAX_RECOVERY_TRADES_PER_CYCLE - slot_counts["recovery"])
                        if state.mode == "RECOVERY_LONG":
                            state = register_recovery_trade(state)
                    else:
                        slot_counts["general"] += 1

                future = symbol_candles.get(pair.symbol, [])[idx + 1 : idx + 97]
                outcome = evaluate_trade_outcome(future, signal.entry, signal.tp1, signal.tp2, signal.sl, horizon_bars=96)
                rec = _record_from_signal(run_id, symbol_candles[pair.symbol][idx].ts, signal, exec_result, snapshot, outcome)
                batch.append(rec)
                counts["records"] += 1
                if rec.get("quality_candidate"):
                    counts["quality_candidates"] += 1
                else:
                    counts["normal"] += 1
                if rec.get("execution_candidate"):
                    counts["execution_candidates"] += 1
                if rec.get("blocked_by_limit"):
                    counts["blocked_by_limits"] += 1

            if batch:
                append_records(batch, redis_client=redis_client)

            if step_no % 8 == 0 or step_no == total_steps:
                progress = 35.0 + (step_no / total_steps) * 65.0
                status.update(counts)
                status.update({
                    "state": "running",
                    "running": True,
                    "progress_pct": round(min(progress, 99.8), 2),
                    "message": f"Replay running: candle {step_no}/{total_steps}, records={counts['records']}",
                })
                set_status(status, redis_client)

        status.update(counts)
        status.update({
            "running": False,
            "state": "completed",
            "progress_pct": 100.0,
            "completed_at": utc_now_iso(),
            "message": f"Historical replay completed. Records: {counts['records']}",
        })
        set_status(status, redis_client)
        append_log(f"Replay completed: {run_id} records={counts['records']}", redis_client)
    except Exception as exc:
        status = get_status(redis_client)
        status.update({"running": False, "state": "error", "completed_at": utc_now_iso(), "message": f"Replay error: {exc}"})
        set_status(status, redis_client)
        append_log(f"Replay error: {exc}", redis_client)


def start_replay(days: int = 30, symbols_limit: int = 200, timeframe: str = "15m", redis_client: Any | None = None, settings: Any | None = None) -> dict[str, Any]:
    global _WORKER_THREAD
    with _WORKER_LOCK:
        status = get_status(redis_client)
        if bool(status.get("running")):
            return {"ok": False, "message": "Historical replay is already running.", "status": status}
        clear_stop(redis_client)
        _WORKER_THREAD = threading.Thread(
            target=_run_replay_worker,
            kwargs={"days": days, "symbols_limit": symbols_limit, "timeframe": timeframe, "redis_client": redis_client, "settings": settings},
            daemon=True,
            name="historical-replay-runner",
        )
        _WORKER_THREAD.start()
        status.update({
            "running": True,
            "state": "starting",
            "days": int(days),
            "symbols_limit": int(symbols_limit),
            "timeframe": timeframe,
            "started_at": utc_now_iso(),
            "message": "Historical replay runner started in background.",
        })
        status = set_status(status, redis_client)
        return {"ok": True, "message": "Historical replay runner started.", "status": status}
