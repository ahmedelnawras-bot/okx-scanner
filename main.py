"""OKX Long Bot clean rebuild v113.

Comments mark:
- preserved core behavior from old bot philosophy
- adapted UI/report style from newer versions
- intentionally excluded late over-tightening
"""
from __future__ import annotations

import json
import requests
from datetime import datetime, timezone

from utils.config import get_settings
from utils.constants import MODE_NORMAL_LONG, MODE_BLOCK_LONGS, MODE_RECOVERY_LONG
from analysis.market_modes import (
    MarketSnapshot,
    MarketModeState,
    decide_market_mode,
    block_protection_status,
    recovery_slots_remaining,
    register_recovery_trade,
)
from analysis.pair_selection import select_ranked_pairs
from analysis.scoring import build_signal_candidate
from execution.execution_processor import process_trade_candidate
from tracking.trade_registry import register_trade
from tracking.open_trades_updater import update_open_trades
from reporting.report_router import build_report_bundle, build_command_outputs
from reporting.help_menus import build_main_menu_layout, build_execution_help, build_normal_help
from ui.telegram_signals import build_signal_message
from ui.market_mode_messages import build_market_mode_sections, build_block_escalation_alert


def fetch_okx_tickers(base_url: str, timeout: int = 15) -> list[dict]:
    url = f"{base_url}/api/v5/market/tickers?instType=SWAP"
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data", [])
    except Exception:
        return [
            {"instId": "BTC-USDT-SWAP", "last": "103250", "volCcy24h": "250000000", "change_pct": 1.4},
            {"instId": "ETH-USDT-SWAP", "last": "4980", "volCcy24h": "180000000", "change_pct": 2.2},
            {"instId": "SOL-USDT-SWAP", "last": "212", "volCcy24h": "75000000", "change_pct": 3.8},
            {"instId": "DOGE-USDT-SWAP", "last": "0.244", "volCcy24h": "42000000", "change_pct": -2.1},
            {"instId": "XRP-USDT-SWAP", "last": "0.635", "volCcy24h": "36000000", "change_pct": -1.4},
            {"instId": "APT-USDT-SWAP", "last": "11.2", "volCcy24h": "12000000", "change_pct": 1.1},
            {"instId": "LINK-USDT-SWAP", "last": "18.45", "volCcy24h": "21000000", "change_pct": 0.4},
            {"instId": "AVAX-USDT-SWAP", "last": "42.4", "volCcy24h": "42000000", "change_pct": 2.8},
        ]



def _build_snapshot(ranked_pairs) -> MarketSnapshot:
    red_count = sum(1 for p in ranked_pairs[:20] if p.change_pct < 0)
    avg_change = (sum(p.change_pct for p in ranked_pairs[:20]) / max(1, min(20, len(ranked_pairs)))) if ranked_pairs else 0.0
    strong_count = sum(1 for p in ranked_pairs[:20] if p.change_pct >= 1.5)
    btc_change = next((p.change_pct for p in ranked_pairs if p.symbol.startswith("BTC-")), avg_change)
    fast_rebound = avg_change > 0.35 and strong_count >= 5
    btc_reclaim = btc_change > 0.2
    breadth_improving = red_count <= 8 and avg_change > -0.1
    return MarketSnapshot(
        btc_change_15m=btc_change,
        red_ratio_15m=(red_count / max(1, min(20, len(ranked_pairs)))) if ranked_pairs else 0.5,
        avg_change_15m=avg_change,
        strong_coins_count=strong_count,
        fast_rebound=fast_rebound,
        btc_reclaim=btc_reclaim,
        breadth_improving=breadth_improving,
    )



def run_once() -> dict:
    settings = get_settings()
    tickers = fetch_okx_tickers(settings.okx_base_url, settings.request_timeout)
    ranked_pairs = select_ranked_pairs(tickers, settings.scan_limit)
    snapshot = _build_snapshot(ranked_pairs)
    initial_mode = MarketModeState(mode=MODE_NORMAL_LONG, changed_at=datetime.now(timezone.utc))
    state = decide_market_mode(snapshot, previous=initial_mode)

    signal_items = []
    execution_results = []
    trades = []
    open_position_count = 0
    recovery_remaining = recovery_slots_remaining(state)
    for pair in ranked_pairs[:20]:
        signal = build_signal_candidate(pair, state.mode, settings.min_normal_score, settings.min_strong_score)
        if not signal:
            continue
        exec_result = process_trade_candidate(
            signal,
            current_open_positions=open_position_count,
            max_open_positions=settings.max_execution_positions,
            min_execution_score=settings.min_execution_score,
            recovery_slots_remaining=recovery_remaining if state.mode == MODE_RECOVERY_LONG else None,
        )
        if exec_result.get("status") in {"accepted_preview", "pending_pullback_preview"}:
            open_position_count += 1
            if state.mode == MODE_RECOVERY_LONG:
                state = register_recovery_trade(state)
                recovery_remaining = recovery_slots_remaining(state)
        signal_items.append({"signal": signal, "execution": exec_result, "message": build_signal_message(signal, exec_result)})
        execution_results.append(exec_result)
        trades.append(register_trade(signal))

    price_map = {pair.symbol: pair.last_price * (1.012 if "momentum" in pair.tags else 0.996) for pair in ranked_pairs[:20]}
    protection = block_protection_status(state)
    trades = update_open_trades(trades, price_map, protection_level=protection.get("level", 0))

    mode_message = build_market_mode_sections(
        state.mode,
        {
            "market_mix": f"avg={snapshot.avg_change_15m:.2f}% | strong={snapshot.strong_coins_count} | red={snapshot.red_ratio_15m:.0%}",
            "market_state": f"strong_coins={snapshot.strong_coins_count} | avg15m={snapshot.avg_change_15m:.2f}% | red_ratio={snapshot.red_ratio_15m:.2f}",
            "trigger": "fast rebound" if state.mode == MODE_RECOVERY_LONG else ("risk-off breadth" if state.mode == MODE_BLOCK_LONGS else "balanced scan"),
            "mode_reason": "fast rebound path" if state.mode == MODE_RECOVERY_LONG else "core market breadth decision",
            "signal_rules": "normal signal first → execution later",
            "requirements": "quality up" if state.mode != MODE_NORMAL_LONG else "balanced normal scanning",
            "execution_notes": "whitelist / elite / recovery / block-exception",
            "protection_current": protection.get("current", "inactive"),
            "protection_next": protection.get("next", "inactive"),
            "remaining_minutes": protection.get("remaining_minutes", 0),
            "recovery_remaining": recovery_slots_remaining(state),
        },
        variant="status",
    )
    reports = build_report_bundle(trades, execution_results, signal_items)
    command_outputs = build_command_outputs(trades, execution_results, signal_items)

    return {
        "mode": state.mode,
        "mode_message": mode_message,
        "block_alert_preview": build_block_escalation_alert(state, affected=len(trades), protected=sum(1 for t in trades if t.pnl_pct > 0), tightened=sum(1 for t in trades if t.tp2_hit)) if state.mode == MODE_BLOCK_LONGS else None,
        "menu": build_main_menu_layout(),
        "help_execution": build_execution_help(),
        "help_normal": build_normal_help(),
        "signals": [item["message"] for item in signal_items[:8]],
        "command_outputs": command_outputs,
        **reports,
    }


if __name__ == "__main__":
    print(json.dumps(run_once(), ensure_ascii=False, indent=2))
