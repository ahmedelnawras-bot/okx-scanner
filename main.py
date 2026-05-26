"""OKX Long Bot clean rebuild v125 mode-guard/report-style worker.

Preserved design:
- main.py orchestrates only
- normal signal first, execution decision second
- Telegram/OKX adapters are isolated from core analysis
- OKX orders are blocked from live trading unless explicitly enabled

Phase 1 fixes applied:
- FIX 1: Variable shadowing (result → doc_result) في 4 أماكن
- FIX 2: State mutation في run_once (scan_mode snapshot قبل اللوب)
"""
from __future__ import annotations

import json
import re
import threading
import time
import traceback
import requests
from datetime import datetime, timezone, timedelta

from utils.config import get_settings, Settings
from utils.constants import MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY, MODE_BLOCK_LONGS, MODE_RECOVERY_LONG, MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE, MAX_RECOVERY_TRADES_PER_CYCLE
from analysis.market_modes import (
    MarketSnapshot,
    MarketModeState,
    decide_market_mode,
    block_protection_status,
    recovery_slots_remaining,
    register_recovery_trade,
)
from analysis.pair_selection import select_ranked_pairs
from analysis.market_guard import build_market_guard_snapshot, fetch_okx_candles
from analysis.scoring import build_signal_candidate
from execution.execution_processor import process_trade_candidate
from execution.okx_trade_client import OKXTradeClient
try:
    from risk import risk_manager as risk_manager_module
except Exception:
    risk_manager_module = None
from risk.portfolio_state import build_portfolio_state_from_trades
from risk.drawdown_monitor import evaluate_drawdown, build_drawdown_report
from tracking.trade_registry import register_trade
from tracking.open_trades_updater import update_open_trades
from tracking.persistence import RedisTradeStore, trade_to_dict, trade_from_dict
from reporting.report_router import build_report_bundle, build_command_outputs
from reporting.report_simulation import build_simulation_command_outputs as build_simulation_report_command_outputs
from reporting.help_menus import (
    build_main_menu_layout,
    build_main_inline_keyboard,
    build_execution_help,
    build_normal_help,
    build_master_help,
    build_okx_control_help,
    build_admin_help,
    build_diagnostics_help,
    build_diagnostics_commands_help,
)
try:
    from ui.telegram_signals import (
        build_signal_message,
        build_signal_buttons,
        build_track_message,
        build_execution_confirmation_message,
        build_execution_failure_message,
    )
except ImportError:
    from ui.telegram_signals import build_signal_message, build_signal_buttons, build_track_message

    def build_execution_confirmation_message(signal, execution_result=None, order_result=None, trade=None) -> str:
        order_result = order_result or {}
        entry = (order_result or {}).get("entry") or {}
        return "\n".join([
            "✅ OKX EXECUTION CONFIRMED",
            f"💎 {getattr(signal, 'symbol', '-')}",
            f"• Entry Order ID: {entry.get('order_id') or '-'}",
            f"• SL Attached: {'YES' if (order_result or {}).get('sl_attached') else 'NO'}",
        ])

    def build_execution_failure_message(signal, execution_result=None, order_result=None) -> str:
        order_result = order_result or {}
        entry = (order_result or {}).get("entry") or {}
        return "\n".join([
            "⚠️ OKX EXECUTION FAILED",
            f"💎 {getattr(signal, 'symbol', '-')}",
            f"• Reason: {entry.get('reason') or order_result.get('reason') or 'okx_execution_failed'}",
        ])
from ui.market_mode_messages import build_market_mode_sections, build_block_escalation_alert

BLOCK_REMINDER_THRESHOLDS = [(5, 1), (10, 2), (15, 3)]
GENERAL_MODE_REMINDER_MINUTES = 30

# Symbol-level duplicate suppression before live.
# Keep actual execution alerts visible, but stop repeating the same coin
# every scan when nothing materially changed.
SYMBOL_OBSERVATION_DEDUP_TTL_SECONDS = 45 * 60
SYMBOL_PULLBACK_DEDUP_TTL_SECONDS = 60 * 60
SYMBOL_EXECUTION_DEDUP_TTL_SECONDS = 2 * 60 * 60

# Telegram send pacing.
# This only spaces Telegram messages after decisions are already made.
# It does not delay process_trade_candidate, OKX execution, slots, or simulation tracking.
TELEGRAM_SEND_GAP_SECONDS = 0.65
TELEGRAM_EXECUTION_SEND_GAP_SECONDS = 0.35
TELEGRAM_NORMAL_SEND_GAP_SECONDS = 0.85
TELEGRAM_COMMAND_POLL_SLEEP_SECONDS = 2.0


# Simulation Trading Mode
# Mirror of trading mode execution decisions, but with internal virtual execution.
SIMULATION_START_BALANCE_USDT = 1000.0
SIMULATION_REDIS_PREFIX = "okx:longbot:simulation:v1"
SIMULATION_OPEN_SET = f"{SIMULATION_REDIS_PREFIX}:trades:open"
SIMULATION_HISTORY_SET = f"{SIMULATION_REDIS_PREFIX}:trades:history"
SIMULATION_EXEC_CHECKS_LIST = f"{SIMULATION_REDIS_PREFIX}:execution:checks"
SIMULATION_DAILY_BALANCE_HASH = f"{SIMULATION_REDIS_PREFIX}:daily_balance"
SIMULATION_BALANCE_STATE_KEY = f"{SIMULATION_REDIS_PREFIX}:wallet:state"
SIMULATION_ALLOCATION_PCT = 24.0


# Loss Streak Guard: pause new execution after repeated SL hits before TP1.
LOSS_STREAK_NO_TP1_LIMIT = 5
LOSS_STREAK_COOLDOWN_MINUTES = 120

from services.telegram_sender import TelegramSender
from analytics.gate_simulation import build_gate_sim_all_artifact, build_gate_sim_all_report, build_gate_sim_artifact, build_gate_sim_report, build_mode_coverage_report, build_score_calibration_report
from analytics.technical_dataset import (
    append_many_signal_snapshots,
    build_signal_snapshot,
    build_technical_dataset_export,
    build_technical_dataset_export_file,
    build_technical_dataset_status,
    build_clear_snapshot_result,
    build_gate_suggestions_report,
    is_snapshot_enabled,
    set_snapshot_enabled,
)
from reporting.report_technical_dataset import build_historical_report, build_technical_dataset_help
from historical_replay.reports import (
    build_compare_live_vs_replay_report,
    build_historical_replay_help,
    build_replay_clear_report,
    build_replay_export_file,
    build_replay_export_report,
    build_replay_start_report,
    build_replay_status_report,
    build_replay_stop_report,
    build_replay_summary_report,
)


def _snapshot_redis_client(trade_store=None):
    if trade_store and getattr(trade_store, "enabled", False):
        return getattr(trade_store, "client", None)
    return None


# ✅ FIX Phase 3: threading.Lock بدل bool flag — thread-safe
_GATE_SIM_LOCK = threading.Lock()


def _send_gate_sim_artifact(sender: TelegramSender, gate: str, settings: Settings, trade_store: RedisTradeStore | None = None) -> None:
    # ✅ FIX: acquire مش بيبلوك — لو مش free بيرجع False فوراً
    if not _GATE_SIM_LOCK.acquire(blocking=False):
        _send_text(sender, "⏳ Gate Simulation شغال بالفعل. استنى النتيجة الحالية قبل تشغيل أمر جديد.")
        return
    try:
        _send_text(sender, f"⏳ جاري تحليل /gate_sim_{gate} على replay + live snapshots... قد يستغرق عدة دقائق مع 90d.")
        artifact = build_gate_sim_artifact(gate, settings, redis_client=_snapshot_redis_client(trade_store))
        _send_text(sender, artifact.get("text") or "⚠️ Gate simulation failed.")
        if artifact.get("ok") and artifact.get("path"):
            doc_result = sender.send_document(str(artifact.get("path")), caption=str(artifact.get("caption") or "Gate Simulation JSON"))
            if not doc_result.get("ok"):
                _send_text(sender, "⚠️ فشل إرسال ملف JSON. الملف جاهز على السيرفر:\n" + str(artifact.get("path")) + "\nError: " + str(doc_result.get("error") or doc_result))
    finally:
        _GATE_SIM_LOCK.release()


def fetch_okx_tickers(base_url: str, timeout: int = 15, offline_test_mode: bool = False) -> list[dict]:
    url = f"{base_url}/api/v5/market/tickers?instType=SWAP"
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data", [])
    except Exception as exc:
        if not offline_test_mode:
            print(f"⚠️ OKX tickers fetch failed; live fake fallback disabled: {exc}", flush=True)
            return []
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


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default



def _extract_okx_reference_balance_usdt(balance_response: dict | None) -> float:
    if not isinstance(balance_response, dict):
        return 0.0

    data = balance_response.get("data") or []
    if not isinstance(data, list):
        data = []

    # Prefer account-level totals when available.
    for account in data:
        if not isinstance(account, dict):
            continue
        for key in ("totalEq", "adjEq", "availEq"):
            value = _safe_float(account.get(key), 0.0)
            if value > 0:
                return value

    # Fallback: sum stable-coin details when totals are unavailable.
    total = 0.0
    for account in data:
        if not isinstance(account, dict):
            continue
        for detail in account.get("details") or []:
            if not isinstance(detail, dict):
                continue
            ccy = str(detail.get("ccy") or "").upper()
            if ccy not in {"USDT", "USDC"}:
                continue
            value = 0.0
            for key in ("eqUsd", "eq", "cashBal", "availEq"):
                value = _safe_float(detail.get(key), 0.0)
                if value > 0:
                    break
            total += max(0.0, value)
    return total


def _risk_sizing_constants(settings: Settings) -> tuple[float, int]:
    allocation_pct = 24.0
    slot_count = max(1, int(getattr(settings, "max_execution_positions", 7) or 7))

    if risk_manager_module is not None:
        allocation_pct = _safe_float(getattr(risk_manager_module, "max_portion_pct", allocation_pct), allocation_pct)
        slot_count = max(
            1,
            int(getattr(risk_manager_module, "max_positions_total_normal_strong", slot_count) or slot_count),
        )

    return allocation_pct, slot_count


def _compute_margin_from_reference(reference_balance_usdt: float, settings: Settings) -> float:
    allocation_pct, slot_count = _risk_sizing_constants(settings)
    if reference_balance_usdt <= 0 or slot_count <= 0:
        return 0.0
    total_allocation = float(reference_balance_usdt) * (allocation_pct / 100.0)
    return total_allocation / float(slot_count)


def _snapshot_risk_manager_state(settings: Settings) -> dict:
    if risk_manager_module is None:
        return {}

    reference_balance = _safe_float(getattr(risk_manager_module, "reference_portfolio", 0.0), 0.0)
    position_margin = _safe_float(getattr(risk_manager_module, "position_size", 0.0), 0.0)
    position_pct = _safe_float(getattr(risk_manager_module, "position_pct", 0.0), 0.0)

    if reference_balance > 0 and position_margin > 0:
        return {
            "source": "risk_manager",
            "reference_balance_usdt": reference_balance,
            "margin_usdt": position_margin,
            "position_pct": position_pct,
        }

    fallback_margin = _compute_margin_from_reference(reference_balance, settings)
    if reference_balance > 0 and fallback_margin > 0:
        return {
            "source": "risk_manager_balance_only",
            "reference_balance_usdt": reference_balance,
            "margin_usdt": fallback_margin,
            "position_pct": (fallback_margin / reference_balance) * 100.0 if reference_balance > 0 else 0.0,
        }

    return {}


def _resolve_entry_margin_plan(
    okx_client: OKXTradeClient,
    settings: Settings,
) -> dict:
    fallback_margin = max(_safe_float(getattr(settings, "paper_margin_usdt", 35.0), 35.0), 0.0) or 35.0
    fallback_plan = {
        "source": "settings.paper_margin_usdt",
        "reference_balance_usdt": 0.0,
        "margin_usdt": fallback_margin,
        "position_pct": 0.0,
        "reason": "fallback_static_margin",
    }

    balance_response = None
    if okx_client is not None and getattr(okx_client, "configured", False):
        try:
            balance_response = okx_client.get_balance()
        except Exception:
            balance_response = None

    okx_reference_balance = _extract_okx_reference_balance_usdt(balance_response if isinstance(balance_response, dict) else None)
    okx_margin = _compute_margin_from_reference(okx_reference_balance, settings)
    if okx_reference_balance > 0 and okx_margin > 0:
        return {
            "source": "okx_balance",
            "reference_balance_usdt": okx_reference_balance,
            "margin_usdt": okx_margin,
            "position_pct": (okx_margin / okx_reference_balance) * 100.0 if okx_reference_balance > 0 else 0.0,
            "reason": "daily_reference_from_okx_balance",
        }

    live_okx_mode = bool(
        okx_client is not None
        and getattr(okx_client, "configured", False)
        and not bool(getattr(settings, "okx_simulated", True))
    )
    if live_okx_mode:
        return {
            "source": "okx_balance",
            "reference_balance_usdt": 0.0,
            "margin_usdt": 0.0,
            "position_pct": 0.0,
            "reason": "live_okx_balance_zero_or_unavailable",
            "balance_fetch_msg": str((balance_response or {}).get("msg") or ""),
        }

    risk_snapshot = _snapshot_risk_manager_state(settings)
    if risk_snapshot.get("margin_usdt", 0.0):
        risk_snapshot.setdefault("reason", "risk_manager_reference")
        return risk_snapshot

    if isinstance(balance_response, dict):
        fallback_plan["balance_fetch_msg"] = str(balance_response.get("msg") or "")

    return fallback_plan


def _resolve_portfolio_state_inputs(
    okx_client: OKXTradeClient | None,
    settings: Settings,
) -> dict:
    sizing = _resolve_entry_margin_plan(okx_client, settings)
    reference_balance = _safe_float((sizing or {}).get("reference_balance_usdt"), 0.0)
    margin_per_trade = _safe_float((sizing or {}).get("margin_usdt"), 0.0)

    live_okx_mode = bool(
        okx_client is not None
        and getattr(okx_client, "configured", False)
        and not bool(getattr(settings, "okx_simulated", True))
    )

    if live_okx_mode and reference_balance <= 0:
        margin_per_trade = 0.0
    elif margin_per_trade <= 0:
        margin_per_trade = max(_safe_float(getattr(settings, "paper_margin_usdt", 35.0), 35.0), 0.0) or 35.0

    if reference_balance <= 0 and not live_okx_mode:
        allocation_pct, slot_count = _risk_sizing_constants(settings)
        if allocation_pct > 0 and slot_count > 0:
            reference_balance = margin_per_trade * float(slot_count) / (allocation_pct / 100.0)

    reference_balance = max(reference_balance, 0.0)
    leverage = max(1, int(getattr(settings, "default_leverage", 1) or 1))

    return {
        "reference_portfolio": reference_balance,
        "start_of_day_balance": reference_balance,
        "margin_per_trade": margin_per_trade,
        "leverage": leverage,
    }


def _execution_report_balance_kwargs(portfolio_state_inputs: dict | None = None) -> dict:
    """Use real execution wallet context in execution reports.

    Simulation reports keep their own 1000 USDT virtual wallet. Execution
    reports should reflect the OKX-derived reference balance and planned
    margin per trade from _resolve_portfolio_state_inputs().
    """
    inputs = dict(portfolio_state_inputs or {})
    reference_balance = _safe_float(inputs.get("reference_portfolio"), 0.0)
    margin_per_trade = _safe_float(inputs.get("margin_per_trade"), 0.0)

    return {
        "execution_starting_balance": max(0.0, reference_balance),
        "execution_margin_per_trade": margin_per_trade if margin_per_trade > 0 else 0.0,
    }


def _fmt_money(value: object) -> str:
    number = _safe_float(value, 0.0)
    return f"{number:.2f}" if abs(number) >= 1 else f"{number:.4f}"


def _sim_code(value: object) -> str:
    return f"<code>{value}</code>"


def _sim_money(value: object, suffix: str = "USDT", signed: bool = False) -> str:
    number = _safe_float(value, 0.0)
    sign = "+" if signed and number >= 0 else ""
    return _sim_code(f"{sign}{number:,.2f} {suffix}".strip())


def _sim_pct(value: object, signed: bool = False) -> str:
    number = _safe_float(value, 0.0)
    sign = "+" if signed and number >= 0 else ""
    return _sim_code(f"{sign}{number:.2f}%")


def _sim_metric(label: str, value: object, icon: str = "•") -> str:
    return f"{icon} {label}\n{_sim_code(value)}"

def _build_live_price_map(raw_tickers: list[dict], fallback_pairs=None) -> dict[str, float]:
    price_map: dict[str, float] = {}
    for raw in raw_tickers or []:
        symbol = str(raw.get("instId") or raw.get("symbol") or "")
        price = _safe_float(raw.get("last") or raw.get("lastPrice"))
        if symbol and price > 0:
            price_map[symbol] = price
    for pair in fallback_pairs or []:
        symbol = str(getattr(pair, "symbol", "") or "")
        price = _safe_float(getattr(pair, "last_price", 0.0))
        if symbol and price > 0 and symbol not in price_map:
            price_map[symbol] = price
    return price_map




def _build_price_action_candles_for_pair(pair, settings: Settings, bar: str = "15m", limit: int = 10) -> list[dict]:
    """Fetch recent closed candles for the Price Action Evidence layer.

    Surgical note:
    - This helper only attaches observational candle data to the pair.
    - It does not change scoring, modes, thresholds, or execution decisions.
    - OKX returns latest candle first; the latest row may still be forming,
      so we prefer closed candles from index 1 onward and return chronological order.
    """
    symbol = str(getattr(pair, "symbol", "") or "")
    if not symbol:
        return []

    try:
        rows = fetch_okx_candles(
            settings.okx_base_url,
            symbol,
            bar=bar,
            limit=limit,
            timeout=settings.request_timeout,
        )
    except Exception:
        return []

    if not isinstance(rows, list) or not rows:
        return []

    closed_rows = rows[1:] if len(rows) > 1 else rows
    candles: list[dict] = []

    for row in reversed(closed_rows):
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        try:
            candles.append({
                "timestamp": row[0],
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
            })
        except Exception:
            continue

    return candles



def _build_4h_resistance_context_for_pair(pair, settings: Settings, bar: str = "4H", limit: int = 30) -> dict:
    """Build lightweight 4H resistance context for Market Context Layer.

    Observational only:
    - Adds context to the pair before scoring.
    - Does not place orders.
    - Does not mutate market mode.
    """
    symbol = str(getattr(pair, "symbol", "") or "")
    last_price = _safe_float(getattr(pair, "last_price", 0.0), 0.0)

    if not symbol or last_price <= 0:
        return {
            "status": "unknown",
            "distance_pct": None,
            "resistance": None,
            "reason": "missing_symbol_or_price",
        }

    try:
        rows = fetch_okx_candles(
            settings.okx_base_url,
            symbol,
            bar=bar,
            limit=limit,
            timeout=settings.request_timeout,
        )
    except Exception as exc:
        return {
            "status": "unknown",
            "distance_pct": None,
            "resistance": None,
            "reason": f"fetch_failed:{exc}",
        }

    if not isinstance(rows, list) or len(rows) < 6:
        return {
            "status": "unknown",
            "distance_pct": None,
            "resistance": None,
            "reason": "not_enough_4h_candles",
        }

    highs: list[float] = []
    for row in rows[1:]:  # skip current forming candle
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        high = _safe_float(row[2], 0.0)
        if high > 0:
            highs.append(high)

    if not highs:
        return {
            "status": "unknown",
            "distance_pct": None,
            "resistance": None,
            "reason": "no_valid_highs",
        }

    resistance = max(highs)
    distance_pct = ((resistance - last_price) / last_price) * 100.0

    if distance_pct < 0:
        status = "cleared"
    elif distance_pct <= 0.75:
        status = "very_near"
    elif distance_pct <= 2.00:
        status = "near"
    elif distance_pct <= 4.00:
        status = "watch"
    else:
        status = "clear"

    return {
        "status": status,
        "distance_pct": round(distance_pct, 3),
        "resistance": resistance,
        "reason": f"nearest_4h_high_{status}",
    }


def _build_snapshot(ranked_pairs, settings: Settings) -> MarketSnapshot:
    return build_market_guard_snapshot(
        ranked_pairs,
        base_url=settings.okx_base_url,
        timeout=settings.request_timeout,
        sample_size=50,
        min_valid=20,
        timeframe="15m",
        debug=True,
        verbose=settings.verbose_logs,
    )


def _build_mode_context(state: MarketModeState, snapshot: MarketSnapshot, protection: dict) -> dict:
    avg15m = float(snapshot.avg_change_15m or 0.0)
    red_ratio_pct = float(snapshot.red_ratio_15m or 0.0) * 100.0
    strong_coins = int(snapshot.strong_coins_count or 0)
    hourly_ma5_pressure = bool(getattr(snapshot, "hourly_ma5_pressure", False))
    btc_1h_ma5_gap_pct = float(getattr(snapshot, "btc_1h_ma5_gap_pct", 0.0) or 0.0)
    hourly_ma_guard = "pressure" if hourly_ma5_pressure else "clear"
    return {
        "mode": state.mode,
        "strong_coins": strong_coins,
        "red_ratio": red_ratio_pct,
        "avg15m": avg15m,
        "btc15m": float(snapshot.btc_change_15m or 0.0),
        "hourly_ma5_pressure": hourly_ma5_pressure,
        "btc_1h_close": float(getattr(snapshot, "btc_1h_close", 0.0) or 0.0),
        "btc_1h_ma5": float(getattr(snapshot, "btc_1h_ma5", 0.0) or 0.0),
        "btc_1h_ma5_gap_pct": btc_1h_ma5_gap_pct,
        "hourly_ma_guard": hourly_ma_guard,
        "sample_size": int(getattr(snapshot, "market_guard_valid_count", 0) or getattr(snapshot, "market_guard_sample_size", 200) or 200),
        "market_mix": f"Strong Coins: {strong_coins} | Red Ratio: {red_ratio_pct:.0f}% | Avg 15m Move: {avg15m:.2f}%",
        "market_state": f"strong_coins={strong_coins} | avg15m={avg15m:.2f}% | red_ratio={red_ratio_pct:.0f}% | 1h_ma5={hourly_ma_guard}",
        "trigger": "fast rebound" if state.mode == MODE_RECOVERY_LONG else ("risk-off breadth" if state.mode == MODE_BLOCK_LONGS else "balanced scan"),
        "mode_reason": "fast rebound path" if state.mode == MODE_RECOVERY_LONG else "core market breadth decision",
        "signal_rules": "normal signal first → execution later",
        "requirements": "quality up" if state.mode != MODE_NORMAL_LONG else "balanced normal scanning",
        "execution_notes": "whitelist / elite / recovery / block-exception",
        "protection_current": protection.get("current", "inactive"),
        "protection_next": protection.get("next", "inactive"),
        "remaining_minutes": protection.get("remaining_minutes", 0),
        "recovery_remaining": recovery_slots_remaining(state),
    }


def _build_mode_message(
    state: MarketModeState,
    snapshot: MarketSnapshot,
    protection: dict,
    variant: str = "status",
    reminder_count: int = 1,
    old_mode: str | None = None,
) -> str:
    context = _build_mode_context(state, snapshot, protection)
    if variant == "reminder":
        minutes_in_mode = int((datetime.now(timezone.utc) - state.changed_at).total_seconds() // 60)
        context.update({"reminder_count": reminder_count, "minutes_in_mode": minutes_in_mode})
    if old_mode:
        context["old_mode"] = old_mode
    return build_market_mode_sections(state.mode, context, variant=variant)


def _refresh_mode_outputs(result: dict, state: MarketModeState, snapshot: MarketSnapshot) -> dict:
    protection = block_protection_status(state)
    result["state"] = state
    result["mode"] = state.mode
    result["mode_context"] = _build_mode_context(state, snapshot, protection)
    result["mode_message"] = _build_mode_message(state, snapshot, protection)
    result["mode_transition_message"] = None
    result["block_alert_preview"] = (
        build_block_escalation_alert(
            state,
            affected=len(result.get("trades", [])),
            protected=sum(1 for t in result.get("trades", []) if getattr(t, "pnl_pct", 0) > 0),
            tightened=sum(1 for t in result.get("trades", []) if getattr(t, "tp2_hit", False)),
        )
        if state.mode == MODE_BLOCK_LONGS else None
    )
    return result


def _run_market_mode_guard(
    sender: TelegramSender,
    result: dict,
    settings: Settings,
    state: MarketModeState | None,
    reminder_tracker: dict,
) -> MarketModeState | None:
    if state is None:
        return state
    tickers = fetch_okx_tickers(settings.okx_base_url, settings.request_timeout, settings.offline_test_mode)
    ranked_pairs = select_ranked_pairs(tickers, settings.scan_limit)
    snapshot = _build_snapshot(ranked_pairs, settings)
    previous_mode = state.mode
    guarded_state = decide_market_mode(snapshot, previous=state)
    _refresh_mode_outputs(result, guarded_state, snapshot)
    if guarded_state.mode != previous_mode:
        reminder_tracker.clear()
        transition_message = _build_mode_message(
            guarded_state,
            snapshot,
            block_protection_status(guarded_state),
            variant="transition",
            old_mode=previous_mode,
        )
        result["mode_transition_message"] = transition_message
        _send_text(sender, transition_message)
    else:
        result["mode_transition_message"] = None
    return guarded_state


def prefilter_pair_before_candles(pair, current_mode: str) -> bool:
    try:
        if not pair or float(getattr(pair, "last_price", 0.0) or 0.0) <= 0:
            return False
        turnover = float(getattr(pair, "turnover_usdt", 0.0) or 0.0)
        tags = set(getattr(pair, "tags", []) or [])
        if turnover < 500_000:
            return False
        if current_mode == MODE_BLOCK_LONGS:
            return turnover >= 2_000_000 or bool(tags & {"rs_btc", "breakout", "rebound", "major"})
        return True
    except Exception:
        return False


def _is_trade_closed(trade) -> bool:
    status = str(getattr(trade, "status", "") or "").lower()

    # TP2 can still leave a live runner (20%).
    # So TP2 by itself is NOT a fully closed trade.
    if bool(getattr(trade, "has_open_runner", False)):
        return False

    if bool(getattr(trade, "tp2_hit", False)) and bool(
        getattr(trade, "runner_active", False) or getattr(trade, "protected_runner", False)
    ):
        return False

    return bool(
        (
            getattr(trade, "is_closed", False)
            and not bool(getattr(trade, "tp2_hit", False))
        )
        or status in {
            "closed",
            "stopped",
            "closed_win",
            "closed_loss",
            "breakeven_after_tp1",
            "trailing_hit",
            "expired",
        }
    )


def _is_counted_open_trade(trade) -> bool:
    counted = getattr(trade, "counts_as_active_slot", None)
    if counted is not None:
        return bool(counted)
    if bool(getattr(trade, "tp2_hit", False)):
        return False
    return bool(not _is_trade_closed(trade) and not getattr(trade, "slot_exempt", False))


def _blocks_same_symbol_reentry(trade) -> bool:
    if bool(getattr(trade, "same_symbol_block_exempt", False)):
        return False
    blocks = getattr(trade, "blocks_same_symbol_reentry", None)
    if blocks is not None:
        return bool(blocks)
    if bool(getattr(trade, "tp2_hit", False)):
        return False
    return _is_counted_open_trade(trade)



def _trade_closed_at(trade) -> datetime:
    value = getattr(trade, "closed_at", None) or getattr(trade, "updated_at", None) or getattr(trade, "opened_at", None)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _is_execution_closed_trade(trade) -> bool:
    if not getattr(trade, "execution_trade", False):
        return False
    return bool(_is_trade_closed(trade) or getattr(trade, "closed_at", None))


def _is_sl_before_tp1_loss(trade) -> bool:
    status = str(getattr(trade, "status", "") or "").strip().lower()
    return bool(status == "closed_loss" and not bool(getattr(trade, "tp1_hit", False)))


def _build_loss_streak_guard(trades, now: datetime | None = None) -> dict:
    """Return execution pause state after consecutive SL losses before TP1.

    The streak counts only bot execution trades that closed by SL before TP1.
    Any closed bot execution trade that reached TP1 resets the streak.
    """
    now = now or datetime.now(timezone.utc)
    closed_trades = sorted(
        [trade for trade in (trades or []) if _is_execution_closed_trade(trade)],
        key=_trade_closed_at,
    )

    streak = 0
    streak_symbols: list[str] = []
    last_loss_at: datetime | None = None

    for trade in closed_trades:
        if bool(getattr(trade, "tp1_hit", False)):
            streak = 0
            streak_symbols = []
            last_loss_at = None
            continue

        if _is_sl_before_tp1_loss(trade):
            streak += 1
            streak_symbols.append(str(getattr(trade, "symbol", "") or "-"))
            last_loss_at = _trade_closed_at(trade)
        else:
            # Non-SL/non-TP1 closure breaks a pure SL streak.
            streak = 0
            streak_symbols = []
            last_loss_at = None

    cooldown_until = None
    active = False
    remaining_minutes = 0
    if streak >= LOSS_STREAK_NO_TP1_LIMIT and last_loss_at is not None:
        cooldown_until = last_loss_at + timedelta(minutes=LOSS_STREAK_COOLDOWN_MINUTES)
        active = now < cooldown_until
        if active:
            remaining_minutes = max(1, int((cooldown_until - now).total_seconds() // 60))

    return {
        "active": active,
        "streak": streak,
        "limit": LOSS_STREAK_NO_TP1_LIMIT,
        "cooldown_minutes": LOSS_STREAK_COOLDOWN_MINUTES,
        "remaining_minutes": remaining_minutes,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until else "",
        "last_loss_at": last_loss_at.isoformat() if last_loss_at else "",
        "symbols": streak_symbols[-LOSS_STREAK_NO_TP1_LIMIT:],
        "reason": "loss_streak_no_tp1_guard",
    }


def _loss_streak_rejection(guard: dict) -> dict:
    return {
        "status": "rejected_loss_streak_guard",
        "reason": guard.get("reason") or "loss_streak_no_tp1_guard",
        "path": "",
        "slot_scope": "loss_streak_guard",
        "loss_streak": int(guard.get("streak", 0) or 0),
        "loss_streak_limit": int(guard.get("limit", LOSS_STREAK_NO_TP1_LIMIT) or LOSS_STREAK_NO_TP1_LIMIT),
        "cooldown_minutes": int(guard.get("cooldown_minutes", LOSS_STREAK_COOLDOWN_MINUTES) or LOSS_STREAK_COOLDOWN_MINUTES),
        "cooldown_remaining_minutes": int(guard.get("remaining_minutes", 0) or 0),
        "cooldown_until": guard.get("cooldown_until", ""),
        "message_ar": f"تم إيقاف التنفيذ مؤقتًا بعد {int(guard.get('streak', 0) or 0)} ضربات SL متتالية بدون TP1.",
    }


def _trade_slot_path(trade) -> str:
    path = str(getattr(trade, "execution_path", "") or "")
    if path == "block_exception":
        return "block_exception"
    if path == "recovery":
        return "recovery"
    return "general"


def _execution_slot_counts(trades) -> dict[str, int]:
    counts = {"general": 0, "block_exception": 0, "recovery": 0}
    for trade in trades or []:
        if not getattr(trade, "execution_trade", False):
            continue
        if not _is_counted_open_trade(trade):
            continue
        counts[_trade_slot_path(trade)] = counts.get(_trade_slot_path(trade), 0) + 1
    return counts


def _has_active_same_symbol(trades, candidate_trade) -> bool:
    symbol = getattr(candidate_trade, "symbol", "")

    # Same-symbol blocking should be symbol-wide.
    # Do not allow duplicates just because tracking bucket or execution path changed.
    for trade in trades or []:
        if getattr(trade, "symbol", "") != symbol:
            continue
        if _blocks_same_symbol_reentry(trade):
            return True
    return False


def _is_simulation_mode(settings: Settings) -> bool:
    return _get_signal_delivery_mode(settings) == "simulation"


def _simulation_trade_key(trade_id: str) -> str:
    return f"{SIMULATION_REDIS_PREFIX}:trade:{trade_id}"



def _simulation_today_key(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.date().isoformat()


def _simulation_margin_usdt(balance: float, settings: Settings | None = None) -> float:
    """Simulation sizing must mirror execution sizing exactly.

    The only intended difference:
    - Execution reference balance comes from OKX.
    - Simulation reference balance comes from the virtual simulation wallet.

    Therefore we reuse _risk_sizing_constants() so any future change in
    execution allocation/slot rules automatically affects simulation too.
    """
    if settings is not None:
        allocation_pct, slot_count = _risk_sizing_constants(settings)
    else:
        allocation_pct = SIMULATION_ALLOCATION_PCT
        slot_count = 7
        if risk_manager_module is not None:
            allocation_pct = _safe_float(getattr(risk_manager_module, "max_portion_pct", allocation_pct), allocation_pct)
            slot_count = max(1, int(getattr(risk_manager_module, "max_positions_total_normal_strong", slot_count) or slot_count))

    if float(balance or 0.0) <= 0 or int(slot_count or 0) <= 0:
        return 0.0

    return max(0.0, float(balance or 0.0) * (float(allocation_pct or 0.0) / 100.0) / float(slot_count))


def _simulation_equity_from_trades(
    sim_trades: list,
    start_balance: float = SIMULATION_START_BALANCE_USDT,
) -> float:
    equity = float(start_balance or SIMULATION_START_BALANCE_USDT)
    for trade in sim_trades or []:
        margin = _safe_float(getattr(trade, "simulation_margin_usdt", 0.0), 0.0)
        if margin <= 0:
            margin = _simulation_margin_usdt(float(start_balance or SIMULATION_START_BALANCE_USDT), None)
        equity += _money_from_pct(_trade_effective_pnl_pct(trade), margin=margin)
    return equity


def _load_simulation_daily_log(trade_store: RedisTradeStore | None = None) -> list[dict]:
    if not trade_store or not getattr(trade_store, "enabled", False) or not getattr(trade_store, "client", None):
        return []
    try:
        raw = trade_store.client.hgetall(SIMULATION_DAILY_BALANCE_HASH) or {}
        rows = []
        for day, payload in raw.items():
            try:
                item = json.loads(payload)
            except Exception:
                continue
            item.setdefault("date", str(day))
            rows.append(item)
        return sorted(rows, key=lambda x: str(x.get("date", "")))
    except Exception as exc:
        print(f"⚠️ Simulation daily log load failed: {exc}", flush=True)
        return []


def _ensure_simulation_daily_log(
    sim_trades: list,
    trade_store: RedisTradeStore | None = None,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> dict:
    """Persist a daily virtual balance row for Simulation only.

    It behaves like a paper account:
    - First day starts from 1000 USDT.
    - Next day starts from previous day's ending/current equity.
    - No reset means the sequence continues across deploys/restarts.
    """
    now = now or datetime.now(timezone.utc)
    today = _simulation_today_key(now)

    wallet = _build_simulation_wallet_snapshot(sim_trades)
    current_equity = float(wallet.get("equity", SIMULATION_START_BALANCE_USDT) or SIMULATION_START_BALANCE_USDT)

    rows = _load_simulation_daily_log(trade_store)
    previous_rows = [r for r in rows if str(r.get("date", "")) < today]
    previous_equity = None
    if previous_rows:
        last = previous_rows[-1]
        previous_equity = _safe_float(last.get("end_balance") or last.get("current_balance") or last.get("equity"), 0.0)

    if previous_equity and previous_equity > 0:
        start_balance = previous_equity
    elif rows and str(rows[-1].get("date", "")) == today:
        start_balance = _safe_float(rows[-1].get("start_balance"), SIMULATION_START_BALANCE_USDT)
    else:
        start_balance = SIMULATION_START_BALANCE_USDT

    realized = _safe_float(wallet.get("realized"), 0.0)
    floating = _safe_float(wallet.get("floating"), 0.0)
    row = {
        "date": today,
        "start_balance": start_balance,
        "current_balance": current_equity,
        "end_balance": current_equity,
        "realized": realized,
        "floating": floating,
        "open_trades": int(wallet.get("open_count", 0) or 0),
        "closed_trades": int(wallet.get("closed_count", 0) or 0),
        "margin_per_trade": _simulation_margin_usdt(start_balance, settings),
        "updated_at": now.isoformat(),
    }

    if trade_store and getattr(trade_store, "enabled", False) and getattr(trade_store, "client", None):
        try:
            trade_store.client.hset(SIMULATION_DAILY_BALANCE_HASH, today, json.dumps(row, ensure_ascii=False, default=str))
            trade_store.client.expire(SIMULATION_DAILY_BALANCE_HASH, 180 * 24 * 60 * 60)
            trade_store.client.setex(SIMULATION_BALANCE_STATE_KEY, 180 * 24 * 60 * 60, json.dumps(row, ensure_ascii=False, default=str))
        except Exception as exc:
            print(f"⚠️ Simulation daily log save failed: {exc}", flush=True)

    return row


def _build_simulation_daily_balance_text(trade_store: RedisTradeStore | None = None, limit: int = 10) -> str:
    rows = _load_simulation_daily_log(trade_store)
    if not rows:
        rows = [{
            "date": _simulation_today_key(),
            "start_balance": SIMULATION_START_BALANCE_USDT,
            "current_balance": SIMULATION_START_BALANCE_USDT,
            "realized": 0.0,
            "floating": 0.0,
            "open_trades": 0,
        }]

    selected = rows[-max(1, int(limit or 10)):]
    lines = [
        "📅 Simulation Daily Balance",
        "━━━━━━━━━━━━",
    ]
    for item in selected:
        start = _safe_float(item.get("start_balance"), 0.0)
        current = _safe_float(item.get("current_balance") or item.get("end_balance"), 0.0)
        delta = current - start
        icon = "🟢" if delta >= 0 else "🔴"
        lines.append(
            f"{icon} {item.get('date')} | Start {start:.2f} → {current:.2f} | Δ {delta:+.2f} | Open {int(item.get('open_trades', 0) or 0)}"
        )
    return "\n".join(lines)


def _load_simulation_trades(trade_store: RedisTradeStore | None = None) -> list:
    if not trade_store or not getattr(trade_store, "enabled", False) or not getattr(trade_store, "client", None):
        return []

    trades = []
    try:
        ids = set(trade_store.client.smembers(SIMULATION_OPEN_SET) or []) | set(trade_store.client.smembers(SIMULATION_HISTORY_SET) or [])
        for trade_id in ids:
            raw = trade_store.client.get(_simulation_trade_key(trade_id))
            if not raw:
                continue
            try:
                trade = trade_from_dict(json.loads(raw))
            except Exception:
                trade = None
            if trade:
                setattr(trade, "trade_source", "simulation")
                setattr(trade, "tracking_bucket", "execution")
                setattr(trade, "execution_trade", True)
                if str(getattr(trade, "status", "") or "").lower() not in {"closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit", "expired"}:
                    setattr(trade, "status", str(getattr(trade, "status", "") or "open"))
                trades.append(trade)
    except Exception as exc:
        print(f"⚠️ Simulation load failed: {exc}", flush=True)
    return trades


def _save_simulation_trades(trades: list, trade_store: RedisTradeStore | None = None) -> None:
    if not trade_store or not getattr(trade_store, "enabled", False) or not getattr(trade_store, "client", None):
        return

    try:
        pipe = trade_store.client.pipeline()
        for trade in trades or []:
            trade_id = str(getattr(trade, "trade_id", "") or "")
            if not trade_id:
                continue

            setattr(trade, "trade_source", "simulation")
            setattr(trade, "tracking_bucket", "execution")
            setattr(trade, "execution_trade", True)
            payload = json.dumps(trade_to_dict(trade), ensure_ascii=False, default=str)
            key = _simulation_trade_key(trade_id)

            if _is_trade_closed(trade):
                pipe.setex(key, 90 * 24 * 60 * 60, payload)
                pipe.srem(SIMULATION_OPEN_SET, trade_id)
                pipe.sadd(SIMULATION_HISTORY_SET, trade_id)
                pipe.expire(SIMULATION_HISTORY_SET, 90 * 24 * 60 * 60)
            else:
                pipe.setex(key, 90 * 24 * 60 * 60, payload)
                pipe.sadd(SIMULATION_OPEN_SET, trade_id)
                pipe.srem(SIMULATION_HISTORY_SET, trade_id)

        pipe.execute()
    except Exception as exc:
        print(f"⚠️ Simulation save failed: {exc}", flush=True)


def _append_simulation_execution_checks(execution_results: list[dict], trade_store: RedisTradeStore | None = None, limit: int = 10000) -> None:
    if not trade_store or not getattr(trade_store, "enabled", False) or not getattr(trade_store, "client", None) or not execution_results:
        return

    try:
        now = datetime.now(timezone.utc).isoformat()
        pipe = trade_store.client.pipeline()
        for item in execution_results:
            payload = dict(item or {})
            payload["ts"] = now
            payload["simulation_mode"] = True
            pipe.lpush(SIMULATION_EXEC_CHECKS_LIST, json.dumps(payload, ensure_ascii=False, default=str))
        pipe.ltrim(SIMULATION_EXEC_CHECKS_LIST, 0, max(0, limit - 1))
        pipe.execute()
    except Exception as exc:
        print(f"⚠️ Simulation checks append failed: {exc}", flush=True)


def _load_simulation_execution_checks(trade_store: RedisTradeStore | None = None, limit: int = 500) -> list[dict]:
    if not trade_store or not getattr(trade_store, "enabled", False) or not getattr(trade_store, "client", None):
        return []
    try:
        rows = trade_store.client.lrange(SIMULATION_EXEC_CHECKS_LIST, 0, max(0, int(limit or 500) - 1)) or []
        out = []
        for raw in reversed(rows):
            try:
                out.append(json.loads(raw))
            except Exception:
                continue
        return out
    except Exception as exc:
        print(f"⚠️ Simulation checks load failed: {exc}", flush=True)
        return []



def _trade_effective_pnl_pct(trade) -> float:
    """Safe effective PnL% helper for simulation wallet."""
    try:
        realized = _safe_float(getattr(trade, "realized_pnl_pct", 0.0), 0.0)
        floating = _safe_float(getattr(trade, "floating_pnl_pct", 0.0), 0.0)
        pnl_pct = _safe_float(getattr(trade, "pnl_pct", 0.0), 0.0)

        if abs(realized) > 0 or abs(floating) > 0:
            return realized + floating
        if abs(pnl_pct) > 0:
            return pnl_pct

        entry = _safe_float(getattr(trade, "entry", 0.0), 0.0)
        current = _safe_float(getattr(trade, "current_price", 0.0), 0.0)
        if entry > 0 and current > 0:
            return ((current - entry) / entry) * 100.0
    except Exception:
        pass
    return 0.0


def _money_from_pct(pct: float, margin: float = 35.0) -> float:
    """Convert PnL% to rough USDT impact."""
    try:
        return float(margin or 0.0) * (float(pct or 0.0) / 100.0)
    except Exception:
        return 0.0


def _build_simulation_wallet_snapshot(sim_trades: list, start_balance: float = SIMULATION_START_BALANCE_USDT) -> dict:
    """Build a simple virtual wallet snapshot from simulation trades.

    This uses the same lifecycle PnL fields produced by update_open_trades.
    Each trade is assumed to use the configured paper margin if available,
    falling back to 35 USDT. The wallet itself starts at 1000 USDT.
    """
    open_trades = [t for t in sim_trades or [] if not _is_trade_closed(t)]
    closed_trades = [t for t in sim_trades or [] if _is_trade_closed(t)]

    realized = 0.0
    floating = 0.0
    for trade in sim_trades or []:
        margin = _safe_float(getattr(trade, "simulation_margin_usdt", 0.0), 0.0)
        if margin <= 0:
            margin = _simulation_margin_usdt(float(start_balance or SIMULATION_START_BALANCE_USDT), None)
        pct = _trade_effective_pnl_pct(trade)
        usd = _money_from_pct(pct, margin=margin)
        if _is_trade_closed(trade):
            realized += usd
        else:
            floating += usd

    equity = float(start_balance or SIMULATION_START_BALANCE_USDT) + realized + floating
    return {
        "start_balance": float(start_balance or SIMULATION_START_BALANCE_USDT),
        "equity": equity,
        "realized": realized,
        "floating": floating,
        "open_count": len(open_trades),
        "closed_count": len(closed_trades),
        "total_count": len(sim_trades or []),
    }




def _simulation_protection_label(result: dict | None = None) -> str:
    result = result or {}
    mode = str(result.get("mode") or "").strip()
    if mode == MODE_BLOCK_LONGS:
        return "BLOCKED"
    if mode == MODE_RECOVERY_LONG:
        return "RECOVERY"
    if mode == MODE_STRONG_LONG_ONLY:
        return "STRONG"
    return "NORMAL"


def _format_simulation_equity_curve_rows(rows: list[dict], current_row: dict | None = None, limit: int = 5) -> list[str]:
    """Compact Simulation equity rows using the same simple style as execution reports."""
    merged: list[dict] = []

    for item in rows or []:
        if not isinstance(item, dict):
            continue
        day = str(item.get("date") or "").strip()
        if not day:
            continue
        merged.append(item)

    if isinstance(current_row, dict):
        day = str(current_row.get("date") or "").strip()
        if day:
            merged = [item for item in merged if str(item.get("date") or "") != day]
            merged.append(current_row)

    if not merged:
        merged = [{
            "date": _simulation_today_key(),
            "start_balance": SIMULATION_START_BALANCE_USDT,
            "current_balance": SIMULATION_START_BALANCE_USDT,
            "end_balance": SIMULATION_START_BALANCE_USDT,
        }]

    merged = sorted(merged, key=lambda item: str(item.get("date") or ""))
    selected = merged[-max(1, int(limit or 5)):]

    out: list[str] = []
    for item in selected:
        date = str(item.get("date") or "-")
        start_eq = _safe_float(item.get("start_balance"), SIMULATION_START_BALANCE_USDT)
        end_eq = _safe_float(item.get("end_balance") or item.get("current_balance"), start_eq)
        pnl = end_eq - start_eq
        icon = "🟢" if pnl >= 0 else "🔴"
        out.append(f"• {date} | {start_eq:,.2f} → {end_eq:,.2f} USDT | {icon} {pnl:+,.2f} USDT")

    return out

def _build_simulation_account_summary(result: dict | None = None) -> str:
    """Small Simulation account block.

    Same style as execution reports:
    - short English/LTR metric lines
    - no split Arabic labels vs numbers
    - no extra blank lines
    """
    result = result or {}
    sim_trades = list(result.get("simulation_trades", []) or [])
    wallet = result.get("simulation_wallet") or _build_simulation_wallet_snapshot(sim_trades)
    daily_row = result.get("simulation_daily_balance") or {}

    start_balance = _safe_float(
        daily_row.get("start_balance"),
        _safe_float(wallet.get("start_balance"), SIMULATION_START_BALANCE_USDT),
    )
    current_balance = _safe_float(
        daily_row.get("current_balance") or daily_row.get("end_balance"),
        _safe_float(wallet.get("equity"), start_balance),
    )
    delta = current_balance - start_balance
    growth_pct = ((delta / start_balance) * 100.0) if start_balance else 0.0
    realized = _safe_float(wallet.get("realized"), 0.0)
    floating = _safe_float(wallet.get("floating"), 0.0)
    risk_mode = _simulation_protection_label(result)
    icon = "🟢" if delta >= 0 else "🔴"

    rows = list(result.get("simulation_daily_log", []) or [])
    equity_rows = _format_simulation_equity_curve_rows(rows, daily_row, limit=5)

    return "\n".join([
        "💰 <b>Simulation Daily Balance</b>",
        "━━━━━━━━━━━━",
        f"📍 Start Balance: {start_balance:,.2f} USDT",
        f"💼 Current Balance: {current_balance:,.2f} USDT",
        f"{icon} Daily Net: {delta:+,.2f} USDT | {growth_pct:+.2f}%",
        f"✅ Realized: {realized:+,.2f} USDT",
        f"📊 Floating: {floating:+,.2f} USDT",
        f"🛡 Protection: {risk_mode}",
        "",
        "📈 <b>Simulation Equity Curve</b>",
        *equity_rows,
        "",
    ])

def _clean_orphan_wallet_icon_before_daily_balance(text: str) -> str:
    """Remove orphan wallet emoji line that old reports may leave before Daily Balance.

    Some report bundles render Wallet Impact as:
        💰
        Wallet Impact

    When the Daily Balance block is injected before "Wallet Impact", the standalone
    emoji can remain above Daily Balance. This cleanup removes only that orphan
    wallet/briefcase line when it directly precedes the Daily Balance block.
    """
    lines = str(text or "").splitlines()
    cleaned: list[str] = []

    for idx, line in enumerate(lines):
        stripped = line.strip()
        is_orphan_icon = stripped in {"💰", "💼"}
        next_few = "\n".join(lines[idx + 1: idx + 5])
        if is_orphan_icon and ("💰 Daily Balance" in next_few or "💰 <b>Simulation Daily Balance</b>" in next_few or "Simulation Daily Balance" in next_few):
            continue
        cleaned.append(line)

    value = "\n".join(cleaned)

    # Collapse excessive blank lines around the injected block.
    value = re.sub(r"\n{3,}(💰 (?:<b>)?Simulation Daily Balance(?:</b>)?|💰 Daily Balance)", r"\n\n\1", value)
    value = re.sub(r"(━━━━━━━━━━━━)\n{2,}(📍 بداية اليوم)", r"\1\n\2", value)
    return value.strip()


def _inject_simulation_account_summary(text: str, result: dict | None = None) -> str:
    value = _clean_orphan_wallet_icon_before_daily_balance(str(text or ""))

    # If the block is already present, do not inject again; just cleanup old orphan icon.
    if "💰 Daily Balance" in value and ("📊 Equity Curve" in value or "📈 Equity Curve" in value):
        return _clean_orphan_wallet_icon_before_daily_balance(value)

    block = _build_simulation_account_summary(result)

    # Match the full title first. If we only match "Wallet Impact",
    # an original "💰 Wallet Impact" title or split "💰\\nWallet Impact"
    # can leave a useless standalone icon before Daily Balance.
    wallet_markers = ["💰 Wallet Impact", "💼 Wallet Impact", "Wallet Impact"]
    for marker in wallet_markers:
        idx = value.find(marker)
        if idx >= 0:
            before = value[:idx].rstrip()
            after = value[idx:].lstrip()

            # Defensive cleanup for split-title formats:
            #   💰
            #   Wallet Impact
            before_lines = before.splitlines()
            while before_lines and before_lines[-1].strip() in {"💰", "💼", ""}:
                last = before_lines[-1].strip()
                before_lines.pop()
                if last in {"💰", "💼"}:
                    break
            before = "\n".join(before_lines).rstrip()

            return _clean_orphan_wallet_icon_before_daily_balance(
                before + "\n\n" + block + "\n" + after
            )

    return _clean_orphan_wallet_icon_before_daily_balance(block + "\n" + value)

def _simulation_header(text: str) -> str:
    return "🧪 Simulation Mode\n━━━━━━━━━━━━\n" + str(text or "")


def _simulation_command_aliases_for_execution_command(command_key: str) -> list[str]:
    """Map execution report command names to simulation report command names.

    Examples:
    /report_execution -> /report_simulation
    /report_execution_open_7d -> /report_simulation_open_7d
    /report_execution_profit_analysis_today -> /report_simulation_profit_analysis_today
    """
    command_key = str(command_key or "").strip()
    if not command_key.startswith("/"):
        command_key = "/" + command_key

    aliases: list[str] = []

    if command_key == "/report_execution":
        aliases.append("/report_simulation")
    elif command_key.startswith("/report_execution_"):
        suffix = command_key[len("/report_execution_"):]
        aliases.append("/report_simulation_" + suffix)

    # Also support old generated names just in case.
    if command_key.startswith("/report_"):
        aliases.append(command_key.replace("/report_", "/report_simulation_", 1))

    return list(dict.fromkeys(a for a in aliases if a and a not in {"/report_simulation_execution"}))


def _build_simulation_command_outputs(result: dict) -> dict:
    """Build Simulation reports through reporting/report_simulation.py only.

    Important:
    - Uses simulation_trades / simulation_execution_results only.
    - Does not touch execution reports.
    - Keeps shared report_format.py unchanged.
    """
    wallet = _build_simulation_wallet_snapshot(list(result.get("simulation_trades", []) or []))

    wallet_text = "\n".join([
        _build_simulation_account_summary(result),
        "",
        "🧪 <b>Simulation Wallet</b>",
        "━━━━━━━━━━━━",
        "Start Balance",
        _sim_money(wallet['start_balance']),
        "Equity",
        _sim_money(wallet['equity']),
        "Realized",
        _sim_money(wallet['realized'], signed=True),
        "Floating",
        _sim_money(wallet['floating'], signed=True),
        "Trades",
        _sim_code(f"Open {wallet['open_count']} | Closed {wallet['closed_count']} | Total {wallet['total_count']}"),
    ])
    wallet_text = _simulation_header(wallet_text)

    daily_balance_text = _simulation_header("\n".join([
        "📅 <b>رصيد المحاكاة اليومي</b>",
        "━━━━━━━━━━━━",
        *_format_simulation_equity_curve_rows(
            list(result.get("simulation_daily_log", []) or []),
            result.get("simulation_daily_balance") or {},
            limit=10,
        ),
    ]))

    return build_simulation_report_command_outputs(
        result,
        account_summary=_build_simulation_account_summary(result),
        wallet_text=wallet_text,
        daily_balance_text=daily_balance_text,
    )

def run_once(
    previous_state: MarketModeState | None = None,
    settings: Settings | None = None,
    trade_store: RedisTradeStore | None = None,
    okx_client: OKXTradeClient | None = None,
) -> dict:
    settings = settings or get_settings()
    persisted_trades = trade_store.load_trades() if trade_store else []
    simulation_trades = _load_simulation_trades(trade_store)
    simulation_mode_active = _is_simulation_mode(settings)

    tickers = fetch_okx_tickers(settings.okx_base_url, settings.request_timeout, settings.offline_test_mode)
    ranked_pairs = select_ranked_pairs(tickers, settings.scan_limit)
    snapshot = _build_snapshot(ranked_pairs, settings)
    initial_mode = previous_state or MarketModeState(mode=MODE_NORMAL_LONG, changed_at=datetime.now(timezone.utc))
    state = decide_market_mode(snapshot, previous=initial_mode)
    scan_id = datetime.now(timezone.utc).isoformat()

    initial_protection = block_protection_status(state)
    initial_price_map = _build_live_price_map(tickers, fallback_pairs=ranked_pairs)
    exchange_reconcile_enabled = bool(
        okx_client is not None
        and getattr(okx_client, "configured", False)
        and not bool(getattr(settings, "offline_test_mode", False))
    )
    exchange_stop_sync_enabled = bool(
        exchange_reconcile_enabled
        and bool(getattr(settings, "okx_place_orders", False))
        and state.mode == MODE_BLOCK_LONGS
        and int(initial_protection.get("level", 0) or 0) >= 2
    )
    if persisted_trades:
        persisted_trades = update_open_trades(
            persisted_trades,
            initial_price_map,
            protection_level=initial_protection.get("level", 0),
            okx_client=okx_client if exchange_reconcile_enabled else None,
            sync_exchange=exchange_reconcile_enabled,
            sync_exchange_stop=exchange_stop_sync_enabled,
        )

    if simulation_trades:
        simulation_trades = update_open_trades(
            simulation_trades,
            initial_price_map,
            protection_level=initial_protection.get("level", 0),
            okx_client=None,
            sync_exchange=False,
            sync_exchange_stop=False,
        )

    portfolio_state_inputs = _resolve_portfolio_state_inputs(okx_client, settings)
    portfolio_state = build_portfolio_state_from_trades(persisted_trades, **portfolio_state_inputs)
    drawdown_status = evaluate_drawdown(portfolio_state)
    loss_streak_base_trades = simulation_trades if simulation_mode_active else persisted_trades
    loss_streak_guard = _build_loss_streak_guard(loss_streak_base_trades)

    signal_items = []
    current_execution_results = []
    technical_snapshot_records = []
    local_gate_trades = []
    gate_base_trades = simulation_trades if simulation_mode_active else persisted_trades
    slot_counts = _execution_slot_counts(gate_base_trades)
    recovery_remaining = max(0, MAX_RECOVERY_TRADES_PER_CYCLE - slot_counts.get("recovery", 0))

    scan_pairs = ranked_pairs
    filtered_pairs = [p for p in scan_pairs if prefilter_pair_before_candles(p, state.mode)]
    btc_bounce_pct = float(snapshot.btc_change_15m or 0.0)

    # ✅ FIX 2: snapshot الـ mode قبل اللوب — يمنع تأثير register_recovery_trade
    # على باقي الـ pairs في نفس الـ scan
    scan_mode = state.mode

    print(
        f"📊 Ranked pairs: {len(ranked_pairs)} | After prefilter: {len(filtered_pairs)} | Scanned pairs: {len(filtered_pairs)}",
        flush=True,
    )

    for pair in filtered_pairs:
        try:
            setattr(pair, "btc_bounce_pct", btc_bounce_pct)
        except Exception:
            pass

        try:
            recent_candles = _build_price_action_candles_for_pair(pair, settings)
            setattr(
                pair,
                "recent_candles",
                recent_candles,
            )
            print(
                f"PA_CANDLES | {pair.symbol} | count={len(recent_candles)}",
                flush=True,
            )
        except Exception as exc:
            print(
                f"PA_CANDLES | {getattr(pair, 'symbol', '-')} | error={exc}",
                flush=True,
            )

        try:
            resistance_4h_context = _build_4h_resistance_context_for_pair(pair, settings)
            setattr(pair, "resistance_4h_context", resistance_4h_context)
            print(
                f"4H_RESISTANCE | {pair.symbol} | "
                f"status={resistance_4h_context.get('status')} | "
                f"distance={resistance_4h_context.get('distance_pct')}",
                flush=True,
            )
        except Exception as exc:
            print(
                f"4H_RESISTANCE | {getattr(pair, 'symbol', '-')} | error={exc}",
                flush=True,
            )

        signal = build_signal_candidate(pair, scan_mode, settings.min_normal_score, settings.min_strong_score)
        if not signal:
            continue

        if not drawdown_status.allowed:
            exec_result = {
                "status": "rejected_risk",
                "reason": drawdown_status.reason,
                "path": "",
                "slot_scope": "drawdown",
                "drawdown_level": drawdown_status.level,
                "drawdown_pct": drawdown_status.drawdown_pct,
                "drawdown_message": drawdown_status.message_ar,
            }
        elif loss_streak_guard.get("active"):
            exec_result = _loss_streak_rejection(loss_streak_guard)
        else:
            exec_result = process_trade_candidate(
                signal,
                open_trades=[*gate_base_trades, *local_gate_trades],
                current_open_positions=slot_counts.get("general", 0),
                max_open_positions=settings.max_execution_positions,
                min_execution_score=settings.min_execution_score,
                recovery_slots_remaining=recovery_remaining if state.mode == MODE_RECOVERY_LONG else None,
                block_open_positions=slot_counts.get("block_exception", 0),
                max_block_positions=MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
                recovery_open_positions=slot_counts.get("recovery", 0),
                max_recovery_positions=MAX_RECOVERY_TRADES_PER_CYCLE,
                drawdown_status=drawdown_status,
                risk_mode=state.mode,
            )
            exec_result["decision_engine"] = "process_trade_candidate"
            exec_result["runtime_mode"] = "simulation" if simulation_mode_active else _get_signal_delivery_mode(settings)
            print(
                f"DECISION_ENGINE | {signal.symbol} | "
                f"runtime={exec_result.get('runtime_mode')} | "
                f"engine={exec_result.get('decision_engine')} | "
                f"status={exec_result.get('status')} | reason={exec_result.get('reason')}",
                flush=True,
            )

        exec_status = str(exec_result.get("status") or "").strip().lower()
        consumes_live_slot = exec_status == "accepted_preview"

        candidate_trade = register_trade(signal, exec_result)
        setattr(candidate_trade, "telegram_announced", False)
        setattr(candidate_trade, "announced_to_telegram", False)

        eligible_for_activation = consumes_live_slot and not _has_active_same_symbol(
            [*gate_base_trades, *local_gate_trades],
            candidate_trade,
        )

        if eligible_for_activation:
            path = str(exec_result.get("path") or "general")
            if path == "block_exception":
                slot_counts["block_exception"] = slot_counts.get("block_exception", 0) + 1
            elif path == "recovery":
                slot_counts["recovery"] = slot_counts.get("recovery", 0) + 1
                recovery_remaining = max(0, MAX_RECOVERY_TRADES_PER_CYCLE - slot_counts.get("recovery", 0))
                if state.mode == MODE_RECOVERY_LONG:
                    state = register_recovery_trade(state)
            else:
                slot_counts["general"] = slot_counts.get("general", 0) + 1

            # Reserve this trade for same-scan gating.
            local_gate_trades.append(candidate_trade)

            # Simulation mode must mirror the trading decision immediately:
            # if process_trade_candidate accepts it and the slot/same-symbol gate allows it,
            # open a virtual tracked trade regardless of Telegram delivery/dedup.
            if simulation_mode_active and consumes_live_slot:
                candidate_trade = _prepare_simulated_trade(candidate_trade, exec_result, settings=settings, balance=_build_simulation_wallet_snapshot(simulation_trades).get('equity', SIMULATION_START_BALANCE_USDT))
                existing_ids = {str(getattr(t, "trade_id", "") or "") for t in simulation_trades}
                candidate_id = str(getattr(candidate_trade, "trade_id", "") or "")
                if candidate_id and candidate_id not in existing_ids:
                    simulation_trades.append(candidate_trade)
                elif not candidate_id:
                    simulation_trades.append(candidate_trade)
                local_gate_trades[-1] = candidate_trade
                print(
                    f"SIM_TRADE_OPEN | {candidate_trade.symbol} | "
                    f"id={candidate_id or '-'} | entry={getattr(candidate_trade, 'entry', '-')} | "
                    f"path={getattr(candidate_trade, 'execution_path', '-')}",
                    flush=True,
                )

        signal_items.append({
            "signal": signal,
            "execution": exec_result,
            "message": build_signal_message(signal, exec_result),
            "candidate_trade": candidate_trade,
            "eligible_for_activation": eligible_for_activation,
            "telegram_announced": False,
            "exchange_required": False,
            "exchange_order_ok": False,
            "exchange_order_result": None,
            "announcement_status": "pending" if exec_status in {"accepted_preview", "pending_pullback_preview"} else "n/a",
            "simulation_mode": simulation_mode_active,
        })
        current_execution_results.append(exec_result)

        if is_snapshot_enabled(settings, redis_client=_snapshot_redis_client(trade_store)):
            technical_snapshot_records.append(
                build_signal_snapshot(
                    scan_id,
                    signal,
                    exec_result,
                    market_context={
                        "mode": state.mode,
                        "btc_change_15m": float(snapshot.btc_change_15m or 0.0),
                        "avg_change_15m": float(snapshot.avg_change_15m or 0.0),
                        "red_ratio_15m": float(snapshot.red_ratio_15m or 0.0),
                        "strong_coins_count": int(snapshot.strong_coins_count or 0),
                        "market_guard_valid_count": int(getattr(snapshot, "market_guard_valid_count", 0) or 0),
                    },
                )
            )

    price_map = _build_live_price_map(tickers, fallback_pairs=filtered_pairs)
    protection = block_protection_status(state)
    trades = update_open_trades(
        list(persisted_trades),
        price_map,
        protection_level=protection.get("level", 0),
    )

    simulation_trades = update_open_trades(
        list(simulation_trades),
        price_map,
        protection_level=protection.get("level", 0),
        okx_client=None,
        sync_exchange=False,
        sync_exchange_stop=False,
    )

    if trade_store:
        trade_store.save_trades(trades)
        _save_simulation_trades(simulation_trades, trade_store)
        _ensure_simulation_daily_log(simulation_trades, trade_store=trade_store, settings=settings)
        if simulation_mode_active:
            _append_simulation_execution_checks(current_execution_results, trade_store)
        else:
            trade_store.append_execution_checks(current_execution_results)
        execution_results_for_reports = trade_store.load_execution_checks(limit=500) or current_execution_results
        simulation_execution_results_for_reports = _load_simulation_execution_checks(trade_store, limit=500)
    else:
        execution_results_for_reports = current_execution_results
        simulation_execution_results_for_reports = current_execution_results if simulation_mode_active else []

    if technical_snapshot_records:
        snapshot_write_result = append_many_signal_snapshots(technical_snapshot_records, settings, redis_client=_snapshot_redis_client(trade_store))
        if not snapshot_write_result.get("ok"):
            print(f"⚠️ Technical snapshot write failed: {snapshot_write_result}", flush=True)

    mode_message = _build_mode_message(state, snapshot, protection)
    mode_context = _build_mode_context(state, snapshot, protection)
    portfolio_state = build_portfolio_state_from_trades(trades, **portfolio_state_inputs)
    drawdown_status = evaluate_drawdown(portfolio_state)
    drawdown_report = build_drawdown_report(portfolio_state)
    loss_streak_base_trades = simulation_trades if simulation_mode_active else trades
    loss_streak_guard = _build_loss_streak_guard(loss_streak_base_trades)

    execution_report_kwargs = _execution_report_balance_kwargs(portfolio_state_inputs)
    reports = build_report_bundle(trades, execution_results_for_reports, signal_items, **execution_report_kwargs)
    command_outputs = build_command_outputs(trades, execution_results_for_reports, signal_items, **execution_report_kwargs)

    return {
        "state": state,
        "mode": state.mode,
        "mode_message": mode_message,
        "mode_transition_message": _build_mode_message(
            state,
            snapshot,
            protection,
            variant="transition",
            old_mode=initial_mode.mode,
        ) if state.mode != initial_mode.mode else None,
        "block_alert_preview": build_block_escalation_alert(state, affected=len(trades), protected=sum(1 for t in trades if t.pnl_pct > 0), tightened=sum(1 for t in trades if t.tp2_hit)) if state.mode == MODE_BLOCK_LONGS else None,
        "menu": build_main_menu_layout(),
        "menu_keyboard": _build_main_inline_keyboard_with_bot_modes(settings),
        "mode_context": mode_context,
        "scan_stats": {"ranked_pairs": len(ranked_pairs), "after_prefilter": len(filtered_pairs), "scanned_pairs": len(filtered_pairs)},
        "technical_snapshot_enabled": is_snapshot_enabled(settings, redis_client=_snapshot_redis_client(trade_store)),
        "technical_snapshot_written": len(technical_snapshot_records),
        "portfolio_state": portfolio_state,
        "drawdown_status": drawdown_status,
        "drawdown_report": drawdown_report,
        "loss_streak_guard": loss_streak_guard,
        "portfolio_state_inputs": portfolio_state_inputs,
        "help": build_master_help(
            mode=state.mode,
            execution_enabled=settings.execution_enabled,
            risk_enabled=True,
            okx_orders=settings.okx_place_orders,
        ),
        "help_execution": build_execution_help(),
        "help_normal": build_normal_help(),
        "signals": [item["message"] for item in signal_items[:8]],
        "signal_items": signal_items,
        "execution_results": execution_results_for_reports,
        "current_execution_results": current_execution_results,
        "simulation_trades": simulation_trades,
        "simulation_execution_results": simulation_execution_results_for_reports,
        "simulation_signal_items": signal_items if simulation_mode_active else [],
        "simulation_wallet": _build_simulation_wallet_snapshot(simulation_trades),
        "simulation_daily_balance": _ensure_simulation_daily_log(simulation_trades, trade_store=trade_store, settings=settings),
        "simulation_daily_log": _load_simulation_daily_log(trade_store),
        "trades": trades,
        "command_outputs": command_outputs,
        "simulation_command_outputs": {},
        **reports,
    }


def _refresh_runtime_result_outputs(result: dict, trade_store: RedisTradeStore | None = None) -> None:
    trades = list(result.get("trades", []) or [])
    execution_results = result.get("execution_results", []) or []
    signal_items = result.get("signal_items", []) or []

    portfolio_state_inputs = dict(result.get("portfolio_state_inputs", {}) or {})
    execution_report_kwargs = _execution_report_balance_kwargs(portfolio_state_inputs)
    reports = build_report_bundle(trades, execution_results, signal_items, **execution_report_kwargs)
    command_outputs = build_command_outputs(trades, execution_results, signal_items, **execution_report_kwargs)

    result["trades"] = trades
    result["command_outputs"] = command_outputs
    result.update(reports)

    portfolio_state_inputs = dict(result.get("portfolio_state_inputs", {}) or {})
    portfolio_state = build_portfolio_state_from_trades(trades, **portfolio_state_inputs)
    result["portfolio_state"] = portfolio_state
    result["drawdown_status"] = evaluate_drawdown(portfolio_state)
    result["drawdown_report"] = build_drawdown_report(portfolio_state)
    result["loss_streak_guard"] = _build_loss_streak_guard(trades)

    if trade_store:
        trade_store.save_trades(trades)


def _activate_announced_trade(
    result: dict,
    item: dict,
    trade_store: RedisTradeStore | None = None,
) -> bool:
    if not isinstance(item, dict):
        return False

    exec_result = item.get("execution") or {}
    exec_status = str(exec_result.get("status") or "").strip().lower()
    if exec_status not in {"accepted_preview", "pending_pullback_preview"}:
        return False

    if bool(item.get("telegram_announced")):
        return False

    candidate_trade = item.get("candidate_trade")
    if candidate_trade is None:
        return False

    exchange_required = bool(item.get("exchange_required"))
    exchange_order_ok = bool(item.get("exchange_order_ok", not exchange_required))
    if exec_status == "accepted_preview" and exchange_required and not exchange_order_ok:
        item["announcement_status"] = "exchange_failed"
        _attach_exchange_state_to_trade(candidate_trade, item.get("exchange_order_result"))
        return False

    announced_at = datetime.now(timezone.utc)
    setattr(candidate_trade, "telegram_announced", True)
    setattr(candidate_trade, "announced_to_telegram", True)
    setattr(candidate_trade, "telegram_announced_at", announced_at)
    item["telegram_announced"] = True
    item["announcement_status"] = "sent"

    _attach_exchange_state_to_trade(candidate_trade, item.get("exchange_order_result"))

    trades = list(result.get("trades", []) or [])
    trade_id = getattr(candidate_trade, "trade_id", None)
    updated_existing = False

    for trade in trades:
        if trade_id and getattr(trade, "trade_id", None) == trade_id:
            setattr(trade, "telegram_announced", True)
            setattr(trade, "announced_to_telegram", True)
            setattr(trade, "telegram_announced_at", announced_at)
            _attach_exchange_state_to_trade(trade, item.get("exchange_order_result"))
            updated_existing = True
            break

    if exec_status != "accepted_preview":
        if updated_existing:
            _refresh_runtime_result_outputs(result, trade_store=trade_store)
        return True

    if not bool(item.get("eligible_for_activation")):
        # This execution check passed a gate label, but it was not eligible to
        # become a live tracked trade in this scan (same-symbol / slot context).
        return True

    if not updated_existing:
        trades.append(candidate_trade)
        result["trades"] = trades

    _refresh_runtime_result_outputs(result, trade_store=trade_store)
    return True



def _prepare_simulated_trade(candidate_trade, exec_result: dict | None = None, settings: Settings | None = None, balance: float = SIMULATION_START_BALANCE_USDT):
    """Mark a candidate trade as an opened virtual simulation trade.

    Important:
    - trade_source stays "simulation" so it never mixes with live trades.
    - tracking_bucket is "execution" so existing execution reports/open-trade
      reports can read it exactly like real execution trades.
    - No OKX metadata is required; lifecycle is price-driven.
    """
    exec_result = exec_result or {}
    opened_at = datetime.now(timezone.utc)

    setattr(candidate_trade, "trade_source", "simulation")
    setattr(candidate_trade, "tracking_bucket", "execution")
    setattr(candidate_trade, "execution_trade", True)
    setattr(candidate_trade, "execution_checked", True)
    setattr(candidate_trade, "execution_status", "accepted_preview")
    setattr(candidate_trade, "execution_reason", str(exec_result.get("reason") or "simulation"))
    setattr(candidate_trade, "execution_path", str(exec_result.get("path") or getattr(candidate_trade, "execution_path", "") or "general"))
    setattr(candidate_trade, "exchange_sync_state", "simulation_virtual_fill")
    setattr(candidate_trade, "exchange_order_ok", True)
    setattr(candidate_trade, "exchange_order_reason", "simulation_virtual_fill")
    setattr(candidate_trade, "telegram_announced", True)
    setattr(candidate_trade, "announced_to_telegram", True)
    setattr(candidate_trade, "telegram_announced_at", opened_at)
    setattr(candidate_trade, "opened_at", getattr(candidate_trade, "opened_at", None) or opened_at)
    setattr(candidate_trade, "updated_at", opened_at)
    setattr(candidate_trade, "closed_at", None)
    setattr(candidate_trade, "status", "open")
    setattr(candidate_trade, "slot_exempt", False)
    setattr(candidate_trade, "slot_exempt_reason", "")
    setattr(candidate_trade, "daily_open_risk_exempt", False)
    margin_usdt = _simulation_margin_usdt(balance, settings)
    setattr(candidate_trade, "simulation_balance_reference", float(balance or SIMULATION_START_BALANCE_USDT))
    setattr(candidate_trade, "simulation_margin_usdt", margin_usdt)
    setattr(candidate_trade, "used_margin_usdt", margin_usdt)

    entry = float(getattr(candidate_trade, "entry", 0.0) or 0.0)
    if entry > 0:
        setattr(candidate_trade, "current_price", entry)
        setattr(candidate_trade, "highest_price", max(float(getattr(candidate_trade, "highest_price", 0.0) or 0.0), entry))

    return candidate_trade


def _activate_simulated_trade(
    result: dict,
    item: dict,
    trade_store: RedisTradeStore | None = None,
    settings: Settings | None = None,
) -> bool:
    """Activate accepted_preview as a virtual simulation trade only.

    This is intentionally the same decision path as trading:
    process_trade_candidate decides; this function only replaces the final OKX fill
    with an internal simulated fill.
    """
    if not isinstance(item, dict):
        return False

    exec_result = item.get("execution") or {}
    exec_status = str(exec_result.get("status") or "").strip().lower()
    if exec_status != "accepted_preview":
        return False

    if bool(item.get("telegram_announced")):
        return False

    candidate_trade = item.get("candidate_trade")
    if candidate_trade is None:
        return False

    if not bool(item.get("eligible_for_activation")):
        item["announcement_status"] = "simulation_not_eligible"
        return True

    candidate_trade = _prepare_simulated_trade(candidate_trade, exec_result, settings=settings, balance=_build_simulation_wallet_snapshot(result.get('simulation_trades', []) or []).get('equity', SIMULATION_START_BALANCE_USDT))

    item["telegram_announced"] = True
    item["announcement_status"] = "simulation_sent"
    item["decision_engine"] = "process_trade_candidate"
    item["execution_source"] = "same_trading_decision_virtual_fill"

    sim_trades = list(result.get("simulation_trades", []) or [])
    trade_id = getattr(candidate_trade, "trade_id", None)
    updated_existing = False

    for idx, trade in enumerate(sim_trades):
        if trade_id and getattr(trade, "trade_id", None) == trade_id:
            sim_trades[idx] = candidate_trade
            updated_existing = True
            break

    if not updated_existing:
        sim_trades.append(candidate_trade)

    result["simulation_trades"] = sim_trades
    result["simulation_wallet"] = _build_simulation_wallet_snapshot(sim_trades)

    _save_simulation_trades(sim_trades, trade_store=trade_store)

    try:
        result["simulation_command_outputs"] = _build_simulation_command_outputs(result)
    except Exception as exc:
        print(f"⚠️ Simulation command output refresh failed: {exc}", flush=True)

    return True


def _plain_result(result: dict) -> dict:
    return {k: v for k, v in result.items() if k not in {"state", "signal_items", "trades", "simulation_trades"}}


def _print_scan_summary(result: dict, trade_store: RedisTradeStore | None = None) -> None:
    scan = result.get("scan_stats", {}) or {}
    ctx = result.get("mode_context", {}) or {}
    execution_results = result.get("current_execution_results") or result.get("execution_results") or []
    trades = result.get("trades", []) or []

    checked = len(execution_results)
    accepted = sum(1 for r in execution_results if r.get("status") in {"accepted_preview", "pending_pullback_preview"})
    rejected = sum(1 for r in execution_results if str(r.get("status", "")).startswith("rejected"))
    candidate_only = sum(1 for r in execution_results if r.get("status") == "candidate_only")
    open_trades = sum(1 for t in trades if _is_counted_open_trade(t))
    protected = sum(
        1 for t in trades
        if bool(getattr(t, "protected_runner", False))
        or (
            bool(getattr(t, "tp2_hit", False))
            and bool(
                getattr(t, "runner_active", False)
                or getattr(t, "has_open_runner", False)
            )
        )
    )

    print(
        " | ".join([
            f"📊 Scan ranked={scan.get('ranked_pairs', 0)}",
            f"prefilter={scan.get('after_prefilter', 0)}",
            f"scanned={scan.get('scanned_pairs', 0)}",
        ]),
        flush=True,
    )
    print(
        " | ".join([
            f"🧭 Mode={result.get('mode', 'UNKNOWN')}",
            f"Avg15m={float(ctx.get('avg15m', 0) or 0):+.2f}%",
            f"Red={float(ctx.get('red_ratio', 0) or 0):.0f}%",
            f"Strong={int(ctx.get('strong_coins', 0) or 0)}",
        ]),
        flush=True,
    )
    print(
        " | ".join([
            f"⚡ Execution checked={checked}",
            f"accepted={accepted}",
            f"rejected={rejected}",
            f"candidate_only={candidate_only}",
        ]),
        flush=True,
    )
    sim_trades = result.get("simulation_trades", []) or []
    sim_open = sum(1 for t in sim_trades if _is_counted_open_trade(t))
    print(
        " | ".join([
            f"📂 Open trades={open_trades}",
            f"simulation_open={sim_open}",
            f"protected runners={protected}",
            f"Redis={'ON' if trade_store and trade_store.enabled else 'OFF'}",
        ]),
        flush=True,
    )


def _purge_expired_fingerprints(sent_fingerprints: dict[str, float], now_ts: float | None = None) -> None:
    now_ts = now_ts or time.time()
    expired = [fp for fp, expires_at in sent_fingerprints.items() if float(expires_at or 0) <= now_ts]
    for fp in expired:
        sent_fingerprints.pop(fp, None)


def _is_duplicate_signal_fingerprint(
    fingerprint: str,
    sent_fingerprints: dict[str, float],
    trade_store: RedisTradeStore | None = None,
    ttl_seconds: int = SYMBOL_OBSERVATION_DEDUP_TTL_SECONDS,
) -> bool:
    now_ts = time.time()
    _purge_expired_fingerprints(sent_fingerprints, now_ts)

    expires_at = float(sent_fingerprints.get(fingerprint, 0) or 0)
    if expires_at > now_ts:
        return True

    if trade_store and trade_store.enabled and trade_store.mark_signal_fingerprint(
        fingerprint, ttl_seconds=ttl_seconds
    ):
        sent_fingerprints[fingerprint] = now_ts + max(60, int(ttl_seconds))
        return True

    sent_fingerprints[fingerprint] = now_ts + max(60, int(ttl_seconds))
    return False


def _signal_status_bucket(exec_status: str | None) -> str:
    status = str(exec_status or "").strip().lower()
    if status == "accepted_preview":
        return "execution_accepted"
    if status == "pending_pullback_preview":
        return "execution_pullback"

    # Everything else is an observation-level alert for this symbol.
    # This stops normal/candidate/rejected spam from repeating every scan.
    return "symbol_observation"


def _signal_fingerprint_ttl(exec_result: dict | None) -> int:
    status = str((exec_result or {}).get("status") or "").strip().lower()
    if status == "accepted_preview":
        return SYMBOL_EXECUTION_DEDUP_TTL_SECONDS
    if status == "pending_pullback_preview":
        return SYMBOL_PULLBACK_DEDUP_TTL_SECONDS
    return SYMBOL_OBSERVATION_DEDUP_TTL_SECONDS


def _build_signal_fingerprint(signal, exec_result: dict) -> str:
    return "|".join([
        str(getattr(signal, "symbol", "")).upper(),
        "LONG",
        _signal_status_bucket(exec_result.get("status") if isinstance(exec_result, dict) else None),
    ])


def _iter_signal_items_for_dispatch(result: dict) -> list[dict]:
    items = list(result.get("signal_items", []) or [])

    actionable_items = []
    normal_items = []

    for item in items:
        exec_status = str((item.get("execution") or {}).get("status") or "").strip().lower()

        # Must always be eligible for dispatch before the normal observation limit:
        # - accepted execution previews
        # - pullback previews
        # - any rejected_* status, including PA gate rejects
        if _is_actionable_signal_status(exec_status):
            actionable_items.append(item)
        else:
            normal_items.append(item)

    # Always send all actionable trading-mode alerts.
    # Limit only non-actionable normal observations to avoid Telegram spam.
    return [*actionable_items, *normal_items[:8]]



def _safe_set_trade_attr(trade, name: str, value) -> None:
    try:
        setattr(trade, name, value)
    except Exception:
        pass


def _attach_exchange_state_to_trade(trade, managed_order_result: dict | None) -> None:
    if trade is None or not isinstance(managed_order_result, dict):
        return

    entry = managed_order_result.get("entry") or {}
    tp_split = managed_order_result.get("tp_split") or {}
    tp1 = tp_split.get("tp1") or {}
    tp2 = tp_split.get("tp2") or {}
    plan = managed_order_result.get("plan") or {}

    _safe_set_trade_attr(trade, "exchange_order_ok", bool(entry.get("ok")))
    _safe_set_trade_attr(trade, "exchange_order_reason", entry.get("reason"))
    _safe_set_trade_attr(trade, "entry_order_id", entry.get("order_id"))
    _safe_set_trade_attr(trade, "entry_client_order_id", entry.get("client_order_id"))
    _safe_set_trade_attr(trade, "entry_order_payload", entry.get("payload"))
    _safe_set_trade_attr(trade, "sl_attached_on_entry", bool(managed_order_result.get("sl_attached")))
    _safe_set_trade_attr(trade, "sl_attached_payload", (entry.get("payload") or {}).get("attachAlgoOrds"))
    _safe_set_trade_attr(trade, "tp_split_ok", tp_split.get("ok"))
    _safe_set_trade_attr(trade, "tp_split_reason", tp_split.get("reason"))
    _safe_set_trade_attr(trade, "tp1_order_id", tp1.get("order_id"))
    _safe_set_trade_attr(trade, "tp2_order_id", tp2.get("order_id"))
    _safe_set_trade_attr(trade, "tp1_client_order_id", tp1.get("client_order_id"))
    _safe_set_trade_attr(trade, "tp2_client_order_id", tp2.get("client_order_id"))
    _safe_set_trade_attr(trade, "runner_expected_size", (plan.get("runner") or {}).get("size"))
    _safe_set_trade_attr(trade, "runner_requires_trailing_after_tp2", bool(managed_order_result.get("requires_runner_trailing")))
    _safe_set_trade_attr(trade, "managed_trade_plan", plan)


def _execute_managed_okx_order(
    okx_client: OKXTradeClient,
    signal,
    settings: Settings,
) -> dict:
    sl_value = float(getattr(signal, "sl", 0.0) or 0.0)
    tp1_value = float(getattr(signal, "tp1", 0.0) or 0.0)
    tp2_value = float(getattr(signal, "tp2", 0.0) or 0.0)
    entry_value = float(getattr(signal, "entry", 0.0) or 0.0)

    sizing = _resolve_entry_margin_plan(okx_client, settings)
    margin_usdt = max(_safe_float(sizing.get("margin_usdt"), 0.0), 0.0) or max(_safe_float(getattr(settings, "paper_margin_usdt", 35.0), 35.0), 0.0) or 35.0

    entry_result = okx_client.place_market_long(
        signal.symbol,
        entry_value,
        margin_usdt=margin_usdt,
        leverage=settings.default_leverage,
        td_mode=settings.okx_td_mode,
        sl_trigger_px=sl_value if sl_value > 0 else None,
        tag="entry",
    )

    plan = {}
    if entry_value > 0 and sl_value > 0 and tp1_value > 0 and tp2_value > 0:
        plan = okx_client.build_managed_trade_plan(
            signal.symbol,
            entry_value,
            margin_usdt,
            settings.default_leverage,
            sl_value,
            tp1_value,
            tp2_value,
        )

    tp_split_result = None
    if entry_result.get("ok") and tp1_value > 0 and tp2_value > 0:
        tp_split_result = okx_client.place_reduce_only_tp_split(
            signal.symbol,
            entry_value,
            margin_usdt,
            settings.default_leverage,
            tp1_price=tp1_value,
            tp2_price=tp2_value,
            td_mode=settings.okx_td_mode,
            tag="tp",
        )

    return {
        "ok": bool(entry_result.get("ok")),
        "entry": entry_result,
        "tp_split": tp_split_result,
        "plan": plan,
        "sizing": sizing,
        "used_margin_usdt": margin_usdt,
        "sl_attached": bool(sl_value > 0 and ((entry_result.get("payload") or {}).get("attachAlgoOrds"))),
        "tp_orders_ok": None if tp_split_result is None else bool(tp_split_result.get("ok")),
        "requires_runner_trailing": bool((plan.get("runner") or {}).get("requires_trailing_after_tp2")) if isinstance(plan, dict) else False,
    }



def _bool_label(value: bool) -> str:
    return "نعم" if bool(value) else "لا"


def _mode_label(simulated: bool | None) -> str:
    if simulated is None:
        return "-"
    return "Simulated" if bool(simulated) else "Live"


def _reason_label(value: object) -> str:
    text = str(value or "").strip()
    return text or "-"


def _build_compact_okx_result_message(signal, managed_order_result: dict | None, ok: bool) -> str:
    managed_order_result = managed_order_result or {}
    entry = managed_order_result.get("entry") or {}
    tp_split = managed_order_result.get("tp_split") or {}
    plan = managed_order_result.get("plan") or {}
    sizing = managed_order_result.get("sizing") or {}

    symbol = str(getattr(signal, "symbol", "-") or "-")
    path = str((getattr(signal, "meta", {}) or {}).get("execution_path") or "")
    entry_price = getattr(signal, "entry", "-")
    sl_price = getattr(signal, "sl", "-")
    tp1_price = getattr(signal, "tp1", "-")
    tp2_price = getattr(signal, "tp2", "-")
    mode_text = _mode_label(entry.get("simulated"))
    reason_text = _reason_label(
        entry.get("reason") or managed_order_result.get("reason") or ("submitted" if ok else "not_submitted")
    )

    title = "✅ <b>OKX Confirmed</b>" if ok else "⚠️ <b>OKX Failed</b>"
    path_text = f" | {path}" if path else ""

    lines = [
        title,
        f"💎 <b>{symbol}</b>{path_text}",
        f"⚙️ {mode_text} | {'Accepted' if ok else 'Failed'}",
        f"📝 {reason_text}",
    ]

    if ok:
        lines.append(f"🆔 Order: {_reason_label(entry.get('order_id'))} | SL: {_bool_label(managed_order_result.get('sl_attached'))}")
    else:
        lines.append(f"📍 Entry {entry_price} | SL {sl_price}")

    if sizing:
        lines.append(
            f"💼 Balance {_fmt_money(sizing.get('reference_balance_usdt'))} | Margin {_fmt_money(sizing.get('margin_usdt'))} USDT | {_safe_float(sizing.get('position_pct'), 0.0):.2f}%"
        )

    runner_pct = (plan.get('runner', {}) or {}).get('close_pct', '-') if isinstance(plan, dict) else '-'
    lines.append(f"🎯 TP1 {tp1_price} | TP2 {tp2_price} | Runner {runner_pct}%")

    if isinstance(tp_split, dict) and tp_split:
        lines.append(f"📤 TP Orders: {'جاهزة' if tp_split.get('ok') else 'بحاجة مراجعة'}")

    lines.append("✅ تم إرسال أمر OKX بنجاح." if ok else "📌 الصفقة لم تُفتح على OKX.")
    return "\n".join(lines)


def _build_managed_execution_lines(managed_order_result: dict | None) -> list[str]:
    """Compact OKX execution block inside the main signal card.

    Full OKX details remain in the separate OKX result message.
    """
    if not isinstance(managed_order_result, dict):
        return []

    entry = managed_order_result.get("entry") or {}
    plan = managed_order_result.get("plan") or {}
    sizing = managed_order_result.get("sizing") or {}

    ok = bool(entry.get("ok"))
    simulated = entry.get("simulated")
    status_text = "Accepted" if ok else "Failed"
    mode_text = "Paper Mode" if bool(simulated) else "Live Mode"

    lines = [
        "🤖 <b>OKX</b>",
        f"• {mode_text} | {status_text}",
        f"• SL Attached: {'✅' if managed_order_result.get('sl_attached') else '❌'}",
    ]

    if sizing:
        lines.append(
            f"• Margin: {_fmt_money(sizing.get('margin_usdt'))} USDT ({_safe_float(sizing.get('position_pct'), 0.0):.2f}%)"
        )

    tp1_pct = "-"
    tp2_pct = "-"
    runner_pct = "-"

    if isinstance(plan, dict):
        tp1_pct = (plan.get("tp1") or {}).get("close_pct", "-")
        tp2_pct = (plan.get("tp2") or {}).get("close_pct", "-")
        runner_pct = (plan.get("runner") or {}).get("close_pct", "-")

    def _pct_text(value) -> str:
        try:
            return str(int(round(float(value))))
        except Exception:
            return str(value or "-")

    if tp1_pct != "-" or tp2_pct != "-" or runner_pct != "-":
        lines.append(
            f"📌 <b>Plan</b>: TP1 {_pct_text(tp1_pct)}% • TP2 {_pct_text(tp2_pct)}% • Runner {_pct_text(runner_pct)}%"
        )

    if managed_order_result.get("requires_runner_trailing"):
        lines.append("🏃 Runner Trail after TP2")

    if not ok:
        lines.append("📌 لم يتم فتح الصفقة على OKX")

    return lines





def _simulation_signal_badge(text: str) -> str:
    """Small Simulation-only badge above Telegram signal cards.

    It is intentionally tiny and does not alter the original signal layout.
    """
    value = str(text or "")
    if value.lstrip().startswith("🧪 Simulation Mode"):
        return value
    return "🧪 <b>Simulation Mode</b>\n" + value


def _dispatch_signals(sender: TelegramSender, result: dict, settings: Settings, sent_fingerprints: dict[str, float], okx_client: OKXTradeClient | None = None, trade_store: RedisTradeStore | None = None) -> None:
    for item in _iter_signal_items_for_dispatch(result):
        signal = item["signal"]
        exec_result = item["execution"]
        exec_status = str(exec_result.get("status") or "")
        is_execution = exec_status in {"accepted_preview", "pending_pullback_preview"}
        can_place_order = exec_status == "accepted_preview"
        if not _should_dispatch_signal_item(item, settings):
            item["announcement_status"] = "filtered_signal_mode"
            continue
        fingerprint = _build_signal_fingerprint(signal, exec_result)
        if _is_duplicate_signal_fingerprint(
            fingerprint,
            sent_fingerprints,
            trade_store,
            ttl_seconds=_signal_fingerprint_ttl(exec_result),
        ):
            item["announcement_status"] = "deduplicated"
            continue

        text = item["message"]
        managed_order_result = None
        simulation_mode_active = _is_simulation_mode(settings)
        exchange_required = bool(
            can_place_order
            and not simulation_mode_active
            and settings.execution_enabled
            and settings.okx_place_orders
            and okx_client
        )
        exchange_order_ok = True

        if simulation_mode_active:
            text = _simulation_signal_badge(text)

        if exchange_required:
            managed_order_result = _execute_managed_okx_order(okx_client, signal, settings)
            exchange_order_ok = bool(managed_order_result.get("ok"))
            text += "\n\n" + "\n".join(_build_managed_execution_lines(managed_order_result))
        elif simulation_mode_active and can_place_order:
            text += "\n\n" + "\n".join([
                "🧪 <b>Simulation Execution</b>",
                "• Virtual fill only",
                "• OKX live orders forced OFF",
                f"• Start Balance: {SIMULATION_START_BALANCE_USDT:.2f} USDT",
            ])

        item["exchange_required"] = exchange_required
        item["exchange_order_result"] = managed_order_result
        item["exchange_order_ok"] = exchange_order_ok

        if exchange_required:
            _attach_exchange_state_to_trade(item.get("candidate_trade"), managed_order_result)

        send_result = _send_text(sender, text, reply_markup=build_signal_buttons(signal))
        send_ok = bool(isinstance(send_result, dict) and send_result.get("ok"))
        _telegram_send_pause(
            TELEGRAM_EXECUTION_SEND_GAP_SECONDS if is_execution else TELEGRAM_NORMAL_SEND_GAP_SECONDS
        )

        if exchange_required:
            try:
                _send_text(
                    sender,
                    _build_compact_okx_result_message(
                        signal,
                        managed_order_result,
                        ok=exchange_order_ok,
                    ),
                    reply_markup=build_signal_buttons(signal),
                )
                _telegram_send_pause(TELEGRAM_EXECUTION_SEND_GAP_SECONDS)
            except Exception:
                pass

        if send_ok and is_execution:
            if simulation_mode_active and can_place_order:
                _activate_simulated_trade(result, item, trade_store=trade_store, settings=settings)
                continue
            if exchange_required and not exchange_order_ok:
                item["announcement_status"] = "exchange_failed"
                _attach_exchange_state_to_trade(item.get("candidate_trade"), managed_order_result)
                continue
            _activate_announced_trade(result, item, trade_store=trade_store)
        else:
            item["announcement_status"] = "sent" if send_ok else "send_failed"

def _build_fast_status(result: dict, settings: Settings, trade_store: RedisTradeStore | None = None) -> str:
    execution_results = result.get("execution_results", []) or []
    last_rejection = next(
        (r for r in reversed(execution_results) if str(r.get("status", "")).startswith("rejected")),
        None,
    )
    rejection_reason = "none"
    if last_rejection:
        rejection_reason = f"{last_rejection.get('status')} | {last_rejection.get('reason', 'unknown')}"

    redis_stats = trade_store.health_snapshot() if trade_store else {"enabled": False}
    drawdown = result.get("drawdown_status")
    drawdown_line = "n/a"
    if drawdown is not None:
        drawdown_line = f"{float(getattr(drawdown, 'drawdown_pct', 0.0) or 0.0):.1f}% | level={int(getattr(drawdown, 'level', 0) or 0)} | {'ALLOWED' if getattr(drawdown, 'allowed', True) else 'HALTED'}"

    loss_guard = result.get("loss_streak_guard") or {}
    if loss_guard.get("active"):
        loss_guard_line = (
            f"ACTIVE | streak={int(loss_guard.get('streak', 0) or 0)} | "
            f"remaining={int(loss_guard.get('remaining_minutes', 0) or 0)}m"
        )
    else:
        loss_guard_line = f"OFF | streak={int(loss_guard.get('streak', 0) or 0)}"

    return "\n".join([
        "🟢 Bot Status",
        "━━━━━━━━━━━━",
        f"📈 Market Mode: {result.get('mode', 'UNKNOWN')}",
        f"⚡ Execution Engine: {'ON' if settings.execution_enabled else 'OFF'}",
        f"🧪 OKX Paper Orders: {'ON' if settings.okx_place_orders else 'OFF'}",
        f"🧰 Offline Test Mode: {'ON' if settings.offline_test_mode else 'OFF'}",
        f"🔒 Live Trading: {'ALLOWED' if settings.allow_live_trading else 'BLOCKED'}",
        f"📡 Signal Mode: {_signal_delivery_mode_label(settings)}",
        f"🧪 Simulation: {'ON' if _is_simulation_mode(settings) else 'OFF'} | Wallet={result.get('simulation_wallet', {}).get('equity', SIMULATION_START_BALANCE_USDT):.2f} USDT",
        "",
        f"📡 Telegram: {'ON' if settings.telegram_enabled else 'OFF'}",
        f"🧠 Redis: {'ON' if redis_stats.get('enabled') else 'OFF'} | open={redis_stats.get('open_set', 0)} | history={redis_stats.get('history_set', 0)} | checks={redis_stats.get('execution_checks', 0)}",
        f"💼 Drawdown: {drawdown_line}",
        f"🛑 Loss Streak Guard: {loss_guard_line}",
        f"⏱ Full Scan: {settings.scan_interval_seconds}s",
        f"🛡 Mode Guard: {settings.market_mode_guard_interval_seconds}s",
        f"🧠 Technical Snapshot: {'ON' if is_snapshot_enabled(settings, redis_client=_snapshot_redis_client(trade_store)) else 'OFF'}",
        "",
        "🧠 آخر حالة تنفيذ:",
        f"{rejection_reason}",
        "",
        "🕹 Runtime Toggle: /okx_orders_on | /okx_orders_off",
        "✅ Managed OKX entry + SL + TP split enabled" if settings.okx_place_orders else "✅ Preview mode only — managed exchange placement paused",
    ])



def _okx_response_ok(payload: dict | None) -> bool:
    return bool(isinstance(payload, dict) and str(payload.get("code", "")) == "0")


def _okx_response_error(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return "no_response"
    return str(payload.get("msg") or payload.get("error") or payload.get("reason") or payload.get("code") or "unknown")


def _build_okx_status_panel(
    settings: Settings,
    okx_client: OKXTradeClient | None = None,
) -> str:
    """Build a dedicated OKX connectivity/account status panel.

    This is intentionally separate from /status:
    - /status remains the general bot status.
    - /okx_status performs a lightweight OKX balance read when credentials exist.
    """
    client = okx_client or OKXTradeClient(
        api_key=settings.okx_api_key,
        api_secret=settings.okx_api_secret,
        passphrase=settings.okx_passphrase,
        base_url=settings.okx_base_url,
        simulated=settings.okx_simulated,
        allow_live_trading=settings.allow_live_trading,
        timeout=settings.request_timeout,
    )

    configured = bool(getattr(client, "configured", False))
    paper_mode = bool(getattr(getattr(client, "credentials", None), "simulated", getattr(settings, "okx_simulated", True)))
    orders_on = bool(getattr(settings, "okx_place_orders", False))
    live_guard = bool(getattr(settings, "allow_live_trading", False))

    lines = [
        "📘 <b>OKX Status</b>",
        "━━━━━━━━━━━━",
        f"• Credentials: <b>{'CONFIGURED' if configured else 'MISSING'}</b>",
        f"• Account Mode: <b>{'PAPER / DEMO' if paper_mode else 'LIVE'}</b>",
        f"• OKX Orders: <b>{'ON' if orders_on else 'OFF'}</b>",
        f"• Live Trading Guard: <b>{'ALLOWED' if live_guard else 'BLOCKED'}</b>",
        f"• Base URL: {getattr(settings, 'okx_base_url', '-')}",
    ]

    if not configured:
        lines.extend([
            "",
            "⚠️ لم يتم ضبط مفاتيح OKX بالكامل.",
            "المطلوب: OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE",
        ])
        return "\n".join(lines)

    balance_response = None
    try:
        balance_response = client.get_balance()
    except Exception as exc:
        balance_response = {"code": "-1", "msg": str(exc), "data": []}

    ok = _okx_response_ok(balance_response)
    reference_balance = _extract_okx_reference_balance_usdt(balance_response if isinstance(balance_response, dict) else None)
    sizing = _compute_margin_from_reference(reference_balance, settings) if reference_balance > 0 else 0.0
    allocation_pct, slot_count = _risk_sizing_constants(settings)

    lines.extend([
        "",
        f"• Balance API: <b>{'OK' if ok else 'FAILED'}</b>",
    ])

    if ok:
        lines.extend([
            f"• Reference Balance: <b>{reference_balance:,.2f} USDT</b>",
            f"• Allocation: {allocation_pct:.2f}% / {slot_count} slots",
            f"• Planned Margin / Trade: <b>{sizing:,.2f} USDT</b>",
        ])
    else:
        lines.append(f"• Error: {_okx_response_error(balance_response)}")

    lines.extend([
        "",
        "📌 /status = حالة البوت العامة",
        "📌 /okx_status = حالة اتصال OKX والرصيد",
    ])
    return "\n".join(lines)

def _extract_commands(text: str) -> list[str]:
    commands: list[str] = []
    for line in str(text or "").splitlines():
        for token in line.strip().split():
            if token.startswith("/"):
                commands.append(token.split("@", 1)[0])
                break
    return commands


def _strip_basic_html(text: str) -> str:
    """Fallback renderer when Telegram rejects HTML.

    Keep anchor labels compact. This prevents long TradingView URLs from
    reappearing when a formatted report needs a plain-text fallback.
    """
    value = str(text or "")
    value = value.replace("<b>", "").replace("</b>", "")
    value = value.replace("<i>", "").replace("</i>", "")
    value = re.sub(r'<a\s+href="([^"]+)">([^<]+)</a>', r'\2', value)
    value = value.replace("<code>", "").replace("</code>", "")
    return value


def _has_basic_html(text: str) -> bool:
    value = str(text or "")
    return bool("<b>" in value or "<code>" in value or "<a " in value or "<i>" in value)


def _chunk_text_for_telegram(text: str, max_len: int = 3600) -> list[str]:
    value = str(text or "")
    if len(value) <= max_len:
        return [value]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in value.splitlines():
        add_len = len(line) + 1
        if current and current_len + add_len > max_len:
            chunks.append("\n".join(current))
            current = [line]
            current_len = add_len
        elif add_len > max_len:
            if current:
                chunks.append("\n".join(current))
                current = []
                current_len = 0
            for i in range(0, len(line), max_len):
                chunks.append(line[i:i + max_len])
        else:
            current.append(line)
            current_len += add_len
    if current:
        chunks.append("\n".join(current))
    return chunks or [""]


def _send_text(sender: TelegramSender, text: str, reply_markup: dict | None = None):
    raw_text = str(text or "")
    parse_mode = "HTML" if _has_basic_html(raw_text) else None

    # Telegram hard-limits message size. Long reports are split by lines.
    # We keep HTML parse mode for chunks so compact <a href=...>TV</a> links
    # stay embedded instead of expanding back to full URLs.
    if len(raw_text) > 3800:
        chunks = _chunk_text_for_telegram(raw_text, max_len=3600)
        last_result = None
        for idx, chunk in enumerate(chunks):
            suffix = f"\n\n({idx + 1}/{len(chunks)})" if len(chunks) > 1 else ""
            chunk_text = chunk + suffix
            last_result = sender.send_message(
                chunk_text,
                parse_mode=parse_mode,
                reply_markup=reply_markup if idx == len(chunks) - 1 else None,
            )
            if isinstance(last_result, dict) and not last_result.get("ok") and parse_mode:
                last_result = sender.send_message(
                    _strip_basic_html(chunk_text),
                    parse_mode=None,
                    reply_markup=reply_markup if idx == len(chunks) - 1 else None,
                )
            _telegram_send_pause(0.45)
        return last_result

    result = sender.send_message(raw_text, parse_mode=parse_mode, reply_markup=reply_markup)

    # If Telegram rejects HTML formatting, retry as plain text once.
    if isinstance(result, dict) and not result.get("ok") and parse_mode:
        plain = _strip_basic_html(raw_text)
        result = sender.send_message(plain, parse_mode=None, reply_markup=reply_markup)

    return result


def _telegram_send_pause(seconds: float | None = None) -> None:
    """Small Telegram-only pacing pause to prevent bursty messages.

    Safe by design:
    - Called only after Telegram send calls.
    - Not used inside the decision engine.
    - Not used before OKX order submission.
    """
    try:
        delay = float(TELEGRAM_SEND_GAP_SECONDS if seconds is None else seconds)
    except Exception:
        delay = TELEGRAM_SEND_GAP_SECONDS
    if delay <= 0:
        return
    time.sleep(min(delay, 2.0))


def _set_runtime_okx_orders(settings: Settings, enabled: bool) -> bool:
    try:
        setattr(settings, "okx_place_orders", bool(enabled))
        return bool(getattr(settings, "okx_place_orders")) == bool(enabled)
    except Exception:
        try:
            object.__setattr__(settings, "okx_place_orders", bool(enabled))
            return bool(getattr(settings, "okx_place_orders")) == bool(enabled)
        except Exception:
            return False


def _get_signal_delivery_mode(settings: Settings) -> str:
    # Default after restart/deploy: Simulation.
    # Explicit Railway/config value still wins if set to scan/trading/simulation.
    mode = str(getattr(settings, "signal_delivery_mode", "simulation") or "simulation").strip().lower()
    return mode if mode in {"scan", "trading", "simulation"} else "simulation"


def _set_runtime_signal_delivery_mode(settings: Settings, mode: str) -> bool:
    normalized = str(mode or "simulation").strip().lower()
    if normalized not in {"scan", "trading", "simulation"}:
        return False
    try:
        setattr(settings, "signal_delivery_mode", normalized)
        return _get_signal_delivery_mode(settings) == normalized
    except Exception:
        try:
            object.__setattr__(settings, "signal_delivery_mode", normalized)
            return _get_signal_delivery_mode(settings) == normalized
        except Exception:
            return False


def _signal_delivery_mode_label(settings: Settings) -> str:
    mode = _get_signal_delivery_mode(settings)
    if mode == "simulation":
        return "وضع المحاكاة"
    if mode == "trading":
        return "وضع التداول"
    return "وضع الاسكان"


def _is_actionable_signal_status(exec_status: str) -> bool:
    status = str(exec_status or "").strip().lower()
    return bool(
        status in {"accepted_preview", "pending_pullback_preview"}
        or status.startswith("rejected")
    )


def _should_dispatch_signal_item(item: dict, settings: Settings) -> bool:
    exec_status = str(((item or {}).get("execution") or {}).get("status") or "").strip().lower()
    if _get_signal_delivery_mode(settings) == "scan":
        is_execution = exec_status in {"accepted_preview", "pending_pullback_preview"}
        if not settings.send_normal_signals and not is_execution:
            return False
        return True
    return _is_actionable_signal_status(exec_status)


def _build_main_inline_keyboard_with_bot_modes(settings: Settings | None = None) -> dict:
    """Main /help keyboard with active runtime mode marker.

    Telegram does not support custom button background colors, so the active mode
    is marked with 🟢 while preserving the original visual logos.
    """
    try:
        runtime_settings = settings or get_settings()
        mode = _get_signal_delivery_mode(runtime_settings)
    except Exception:
        mode = "scan"

    def active(name: str, label: str) -> str:
        return f"🟢{label}" if mode == name else label

    return {
        "inline_keyboard": [
            [
                {"text": active("trading", "🚀 Execution"), "callback_data": "menu:execution"},
                {"text": active("scan", "📊 Normal Trades"), "callback_data": "menu:normal"},
                {"text": active("simulation", "🧪 Simulation"), "callback_data": "menu:simulation"},
            ],
            [
                {"text": active("trading", "🧠🚀 Exec Intel"), "callback_data": "cmd:/report_execution_intelligence"},
                {"text": active("scan", "🧠📊 Market Intel"), "callback_data": "cmd:/report_intelligence"},
                {"text": active("simulation", "🧠🧪 Sim Intel"), "callback_data": "cmd:/report_simulation_intelligence"},
            ],
            [
                {"text": "🧭 أوضاع البوت", "callback_data": "menu:bot_modes"},
            ],
            [
                {"text": "🧠 Diagnostics", "callback_data": "menu:diagnostics"},
                {"text": "🤖 OKX Control", "callback_data": "menu:okx_control"},
            ],
            [
                {"text": "⚙️ Admin", "callback_data": "menu:admin"},
                {"text": "📘 System Info", "callback_data": "menu:system_info"},
            ],
        ]
    }



def _build_bot_modes_panel(settings: Settings) -> str:
    mode = _get_signal_delivery_mode(settings)
    mode_label = _signal_delivery_mode_label(settings)

    def mark(name: str) -> str:
        return "✅" if mode == name else "⬜"

    return "\n".join([
        "🧭 <b>أوضاع البوت</b>",
        "━━━━━━━━━━━━",
        f"الحالي: <b>{mode_label}</b>",
        "",
        f"{mark('scan')} 📡 <b>وضع الاسكان</b>",
        "يعرض العادي + المرشح وينفذ حسب إعدادات OKX.",
        "",
        f"{mark('trading')} 🎯 <b>وضع التداول</b>",
        "يعرض المرشح و rejected ورسائل OKX فقط.",
        "",
        f"{mark('simulation')} 🧪 <b>وضع المحاكاة</b>",
        "نفس قرارات وضع التداول لكن تنفيذ داخلي فقط، و OKX live orders OFF.",
    ])


def _build_bot_modes_keyboard(settings: Settings | None = None) -> dict:
    try:
        runtime_settings = settings or get_settings()
        mode = _get_signal_delivery_mode(runtime_settings)
    except Exception:
        mode = "scan"

    def mark(name: str, label: str) -> str:
        return f"🟢{label}" if mode == name else f"⚪{label}"

    return {
        "inline_keyboard": [
            [
                {"text": mark("scan", "📡 وضع الاسكان"), "callback_data": "signal_mode:scan"},
                {"text": mark("trading", "🎯 وضع التداول"), "callback_data": "signal_mode:trading"},
            ],
            [
                {"text": mark("simulation", "🧪 وضع المحاكاة"), "callback_data": "signal_mode:simulation"},
            ],
            [
                {"text": "🤖 OKX Control", "callback_data": "menu:okx_control"},
                {"text": "🔄 تحديث", "callback_data": "menu:bot_modes"},
            ],
        ]
    }




def _build_okx_control_keyboard(settings: Settings) -> dict:
    orders_on = bool(getattr(settings, "okx_place_orders", False))
    toggle_text = "⏸ إيقاف تنفيذ OKX" if orders_on else "▶️ تشغيل تنفيذ OKX"
    toggle_data = "okx_orders:off" if orders_on else "okx_orders:on"

    signal_mode = _get_signal_delivery_mode(settings)

    return {
        "inline_keyboard": [
            [{"text": toggle_text, "callback_data": toggle_data}],
            [
                {"text": "📡 وضع الاسكان", "callback_data": "signal_mode:scan"},
                {"text": "🎯 وضع التداول", "callback_data": "signal_mode:trading"},
            ],
            [
                {"text": "🧪 وضع المحاكاة", "callback_data": "signal_mode:simulation"},
            ],
            [
                {"text": "📘 حالة OKX", "callback_data": "cmd:/okx_status"},
                {"text": "🔄 تحديث", "callback_data": "menu:okx_control"},
            ],
        ]
    }



def _build_okx_control_panel(settings: Settings) -> str:
    runtime_status = "ON" if bool(getattr(settings, "okx_place_orders", False)) else "OFF"
    live_guard = "ALLOWED" if bool(getattr(settings, "allow_live_trading", False)) else "BLOCKED"
    simulated = "ON" if bool(getattr(settings, "okx_simulated", True)) else "OFF"
    signal_mode = _signal_delivery_mode_label(settings)
    return "\n".join([
        build_okx_control_help(),
        "",
        "⚙️ <b>Runtime OKX Control</b>",
        f"• OKX Orders: <b>{runtime_status}</b>",
        f"• Signal Mode: <b>{signal_mode}</b>",
        f"• Simulated Mode: <b>{simulated}</b>",
        f"• Live Trading Guard: <b>{live_guard}</b>",
        "• وضع الاسكان: يعرض العادي + المرشح وينفذ حسب إعدادات OKX",
        "• وضع التداول: المرشح + كل rejected + رسائل OKX فقط",
        "• وضع المحاكاة: نفس شروط التداول لكن بدون أي OKX live order",
        "• المحاكاة لا تغلق الصفقات الحقيقية المفتوحة ولا تمس إدارتها.",
    ])


def _format_clean_preview(stats: dict, title: str, confirm_command: str) -> str:
    if not stats.get("enabled"):
        return "⚠️ Redis غير متاح حاليًا — لا يمكن تنفيذ التنظيف."
    lines = [
        title,
        "┄┄┄┄┄┄┄┄",
        f"Prefix: {stats.get('prefix', 'n/a')}",
        f"Open set: {stats.get('open_set', 0)}",
        f"History set: {stats.get('history_set', 0)}",
        f"Trade keys: {stats.get('trade_keys', 0)}",
        f"Execution checks: {stats.get('execution_checks', 0)}",
        f"Signal fingerprints: {stats.get('signal_fingerprints', 0)}",
    ]
    if stats.get("mode") == "deep":
        lines += [
            f"Keys to delete: {stats.get('keys_to_delete', 0)}",
            "",
            "⚠️ Deep Clean سيمسح بيانات البوت تحت نفس Prefix ويبدأ baseline جديد.",
            f"للتأكيد أرسل: {confirm_command}",
        ]
    else:
        lines += [
            f"Stale open candidates: {stats.get('stale_open_candidates', 0)}",
            f"Old execution checks: {stats.get('old_execution_checks', 0)}",
            "",
            "🧹 Soft Clean ينظف القديم/المعطوب فقط ولا يمسح كل التاريخ.",
            f"للتأكيد أرسل: {confirm_command}",
        ]
    if stats.get("error"):
        lines.append(f"Error: {stats.get('error')}")
    return "\n".join(lines)


def _format_clean_result(stats: dict, title: str) -> str:
    if not stats.get("enabled"):
        return "⚠️ Redis غير متاح حاليًا — لم يتم تنفيذ التنظيف."
    lines = [title, "┄┄┄┄┄┄┄┄"]
    if stats.get("mode") == "deep":
        deleted = int(stats.get("deleted_keys", 0) or 0)
        remaining = int(stats.get("remaining_keys", 0) or 0)
        attempted = int(stats.get("delete_attempted", deleted) or 0)
        lines.append(f"Delete Attempted: {attempted}")
        lines.append(f"Deleted keys: {deleted}")
        if "current_namespace_keys" in stats:
            lines.append(f"Current namespace keys: {int(stats.get('current_namespace_keys', 0) or 0)}")
        lines.append(f"Remaining keys: {remaining}")
        if stats.get("error"):
            lines.append(f"⚠️ Error: {stats.get('error')}")
        elif deleted > 0 and remaining == 0:
            lines.append("✅ تم مسح بيانات Redis الخاصة بالبوت بالكامل.")
        elif deleted > 0 and remaining > 0:
            lines.append("⚠️ تم حذف جزء من البيانات لكن ما زالت هناك مفاتيح متبقية.")
            lines.append("🔁 أعد تشغيل /deep_clean_confirm مرة أخرى أو راجع Redis namespace.")
        else:
            lines.append("⚠️ لم يتم العثور على مفاتيح للحذف، لذلك لم يتم تصفير Redis فعليًا.")
    else:
        lines += [
            f"Removed open members: {stats.get('removed_open_members', 0)}",
            f"Deleted stale trade keys: {stats.get('deleted_trade_keys', 0)}",
            f"Removed old checks: {stats.get('removed_execution_checks', 0)}",
            f"Kept checks: {stats.get('kept_execution_checks', 0)}",
            "✅ تم تنظيف البيانات القديمة/المعطوبة فقط.",
        ]
        if stats.get("error"):
            lines.append(f"⚠️ Error: {stats.get('error')}")
    return "\n".join(lines)


def _reset_runtime_state_after_clean(result: dict, *, keep_mode_state: bool = True) -> None:
    if not isinstance(result, dict):
        return

    empty_trades: list = []
    empty_execution_results: list[dict] = []
    empty_signal_items: list[dict] = []

    portfolio_state_inputs = dict(result.get("portfolio_state_inputs", {}) or {})
    execution_report_kwargs = _execution_report_balance_kwargs(portfolio_state_inputs)
    reports = build_report_bundle(
        empty_trades,
        empty_execution_results,
        empty_signal_items,
        **execution_report_kwargs,
    )
    command_outputs = build_command_outputs(
        empty_trades,
        empty_execution_results,
        empty_signal_items,
        **execution_report_kwargs,
    )

    result["trades"] = empty_trades
    result["signal_items"] = empty_signal_items
    result["signals"] = []
    result["execution_results"] = empty_execution_results
    result["current_execution_results"] = []
    result["technical_snapshot_written"] = 0
    result["command_outputs"] = command_outputs
    result.update(reports)

    portfolio_state = build_portfolio_state_from_trades(empty_trades)
    drawdown_status = evaluate_drawdown(portfolio_state)
    result["portfolio_state"] = portfolio_state
    result["drawdown_status"] = drawdown_status
    result["drawdown_report"] = build_drawdown_report(portfolio_state)
    result["loss_streak_guard"] = _build_loss_streak_guard(empty_trades)

    if not keep_mode_state:
        result["mode"] = MODE_NORMAL_LONG



def _build_admin_panel() -> str:
    base = build_admin_help()
    reset_lines = [
        "",
        "━━━━━━━━━━━━",
        "🧹 <b>Reset Reports</b>",
        "━━━━━━━━━━━━",
        "الأوامر التالية تعمل Preview أولًا ثم تحتاج Confirm.",
        "",
        "🚀 <b>Execution</b>",
        "/reset_reports_execution",
        "/confirm_reset_reports_execution",
        "",
        "📊 <b>Normal</b>",
        "/reset_reports_normal",
        "/confirm_reset_reports_normal",
        "",
        "🧪 <b>Simulation</b>",
        "/reset_reports_simulation",
        "/confirm_reset_reports_simulation",
        "",
        "🧹 <b>All Reports</b>",
        "/reset_reports_all",
        "/confirm_reset_reports_all",
        "",
        "⚠️ لا تمسح هذه الأوامر: whitelist/config/replay/snapshots/mode state.",
    ]
    return "\n".join([str(base or "").rstrip(), *reset_lines])


def _is_simulation_trade_record(trade) -> bool:
    return str(getattr(trade, "trade_source", "") or "").strip().lower() == "simulation"


def _is_execution_report_trade_record(trade) -> bool:
    if _is_simulation_trade_record(trade):
        return False
    return bool(
        getattr(trade, "execution_trade", False)
        or str(getattr(trade, "tracking_bucket", "") or "").strip().lower() == "execution"
    )


def _is_normal_report_trade_record(trade) -> bool:
    return not _is_simulation_trade_record(trade) and not _is_execution_report_trade_record(trade)


def _delete_redis_keys_by_patterns(trade_store: RedisTradeStore | None, patterns: list[str]) -> int:
    if not trade_store or not getattr(trade_store, "enabled", False) or not getattr(trade_store, "client", None):
        return 0
    client = trade_store.client
    keys: set[str] = set()
    try:
        for pattern in patterns:
            for key in client.scan_iter(pattern):
                keys.add(str(key))
        if keys:
            return int(client.delete(*sorted(keys)) or 0)
    except Exception as exc:
        print(f"⚠️ reset redis delete failed: {exc}", flush=True)
    return 0


def _reset_reports_preview(kind: str, trade_store: RedisTradeStore | None, result: dict | None = None) -> dict:
    live_trades = []
    if trade_store:
        try:
            live_trades = trade_store.load_trades() or []
        except Exception:
            live_trades = []
    if not live_trades and result is not None:
        live_trades = list(result.get("trades", []) or [])

    sim_trades = _load_simulation_trades(trade_store)
    if not sim_trades and result is not None:
        sim_trades = list(result.get("simulation_trades", []) or [])

    execution_count = sum(1 for t in live_trades if _is_execution_report_trade_record(t))
    normal_count = sum(1 for t in live_trades if _is_normal_report_trade_record(t))
    simulation_count = len(sim_trades)

    return {
        "enabled": bool(trade_store and getattr(trade_store, "enabled", False)),
        "kind": kind,
        "execution": execution_count,
        "normal": normal_count,
        "simulation": simulation_count,
        "total_live": len(live_trades),
    }


def _format_reset_reports_preview(stats: dict, confirm_command: str, title: str) -> str:
    lines = [
        title,
        "━━━━━━━━━━━━",
        f"Redis: {'ON' if stats.get('enabled') else 'OFF / runtime only'}",
        f"Execution report trades: {int(stats.get('execution', 0) or 0)}",
        f"Normal report trades: {int(stats.get('normal', 0) or 0)}",
        f"Simulation report trades: {int(stats.get('simulation', 0) or 0)}",
        "",
        "⚠️ هذا Preview فقط.",
        f"للتنفيذ أرسل: {confirm_command}",
    ]
    return "\n".join(lines)


def _format_reset_reports_done(stats: dict, title: str) -> str:
    lines = [
        title,
        "━━━━━━━━━━━━",
        f"Kept live trades: {int(stats.get('kept_live', 0) or 0)}",
        f"Removed execution: {int(stats.get('removed_execution', 0) or 0)}",
        f"Removed normal: {int(stats.get('removed_normal', 0) or 0)}",
        f"Removed simulation: {int(stats.get('removed_simulation', 0) or 0)}",
        f"Deleted simulation Redis keys: {int(stats.get('deleted_sim_keys', 0) or 0)}",
        "",
        "✅ تم تصفير التقارير المطلوبة بدون لمس الإعدادات أو whitelist أو replay/snapshots.",
    ]
    return "\n".join(lines)


def _refresh_runtime_after_report_reset(result: dict | None, trade_store: RedisTradeStore | None = None) -> None:
    if not isinstance(result, dict):
        return

    refreshed_trades = trade_store.load_trades() if trade_store else list(result.get("trades", []) or [])
    refreshed_checks = trade_store.load_execution_checks(limit=500) if trade_store else list(result.get("execution_results", []) or [])
    refreshed_sim_trades = _load_simulation_trades(trade_store)

    result["trades"] = refreshed_trades
    result["simulation_trades"] = refreshed_sim_trades
    result["simulation_wallet"] = _build_simulation_wallet_snapshot(refreshed_sim_trades)
    result["simulation_daily_balance"] = _ensure_simulation_daily_log(refreshed_sim_trades, trade_store=trade_store) if trade_store else _ensure_simulation_daily_log(refreshed_sim_trades)
    result["simulation_daily_log"] = _load_simulation_daily_log(trade_store)
    result["simulation_execution_results"] = _load_simulation_execution_checks(trade_store, limit=500) if trade_store else []
    result["simulation_signal_items"] = []
    result["signal_items"] = []
    result["signals"] = []
    result["execution_results"] = refreshed_checks
    result["current_execution_results"] = []

    portfolio_state_inputs = dict(result.get("portfolio_state_inputs", {}) or {})
    execution_report_kwargs = _execution_report_balance_kwargs(portfolio_state_inputs)
    reports = build_report_bundle(refreshed_trades, refreshed_checks, [], **execution_report_kwargs)
    result["command_outputs"] = build_command_outputs(refreshed_trades, refreshed_checks, [], **execution_report_kwargs)
    result.update(reports)

    portfolio_state_inputs = dict(result.get("portfolio_state_inputs", {}) or {})
    portfolio_state = build_portfolio_state_from_trades(refreshed_trades, **portfolio_state_inputs)
    result["portfolio_state"] = portfolio_state
    result["drawdown_status"] = evaluate_drawdown(portfolio_state)
    result["drawdown_report"] = build_drawdown_report(portfolio_state)
    result["loss_streak_guard"] = _build_loss_streak_guard(refreshed_trades)


def _reset_reports_confirm(kind: str, trade_store: RedisTradeStore | None, result: dict | None = None) -> dict:
    stats = _reset_reports_preview(kind, trade_store, result)
    live_trades = []
    if trade_store:
        try:
            live_trades = trade_store.load_trades() or []
        except Exception:
            live_trades = []
    if not live_trades and result is not None:
        live_trades = list(result.get("trades", []) or [])

    kept_live = []
    removed_execution = 0
    removed_normal = 0

    for trade in live_trades:
        is_exec = _is_execution_report_trade_record(trade)
        is_norm = _is_normal_report_trade_record(trade)

        remove = False
        if kind in {"execution", "all"} and is_exec:
            remove = True
            removed_execution += 1
        elif kind in {"normal", "all"} and is_norm:
            remove = True
            removed_normal += 1

        if not remove:
            kept_live.append(trade)

    if trade_store and getattr(trade_store, "enabled", False):
        try:
            trade_store.save_trades(kept_live)
        except Exception as exc:
            print(f"⚠️ save after report reset failed: {exc}", flush=True)

    if result is not None:
        result["trades"] = kept_live

    removed_simulation = 0
    deleted_sim_keys = 0
    if kind in {"simulation", "all"}:
        sim_trades = _load_simulation_trades(trade_store)
        if not sim_trades and result is not None:
            sim_trades = list(result.get("simulation_trades", []) or [])
        removed_simulation = len(sim_trades)
        deleted_sim_keys = _delete_redis_keys_by_patterns(
            trade_store,
            [
                f"{SIMULATION_REDIS_PREFIX}:*",
            ],
        )
        if result is not None:
            result["simulation_trades"] = []
            result["simulation_execution_results"] = []
            result["simulation_signal_items"] = []
            result["simulation_wallet"] = _build_simulation_wallet_snapshot([])
            result["simulation_daily_balance"] = _ensure_simulation_daily_log([], trade_store=trade_store)
            result["simulation_daily_log"] = _load_simulation_daily_log(trade_store)

    _refresh_runtime_after_report_reset(result, trade_store=trade_store)

    stats.update({
        "kept_live": len(kept_live),
        "removed_execution": removed_execution,
        "removed_normal": removed_normal,
        "removed_simulation": removed_simulation,
        "deleted_sim_keys": deleted_sim_keys,
    })
    return stats


def _handle_admin_clean_command(
    command: str,
    trade_store: RedisTradeStore | None,
    result: dict | None = None,
) -> str | None:
    reset_preview_commands = {
        "/reset_reports_execution": ("execution", "/confirm_reset_reports_execution", "🚀 Reset Execution Reports Preview"),
        "/reset_reports_normal": ("normal", "/confirm_reset_reports_normal", "📊 Reset Normal Reports Preview"),
        "/reset_reports_simulation": ("simulation", "/confirm_reset_reports_simulation", "🧪 Reset Simulation Reports Preview"),
        "/reset_reports_all": ("all", "/confirm_reset_reports_all", "🧹 Reset All Reports Preview"),
    }
    if command in reset_preview_commands:
        kind, confirm_command, title = reset_preview_commands[command]
        return _format_reset_reports_preview(
            _reset_reports_preview(kind, trade_store, result),
            confirm_command,
            title,
        )

    reset_confirm_commands = {
        "/confirm_reset_reports_execution": ("execution", "🚀 Reset Execution Reports Done"),
        "/confirm_reset_reports_normal": ("normal", "📊 Reset Normal Reports Done"),
        "/confirm_reset_reports_simulation": ("simulation", "🧪 Reset Simulation Reports Done"),
        "/confirm_reset_reports_all": ("all", "🧹 Reset All Reports Done"),
    }
    if command in reset_confirm_commands:
        kind, title = reset_confirm_commands[command]
        return _format_reset_reports_done(
            _reset_reports_confirm(kind, trade_store, result),
            title,
        )

    if command in {"/soft_clean", "/soft_clean_preview"}:
        stats = trade_store.clean_preview("soft") if trade_store else {"enabled": False}
        return _format_clean_preview(stats, "🧹 Soft Clean Preview", "/soft_clean_confirm")
    if command == "/soft_clean_confirm":
        stats = trade_store.soft_clean() if trade_store else {"enabled": False, "mode": "soft"}
        if result is not None and stats.get("enabled"):
            refreshed_trades = trade_store.load_trades() if trade_store else []
            refreshed_checks = trade_store.load_execution_checks(limit=500) if trade_store else []
            portfolio_state_inputs = dict(result.get("portfolio_state_inputs", {}) or {})
            execution_report_kwargs = _execution_report_balance_kwargs(portfolio_state_inputs)
            reports = build_report_bundle(refreshed_trades, refreshed_checks, [], **execution_report_kwargs)
            result["trades"] = refreshed_trades
            result["signal_items"] = []
            result["signals"] = []
            result["execution_results"] = refreshed_checks
            result["current_execution_results"] = []
            result["command_outputs"] = build_command_outputs(refreshed_trades, refreshed_checks, [], **execution_report_kwargs)
            result.update(reports)
            portfolio_state_inputs = dict(result.get("portfolio_state_inputs", {}) or {})
            portfolio_state = build_portfolio_state_from_trades(refreshed_trades, **portfolio_state_inputs)
            result["portfolio_state"] = portfolio_state
            result["drawdown_status"] = evaluate_drawdown(portfolio_state)
            result["drawdown_report"] = build_drawdown_report(portfolio_state)
            result["loss_streak_guard"] = _build_loss_streak_guard(refreshed_trades)
        return _format_clean_result(stats, "🧹 Soft Clean Done")
    if command in {"/deep_clean", "/deep_clean_preview"}:
        stats = trade_store.clean_preview("deep") if trade_store else {"enabled": False}
        return _format_clean_preview(stats, "🧨 Deep Clean Preview", "/deep_clean_confirm")
    if command == "/deep_clean_confirm":
        stats = trade_store.deep_clean() if trade_store else {"enabled": False, "mode": "deep"}
        if result is not None and stats.get("enabled"):
            _reset_runtime_state_after_clean(result, keep_mode_state=True)
        return _format_clean_result(stats, "🧨 Deep Clean Done")
    return None


def _handle_callback_query(sender: TelegramSender, result: dict, callback_query: dict, settings: Settings | None = None, okx_client: OKXTradeClient | None = None) -> None:
    callback_id = str(callback_query.get("id") or "")
    data = str(callback_query.get("data") or "")
    if callback_id:
        sender.answer_callback_query(callback_id, "Opened")

    if data.startswith("okx_orders:"):
        desired = data.split(":", 1)[1].strip().lower()
        desired_enabled = desired == "on"
        runtime_settings = settings or get_settings()
        applied = _set_runtime_okx_orders(runtime_settings, desired_enabled)
        state_text = "ON" if desired_enabled else "OFF"
        prefix = "✅" if applied else "⚠️"
        _send_text(
            sender,
            "\n".join([
                f"{prefix} OKX Orders Runtime Toggle",
                "┄┄┄┄┄┄┄┄",
                f"Requested State: {state_text}",
                f"Applied: {'YES' if applied else 'NO'}",
            ]),
            reply_markup=_build_okx_control_keyboard(runtime_settings),
        )
        return

    if data.startswith("signal_mode:"):
        desired_mode = data.split(":", 1)[1].strip().lower()
        runtime_settings = settings or get_settings()
        applied = _set_runtime_signal_delivery_mode(runtime_settings, desired_mode)
        if desired_mode == "simulation":
            _set_runtime_okx_orders(runtime_settings, False)
        mode_text = _signal_delivery_mode_label(runtime_settings)
        prefix = "✅" if applied else "⚠️"
        _send_text(
            sender,
            "\n".join([
                f"{prefix} Bot Mode Runtime Toggle",
                "┄┄┄┄┄┄┄┄",
                f"Requested Mode: {desired_mode.upper() if desired_mode else '-'}",
                f"Applied: {'YES' if applied else 'NO'}",
                f"Current Mode: {mode_text}",
            ]),
            reply_markup=_build_bot_modes_keyboard(runtime_settings),
        )
        return

    if data.startswith("track:"):
        symbol = data.split(":", 1)[1]
        matching_trade = None
        for trade in result.get("trades", []):
            if getattr(trade, "symbol", "") == symbol:
                matching_trade = trade
                if not getattr(trade, "is_closed", False):
                    break
        for item in result.get("signal_items", []):
            signal = item.get("signal")
            if signal and signal.symbol == symbol:
                _send_text(sender, build_track_message(signal, item.get("execution"), trade=matching_trade))
                return
        if matching_trade is not None:
            _send_text(sender, build_track_message(None, None, trade=matching_trade))
            return
        sender.send_message("📊 Track\n┄┄┄┄┄┄┄┄\nلم أجد هذه الصفقة في آخر دورة Scan أو في سجل Redis.")
        return

    if data.startswith("menu:"):
        key = data.split(":", 1)[1]
        if key == "execution":
            _send_text(sender, result.get("help_execution", ""))
        elif key == "normal":
            _send_text(sender, result.get("help_normal", ""))
        elif key == "simulation":
            _send_text(sender, _build_simulation_help())
        elif key == "diagnostics":
            _send_text(sender, build_diagnostics_help())
        elif key == "bot_modes":
            runtime_settings = settings or get_settings()
            _send_text(sender, _build_bot_modes_panel(runtime_settings), reply_markup=_build_bot_modes_keyboard())
        elif key == "okx_control":
            runtime_settings = settings or get_settings()
            _send_text(sender, _build_okx_control_panel(runtime_settings), reply_markup=_build_okx_control_keyboard(runtime_settings))
        elif key == "admin":
            _send_text(sender, _build_admin_panel())
        elif key == "system_info":
            _send_text(sender, _build_fast_status(result, settings or get_settings()))
        else:
            sender.send_message("القسم غير متاح حاليًا.")
        return

    if data.startswith("cmd:"):
        command = data.split(":", 1)[1]
        if command == "/okx_status":
            _send_text(sender, _build_okx_status_panel(settings or get_settings(), okx_client=okx_client))
            return
        simulation_outputs = _build_simulation_command_outputs(result)
        reply = (
            simulation_outputs.get(command)
            or result.get("command_outputs", {}).get(command)
            or "الأمر غير متاح في هذه النسخة."
        )
        _send_text(sender, reply)
        return


def _build_simulation_help() -> str:
    return "\n".join([
        "🧪 <b>Simulation Trading Reports</b>",
        "━━━━━━━━━━━━",
        "📊 التقرير العام",
        "/report_simulation",
        "/report_simulation_7d",
        "/report_simulation_today",
        "/report_simulation_1h",
        "",
        "📂 الصفقات المفتوحة",
        "/report_simulation_open",
        "/report_simulation_open_7d",
        "/report_simulation_open_today",
        "/report_simulation_open_1h",
        "",
        "📈 تحليل أسباب الأرباح",
        "/report_simulation_profit_analysis",
        "/report_simulation_profit_analysis_7d",
        "/report_simulation_profit_analysis_today",
        "/report_simulation_profit_analysis_1h",
        "",
        "📉 تحليل أسباب الخسائر",
        "/report_simulation_losses_analysis",
        "/report_simulation_losses_analysis_7d",
        "/report_simulation_losses_analysis_today",
        "/report_simulation_losses_analysis_1h",
        "",
        "💼 Wallet Impact",
        "/report_simulation_wallet",
        "/simulation_wallet",
        "",
        "📅 رصيد بداية اليوم",
        "/report_simulation_daily_balance",
        "/simulation_daily_balance",
        "",
        "🧠 ذكاء التنفيذ",
        "/report_simulation_intelligence",
        "/report_simulation_intelligence_7d",
        "/report_simulation_intelligence_today",
        "/report_simulation_intelligence_1h",
        "",
        "⚙️ تشخيص التنفيذ",
        "/report_simulation_diagnostics",
        "/report_simulation_diagnostics_7d",
        "/report_simulation_diagnostics_today",
        "/report_simulation_diagnostics_1h",
    ])


def _build_unified_help_reply(result: dict, settings: Settings) -> str:
    """Restore full /help command list while showing current runtime mode."""
    base_help = str(result.get("help") or "OKX Long Bot Dashboard")
    execution_help = str(result.get("help_execution") or build_execution_help())
    normal_help = str(result.get("help_normal") or build_normal_help())

    sections = [
        base_help,
        "",
        "━━━━━━━━━━━━",
        "📌 <b>الأوامر الرئيسية</b>",
        "━━━━━━━━━━━━",
        "/status — حالة البوت والتنفيذ",
        "/mood — حالة السوق الحالية",
        "/okx_control — لوحة أوضاع OKX",
        "/help_execution — تقارير صفقات التنفيذ",
        "/help_normal — تقارير الرسائل العادية",
        "/diagnostics_help — أوامر التشخيص",
        "",
        execution_help,
        "",
        normal_help,
        "",
        _build_simulation_help(),
    ]

    return "\n".join(part for part in sections if str(part).strip())



def _send_full_help_messages(sender: TelegramSender, result: dict, settings: Settings) -> None:
    """Send restored dashboard /help with main keyboard."""
    dashboard = build_master_help(
        mode=result.get("mode", "UNKNOWN"),
        execution_enabled=settings.execution_enabled,
        risk_enabled=True,
        okx_orders=settings.okx_place_orders,
    )

    sender.send_message(
        "⌨️ تم إغلاق لوحة /help القديمة.",
        reply_markup={"remove_keyboard": True},
    )

    sender.send_message(
        dashboard,
        reply_markup=_build_main_inline_keyboard_with_bot_modes(settings),
    )




def _answer_commands(sender: TelegramSender, result: dict, offset: int | None, settings: Settings, trade_store: RedisTradeStore | None = None, okx_client: OKXTradeClient | None = None) -> int | None:
    updates = sender.get_updates(offset=offset, timeout_seconds=0)
    if not updates.get("ok"):
        return offset

    command_outputs = result.get("command_outputs", {})
    for update in updates.get("result", []):
        offset = int(update.get("update_id", 0)) + 1
        callback_query = update.get("callback_query")
        if callback_query:
            _handle_callback_query(sender, result, callback_query, settings, okx_client=okx_client)
            continue

        message = update.get("message") or update.get("channel_post") or {}
        text = str(message.get("text") or "")
        commands = _extract_commands(text)
        plain_text = text.strip()

        if not commands and plain_text:
            button_map = {
                "🚀 Execution": "/help_execution",
                "🟢🚀 Execution": "/help_execution",
                "Execution": "/help_execution",
                "📊 Normal Trades": "/help_normal",
                "🟢📊 Normal Trades": "/help_normal",
                "Normal Trades": "/help_normal",
                "🧪 Simulation": "/help_simulation",
                "🟢🧪 Simulation": "/help_simulation",
                "Simulation": "/help_simulation",
                "🧠🚀 Execution Intelligence": "/report_execution_intelligence",
                "🟢🧠🚀 Exec Intel": "/report_execution_intelligence",
                "🧠🚀 Exec Intel": "/report_execution_intelligence",
                "Exec Intelligence": "/report_execution_intelligence",
                "🧠📊 Market Intelligence": "/report_intelligence",
                "🟢🧠📊 Market Intel": "/report_intelligence",
                "🧠📊 Market Intel": "/report_intelligence",
                "Market Intelligence": "/report_intelligence",
                "🧠🧪 Sim Intel": "/report_simulation_intelligence",
                "🟢🧠🧪 Sim Intel": "/report_simulation_intelligence",
                "🧭 أوضاع البوت": "/bot_modes",
                "Bot Modes": "/bot_modes",
                "اوضاع البوت": "/bot_modes",
                "🧠 Diagnostics": "/report_diagnostics",
                "Diagnostics": "/report_diagnostics",
                "🤖 OKX Control": "/okx_control",
                "OKX Control": "/okx_control",
                "📘 حالة OKX": "/okx_status",
                "حالة OKX": "/okx_status",
                "OKX Status": "/okx_status",
                "⚙️ Admin": "/admin",
                "Admin": "/admin",
                "📘 System Info": "/status",
                "System Info": "/status",
            }
            mapped = button_map.get(plain_text)
            if mapped:
                commands = [mapped]

        if not commands:
            continue

        for command in commands:
            clean_reply = _handle_admin_clean_command(command, trade_store, result)
            if clean_reply is not None:
                _send_text(sender, clean_reply)
                continue

            # /help must always use the original dashboard + main keyboard.
            # Simulation reports are handled only by /help_simulation or /report_simulation*
            # and must never shadow /help.
            if command in ("/start", "/help"):
                reply = result.get("help") or "OKX Long Bot is running."
                sender.send_message("⌨️ تم إغلاق لوحة /help القديمة.", reply_markup={"remove_keyboard": True})
                sender.send_message(reply, reply_markup=_build_main_inline_keyboard_with_bot_modes(settings))
                continue

            simulation_outputs = _build_simulation_command_outputs(result)
            if command in simulation_outputs:
                _send_text(sender, simulation_outputs[command])
                continue
            if command in ("/diagnostics_help", "/help_diagnostics"):
                _send_text(sender, build_diagnostics_commands_help())
                continue
            if command == "/tech_snapshot_on":
                status = set_snapshot_enabled(True, settings, redis_client=_snapshot_redis_client(trade_store))
                _send_text(sender, "✅ Technical Snapshot: ON" if status.get("ok") else f"⚠️ لم أستطع تشغيل التسجيل: {status.get('error')}")
                continue
            if command == "/tech_snapshot_off":
                status = set_snapshot_enabled(False, settings, redis_client=_snapshot_redis_client(trade_store))
                _send_text(sender, "⏸ Technical Snapshot: OFF" if status.get("ok") else f"⚠️ لم أستطع إيقاف التسجيل: {status.get('error')}")
                continue
            if command == "/tech_snapshot_status":
                _send_text(sender, build_technical_dataset_status(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/tech_snapshot_export":
                _send_text(sender, build_technical_dataset_export(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/tech_snapshot_export_file":
                export = build_technical_dataset_export_file(settings, redis_client=_snapshot_redis_client(trade_store))
                if not export.get("ok"):
                    _send_text(sender, "⚠️ " + str(export.get("message") or "Technical snapshot export failed."))
                else:
                    # ✅ FIX 1b: doc_result بدل result — يمنع shadowing على scan result
                    doc_result = sender.send_document(str(export.get("path")), caption=str(export.get("caption") or "Live Technical Snapshot Dataset"))
                    if not doc_result.get("ok"):
                        _send_text(sender, "⚠️ فشل إرسال الملف عبر Telegram. الملف جاهز على السيرفر:\n" + str(export.get("path")) + "\nError: " + str(doc_result.get("error") or doc_result))
                continue
            if command == "/tech_snapshot_clear":
                _send_text(sender, build_clear_snapshot_result(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/gate_suggestions":
                _send_text(sender, build_gate_suggestions_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/gate_sim_normal":
                _send_gate_sim_artifact(sender, "normal", settings, trade_store=trade_store)
                continue
            if command == "/gate_sim_recovery":
                _send_gate_sim_artifact(sender, "recovery", settings, trade_store=trade_store)
                continue
            if command == "/gate_sim_strong":
                _send_gate_sim_artifact(sender, "strong", settings, trade_store=trade_store)
                continue
            if command == "/gate_sim_block":
                _send_gate_sim_artifact(sender, "block", settings, trade_store=trade_store)
                continue
            if command == "/gate_sim_all":
                # ✅ FIX Phase 3: Lock بدل global bool — thread-safe
                if not _GATE_SIM_LOCK.acquire(blocking=False):
                    _send_text(sender, "⏳ Gate Simulation شغال بالفعل. استنى النتيجة الحالية قبل تشغيل أمر جديد.")
                    continue
                try:
                    _send_text(sender, "⏳ جاري تحليل /gate_sim_all على replay + live snapshots... قد يستغرق عدة دقائق مع 90d.")
                    artifact = build_gate_sim_all_artifact(settings, redis_client=_snapshot_redis_client(trade_store))
                    _send_text(sender, artifact.get("text") or "⚠️ Gate simulation failed.")
                    if artifact.get("ok") and artifact.get("path"):
                        doc_result = sender.send_document(str(artifact.get("path")), caption=str(artifact.get("caption") or "Gate Simulation JSON"))
                        if not doc_result.get("ok"):
                            _send_text(sender, "⚠️ فشل إرسال ملف JSON. الملف جاهز على السيرفر:\n" + str(artifact.get("path")) + "\nError: " + str(doc_result.get("error") or doc_result))
                finally:
                    _GATE_SIM_LOCK.release()
                continue
            if command == "/score_calibration":
                _send_text(sender, "⏳ جاري حساب Score Calibration بين replay و live snapshots...")
                _send_text(sender, build_score_calibration_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/mode_coverage":
                _send_text(sender, "⏳ جاري حساب Mode Coverage بين replay و live snapshots...")
                _send_text(sender, build_mode_coverage_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/historical_report":
                _send_text(sender, build_historical_report(settings))
                continue
            if command == "/help_technical_dataset":
                _send_text(sender, build_technical_dataset_help())
                continue
            if command == "/help_historical_replay":
                _send_text(sender, build_historical_replay_help())
                continue
            if command == "/replay_start_30d":
                _send_text(sender, build_replay_start_report(settings, redis_client=_snapshot_redis_client(trade_store), days=30))
                continue
            if command == "/replay_start_45d":
                _send_text(sender, build_replay_start_report(settings, redis_client=_snapshot_redis_client(trade_store), days=45))
                continue
            if command == "/replay_start_90d":
                _send_text(sender, build_replay_start_report(settings, redis_client=_snapshot_redis_client(trade_store), days=90))
                continue
            if command == "/replay_status":
                _send_text(sender, build_replay_status_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/replay_stop":
                _send_text(sender, build_replay_stop_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/replay_export":
                _send_text(sender, build_replay_export_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/replay_export_file":
                export = build_replay_export_file(settings, redis_client=_snapshot_redis_client(trade_store))
                if not export.get("ok"):
                    _send_text(sender, "⚠️ " + str(export.get("message") or "Replay export failed."))
                else:
                    # ✅ FIX 1d: doc_result بدل result — يمنع shadowing على scan result
                    doc_result = sender.send_document(str(export.get("path")), caption=str(export.get("caption") or "Historical Replay Dataset"))
                    if not doc_result.get("ok"):
                        _send_text(sender, "⚠️ فشل إرسال الملف عبر Telegram. الملف جاهز على السيرفر:\n" + str(export.get("path")) + "\nError: " + str(doc_result.get("error") or doc_result))
                continue
            if command == "/replay_summary":
                _send_text(sender, build_replay_summary_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/replay_clear":
                _send_text(sender, build_replay_clear_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/compare_live_vs_replay":
                _send_text(sender, build_compare_live_vs_replay_report(settings, redis_client=_snapshot_redis_client(trade_store)))
                continue
            if command == "/okx_status":
                reply = _build_okx_status_panel(settings, okx_client=okx_client)
            elif command == "/status":
                reply = _build_fast_status(result, settings, trade_store)
            elif command == "/mood":
                reply = result.get("mode_message", "No mode yet")
            elif command == "/help_execution":
                reply = result.get("help_execution", "")
            elif command == "/help_normal":
                reply = result.get("help_normal", "")
            elif command == "/okx_orders_on":
                applied = _set_runtime_okx_orders(settings, True)
                reply = "✅ تم تشغيل تنفيذ OKX." if applied else "⚠️ تعذر تشغيل تنفيذ OKX."
            elif command == "/okx_orders_off":
                applied = _set_runtime_okx_orders(settings, False)
                reply = "⏸ تم إيقاف تنفيذ OKX." if applied else "⚠️ تعذر إيقاف تنفيذ OKX."
            elif command in ("/help_simulation", "/simulation_help"):
                reply = _build_simulation_help()
            elif command in ("/admin", "/help_admin"):
                reply = _build_admin_panel()
            elif command in ("/bot_modes", "/modes", "/mode"):
                reply = _build_bot_modes_panel(settings)
                _send_text(sender, reply, reply_markup=_build_bot_modes_keyboard())
                continue
            elif command == "/okx_control":
                reply = _build_okx_control_panel(settings)
                _send_text(sender, reply, reply_markup=_build_okx_control_keyboard(settings))
                continue
            else:
                simulation_outputs = _build_simulation_command_outputs(result)
                reply = (
                    simulation_outputs.get(command)
                    or command_outputs.get(command)
                    or command_outputs.get(command.lstrip("/"))
                    or "الأمر غير متاح في نسخة v123 بعد."
                )
            _send_text(sender, reply)
    return offset


def _block_protection_alert_for_level(level: int, affected: int = 0, protected: int = 0, tightened: int = 0) -> str:
    if level <= 1:
        return "\n".join([
            "🛡 متابعة حماية البلوك",
            "┄┄┄┄┄┄┄┄",
            "🟡 المستوى 1 — مراقبة فقط",
            f"📊 الصفقات المتأثرة: {affected}",
            "⚙️ الإجراء: مراقبة بدون تعديل SL أو trailing",
            "⏭ Soft Protection بعد ~5m إذا استمر BLOCK_LONGS",
        ])
    if level == 2:
        return "\n".join([
            "🛡 تفعيل حماية البلوك",
            "┄┄┄┄┄┄┄┄",
            "🟠 المستوى 2 — حماية مرنة",
            f"📊 الصفقات المتأثرة: {affected}",
            f"✅ الأرباح المحمية: {protected}",
            f"🔧 Runners تحت حماية أخف: {tightened}",
            "⚪ الصفقات السلبية ما زالت على SL الأصلي",
            "⚙️ الإجراء: حماية الأرباح الحالية بدون إغلاق عشوائي",
            "⏭ Defensive Protection بعد ~10m إذا استمر BLOCK_LONGS",
        ])
    return "\n".join([
        "🛡 تصعيد حماية البلوك",
        "┄┄┄┄┄┄┄┄",
        "🔴 المستوى 3 — حماية دفاعية",
        f"📊 الصفقات المتأثرة: {affected}",
        f"✅ الأرباح المحمية: {protected}",
        f"🔧 Runners تحت حماية مشددة: {tightened}",
        "⚪ الصفقات السلبية ما زالت على SL الأصلي",
        "⚙️ الإجراء: حماية دفاعية بدون إغلاق عشوائي",
        "✅ أقصى مستوى حماية مفعل",
    ])


def _enrich_reminder_context(result: dict, base_context: dict) -> dict:
    from collections import Counter
    ctx = dict(base_context or {})
    trades = result.get("trades", []) or []
    signal_items = result.get("signal_items", []) or []
    execution_results = result.get("current_execution_results") or result.get("execution_results") or []
    scan = result.get("scan_stats", {}) or {}

    counted_open_items = [t for t in trades if _is_counted_open_trade(t)]
    protected_runner_items = [
        t for t in trades
        if bool(getattr(t, "protected_runner", False))
        or (
            bool(getattr(t, "tp2_hit", False))
            and bool(
                getattr(t, "runner_active", False)
                or getattr(t, "has_open_runner", False)
            )
        )
    ]

    ctx.update({
        "scanned_pairs": scan.get("scanned_pairs", ctx.get("sample_size", 200)),
        "signals_count": len(signal_items),
        "exec_accepted": sum(1 for r in execution_results if r.get("status") in {"accepted_preview", "pending_pullback_preview"}),
        "rejects_count": sum(1 for r in execution_results if str(r.get("status", "")).startswith("rejected") or r.get("status") == "candidate_only"),
        "counted_open_positions": len(counted_open_items),
        "open_winners": sum(1 for t in counted_open_items if getattr(t, "pnl_pct", 0.0) >= 0),
        "danger_trades": sum(1 for t in counted_open_items if getattr(t, "pnl_pct", 0.0) < 0),
        "protected_runners": len(protected_runner_items),
    })
    reasons = Counter(str(r.get("reason") or r.get("status") or "unknown") for r in execution_results if str(r.get("status", "")).startswith("rejected") or r.get("status") == "candidate_only")
    ctx["top_reject"] = reasons.most_common(1)[0][0] if reasons else "n/a"
    return ctx


def _maybe_send_mode_reminder(sender: TelegramSender, result: dict, tracker: dict) -> None:
    state = result.get("state")
    if not state:
        return
    mode = state.mode
    now = datetime.now(timezone.utc)
    changed_at = state.changed_at
    minutes_in_mode = int((now - changed_at).total_seconds() // 60)

    if tracker.get("mode") != mode or tracker.get("changed_at") != changed_at:
        tracker.clear()
        tracker.update({"mode": mode, "changed_at": changed_at, "general_sent": 0, "block_levels_sent": set()})

    protection = block_protection_status(state, now=now)

    if mode == MODE_BLOCK_LONGS:
        for threshold, level in BLOCK_REMINDER_THRESHOLDS:
            if minutes_in_mode >= threshold and level not in tracker["block_levels_sent"]:
                tracker["block_levels_sent"].add(level)
                context = _enrich_reminder_context(result, result.get("mode_context", {}))
                context.update({
                    "reminder_count": level,
                    "minutes_in_mode": minutes_in_mode,
                    "protection_current": f"LEVEL {level} — " + ("Monitor Only" if level == 1 else "Soft Protection" if level == 2 else "Defensive Protection"),
                    "protection_next": "Soft Protection" if level == 1 else "Defensive Protection" if level == 2 else "Max protection active",
                    "remaining_minutes": 5 if level == 1 else 5 if level == 2 else 0,
                })
                # ✅ FIX: _send_text لدعم HTML tags في الـ reminder
                _send_text(sender, build_market_mode_sections(mode, context, variant="reminder"))
                trades = result.get("trades", [])
                _send_text(sender, _block_protection_alert_for_level(
                    level,
                    affected=len(trades),
                    protected=sum(1 for t in trades if getattr(t, "pnl_pct", 0) > 0),
                    tightened=sum(1 for t in trades if getattr(t, "tp2_hit", False)),
                ))
                break
        return

    expected_count = minutes_in_mode // GENERAL_MODE_REMINDER_MINUTES
    if expected_count > tracker.get("general_sent", 0):
        tracker["general_sent"] = expected_count
        context = _enrich_reminder_context(result, result.get("mode_context", {}))
        context.update({"reminder_count": expected_count, "minutes_in_mode": minutes_in_mode})
        # ✅ FIX: _send_text لدعم HTML tags في الـ reminder
        _send_text(sender, build_market_mode_sections(mode, context, variant="reminder"))



def _poll_telegram_commands_safe(
    sender: TelegramSender,
    result: dict | None,
    offset: int | None,
    settings: Settings,
    trade_store: RedisTradeStore | None,
    okx_client: OKXTradeClient | None = None,
) -> int | None:
    """Poll Telegram commands without waiting for the next full scan.

    This keeps /status, /help, Admin, reset, and report commands responsive.
    It intentionally does not run trading decisions; it only answers commands
    using the latest completed result.
    """
    if result is None:
        return offset
    try:
        return _answer_commands(sender, result, offset, settings, trade_store, okx_client=okx_client)
    except Exception as exc:
        print(f"telegram command polling error: {exc}", flush=True)
        return offset


def live_worker() -> None:
    settings = get_settings()
    sender = TelegramSender(settings.bot_token, settings.chat_id, timeout=settings.request_timeout)
    okx_client = OKXTradeClient(
        api_key=settings.okx_api_key,
        api_secret=settings.okx_api_secret,
        passphrase=settings.okx_passphrase,
        base_url=settings.okx_base_url,
        simulated=settings.okx_simulated,
        allow_live_trading=settings.allow_live_trading,
        timeout=settings.request_timeout,
    )
    trade_store = RedisTradeStore(settings.redis_url)
    state: MarketModeState | None = None
    sent_fingerprints: dict[str, float] = {}
    telegram_offset: int | None = None
    reminder_tracker: dict = {}
    next_mode_guard_ts: float = 0.0
    last_result: dict | None = None

    startup_lines = [
        "✅ OKX Long Bot v134 started",
        f"Telegram: {'ON' if sender.enabled and settings.telegram_enabled else 'OFF'}",
        f"Execution: {'ON' if settings.execution_enabled else 'OFF'}",
        f"OKX paper orders: {'ON' if settings.okx_place_orders else 'OFF'} | simulated={settings.okx_simulated}",
        f"Full scan: {settings.scan_interval_seconds}s | Mode guard: {settings.market_mode_guard_interval_seconds}s",
        f"Signal Mode: {_signal_delivery_mode_label(settings)}",
        f"Verbose logs: {'ON' if settings.verbose_logs else 'OFF'}",
        trade_store.soft_restart_safe_note(),
    ]
    print("\n".join(startup_lines), flush=True)
    if sender.enabled and settings.telegram_enabled:
        sender.send_message("\n".join(startup_lines))

    while True:
        try:
            # Answer pending Telegram commands before starting a potentially long scan.
            if sender.enabled and settings.telegram_enabled and last_result is not None:
                telegram_offset = _poll_telegram_commands_safe(
                    sender,
                    last_result,
                    telegram_offset,
                    settings,
                    trade_store,
                    okx_client=okx_client,
                )

            previous_scan_mode = state.mode if state is not None else None
            result = run_once(previous_state=state, settings=settings, trade_store=trade_store, okx_client=okx_client)
            state = result["state"]
            if sender.enabled and settings.telegram_enabled:
                # Commands get priority over scan message bursts.
                telegram_offset = _poll_telegram_commands_safe(
                    sender,
                    result,
                    telegram_offset,
                    settings,
                    trade_store,
                    okx_client=okx_client,
                )

                if settings.send_mode_status_each_scan:
                    # ✅ FIX: _send_text بدل send_message لدعم HTML tags
                    mode_changed_in_scan = previous_scan_mode is not None and state.mode != previous_scan_mode
                    if mode_changed_in_scan and result.get("mode_transition_message"):
                        _send_text(sender, result.get("mode_transition_message", ""))
                    else:
                        _send_text(sender, result.get("mode_message", ""))
                next_mode_guard_ts = time.time() + max(60, int(settings.market_mode_guard_interval_seconds))
                _maybe_send_mode_reminder(sender, result, reminder_tracker)
                _dispatch_signals(sender, result, settings, sent_fingerprints, okx_client if settings.execution_enabled else None, trade_store)
                telegram_offset = _poll_telegram_commands_safe(
                    sender,
                    result,
                    telegram_offset,
                    settings,
                    trade_store,
                    okx_client=okx_client,
                )

            last_result = result
            if settings.verbose_logs:
                print(json.dumps(_plain_result(result), ensure_ascii=False, indent=2), flush=True)
            else:
                _print_scan_summary(result, trade_store)
        except Exception as exc:
            error_text = f"❌ OKX bot loop error: {exc}\n{traceback.format_exc()[-1200:]}"
            print(error_text, flush=True)
            if sender.enabled and settings.telegram_enabled:
                sender.send_message(error_text)

        wait_until = time.time() + max(30, int(settings.scan_interval_seconds))
        while time.time() < wait_until:
            if sender.enabled and settings.telegram_enabled:
                try:
                    now_ts = time.time()
                    if last_result is not None:
                        if now_ts >= next_mode_guard_ts:
                            state = _run_market_mode_guard(sender, last_result, settings, state, reminder_tracker)
                            next_mode_guard_ts = now_ts + max(60, int(settings.market_mode_guard_interval_seconds))
                        _maybe_send_mode_reminder(sender, last_result, reminder_tracker)
                        telegram_offset = _poll_telegram_commands_safe(
                            sender,
                            last_result,
                            telegram_offset,
                            settings,
                            trade_store,
                            okx_client=okx_client,
                        )
                except Exception as exc:
                    print(f"telegram command polling error: {exc}", flush=True)
            time.sleep(max(0.5, float(TELEGRAM_COMMAND_POLL_SLEEP_SECONDS)))


if __name__ == "__main__":
    live_worker()
