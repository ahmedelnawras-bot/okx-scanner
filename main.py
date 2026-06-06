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

import csv
import json
import os
import re
import threading
import time
import traceback
from types import SimpleNamespace
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
from tracking.models import TrackedTrade
from reporting.report_router import build_report_bundle, build_command_outputs
from reporting.report_format import trade_effective_pnl as _report_trade_effective_pnl
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
# Trading mode should stay as visible as Simulation, with only a short
# same-symbol cooldown to avoid repeating the same coin every scan.
SYMBOL_OBSERVATION_DEDUP_TTL_SECONDS = 45 * 60
SYMBOL_PULLBACK_DEDUP_TTL_SECONDS = 60 * 60
SYMBOL_EXECUTION_DEDUP_TTL_SECONDS = 2 * 60 * 60
SYMBOL_TRADING_SAME_SYMBOL_DEDUP_TTL_SECONDS = 5 * 60

# Low Balance Mode
# لو الرصيد أقل من الحد → allocation أكبر وعدد slots أقل.
# Block Exception و Recovery استثناء — بيفضلوا على حدودهم الأصلية.
LOW_BALANCE_THRESHOLD_USDT: float = 109.0
LOW_BALANCE_ALLOCATION_PCT: float = 40.0
LOW_BALANCE_MAX_SLOTS: int = 3

# Live execution hard guard: never send OKX orders when planned margin is too small
# to survive OKX lot/min-size normalization. This prevents normalized_size_zero
# and also blocks zero/tiny OKX balances before exchange placement.
LIVE_MIN_EXECUTION_MARGIN_USDT: float = 1.0

# Telegram send pacing.
# This only spaces Telegram messages after decisions are already made.
# It does not delay process_trade_candidate, OKX execution, slots, or simulation tracking.
TELEGRAM_SEND_GAP_SECONDS = 0.65
TELEGRAM_EXECUTION_SEND_GAP_SECONDS = 0.35
TELEGRAM_NORMAL_SEND_GAP_SECONDS = 0.85
TELEGRAM_COMMAND_POLL_SLEEP_SECONDS = 0.5


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

# Execution Daily Baseline
# Position sizing uses the current OKX balance, but Daily DD uses a daily
# baseline that is persisted per UTC day. Large external deposits/withdrawals
# are adjusted into the baseline so cash movements do not look like trading PnL.
EXECUTION_REDIS_PREFIX = "okx:longbot:execution:v1"
EXECUTION_DAILY_BALANCE_HASH = f"{EXECUTION_REDIS_PREFIX}:daily_balance"
EXECUTION_BALANCE_STATE_KEY = f"{EXECUTION_REDIS_PREFIX}:wallet:state"
EXECUTION_CASHFLOW_MIN_ABS_USDT = 5.0
EXECUTION_CASHFLOW_MIN_PCT = 10.0
_EXECUTION_DAILY_RUNTIME_STATE: dict[str, dict] = {}

# OKX recovery grace:
# If this worker has just received OKX success for a symbol, give immediate
# Redis registration a short window before classifying the same live position
# as RECOVERED_FROM_OKX on the next scan. The live OKX guards still count the
# position, so this does not weaken slot/same-symbol protection.
OKX_RECOVERY_GRACE_SECONDS: int = 120
# Keep setup metadata longer than the grace window so delayed recovery can
# rebuild the trade with its real SL/TP instead of conservative placeholders.
OKX_RECOVERY_META_SECONDS: int = 15 * 60
_OKX_RECENT_BOT_OPENED_SYMBOLS: dict[str, dict | float] = {}



# Loss Streak Guard: pause new execution after repeated SL hits before TP1.
LOSS_STREAK_NO_TP1_LIMIT = 5
LOSS_STREAK_COOLDOWN_MINUTES = 120

# Manual protection resume state.
# Stored in Redis when available; kept in memory as a safe fallback.
PROTECTION_STATE_PREFIX = "okx:longbot:protection"
PROTECTION_STATE_TTL_SECONDS = 3 * 24 * 60 * 60
_PROTECTION_RUNTIME_STATE: dict[str, dict] = {}

try:
    from reporting.ai_exporter import export_ai_snapshot
    _AI_EXPORT_ENABLED = True
except ImportError:
    _AI_EXPORT_ENABLED = False
    def export_ai_snapshot(*args, **kwargs) -> dict:
        return {"ok": False, "error": "ai_exporter not found"}

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


def _risk_sizing_constants(settings: Settings, reference_balance: float = 0.0) -> tuple[float, int]:
    allocation_pct = 24.0
    slot_count = max(1, int(getattr(settings, "max_execution_positions", 7) or 7))

    if risk_manager_module is not None:
        allocation_pct = _safe_float(getattr(risk_manager_module, "max_portion_pct", allocation_pct), allocation_pct)
        slot_count = max(
            1,
            int(getattr(risk_manager_module, "max_positions_total_normal_strong", slot_count) or slot_count),
        )

    # Low Balance Mode — رصيد أقل من الحد → allocation أعلى وعدد slots أقل.
    # Block Exception و Recovery استثناء ومش بيتأثروا هنا.
    if 0 < float(reference_balance or 0.0) < LOW_BALANCE_THRESHOLD_USDT:
        allocation_pct = LOW_BALANCE_ALLOCATION_PCT
        slot_count = LOW_BALANCE_MAX_SLOTS

    return allocation_pct, slot_count


def _compute_margin_from_reference(reference_balance_usdt: float, settings: Settings) -> float:
    allocation_pct, slot_count = _risk_sizing_constants(settings, reference_balance=reference_balance_usdt)
    if reference_balance_usdt <= 0 or slot_count <= 0:
        return 0.0
    total_allocation = float(reference_balance_usdt) * (allocation_pct / 100.0)
    return total_allocation / float(slot_count)



def _risk_profile_context(settings: Settings, result: dict | None = None) -> str:
    """Return risk-display context from the same runtime mode used by bot modes panel.

    Do NOT infer this from stale result/runtime fields. The previous build mixed
    execution_enabled / simulation_wallet / runtime_mode and could display
    Simulation while the bot modes panel was in Trading. The single source of
    truth here is Settings.signal_delivery_mode, exactly like /bot_modes.
    """
    snapshot = _runtime_mode_snapshot(settings)
    context = str(snapshot.get("risk_context") or "scanner").strip().lower()
    if context == "execution":
        return "execution"
    if context == "simulation":
        return "simulation"
    return "scan"


def _risk_profile_snapshot(
    settings: Settings,
    result: dict | None = None,
    reference_balance: float | None = None,
    source: str | None = None,
) -> dict:
    """Expose dynamic risk-manager sizing state for /status and mode messages."""
    result = result or {}
    inputs = dict(result.get("portfolio_state_inputs", {}) or {})
    risk_context = _risk_profile_context(settings, result)

    resolved_source = source or str(inputs.get("source") or "dynamic_risk")

    if reference_balance is None:
        if risk_context == "simulation":
            wallet = result.get("simulation_wallet") if isinstance(result, dict) else None
            reference_balance = _safe_float((wallet or {}).get("equity"), SIMULATION_START_BALANCE_USDT)
            resolved_source = "simulation_wallet_balance"
        elif risk_context == "execution":
            reference_balance = _safe_float(inputs.get("reference_portfolio"), 0.0)
            resolved_source = "okx_balance"
        else:
            reference_balance = _safe_float(inputs.get("reference_portfolio"), 0.0)

    reference_balance = _safe_float(reference_balance, 0.0)

    # Live execution must display the current OKX-derived balance only.
    # Do not borrow cached/paper/simulation balances here; zero/tiny OKX balance
    # must remain visible so execution can be blocked safely.
    if reference_balance <= 0 and risk_context == "execution":
        resolved_source = "live_okx_balance_zero_or_unavailable"

    # Low balance mode is applied here via reference_balance
    allocation_pct, slot_count = _risk_sizing_constants(settings, reference_balance=reference_balance)

    margin_per_trade = _safe_float(inputs.get("margin_per_trade"), 0.0)
    if risk_context == "simulation" or margin_per_trade <= 0:
        margin_per_trade = _compute_margin_from_reference(reference_balance, settings) if reference_balance > 0 else 0.0
    if risk_context == "execution" and margin_per_trade < LIVE_MIN_EXECUTION_MARGIN_USDT:
        margin_per_trade = 0.0

    reason_bits: list[str] = []
    if risk_context == "simulation":
        reason_bits.append("simulation_wallet_balance")
    elif risk_context == "execution":
        if reference_balance <= 0:
            reason_bits.append("live_okx_balance_zero_or_unavailable")
        elif margin_per_trade <= 0:
            reason_bits.append("live_okx_margin_too_small")
        else:
            reason_bits.append("okx_live_balance")
    else:
        reason_bits.append("scan_or_paper_sizing")

    if risk_manager_module is not None:
        reason_bits.append("risk_manager_active")
        mode_value = getattr(risk_manager_module, "mode", None) or getattr(risk_manager_module, "current_mode", None)
        if mode_value:
            reason_bits.append(f"mode={mode_value}")
    else:
        reason_bits.append("settings_fallback")

    mode_context = result.get("mode_context") or {}
    if isinstance(mode_context, dict):
        protection_current = str(mode_context.get("protection_current") or "").strip()
        if protection_current and protection_current != "inactive":
            reason_bits.append(f"protection={protection_current}")

    drawdown = result.get("drawdown_status")
    if drawdown is not None:
        try:
            reason_bits.append(
                f"drawdown={float(getattr(drawdown, 'drawdown_pct', 0.0) or 0.0):.1f}%/level{int(getattr(drawdown, 'level', 0) or 0)}"
            )
        except Exception:
            pass

    loss_guard = result.get("loss_streak_guard") or {}
    if isinstance(loss_guard, dict) and loss_guard.get("active"):
        reason_bits.append(f"loss_streak={int(loss_guard.get('streak', 0) or 0)}")

    slot_usage = _risk_slot_usage_snapshot(settings, result, reference_balance=reference_balance)

    return {
        "context": risk_context,
        "source": resolved_source,
        "reference_balance_usdt": reference_balance,
        "allocation_pct": float(allocation_pct or 0.0),
        "slot_count": int(slot_count or 0),
        "margin_per_trade": margin_per_trade,
        "slot_usage": slot_usage,
        "reason": " | ".join(reason_bits[:4]) if reason_bits else "dynamic risk sizing",
    }


def _risk_profile_title(settings: Settings, profile: dict | None = None) -> str:
    context = str((profile or {}).get("context") or _risk_profile_context(settings, None)).strip().lower()
    if context == "simulation":
        return "🧪 Risk Manager — Simulation"
    if context == "execution":
        return "🚀 Risk Manager — Execution"
    return "🧮 Risk Manager"


def _compact_mode_message_text(message: str) -> str:
    """Reduce whitespace/noise without deleting the actual mode details."""
    value = str(message or "").strip()
    value = re.sub(r"\n[ \t]+", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)

    # Remove duplicated split title patterns sometimes generated by templates.
    value = re.sub(r"(?im)^\s*Market Mode:\s*(.+)\n\s*\1\s*$", r"Market Mode: \1", value)

    # If both Market Mix and Market State appear with the same core numbers,
    # keep the readable Market Mix and remove the machine-style Market State line.
    value = re.sub(r"(?im)^\s*Market State:\s*strong_coins=.*\n?", "", value)

    # Compact decorative separators and blank spacing around them.
    value = re.sub(r"\n\s*[-━┄]{6,}\s*\n", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def _risk_slot_usage_snapshot(settings: Settings, result: dict | None, reference_balance: float = 0.0) -> dict:
    """Runtime slot usage for Risk Manager display only.

    This is report/UI-only:
    - Simulation reads simulation_trades.
    - Execution reads execution trades.
    - Low balance uses the active limits: general=3, block=1, recovery=1.
    - Normal balance uses general=max_execution_positions, block=3, recovery=3.
    """
    result = result or {}
    context = _risk_profile_context(settings, result)
    base_trades = list(result.get("simulation_trades", []) or []) if context == "simulation" else list(result.get("trades", []) or [])
    counts = _execution_slot_counts(base_trades)
    low_balance = bool(0 < _safe_float(reference_balance, 0.0) < LOW_BALANCE_THRESHOLD_USDT)
    general_limit = LOW_BALANCE_MAX_SLOTS if low_balance else max(1, int(getattr(settings, "max_execution_positions", 7) or 7))
    block_limit = 1 if low_balance else 3
    recovery_limit = 1 if low_balance else 3
    general_used = int(counts.get("general", 0) or 0)
    block_used = int(counts.get("block_exception", 0) or 0)
    recovery_used = int(counts.get("recovery", 0) or 0)
    return {
        "context": context,
        "low_balance": low_balance,
        "general_used": general_used,
        "general_limit": general_limit,
        "block_used": block_used,
        "block_limit": block_limit,
        "recovery_used": recovery_used,
        "recovery_limit": recovery_limit,
        "total_used": general_used + block_used + recovery_used,
        "total_limit": general_limit + block_limit + recovery_limit,
    }

def _format_risk_profile_block(profile: dict | None, title: str = "🧮 Risk Profile") -> str:
    profile = profile or {}
    usage = dict(profile.get("slot_usage") or {})
    slot_line = f"Slots: <b>{int(profile.get('slot_count', 0) or 0)}</b>"
    if usage:
        slot_line = (
            f"Slots: <b>{int(usage.get('general_used', 0) or 0)} / {int(usage.get('general_limit', profile.get('slot_count', 0)) or 0)}</b> used"
            f" | Block <b>{int(usage.get('block_used', 0) or 0)}/{int(usage.get('block_limit', 0) or 0)}</b>"
            f" | Recovery <b>{int(usage.get('recovery_used', 0) or 0)}/{int(usage.get('recovery_limit', 0) or 0)}</b>"
        )
    return "\n".join([
        f"{title}",
        slot_line,
        f"Allocation: <b>{_safe_float(profile.get('allocation_pct'), 0.0):.2f}%</b>",
        f"Reference Balance: <b>{_safe_float(profile.get('reference_balance_usdt'), 0.0):,.2f} USDT</b>",
        f"Margin / Trade: <b>{_safe_float(profile.get('margin_per_trade'), 0.0):,.2f} USDT</b>",
        f"Reason: <code>{str(profile.get('reason') or 'dynamic risk sizing')}</code>",
    ])

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
    global _CACHED_OKX_BALANCE, _CACHED_OKX_BALANCE_TS
    global _CACHED_OKX_BALANCE_LOG_TS, _CACHED_OKX_BALANCE_LOG_VALUE
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

    # Cache may be updated for diagnostics only. Live execution sizing never
    # falls back to cached balance, because a current zero/tiny OKX balance must
    # block execution instead of reusing stale capital.
    import time as _time
    now_ts = _time.time()
    with _CACHED_OKX_BALANCE_LOCK:
        if okx_reference_balance > 0:
            _CACHED_OKX_BALANCE = okx_reference_balance
            _CACHED_OKX_BALANCE_TS = now_ts

            # Log throttling only:
            # keep balance cache behavior unchanged, but avoid flooding Railway logs.
            # Print when the balance meaningfully changes, or once every configured interval.
            last_log_ts = float(_CACHED_OKX_BALANCE_LOG_TS or 0.0)
            last_log_value = float(_CACHED_OKX_BALANCE_LOG_VALUE or 0.0)
            balance_delta = abs(float(okx_reference_balance or 0.0) - last_log_value)
            should_log_balance = (
                last_log_ts <= 0
                or (now_ts - last_log_ts) >= _CACHED_OKX_BALANCE_LOG_INTERVAL_SECONDS
                or balance_delta >= _CACHED_OKX_BALANCE_LOG_MIN_DELTA_USDT
            )
            if should_log_balance:
                _CACHED_OKX_BALANCE_LOG_TS = now_ts
                _CACHED_OKX_BALANCE_LOG_VALUE = okx_reference_balance
                print(f"💰 OKX balance cached: {okx_reference_balance:.4f} USDT", flush=True)

    live_okx_mode = bool(
        okx_client is not None
        and getattr(okx_client, "configured", False)
        and not bool(getattr(settings, "okx_simulated", True))
    )
    sizing_balance = okx_reference_balance
    execution_daily_for_sizing = {}
    if live_okx_mode and okx_reference_balance > 0:
        sizing_balance, execution_daily_for_sizing = _execution_daily_sizing_balance_from_runtime(okx_reference_balance, settings)

    okx_margin = _compute_margin_from_reference(sizing_balance, settings)
    if okx_reference_balance > 0 and okx_margin > 0:
        if live_okx_mode and okx_margin < LIVE_MIN_EXECUTION_MARGIN_USDT:
            return {
                "source": "okx_balance",
                "reference_balance_usdt": okx_reference_balance,
                "sizing_balance_usdt": sizing_balance,
                "margin_usdt": 0.0,
                "position_pct": 0.0,
                "reason": "live_okx_margin_too_small",
                "min_execution_margin_usdt": LIVE_MIN_EXECUTION_MARGIN_USDT,
                "execution_daily_baseline": execution_daily_for_sizing,
            }
        if live_okx_mode and okx_margin > okx_reference_balance:
            return {
                "source": "okx_balance",
                "reference_balance_usdt": okx_reference_balance,
                "sizing_balance_usdt": sizing_balance,
                "margin_usdt": 0.0,
                "position_pct": 0.0,
                "reason": "live_okx_balance_below_daily_sizing_margin",
                "required_margin_usdt": okx_margin,
                "execution_daily_baseline": execution_daily_for_sizing,
            }
        return {
            "source": "okx_balance",
            "reference_balance_usdt": okx_reference_balance,
            "sizing_balance_usdt": sizing_balance,
            "margin_usdt": okx_margin,
            "position_pct": (okx_margin / sizing_balance) * 100.0 if sizing_balance > 0 else 0.0,
            "reason": "daily_adjusted_baseline_sizing" if live_okx_mode else "daily_reference_from_okx_balance",
            "execution_daily_baseline": execution_daily_for_sizing,
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



def _execution_today_key(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.date().isoformat()


def _execution_cashflow_threshold(last_equity: float, settings: Settings | None = None) -> float:
    """Minimum balance jump treated as external deposit/withdrawal.

    A balance-only detector cannot perfectly distinguish cashflow from PnL, so
    this deliberately ignores small OKX equity moves and reacts only to large
    jumps. The threshold can be tuned from Railway env without code changes.
    """
    try:
        abs_min = _safe_float(
            os.getenv("EXECUTION_CASHFLOW_MIN_ABS_USDT")
            or getattr(settings, "execution_cashflow_min_abs_usdt", EXECUTION_CASHFLOW_MIN_ABS_USDT),
            EXECUTION_CASHFLOW_MIN_ABS_USDT,
        )
    except Exception:
        abs_min = EXECUTION_CASHFLOW_MIN_ABS_USDT
    try:
        pct_min = _safe_float(
            os.getenv("EXECUTION_CASHFLOW_MIN_PCT")
            or getattr(settings, "execution_cashflow_min_pct", EXECUTION_CASHFLOW_MIN_PCT),
            EXECUTION_CASHFLOW_MIN_PCT,
        )
    except Exception:
        pct_min = EXECUTION_CASHFLOW_MIN_PCT
    pct_threshold = abs(_safe_float(last_equity, 0.0)) * max(0.0, pct_min) / 100.0
    return max(0.0, abs_min, pct_threshold)


def _load_execution_daily_balance_row(trade_store: RedisTradeStore | None, day: str) -> dict:
    row = dict(_EXECUTION_DAILY_RUNTIME_STATE.get(str(day) or "") or {})
    if trade_store and getattr(trade_store, "enabled", False) and getattr(trade_store, "client", None):
        try:
            raw = trade_store.client.hget(EXECUTION_DAILY_BALANCE_HASH, day)
            if raw:
                loaded = json.loads(raw)
                if isinstance(loaded, dict):
                    row.update(loaded)
        except Exception as exc:
            print(f"⚠️ Execution daily baseline load failed: {exc}", flush=True)
    return row


def _save_execution_daily_balance_row(trade_store: RedisTradeStore | None, day: str, row: dict) -> dict:
    clean = dict(row or {})
    clean["date"] = str(day)
    clean["updated_at"] = datetime.now(timezone.utc).isoformat()
    _EXECUTION_DAILY_RUNTIME_STATE[str(day)] = clean
    if trade_store and getattr(trade_store, "enabled", False) and getattr(trade_store, "client", None):
        try:
            payload = json.dumps(clean, ensure_ascii=False, default=str)
            trade_store.client.hset(EXECUTION_DAILY_BALANCE_HASH, str(day), payload)
            trade_store.client.expire(EXECUTION_DAILY_BALANCE_HASH, 180 * 24 * 60 * 60)
            trade_store.client.set(EXECUTION_BALANCE_STATE_KEY, payload, ex=180 * 24 * 60 * 60)
        except Exception as exc:
            print(f"⚠️ Execution daily baseline save failed: {exc}", flush=True)
    return clean


def _ensure_execution_daily_baseline(
    current_equity: float,
    trade_store: RedisTradeStore | None = None,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> dict:
    """Persist live execution Daily DD baseline per UTC day.

    - reference_portfolio still shows current OKX equity.
    - start_of_day_balance for Daily DD uses adjusted_start_balance.
    - position sizing uses adjusted_start_balance, not every floating equity move.
    - Large balance jumps are treated as external cashflow and folded into the
      baseline, so deposits/withdrawals update both Daily DD baseline and sizing.
    """
    now = now or datetime.now(timezone.utc)
    today = _execution_today_key(now)
    equity = _safe_float(current_equity, 0.0)
    if equity <= 0:
        return {
            "date": today,
            "start_balance": 0.0,
            "adjusted_start_balance": 0.0,
            "current_balance": 0.0,
            "external_cashflow_net": 0.0,
            "reason": "current_equity_unavailable",
        }

    row = _load_execution_daily_balance_row(trade_store, today)
    if not row:
        row = {
            "date": today,
            "start_balance": equity,
            "adjusted_start_balance": equity,
            "current_balance": equity,
            "last_equity": equity,
            "external_cashflow_net": 0.0,
            "external_deposits": 0.0,
            "external_withdrawals": 0.0,
            "cashflow_events": [],
            "created_at": now.isoformat(),
            "reason": "new_utc_day_baseline_from_okx_equity",
        }
        print(f"📅 EXECUTION_DAILY_BASELINE | new_day | date={today} | start={equity:.4f}", flush=True)
        return _save_execution_daily_balance_row(trade_store, today, row)

    start_balance = _safe_float(row.get("start_balance"), equity)
    if start_balance <= 0:
        start_balance = equity
    external_net = _safe_float(row.get("external_cashflow_net"), 0.0)
    deposits = _safe_float(row.get("external_deposits"), 0.0)
    withdrawals = _safe_float(row.get("external_withdrawals"), 0.0)
    last_equity = _safe_float(row.get("last_equity") or row.get("current_balance"), equity)
    threshold = _execution_cashflow_threshold(last_equity, settings)
    delta = equity - last_equity

    if last_equity > 0 and abs(delta) >= threshold:
        event_type = "deposit" if delta > 0 else "withdrawal"
        external_net += delta
        if delta > 0:
            deposits += delta
        else:
            withdrawals += abs(delta)
        events = list(row.get("cashflow_events") or [])
        events.append({
            "ts": now.isoformat(),
            "type": event_type,
            "amount": round(delta, 8),
            "previous_equity": round(last_equity, 8),
            "current_equity": round(equity, 8),
            "threshold": round(threshold, 8),
        })
        row["cashflow_events"] = events[-20:]
        print(
            f"💸 EXECUTION_CASHFLOW_ADJUST | {event_type} | amount={delta:+.4f} | "
            f"start={start_balance:.4f} | external_net={external_net:+.4f} | threshold={threshold:.4f}",
            flush=True,
        )

    adjusted_start = max(0.0, start_balance + external_net)
    row.update({
        "date": today,
        "start_balance": start_balance,
        "adjusted_start_balance": adjusted_start,
        "current_balance": equity,
        "last_equity": equity,
        "external_cashflow_net": external_net,
        "external_deposits": deposits,
        "external_withdrawals": withdrawals,
        "cashflow_threshold_usdt": threshold,
        "reason": "daily_baseline_adjusted_for_external_cashflow" if abs(external_net) > 0 else "daily_baseline_from_okx_equity",
    })
    return _save_execution_daily_balance_row(trade_store, today, row)


def _execution_daily_sizing_balance_from_runtime(current_equity: float, settings: Settings | None = None) -> tuple[float, dict]:
    """Return today's execution sizing balance from the daily baseline cache.

    This is used by actual OKX order sizing, where trade_store is not available.
    run_once() calls _ensure_execution_daily_baseline() before dispatch, so the
    runtime cache normally contains today's adjusted_start_balance. If not, we
    fall back to current OKX equity safely.
    """
    equity = _safe_float(current_equity, 0.0)
    today = _execution_today_key()
    row = _load_execution_daily_balance_row(None, today)
    sizing_balance = _safe_float(row.get("adjusted_start_balance") or row.get("start_balance"), 0.0)
    if sizing_balance <= 0:
        sizing_balance = equity
    return max(0.0, sizing_balance), row




def _portfolio_state_kwargs(inputs: dict | None) -> dict:
    """Filter runtime diagnostics before calling risk.portfolio_state.

    _resolve_portfolio_state_inputs() carries extra execution-only diagnostics
    such as execution_sizing_balance / execution_daily_baseline for reports and
    logging. risk.portfolio_state.build_portfolio_state_from_trades() accepts
    only accounting inputs, so passing the full dict breaks the worker with:
    unexpected keyword argument 'execution_sizing_balance'.
    """
    source = dict(inputs or {})
    try:
        import inspect
        params = inspect.signature(build_portfolio_state_from_trades).parameters
        allowed = {name for name in params if name != "trades"}
    except Exception:
        allowed = {
            "reference_portfolio",
            "margin_per_trade",
            "leverage",
            "start_of_day_balance",
            "day_started_at",
            "manual_daily_dd_override",
            "manual_daily_dd_baseline",
            "manual_resume_at",
        }
    return {key: value for key, value in source.items() if key in allowed}

def _resolve_portfolio_state_inputs(
    okx_client: OKXTradeClient | None,
    settings: Settings,
    trade_store: RedisTradeStore | None = None,
) -> dict:
    sizing = _resolve_entry_margin_plan(okx_client, settings)
    reference_balance = _safe_float((sizing or {}).get("reference_balance_usdt"), 0.0)
    margin_per_trade = _safe_float((sizing or {}).get("margin_usdt"), 0.0)

    live_okx_mode = bool(
        okx_client is not None
        and getattr(okx_client, "configured", False)
        and not bool(getattr(settings, "okx_simulated", True))
    )

    # In live execution mode, never use cache or paper fallback for sizing.
    # OKX balance zero/tiny => margin 0 and execution remains blocked.
    if live_okx_mode:
        if reference_balance <= 0 or margin_per_trade < LIVE_MIN_EXECUTION_MARGIN_USDT:
            margin_per_trade = 0.0
    elif margin_per_trade <= 0:
        margin_per_trade = max(_safe_float(getattr(settings, "paper_margin_usdt", 35.0), 35.0), 0.0) or 35.0

    if reference_balance <= 0 and not live_okx_mode:
        allocation_pct, slot_count = _risk_sizing_constants(settings)
        if allocation_pct > 0 and slot_count > 0:
            reference_balance = margin_per_trade * float(slot_count) / (allocation_pct / 100.0)

    reference_balance = max(reference_balance, 0.0)
    leverage = max(1, int(getattr(settings, "default_leverage", 1) or 1))

    execution_daily = {}
    start_of_day_balance = reference_balance
    if live_okx_mode and reference_balance > 0:
        execution_daily = _ensure_execution_daily_baseline(
            reference_balance,
            trade_store=trade_store,
            settings=settings,
        )
        start_of_day_balance = _safe_float(
            execution_daily.get("adjusted_start_balance") or execution_daily.get("start_balance"),
            reference_balance,
        )
        if start_of_day_balance <= 0:
            start_of_day_balance = reference_balance

        # Position sizing is intentionally stable during the day.
        # It follows the daily adjusted baseline, so normal floating PnL does not
        # resize trades every scan; external deposits/withdrawals update it.
        daily_sizing_balance = start_of_day_balance
        margin_per_trade = _compute_margin_from_reference(daily_sizing_balance, settings)
        if margin_per_trade < LIVE_MIN_EXECUTION_MARGIN_USDT:
            margin_per_trade = 0.0
        elif reference_balance > 0 and margin_per_trade > reference_balance:
            margin_per_trade = 0.0
            sizing["reason"] = "live_okx_balance_below_daily_sizing_margin"
    else:
        daily_sizing_balance = reference_balance

    return {
        # Current OKX equity: shown as the real wallet value.
        "reference_portfolio": reference_balance,
        # Daily DD baseline: stable per UTC day, adjusted for large external cashflow.
        "start_of_day_balance": start_of_day_balance,
        # Position sizing balance: stable intraday; updates on new day/cashflow/manual resume.
        "execution_sizing_balance": daily_sizing_balance,
        "margin_per_trade": margin_per_trade,
        "leverage": leverage,
        "execution_daily_baseline": execution_daily,
        "execution_daily_start_balance": _safe_float(execution_daily.get("start_balance"), start_of_day_balance),
        "execution_adjusted_start_balance": _safe_float(execution_daily.get("adjusted_start_balance"), start_of_day_balance),
        "execution_daily_sizing_balance": _safe_float(daily_sizing_balance, 0.0),
        "execution_external_cashflow_usdt": _safe_float(execution_daily.get("external_cashflow_net"), 0.0),
        "execution_external_deposits_usdt": _safe_float(execution_daily.get("external_deposits"), 0.0),
        "execution_external_withdrawals_usdt": _safe_float(execution_daily.get("external_withdrawals"), 0.0),
        "execution_daily_baseline_date": str(execution_daily.get("date") or ""),
    }


def _resolve_simulation_portfolio_state_inputs(
    simulation_trades: list,
    settings: Settings,
    trade_store: RedisTradeStore | None = None,
    daily_balance: dict | None = None,
) -> dict:
    """Build Daily DD inputs from the Simulation wallet only.

    This deliberately does not call _resolve_portfolio_state_inputs(), because
    that function is execution/OKX-oriented and may fall back to risk_manager
    micro-values. Simulation DD must use the virtual wallet journal:
    - start_of_day_balance: today's simulation daily start
    - reference_portfolio: current simulation wallet equity
    - margin_per_trade: simulation sizing for the same virtual account

    Execution/trading mode remains OKX-based through _resolve_portfolio_state_inputs().
    """
    wallet = _build_simulation_wallet_snapshot(
        list(simulation_trades or []),
        start_balance=SIMULATION_START_BALANCE_USDT,
    )

    row = dict(daily_balance or {})
    if not row:
        try:
            row = _ensure_simulation_daily_log(
                list(simulation_trades or []),
                trade_store=trade_store,
                settings=settings,
            )
        except Exception:
            row = {}

    start_balance = _safe_float(row.get("start_balance"), 0.0)
    if start_balance <= 0:
        start_balance = _safe_float(wallet.get("start_balance"), SIMULATION_START_BALANCE_USDT)
    if start_balance <= 0:
        start_balance = SIMULATION_START_BALANCE_USDT

    current_equity = _safe_float(wallet.get("equity"), 0.0)
    if current_equity <= 0:
        current_equity = _safe_float(row.get("current_balance") or row.get("end_balance"), start_balance)
    if current_equity <= 0:
        current_equity = start_balance

    margin_per_trade = _safe_float(row.get("margin_per_trade"), 0.0)
    if margin_per_trade <= 0:
        margin_per_trade = _simulation_margin_usdt(start_balance, settings)
    if margin_per_trade <= 0:
        margin_per_trade = _simulation_margin_usdt(current_equity, settings)

    leverage = max(1, int(getattr(settings, "default_leverage", 1) or 1))

    return {
        "reference_portfolio": float(current_equity),
        "start_of_day_balance": float(start_balance),
        "margin_per_trade": float(margin_per_trade or 0.0),
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



def _is_live_okx_execution_mode(settings: Settings, okx_client: OKXTradeClient | None) -> bool:
    return bool(
        okx_client is not None
        and getattr(okx_client, "configured", False)
        and not bool(getattr(settings, "okx_simulated", True))
        and not bool(getattr(settings, "offline_test_mode", False))
    )


def _trade_symbol_inst_id(trade) -> str:
    return _normalize_okx_inst_id(getattr(trade, "symbol", ""))


def _normalize_okx_inst_id(value: object) -> str:
    """Normalize OKX symbols so BASEDUSDT / BASED-USDT-SWAP compare safely."""
    text = str(value or "").strip().upper()
    if not text:
        return ""
    compact = text.replace("-", "")
    if compact.endswith("USDT") and not text.endswith("-USDT-SWAP"):
        base = compact[:-4]
        if base:
            return f"{base}-USDT-SWAP"
    if compact.endswith("USDTSWAP") and not text.endswith("-USDT-SWAP"):
        base = compact[:-8]
        if base:
            return f"{base}-USDT-SWAP"
    return text


def _okx_recent_mark_expiry(mark: object, key: str = "grace_until") -> float:
    if isinstance(mark, dict):
        return _safe_float(mark.get(key), 0.0)
    return _safe_float(mark, 0.0)


def _purge_recent_bot_okx_order_marks(now_ts: float | None = None) -> None:
    now_ts = float(now_ts or time.time())
    expired = []
    for symbol, mark in list(_OKX_RECENT_BOT_OPENED_SYMBOLS.items()):
        meta_until = _okx_recent_mark_expiry(mark, "meta_until")
        grace_until = _okx_recent_mark_expiry(mark, "grace_until")
        expires_at = meta_until or grace_until
        if expires_at <= now_ts:
            expired.append(symbol)
    for symbol in expired:
        _OKX_RECENT_BOT_OPENED_SYMBOLS.pop(symbol, None)


def _extract_signal_trade_setup(signal=None, managed_order_result: dict | None = None, trade=None) -> dict:
    managed_order_result = managed_order_result or {}
    sizing = managed_order_result.get("sizing") or {}
    return {
        "entry": _safe_float(getattr(signal, "entry", 0.0) if signal is not None else getattr(trade, "entry", 0.0), 0.0),
        "sl": _safe_float(getattr(signal, "sl", 0.0) if signal is not None else getattr(trade, "sl", 0.0), 0.0),
        "tp1": _safe_float(getattr(signal, "tp1", 0.0) if signal is not None else getattr(trade, "tp1", 0.0), 0.0),
        "tp2": _safe_float(getattr(signal, "tp2", 0.0) if signal is not None else getattr(trade, "tp2", 0.0), 0.0),
        "margin": _safe_float(
            managed_order_result.get("used_margin_usdt")
            or sizing.get("margin_usdt")
            or getattr(trade, "used_margin_usdt", 0.0),
            0.0,
        ),
        "leverage": _safe_float(
            managed_order_result.get("effective_leverage")
            or managed_order_result.get("actual_leverage")
            or managed_order_result.get("requested_leverage")
            or getattr(trade, "effective_leverage", 0.0),
            0.0,
        ),
        "td_mode": str(managed_order_result.get("td_mode") or getattr(trade, "td_mode", "") or "").strip(),
    }


def _mark_recent_bot_okx_order(
    symbol: object,
    reason: str = "okx_order_success",
    signal=None,
    managed_order_result: dict | None = None,
    trade=None,
) -> None:
    """Remember symbols this worker just opened on OKX.

    The grace window only delays RECOVERED_FROM_OKX classification. The metadata
    window lasts longer so delayed recovery can keep Entry/SL/TP from the order.
    """
    inst_id = _normalize_okx_inst_id(symbol)
    if not inst_id:
        return
    now_ts = time.time()
    _purge_recent_bot_okx_order_marks(now_ts)
    grace_ttl = max(5, int(OKX_RECOVERY_GRACE_SECONDS or 120))
    meta_ttl = max(grace_ttl, int(OKX_RECOVERY_META_SECONDS or (15 * 60)))
    setup = _extract_signal_trade_setup(signal=signal, managed_order_result=managed_order_result, trade=trade)
    _OKX_RECENT_BOT_OPENED_SYMBOLS[inst_id] = {
        "grace_until": now_ts + grace_ttl,
        "meta_until": now_ts + meta_ttl,
        "reason": str(reason or "okx_order_success"),
        "symbol": inst_id,
        **setup,
    }
    print(
        f"OKX_RECOVERY_GRACE_MARK | {inst_id} | ttl={grace_ttl}s | meta_ttl={meta_ttl}s | reason={reason}",
        flush=True,
    )


def _recent_bot_okx_order_grace_remaining(symbol: object) -> int:
    inst_id = _normalize_okx_inst_id(symbol)
    if not inst_id:
        return 0
    now_ts = time.time()
    _purge_recent_bot_okx_order_marks(now_ts)
    mark = _OKX_RECENT_BOT_OPENED_SYMBOLS.get(inst_id, 0.0)
    expires_at = _okx_recent_mark_expiry(mark, "grace_until")
    return int(max(0.0, expires_at - now_ts))


def _recent_bot_okx_order_metadata(symbol: object) -> dict:
    inst_id = _normalize_okx_inst_id(symbol)
    if not inst_id:
        return {}
    now_ts = time.time()
    _purge_recent_bot_okx_order_marks(now_ts)
    mark = _OKX_RECENT_BOT_OPENED_SYMBOLS.get(inst_id)
    if not isinstance(mark, dict):
        return {}
    if _safe_float(mark.get("meta_until"), 0.0) <= now_ts:
        return {}
    return dict(mark)


def _row_inst_id(row: dict) -> str:
    return _normalize_okx_inst_id((row or {}).get("instId"))


def _row_float(row: dict, *keys: str) -> float:
    for key in keys:
        value = _safe_float((row or {}).get(key), 0.0)
        if abs(value) > 0:
            return value
    return 0.0


def _okx_result_rows(payload: dict | None) -> list[dict]:
    """Return OKX rows from either our normalized client shape or raw OKX shape.

    Some client helpers return {ok, rows}, while raw OKX responses use {code, data}.
    Execution safety must support both, otherwise the bot can think OKX has
    zero live positions and open duplicate symbols.
    """
    if not isinstance(payload, dict):
        return []
    rows = payload.get("rows")
    if rows is None:
        rows = payload.get("data")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _okx_result_is_ok(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if "ok" in payload:
        return bool(payload.get("ok"))
    return str(payload.get("code", "")) == "0"


def _okx_positions_debug_log(label: str, payload: dict | None, *, max_rows: int = 8) -> None:
    """Print compact OKX positions diagnostics without exposing secrets.

    This is intentionally log-only. It does not change trading decisions.
    It tells us whether OKX returned positions, what margin mode they are in,
    and why recovery/import might still be zero.
    """
    try:
        ok = _okx_result_is_ok(payload)
        response = (payload or {}).get("response") if isinstance(payload, dict) else None
        if not isinstance(response, dict):
            response = payload if isinstance(payload, dict) else {}
        code = str(response.get("code") or (payload or {}).get("code") or "") if isinstance(payload, dict) else ""
        msg = str(response.get("msg") or (payload or {}).get("reason") or (payload or {}).get("msg") or "") if isinstance(payload, dict) else ""
        rows = _okx_result_rows(payload)
        print(
            f"OKX_POSITIONS_DEBUG | {label} | ok={ok} | code={code or '-'} | rows={len(rows)} | msg={msg[:140] or '-'}",
            flush=True,
        )
        for row in rows[:max(1, int(max_rows or 8))]:
            inst_id = _row_inst_id(row)
            pos = _row_float(row, "pos", "availPos")
            notional = _row_float(row, "notionalUsd", "notional")
            margin = _row_float(row, "margin", "imr", "initialMargin", "marginUsd")
            avg = _position_row_price(row, "avgPx", "avgPxUsd", "openAvgPx", "entryPx")
            mark = _position_row_price(row, "markPx", "last", "lastPx", "idxPx")
            print(
                "OKX_POS_ROW | "
                f"{label} | instId={inst_id or '-'} | pos={pos} | notional={notional} | margin={margin} | "
                f"avgPx={avg} | markPx={mark} | mgnMode={row.get('mgnMode') or '-'} | "
                f"posSide={row.get('posSide') or '-'}",
                flush=True,
            )
    except Exception as exc:
        print(f"OKX_POSITIONS_DEBUG | {label} | log_failed={exc}", flush=True)


def _resolve_okx_td_mode(settings: Settings | None = None) -> str:
    """Resolve OKX tdMode with isolated as the live-execution safety default."""
    env_raw = str(os.getenv("OKX_TD_MODE") or os.getenv("TD_MODE") or "").strip().lower()
    settings_raw = ""
    try:
        settings_raw = str(getattr(settings, "okx_td_mode", "") or getattr(settings, "td_mode", "") or "").strip().lower()
    except Exception:
        settings_raw = ""

    raw = env_raw or settings_raw or "isolated"
    if raw not in {"isolated", "cross"}:
        print(f"⚠️ OKX_TD_MODE invalid '{raw}' — using isolated", flush=True)
        return "isolated"

    if raw == "cross":
        allow_cross = str(os.getenv("ALLOW_OKX_CROSS_MARGIN") or "").strip().lower() in {"1", "true", "yes", "on"}
        if not allow_cross:
            print("⚠️ OKX_TD_MODE resolved cross — forcing isolated (set ALLOW_OKX_CROSS_MARGIN=true to override)", flush=True)
            return "isolated"

    return raw


def _extract_live_okx_position_inst_ids(positions_result: dict | None) -> set[str]:
    if not _okx_result_is_ok(positions_result):
        return set()
    live: set[str] = set()
    for row in _okx_result_rows(positions_result):
        inst_id = _row_inst_id(row)
        if not inst_id:
            continue
        pos_size = _row_float(row, "pos", "availPos", "notionalUsd", "imr", "margin")
        if abs(pos_size) > 0:
            live.add(inst_id)
    return live


def _extract_pending_okx_order_inst_ids(pending_result: dict | None) -> set[str]:
    if not _okx_result_is_ok(pending_result):
        return set()
    pending: set[str] = set()
    for row in _okx_result_rows(pending_result):
        inst_id = _row_inst_id(row)
        state = str(row.get("state") or row.get("ordState") or "").lower()
        if inst_id and state not in {"filled", "canceled", "mmp_canceled"}:
            pending.add(inst_id)
    return pending


def _tracked_live_symbol_set(trades: list) -> set[str]:
    """Symbols already represented by an active execution TrackedTrade.

    TP2 runners are intentionally still represented here. They do not consume
    normal slots or block re-entry, but they must not be imported again from
    OKX as a second recovered trade.
    """
    out: set[str] = set()
    for trade in trades or []:
        if not getattr(trade, "execution_trade", False):
            continue
        status = str(getattr(trade, "status", "") or "").strip().lower()
        if bool(getattr(trade, "is_closed", False)) or status in {"closed", "closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit", "expired"}:
            continue
        inst_id = _trade_symbol_inst_id(trade)
        if inst_id:
            out.add(inst_id)
    return out


def _position_row_price(row: dict, *keys: str) -> float:
    for key in keys:
        value = _safe_float((row or {}).get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def _build_recovered_execution_trade_from_okx_position(row: dict, settings: Settings | None = None):
    """Build a conservative TrackedTrade for a live OKX position missing from Redis.

    This is a recovery layer, not a strategy entry. It exists so execution reports,
    slots, same-symbol protection, Daily DD and Loss-Streak protection have a
    live state to work with after Redis/report loss or redeploy. We do not invent
    TP/SL order ids; if they are unknown, lifecycle price TP detection is disabled
    by placing TP levels far away.
    """
    inst_id = _row_inst_id(row)
    if not inst_id:
        return None

    recovery_meta = _recent_bot_okx_order_metadata(inst_id)
    entry = _position_row_price(row, "avgPx", "avgPxUsd", "openAvgPx", "entryPx")
    current = _position_row_price(row, "markPx", "last", "lastPx", "idxPx") or entry
    cached_entry = _safe_float(recovery_meta.get("entry"), 0.0)
    if entry <= 0 and cached_entry > 0:
        entry = cached_entry
    if entry <= 0:
        entry = current
    if entry <= 0:
        return None

    now = datetime.now(timezone.utc)
    margin = _row_float(row, "margin", "imr", "initialMargin", "marginUsd")
    if margin <= 0:
        margin = _safe_float(recovery_meta.get("margin"), 0.0)
    notional = _row_float(row, "notionalUsd", "notional")
    leverage = _safe_float((row or {}).get("lever") or (row or {}).get("leverage"), 0.0)
    if leverage <= 0:
        leverage = _safe_float(recovery_meta.get("leverage"), 0.0)
    cached_sl = _safe_float(recovery_meta.get("sl"), 0.0)
    cached_tp1 = _safe_float(recovery_meta.get("tp1"), 0.0)
    cached_tp2 = _safe_float(recovery_meta.get("tp2"), 0.0)
    pnl_pct = ((current - entry) / entry) * 100.0 if current > 0 else 0.0
    trade_id = "okx_recovered_" + inst_id.replace("-", "_").lower()

    trade = TrackedTrade(
        symbol=inst_id,
        entry=float(entry),
        sl=float(cached_sl) if cached_sl > 0 else 0.0,
        tp1=float(cached_tp1) if cached_tp1 > 0 else float(entry) * 999.0,
        tp2=float(cached_tp2) if cached_tp2 > 0 else float(entry) * 999.0,
        setup_type="okx_recovered_position",
        market_mode="RECOVERED_FROM_OKX",
        score=0.0,
        trade_id=trade_id,
    )
    _safe_set_trade_attr(trade, "trade_source", "execution")
    _safe_set_trade_attr(trade, "tracking_bucket", "execution")
    _safe_set_trade_attr(trade, "execution_trade", True)
    _safe_set_trade_attr(trade, "execution_checked", True)
    _safe_set_trade_attr(trade, "execution_status", "recovered_live_okx_position")
    _safe_set_trade_attr(trade, "execution_reason", "okx_position_recovery")
    _safe_set_trade_attr(trade, "execution_path", "general")
    _safe_set_trade_attr(trade, "exchange_order_ok", True)
    _safe_set_trade_attr(trade, "exchange_order_reason", "recovered_from_okx_live_position")
    _safe_set_trade_attr(trade, "exchange_sync_state", "recovered_live_okx_position")
    _safe_set_trade_attr(trade, "last_exchange_sync_at", now)
    _safe_set_trade_attr(trade, "opened_at", now)
    _safe_set_trade_attr(trade, "updated_at", now)
    _safe_set_trade_attr(trade, "status", "open")
    _safe_set_trade_attr(trade, "current_price", float(current or entry))
    _safe_set_trade_attr(trade, "highest_price", float(max(entry, current or entry)))
    _safe_set_trade_attr(trade, "pnl_pct", float(pnl_pct))
    _safe_set_trade_attr(trade, "max_favorable_pct", max(0.0, float(pnl_pct)))
    _safe_set_trade_attr(trade, "max_adverse_pct", min(0.0, float(pnl_pct)))
    _safe_set_trade_attr(trade, "slot_exempt", False)
    _safe_set_trade_attr(trade, "daily_open_risk_exempt", False)
    _safe_set_trade_attr(trade, "same_symbol_block_exempt", False)
    _safe_set_trade_attr(trade, "blocks_same_symbol_reentry", True)
    _safe_set_trade_attr(trade, "target_model", "recovered_okx_position")
    _safe_set_trade_attr(trade, "tp1_close_pct", 30.0)
    _safe_set_trade_attr(trade, "tp2_close_pct", 50.0)
    _safe_set_trade_attr(trade, "runner_close_pct", 20.0)
    if margin > 0:
        _safe_set_trade_attr(trade, "used_margin_usdt", margin)
        _safe_set_trade_attr(trade, "margin_usdt", margin)
        _safe_set_trade_attr(trade, "allocated_margin_usdt", margin)
    if notional > 0:
        _safe_set_trade_attr(trade, "position_notional_usdt", notional)
    if leverage > 0:
        _safe_set_trade_attr(trade, "effective_leverage", leverage)
        _safe_set_trade_attr(trade, "actual_leverage", leverage)
    meta_td_mode = str(recovery_meta.get("td_mode") or "").strip()
    if meta_td_mode:
        _safe_set_trade_attr(trade, "td_mode", meta_td_mode)
        _safe_set_trade_attr(trade, "margin_mode", meta_td_mode)
    if recovery_meta:
        _safe_set_trade_attr(trade, "recovery_metadata_used", True)
        _safe_set_trade_attr(trade, "recovery_metadata_reason", recovery_meta.get("reason"))
    return trade


def _recover_missing_execution_trades_from_okx_positions(
    trades: list,
    okx_client: OKXTradeClient | None,
    settings: Settings,
) -> tuple[list, dict]:
    """Import live OKX positions missing from Redis as conservative execution trades.

    This keeps Simulation isolated: it only runs in live OKX execution mode. The
    imported records are intentionally conservative: they consume slots and block
    same-symbol re-entry before TP2, but they do not invent TP/SL fills.
    """
    stats = {
        "enabled": False,
        "changed": False,
        "imported": 0,
        "grace_skipped": 0,
        "reason": "not_live_okx_mode",
        "symbols": [],
        "grace_symbols": [],
    }
    if not _is_live_okx_execution_mode(settings, okx_client):
        return list(trades or []), stats

    try:
        positions_result = okx_client.get_positions(inst_type="SWAP") if hasattr(okx_client, "get_positions") else None
    except Exception as exc:
        stats.update({"enabled": True, "reason": f"positions_fetch_failed:{exc}"})
        print(f"OKX_POSITION_RECOVERY | fetch_failed={exc}", flush=True)
        return list(trades or []), stats

    _okx_positions_debug_log("recovery", positions_result)

    if not _okx_result_is_ok(positions_result):
        stats.update({"enabled": True, "reason": str((positions_result or {}).get("reason") or (positions_result or {}).get("msg") or "positions_not_ok")})
        print(f"OKX_POSITION_RECOVERY | not_ok | reason={stats.get('reason')}", flush=True)
        return list(trades or []), stats

    recovered = list(trades or [])
    represented = _tracked_live_symbol_set(recovered)
    imported_symbols: list[str] = []

    for row in _okx_result_rows(positions_result):
        inst_id = _row_inst_id(row)
        if not inst_id:
            continue
        pos_size = _row_float(row, "pos", "availPos", "notionalUsd", "imr", "margin")
        if abs(pos_size) <= 0:
            continue
        if inst_id in represented:
            print(f"OKX_POSITION_RECOVERY_SKIP | {inst_id} | reason=already_represented", flush=True)
            continue
        grace_remaining = _recent_bot_okx_order_grace_remaining(inst_id)
        if grace_remaining > 0:
            imported_symbols_marker = stats.setdefault("grace_symbols", [])
            if isinstance(imported_symbols_marker, list):
                imported_symbols_marker.append(inst_id)
            stats["grace_skipped"] = int(stats.get("grace_skipped", 0) or 0) + 1
            print(
                f"OKX_POSITION_RECOVERY_GRACE | {inst_id} | "
                f"remaining={grace_remaining}s | reason=recent_bot_okx_order",
                flush=True,
            )
            continue
        trade = _build_recovered_execution_trade_from_okx_position(row, settings=settings)
        if trade is None:
            print(f"OKX_POSITION_RECOVERY_SKIP | {inst_id} | reason=build_trade_failed", flush=True)
            continue
        recovered.append(trade)
        represented.add(inst_id)
        imported_symbols.append(inst_id)

    stats.update({
        "enabled": True,
        "changed": bool(imported_symbols),
        "imported": len(imported_symbols),
        "reason": "ok",
        "symbols": imported_symbols[:20],
    })
    if imported_symbols:
        print(
            f"♻️ OKX_POSITION_RECOVERY | imported={len(imported_symbols)} | symbols={','.join(imported_symbols)}",
            flush=True,
        )
    else:
        print(
            f"OKX_POSITION_RECOVERY | imported=0 | rows={len(_okx_result_rows(positions_result))} | represented={len(represented)} | reason=ok",
            flush=True,
        )
    return recovered, stats


def _fetch_live_okx_position_inst_ids_strict(okx_client: OKXTradeClient | None) -> tuple[bool, set[str], str]:
    """Fetch live OKX positions, fail-closed for execution guards.

    If we cannot read OKX positions, live execution must be blocked. Returning an
    empty set on API failure is unsafe because the bot may think there are no
    positions and open duplicate symbols / extra slots.
    """
    if okx_client is None or not hasattr(okx_client, "get_positions"):
        return False, set(), "okx_client_missing_get_positions"
    try:
        positions_result = okx_client.get_positions(inst_type="SWAP") or {}
    except Exception as exc:
        print(f"⚠️ OKX position fetch failed: {exc}", flush=True)
        return False, set(), f"okx_positions_fetch_exception:{exc}"
    _okx_positions_debug_log("strict_guard", positions_result, max_rows=6)
    if not _okx_result_is_ok(positions_result):
        reason = str((positions_result or {}).get("reason") or (positions_result or {}).get("msg") or "positions_not_ok")
        return False, set(), reason
    return True, _extract_live_okx_position_inst_ids(positions_result), "ok"


def _fetch_live_okx_position_inst_ids(okx_client: OKXTradeClient | None) -> set[str]:
    ok, live, reason = _fetch_live_okx_position_inst_ids_strict(okx_client)
    if not ok:
        print(f"⚠️ OKX live positions unavailable: {reason}", flush=True)
    return live


def _is_open_execution_trade_for_reconcile(trade) -> bool:
    return bool(
        _is_execution_report_trade_record(trade)
        and not bool(getattr(trade, "is_closed", False))
    )


def _estimate_reconcile_close_raw_pnl_pct(trade) -> float:
    """Best-effort raw PnL% when an OKX live position disappears.

    Manual OKX closes do not always pass through the lifecycle TP/SL handlers,
    so realized_pnl_pct can still be zero even after TP1/TP2 was reached.
    This helper preserves report analytics by converting the last known trade
    state into a raw realized percentage. report_format later applies leverage.
    """
    realized = _safe_float(getattr(trade, "realized_pnl_pct", 0.0), 0.0)
    if abs(realized) > 1e-12:
        return realized

    current_raw = _safe_float(getattr(trade, "pnl_pct", 0.0), 0.0)
    entry = _safe_float(getattr(trade, "entry", 0.0), 0.0)
    tp1 = _safe_float(getattr(trade, "tp1", 0.0), 0.0)
    tp2 = _safe_float(getattr(trade, "tp2", 0.0), 0.0)

    def _raw_move(price: float) -> float:
        if entry > 0 and price > 0:
            return ((price - entry) / entry) * 100.0
        return 0.0

    tp1_raw = _raw_move(tp1)
    tp2_raw = _raw_move(tp2)
    tp1_close = max(0.0, min(100.0, _safe_float(getattr(trade, "tp1_close_pct", 30.0), 30.0)))
    tp2_close = max(0.0, min(100.0, _safe_float(getattr(trade, "tp2_close_pct", 50.0), 50.0)))
    runner_close = max(0.0, min(100.0, _safe_float(getattr(trade, "runner_close_pct", 20.0), 20.0)))

    if bool(getattr(trade, "tp2_hit", False)):
        return (
            tp1_raw * (tp1_close / 100.0)
            + tp2_raw * (tp2_close / 100.0)
            + current_raw * (runner_close / 100.0)
        )

    if bool(getattr(trade, "tp1_hit", False)):
        remaining = max(0.0, 100.0 - tp1_close)
        return tp1_raw * (tp1_close / 100.0) + current_raw * (remaining / 100.0)

    return current_raw


def _mark_execution_trade_closed_by_reconcile(trade, reason: str = "okx_position_not_live"):
    """Preserve execution report history when OKX position disappears.

    Old behavior deleted the tracked trade from Redis when OKX no longer had a
    live position/order. That made /report_execution lose closed trades, WinRate,
    TP/SL stats and Realized PnL. In live mode a missing OKX position usually
    means the trade has finished on the exchange, so keep it as a closed record.
    """
    now = datetime.now(timezone.utc)
    try:
        status = str(getattr(trade, "status", "") or "").strip().lower()
        realized = _estimate_reconcile_close_raw_pnl_pct(trade)

        # Keep TP flags as-is; only fill realized PnL if lifecycle did not.
        if abs(_safe_float(getattr(trade, "realized_pnl_pct", 0.0), 0.0)) <= 1e-12:
            setattr(trade, "realized_pnl_pct", realized)
        setattr(trade, "manual_close_estimated_pnl_pct", realized)

        if status not in {"closed", "closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit", "expired"}:
            if realized > 0 or bool(getattr(trade, "tp1_hit", False)) or bool(getattr(trade, "tp2_hit", False)):
                status = "closed_win"
            elif realized < 0:
                status = "closed_loss"
            else:
                status = "breakeven_after_tp1" if bool(getattr(trade, "tp1_hit", False)) else "closed"
            setattr(trade, "status", status)

        setattr(trade, "is_closed", True)
        setattr(trade, "closed_at", getattr(trade, "closed_at", None) or now)
        setattr(trade, "updated_at", now)
        setattr(trade, "slot_exempt", True)
        setattr(trade, "blocks_same_symbol_reentry", False)
        setattr(trade, "same_symbol_block_exempt", True)
        setattr(trade, "exchange_sync_state", "closed_by_okx_reconcile")
        setattr(trade, "exchange_close_reason", reason)
        print(
            f"OKX_RECONCILE_CLOSED | {getattr(trade, 'symbol', '-') or '-'} | "
            f"status={status} | realized_raw={realized:+.4f}% | "
            f"tp1={bool(getattr(trade, 'tp1_hit', False))} | tp2={bool(getattr(trade, 'tp2_hit', False))} | reason={reason}",
            flush=True,
        )
    except Exception as exc:
        print(f"⚠️ OKX_RECONCILE_CLOSE_FAILED | {getattr(trade, 'symbol', '-') or '-'} | {exc}", flush=True)
    return trade


def _reconcile_execution_trades_with_okx(
    trades: list,
    okx_client: OKXTradeClient | None,
    settings: Settings,
) -> tuple[list, dict]:
    """Reconcile execution trades against OKX without deleting report history.

    OKX is still the live source of truth for slots and duplicate protection.
    But when an already tracked execution trade disappears from OKX, do not
    drop it from Redis. Mark it closed and let persistence move it to history.
    """
    stats = {
        "enabled": False,
        "changed": False,
        "removed": 0,
        "closed_by_reconcile": 0,
        "kept": len(trades or []),
        "reason": "not_live_okx_mode",
    }
    if not trades or not _is_live_okx_execution_mode(settings, okx_client):
        return list(trades or []), stats

    try:
        positions_result = okx_client.get_positions(inst_type="SWAP") if hasattr(okx_client, "get_positions") else None
    except Exception as exc:
        stats["reason"] = f"positions_fetch_failed:{exc}"
        return list(trades or []), stats

    if not _okx_result_is_ok(positions_result):
        stats["reason"] = str((positions_result or {}).get("reason") or (positions_result or {}).get("msg") or "positions_not_ok")
        return list(trades or []), stats

    try:
        pending_result = okx_client.list_pending_orders(inst_type="SWAP", limit=100)
    except Exception:
        pending_result = None

    live_inst_ids = _extract_live_okx_position_inst_ids(positions_result)
    pending_inst_ids = _extract_pending_okx_order_inst_ids(pending_result)
    protected_inst_ids = live_inst_ids | pending_inst_ids

    kept = []
    closed_count = 0
    closed_symbols = []
    for trade in list(trades or []):
        if _is_open_execution_trade_for_reconcile(trade):
            inst_id = _trade_symbol_inst_id(trade)
            if inst_id and inst_id not in protected_inst_ids:
                trade = _mark_execution_trade_closed_by_reconcile(trade, reason="okx_position_and_orders_missing")
                closed_count += 1
                closed_symbols.append(inst_id)
        kept.append(trade)

    stats.update({
        "enabled": True,
        "changed": closed_count > 0,
        "removed": 0,
        "closed_by_reconcile": closed_count,
        "kept": len(kept),
        "live_positions": len(live_inst_ids),
        "pending_orders": len(pending_inst_ids),
        "closed_symbols": closed_symbols[:20],
        "reason": "ok",
    })
    return kept, stats


def _rebuild_runtime_reports_after_reconcile(result: dict, trades: list, trade_store: RedisTradeStore | None, settings: Settings, okx_client: OKXTradeClient | None, stats: dict | None = None) -> None:
    if not isinstance(result, dict):
        return
    refreshed_checks = trade_store.load_execution_checks(limit=500) if trade_store else list(result.get("execution_results", []) or [])
    result["trades"] = list(trades or [])
    if stats:
        result["exchange_reconcile_stats"] = stats
    portfolio_state_inputs = _resolve_portfolio_state_inputs(okx_client, settings, trade_store=trade_store)
    result["portfolio_state_inputs"] = portfolio_state_inputs
    execution_report_kwargs = _execution_report_balance_kwargs(portfolio_state_inputs)
    reports = build_report_bundle(result["trades"], refreshed_checks, list(result.get("signal_items", []) or []), **execution_report_kwargs)
    result["command_outputs"] = build_command_outputs(result["trades"], refreshed_checks, list(result.get("signal_items", []) or []), **execution_report_kwargs)
    result.update(reports)
    portfolio_state = build_portfolio_state_from_trades(result["trades"], **_portfolio_state_kwargs(portfolio_state_inputs))
    result["portfolio_state"] = portfolio_state
    result["drawdown_status"] = evaluate_drawdown(portfolio_state)
    result["drawdown_report"] = build_drawdown_report(portfolio_state)
    result["loss_streak_guard"] = _build_loss_streak_guard(
        _loss_streak_base_trades_for_runtime(settings, result, execution_trades=result["trades"])
    )

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
    dom_change = getattr(snapshot, "btc_dominance_change_1h", None)
    dom_unknown = bool(getattr(snapshot, "btc_dominance_unknown", False) or dom_change is None)
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
        "btc_dominance_change_1h": dom_change,
        "btc_dominance_unknown": dom_unknown,
        "sample_size": int(getattr(snapshot, "market_guard_valid_count", 0) or getattr(snapshot, "market_guard_sample_size", 200) or 200),
        "market_mix": f"Strong Coins: {strong_coins} | Red Ratio: {red_ratio_pct:.0f}% | Avg 15m Move: {avg15m:.2f}%",
        "market_state": f"strong_coins={strong_coins} | avg15m={avg15m:.2f}% | red_ratio={red_ratio_pct:.0f}% | 1h_ma5={hourly_ma_guard}",
        "trigger": "fast rebound" if state.mode == MODE_RECOVERY_LONG else ("risk-off breadth" if state.mode == MODE_BLOCK_LONGS else "balanced scan"),
        "mode_reason": "fast rebound path" if state.mode == MODE_RECOVERY_LONG else "core market breadth decision",
        "signal_rules": "normal signal first → execution later",
        "requirements": "quality up" if state.mode != MODE_NORMAL_LONG else "balanced normal scanning",
        "execution_notes": "whitelist / elite / recovery / block-exception",
        "protection_level": protection.get("level", 0),
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
    settings: Settings | None = None,
    result: dict | None = None,
) -> str:
    runtime_settings = settings or get_settings()
    context = _build_mode_context(state, snapshot, protection)
    if variant == "reminder":
        minutes_in_mode = int((datetime.now(timezone.utc) - state.changed_at).total_seconds() // 60)
        context.update({"reminder_count": reminder_count, "minutes_in_mode": minutes_in_mode})
    if old_mode:
        context["old_mode"] = old_mode

    message = _compact_mode_message_text(build_market_mode_sections(state.mode, context, variant=variant))

    risk_result = dict(result or {})
    risk_result.setdefault("mode_context", context)
    risk_profile = _risk_profile_snapshot(runtime_settings, risk_result)
    risk_block = _format_risk_profile_block(risk_profile, title=_risk_profile_title(runtime_settings, risk_profile))
    return _append_protection_notice(message + "\n" + risk_block, risk_result)


def _refresh_risk_block_in_mode_message(message: str, settings: Settings, result: dict | None = None) -> str:
    """Refresh only the Risk Manager block using the current runtime snapshot.

    /mood may use a cached scan result while the user just switched between
    Simulation and Trading. The market analysis part can remain cached, but the
    bottom Risk Manager block must always reflect the current runtime mode.
    """
    base = str(message or "").strip()
    # Remove the old trailing risk block regardless of its previous context.
    base = re.sub(
        r"\n(?:🧪|🚀|🧮)\s*Risk Manager\s*(?:—\s*(?:Simulation|Execution))?.*\Z",
        "",
        base,
        flags=re.DOTALL,
    ).strip()
    risk_result = dict(result or {})
    risk_profile = _risk_profile_snapshot(settings, risk_result)
    risk_block = _format_risk_profile_block(risk_profile, title=_risk_profile_title(settings, risk_profile))
    return _append_protection_notice(base + "\n" + risk_block, risk_result)


def _refresh_mode_outputs(result: dict, state: MarketModeState, snapshot: MarketSnapshot, settings: Settings | None = None) -> dict:
    protection = block_protection_status(state)
    result["state"] = state
    result["mode"] = state.mode
    result["mode_context"] = _build_mode_context(state, snapshot, protection)
    result["mode_message"] = _build_mode_message(state, snapshot, protection, settings=settings, result=result)
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
    _refresh_mode_outputs(result, guarded_state, snapshot, settings=settings)
    if guarded_state.mode != previous_mode:
        reminder_tracker.clear()
        transition_message = _build_mode_message(
            guarded_state,
            snapshot,
            block_protection_status(guarded_state),
            variant="transition",
            old_mode=previous_mode,
            settings=settings,
            result=result,
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
    # Design rule: TP2 releases the slot even if a runner is still open.
    # The runner is managed as residual profit, not as a full active entry slot.
    if bool(getattr(trade, "tp2_hit", False)):
        return False
    counted = getattr(trade, "counts_as_active_slot", None)
    if counted is not None:
        return bool(counted)
    return bool(not _is_trade_closed(trade) and not getattr(trade, "slot_exempt", False))


def _blocks_same_symbol_reentry(trade) -> bool:
    # Design rule: after TP2, the same symbol is allowed to re-enter even if
    # a runner/protected runner is still live on OKX.
    if bool(getattr(trade, "same_symbol_block_exempt", False)):
        return False
    if bool(getattr(trade, "tp2_hit", False)):
        return False
    blocks = getattr(trade, "blocks_same_symbol_reentry", None)
    if blocks is not None:
        return bool(blocks)
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



def _parse_protection_dt(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _protection_scope(settings: Settings | None = None) -> str:
    try:
        runtime_settings = settings or get_settings()
        return "simulation" if _is_simulation_mode(runtime_settings) else "execution"
    except Exception:
        return "execution"


def _protection_state_key(scope: str) -> str:
    scope = str(scope or "execution").strip().lower()
    if scope not in {"execution", "simulation"}:
        scope = "execution"
    return f"{PROTECTION_STATE_PREFIX}:{scope}"


def _load_protection_state(trade_store: RedisTradeStore | None = None, scope: str = "execution") -> dict:
    key = _protection_state_key(scope)
    state = dict(_PROTECTION_RUNTIME_STATE.get(key) or {})
    if trade_store and getattr(trade_store, "enabled", False) and getattr(trade_store, "client", None):
        try:
            raw = trade_store.client.get(key)
            if raw:
                loaded = json.loads(raw)
                if isinstance(loaded, dict):
                    state.update(loaded)
        except Exception as exc:
            print(f"⚠️ protection state load failed: {exc}", flush=True)
    return state


def _save_protection_state(trade_store: RedisTradeStore | None, scope: str, state: dict) -> dict:
    key = _protection_state_key(scope)
    clean = dict(state or {})
    clean["scope"] = str(scope or "execution")
    clean["updated_at"] = datetime.now(timezone.utc).isoformat()
    _PROTECTION_RUNTIME_STATE[key] = clean
    if trade_store and getattr(trade_store, "enabled", False) and getattr(trade_store, "client", None):
        try:
            trade_store.client.set(
                key,
                json.dumps(clean, ensure_ascii=False, default=str),
                ex=PROTECTION_STATE_TTL_SECONDS,
            )
        except Exception as exc:
            print(f"⚠️ protection state save failed: {exc}", flush=True)
    return clean


def _apply_daily_dd_manual_baseline(portfolio_state_inputs: dict, protection_state: dict | None) -> dict:
    """Apply manual Daily DD baseline override for the current UTC day only."""
    inputs = dict(portfolio_state_inputs or {})
    state = dict(protection_state or {})
    baseline = _safe_float(state.get("daily_dd_baseline"), 0.0)
    resumed_at = _parse_protection_dt(state.get("manual_resume_at") or state.get("daily_dd_override_at"))
    now = datetime.now(timezone.utc)
    if baseline > 0 and resumed_at and resumed_at.date() == now.date():
        inputs["start_of_day_balance"] = baseline
        inputs["manual_daily_dd_override"] = True
        inputs["manual_daily_dd_baseline"] = baseline
        inputs["manual_resume_at"] = resumed_at.isoformat()
    return inputs


def _current_equity_for_manual_resume(result: dict | None, settings: Settings, portfolio_state_inputs: dict | None = None) -> float:
    result = result or {}
    if _is_simulation_mode(settings):
        wallet = result.get("simulation_wallet") or {}
        equity = _safe_float(wallet.get("equity"), 0.0)
        if equity > 0:
            return equity
    portfolio_state = result.get("portfolio_state")
    for attr in ("current_equity", "equity", "balance", "portfolio_value", "current_balance"):
        try:
            value = _safe_float(getattr(portfolio_state, attr), 0.0)
            if value > 0:
                return value
        except Exception:
            pass
    inputs = dict(portfolio_state_inputs or result.get("portfolio_state_inputs") or {})
    for key in ("reference_portfolio", "start_of_day_balance", "manual_daily_dd_baseline"):
        value = _safe_float(inputs.get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def _build_manual_resume_preview(result: dict | None, settings: Settings) -> str:
    scope = _protection_scope(settings)
    equity = _current_equity_for_manual_resume(result, settings)
    loss_guard = (result or {}).get("loss_streak_guard") or {}
    drawdown = (result or {}).get("drawdown_status")
    dd_line = "غير متاح"
    if drawdown is not None:
        try:
            dd_line = f"{float(getattr(drawdown, 'drawdown_pct', 0.0) or 0.0):.2f}% | مستوى {int(getattr(drawdown, 'level', 0) or 0)}"
        except Exception:
            pass
    return "\n".join([
        "⚠️ <b>استئناف يدوي للتداول — Preview</b>",
        "━━━━━━━━━━━━",
        f"النطاق: <b>{scope}</b>",
        f"الرصيد/الـ equity الحالي: <b>{equity:,.2f} USDT</b>",
        f"Daily DD الحالي: <code>{dd_line}</code>",
        f"Loss Streak الحالي: <code>{int(loss_guard.get('streak', 0) or 0)} / {int(loss_guard.get('limit', LOSS_STREAK_NO_TP1_LIMIT) or LOSS_STREAK_NO_TP1_LIMIT)}</code>",
        "",
        "🧯 <b>عند التأكيد سيتم:</b>",
        "• إعادة تفعيل فتح الصفقات.",
        "• تصفير عداد 5SL / No TP1 من هذه اللحظة.",
        "• اعتماد الرصيد الحالي كبداية جديدة للـ Daily DD لباقي اليوم.",
        "• تسجيل manual_resume_at داخل حالة الحماية.",
        "",
        "⚠️ لا يتم التنفيذ إلا بعد التأكيد الصريح.",
        "للتنفيذ أرسل: <code>/confirm_resume_trading</code>",
    ])


def _confirm_manual_resume_trading(
    result: dict | None,
    settings: Settings,
    trade_store: RedisTradeStore | None = None,
) -> str:
    scope = _protection_scope(settings)
    now = datetime.now(timezone.utc)
    equity = _current_equity_for_manual_resume(result, settings)
    state = _load_protection_state(trade_store, scope)
    state.update({
        "manual_override": True,
        "manual_resume_at": now.isoformat(),
        "loss_streak_reset_at": now.isoformat(),
        "daily_dd_override_at": now.isoformat(),
        "daily_dd_baseline": equity,
        "override_type": "manual_resume_trading",
        "reason": "manual_resume_after_protection",
    })
    _save_protection_state(trade_store, scope, state)

    if isinstance(result, dict):
        result["protection_state"] = state
        inputs = _apply_daily_dd_manual_baseline(dict(result.get("portfolio_state_inputs") or {}), state)
        result["portfolio_state_inputs"] = inputs
        try:
            trades_for_dd = result.get("simulation_trades") if _is_simulation_mode(settings) else result.get("trades")
            portfolio_state = build_portfolio_state_from_trades(list(trades_for_dd or []), **_portfolio_state_kwargs(inputs))
            result["portfolio_state"] = portfolio_state
            result["drawdown_status"] = evaluate_drawdown(portfolio_state)
            result["drawdown_report"] = build_drawdown_report(portfolio_state)
            base_trades = _loss_streak_base_trades_for_runtime(
                settings,
                result,
                execution_trades=list(result.get("trades", []) or []),
                simulation_trades=list(result.get("simulation_trades", []) or []),
            )
            result["loss_streak_guard"] = _build_loss_streak_guard(base_trades, reset_at=now)
        except Exception as exc:
            print(f"⚠️ manual resume runtime refresh failed: {exc}", flush=True)

    return "\n".join([
        "✅ <b>تم استئناف التداول يدويًا</b>",
        "━━━━━━━━━━━━",
        f"النطاق: <b>{scope}</b>",
        f"Baseline جديد للـ Daily DD: <b>{equity:,.2f} USDT</b>",
        "تم تصفير عداد حماية 5SL / No TP1 من هذه اللحظة.",
        "تم اعتماد الرصيد الحالي كبداية جديدة للـ Daily DD لباقي اليوم.",
        "أي تفعيل جديد للحماية سيُحسب من الصفقات التي تُغلق بعد وقت الاستئناف فقط.",
    ])


def _maybe_finalize_loss_streak_cooldown(
    guard: dict,
    trade_store: RedisTradeStore | None,
    scope: str,
    protection_state: dict | None = None,
) -> dict:
    """Persist reset_at once cooldown naturally ends, then let the next call count fresh losses only."""
    if not isinstance(guard, dict):
        return dict(protection_state or {})
    recommended = _parse_protection_dt(guard.get("reset_recommended_at"))
    if not recommended:
        return dict(protection_state or {})
    state = dict(protection_state or _load_protection_state(trade_store, scope))
    current_reset = _parse_protection_dt(state.get("loss_streak_reset_at"))
    if current_reset is None or current_reset < recommended:
        state["loss_streak_reset_at"] = recommended.isoformat()
        state["loss_streak_auto_reset_at"] = datetime.now(timezone.utc).isoformat()
        state["loss_streak_auto_reset_reason"] = "cooldown_finished"
        state = _save_protection_state(trade_store, scope, state)
    return state

def _build_loss_streak_guard(trades, now: datetime | None = None, reset_at: datetime | None = None) -> dict:
    """Return execution pause state after consecutive SL losses before TP1.

    The streak counts only bot execution trades that closed by SL before TP1.
    Any closed bot execution trade that reached TP1 resets the streak.
    """
    now = now or datetime.now(timezone.utc)
    reset_at = reset_at if isinstance(reset_at, datetime) else _parse_protection_dt(reset_at)
    closed_trades = sorted(
        [
            trade for trade in (trades or [])
            if _is_execution_closed_trade(trade)
            and (reset_at is None or _trade_closed_at(trade) > reset_at)
        ],
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
    reset_recommended_at = None
    if streak >= LOSS_STREAK_NO_TP1_LIMIT and last_loss_at is not None:
        cooldown_until = last_loss_at + timedelta(minutes=LOSS_STREAK_COOLDOWN_MINUTES)
        active = now < cooldown_until
        if active:
            remaining_minutes = max(1, int((cooldown_until - now).total_seconds() // 60))
        else:
            # Cooldown انتهى طبيعيًا → نوصي بتثبيت reset_at عند نهاية التهدئة.
            # هذا يمنع إعادة استخدام نفس الخمس خسائر القديمة في أي scan لاحق.
            reset_recommended_at = cooldown_until
            streak = 0
            streak_symbols = []

    return {
        "active": active,
        "streak": streak,
        "limit": LOSS_STREAK_NO_TP1_LIMIT,
        "cooldown_minutes": LOSS_STREAK_COOLDOWN_MINUTES,
        "remaining_minutes": remaining_minutes,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until else "",
        "last_loss_at": last_loss_at.isoformat() if last_loss_at else "",
        "reset_at": reset_at.isoformat() if reset_at else "",
        "reset_recommended_at": reset_recommended_at.isoformat() if reset_recommended_at else "",
        "symbols": streak_symbols[-LOSS_STREAK_NO_TP1_LIMIT:],
        "reason": "loss_streak_no_tp1_guard",
    }




def _format_remaining_minutes_ar(minutes: object) -> str:
    """Arabic display helper for protection cooldown counters."""
    total = max(0, int(_safe_float(minutes, 0.0) or 0))
    if total >= 60:
        hours = total // 60
        mins = total % 60
        if mins:
            return f"{hours} ساعة و {mins} دقيقة"
        return f"{hours} ساعة"
    return f"{total} دقيقة"


def _minutes_until_utc_day_end(now: datetime | None = None) -> int:
    """Minutes until the next UTC day boundary for Daily DD automatic reset display."""
    now = now or datetime.now(timezone.utc)
    end = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)
    return max(0, int((end - now).total_seconds() // 60))


def _loss_streak_guard_message_ar(guard: dict | None) -> str:
    """Human-readable Arabic message for the 5-loss protection pause."""
    guard = dict(guard or {})
    streak = int(guard.get("streak", 0) or 0)
    limit = int(guard.get("limit", LOSS_STREAK_NO_TP1_LIMIT) or LOSS_STREAK_NO_TP1_LIMIT)
    cooldown = int(guard.get("cooldown_minutes", LOSS_STREAK_COOLDOWN_MINUTES) or LOSS_STREAK_COOLDOWN_MINUTES)
    remaining = int(guard.get("remaining_minutes", 0) or 0)

    lines = [
        (
            f"🛡️ تم إيقاف فتح صفقات جديدة لمدة {cooldown} دقيقة بسبب "
            f"{max(streak, limit)} صفقات متتالية لم تحقق TP1."
        ),
        "هذا إجراء وقائي يهدف إلى الحد من التداول أثناء فترات ضعف أداء السوق.",
    ]
    if bool(guard.get("active")):
        lines.append(f"⏳ الوقت المتبقي: {_format_remaining_minutes_ar(remaining)}.")
    lines.append("سيستمر البوت في متابعة السوق وإرسال الإشارات، لكن بدون فتح صفقات جديدة أثناء الحماية.")
    return "\n".join(lines)


def _drawdown_protection_message_ar(drawdown_status) -> str:
    """Human-readable Arabic message for daily drawdown protection."""
    if drawdown_status is None:
        return ""
    try:
        level = int(getattr(drawdown_status, "level", 0) or 0)
        allowed = bool(getattr(drawdown_status, "allowed", True))
        dd_pct = float(getattr(drawdown_status, "drawdown_pct", 0.0) or 0.0)
        message = str(getattr(drawdown_status, "message_ar", "") or "").strip()
    except Exception:
        return ""

    if level <= 0 and allowed:
        return ""

    if not allowed:
        remaining = _format_remaining_minutes_ar(_minutes_until_utc_day_end())
        return (
            f"🛡️ تم إيقاف فتح صفقات جديدة بسبب تجاوز حد الخسارة اليومية.\n"
            f"📉 الخسارة اليومية الحالية: {dd_pct:.2f}%.\n"
            f"⏳ المتبقي لنهاية اليوم: {remaining}.\n"
            "لن يتم فتح أي صفقات جديدة حتى نهاية اليوم أو حتى استئناف التداول يدويًا.\n"
            f"{message}"
        ).strip()

    return (
        f"🛡️ حماية السحب اليومي نشطة — مستوى {level}.\n"
        f"📉 الخسارة اليومية الحالية: {dd_pct:.2f}%.\n"
        f"{message}"
    ).strip()


def _block_mode_protection_message_ar(result: dict | None) -> str:
    """Human-readable Arabic message for market BLOCK reminder protection."""
    result = result or {}
    ctx = dict(result.get("mode_context") or {})
    mode = str(result.get("mode") or ctx.get("mode") or "").strip()
    if mode != MODE_BLOCK_LONGS:
        return ""

    level = int(_safe_float(ctx.get("protection_level") or 0, 0.0) or 0)
    # Older context does not include protection_level, so infer it from text.
    current = str(ctx.get("protection_current") or "").strip()
    if level <= 0:
        if "LEVEL 3" in current:
            level = 3
        elif "LEVEL 2" in current:
            level = 2
        elif "LEVEL 1" in current:
            level = 1

    remaining = int(_safe_float(ctx.get("remaining_minutes"), 0.0) or 0)
    if level <= 0 and current in {"", "inactive"}:
        return ""

    lines = [
        f"🛡️ حماية السوق نشطة — مستوى {max(level, 1)}.",
        "تم تشديد التعامل مع فتح الصفقات بسبب ضغط واضح في حالة السوق.",
    ]
    if remaining > 0:
        lines.append(f"⏳ الوقت المتبقي للمرحلة الحالية: {_format_remaining_minutes_ar(remaining)}.")
    return "\n".join(lines)


def _protection_notice_text(result: dict | None) -> str:
    """Build a compact Arabic protection notice for mode/reminder/status messages."""
    result = result or {}
    notices: list[str] = []

    loss_guard = result.get("loss_streak_guard") or {}
    if isinstance(loss_guard, dict) and loss_guard.get("active"):
        notices.append(_loss_streak_guard_message_ar(loss_guard))

    drawdown_message = _drawdown_protection_message_ar(result.get("drawdown_status"))
    if drawdown_message:
        notices.append(drawdown_message)

    block_message = _block_mode_protection_message_ar(result)
    if block_message:
        notices.append(block_message)

    notices = [notice.strip() for notice in notices if str(notice or "").strip()]
    if not notices:
        return ""

    return "🛡️ <b>تنبيه الحماية</b>\n" + "\n\n".join(notices)


def _append_protection_notice(message: str, result: dict | None) -> str:
    notice = _protection_notice_text(result)
    if not notice:
        return str(message or "").strip()
    base = str(message or "").strip()
    if "🛡️ <b>تنبيه الحماية</b>" in base:
        return base
    return (base + "\n\n" + notice).strip()




def _loss_streak_base_trades_for_runtime(
    settings: Settings,
    result: dict | None = None,
    execution_trades: list | None = None,
    simulation_trades: list | None = None,
) -> list:
    """Return the correct loss-streak source for the active runtime mode.

    Simulation counts simulation trades only. Execution/trading counts real
    execution trades only. This prevents report refresh/reset/reconcile paths
    from overwriting the guard with the wrong trade source.
    """
    result = result or {}
    if _is_simulation_mode(settings):
        if simulation_trades is not None:
            return list(simulation_trades or [])
        return list(result.get("simulation_trades", []) or [])
    if execution_trades is not None:
        return list(execution_trades or [])
    return list(result.get("trades", []) or [])


def _loss_streak_rejection(guard: dict) -> dict:
    message_ar = _loss_streak_guard_message_ar(guard)
    return {
        "status": "protection_pause",
        "reason": "cooldown_after_consecutive_losses",
        "raw_reason": guard.get("reason") or "loss_streak_no_tp1_guard",
        "path": "",
        "slot_scope": "loss_streak_guard",
        "rejection_category": "protection_pause",
        "protection_active": bool(guard.get("active")),
        "protection_type": "loss_streak_guard",
        "protection_remaining_minutes": int(guard.get("remaining_minutes", 0) or 0),
        "loss_streak": int(guard.get("streak", 0) or 0),
        "loss_streak_limit": int(guard.get("limit", LOSS_STREAK_NO_TP1_LIMIT) or LOSS_STREAK_NO_TP1_LIMIT),
        "cooldown_minutes": int(guard.get("cooldown_minutes", LOSS_STREAK_COOLDOWN_MINUTES) or LOSS_STREAK_COOLDOWN_MINUTES),
        "cooldown_remaining_minutes": int(guard.get("remaining_minutes", 0) or 0),
        "cooldown_until": guard.get("cooldown_until", ""),
        "human_reason": message_ar,
        "message_ar": message_ar,
    }


def _hard_execution_protection_rejection(drawdown_status=None, loss_streak_guard: dict | None = None) -> dict | None:
    """Return a no-exceptions execution block for true danger protections.

    Hard protections are deliberately stronger than market-mode exceptions:
    - Daily DD hard stop (drawdown_status.allowed == False)
    - Loss-streak cooldown after repeated SL/no-TP1 losses

    Signals may still be built and reported, but no whitelist/elite/recovery/
    block-exception path is allowed to open a trade while this returns a row.
    """
    loss_guard = dict(loss_streak_guard or {})
    if loss_guard.get("active"):
        row = _loss_streak_rejection(loss_guard)
        row["hard_protection"] = True
        row["no_exceptions"] = True
        row["execution_block_only"] = True
        row["reason"] = "hard_loss_streak_pause_no_exceptions"
        row.setdefault("raw_reason", loss_guard.get("reason") or "loss_streak_no_tp1_guard")
        return row

    if drawdown_status is not None and not bool(getattr(drawdown_status, "allowed", True)):
        message_ar = _drawdown_protection_message_ar(drawdown_status)
        return {
            "status": "protection_pause",
            "reason": "hard_daily_drawdown_pause_no_exceptions",
            "raw_reason": str(getattr(drawdown_status, "reason", "") or "daily_drawdown_guard"),
            "path": "",
            "slot_scope": "daily_drawdown_guard",
            "rejection_category": "protection_pause",
            "protection_active": True,
            "protection_type": "daily_drawdown_guard",
            "hard_protection": True,
            "no_exceptions": True,
            "execution_block_only": True,
            "drawdown_level": int(getattr(drawdown_status, "level", 0) or 0),
            "drawdown_pct": float(getattr(drawdown_status, "drawdown_pct", 0.0) or 0.0),
            "drawdown_message": str(getattr(drawdown_status, "message_ar", "") or ""),
            "human_reason": message_ar,
            "message_ar": message_ar,
        }

    return None


def _active_protections_snapshot(result: dict | None) -> list[dict]:
    """Compact machine-readable protection list for reports and AI exports."""
    result = result or {}
    out: list[dict] = []

    loss_guard = result.get("loss_streak_guard") or {}
    if isinstance(loss_guard, dict):
        out.append({
            "type": "loss_streak_guard",
            "active": bool(loss_guard.get("active")),
            "streak": int(loss_guard.get("streak", 0) or 0),
            "limit": int(loss_guard.get("limit", LOSS_STREAK_NO_TP1_LIMIT) or LOSS_STREAK_NO_TP1_LIMIT),
            "remaining_minutes": int(loss_guard.get("remaining_minutes", 0) or 0),
            "cooldown_until": str(loss_guard.get("cooldown_until") or ""),
            "reset_at": str(loss_guard.get("reset_at") or ""),
            "no_exceptions": bool(loss_guard.get("active")),
        })

    drawdown = result.get("drawdown_status")
    if drawdown is not None:
        try:
            allowed = bool(getattr(drawdown, "allowed", True))
            level = int(getattr(drawdown, "level", 0) or 0)
            dd_pct = float(getattr(drawdown, "drawdown_pct", 0.0) or 0.0)
        except Exception:
            allowed, level, dd_pct = True, 0, 0.0
        out.append({
            "type": "daily_drawdown_guard",
            "active": not allowed,
            "level": level,
            "drawdown_pct": dd_pct,
            "remaining_minutes": _minutes_until_utc_day_end() if not allowed else 0,
            "no_exceptions": not allowed,
        })

    return out


def _risk_protection_summary(result: dict | None) -> dict:
    """Expose protection state with the priority agreed for JSON/AI reports."""
    result = result or {}
    protection_state = dict(result.get("protection_state") or {})
    loss_guard = dict(result.get("loss_streak_guard") or {})
    drawdown = result.get("drawdown_status")
    daily_active = False
    daily_level = 0
    daily_pct = 0.0
    if drawdown is not None:
        try:
            daily_active = not bool(getattr(drawdown, "allowed", True))
            daily_level = int(getattr(drawdown, "level", 0) or 0)
            daily_pct = float(getattr(drawdown, "drawdown_pct", 0.0) or 0.0)
        except Exception:
            pass
    return {
        "priority": ["daily_drawdown_guard", "loss_streak_guard", "market_block_recovery", "normal_filters"],
        "hard_protection_active": bool(daily_active or loss_guard.get("active")),
        "no_exceptions_when_hard_active": bool(daily_active or loss_guard.get("active")),
        "active_protections": _active_protections_snapshot(result),
        "loss_streak_guard": {
            "active": bool(loss_guard.get("active")),
            "streak": int(loss_guard.get("streak", 0) or 0),
            "limit": int(loss_guard.get("limit", LOSS_STREAK_NO_TP1_LIMIT) or LOSS_STREAK_NO_TP1_LIMIT),
            "remaining_minutes": int(loss_guard.get("remaining_minutes", 0) or 0),
            "reset_at": str(loss_guard.get("reset_at") or protection_state.get("loss_streak_reset_at") or ""),
            "cooldown_until": str(loss_guard.get("cooldown_until") or ""),
        },
        "daily_dd_guard": {
            "active": daily_active,
            "level": daily_level,
            "drawdown_pct": daily_pct,
            "remaining_minutes": _minutes_until_utc_day_end() if daily_active else 0,
            "manual_baseline": _safe_float(protection_state.get("daily_dd_baseline"), 0.0),
        },
        "manual_resume_at": str(protection_state.get("manual_resume_at") or ""),
        "loss_streak_reset_at": str(protection_state.get("loss_streak_reset_at") or ""),
        "daily_dd_baseline": _safe_float(protection_state.get("daily_dd_baseline"), 0.0),
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
        allocation_pct, slot_count = _risk_sizing_constants(settings, reference_balance=float(balance or 0.0))
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
        try:
            pct = _report_trade_effective_pnl(trade)
        except Exception:
            pct = _trade_effective_pnl_pct(trade)
        equity += _money_from_pct(pct, margin=margin)
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
            trade_store.client.set(SIMULATION_BALANCE_STATE_KEY, json.dumps(row, ensure_ascii=False, default=str), ex=180 * 24 * 60 * 60)
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
                pipe.set(key, payload, ex=90 * 24 * 60 * 60)
                pipe.srem(SIMULATION_OPEN_SET, trade_id)
                pipe.sadd(SIMULATION_HISTORY_SET, trade_id)
                pipe.expire(SIMULATION_HISTORY_SET, 90 * 24 * 60 * 60)
            else:
                pipe.set(key, payload, ex=90 * 24 * 60 * 60)
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
        try:
            pct = _report_trade_effective_pnl(trade)
        except Exception:
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


def _parse_simulation_wallet_day(value: object):
    """Parse a persisted simulation wallet day safely."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except Exception:
        return None


def _format_simulation_wallet_day(value: object, include_year: bool = False) -> str:
    day = _parse_simulation_wallet_day(value)
    if not day:
        return "-"
    return day.strftime("%d-%m-%Y" if include_year else "%d-%m")


def _simulation_wallet_journal_rows(
    rows: list[dict],
    current_row: dict | None = None,
) -> list[dict]:
    """Build daily journal rows where each day starts from previous close.

    This is Simulation-only. Execution reports and live OKX state are untouched.
    """
    merged: dict[str, dict] = {}

    for item in rows or []:
        if not isinstance(item, dict):
            continue
        day = str(item.get("date") or "").strip()
        if day:
            merged[day] = dict(item)

    if isinstance(current_row, dict):
        day = str(current_row.get("date") or "").strip()
        if day:
            merged[day] = dict(current_row)

    if not merged:
        today = _simulation_today_key()
        merged[today] = {
            "date": today,
            "start_balance": SIMULATION_START_BALANCE_USDT,
            "current_balance": SIMULATION_START_BALANCE_USDT,
            "end_balance": SIMULATION_START_BALANCE_USDT,
        }

    sorted_items = sorted(
        merged.values(),
        key=lambda item: str(item.get("date") or ""),
    )

    journal: list[dict] = []
    previous_close: float | None = None

    for item in sorted_items:
        day_text = str(item.get("date") or "").strip()
        if not day_text:
            continue

        stored_start = _safe_float(item.get("start_balance"), SIMULATION_START_BALANCE_USDT)
        start_balance = previous_close if previous_close is not None else stored_start
        if start_balance <= 0:
            start_balance = SIMULATION_START_BALANCE_USDT

        close_balance = _safe_float(item.get("end_balance") or item.get("current_balance"), start_balance)
        if close_balance <= 0:
            close_balance = start_balance

        pnl = close_balance - start_balance
        pnl_pct = (pnl / start_balance * 100.0) if start_balance else 0.0
        if pnl > 0:
            result = "🟢 Profit"
        elif pnl < 0:
            result = "🔴 Loss"
        else:
            result = "⚪ Flat"

        journal.append({
            "date": day_text,
            "day": _parse_simulation_wallet_day(day_text),
            "start": start_balance,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "close": close_balance,
            "result": result,
        })
        previous_close = close_balance

    return journal


def _simulation_wallet_period_rows(journal: list[dict], days: int | None = None) -> list[dict]:
    if not journal:
        return []
    if days is None:
        return list(journal)

    valid_days = [row.get("day") for row in journal if row.get("day") is not None]
    if not valid_days:
        return list(journal[-max(1, int(days)):])

    end_day = max(valid_days)
    cutoff = end_day - timedelta(days=max(0, int(days) - 1))
    return [row for row in journal if row.get("day") is None or row.get("day") >= cutoff]


def _format_simulation_wallet_money(value: object, signed: bool = False) -> str:
    number = _safe_float(value, 0.0)
    if signed:
        return f"{number:+,.2f}"
    return f"{number:,.2f}"


def _build_simulation_wallet_period_block(title: str, rows: list[dict]) -> str:
    if not rows:
        return "\n".join([
            f"📊 <b>{title}</b>",
            "From: - → -",
            "",
            "<code>No wallet rows yet.</code>",
        ])

    from_date = _format_simulation_wallet_day(rows[0].get("date"), include_year=True)
    to_date = _format_simulation_wallet_day(rows[-1].get("date"), include_year=True)

    table_lines = [
        f"{'Date':<7} {'Start$':>10} {'PnL$':>10} {'PnL%':>8} {'Close$':>10} Result",
    ]
    for row in rows:
        table_lines.append(
            f"{_format_simulation_wallet_day(row.get('date')):<7} "
            f"{_format_simulation_wallet_money(row.get('start')):>10} "
            f"{_format_simulation_wallet_money(row.get('pnl'), signed=True):>10} "
            f"{_safe_float(row.get('pnl_pct'), 0.0):+7.2f}% "
            f"{_format_simulation_wallet_money(row.get('close')):>10} "
            f"{row.get('result') or '-'}"
        )

    return "\n".join([
        f"📊 <b>{title}</b>",
        f"From: {from_date} → {to_date}",
        "",
        "<code>" + "\n".join(table_lines) + "</code>",
    ])


def _build_simulation_wallet_journal_report(result: dict | None = None) -> str:
    """Detailed Simulation wallet journal for /report_simulation_wallet.

    Sections:
    - Since Start
    - Last Month (30 days)
    - Last Week (7 days)
    """
    result = result or {}
    journal = _simulation_wallet_journal_rows(
        list(result.get("simulation_daily_log", []) or []),
        result.get("simulation_daily_balance") or {},
    )

    current = _safe_float(journal[-1].get("close") if journal else SIMULATION_START_BALANCE_USDT, SIMULATION_START_BALANCE_USDT)
    start = _safe_float(journal[0].get("start") if journal else SIMULATION_START_BALANCE_USDT, SIMULATION_START_BALANCE_USDT)
    net = current - start
    net_pct = (net / start * 100.0) if start else 0.0
    best = max((row.get("pnl", 0.0) for row in journal), default=0.0)
    worst = min((row.get("pnl", 0.0) for row in journal), default=0.0)

    lines = [
        "💼 <b>Simulation Wallet Journal</b>",
        "━━━━━━━━━━━━",
        f"Current: <code>{current:,.2f} USDT</code>",
        f"Net Since Start: <code>{net:+,.2f} USDT | {net_pct:+.2f}%</code>",
        f"Best Day: <code>{best:+,.2f} USDT</code> | Worst Day: <code>{worst:+,.2f} USDT</code>",
        "",
        _build_simulation_wallet_period_block("Since Start", _simulation_wallet_period_rows(journal, None)),
        "",
        _build_simulation_wallet_period_block("Last Month", _simulation_wallet_period_rows(journal, 30)),
        "",
        _build_simulation_wallet_period_block("Last Week", _simulation_wallet_period_rows(journal, 7)),
    ]
    return "\n".join(lines).strip()


_SIM_WALLET_PERIOD_COMMANDS = {
    "/report_simulation_wallet_since_start": ("since_start", "Since Start", None),
    "/simulation_wallet_since_start": ("since_start", "Since Start", None),
    "/report_simulation_wallet_30d": ("30d", "Last Month", 30),
    "/simulation_wallet_30d": ("30d", "Last Month", 30),
    "/report_simulation_wallet_7d": ("7d", "Last Week", 7),
    "/simulation_wallet_7d": ("7d", "Last Week", 7),
}


def _simulation_wallet_period_payload(result: dict | None, title: str, days: int | None) -> dict:
    result = result or {}
    journal = _simulation_wallet_journal_rows(
        list(result.get("simulation_daily_log", []) or []),
        result.get("simulation_daily_balance") or {},
    )
    rows = _simulation_wallet_period_rows(journal, days)
    start = _safe_float(rows[0].get("start") if rows else SIMULATION_START_BALANCE_USDT, SIMULATION_START_BALANCE_USDT)
    close = _safe_float(rows[-1].get("close") if rows else start, start)
    net = close - start
    net_pct = (net / start * 100.0) if start else 0.0
    best_row = max(rows, key=lambda r: _safe_float(r.get("pnl"), 0.0), default=None)
    worst_row = min(rows, key=lambda r: _safe_float(r.get("pnl"), 0.0), default=None)
    wins = sum(1 for row in rows if _safe_float(row.get("pnl"), 0.0) > 0)
    losses = sum(1 for row in rows if _safe_float(row.get("pnl"), 0.0) < 0)
    flats = sum(1 for row in rows if abs(_safe_float(row.get("pnl"), 0.0)) <= 1e-9)
    return {
        "title": title,
        "from": _format_simulation_wallet_day(rows[0].get("date"), include_year=True) if rows else "-",
        "to": _format_simulation_wallet_day(rows[-1].get("date"), include_year=True) if rows else "-",
        "start_balance": start,
        "close_balance": close,
        "net_usdt": net,
        "net_pct": net_pct,
        "days": len(rows),
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "best_day": best_row,
        "worst_day": worst_row,
        "rows": rows,
    }


def _build_simulation_wallet_period_report(result: dict | None, title: str, days: int | None) -> str:
    payload = _simulation_wallet_period_payload(result, title, days)
    block = _build_simulation_wallet_period_block(title, payload.get("rows") or [])
    return "\n".join([
        "💼 <b>Simulation Wallet Impact</b>",
        "━━━━━━━━━━━━",
        f"Report: <b>{title}</b>",
        f"From: <code>{payload.get('from')}</code> → <code>{payload.get('to')}</code>",
        f"Days: <b>{int(payload.get('days', 0) or 0)}</b>",
        f"Net: <b>{_safe_float(payload.get('net_usdt'), 0.0):+,.2f} USDT | {_safe_float(payload.get('net_pct'), 0.0):+.2f}%</b>",
        f"Result Days: 🟢 {payload.get('wins', 0)} | 🔴 {payload.get('losses', 0)} | ⚪ {payload.get('flats', 0)}",
        "",
        block,
        "",
        "📎 CSV + JSON exports are sent with this report.",
    ]).strip()


def _wallet_export_safe_name(title: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", str(title or "wallet").strip().lower()).strip("_") or "wallet"


def _build_simulation_wallet_export_files(result: dict | None, title: str, days: int | None) -> list[dict]:
    payload = _simulation_wallet_period_payload(result, title, days)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe = _wallet_export_safe_name(title)
    from_safe = _wallet_export_safe_name(str(payload.get("from") or "start"))
    to_safe = _wallet_export_safe_name(str(payload.get("to") or "now"))
    base = f"/tmp/simulation_wallet_{safe}_{from_safe}_to_{to_safe}_{stamp}"
    csv_path = base + ".csv"
    json_path = base + ".json"

    rows = payload.get("rows") or []
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["date", "start_usdt", "pnl_usdt", "pnl_pct", "close_usdt", "result"])
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "date": _format_simulation_wallet_day(row.get("date"), include_year=True),
                "start_usdt": round(_safe_float(row.get("start"), 0.0), 6),
                "pnl_usdt": round(_safe_float(row.get("pnl"), 0.0), 6),
                "pnl_pct": round(_safe_float(row.get("pnl_pct"), 0.0), 6),
                "close_usdt": round(_safe_float(row.get("close"), 0.0), 6),
                "result": str(row.get("result") or ""),
            })

    json_payload = dict(payload)
    json_payload["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    json_payload["rows"] = [
        {
            "date": _format_simulation_wallet_day(row.get("date"), include_year=True),
            "raw_date": str(row.get("date") or ""),
            "start_usdt": _safe_float(row.get("start"), 0.0),
            "pnl_usdt": _safe_float(row.get("pnl"), 0.0),
            "pnl_pct": _safe_float(row.get("pnl_pct"), 0.0),
            "close_usdt": _safe_float(row.get("close"), 0.0),
            "result": str(row.get("result") or ""),
        }
        for row in rows
    ]
    for key in ("best_day", "worst_day"):
        row = json_payload.get(key)
        if isinstance(row, dict):
            json_payload[key] = {
                "date": _format_simulation_wallet_day(row.get("date"), include_year=True),
                "pnl_usdt": _safe_float(row.get("pnl"), 0.0),
                "pnl_pct": _safe_float(row.get("pnl_pct"), 0.0),
                "close_usdt": _safe_float(row.get("close"), 0.0),
                "result": str(row.get("result") or ""),
            }
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(json_payload, fh, ensure_ascii=False, indent=2, default=str)

    return [
        {"path": csv_path, "caption": f"Simulation Wallet {title} CSV"},
        {"path": json_path, "caption": f"Simulation Wallet {title} JSON"},
    ]


def _simulation_wallet_menu_text() -> str:
    return "\n".join([
        "💼 <b>Simulation Wallet Impact</b>",
        "━━━━━━━━━━━━",
        "اختار فترة التقرير:",
        "",
        "📊 Since Start",
        "/report_simulation_wallet_since_start",
        "",
        "📆 Last Month",
        "/report_simulation_wallet_30d",
        "",
        "🗓 Last Week",
        "/report_simulation_wallet_7d",
        "",
        "كل أمر يرسل تقرير Telegram + ملف CSV + ملف JSON.",
    ])

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


def _normalize_wallet_impact_percentages_for_capital(text: str, capital_base: float) -> str:
    """Normalize Wallet Impact percentages to full wallet capital.

    Shared report builders express PnL% as exposure/trade PnL. For Wallet Impact,
    the money value is the source of truth and the displayed percentage should be
    wallet impact versus the simulation baseline capital.
    """
    value = str(text or "")
    capital = _safe_float(capital_base, 0.0)
    if capital <= 0:
        return value

    money_pattern = re.compile(
        r"(?P<money>[+-]\s*\d[\d,]*(?:\.\d+)?)\$\s*\|\s*"
        r"(?P<old>[+-]\s*\d[\d,]*(?:\.\d+)?)%\s*"
        r"(?P<label>(?:Realized PnL|Total Floating PnL))"
    )

    def _replace(match: re.Match) -> str:
        money_text = str(match.group("money") or "").replace(" ", "").replace(",", "")
        money = _safe_float(money_text, 0.0)
        wallet_pct = (money / capital) * 100.0 if capital else 0.0
        sign_money = "+" if money >= 0 else ""
        sign_pct = "+" if wallet_pct >= 0 else ""
        return f"{sign_money}{money:.2f}$ | {sign_pct}{wallet_pct:.2f}% {match.group('label')}"

    return money_pattern.sub(_replace, value)


def _simulation_wallet_capital_base(result: dict | None = None) -> float:
    """Simulation Wallet Impact baseline.

    The baseline is the start of the current simulation experiment. It stays
    1000 USDT after restart/deploy and changes only when simulation reset logic
    creates a new baseline.
    """
    result = result or {}
    wallet = result.get("simulation_wallet") or {}
    daily = result.get("simulation_daily_balance") or {}
    for value in (
        wallet.get("start_balance") if isinstance(wallet, dict) else None,
        daily.get("start_balance") if isinstance(daily, dict) else None,
        SIMULATION_START_BALANCE_USDT,
    ):
        base = _safe_float(value, 0.0)
        if base > 0:
            return base
    return SIMULATION_START_BALANCE_USDT



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

    wallet_text = _simulation_header(_simulation_wallet_menu_text())

    daily_balance_text = _simulation_header("\n".join([
        "📅 <b>Simulation Daily Balance</b>",
        "━━━━━━━━━━━━",
        *_format_simulation_equity_curve_rows(
            list(result.get("simulation_daily_log", []) or []),
            result.get("simulation_daily_balance") or {},
            limit=10,
        ),
    ]))

    outputs = build_simulation_report_command_outputs(
        result,
        account_summary=_build_simulation_account_summary(result),
        wallet_text=wallet_text,
        daily_balance_text=daily_balance_text,
    )
    capital_base = _simulation_wallet_capital_base(result)
    outputs = {
        key: _normalize_wallet_impact_percentages_for_capital(value, capital_base)
        for key, value in (outputs or {}).items()
    }
    for cmd, (_key, title, days) in _SIM_WALLET_PERIOD_COMMANDS.items():
        outputs[cmd] = _simulation_header(_build_simulation_wallet_period_report(result, title, days))
    outputs["/report_simulation_wallet"] = _simulation_header(_simulation_wallet_menu_text())
    outputs["/simulation_wallet"] = _simulation_header(_simulation_wallet_menu_text())
    return outputs


def _trade_identity_key(trade) -> str:
    trade_id = str(getattr(trade, "trade_id", "") or "").strip()
    if trade_id:
        return "id:" + trade_id
    return "sym:" + _normalize_okx_inst_id(getattr(trade, "symbol", ""))


def _trade_active_sl_value(trade) -> float:
    values = [
        _safe_float(getattr(trade, "live_stop_loss_px", 0.0), 0.0),
        _safe_float(getattr(trade, "protected_sl", 0.0), 0.0),
        _safe_float(getattr(trade, "sl", 0.0), 0.0),
    ]
    return max([v for v in values if v > 0] or [0.0])


def _execution_lifecycle_snapshot(trades: list) -> dict:
    out = {}
    for trade in trades or []:
        if not bool(getattr(trade, "execution_trade", False)):
            continue
        key = _trade_identity_key(trade)
        if not key or key == "sym:":
            continue
        out[key] = {
            "tp1_hit": bool(getattr(trade, "tp1_hit", False)),
            "tp2_hit": bool(getattr(trade, "tp2_hit", False)),
            "runner_active": bool(getattr(trade, "runner_active", False) or getattr(trade, "protected_runner", False)),
            "is_closed": bool(_is_trade_closed(trade) or getattr(trade, "closed_at", None)),
            "status": str(getattr(trade, "status", "") or "").strip().lower(),
            "active_sl": _trade_active_sl_value(trade),
        }
    return out


def _trade_margin_for_notice(trade) -> float:
    for attr in ("used_margin_usdt", "margin_usdt", "allocated_margin_usdt", "simulation_margin_usdt"):
        value = _safe_float(getattr(trade, attr, 0.0), 0.0)
        if value > 0:
            return value
    return 0.0


def _trade_leverage_for_notice(trade) -> float:
    for attr in ("effective_leverage", "actual_leverage", "exchange_leverage", "leverage"):
        value = _safe_float(getattr(trade, attr, 0.0), 0.0)
        if value > 0:
            return value
    return 0.0


def _trade_impact_for_notice(trade) -> float:
    margin = _trade_margin_for_notice(trade)
    try:
        pct = _report_trade_effective_pnl(trade)
    except Exception:
        pct = _trade_effective_pnl_pct(trade)
    return _money_from_pct(pct, margin=margin)


def _fmt_notice_price(value: object) -> str:
    number = _safe_float(value, 0.0)
    if number <= 0:
        return "-"
    if number >= 100:
        return f"{number:.2f}"
    if number >= 1:
        return f"{number:.4f}"
    return f"{number:.8f}".rstrip("0").rstrip(".")


def _format_trade_lifecycle_notice(trade, event: str, old_sl: float = 0.0, new_sl: float = 0.0, reason: str = "") -> str:
    symbol = str(getattr(trade, "symbol", "-") or "-")
    lev = _trade_leverage_for_notice(trade)
    margin = _trade_margin_for_notice(trade)
    impact = _trade_impact_for_notice(trade)
    try:
        pnl_pct = _report_trade_effective_pnl(trade)
    except Exception:
        pnl_pct = _trade_effective_pnl_pct(trade)
    lev_text = f"{lev:.0f}x" if lev > 0 else "-x"
    header_map = {
        "tp1": "🎯 <b>TP1 HIT</b>",
        "tp2": "🏁 <b>TP2 HIT</b>",
        "runner": "🏃 <b>Runner 20% Active</b>",
        "sl_hit": "🛑 <b>SL HIT</b>",
        "sl_update": "🛡 <b>SL UPDATED</b>",
    }
    lines = [
        header_map.get(event, "📌 <b>Trade Update</b>"),
        f"💎 <b>{symbol}</b>",
        f"⚙️ {lev_text} | Margin {margin:.2f}$ | Impact {impact:+.2f}$",
    ]
    if event == "tp1":
        lines += [
            f"📍 Entry: {_fmt_notice_price(getattr(trade, 'entry', 0.0))}",
            f"🎯 TP1: {_fmt_notice_price(getattr(trade, 'tp1', 0.0))}",
            f"💰 Closed: {_safe_float(getattr(trade, 'tp1_close_pct', 30.0), 30.0):.0f}%",
            f"📊 PnL: {pnl_pct:+.2f}%",
        ]
    elif event == "tp2":
        closed_total = _safe_float(getattr(trade, "closed_portion_pct", 80.0), 80.0)
        lines += [
            f"🎯 TP2: {_fmt_notice_price(getattr(trade, 'tp2', 0.0))}",
            f"💰 Closed Total: {closed_total:.0f}%",
            "🏃 Runner: 20%",
            f"🛡 Protected SL: {_fmt_notice_price(_trade_active_sl_value(trade))}",
        ]
    elif event == "runner":
        lines += [
            "Remaining: 20%",
            f"🛡 Protected SL: {_fmt_notice_price(_trade_active_sl_value(trade))}",
            "📈 Trailing: ON",
        ]
    elif event == "sl_hit":
        lines += [
            f"📍 Entry: {_fmt_notice_price(getattr(trade, 'entry', 0.0))}",
            f"🛡 SL: {_fmt_notice_price(_trade_active_sl_value(trade) or getattr(trade, 'sl', 0.0))}",
            f"📊 Result: {pnl_pct:+.2f}%",
            f"📌 Reason: {reason or str(getattr(trade, 'status', '-') or '-')}",
        ]
    elif event == "sl_update":
        sync = str(getattr(trade, "exchange_sync_state", "") or "-")
        lines += [
            f"Old SL: {_fmt_notice_price(old_sl)}",
            f"New SL: {_fmt_notice_price(new_sl)}",
            f"📌 Reason: {reason or 'strategy protection'}",
            f"✅ OKX Sync: {sync}",
        ]
    return "\n".join(lines)


def _sl_update_reason(trade, protection_level: int = 0) -> str:
    status = str(getattr(trade, "status", "") or "").lower()
    if bool(getattr(trade, "tp2_hit", False)):
        return "TP2 runner protection"
    if int(protection_level or 0) >= 2 or bool(getattr(trade, "protected_on_block", False)):
        return f"Market protection level {int(protection_level or getattr(trade, 'protection_level', 0) or 0)}"
    if "runner" in status or bool(getattr(trade, "trailing_active", False)):
        return "Runner trailing stop"
    return "SL protection sync"


def _collect_execution_lifecycle_notifications(before: dict, trades: list, protection_level: int = 0) -> list[dict]:
    notifications = []
    closed_sl_statuses = {"closed_loss", "breakeven_after_tp1", "trailing_hit", "protected_entry_exit", "stopped"}
    for trade in trades or []:
        if not bool(getattr(trade, "execution_trade", False)):
            continue
        key = _trade_identity_key(trade)
        prev = before.get(key)
        if not prev:
            continue
        status = str(getattr(trade, "status", "") or "").strip().lower()
        now_closed = bool(_is_trade_closed(trade) or getattr(trade, "closed_at", None))
        events = []
        if bool(getattr(trade, "tp1_hit", False)) and not prev.get("tp1_hit") and not bool(getattr(trade, "tp1_telegram_sent", False)):
            events.append(("tp1", _format_trade_lifecycle_notice(trade, "tp1"), "tp1_telegram_sent"))
        if bool(getattr(trade, "tp2_hit", False)) and not prev.get("tp2_hit") and not bool(getattr(trade, "tp2_telegram_sent", False)):
            events.append(("tp2", _format_trade_lifecycle_notice(trade, "tp2"), "tp2_telegram_sent"))
        runner_now = bool(getattr(trade, "runner_active", False) or getattr(trade, "protected_runner", False))
        if runner_now and not prev.get("runner_active") and not bool(getattr(trade, "runner_telegram_sent", False)):
            events.append(("runner", _format_trade_lifecycle_notice(trade, "runner"), "runner_telegram_sent"))
        if now_closed and not prev.get("is_closed") and status in closed_sl_statuses and not bool(getattr(trade, "sl_hit_telegram_sent", False)):
            reason = str(getattr(trade, "exchange_sync_state", "") or status)
            events.append(("sl_hit", _format_trade_lifecycle_notice(trade, "sl_hit", reason=reason), "sl_hit_telegram_sent"))

        old_sl = _safe_float(prev.get("active_sl"), 0.0)
        new_sl = _trade_active_sl_value(trade)
        last_notified_sl = _safe_float(getattr(trade, "last_notified_sl", 0.0), 0.0)
        moved_up = new_sl > 0 and (old_sl <= 0 or new_sl > old_sl + max(abs(old_sl) * 0.0005, 1e-12))
        not_already = last_notified_sl <= 0 or abs(new_sl - last_notified_sl) > max(abs(last_notified_sl) * 0.0005, 1e-12)
        if moved_up and not_already and not now_closed:
            reason = _sl_update_reason(trade, protection_level=protection_level)
            events.append(("sl_update", _format_trade_lifecycle_notice(trade, "sl_update", old_sl=old_sl, new_sl=new_sl, reason=reason), "last_notified_sl"))

        for event, message, flag in events:
            if flag == "last_notified_sl":
                _safe_set_trade_attr(trade, "last_notified_sl", new_sl)
                _safe_set_trade_attr(trade, "last_sl_update_telegram_at", datetime.now(timezone.utc))
            else:
                _safe_set_trade_attr(trade, flag, True)
                _safe_set_trade_attr(trade, flag + "_at", datetime.now(timezone.utc))
            notifications.append({"symbol": str(getattr(trade, "symbol", "") or ""), "event": event, "message": message})
    return notifications


def _send_lifecycle_notifications(sender: TelegramSender, result: dict, trade_store: RedisTradeStore | None = None) -> None:
    for item in list((result or {}).get("lifecycle_notifications", []) or []):
        message = str((item or {}).get("message") or "").strip()
        if not message:
            continue
        send_result = _send_text(sender, message)
        if not (isinstance(send_result, dict) and send_result.get("ok")):
            time.sleep(1.0)
            send_result = _send_text(sender, message)
            if not (isinstance(send_result, dict) and send_result.get("ok")):
                print(f"⚠️ LIFECYCLE_TELEGRAM_SEND_FAILED | {(item or {}).get('symbol') or '-'} | {(item or {}).get('event') or '-'}", flush=True)
        _telegram_send_pause(TELEGRAM_EXECUTION_SEND_GAP_SECONDS)

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
    lifecycle_notifications: list[dict] = []

    # ✅ FIX: cache الـ price_map — fallback لو fetch فشل في scan تاني
    global _CACHED_PRICE_MAP, _CACHED_PRICE_MAP_TS
    import time as _ptime
    if initial_price_map:
        _CACHED_PRICE_MAP = dict(initial_price_map)
        _CACHED_PRICE_MAP_TS = _ptime.time()
    elif _CACHED_PRICE_MAP and (_ptime.time() - _CACHED_PRICE_MAP_TS) < _CACHED_PRICE_MAP_TTL_SECONDS:
        print("⚠️ price_map empty — using cached fallback", flush=True)
        initial_price_map = dict(_CACHED_PRICE_MAP)
    exchange_reconcile_enabled = bool(
        okx_client is not None
        and getattr(okx_client, "configured", False)
        and not bool(getattr(settings, "offline_test_mode", False))
    )
    exchange_stop_sync_enabled = bool(
        exchange_reconcile_enabled
        and bool(_runtime_mode_snapshot(settings).get("effective_orders_enabled", False))
        and state.mode == MODE_BLOCK_LONGS
        and int(initial_protection.get("level", 0) or 0) >= 2
    )
    if persisted_trades:
        persisted_trades, exchange_reconcile_stats = _reconcile_execution_trades_with_okx(
            persisted_trades,
            okx_client,
            settings,
        )
        if exchange_reconcile_stats.get("changed") and trade_store:
            trade_store.save_trades(persisted_trades)
    else:
        exchange_reconcile_stats = {"enabled": False, "changed": False, "removed": 0, "reason": "no_trades"}

    if not simulation_mode_active:
        persisted_trades, okx_recovery_stats = _recover_missing_execution_trades_from_okx_positions(
            persisted_trades,
            okx_client,
            settings,
        )
        if okx_recovery_stats.get("changed") and trade_store:
            trade_store.save_trades(persisted_trades)
        if isinstance(exchange_reconcile_stats, dict):
            exchange_reconcile_stats["okx_position_recovery"] = okx_recovery_stats
    else:
        okx_recovery_stats = {"enabled": False, "changed": False, "imported": 0, "reason": "simulation_mode"}

    if persisted_trades:
        _before_lifecycle = _execution_lifecycle_snapshot(persisted_trades)
        persisted_trades = update_open_trades(
            persisted_trades,
            initial_price_map,
            protection_level=initial_protection.get("level", 0),
            okx_client=okx_client if exchange_reconcile_enabled else None,
            sync_exchange=exchange_reconcile_enabled,
            sync_exchange_stop=exchange_stop_sync_enabled,
        )
        lifecycle_notifications.extend(
            _collect_execution_lifecycle_notifications(
                _before_lifecycle,
                persisted_trades,
                protection_level=initial_protection.get("level", 0),
            )
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

    if simulation_mode_active:
        simulation_daily_balance_snapshot = _ensure_simulation_daily_log(
            simulation_trades,
            trade_store=trade_store,
            settings=settings,
        )
        portfolio_state_inputs = _resolve_simulation_portfolio_state_inputs(
            simulation_trades,
            settings,
            trade_store=trade_store,
            daily_balance=simulation_daily_balance_snapshot,
        )
    else:
        simulation_daily_balance_snapshot = None
        portfolio_state_inputs = _resolve_portfolio_state_inputs(okx_client, settings, trade_store=trade_store)
    protection_scope = _protection_scope(settings)
    protection_state = _load_protection_state(trade_store, protection_scope)
    portfolio_state_inputs = _apply_daily_dd_manual_baseline(portfolio_state_inputs, protection_state)
    # Daily DD must use the active runtime bucket:
    # - Simulation mode evaluates the virtual simulation wallet/trades.
    # - Trading/execution evaluates live execution trades.
    # This keeps DD protection independent between simulation and execution.
    dd_base_trades = simulation_trades if simulation_mode_active else persisted_trades
    portfolio_state = build_portfolio_state_from_trades(dd_base_trades, **_portfolio_state_kwargs(portfolio_state_inputs))
    drawdown_status = evaluate_drawdown(portfolio_state)
    loss_streak_base_trades = simulation_trades if simulation_mode_active else persisted_trades
    loss_streak_reset_at = _parse_protection_dt(protection_state.get("loss_streak_reset_at"))
    loss_streak_guard = _build_loss_streak_guard(loss_streak_base_trades, reset_at=loss_streak_reset_at)
    protection_state = _maybe_finalize_loss_streak_cooldown(loss_streak_guard, trade_store, protection_scope, protection_state)
    loss_streak_reset_at = _parse_protection_dt(protection_state.get("loss_streak_reset_at"))
    if loss_streak_guard.get("reset_recommended_at"):
        loss_streak_guard = _build_loss_streak_guard(loss_streak_base_trades, reset_at=loss_streak_reset_at)

    signal_items = []
    current_execution_results = []
    technical_snapshot_records = []
    local_gate_trades = []
    gate_base_trades = simulation_trades if simulation_mode_active else persisted_trades
    slot_counts = _execution_slot_counts(gate_base_trades)

    # ✅ SAFETY GUARD: لو Redis OFF والـ OKX Orders ON → لا تفتح صفقات
    # بدون Redis مفيش persistence → البوت هيفتح 3 صفقات كل scan بدون ما يعرف
    _redis_enabled = bool(trade_store and getattr(trade_store, "enabled", False))
    _okx_orders_active = bool(_runtime_mode_snapshot(settings).get("effective_orders_enabled", False))
    if not _redis_enabled and _okx_orders_active:
        print(
            "🚨 SAFETY GUARD: Redis is OFF but OKX orders are ON — "
            "blocking all execution to prevent untracked positions.",
            flush=True,
        )
        # نحوّل الـ mode لـ scan فقط مؤقتاً
        _okx_orders_active = False
        # ✅ FIX: أوقف الـ OKX orders على مستوى الـ runtime فعلياً
        _set_runtime_okx_orders(settings, False)

    scan_pairs = ranked_pairs
    filtered_pairs = [p for p in scan_pairs if prefilter_pair_before_candles(p, state.mode)]
    btc_bounce_pct = float(snapshot.btc_change_15m or 0.0)

    # ✅ FIX 2: snapshot الـ mode قبل اللوب — يمنع تأثير register_recovery_trade
    # على باقي الـ pairs في نفس الـ scan
    scan_mode = state.mode

    # Low Balance Mode — لو الرصيد أقل من الحد، نغير الـ slots والـ margin.
    # Block Exception و Recovery بيفضلوا على حدودهم الأصلية.
    if simulation_mode_active:
        _sim_wallet_balance = _safe_float(
            _build_simulation_wallet_snapshot(simulation_trades).get("equity"),
            SIMULATION_START_BALANCE_USDT,
        )
        _effective_reference_balance = _sim_wallet_balance
    else:
        _effective_reference_balance = _safe_float(
            portfolio_state_inputs.get("reference_portfolio"), 0.0
        )

    _low_balance_mode = bool(0 < _effective_reference_balance < LOW_BALANCE_THRESHOLD_USDT)
    effective_max_positions = LOW_BALANCE_MAX_SLOTS if _low_balance_mode else settings.max_execution_positions

    # ✅ OKX GATE GUARD: لو execution mode والـ Redis فاضي (أول scan بعد التحويل)
    # اسأل OKX مباشرة عن الـ open positions عشان الـ slot_counts يكون صح
    # مهم: بنستخدم effective_max_positions المحسوب صح (low balance أو normal)
    if (
        not simulation_mode_active
        and slot_counts.get("general", 0) == 0
        and okx_client is not None
    ):
        try:
            _okx_pos_response = okx_client.get_positions(inst_type="SWAP") or {}
            _okx_positions_debug_log("gate_guard", _okx_pos_response, max_rows=6)
            _okx_live_symbols = _extract_live_okx_position_inst_ids(_okx_pos_response)
            _okx_open_count = len(_okx_live_symbols)
            if _okx_open_count > 0:
                # OKX is the hard source of truth for live slot protection.
                # Keep tracked Redis count, but never let slots be lower than live OKX positions.
                previous_general = int(slot_counts.get("general", 0) or 0)
                slot_counts["general"] = min(max(previous_general, _okx_open_count), effective_max_positions)
                if exchange_reconcile_stats is not None:
                    exchange_reconcile_stats.update({
                        "okx_gate_guard_live_positions": _okx_open_count,
                        "okx_gate_guard_symbols": sorted(_okx_live_symbols)[:20],
                        "tracking_mismatch": _okx_open_count > previous_general,
                    })
                print(
                    f"🛡 OKX_GATE_GUARD | "
                    f"OKX positions={_okx_open_count} | tracked_general={previous_general} | "
                    f"max_allowed={'LOW='+str(effective_max_positions) if _low_balance_mode else 'NORMAL='+str(effective_max_positions)} | "
                    f"slot_counts[general] set to {slot_counts['general']} | "
                    f"symbols={','.join(sorted(_okx_live_symbols))}",
                    flush=True,
                )
        except Exception as _exc:
            print(f"⚠️ OKX_GATE_GUARD | failed to fetch positions: {_exc}", flush=True)

    # Block Exception و Recovery slots — ديناميكية حسب الرصيد
    # رصيد ≥ $109 → كل مود عنده 3 slots منفصلة
    # رصيد < $109 → كل مود عنده slot واحد فقط
    NORMAL_BLOCK_SLOTS = 3
    NORMAL_RECOVERY_SLOTS = 3
    LOW_BALANCE_BLOCK_SLOTS = 1
    LOW_BALANCE_RECOVERY_SLOTS = 1

    effective_max_block_positions = LOW_BALANCE_BLOCK_SLOTS if _low_balance_mode else NORMAL_BLOCK_SLOTS
    effective_max_recovery_positions = LOW_BALANCE_RECOVERY_SLOTS if _low_balance_mode else NORMAL_RECOVERY_SLOTS

    # إعادة حساب recovery_remaining بالحد الجديد
    recovery_remaining = max(0, effective_max_recovery_positions - slot_counts.get("recovery", 0))

    if _low_balance_mode:
        print(
            f"⚠️ LOW_BALANCE_MODE | balance={_effective_reference_balance:.2f} < {LOW_BALANCE_THRESHOLD_USDT} | "
            f"general={effective_max_positions} | block={effective_max_block_positions} | "
            f"recovery={effective_max_recovery_positions} | alloc={LOW_BALANCE_ALLOCATION_PCT}%",
            flush=True,
        )
    else:
        print(
            f"✅ NORMAL_MODE | balance={_effective_reference_balance:.2f} | "
            f"general={effective_max_positions} | block={effective_max_block_positions} | "
            f"recovery={effective_max_recovery_positions} | alloc=24%",
            flush=True,
        )

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

        # ✅ DIAGNOSTIC: تتبع raw_candles داخل الـ signal بعد البناء
        _signal_candles_count = len((signal.meta or {}).get("raw_candles") or [])
        print(
            f"SIGNAL_CANDLES | {signal.symbol} | "
            f"setup={signal.setup_type} | "
            f"mode={scan_mode} | "
            f"raw_candles_in_meta={_signal_candles_count}",
            flush=True,
        )

        # First let the normal execution decision run, including BLOCK/RECOVERY
        # exception logic. Then, if a higher hard protection is active
        # (Daily DD or 5SL/No-TP1), override the final execution result to
        # protection_pause with no exceptions. This preserves decision visibility
        # while still preventing any execution.
        pre_protection_exec_result = process_trade_candidate(
            signal,
            open_trades=[*gate_base_trades, *local_gate_trades],
            current_open_positions=slot_counts.get("general", 0),
            max_open_positions=effective_max_positions,
            min_execution_score=settings.min_execution_score,
            recovery_slots_remaining=recovery_remaining if state.mode == MODE_RECOVERY_LONG else None,
            block_open_positions=slot_counts.get("block_exception", 0),
            max_block_positions=effective_max_block_positions,
            recovery_open_positions=slot_counts.get("recovery", 0),
            max_recovery_positions=effective_max_recovery_positions,
            drawdown_status=drawdown_status,
            risk_mode=state.mode,
        )
        pre_protection_exec_result["decision_engine"] = "process_trade_candidate"
        pre_protection_exec_result["runtime_mode"] = "simulation" if simulation_mode_active else _get_signal_delivery_mode(settings)
        pre_protection_exec_result["risk_mode"] = scan_mode  # ✅ للـ dedup في Recovery mode

        hard_protection_rejection = _hard_execution_protection_rejection(drawdown_status, loss_streak_guard)
        if hard_protection_rejection:
            exec_result = dict(hard_protection_rejection)
            exec_result["pre_protection_status"] = pre_protection_exec_result.get("status")
            exec_result["pre_protection_reason"] = pre_protection_exec_result.get("reason")
            exec_result["pre_protection_path"] = pre_protection_exec_result.get("path")
            exec_result["decision_engine"] = "hard_protection_after_candidate"
            exec_result["runtime_mode"] = "simulation" if simulation_mode_active else _get_signal_delivery_mode(settings)
            exec_result["risk_mode"] = scan_mode
        else:
            exec_result = pre_protection_exec_result

        print(
            f"DECISION_ENGINE | {signal.symbol} | "
            f"runtime={exec_result.get('runtime_mode')} | "
            f"engine={exec_result.get('decision_engine')} | "
            f"status={exec_result.get('status')} | reason={exec_result.get('reason')} | "
            f"pre={exec_result.get('pre_protection_status', '-')}",
            flush=True,
        )

        exec_status = str(exec_result.get("status") or "").strip().lower()
        consumes_live_slot = exec_status in {"accepted_preview", "pending_pullback_preview"}

        candidate_trade = register_trade(signal, exec_result)
        setattr(candidate_trade, "telegram_announced", False)
        setattr(candidate_trade, "announced_to_telegram", False)

        # ✅ FIX: الـ slot بيتحجز في نفس الـ scan في simulation وexecution
        # في simulation: بيفتح trade فعلي جوه الـ scan
        # في execution: بس بيحجز الـ slot عشان باقي الـ pairs في نفس الـ scan
        #               يشوفوا الـ slot محجوز ومش يتجاوزوا الـ max_positions
        reserve_same_scan_slot = consumes_live_slot

        eligible_for_activation = consumes_live_slot and not _has_active_same_symbol(
            [*gate_base_trades, *local_gate_trades],
            candidate_trade,
        )
        activation_block_reason = ""
        if consumes_live_slot and not eligible_for_activation:
            activation_block_reason = "same_symbol_or_slot_gate_blocked"

        if eligible_for_activation and reserve_same_scan_slot:
            path = str(exec_result.get("path") or "general")
            if path == "block_exception":
                slot_counts["block_exception"] = slot_counts.get("block_exception", 0) + 1
                # ✅ FIX: تأكد إن الـ slot مش بيتجاوز الحد بعد الإضافة
                if slot_counts["block_exception"] > effective_max_block_positions:
                    slot_counts["block_exception"] -= 1
                    eligible_for_activation = False
                    activation_block_reason = "block_slots_reached_same_scan"
            elif path == "recovery":
                slot_counts["recovery"] = slot_counts.get("recovery", 0) + 1
                # ✅ FIX: تأكد إن الـ slot مش بيتجاوز الحد بعد الإضافة
                if slot_counts["recovery"] > effective_max_recovery_positions:
                    slot_counts["recovery"] -= 1
                    eligible_for_activation = False
                    activation_block_reason = "recovery_slots_reached_same_scan"
                else:
                    recovery_remaining = max(0, effective_max_recovery_positions - slot_counts.get("recovery", 0))
                    if state.mode == MODE_RECOVERY_LONG:
                        state = register_recovery_trade(state)
            else:
                slot_counts["general"] = slot_counts.get("general", 0) + 1
                # ✅ FIX: تأكد إن الـ slot مش بيتجاوز الحد بعد الإضافة
                if slot_counts["general"] > effective_max_positions:
                    slot_counts["general"] -= 1
                    eligible_for_activation = False
                    activation_block_reason = "general_slots_reached_same_scan"

            # Reserve this trade for same-scan gating only in simulation mode.
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
            "activation_block_reason": activation_block_reason,
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
    # ✅ FIX: fallback لو price_map فاضي
    if price_map:
        _CACHED_PRICE_MAP = dict(price_map)
        _CACHED_PRICE_MAP_TS = _ptime.time()
    elif _CACHED_PRICE_MAP and (_ptime.time() - _CACHED_PRICE_MAP_TS) < _CACHED_PRICE_MAP_TTL_SECONDS:
        print("⚠️ final price_map empty — using cached fallback", flush=True)
        price_map = dict(_CACHED_PRICE_MAP)
    protection = block_protection_status(state)

    # ✅ FIX: احسب exchange_stop_sync للـ final update بعد ما الصفقات اتفتحت في اللوب
    _final_exchange_stop_sync = bool(
        exchange_reconcile_enabled
        and bool(_runtime_mode_snapshot(settings).get("effective_orders_enabled", False))
        and (
            (state.mode == MODE_BLOCK_LONGS and int(protection.get("level", 0) or 0) >= 2)
            or any(getattr(t, "tp2_hit", False) for t in persisted_trades)
        )
    )

    _before_lifecycle = _execution_lifecycle_snapshot(persisted_trades)
    trades = update_open_trades(
        list(persisted_trades),
        price_map,
        protection_level=protection.get("level", 0),
        okx_client=okx_client if exchange_reconcile_enabled else None,
        sync_exchange=exchange_reconcile_enabled,
        sync_exchange_stop=_final_exchange_stop_sync,
    )
    lifecycle_notifications.extend(
        _collect_execution_lifecycle_notifications(
            _before_lifecycle,
            trades,
            protection_level=protection.get("level", 0),
        )
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
        # ✅ FIX: Redis retry بـ exponential backoff
        _redis_saved = False
        for _attempt in range(3):
            try:
                trade_store.save_trades(trades)
                _redis_saved = True
                break
            except Exception as _exc:
                _wait = 0.5 * (2 ** _attempt)
                print(f"⚠️ Redis save_trades attempt {_attempt+1}/3 failed: {_exc} — retry in {_wait}s", flush=True)
                time.sleep(_wait)
        if not _redis_saved:
            print("🚨 Redis save_trades failed after 3 attempts — trades may be lost on restart", flush=True)

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

    mode_context = _build_mode_context(state, snapshot, protection)
    protection_scope = _protection_scope(settings)
    protection_state = _load_protection_state(trade_store, protection_scope)
    if simulation_mode_active:
        simulation_daily_balance_snapshot = _ensure_simulation_daily_log(
            simulation_trades,
            trade_store=trade_store,
            settings=settings,
        )
        portfolio_state_inputs = _resolve_simulation_portfolio_state_inputs(
            simulation_trades,
            settings,
            trade_store=trade_store,
            daily_balance=simulation_daily_balance_snapshot,
        )
    portfolio_state_inputs = _apply_daily_dd_manual_baseline(portfolio_state_inputs, protection_state)
    # Final Daily DD snapshot must also follow the active runtime bucket.
    # Without this, simulation DD would be calculated from execution trades.
    dd_base_trades = simulation_trades if simulation_mode_active else trades
    portfolio_state = build_portfolio_state_from_trades(dd_base_trades, **_portfolio_state_kwargs(portfolio_state_inputs))
    drawdown_status = evaluate_drawdown(portfolio_state)
    drawdown_report = build_drawdown_report(portfolio_state)
    loss_streak_base_trades = simulation_trades if simulation_mode_active else trades
    loss_streak_reset_at = _parse_protection_dt(protection_state.get("loss_streak_reset_at"))
    loss_streak_guard = _build_loss_streak_guard(loss_streak_base_trades, reset_at=loss_streak_reset_at)
    protection_state = _maybe_finalize_loss_streak_cooldown(loss_streak_guard, trade_store, protection_scope, protection_state)
    loss_streak_reset_at = _parse_protection_dt(protection_state.get("loss_streak_reset_at"))
    if loss_streak_guard.get("reset_recommended_at"):
        loss_streak_guard = _build_loss_streak_guard(loss_streak_base_trades, reset_at=loss_streak_reset_at)

    display_result_for_protection = {
        "mode": state.mode,
        "mode_context": mode_context,
        "drawdown_status": drawdown_status,
        "loss_streak_guard": loss_streak_guard,
        "protection_state": protection_state,
        "portfolio_state_inputs": portfolio_state_inputs,
        "trades": trades,
        "simulation_trades": simulation_trades,
        "simulation_wallet": _build_simulation_wallet_snapshot(simulation_trades),
    }
    mode_message = _build_mode_message(state, snapshot, protection, settings=settings, result=display_result_for_protection)
    mode_transition_message = _build_mode_message(
        state,
        snapshot,
        protection,
        variant="transition",
        old_mode=initial_mode.mode,
        settings=settings,
        result=display_result_for_protection,
    ) if state.mode != initial_mode.mode else None
    protection_summary = _risk_protection_summary(display_result_for_protection)

    execution_report_kwargs = _execution_report_balance_kwargs(portfolio_state_inputs)
    reports = build_report_bundle(trades, execution_results_for_reports, signal_items, **execution_report_kwargs)
    command_outputs = build_command_outputs(trades, execution_results_for_reports, signal_items, **execution_report_kwargs)

    return {
        "state": state,
        "mode": state.mode,
        "mode_message": mode_message,
        "mode_transition_message": mode_transition_message,
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
        "protection_state": protection_state,
        "active_protections": protection_summary.get("active_protections", []),
        "protection_status": protection_summary,
        "risk_protection_summary": protection_summary,
        "portfolio_state_inputs": portfolio_state_inputs,
        "help": build_master_help(
            mode=state.mode,
            execution_enabled=settings.execution_enabled,
            risk_enabled=True,
            okx_orders=_runtime_mode_snapshot(settings).get("orders_enabled", False),
            runtime_snapshot=_runtime_mode_snapshot(settings),
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
        "simulation_daily_balance": simulation_daily_balance_snapshot if simulation_daily_balance_snapshot is not None else _ensure_simulation_daily_log(simulation_trades, trade_store=trade_store, settings=settings),
        "simulation_daily_log": _load_simulation_daily_log(trade_store),
        "trades": trades,
        "command_outputs": command_outputs,
        "exchange_reconcile_stats": exchange_reconcile_stats,
        "lifecycle_notifications": lifecycle_notifications,
        "simulation_command_outputs": {},
        **reports,
    }


def _run_ai_export(result: dict, settings: Settings | None = None) -> None:
    """تصدير AI snapshot في thread منفصل — لا يأثر على الـ scan loop."""
    if not _AI_EXPORT_ENABLED:
        return

    def _do_export():
        try:
            # Simulation export
            sim_stats = export_ai_snapshot(result, source="simulation")
            if not sim_stats.get("ok"):
                print(f"⚠️ AI export simulation failed: {sim_stats.get('error')}", flush=True)
            else:
                print(
                    f"📊 AI export simulation | trades={sim_stats.get('trades_written')} | "
                    f"rejections={sim_stats.get('rejections_written')} | "
                    f"snapshot={'✅' if sim_stats.get('daily_snapshot_written') else '❌'}",
                    flush=True,
                )

            # Execution export
            exec_stats = export_ai_snapshot(result, source="execution")
            if not exec_stats.get("ok"):
                print(f"⚠️ AI export execution failed: {exec_stats.get('error')}", flush=True)
            else:
                print(
                    f"📊 AI export execution | trades={exec_stats.get('trades_written')} | "
                    f"rejections={exec_stats.get('rejections_written')} | "
                    f"snapshot={'✅' if exec_stats.get('daily_snapshot_written') else '❌'}",
                    flush=True,
                )
        except Exception as exc:
            print(f"⚠️ AI export thread error: {exc}", flush=True)

    import threading
    t = threading.Thread(target=_do_export, daemon=True, name="ai_export")
    t.start()


def _refresh_runtime_result_outputs(result: dict, trade_store: RedisTradeStore | None = None, settings: Settings | None = None) -> None:
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
    runtime_settings = settings or get_settings()
    protection_scope = _protection_scope(runtime_settings)
    protection_state = _load_protection_state(trade_store, protection_scope)
    portfolio_state_inputs = _apply_daily_dd_manual_baseline(portfolio_state_inputs, protection_state)
    result["portfolio_state_inputs"] = portfolio_state_inputs
    result["protection_state"] = protection_state
    dd_base_trades = _loss_streak_base_trades_for_runtime(
        runtime_settings,
        result,
        execution_trades=trades,
        simulation_trades=list(result.get("simulation_trades", []) or []),
    )
    portfolio_state = build_portfolio_state_from_trades(dd_base_trades, **_portfolio_state_kwargs(portfolio_state_inputs))
    result["portfolio_state"] = portfolio_state
    result["drawdown_status"] = evaluate_drawdown(portfolio_state)
    result["drawdown_report"] = build_drawdown_report(portfolio_state)
    result["loss_streak_guard"] = _build_loss_streak_guard(
        dd_base_trades,
        reset_at=_parse_protection_dt(protection_state.get("loss_streak_reset_at")),
    )
    protection_summary = _risk_protection_summary(result)
    result["active_protections"] = protection_summary.get("active_protections", [])
    result["protection_status"] = protection_summary
    result["risk_protection_summary"] = protection_summary

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
    register_as_open_trade = bool(item.get("register_as_open_trade", False))

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
        return True

    # accepted_preview must become an open tracked trade ONLY after real OKX success.
    # Preview-only / orders-off / rejected-by-exchange signals are announced, but never
    # inserted into Open Trades or execution reports.
    if not register_as_open_trade:
        item["announcement_status"] = "preview_only_sent"
        return True

    if not updated_existing:
        setattr(candidate_trade, "execution_trade", True)
        setattr(candidate_trade, "tracking_bucket", "execution")
        trades.append(candidate_trade)
        result["trades"] = trades

    _refresh_runtime_result_outputs(result, trade_store=trade_store)
    return True





def _register_exchange_trade_immediately(
    result: dict,
    item: dict,
    managed_order_result: dict | None,
    trade_store: RedisTradeStore | None = None,
) -> bool:
    """Persist a live OKX trade immediately after exchange success.

    This decouples execution tracking from Telegram delivery. If OKX fills but
    Telegram fails, the trade still exists in Redis/reports and blocks duplicate
    same-symbol entries in later scans.
    """
    if not isinstance(result, dict) or not isinstance(item, dict):
        return False
    exec_result = item.get("execution") or {}
    if str(exec_result.get("status") or "").strip().lower() != "accepted_preview":
        return False
    # If OKX already accepted the order, tracking must be persisted even if a
    # stale UI/slot flag says eligible_for_activation=False. Otherwise the bot
    # can open a real position and fail to record it. Pre-order eligibility is
    # enforced in _dispatch_signals before sending any OKX order.
    if not bool((managed_order_result or {}).get("ok")):
        return False

    candidate_trade = item.get("candidate_trade")
    if candidate_trade is None:
        return False

    _attach_exchange_state_to_trade(candidate_trade, managed_order_result)
    now = datetime.now(timezone.utc)
    _safe_set_trade_attr(candidate_trade, "execution_trade", True)
    _safe_set_trade_attr(candidate_trade, "tracking_bucket", "execution")
    _safe_set_trade_attr(candidate_trade, "trade_source", "execution")
    _safe_set_trade_attr(candidate_trade, "exchange_sync_state", "okx_order_submitted")
    _safe_set_trade_attr(candidate_trade, "exchange_order_ok", True)
    _safe_set_trade_attr(candidate_trade, "opened_at", getattr(candidate_trade, "opened_at", None) or now)
    _safe_set_trade_attr(candidate_trade, "updated_at", now)
    _safe_set_trade_attr(candidate_trade, "closed_at", None)
    _safe_set_trade_attr(candidate_trade, "status", str(getattr(candidate_trade, "status", "") or "open"))
    _safe_set_trade_attr(candidate_trade, "slot_exempt", False)
    _safe_set_trade_attr(candidate_trade, "same_symbol_block_exempt", False)
    _safe_set_trade_attr(candidate_trade, "blocks_same_symbol_reentry", True)

    used_margin = _safe_float((managed_order_result or {}).get("used_margin_usdt"), 0.0)
    if used_margin > 0:
        _safe_set_trade_attr(candidate_trade, "used_margin_usdt", used_margin)
        _safe_set_trade_attr(candidate_trade, "margin_usdt", used_margin)

    trades = list(result.get("trades", []) or [])
    trade_id = str(getattr(candidate_trade, "trade_id", "") or "")
    symbol = str(getattr(candidate_trade, "symbol", "") or "").upper()
    updated_existing = False

    for idx, trade in enumerate(trades):
        existing_id = str(getattr(trade, "trade_id", "") or "")
        existing_symbol = str(getattr(trade, "symbol", "") or "").upper()
        if (trade_id and existing_id == trade_id) or (symbol and existing_symbol == symbol and _blocks_same_symbol_reentry(trade)):
            trades[idx] = candidate_trade
            updated_existing = True
            break

    if not updated_existing:
        trades.append(candidate_trade)

    result["trades"] = trades
    item["register_as_open_trade"] = True
    item["exchange_order_ok"] = True
    item["exchange_order_result"] = managed_order_result
    item["announcement_status"] = "registered_after_okx_success"

    try:
        _refresh_runtime_result_outputs(result, trade_store=trade_store)
    except Exception as exc:
        print(f"🚨 Immediate trade registration refresh failed: {exc}", flush=True)
        if trade_store:
            try:
                trade_store.save_trades(trades)
            except Exception as save_exc:
                print(f"🚨 Immediate trade registration Redis save failed: {save_exc}", flush=True)
                return False
    print(
        f"✅ TRACK_TRADE_IMMEDIATE | {symbol or '-'} | "
        f"id={trade_id or '-'} | margin={used_margin:.4f}",
        flush=True,
    )
    return True


def _has_tracked_tp2_release_for_symbol(trades: list, symbol: str) -> bool:
    """Return True when the tracked strategy state says TP2 already released this symbol.

    This intentionally allows a new entry while an OKX runner is still open,
    because the project design treats TP2 as the re-entry unlock point.
    """
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return False
    for trade in trades or []:
        if str(getattr(trade, "symbol", "") or "").strip().upper() != symbol:
            continue
        if bool(getattr(trade, "tp2_hit", False)):
            return True
    return False


def _has_tracked_pre_tp2_block_for_symbol(trades: list, symbol: str) -> bool:
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return False
    for trade in trades or []:
        if str(getattr(trade, "symbol", "") or "").strip().upper() != symbol:
            continue
        if _blocks_same_symbol_reentry(trade):
            return True
    return False


def _okx_symbol_blocks_reentry(
    okx_client: OKXTradeClient | None,
    symbol: str,
    tracked_trades: list | None = None,
) -> tuple[bool, str]:
    """Live OKX same-symbol guard with TP2 re-entry unlock.

    Blocks when OKX has a live position and the tracked trade has not reached TP2.
    Allows when the only known tracked state for that symbol has TP2 hit, even
    if the residual runner is still live on OKX.
    If OKX has a live position but tracking is missing, block safely.
    """
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return False, "missing_symbol"

    symbol = _normalize_okx_inst_id(symbol)
    ok, live_symbols, live_reason = _fetch_live_okx_position_inst_ids_strict(okx_client)
    if not ok:
        return True, f"okx_positions_unavailable_fail_closed:{live_reason}"
    if symbol not in live_symbols:
        return False, "not_live_on_okx"

    trades = list(tracked_trades or [])
    if _has_tracked_pre_tp2_block_for_symbol(trades, symbol):
        return True, "live_on_okx_pre_tp2_tracked"
    if _has_tracked_tp2_release_for_symbol(trades, symbol):
        return False, "live_runner_after_tp2_allowed"
    return True, "live_on_okx_tracking_missing_or_not_tp2"


def _okx_symbol_already_live(okx_client: OKXTradeClient | None, symbol: str) -> bool:
    # Backward-compatible wrapper for any older call sites.
    blocked, _reason = _okx_symbol_blocks_reentry(okx_client, symbol, tracked_trades=None)
    return blocked

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
    exec_result = exec_result or {}
    status = str(exec_result.get("status") or "").strip().lower()
    runtime_mode = str(exec_result.get("runtime_mode") or "").strip().lower()
    risk_mode = str(exec_result.get("risk_mode") or "").strip().lower()

    # Trading visibility rule:
    # Keep execution-mode Telegram behavior close to Simulation.
    # The only suppression in Trading is a short 5-minute same-symbol cooldown.
    if runtime_mode == "trading":
        return SYMBOL_TRADING_SAME_SYMBOL_DEDUP_TTL_SECONDS

    if status == "accepted_preview":
        return SYMBOL_EXECUTION_DEDUP_TTL_SECONDS
    if status == "pending_pullback_preview":
        return SYMBOL_PULLBACK_DEDUP_TTL_SECONDS

    # ✅ FIX: Recovery mode signals → TTL أطول لمنع الـ spam
    if risk_mode == "recovery_long" or runtime_mode == "recovery":
        return SYMBOL_PULLBACK_DEDUP_TTL_SECONDS  # 60 دقيقة بدل 45
    return SYMBOL_OBSERVATION_DEDUP_TTL_SECONDS


def _build_signal_fingerprint(signal, exec_result: dict) -> str:
    """Build dedup key without mixing Simulation / Trading / Scan.

    Trading-specific behavior:
    - Trading uses runtime + symbol only.
    - This means messages behave like Simulation, except the same coin is not
      repeated more than once every 5 minutes.
    - Same-symbol open guards still decide whether a trade can actually open;
      dedup is only Telegram/noise protection.
    """
    exec_result = exec_result or {}
    runtime_mode = str(exec_result.get("runtime_mode") or "unknown").strip().lower()
    risk_mode = str(exec_result.get("risk_mode") or "").strip().lower()

    if runtime_mode not in {"scan", "trading", "simulation"}:
        runtime_mode = "unknown"

    symbol = str(getattr(signal, "symbol", "")).upper()

    # In Trading, only protect the same coin for 5 minutes, regardless of
    # accepted/rejected/reason. This mirrors Simulation visibility while
    # preventing same-symbol Telegram spam every scan.
    if runtime_mode == "trading":
        return "|".join([runtime_mode, symbol, "LONG", "same_symbol_5m"])

    mode_suffix = ":recovery" if risk_mode == "recovery_long" else ""
    status_bucket = _signal_status_bucket(
        exec_result.get("status") if isinstance(exec_result, dict) else None
    )

    return "|".join([
        runtime_mode,
        symbol,
        "LONG",
        status_bucket + mode_suffix,
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

    # Report-card diagnostics: actual leverage/margin used by the exchange path.
    used_margin = _safe_float(managed_order_result.get("used_margin_usdt"), 0.0)
    if used_margin > 0:
        _safe_set_trade_attr(trade, "used_margin_usdt", used_margin)
        _safe_set_trade_attr(trade, "margin_usdt", used_margin)
        _safe_set_trade_attr(trade, "allocated_margin_usdt", used_margin)

    actual_leverage = _safe_float(
        managed_order_result.get("effective_leverage")
        or managed_order_result.get("actual_leverage")
        or managed_order_result.get("requested_leverage"),
        0.0,
    )
    if actual_leverage > 0:
        _safe_set_trade_attr(trade, "effective_leverage", actual_leverage)
        _safe_set_trade_attr(trade, "actual_leverage", actual_leverage)

    td_mode = str(managed_order_result.get("td_mode") or "").strip()
    if td_mode:
        _safe_set_trade_attr(trade, "td_mode", td_mode)
        _safe_set_trade_attr(trade, "margin_mode", td_mode)



def _execute_managed_okx_order(
    okx_client: OKXTradeClient,
    signal,
    settings: Settings,
) -> dict:
    sl_value = float(getattr(signal, "sl", 0.0) or 0.0)
    raw_tp1 = getattr(signal, "tp1", None)
    raw_tp2 = getattr(signal, "tp2", None)
    tp1_value = float(raw_tp1) if raw_tp1 not in (None, "", "-", 0) else None
    tp2_value = float(raw_tp2) if raw_tp2 not in (None, "", "-", 0) else None
    entry_value = float(getattr(signal, "entry", 0.0) or 0.0)

    sizing = _resolve_entry_margin_plan(okx_client, settings)
    margin_usdt = max(_safe_float(sizing.get("margin_usdt"), 0.0), 0.0)

    TD_MODE = _resolve_okx_td_mode(settings)
    print(f"OKX_TD_MODE_ACTIVE | {TD_MODE}", flush=True)

    live_okx_mode = bool(
        okx_client is not None
        and getattr(okx_client, "configured", False)
        and not bool(getattr(settings, "okx_simulated", True))
    )
    if live_okx_mode and margin_usdt < LIVE_MIN_EXECUTION_MARGIN_USDT:
        reason = str(sizing.get("reason") or "live_okx_margin_too_small")
        entry_result = {
            "ok": False,
            "reason": reason,
            "simulated": False,
            "balance": sizing.get("reference_balance_usdt"),
            "margin_usdt": margin_usdt,
            "min_execution_margin_usdt": LIVE_MIN_EXECUTION_MARGIN_USDT,
        }
        return {
            "ok": False,
            "entry": entry_result,
            "tp_split": None,
            "plan": {},
            "sizing": sizing,
            "used_margin_usdt": margin_usdt,
            "td_mode": TD_MODE,
            "requested_leverage": max(1, int(getattr(settings, "default_leverage", 1) or 1)),
            "effective_leverage": max(1, int(getattr(settings, "default_leverage", 1) or 1)),
            "leverage_set_result": None,
            "sl_attached": False,
            "tp_orders_ok": False,
            "requires_runner_trailing": False,
            "reason": reason,
        }
    if not live_okx_mode and margin_usdt <= 0:
        margin_usdt = max(_safe_float(getattr(settings, "paper_margin_usdt", 35.0), 35.0), 0.0) or 35.0

    leverage = max(1, int(getattr(settings, "default_leverage", 1) or 1))
    effective_leverage = leverage
    leverage_set_result = None
    try:
        if hasattr(okx_client, "set_leverage"):
            leverage_set_result = okx_client.set_leverage(
                inst_id=signal.symbol,
                lever=leverage,
                mgn_mode=TD_MODE,
                pos_side="long",
            )
            if isinstance(leverage_set_result, dict):
                if leverage_set_result.get("ok"):
                    actual = int(_safe_float(
                        leverage_set_result.get("lever_set")
                        or leverage_set_result.get("capped_to_max")
                        or leverage,
                        leverage,
                    ))
                    if actual > 0 and actual != leverage:
                        print(
                            f"⚠️ Leverage capped for {signal.symbol}: "
                            f"requested={leverage}x → actual={actual}x",
                            flush=True,
                        )
                    effective_leverage = max(1, actual)
                else:
                    print(
                        f"⚠️ set_leverage failed for {signal.symbol}: "
                        f"{leverage_set_result.get('msg') or leverage_set_result}",
                        flush=True,
                    )
        else:
            print(
                f"⚠️ set_leverage method not found on okx_client — "
                f"skipping leverage pre-set for {signal.symbol}",
                flush=True,
            )
    except Exception as exc:
        print(f"⚠️ set_leverage exception for {signal.symbol}: {exc}", flush=True)

    entry_result = okx_client.place_market_long(
        signal.symbol,
        entry_value,
        margin_usdt=margin_usdt,
        leverage=effective_leverage,
        td_mode=TD_MODE,
        sl_trigger_px=sl_value if sl_value > 0 else None,
        tag="entry",
    )

    plan = {}
    if entry_value > 0 and sl_value > 0 and tp1_value and tp2_value:
        plan = okx_client.build_managed_trade_plan(
            signal.symbol,
            entry_value,
            margin_usdt,
            effective_leverage,
            sl_value,
            tp1_value,
            tp2_value,
        )

    tp_split_result = None
    if entry_result.get("ok"):
        if tp1_value is None or tp2_value is None:
            print(
                f"⚠️ Skipping TP placement due to invalid TP values: "
                f"tp1={tp1_value}, tp2={tp2_value}",
                flush=True,
            )
            tp_split_result = {
                "ok": False,
                "reason": "invalid_tp_values",
                "tp1_price": tp1_value,
                "tp2_price": tp2_value,
            }
        else:
            try:
                tp_response = okx_client.place_reduce_only_tp_split(
                    signal.symbol,
                    entry_value,
                    margin_usdt,
                    effective_leverage,
                    tp1_price=tp1_value,
                    tp2_price=tp2_value,
                    td_mode=TD_MODE,
                    tag="tp",
                )

                if not tp_response:
                    print("❌ TP response is empty", flush=True)
                    tp_split_result = {"ok": False, "reason": "empty_tp_response"}
                elif isinstance(tp_response, dict):
                    ok_flag = bool(tp_response.get("ok"))
                    # place_reduce_only_tp_split returns {ok, tp1, tp2, ...}; it does not return raw data.
                    # Treat tp1/tp2 success as the source of truth.
                    tp1_ok = bool((tp_response.get("tp1") or {}).get("ok"))
                    tp2_ok = bool((tp_response.get("tp2") or {}).get("ok"))
                    if not ok_flag:
                        print(f"❌ TP rejected by OKX: {tp_response}", flush=True)
                        tp_split_result = tp_response
                    elif not (tp1_ok and tp2_ok):
                        print(f"⚠️ TP response accepted but partial status unclear: {tp_response}", flush=True)
                        tp_split_result = {
                            **tp_response,
                            "ok": bool(ok_flag),
                            "reason": tp_response.get("reason") or "tp_split_placed_partial_status_unverified",
                        }
                    else:
                        print(f"✅ TP placed successfully: tp1={tp_response.get('tp1_size')} tp2={tp_response.get('tp2_size')}", flush=True)
                        tp_split_result = tp_response
                else:
                    print(f"⚠️ Unexpected TP response format: {tp_response}", flush=True)
                    tp_split_result = {
                        "ok": False,
                        "reason": "unexpected_tp_response_format",
                        "raw": str(tp_response),
                    }
            except Exception as exc:
                print(f"❌ TP placement failed (exception): {exc}", flush=True)
                tp_split_result = {
                    "ok": False,
                    "reason": f"tp_exception:{exc}",
                }

    return {
        "ok": bool(entry_result.get("ok")),
        "entry": entry_result,
        "tp_split": tp_split_result,
        "plan": plan,
        "sizing": sizing,
        "used_margin_usdt": margin_usdt,
        "td_mode": TD_MODE,
        "requested_leverage": leverage,
        "effective_leverage": effective_leverage,
        "leverage_set_result": leverage_set_result,
        "sl_attached": bool(
            sl_value > 0 and ((entry_result.get("payload") or {}).get("attachAlgoOrds"))
        ),
        "tp_orders_ok": None if tp_split_result is None else bool(tp_split_result.get("ok")),
        "requires_runner_trailing": bool(
            (plan.get("runner") or {}).get("requires_trailing_after_tp2")
        ) if isinstance(plan, dict) else False,
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



def _build_runtime_track_buttons(signal, source: str | None = None) -> dict:
    """Build Track button with explicit source to avoid Simulation/Execution mixups."""
    buttons = build_signal_buttons(signal)
    symbol = str(getattr(signal, "symbol", "") or "")
    src = str(source or "auto").strip().lower()
    if src not in {"simulation", "execution", "auto"}:
        src = "auto"
    try:
        buttons = dict(buttons or {})
        rows = [list(row) for row in buttons.get("inline_keyboard", [])]
        if rows and rows[0] and symbol:
            rows[0][0] = dict(rows[0][0])
            rows[0][0]["callback_data"] = f"track:{src}:{symbol}"[:64]
            buttons["inline_keyboard"] = rows
    except Exception:
        return build_signal_buttons(signal)
    return buttons


def _track_candidates_for_source(result: dict, source: str, settings: Settings) -> list:
    """Return track candidates from the correct runtime bucket.

    Explicit source comes from new buttons. Old buttons use current runtime mode.
    This prevents a Simulation trade and an Execution trade with the same symbol
    from shadowing each other.
    """
    source = str(source or "auto").strip().lower()
    runtime_mode = _get_signal_delivery_mode(settings)
    execution_trades = list((result or {}).get("trades", []) or [])
    simulation_trades = list((result or {}).get("simulation_trades", []) or [])

    if source in {"sim", "simulation"}:
        return simulation_trades
    if source in {"exec", "execution", "trading"}:
        return execution_trades
    if runtime_mode == "simulation":
        return simulation_trades
    if runtime_mode == "trading":
        return execution_trades
    return [*execution_trades, *simulation_trades]

def _dispatch_signals(sender: TelegramSender, result: dict, settings: Settings, sent_fingerprints: dict[str, float], okx_client: OKXTradeClient | None = None, trade_store: RedisTradeStore | None = None) -> None:
    for item in _iter_signal_items_for_dispatch(result):
        signal = item["signal"]
        exec_result = item["execution"]

        # Final safety net: true danger protections have zero exceptions.
        # Even if a stale/older result item reached dispatch as accepted_preview,
        # convert it to protection_pause before any simulation fill or OKX order.
        hard_protection_rejection = _hard_execution_protection_rejection(
            (result or {}).get("drawdown_status"),
            (result or {}).get("loss_streak_guard"),
        )
        if hard_protection_rejection and str((exec_result or {}).get("status") or "").strip().lower() in {"accepted_preview", "pending_pullback_preview"}:
            exec_result = dict(hard_protection_rejection)
            item["execution"] = exec_result
            item["message"] = build_signal_message(signal, exec_result)
            item["eligible_for_activation"] = False
            item["register_as_open_trade"] = False

        exec_status = str(exec_result.get("status") or "")
        is_execution = exec_status in {"accepted_preview", "pending_pullback_preview"}
        can_place_order = exec_status == "accepted_preview"
        if not _should_dispatch_signal_item(item, settings):
            item["announcement_status"] = "filtered_signal_mode"
            continue
        fingerprint = _build_signal_fingerprint(signal, exec_result)
        dedup_ttl = _signal_fingerprint_ttl(exec_result)
        if _is_duplicate_signal_fingerprint(
            fingerprint,
            sent_fingerprints,
            trade_store,
            ttl_seconds=dedup_ttl,
        ):
            item["announcement_status"] = "deduplicated"
            item["dedup_fingerprint"] = fingerprint
            item["dedup_ttl_seconds"] = dedup_ttl
            print(
                f"DEDUP_SKIP | {getattr(signal, 'symbol', '-')} | "
                f"status={exec_status} | "
                f"runtime={(exec_result or {}).get('runtime_mode')} | "
                f"risk_mode={(exec_result or {}).get('risk_mode')} | "
                f"ttl={dedup_ttl}s | fp={fingerprint}",
                flush=True,
            )
            continue

        text = item["message"]
        managed_order_result = None
        runtime = _runtime_mode_snapshot(settings)
        simulation_mode_active = str(runtime.get("active_mode")) == "simulation"
        _redis_ok = bool(trade_store and getattr(trade_store, "enabled", False))
        exchange_required = bool(
            can_place_order
            and str(runtime.get("active_mode")) == "trading"
            and bool(runtime.get("effective_orders_enabled"))
            and settings.execution_enabled
            and okx_client
            and _redis_ok  # ✅ SAFETY: لا تفتح صفقات بدون Redis
        )
        if can_place_order and not _redis_ok and str(runtime.get("active_mode")) == "trading":
            print(
                f"🚨 SAFETY: Blocked OKX order for {getattr(item.get('signal'), 'symbol', '-')} "
                f"— Redis is OFF, execution blocked to prevent untracked positions.",
                flush=True,
            )

        # Final pre-order activation guard.
        # accepted_preview can still have eligible_for_activation=False after
        # same-scan slot/same-symbol gates. In that case we must NOT send a real
        # OKX order, because _register_exchange_trade_immediately intentionally
        # registers only real exchange fills. This prevents live positions that
        # are opened but not tracked in Redis/reports.
        if exchange_required and not bool(item.get("eligible_for_activation")):
            reason = str(item.get("activation_block_reason") or "not_eligible_for_activation")
            managed_order_result = {
                "ok": False,
                "entry": {
                    "ok": False,
                    "simulated": False,
                    "reason": reason,
                },
                "reason": reason,
                "tp_split": None,
                "plan": {},
                "sizing": {},
                "sl_attached": False,
                "tp_orders_ok": False,
                "requires_runner_trailing": False,
            }
            exchange_order_ok = False
            exchange_required = False
            item["exchange_required"] = False
            item["exchange_order_ok"] = False
            item["exchange_order_result"] = managed_order_result
            item["register_as_open_trade"] = False
            item["announcement_status"] = "activation_blocked_before_okx_order"
            print(
                f"🛑 OKX_PRE_ORDER_BLOCK | {getattr(signal, 'symbol', '-')} | reason={reason}",
                flush=True,
            )
        else:
            exchange_order_ok = True

        if simulation_mode_active:
            text = _simulation_signal_badge(text)

        if exchange_required:
            # Final fail-closed live guard immediately before sending any OKX order.
            # It protects against stale Redis/report state and against positions opened
            # earlier in the same scan/deploy that are not yet reflected in reports.
            _live_ok, _live_symbols, _live_reason = _fetch_live_okx_position_inst_ids_strict(okx_client)
            _candidate_symbol_norm = _normalize_okx_inst_id(getattr(signal, "symbol", ""))
            _tracked_trades_now = list(result.get("trades", []) or [])
            _ref_balance_now = _safe_float(((result or {}).get("portfolio_state_inputs") or {}).get("reference_portfolio"), 0.0)
            _alloc_now, _max_live_positions_now = _risk_sizing_constants(settings, reference_balance=_ref_balance_now)
            _max_live_positions_now = max(1, int(_max_live_positions_now or getattr(settings, "max_execution_positions", 1) or 1))

            _okx_blocks = False
            _okx_block_reason = "ok"
            if not _live_ok:
                _okx_blocks = True
                _okx_block_reason = f"okx_positions_unavailable_fail_closed:{_live_reason}"
            elif len(_live_symbols) >= _max_live_positions_now and _candidate_symbol_norm not in _live_symbols:
                _okx_blocks = True
                _okx_block_reason = f"live_okx_max_positions_reached:{len(_live_symbols)}/{_max_live_positions_now}"
            else:
                # Last live-exchange same-symbol guard: prevent duplicates before TP2,
                # while still allowing the project-designed TP2 re-entry unlock.
                _okx_blocks, _okx_block_reason = _okx_symbol_blocks_reentry(
                    okx_client,
                    _candidate_symbol_norm,
                    tracked_trades=_tracked_trades_now,
                )

            if _okx_blocks:
                managed_order_result = {
                    "ok": False,
                    "entry": {
                        "ok": False,
                        "simulated": False,
                        "reason": _okx_block_reason,
                    },
                    "reason": _okx_block_reason,
                    "tp_split": None,
                    "plan": {},
                    "sizing": {},
                    "sl_attached": False,
                    "tp_orders_ok": False,
                    "requires_runner_trailing": False,
                }
                exchange_order_ok = False
                item["eligible_for_activation"] = False
                item["register_as_open_trade"] = False
                print(
                    f"🛑 OKX_LIVE_GUARD_BLOCK | {getattr(signal, 'symbol', '-')} | reason={_okx_block_reason} | live={','.join(sorted(_live_symbols)) if '_live_symbols' in locals() else '-'}",
                    flush=True,
                )
            else:
                if _okx_block_reason == "live_runner_after_tp2_allowed":
                    print(
                        f"✅ OKX_TP2_REENTRY_ALLOWED | {getattr(signal, 'symbol', '-')} | runner_live=True",
                        flush=True,
                    )
                managed_order_result = _execute_managed_okx_order(okx_client, signal, settings)
                exchange_order_ok = bool(managed_order_result.get("ok"))
                if exchange_order_ok:
                    _mark_recent_bot_okx_order(
                        getattr(signal, "symbol", ""),
                        reason="managed_order_ok_before_immediate_registration",
                        signal=signal,
                        managed_order_result=managed_order_result,
                        trade=item.get("candidate_trade"),
                    )
                    _register_exchange_trade_immediately(
                        result,
                        item,
                        managed_order_result,
                        trade_store=trade_store,
                    )
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
        item["register_as_open_trade"] = bool(
            simulation_mode_active or (exchange_required and exchange_order_ok)
        )

        if exchange_required:
            _attach_exchange_state_to_trade(item.get("candidate_trade"), managed_order_result)

        track_source = "simulation" if simulation_mode_active else ("execution" if exchange_required else "auto")
        send_result = _send_text(sender, text, reply_markup=_build_runtime_track_buttons(signal, track_source))
        send_ok = bool(isinstance(send_result, dict) and send_result.get("ok"))
        if not send_ok and exchange_required and exchange_order_ok:
            try:
                time.sleep(2.0)
                send_result = _send_text(sender, text, reply_markup=_build_runtime_track_buttons(signal, track_source))
                send_ok = bool(isinstance(send_result, dict) and send_result.get("ok"))
                if not send_ok:
                    print(
                        f"⚠️ TELEGRAM_SEND_FAILED | {getattr(signal, 'symbol', '-')} | "
                        "trade registered in Redis but notification failed",
                        flush=True,
                    )
            except Exception as exc:
                print(
                    f"⚠️ TELEGRAM_SEND_RETRY_EXCEPTION | {getattr(signal, 'symbol', '-')} | {exc}",
                    flush=True,
                )
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
                    reply_markup=_build_runtime_track_buttons(signal, "execution"),
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
            try:
                _activate_announced_trade(result, item, trade_store=trade_store)
            except Exception as exc:
                print(f"❌ Trade registration failed: {exc}", flush=True)
        else:
            item["announcement_status"] = "sent" if send_ok else "send_failed"

def _build_execution_balance_header(result: dict, settings: Settings) -> str:
    """بلوك رصيد OKX يظهر في أعلى تقارير التنفيذ.

    بيأخذ البيانات من portfolio_state_inputs اللي بتجي من OKX balance
    أو fallback وبيظهرها بشكل واضح مع Low Balance Mode لو مفعل.
    """
    inputs = dict((result or {}).get("portfolio_state_inputs") or {})
    reference_balance = _safe_float(inputs.get("reference_portfolio"), 0.0)
    margin_per_trade = _safe_float(inputs.get("margin_per_trade"), 0.0)
    allocation_pct, slot_count = _risk_sizing_constants(settings, reference_balance=reference_balance)
    low_balance = bool(0 < reference_balance < LOW_BALANCE_THRESHOLD_USDT)

    lines = [
        "💼 <b>OKX Account — Execution Sizing</b>",
        "━━━━━━━━━━━━",
        f"• Reference Balance: <b>{reference_balance:,.2f} USDT</b>",
        f"• Allocation: <b>{allocation_pct:.2f}%</b> / <b>{slot_count} slots</b>",
        f"• Planned Margin / Trade: <b>{margin_per_trade:,.2f} USDT</b>",
    ]
    if low_balance:
        lines.append(f"⚠️ <b>Low Balance Mode</b> — balance &lt; {LOW_BALANCE_THRESHOLD_USDT:.0f} USDT")
    lines.append("━━━━━━━━━━━━")
    return "\n".join(lines)


def _build_exec_intel_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "📊 تقرير التنفيذ", "callback_data": "cmd:/report_execution_intelligence"},
            ],
            [
                {"text": "📋 Trades JSONL", "callback_data": "ai:exec_trades"},
                {"text": "📋 Rejections JSONL", "callback_data": "ai:exec_rejections"},
            ],
            [
                {"text": "📦 Daily Snapshot JSON", "callback_data": "ai:exec_snapshot"},
            ],
            [
                {"text": "🔙 رجوع", "callback_data": "menu:main"},
            ],
        ]
    }


def _build_sim_intel_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "📊 تقرير المحاكاة", "callback_data": "cmd:/report_simulation_intelligence"},
            ],
            [
                {"text": "📋 Trades JSONL", "callback_data": "ai:sim_trades"},
                {"text": "📋 Rejections JSONL", "callback_data": "ai:sim_rejections"},
            ],
            [
                {"text": "📦 Daily Snapshot JSON", "callback_data": "ai:sim_snapshot"},
            ],
            [
                {"text": "🔙 رجوع", "callback_data": "menu:main"},
            ],
        ]
    }


def _build_exec_intel_panel() -> str:
    return "\n".join([
        "🧠🚀 <b>Execution Intelligence</b>",
        "━━━━━━━━━━━━",
        "📊 تقرير ذكاء صفقات التنفيذ",
        "اختار:",
        "",
        "📊 <b>التقرير العادي</b>",
        "تقرير Telegram كامل بالـ setups والـ win rate والـ diagnostics",
        "",
        "🤖 <b>AI Research Export</b>",
        "ملفات JSON/JSONL جاهزة للتحليل بـ GPT / Claude / Python",
        "• Trades JSONL — كل صفقة مغلقة بيانات كاملة",
        "• Rejections JSONL — كل رفض مع snapshot كامل",
        "• Daily Snapshot — حالة يومية شاملة",
    ])


def _build_sim_intel_panel() -> str:
    return "\n".join([
        "🧠🧪 <b>Simulation Intelligence</b>",
        "━━━━━━━━━━━━",
        "📊 تقرير ذكاء صفقات المحاكاة",
        "اختار:",
        "",
        "📊 <b>التقرير العادي</b>",
        "تقرير Telegram كامل بالـ setups والـ win rate والـ diagnostics",
        "",
        "🤖 <b>AI Research Export</b>",
        "ملفات JSON/JSONL جاهزة للتحليل بـ GPT / Claude / Python",
        "• Trades JSONL — كل صفقة مغلقة بيانات كاملة",
        "• Rejections JSONL — كل رفض مع snapshot كامل",
        "• Daily Snapshot — حالة يومية شاملة",
    ])


def _build_ai_report_panel(result: dict, settings: Settings, trade_store: RedisTradeStore | None = None) -> str:
    """ملخص حالة AI Export Layer."""
    from pathlib import Path
    import os

    base = Path(os.environ.get("AI_REPORTS_DIR", "./data/ai_reports"))
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _count_lines(path: Path) -> int:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return sum(1 for _ in fh)
        except Exception:
            return 0

    def _file_size_kb(path: Path) -> float:
        try:
            return round(path.stat().st_size / 1024, 1)
        except Exception:
            return 0.0

    lines = [
        "🤖 <b>AI Research Export Layer</b>",
        "━━━━━━━━━━━━",
        f"📅 Date: {today}",
        f"📁 Base Dir: {base}",
        "",
        "🧪 <b>Simulation</b>",
    ]

    for label, key in [("Simulation", "simulation"), ("Execution", "execution")]:
        src = base / key
        trades_path = src / f"{today}_trades.jsonl"
        rejections_path = src / f"{today}_rejections.jsonl"
        snapshot_path = base / "daily_snapshots" / f"{today}_{key}.json"

        t_count = _count_lines(trades_path)
        r_count = _count_lines(rejections_path)
        s_exists = snapshot_path.exists()

        if label == "Execution":
            lines.append("")
            lines.append(f"🚀 <b>{label}</b>")

        lines += [
            f"• Trades JSONL: {t_count} records | {_file_size_kb(trades_path)} KB",
            f"• Rejections JSONL: {r_count} records | {_file_size_kb(rejections_path)} KB",
            f"• Daily Snapshot: {'✅' if s_exists else '❌ not yet'}",
        ]

    lines += [
        "",
        "━━━━━━━━━━━━",
        "📤 <b>Export Commands</b>",
        "/ai_report_sim_trades — Simulation trades JSONL",
        "/ai_report_sim_rejections — Simulation rejections JSONL",
        "/ai_report_exec_trades — Execution trades JSONL",
        "/ai_report_exec_rejections — Execution rejections JSONL",
        "/ai_report_snapshot_sim — Simulation daily snapshot JSON",
        "/ai_report_snapshot_exec — Execution daily snapshot JSON",
    ]
    return "\n".join(lines)


def _send_ai_report_file(sender: TelegramSender, key: str, today: str) -> None:
    """بعت ملف AI export للـ Telegram."""
    from pathlib import Path
    import os

    base = Path(os.environ.get("AI_REPORTS_DIR", "./data/ai_reports"))

    path_map = {
        "/ai_report_sim_trades":        base / "simulation" / f"{today}_trades.jsonl",
        "/ai_report_sim_rejections":    base / "simulation" / f"{today}_rejections.jsonl",
        "/ai_report_exec_trades":       base / "execution" / f"{today}_trades.jsonl",
        "/ai_report_exec_rejections":   base / "execution" / f"{today}_rejections.jsonl",
        "/ai_report_snapshot_sim":      base / "daily_snapshots" / f"{today}_simulation.json",
        "/ai_report_snapshot_exec":     base / "daily_snapshots" / f"{today}_execution.json",
    }

    caption_map = {
        "/ai_report_sim_trades":        f"🧪 Simulation Trades {today}",
        "/ai_report_sim_rejections":    f"🧪 Simulation Rejections {today}",
        "/ai_report_exec_trades":       f"🚀 Execution Trades {today}",
        "/ai_report_exec_rejections":   f"🚀 Execution Rejections {today}",
        "/ai_report_snapshot_sim":      f"🧪 Simulation Daily Snapshot {today}",
        "/ai_report_snapshot_exec":     f"🚀 Execution Daily Snapshot {today}",
    }

    path = path_map.get(key)
    caption = caption_map.get(key, "AI Export")

    if path is None or not path.exists():
        _send_text(sender, f"⚠️ الملف غير موجود بعد:\n<code>{path}</code>")
        return

    doc_result = sender.send_document(str(path), caption=caption)
    if not doc_result.get("ok"):
        _send_text(sender, f"⚠️ فشل إرسال الملف:\n<code>{path}</code>\nError: {doc_result.get('error') or doc_result}")


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

    protection_state = result.get("protection_state") or {}
    manual_resume_line = "OFF"
    if isinstance(protection_state, dict) and protection_state.get("manual_resume_at"):
        manual_resume_line = str(protection_state.get("manual_resume_at"))

    loss_guard = result.get("loss_streak_guard") or {}
    if loss_guard.get("active"):
        loss_guard_line = (
            f"ACTIVE | streak={int(loss_guard.get('streak', 0) or 0)} | "
            f"remaining={int(loss_guard.get('remaining_minutes', 0) or 0)}m"
        )
    else:
        loss_guard_line = f"OFF | streak={int(loss_guard.get('streak', 0) or 0)}"

    runtime = _runtime_mode_snapshot(settings)
    simulation_active = str(runtime.get("active_mode") or "").lower() == "simulation"
    if simulation_active:
        okx_status_line = f"FORCED OFF in Simulation | raw={'ON' if runtime.get('orders_enabled') else 'OFF'} | effective=OFF"
        live_status_line = "DISABLED BY SIMULATION"
    else:
        okx_status_line = f"{'ON' if runtime.get('orders_enabled') else 'OFF'} | Effective: {'ON' if runtime.get('effective_orders_enabled') else 'OFF'}"
        live_status_line = "ALLOWED" if settings.allow_live_trading else "BLOCKED"
    risk_profile = _risk_profile_snapshot(settings, result)
    risk_block = _format_risk_profile_block(risk_profile, title=_risk_profile_title(settings, risk_profile))

    # UI-only status flag. Keep it local to avoid NameError in Telegram command polling.
    drawdown_state = "halted" if (drawdown is not None and not bool(getattr(drawdown, "allowed", True))) else "active"
    trading_state_line = "🛡️ Trading State: ACTIVE"
    if drawdown_state == "halted":
        trading_state_line = "🛡️ Trading State: HALTED | Reason: Daily DD"

    return "\n".join([
        "🟢 Bot Status",
        "━━━━━━━━━━━━",
        trading_state_line,
        f"📈 Market Mode: {result.get('mode', 'UNKNOWN')}",
        f"⚡ Execution Engine: {'ON' if settings.execution_enabled else 'OFF'}",
        "🔌 OKX",
        f"• Orders: {'ON' if runtime.get('orders_enabled') else 'OFF'}",
        f"• Raw: {'ON' if runtime.get('orders_enabled') else 'OFF'}",
        f"• Effective: {'ON' if runtime.get('effective_orders_enabled') else 'OFF'}",
        f"• Live Trading: {live_status_line}",
        f"🧰 Offline Test Mode: {'ON' if settings.offline_test_mode else 'OFF'}",
        f"📡 Signal Mode: {_signal_delivery_mode_label(settings)}",
        f"🧪 Simulation: {'ON' if _is_simulation_mode(settings) else 'OFF'} | Wallet={result.get('simulation_wallet', {}).get('equity', SIMULATION_START_BALANCE_USDT):.2f} USDT",
        "",
        risk_block,
        "",
        f"📡 Telegram: {'ON' if settings.telegram_enabled else 'OFF'}",
        f"🧠 Redis: {'ON' if redis_stats.get('enabled') else '🚨 OFF — OKX execution BLOCKED for safety'} | open={redis_stats.get('open_set', 0)} | history={redis_stats.get('history_set', 0)} | checks={redis_stats.get('execution_checks', 0)}",
        f"💼 Drawdown: {drawdown_line}",
        f"🛑 Loss Streak Guard: {loss_guard_line}",
        f"🧯 Manual Resume: {manual_resume_line}",
        f"💰 Low Balance Mode: {'⚠️ ON — general=3 | block=1 | recovery=1 | alloc=40%' if (lambda b: 0 < b < LOW_BALANCE_THRESHOLD_USDT)(_safe_float((result.get('portfolio_state_inputs') or {}).get('reference_portfolio'), 0.0)) else f'OFF — general=7 | block=3 | recovery=3 | alloc=24%'}",
        f"⏱ Full Scan: {settings.scan_interval_seconds}s",
        f"🛡 Mode Guard: {settings.market_mode_guard_interval_seconds}s",
        f"🧠 Technical Snapshot: {'ON' if is_snapshot_enabled(settings, redis_client=_snapshot_redis_client(trade_store)) else 'OFF'}",
        "",
        "📌 Last Decision:",
        f"• {rejection_reason}",
        "",
        "🕹 Runtime Toggle: /okx_orders_on | /okx_orders_off",
        "✅ Managed OKX entry + SL + TP split enabled" if runtime.get("effective_orders_enabled") else "✅ Preview mode only — managed exchange placement paused",
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
    orders_on = bool(_runtime_mode_snapshot(settings).get("orders_enabled", False))
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
    allocation_pct, slot_count = _risk_sizing_constants(settings, reference_balance=reference_balance)

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


RUNTIME_OKX_ORDERS_OVERRIDE: bool | None = None
RUNTIME_SIGNAL_DELIVERY_MODE_OVERRIDE: str | None = None

# Cache آخر price_map صالح — fallback لو fetch فشل
_CACHED_PRICE_MAP: dict[str, float] = {}
_CACHED_PRICE_MAP_TS: float = 0.0
_CACHED_PRICE_MAP_TTL_SECONDS: float = 60.0  # صالح دقيقة واحدة كـ fallback

# Cache آخر رصيد OKX صحيح — يُستخدم كـ fallback لو fetch فشل
_CACHED_OKX_BALANCE: float = 0.0
_CACHED_OKX_BALANCE_TS: float = 0.0
_CACHED_OKX_BALANCE_TTL_SECONDS: float = 60.0   # دقيقة — يتحدث فوراً بعد تغيير المود

# Log-only throttle for the noisy OKX balance line.
# Does NOT change sizing, balance caching, execution guards, Redis, reports, or OKX orders.
_CACHED_OKX_BALANCE_LOG_TS: float = 0.0
_CACHED_OKX_BALANCE_LOG_VALUE: float = 0.0
_CACHED_OKX_BALANCE_LOG_INTERVAL_SECONDS: float = 300.0
_CACHED_OKX_BALANCE_LOG_MIN_DELTA_USDT: float = 0.25

_CACHED_OKX_BALANCE_LOCK = threading.Lock()  # ✅ thread-safe cache access


def _get_runtime_okx_orders(settings: Settings) -> bool:
    """Process-wide OKX orders switch used by all UI/runtime paths."""
    global RUNTIME_OKX_ORDERS_OVERRIDE
    if RUNTIME_OKX_ORDERS_OVERRIDE is not None:
        return bool(RUNTIME_OKX_ORDERS_OVERRIDE)
    return bool(getattr(settings, "okx_place_orders", False))


def _set_runtime_okx_orders(settings: Settings, enabled: bool) -> bool:
    """Update OKX order switch in one runtime source and mirror it to Settings."""
    global RUNTIME_OKX_ORDERS_OVERRIDE
    RUNTIME_OKX_ORDERS_OVERRIDE = bool(enabled)
    try:
        setattr(settings, "okx_place_orders", bool(enabled))
    except Exception:
        try:
            object.__setattr__(settings, "okx_place_orders", bool(enabled))
        except Exception:
            pass
    return _get_runtime_okx_orders(settings) == bool(enabled)


def _runtime_mode_snapshot(settings: Settings) -> dict:
    """Single source of truth for runtime mode and effective order state.

    This prevents split-brain states where /bot_modes says Simulation while
    /help or mode messages still read old OKX order flags from a stale Settings
    instance. Trading, simulation and scan are mutually exclusive runtime paths.
    """
    active_mode = _get_signal_delivery_mode(settings)
    raw_orders_enabled = _get_runtime_okx_orders(settings)
    simulated_okx = bool(getattr(settings, "okx_simulated", True))

    if active_mode == "simulation":
        risk_context = "simulation"
        effective_orders_enabled = False
        balance_source = "simulation_wallet"
    elif active_mode == "trading":
        risk_context = "execution"
        effective_orders_enabled = bool(raw_orders_enabled)
        balance_source = "okx_balance"
    else:
        risk_context = "scanner"
        effective_orders_enabled = bool(raw_orders_enabled)
        balance_source = "scanner_okx_config" if raw_orders_enabled else "scanner_only"

    return {
        "active_mode": active_mode,
        "risk_context": risk_context,
        "orders_enabled": bool(raw_orders_enabled),
        "effective_orders_enabled": bool(effective_orders_enabled),
        "simulated_okx": simulated_okx,
        "balance_source": balance_source,
    }



def _get_signal_delivery_mode(settings: Settings) -> str:
    """Single runtime source of truth for Scan/Trading/Simulation.

    Runtime button clicks may use a fresh Settings instance while the scan loop
    keeps an older Settings object. A module-level override prevents split-brain
    states where /bot_modes says Trading but mode messages still render Simulation.
    """
    global RUNTIME_SIGNAL_DELIVERY_MODE_OVERRIDE
    override = str(RUNTIME_SIGNAL_DELIVERY_MODE_OVERRIDE or "").strip().lower()
    if override in {"scan", "trading", "simulation"}:
        return override

    # Default after restart/deploy: Simulation.
    mode = str(getattr(settings, "signal_delivery_mode", "simulation") or "simulation").strip().lower()
    return mode if mode in {"scan", "trading", "simulation"} else "simulation"


def _set_runtime_signal_delivery_mode(settings: Settings, mode: str) -> bool:
    global RUNTIME_SIGNAL_DELIVERY_MODE_OVERRIDE
    normalized = str(mode or "simulation").strip().lower()
    if normalized not in {"scan", "trading", "simulation"}:
        return False

    RUNTIME_SIGNAL_DELIVERY_MODE_OVERRIDE = normalized

    # Enforce mutual exclusion. Simulation must never leave OKX orders
    # effectively enabled. Trading/scan keep the raw OKX switch as-is unless the
    # user explicitly toggles OKX orders.
    if normalized == "simulation":
        _set_runtime_okx_orders(settings, False)

    try:
        setattr(settings, "signal_delivery_mode", normalized)
    except Exception:
        try:
            object.__setattr__(settings, "signal_delivery_mode", normalized)
        except Exception:
            pass

    return _get_signal_delivery_mode(settings) == normalized


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
        status in {"accepted_preview", "pending_pullback_preview", "protection_pause"}
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
                {"text": active("trading", "🧠🚀 Exec Intel"), "callback_data": "menu:exec_intel"},
                {"text": active("scan", "🧠📊 Market Intel"), "callback_data": "cmd:/report_intelligence"},
                {"text": active("simulation", "🧠🧪 Sim Intel"), "callback_data": "menu:sim_intel"},
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
        "",
        "🧯 <b>استئناف التداول</b>",
        "يعيد تفعيل التداول ويصفر عداد 5SL / No TP1 ويعتمد الرصيد الحالي كبداية جديدة للـ Daily DD.",
        "يعرض Preview ثم يحتاج تأكيد: /confirm_resume_trading",
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
                {"text": "🧯 استئناف التداول", "callback_data": "cmd:/resume_trading"},
            ],
            [
                {"text": "🤖 OKX Control", "callback_data": "menu:okx_control"},
                {"text": "🔄 تحديث", "callback_data": "menu:bot_modes"},
            ],
        ]
    }




def _build_okx_control_keyboard(settings: Settings) -> dict:
    runtime = _runtime_mode_snapshot(settings)
    orders_on = bool(runtime.get("orders_enabled"))
    simulation_active = str(runtime.get("active_mode") or "").lower() == "simulation"
    if simulation_active:
        # In Simulation, OKX live placement is always forced OFF. Keep the raw
        # toggle available only to clear an old ON flag, but label it safely.
        toggle_text = "🧪 OKX مجبر OFF في المحاكاة" if not orders_on else "⏸ إيقاف OKX الخام"
        toggle_data = "okx_orders:off"
    else:
        toggle_text = "⏸ إيقاف تنفيذ OKX" if orders_on else "▶️ تشغيل تنفيذ OKX"
        toggle_data = "okx_orders:off" if orders_on else "okx_orders:on"

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
    runtime = _runtime_mode_snapshot(settings)
    simulation_active = str(runtime.get("active_mode") or "").lower() == "simulation"
    raw_orders_status = "ON" if bool(runtime.get("orders_enabled")) else "OFF"
    effective_status = "ON" if bool(runtime.get("effective_orders_enabled")) else "OFF"
    if simulation_active:
        okx_line = f"FORCED OFF in Simulation | raw={raw_orders_status} | effective=OFF"
        live_guard = "DISABLED BY SIMULATION"
    else:
        okx_line = f"{raw_orders_status} | Effective: {effective_status}"
        live_guard = "ALLOWED" if bool(getattr(settings, "allow_live_trading", False)) else "BLOCKED"
    simulated = "ON" if bool(runtime.get("simulated_okx")) else "OFF"
    signal_mode = _signal_delivery_mode_label(settings)
    return "\n".join([
        build_okx_control_help(),
        "",
        "⚙️ <b>Runtime OKX Control</b>",
        f"• Runtime Mode: <b>{str(runtime.get('active_mode') or '-').upper()}</b>",
        f"• Risk Context: <b>{str(runtime.get('risk_context') or '-')}</b>",
        f"• OKX Orders: <b>{okx_line}</b>",
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




def _replace_persisted_live_trades(
    trade_store: RedisTradeStore | None,
    trades: list | None,
) -> None:
    """Hard-replace current live-trade snapshot in Redis.

    save_trades() upserts provided trades but does not delete omitted closed/history
    records from Redis. This helper clears current trade/check keys first, then
    writes back only the trades that should remain.
    """
    if not trade_store or not getattr(trade_store, "enabled", False) or not getattr(trade_store, "client", None):
        return

    client = trade_store.client
    keep_trades = list(trades or [])
    try:
        namespace_keys = []
        try:
            namespace_keys = list(getattr(trade_store, "_current_namespace_keys")() or [])
        except Exception:
            namespace_keys = []

        keys_to_delete = []
        for key in namespace_keys:
            key_text = str(key or "")
            if (
                ":trade:" in key_text
                or key_text.endswith(":trades:open")
                or key_text.endswith(":trades:history")
                or key_text.endswith(":execution:checks")
            ):
                keys_to_delete.append(key_text)

        if keys_to_delete:
            for i in range(0, len(keys_to_delete), 500):
                client.delete(*keys_to_delete[i:i + 500])

        if keep_trades:
            trade_store.save_trades(keep_trades)
    except Exception as exc:
        print(f"⚠️ replace persisted live trades failed: {exc}", flush=True)


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


def _refresh_execution_reports_from_redis(
    result: dict | None,
    trade_store: RedisTradeStore | None = None,
    settings: Settings | None = None,
    okx_client: OKXTradeClient | None = None,
) -> None:
    """Force /report_execution to use the latest Redis state, not stale scan output."""
    if not isinstance(result, dict):
        return
    runtime_settings = settings or get_settings()
    refreshed_trades = list(result.get("trades", []) or [])
    refreshed_checks = list(result.get("execution_results", []) or [])
    if trade_store:
        try:
            refreshed_trades = trade_store.load_trades() or refreshed_trades
        except Exception as exc:
            print(f"⚠️ execution report redis trade refresh failed: {exc}", flush=True)
        try:
            refreshed_checks = trade_store.load_execution_checks(limit=500) or refreshed_checks
        except Exception as exc:
            print(f"⚠️ execution report redis checks refresh failed: {exc}", flush=True)

    reconcile_stats = None
    if trade_store and _is_live_okx_execution_mode(runtime_settings, okx_client):
        try:
            reconciled, reconcile_stats = _reconcile_execution_trades_with_okx(refreshed_trades, okx_client, runtime_settings)
            if reconcile_stats.get("changed"):
                trade_store.save_trades(reconciled)
            refreshed_trades = reconciled
        except Exception as exc:
            print(f"⚠️ execution report reconcile refresh failed: {exc}", flush=True)

    result["trades"] = refreshed_trades
    result["execution_results"] = refreshed_checks
    if reconcile_stats:
        result["exchange_reconcile_stats"] = reconcile_stats
    portfolio_state_inputs = _resolve_portfolio_state_inputs(okx_client, runtime_settings, trade_store=trade_store)
    result["portfolio_state_inputs"] = portfolio_state_inputs
    kwargs = _execution_report_balance_kwargs(portfolio_state_inputs)
    reports = build_report_bundle(refreshed_trades, refreshed_checks, list(result.get("signal_items", []) or []), **kwargs)
    result["command_outputs"] = build_command_outputs(refreshed_trades, refreshed_checks, list(result.get("signal_items", []) or []), **kwargs)
    result.update(reports)


def _refresh_runtime_after_report_reset(result: dict | None, trade_store: RedisTradeStore | None = None, settings: Settings | None = None) -> None:
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
    portfolio_state = build_portfolio_state_from_trades(refreshed_trades, **_portfolio_state_kwargs(portfolio_state_inputs))
    result["portfolio_state"] = portfolio_state
    result["drawdown_status"] = evaluate_drawdown(portfolio_state)
    result["drawdown_report"] = build_drawdown_report(portfolio_state)
    runtime_settings = settings or get_settings()
    result["loss_streak_guard"] = _build_loss_streak_guard(
        _loss_streak_base_trades_for_runtime(
            runtime_settings,
            result,
            execution_trades=refreshed_trades,
            simulation_trades=refreshed_sim_trades,
        )
    )


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
            _replace_persisted_live_trades(trade_store, kept_live)
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

    _refresh_runtime_after_report_reset(result, trade_store=trade_store, settings=get_settings())

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
    settings: Settings | None = None,
) -> str | None:
    runtime_settings = settings or get_settings()
    if command in {"/resume_trading", "/resume_protection", "/resume_daily_dd"}:
        return _build_manual_resume_preview(result, runtime_settings)
    if command in {"/confirm_resume_trading", "/confirm_resume_protection", "/confirm_resume_daily_dd"}:
        return _confirm_manual_resume_trading(result, runtime_settings, trade_store)

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
            portfolio_state = build_portfolio_state_from_trades(refreshed_trades, **_portfolio_state_kwargs(portfolio_state_inputs))
            result["portfolio_state"] = portfolio_state
            result["drawdown_status"] = evaluate_drawdown(portfolio_state)
            result["drawdown_report"] = build_drawdown_report(portfolio_state)
            runtime_settings = settings or get_settings()
            refreshed_sim_trades = _load_simulation_trades(trade_store) if trade_store else list(result.get("simulation_trades", []) or [])
            result["loss_streak_guard"] = _build_loss_streak_guard(
                _loss_streak_base_trades_for_runtime(
                    runtime_settings,
                    result,
                    execution_trades=refreshed_trades,
                    simulation_trades=refreshed_sim_trades,
                )
            )
        return _format_clean_result(stats, "🧹 Soft Clean Done")
    if command in {"/deep_clean", "/deep_clean_preview"}:
        stats = trade_store.clean_preview("deep") if trade_store else {"enabled": False}
        return _format_clean_preview(stats, "🧨 Deep Clean Preview", "/deep_clean_confirm")
    if command == "/deep_clean_confirm":
        stats = trade_store.deep_clean() if trade_store else {"enabled": False, "mode": "deep"}
        if result is not None and stats.get("enabled"):
            _refresh_runtime_after_report_reset(
                result,
                trade_store=trade_store,
                settings=settings or get_settings(),
            )
        return _format_clean_result(stats, "🧨 Deep Clean Done")
    return None




def _refresh_track_trades_before_reply(
    result: dict,
    settings: Settings,
    trade_store: RedisTradeStore | None = None,
    okx_client: OKXTradeClient | None = None,
) -> None:
    """Read-only price refresh for Track button display.

    IMPORTANT: This function is a VIEWER only.
    It must never:
    - save trades to Redis
    - modify trade state
    - affect reports or portfolio
    It only updates in-memory prices for display purposes.
    """
    if not isinstance(result, dict):
        return

    trades = list(result.get("trades", []) or [])
    simulation_trades = list(result.get("simulation_trades", []) or [])
    if not trades and not simulation_trades:
        return

    try:
        tickers = fetch_okx_tickers(
            settings.okx_base_url,
            settings.request_timeout,
            settings.offline_test_mode,
        )
        price_map = _build_live_price_map(tickers)
    except Exception as exc:
        print(f"⚠️ track refresh tickers failed: {exc}", flush=True)
        price_map = {}

    if not price_map:
        return

    try:
        protection_level = int(block_protection_status(result.get("state")).get("level", 0) or 0) if result.get("state") else 0
    except Exception:
        protection_level = 0

    # ✅ READ-ONLY: update in-memory only, never save to Redis
    if trades:
        try:
            refreshed_trades = update_open_trades(
                trades,
                price_map,
                protection_level=protection_level,
                okx_client=None,
                sync_exchange=False,
                sync_exchange_stop=False,
            )
            result["trades"] = refreshed_trades
            # ✅ NO trade_store.save_trades() — read-only
        except Exception as exc:
            print(f"⚠️ track execution refresh failed: {exc}", flush=True)

    if simulation_trades:
        try:
            refreshed_sim_trades = update_open_trades(
                simulation_trades,
                price_map,
                protection_level=protection_level,
                okx_client=None,
                sync_exchange=False,
                sync_exchange_stop=False,
            )
            result["simulation_trades"] = refreshed_sim_trades
            result["simulation_wallet"] = _build_simulation_wallet_snapshot(refreshed_sim_trades)
            # ✅ NO _save_simulation_trades() — read-only
        except Exception as exc:
            print(f"⚠️ track simulation refresh failed: {exc}", flush=True)


def _build_track_message_with_status(
    signal,
    exec_result: dict | None,
    trade=None,
) -> str:
    """Wrap build_track_message with EXECUTED TRADE / TRACKING ONLY label.

    This is display-only. It never modifies trades or affects reports.
    """
    base = build_track_message(signal, exec_result, trade=trade)

    is_executed = bool(
        trade is not None
        and getattr(trade, "execution_trade", False)
        and getattr(trade, "exchange_order_ok", False)
    )

    if is_executed:
        label = "\n".join([
            "🚀 <b>EXECUTED TRADE</b>",
            "• Slot Reserved: <b>YES</b>",
            "• Included In Live Reports: <b>YES</b>",
            "• Protection System: <b>ACTIVE</b>",
            "━━━━━━━━━━━━",
        ])
    else:
        label = "\n".join([
            "👁 <b>TRACKING ONLY</b>",
            "• Slot Reserved: <b>NO</b>",
            "• Included In Live Reports: <b>NO</b>",
            "• Protection System: <b>INACTIVE</b>",
            "━━━━━━━━━━━━",
        ])

    return label + "\n" + str(base or "")


def _handle_callback_query(sender: TelegramSender, result: dict, callback_query: dict, settings: Settings | None = None, okx_client: OKXTradeClient | None = None, trade_store: RedisTradeStore | None = None) -> None:
    callback_id = str(callback_query.get("id") or "")
    data = str(callback_query.get("data") or "")
    if callback_id:
        sender.answer_callback_query(callback_id, "Opened")

    if data.startswith("ai:"):
        key = data.split(":", 1)[1]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cmd_map = {
            "exec_trades":       "/ai_report_exec_trades",
            "exec_rejections":   "/ai_report_exec_rejections",
            "exec_snapshot":     "/ai_report_snapshot_exec",
            "sim_trades":        "/ai_report_sim_trades",
            "sim_rejections":    "/ai_report_sim_rejections",
            "sim_snapshot":      "/ai_report_snapshot_sim",
        }
        cmd = cmd_map.get(key)
        if cmd:
            _send_ai_report_file(sender, cmd, today)
        else:
            _send_text(sender, "⚠️ أمر AI غير معروف.")
        return

    if data.startswith("okx_orders:"):
        desired = data.split(":", 1)[1].strip().lower()
        desired_enabled = desired == "on"
        runtime_settings = settings or get_settings()
        if desired_enabled:
            _set_runtime_signal_delivery_mode(runtime_settings, "trading")
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

        # ✅ FIX: invalidate OKX balance cache فوراً عند التحويل لـ trading
        # عشان الـ effective_reference_balance يتحسب صح من أول scan
        # وبالتالي الـ low_balance_mode والـ max_positions يتحددوا صح
        if applied and desired_mode == "trading":
            global _CACHED_OKX_BALANCE, _CACHED_OKX_BALANCE_TS
            with _CACHED_OKX_BALANCE_LOCK:
                _CACHED_OKX_BALANCE = 0.0
                _CACHED_OKX_BALANCE_TS = 0.0

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
        runtime_settings = settings or get_settings()
        _refresh_track_trades_before_reply(result, runtime_settings, trade_store=trade_store, okx_client=okx_client)
        parts = data.split(":", 2)
        if len(parts) >= 3:
            track_source = parts[1].strip().lower() or "auto"
            symbol = parts[2]
        else:
            track_source = "auto"
            symbol = data.split(":", 1)[1]
        matching_trade = None
        track_candidates = _track_candidates_for_source(result, track_source, runtime_settings)
        symbol_trades = [trade for trade in track_candidates if getattr(trade, "symbol", "") == symbol]
        if not symbol_trades and track_source != "auto":
            # Fallback for old messages or after mode switches: search both, but only
            # after the requested bucket fails.
            symbol_trades = [trade for trade in _track_candidates_for_source(result, "auto", runtime_settings) if getattr(trade, "symbol", "") == symbol]
        if symbol_trades:
            def _track_sort_key(trade):
                return (
                    1 if not getattr(trade, "is_closed", False) else 0,
                    getattr(trade, "updated_at", None) or getattr(trade, "opened_at", None) or datetime.min.replace(tzinfo=timezone.utc),
                )
            matching_trade = sorted(symbol_trades, key=_track_sort_key, reverse=True)[0]
        for item in result.get("signal_items", []):
            signal = item.get("signal")
            if signal and signal.symbol == symbol:
                _send_text(sender, _build_track_message_with_status(signal, item.get("execution"), trade=matching_trade))
                return
        if matching_trade is not None:
            _send_text(sender, _build_track_message_with_status(None, None, trade=matching_trade))
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
        elif key == "exec_intel":
            _send_text(sender, _build_exec_intel_panel(), reply_markup=_build_exec_intel_keyboard())
        elif key == "sim_intel":
            _send_text(sender, _build_sim_intel_panel(), reply_markup=_build_sim_intel_keyboard())
        elif key == "main":
            runtime_settings = settings or get_settings()
            dashboard = build_master_help(
                mode=result.get("mode", "UNKNOWN"),
                execution_enabled=runtime_settings.execution_enabled,
                risk_enabled=True,
                okx_orders=_runtime_mode_snapshot(runtime_settings).get("orders_enabled", False),
                runtime_snapshot=_runtime_mode_snapshot(runtime_settings),
            )
            _send_text(sender, dashboard, reply_markup=_build_main_inline_keyboard_with_bot_modes(runtime_settings))
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
            _send_text(sender, _build_fast_status(result, settings or get_settings(), trade_store))
        else:
            sender.send_message("القسم غير متاح حاليًا.")
        return

    if data.startswith("cmd:"):
        command = data.split(":", 1)[1]
        runtime_settings = settings or get_settings()
        admin_reply = _handle_admin_clean_command(
            command,
            trade_store,
            result=result,
            settings=runtime_settings,
        )
        if admin_reply is not None:
            _send_text(sender, admin_reply, reply_markup=_build_bot_modes_keyboard(runtime_settings))
            return
        if command == "/okx_status":
            _send_text(sender, _build_okx_status_panel(runtime_settings, okx_client=okx_client))
            return
        if command.startswith("/report_execution"):
            _refresh_execution_reports_from_redis(result, trade_store=trade_store, settings=runtime_settings, okx_client=okx_client)
        simulation_outputs = _build_simulation_command_outputs(result)
        reply = (
            simulation_outputs.get(command)
            or result.get("command_outputs", {}).get(command)
            or "الأمر غير متاح في هذه النسخة."
        )
        # أضف رصيد OKX في أعلى تقارير التنفيذ فقط
        _cb_settings = settings or get_settings()
        _is_exec_report_cb = (
            command.startswith("/report_execution")
            and command not in simulation_outputs
        )
        if _is_exec_report_cb and reply and not _is_simulation_mode(_cb_settings):
            reply = _build_execution_balance_header(result, _cb_settings) + "\n" + reply
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
        "📊 Since Start",
        "/report_simulation_wallet_since_start",
        "📆 Last Month",
        "/report_simulation_wallet_30d",
        "🗓 Last Week",
        "/report_simulation_wallet_7d",
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
        okx_orders=_runtime_mode_snapshot(settings).get("orders_enabled", False),
        runtime_snapshot=_runtime_mode_snapshot(settings),
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

    if trade_store and _is_live_okx_execution_mode(settings, okx_client):
        try:
            live_trades = trade_store.load_trades() or []
            reconciled_trades, reconcile_stats = _reconcile_execution_trades_with_okx(live_trades, okx_client, settings)
            if reconcile_stats.get("changed"):
                trade_store.save_trades(reconciled_trades)
                _rebuild_runtime_reports_after_reconcile(result, reconciled_trades, trade_store, settings, okx_client, reconcile_stats)
        except Exception as exc:
            print(f"⚠️ command exchange reconcile failed: {exc}", flush=True)

    # Keep execution reports fresh at command time.
    _refresh_execution_reports_from_redis(result, trade_store=trade_store, settings=settings, okx_client=okx_client)
    command_outputs = result.get("command_outputs", {})
    for update in updates.get("result", []):
        offset = int(update.get("update_id", 0)) + 1
        callback_query = update.get("callback_query")
        if callback_query:
            _handle_callback_query(sender, result, callback_query, settings, okx_client=okx_client, trade_store=trade_store)
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
            clean_reply = _handle_admin_clean_command(command, trade_store, result, settings)
            if clean_reply is not None:
                _send_text(sender, clean_reply)
                continue

            # /help must always use the original dashboard + main keyboard.
            # Simulation reports are handled only by /help_simulation or /report_simulation*
            # and must never shadow /help.
            if command in ("/start", "/help"):
                reply = build_master_help(
                    mode=result.get("mode", "UNKNOWN"),
                    execution_enabled=settings.execution_enabled,
                    risk_enabled=True,
                    okx_orders=_runtime_mode_snapshot(settings).get("orders_enabled", False),
                    runtime_snapshot=_runtime_mode_snapshot(settings),
                )
                sender.send_message("⌨️ تم إغلاق لوحة /help القديمة.", reply_markup={"remove_keyboard": True})
                sender.send_message(reply, reply_markup=_build_main_inline_keyboard_with_bot_modes(settings))
                continue

            simulation_outputs = _build_simulation_command_outputs(result)
            if command in _SIM_WALLET_PERIOD_COMMANDS:
                _key, title, days = _SIM_WALLET_PERIOD_COMMANDS[command]
                _send_text(sender, simulation_outputs.get(command) or _simulation_header(_build_simulation_wallet_period_report(result, title, days)))
                for export in _build_simulation_wallet_export_files(result, title, days):
                    doc_result = sender.send_document(str(export.get("path")), caption=str(export.get("caption") or "Simulation Wallet Export"))
                    if not doc_result.get("ok"):
                        _send_text(sender, "⚠️ فشل إرسال ملف Wallet export. الملف جاهز على السيرفر:\n" + str(export.get("path")) + "\nError: " + str(doc_result.get("error") or doc_result))
                continue
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
            elif command == "/ai_report":
                _send_text(sender, _build_ai_report_panel(settings))
                continue
            elif command in {
                "/ai_report_sim_trades", "/ai_report_sim_rejections",
                "/ai_report_exec_trades", "/ai_report_exec_rejections",
                "/ai_report_snapshot_sim", "/ai_report_snapshot_exec",
            }:
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                _send_ai_report_file(sender, command, today)
                continue
            elif command == "/status":
                reply = _build_fast_status(result, settings, trade_store)
            elif command == "/mood":
                reply = _refresh_risk_block_in_mode_message(result.get("mode_message", "No mode yet"), settings, result)
            elif command == "/help_execution":
                reply = result.get("help_execution", "")
            elif command == "/help_normal":
                reply = result.get("help_normal", "")
            elif command == "/okx_orders_on":
                mode_applied = _set_runtime_signal_delivery_mode(settings, "trading")
                applied = bool(mode_applied and _set_runtime_okx_orders(settings, True) and _runtime_mode_snapshot(settings).get("active_mode") == "trading")
                reply = "✅ تم تشغيل وضع التداول وتنفيذ OKX." if applied else "⚠️ تعذر تشغيل تنفيذ OKX لأن Runtime Mode لم يصبح Trading."
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
                if command.startswith("/report_execution"):
                    _refresh_execution_reports_from_redis(result, trade_store=trade_store, settings=settings, okx_client=okx_client)
                    command_outputs = result.get("command_outputs", {})
                simulation_outputs = _build_simulation_command_outputs(result)
                reply = (
                    simulation_outputs.get(command)
                    or command_outputs.get(command)
                    or command_outputs.get(command.lstrip("/"))
                    or "الأمر غير متاح في نسخة v123 بعد."
                )
                # أضف رصيد OKX في أعلى تقارير التنفيذ فقط
                _is_exec_report = (
                    command.startswith("/report_execution")
                    and not command.startswith("/report_execution_intelligence")
                    and command not in simulation_outputs
                )
                if _is_exec_report and reply and not _is_simulation_mode(settings):
                    balance_header = _build_execution_balance_header(result, settings)
                    reply = balance_header + "\n" + reply
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



def _reminder_trades_for_runtime(result: dict | None, settings: Settings | None = None) -> list:
    """Return the trade list that mode reminders should display.

    Simulation reminders must count simulation_trades, while live/trading
    reminders must count real execution trades. This keeps BLOCK reminders
    from showing zero open positions during Simulation.
    """
    result = result or {}
    runtime_settings = settings or get_settings()
    if _is_simulation_mode(runtime_settings):
        return list(result.get("simulation_trades", []) or [])
    return list(result.get("trades", []) or [])


def _reminder_execution_results_for_runtime(result: dict | None, settings: Settings | None = None) -> list:
    """Return execution-check rows matching the active runtime mode."""
    result = result or {}
    runtime_settings = settings or get_settings()
    if _is_simulation_mode(runtime_settings):
        return list(result.get("simulation_execution_results", []) or result.get("current_execution_results", []) or [])
    return list(result.get("current_execution_results") or result.get("execution_results") or [])

def _enrich_reminder_context(result: dict, base_context: dict, settings: Settings | None = None) -> dict:
    from collections import Counter
    ctx = dict(base_context or {})
    trades = _reminder_trades_for_runtime(result, settings)
    signal_items = result.get("signal_items", []) or []
    execution_results = _reminder_execution_results_for_runtime(result, settings)
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



def _maybe_send_protection_activation_alert(
    sender: TelegramSender,
    result: dict | None,
    tracker: dict,
    settings: Settings | None = None,
) -> None:
    """Send one standalone Telegram alert when a protection state becomes active.

    Display-only:
    - does not change trading logic
    - does not change cooldowns or drawdown levels
    - avoids spam by remembering the last sent protection key in tracker
    """
    result = result or {}
    if not isinstance(tracker, dict):
        return

    alerts: list[tuple[str, str]] = []

    loss_guard = result.get("loss_streak_guard") or {}
    if isinstance(loss_guard, dict) and loss_guard.get("active"):
        cooldown_until = str(loss_guard.get("cooldown_until") or "")
        streak = int(loss_guard.get("streak", 0) or 0)
        key = f"loss_streak_guard:{cooldown_until or streak}"
        alerts.append((
            key,
            "\n".join([
                "🛡️ <b>تم تفعيل الحماية الوقائية</b>",
                _loss_streak_guard_message_ar(loss_guard),
                "",
                "سيستمر البوت في متابعة السوق، وسيُستأنف فتح الصفقات تلقائيًا بعد انتهاء فترة التهدئة.",
            ]).strip(),
        ))

    drawdown_status = result.get("drawdown_status")
    drawdown_message = _drawdown_protection_message_ar(drawdown_status)
    if drawdown_message:
        try:
            dd_level = int(getattr(drawdown_status, "level", 0) or 0)
            dd_reason = str(getattr(drawdown_status, "reason", "") or "")
            dd_pct = float(getattr(drawdown_status, "drawdown_pct", 0.0) or 0.0)
        except Exception:
            dd_level = 0
            dd_reason = "drawdown_protection"
            dd_pct = 0.0
        key = f"daily_drawdown:{dd_level}:{dd_reason}"
        title = "🛡️ <b>تم تفعيل حماية السحب اليومي</b>" if dd_level < 3 else "🛑 <b>تم تفعيل الإيقاف اليومي الكامل</b>"
        alerts.append((
            key,
            "\n".join([
                title,
                f"مستوى الحماية: {dd_level} | الخسارة اليومية: {dd_pct:.2f}%",
                drawdown_message,
                "",
                "سيظهر هذا الوضع أيضًا في رسائل المود والـ reminders وتقارير JSON للمحاكاة والتنفيذ.",
            ]).strip(),
        ))

    sent_keys = tracker.setdefault("protection_alerts_sent", set())
    if not isinstance(sent_keys, set):
        sent_keys = set(sent_keys or [])
        tracker["protection_alerts_sent"] = sent_keys

    active_keys = {key for key, _message in alerts}
    expired_sent = tracker.setdefault("protection_expiry_sent", set())
    if not isinstance(expired_sent, set):
        expired_sent = set(expired_sent or [])
        tracker["protection_expiry_sent"] = expired_sent

    # Allow a fresh alert next time after a protection fully disappears or changes level,
    # and send one clear standalone expiry/resume message.
    for old_key in list(sent_keys):
        if old_key.startswith("loss_streak_guard:") and old_key not in active_keys:
            if old_key not in expired_sent:
                _send_text(sender, "\n".join([
                    "✅ <b>انتهت فترة الحماية الوقائية</b>",
                    "تمت إعادة تفعيل فتح الصفقات تلقائيًا.",
                    "سيبدأ احتساب سلسلة 5SL / No TP1 من جديد من هذه اللحظة.",
                ]))
                expired_sent.add(old_key)
                _telegram_send_pause(TELEGRAM_NORMAL_SEND_GAP_SECONDS)
            sent_keys.discard(old_key)
        elif old_key.startswith("daily_drawdown:") and old_key not in active_keys:
            if old_key not in expired_sent:
                _send_text(sender, "\n".join([
                    "✅ <b>انتهت حماية السحب اليومي</b>",
                    "تمت إعادة تقييم الـ Daily DD وأصبح فتح الصفقات مسموحًا حسب القواعد الحالية.",
                ]))
                expired_sent.add(old_key)
                _telegram_send_pause(TELEGRAM_NORMAL_SEND_GAP_SECONDS)
            sent_keys.discard(old_key)

    for key, message in alerts:
        if key in sent_keys:
            continue
        _send_text(sender, message)
        sent_keys.add(key)
        _telegram_send_pause(TELEGRAM_NORMAL_SEND_GAP_SECONDS)

def _maybe_send_mode_reminder(sender: TelegramSender, result: dict, tracker: dict, settings: Settings | None = None) -> None:
    state = result.get("state")
    if not state:
        return
    mode = state.mode
    now = datetime.now(timezone.utc)
    changed_at = state.changed_at
    minutes_in_mode = int((now - changed_at).total_seconds() // 60)

    if tracker.get("mode") != mode or tracker.get("changed_at") != changed_at:
        # Preserve protection alert keys across mode-reminder tracker resets.
        # Otherwise a scan can send the standalone protection alert, then the
        # reminder reset clears it and the same protection alert is sent again
        # in the inner loop. This only affects Telegram de-dup state; it does
        # not change any protection/trading decision logic.
        protection_alerts_sent = tracker.get("protection_alerts_sent", set())
        protection_expiry_sent = tracker.get("protection_expiry_sent", set())
        tracker.clear()
        tracker.update({
            "mode": mode,
            "changed_at": changed_at,
            "general_sent": 0,
            "block_levels_sent": set(),
            "protection_alerts_sent": protection_alerts_sent if isinstance(protection_alerts_sent, set) else set(protection_alerts_sent or []),
            "protection_expiry_sent": protection_expiry_sent if isinstance(protection_expiry_sent, set) else set(protection_expiry_sent or []),
        })

    protection = block_protection_status(state, now=now)

    if mode == MODE_BLOCK_LONGS:
        for threshold, level in BLOCK_REMINDER_THRESHOLDS:
            if minutes_in_mode >= threshold and level not in tracker["block_levels_sent"]:
                tracker["block_levels_sent"].add(level)
                context = _enrich_reminder_context(result, result.get("mode_context", {}), settings=settings)
                context.update({
                    "reminder_count": level,
                    "minutes_in_mode": minutes_in_mode,
                    "protection_current": f"LEVEL {level} — " + ("Monitor Only" if level == 1 else "Soft Protection" if level == 2 else "Defensive Protection"),
                    "protection_next": "Soft Protection" if level == 1 else "Defensive Protection" if level == 2 else "Max protection active",
                    "remaining_minutes": 5 if level == 1 else 5 if level == 2 else 0,
                })
                # ✅ FIX: _send_text لدعم HTML tags في الـ reminder
                _send_text(sender, _append_protection_notice(build_market_mode_sections(mode, context, variant="reminder"), result))
                trades = _reminder_trades_for_runtime(result, settings)
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
        context = _enrich_reminder_context(result, result.get("mode_context", {}), settings=settings)
        context.update({"reminder_count": expected_count, "minutes_in_mode": minutes_in_mode})
        # ✅ FIX: _send_text لدعم HTML tags في الـ reminder
        _send_text(sender, _append_protection_notice(build_market_mode_sections(mode, context, variant="reminder"), result))



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

    # ✅ Priority Command Thread — يرد على الأوامر فوراً بدون انتظار الـ scan
    _cmd_lock = threading.Lock()

    def _command_thread_loop():
        nonlocal telegram_offset
        while True:
            try:
                if sender.enabled and settings.telegram_enabled and last_result is not None:
                    with _cmd_lock:
                        telegram_offset = _poll_telegram_commands_safe(
                            sender,
                            last_result,
                            telegram_offset,
                            settings,
                            trade_store,
                            okx_client=okx_client,
                        )
            except Exception as exc:
                print(f"⚠️ command thread error: {exc}", flush=True)
            time.sleep(0.5)

    _cmd_thread = threading.Thread(
        target=_command_thread_loop,
        daemon=True,
        name="telegram_commands",
    )
    _cmd_thread.start()

    startup_runtime = _runtime_mode_snapshot(settings)
    startup_lines = [
        "✅ OKX Long Bot v134 started",
        f"Telegram: {'ON' if sender.enabled and settings.telegram_enabled else 'OFF'}",
        f"Execution: {'ON' if settings.execution_enabled else 'OFF'}",
        f"Runtime mode: {startup_runtime.get('active_mode')} | risk={startup_runtime.get('risk_context')}",
        f"OKX orders: {'ON' if startup_runtime.get('orders_enabled') else 'OFF'} | effective={'ON' if startup_runtime.get('effective_orders_enabled') else 'OFF'} | simulated={startup_runtime.get('simulated_okx')}",
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
            previous_scan_mode = state.mode if state is not None else None
            result = run_once(previous_state=state, settings=settings, trade_store=trade_store, okx_client=okx_client)
            state = result["state"]
            # Make Telegram commands responsive immediately after the scan result is ready,
            # before Telegram signal/reminder dispatch starts.
            last_result = result
            if sender.enabled and settings.telegram_enabled:
                if settings.send_mode_status_each_scan:
                    mode_changed_in_scan = previous_scan_mode is not None and state.mode != previous_scan_mode
                    if mode_changed_in_scan and result.get("mode_transition_message"):
                        _send_text(sender, result.get("mode_transition_message", ""))
                    else:
                        _send_text(sender, _refresh_risk_block_in_mode_message(result.get("mode_message", ""), settings, result))
                next_mode_guard_ts = time.time() + max(60, int(settings.market_mode_guard_interval_seconds))
                _maybe_send_protection_activation_alert(sender, result, reminder_tracker, settings=settings)
                _maybe_send_mode_reminder(sender, result, reminder_tracker, settings=settings)
                _send_lifecycle_notifications(sender, result, trade_store=trade_store)
                _dispatch_signals(sender, result, settings, sent_fingerprints, okx_client if settings.execution_enabled else None, trade_store)

            _run_ai_export(result, settings)
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
                        _maybe_send_protection_activation_alert(sender, last_result, reminder_tracker, settings=settings)
                        _maybe_send_mode_reminder(sender, last_result, reminder_tracker, settings=settings)
                except Exception as exc:
                    print(f"mode reminder error: {exc}", flush=True)
            time.sleep(max(0.5, float(TELEGRAM_COMMAND_POLL_SLEEP_SECONDS)))


if __name__ == "__main__":
    live_worker()
