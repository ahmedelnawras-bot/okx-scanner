# Version: main_v212_ui_intelligence_open_polish.py
# Date: 2026-05-11
# Base: main_v203_open_trades_help_ui.py
# Changes: UI/reporting only: Intelligence UI polish, execution rejection UI polish, compact open dashboard polish.
# Preserved: Trading logic, market modes, reports, tracking/performance integration, execution modules.
# Fixed: /open_trades chunking safety and BLOCK exception setup tag consistency.

import os 
import sys 
import time 
import html 
import json 
import logging 
import threading 
import requests 
import pandas as pd 
import redis 
from concurrent.futures import ThreadPoolExecutor, as_completed 
 
from analysis.scoring import calculate_long_score, is_breakout 
from analysis.backtest import build_deep_report 
from analysis.performance_diagnostics import ( 
 build_setups_report, 
 build_scores_report, 
 build_market_report, 
 build_losses_report, 
 build_full_diagnostics_report, 
) 
from tracking.performance import ( 
 register_trade, 
 update_open_trades, 
 get_winrate_summary, 
 format_winrate_summary, 
 get_period_summary, 
 get_trade_summary, 
 format_period_summary, 
 get_setup_type_stats, 
 get_open_trades_summary, 
 format_open_trades_message,
 load_all_trades_for_report, 
) 
from analysis.rejection_tracking import (
    log_rejected_candidate,
    build_rejections_report_message,
)

# Execution folder integration (safe preview only)
try:
    from execution.executor import process_trade_candidate
    from execution.telegram_commands import (
        build_exec_status_message,
        build_exec_mode_message,
    )
    from execution.long_execution_gate import decide_long_execution_gate
    EXECUTION_GATE_AVAILABLE = True
    EXECUTION_AVAILABLE = True
except ImportError:
    EXECUTION_AVAILABLE = False
    EXECUTION_GATE_AVAILABLE = False
    def process_trade_candidate(*args, **kwargs):
        return {"status": "unavailable", "reason": "execution_module_not_found"}
    def build_exec_status_message(*args, **kwargs):
        return "⚠️ وحدة التنفيذ غير متاحة"
    def build_exec_mode_message(*args, **kwargs):
        return "⚠️ وحدة التنفيذ غير متاحة"
    def decide_long_execution_gate(candidate, market_mode, **kwargs):
        allowed = bool(kwargs.get("base_execution_allowed", False))
        return {
            "allowed": allowed,
            "path": "fallback" if allowed else "blocked",
            "reason": "fallback_base_execution_allowed" if allowed else "fallback_base_execution_rejected",
            "elite": {"passed": False, "score": 0.0, "level": "none", "reasons": [], "warnings": []},
        }

# محاولة استيراد دالة تقدير تأثير المحفظة 
try: 
 from tracking.performance import estimate_wallet_pnl 
except Exception: 
 estimate_wallet_pnl = None 
 
# محاولة استيراد entry_maturity 
try: 
 from analysis.entry_maturity import analyze_entry_maturity 
except Exception: 
 analyze_entry_maturity = None

# =====================
# LOGGING
# =====================
logging.basicConfig(
 level=logging.INFO,
 format="%(asctime)s | %(levelname)s | %(message)s",
 stream=sys.stdout
)
logger = logging.getLogger("okx-scanner")

# =====================
# CONFIG
# =====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
REDIS_URL = os.getenv("REDIS_URL")
OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"
OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
OKX_FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate"
OKX_TICKER_SINGLE_URL = "https://www.okx.com/api/v5/market/ticker"
SCAN_LIMIT = 200
TIMEFRAME = "15m"
HTF_TIMEFRAME = "1H"
FINAL_MIN_SCORE = 6.2
PRE_BREAKOUT_EXTRA_SCORE = 0.2
MAX_ALERTS_PER_RUN = 5
COOLDOWN_SECONDS = 3600
LOCAL_RECENT_SEND_SECONDS = 2700
GLOBAL_COOLDOWN_SECONDS = 120
COMMAND_POLL_INTERVAL = 1
MIN_24H_QUOTE_VOLUME = 1_000_000
NEW_LISTING_MAX_CANDLES = 50
TOP_MOMENTUM_PERCENT = 0.30
TOP_MOMENTUM_MIN_SCORE = 6.4   # modified from 7.0
TOP_MOMENTUM_NEW_MIN_SCORE = 6.0
NEW_LISTING_MIN_VOL_RATIO = 1.8
NEW_LISTING_MIN_CANDLE_STRENGTH = 0.45
NEW_LISTING_MAX_PER_RUN = 1
PRE_BREAKOUT_LOOKBACK = 20
PRE_BREAKOUT_PROXIMITY_MIN = 0.965
PRE_BREAKOUT_VOLUME_SIGNIFICANCE = 1.20
PRE_BREAKOUT_RECENT_VOL_BARS = 3
PRE_BREAKOUT_BASELINE_VOL_BARS = 12

# Late pump / bull-market protection
LATE_PUMP_DIST_MA = 4.2
LATE_PUMP_RSI = 67.0
LATE_PUMP_VOL_RATIO = 1.80
LATE_PUMP_CANDLE_STRENGTH = 0.62

BULL_CONTINUATION_MAX_DIST_MA = 3.2
BULL_CONTINUATION_MAX_RSI = 66.0
BULL_CONTINUATION_MAX_VOL_RATIO = 1.75

BULL_CONTINUATION_SCORE_PENALTY = 0.45
LATE_PUMP_SCORE_PENALTY = 0.65
EXTREME_LATE_PUMP_SCORE_PENALTY = 0.90

# Oversold reversal long
OVERSOLD_REVERSAL_ENABLED = True
OVERSOLD_REVERSAL_MIN_DIST_MA = 5.8
OVERSOLD_REVERSAL_MIN_24H_DROP = -12.0
OVERSOLD_REVERSAL_MAX_RSI = 32.0
OVERSOLD_REVERSAL_MIN_VOL_RATIO = 1.05
OVERSOLD_REVERSAL_SCORE_BONUS = 0.35
OVERSOLD_REVERSAL_MIN_SCORE = 6.2

# Market state sampling
ALT_MARKET_SAMPLE_SIZE = 12
ALT_MARKET_MIN_VALID = 6
ALT_MARKET_TIMEFRAME = "1H"
ALT_MARKET_CANDLE_LIMIT = 60

SCAN_LOCK_KEY = "scan:long:running"
SCAN_LOCK_TTL = 180

SCAN_LOOP_SLEEP_SECONDS = 5
SCAN_IDLE_SLEEP_SECONDS = 8

TELEGRAM_OFFSET_KEY = "telegram:offset:long"
TELEGRAM_BOOTSTRAP_DONE_KEY = "telegram:bootstrap_done:long"
TELEGRAM_POLL_LOCK_KEY = "telegram:poll:lock:long"
TELEGRAM_POLL_LOCK_TTL = 10

# Economic news
NEWS_WINDOW_HOURS = 2
ECONOMIC_CALENDAR_URL = "https://www.tradingview.com/economic-calendar/"

# Admin / stats reset
STATS_RESET_TS_KEY = "stats:last_reset_ts:long"
EXTRA_ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
ADMIN_CHAT_IDS = set()
if CHAT_ID:
    ADMIN_CHAT_IDS.add(str(CHAT_ID))
if EXTRA_ADMIN_CHAT_ID:
    ADMIN_CHAT_IDS.add(str(EXTRA_ADMIN_CHAT_ID))

# Candle cache
CANDLE_CACHE_TTL_15M = 25
CANDLE_CACHE_TTL_1H = 90
CANDLE_CACHE_TTL_4H = 600
CANDLE_CACHE_TTL_DEFAULT = 20

# Alt snapshot cache (خاص بالبوت الطويل)
ALT_SNAPSHOT_CACHE_KEY = "cache:long:alt_snapshot"
ALT_SNAPSHOT_CACHE_TTL = 600

# Market status snapshot cache
MARKET_STATUS_SNAPSHOT_KEY = "cache:long:market_status_snapshot"
MARKET_STATUS_SNAPSHOT_TTL = 180

# Alert tracking
ALERT_KEY_PREFIX = "alert:long"
ALERT_BY_MESSAGE_KEY_PREFIX = "alertmsg:long"
ALERT_TTL_SECONDS = 14 * 24 * 3600

# Execution control
EXECUTION_PAUSE_KEY = "execution:paused:long"
EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT = 35.0
EXECUTION_DRAWDOWN_LOCK_REASON_KEY = "execution:drawdown_lock_reason:long"
EXECUTION_DAILY_START_EQUITY_KEY = "execution:daily_start_equity:long"
EXECUTION_DAILY_START_TS_KEY = "execution:daily_start_ts:long"
EXECUTION_WALLET_FALLBACK_USD = float(os.getenv("EXECUTION_WALLET_SIZE_USD", os.getenv("EXEC_REPORT_WALLET_USD", "1000")) or 1000)
EXECUTION_TRADE_MARGIN_USD = float(os.getenv("EXEC_REPORT_TRADE_MARGIN_USD", "35") or 35)

# Track leverage display
TRACK_LEVERAGE = 15.0

# Partial Take Profit Management
TP1_CLOSE_PCT = 40           # إغلاق 40% عند TP1
TP2_CLOSE_PCT = 40           # إغلاق 40% عند TP2
TRAILING_POSITION_PCT = 20   # 20% تجري مع السوق بعد TP2
TRAILING_PCT = 2.5           # trailing stop: 2.5% تحت أعلى سعر
MOVE_SL_TO_ENTRY_AFTER_TP1 = True

# Market Guard / Modes
MARKET_MODE_KEY = "market_mode:long:current"
MARKET_MODE_LAST_KEY = "market_mode:long:last_mode"
MARKET_MODE_LAST_TRANSITION_KEY = "market_mode:long:last_transition_ts"
MARKET_MODE_LAST_RECOVERY_CHECK_KEY = "market_mode:long:last_recovery_check_ts"
MARKET_MODE_NORMAL_CANDIDATE_KEY = "market_mode:long:normal_candidate_since"
MARKET_MODE_BLOCK_STARTED_KEY = "market_mode:long:block_started_ts"
MARKET_MODE_LAST_SAFE_SEEN_KEY = "market_mode:long:last_safe_seen_ts"
MARKET_MODE_LAST_REMINDER_KEY = "market_mode:long:last_reminder_ts"
MARKET_MODE_REMINDER_COUNT_KEY = "market_mode:long:reminder_count"
MARKET_MODE_REMINDER_MODE_KEY = "market_mode:long:reminder_mode"
MARKET_MODE_BLOCK_PROTECTION_APPLIED_KEY = "market_mode:long:block_protection_applied_ts"

MODE_NORMAL_LONG = "NORMAL_LONG"
MODE_STRONG_LONG_ONLY = "STRONG_LONG_ONLY"
MODE_BLOCK_LONGS = "BLOCK_LONGS"
MODE_RECOVERY_LONG = "RECOVERY_LONG"

# قواعد إضافية للمود الحذر STRONG_LONG_ONLY (بعد حذف CAUTIOUS_LONGS)
STRONG_ONLY_ALLOWED_SETUPS = {
    "vwap_reclaim",
    "retest_breakout_confirmed",
    "wave_3",
    "liquidity_sweep_reclaim",
    "support_bounce_confirmed",
    "higher_low_continuation",
    "relative_strength_vs_btc",
    "failed_breakdown_trap",
}
STRONG_ONLY_MIN_SCORE = 7.0
STRONG_ONLY_MIN_VOL_RATIO = 1.00

# Extra setup groups used only to decide whether a signal may pass the
# STRONG_LONG_ONLY signal gate. NORMAL_LONG is not affected.
STRONG_SIGNAL_TIER_A_SETUPS = {
    "vwap_reclaim",
    "retest_breakout_confirmed",
    "wave_3",
    "higher_low_continuation",
}
STRONG_SIGNAL_TIER_B_SETUPS = {
    "support_bounce_confirmed",
    "relative_strength_vs_btc",
    "compression_before_expansion",
    "golden_pullback",
}

MODE_TRANSITION_MIN_INTERVAL = 480
RECOVERY_CHECK_INTERVAL = 120
NORMAL_CANDIDATE_DURATION = 480
BLOCK_EXIT_CONFIRM_DURATION = 900   # 15 minutes: BLOCK → STRONG confirmation
STRONG_TO_NORMAL_CONFIRM_DURATION = 300  # 5 minutes: STRONG → NORMAL confirmation

# Market Crash Guard
MARKET_GUARD_ENABLED = True
MARKET_GUARD_SAMPLE_SIZE = 30
MARKET_GUARD_TIMEFRAME = "15m"
MARKET_GUARD_CANDLE_LIMIT = 30
MARKET_GUARD_MIN_VALID = 12

MARKET_GUARD_RED_RATIO_BLOCK = 0.68
MARKET_GUARD_AVG_CHANGE_15M_BLOCK = -1.20
MARKET_GUARD_BTC_CHANGE_15M_BLOCK = -0.70
MARKET_GUARD_ALT_WEAK_BLOCK = True

# Recovery Long
RECOVERY_MAX_ALERTS = 2
RECOVERY_TOTAL_SIZE_PCT = 30
RECOVERY_ENTRY1_SIZE_PCT = 15
RECOVERY_ENTRY2_SIZE_PCT = 15
RECOVERY_ENTRY2_ATR_MULT = 0.35
RECOVERY_SL_ATR_MULT = 3.5

# Parallel candle fetch
MAX_CANDLE_FETCH_WORKERS = 10

# Weak setup penalty
WEAK_SETUP_TYPES = {
 "continuation|mtf_yes|vol_mid|bull_market",
 "continuation|mtf_yes|vol_high|bull_market",
 "breakout|mtf_yes|vol_mid|bull_market",
}

# =========================
# NEW CONFIG FOR BLOCK PROTECTION
# =========================
PROTECT_ON_BLOCK_MIN_PROFIT_PCT = 0.15
PROTECT_ON_BLOCK_BUFFER_PCT = 0.10

# =========================
# PULLBACK ENTRY CONFIG
# =========================
PULLBACK_ENTRY_WAIT_ENABLED = True
PULLBACK_ENTRY_MAX_DISTANCE_PCT = 1.20

# =========================
# REDIS
# =========================
r = None
if REDIS_URL:
 try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info("✅ Redis connected")
 except Exception as e:
    logger.error(f"❌ Redis connection error: {e}")
    r = None
else:
 logger.warning("⚠️ REDIS_URL not found")

# =========================
# LOCAL CACHE
# =========================
sent_cache = {}
last_candle_cache = {}
last_candle_cache_meta = {}
last_global_send_ts = 0.0
_local_news_cache = {"created_ts": 0, "events": []}
_last_scan_skip_log_ts = 0.0


def clean_symbol_for_message(symbol: str) -> str:
 return symbol.replace("-SWAP", "")


def get_same_candle_key(symbol: str, candle_time: int, signal_type: str = "long") -> str:
 return f"sent:{signal_type}:{symbol}:{candle_time}"


def get_symbol_cooldown_key(symbol: str, signal_type: str = "long") -> str:
 clean = clean_symbol_for_message(symbol)
 return f"cooldown:{signal_type}:{clean}"


def get_alert_key(alert_id: str) -> str:
 return f"{ALERT_KEY_PREFIX}:{alert_id}"


def get_alert_by_message_key(message_id: str) -> str:
 return f"{ALERT_BY_MESSAGE_KEY_PREFIX}:{message_id}"

def already_sent_same_candle(symbol: str, candle_time: int, signal_type: str = "long") -> bool:
 if not r:
    return False
 try:
    return bool(r.exists(get_same_candle_key(symbol, candle_time, signal_type)))
 except Exception as e:
    logger.error(f"Redis same candle exists error: {e}")
    return False

def is_symbol_on_cooldown(symbol: str, signal_type: str = "long") -> bool:
 if not r:
    return False
 try:
    return bool(r.exists(get_symbol_cooldown_key(symbol, signal_type)))
 except Exception as e:
    logger.error(f"Redis symbol cooldown exists error: {e}")
    return False

def reserve_signal_slot(symbol: str, candle_time: int, signal_type: str = "long") -> bool:
 if not r:
    return True
 same_candle_key = get_same_candle_key(symbol, candle_time, signal_type)
 cooldown_key = get_symbol_cooldown_key(symbol, signal_type)
 try:
    same_candle_ok = r.set(same_candle_key, "1", ex=7200, nx=True)
    if not same_candle_ok:
        logger.info(f"⏭ {symbol} skipped: same candle already sent")
        return False
    cooldown_ok = r.set(cooldown_key, "1", ex=COOLDOWN_SECONDS, nx=True)
    if not cooldown_ok:
        try:
            r.delete(same_candle_key)
        except Exception:
            pass
        logger.info(f"⏭ {symbol} skipped: on cooldown")
        return False
    return True
 except Exception as e:
    logger.error(f"Redis reserve error: {e}")
    return False

def release_signal_slot(symbol: str, candle_time: int, signal_type: str = "long") -> None:
 if not r:
    return
 try:
    r.delete(get_same_candle_key(symbol, candle_time, signal_type))
    r.delete(get_symbol_cooldown_key(symbol, signal_type))
    logger.info(f"🔓 Released slot for {symbol} candle {candle_time}")
 except Exception as e:
    logger.error(f"Redis release error: {e}")

def clear_stale_scan_locks_on_startup() -> None:
 if not r:
    return
 try:
    # Startup safety: after Railway redeploy/restart, any existing scan lock is
    # treated as stale because it belongs to an old process. This prevents the
    # bot from hanging for the remaining TTL before the first scan.
    try:
        if r.exists("scan:running"):
            r.delete("scan:running")
            logger.info("🧹 Cleared legacy scan:running lock on startup")
    except Exception:
        pass

    try:
        ttl = r.ttl(SCAN_LOCK_KEY)
        if ttl is not None and ttl != -2:
            r.delete(SCAN_LOCK_KEY)
            logger.warning(f"🧹 Cleared startup scan lock: {SCAN_LOCK_KEY} previous_ttl={ttl}s")
    except Exception:
        pass
 except Exception as e:
    logger.warning(f"Failed to inspect/clear scan lock on startup: {e}")

def acquire_scan_lock() -> bool:
 if not r:
    return True
 try:
    locked = r.set(SCAN_LOCK_KEY, "1", ex=SCAN_LOCK_TTL, nx=True)
    if locked:
        return True
    ttl = r.ttl(SCAN_LOCK_KEY)
    if ttl == -1:
        r.delete(SCAN_LOCK_KEY)
        logger.warning(f"🧹 Cleared scan lock without TTL during loop: {SCAN_LOCK_KEY}")
        return bool(r.set(SCAN_LOCK_KEY, "1", ex=SCAN_LOCK_TTL, nx=True))
    if ttl and ttl > 300:
        r.delete(SCAN_LOCK_KEY)
        logger.warning(f"🧹 Cleared stale scan lock during loop ttl={ttl}s: {SCAN_LOCK_KEY}")
        return bool(r.set(SCAN_LOCK_KEY, "1", ex=SCAN_LOCK_TTL, nx=True))
    logger.info(f"⏳ Scan lock active, waiting... (ttl={ttl}s)")
    return False
 except Exception as e:
    logger.error(f"Scan lock acquire error: {e}")
    return False

def release_scan_lock() -> None:
 if not r:
    return
 try:
    r.delete(SCAN_LOCK_KEY)
 except Exception as e:
    logger.error(f"Scan lock release error: {e}")

def is_global_cooldown_active() -> bool:
 if not r:
    return False
 try:
    return bool(r.exists("global_cooldown:long"))
 except Exception:
    return False

def set_global_cooldown() -> None:
 if not r:
    return
 try:
    r.set("global_cooldown:long", "1", ex=GLOBAL_COOLDOWN_SECONDS)
 except Exception:
    pass

# =========================
# PULLBACK HELPER
# =========================
def should_wait_for_pullback_entry(candidate: dict) -> bool:
    """Decide whether to delay entry for a pullback plan.

    Pullback is now reserved mainly for ordinary/retest breakouts. Strong reclaim,
    relative-strength, and support-bounce setups should enter at market so the bot
    does not miss fast continuation moves.
    """
    if not PULLBACK_ENTRY_WAIT_ENABLED:
        return False

    setup_type = str(candidate.get("setup_type", "") or "")
    force_market_setups = [
        "vwap_reclaim",
        "liquidity_sweep_reclaim",
        "support_bounce_confirmed",
        "higher_low_continuation",
        "relative_strength_vs_btc",
    ]
    if any(name in setup_type for name in force_market_setups):
        return False

    try:
        vol_ratio = float(candidate.get("vol_ratio", 0.0) or 0.0)
    except Exception:
        vol_ratio = 0.0
    if vol_ratio >= 1.6:
        return False

    pullback_entry = candidate.get("pullback_entry")
    pullback_low = candidate.get("pullback_low")
    pullback_high = candidate.get("pullback_high")
    if not pullback_entry or not pullback_low or not pullback_high or pullback_entry <= 0:
        return False

    opportunity_type = candidate.get("opportunity_type", "")
    if "Breakout" not in opportunity_type and "retest_breakout_confirmed" not in setup_type:
        return False

    entry_timing = candidate.get("entry_timing", "")
    if "🔴" in entry_timing:
        return False

    resistance_warning = candidate.get("resistance_warning", "")
    if resistance_warning == "مقاومة قريبة جدًا قبل TP1":
        return False

    market_entry = candidate.get("market_entry", 0.0)
    if market_entry <= 0:
        return False

    distance_pct = abs(float(pullback_entry) - float(market_entry)) / float(market_entry) * 100.0
    if distance_pct > 0.8:
        return False
    if distance_pct > PULLBACK_ENTRY_MAX_DISTANCE_PCT:
        return False
    return True


# =========================
# CANDLE DATA QUALITY GUARD
# =========================
NO_CANDLES_FAIL_PREFIX = "data:no_candles_fail:long"
NO_CANDLES_BLOCK_PREFIX = "data:no_candles_block:long"
NO_CANDLES_FAIL_LIMIT = 3
NO_CANDLES_FAIL_TTL_SECONDS = 60 * 60
NO_CANDLES_BLOCK_TTL_SECONDS = 60 * 60
NO_CANDLES_LOG_SAMPLE_LIMIT = 10

def _candle_data_key(prefix: str, symbol: str, timeframe: str = TIMEFRAME) -> str:
    return f"{prefix}:{str(timeframe).lower()}:{symbol}"

def is_candle_temporarily_blocked(symbol: str, timeframe: str = TIMEFRAME) -> bool:
    try:
        return bool(r and r.exists(_candle_data_key(NO_CANDLES_BLOCK_PREFIX, symbol, timeframe)))
    except Exception:
        return False

def record_candle_fetch_success(symbol: str, timeframe: str = TIMEFRAME) -> None:
    try:
        if not r:
            return
        r.delete(_candle_data_key(NO_CANDLES_FAIL_PREFIX, symbol, timeframe))
        r.delete(_candle_data_key(NO_CANDLES_BLOCK_PREFIX, symbol, timeframe))
    except Exception as e:
        logger.warning(f"record_candle_fetch_success failed for {symbol}: {e}")

def record_candle_fetch_failure(symbol: str, timeframe: str = TIMEFRAME) -> int:
    try:
        if not r:
            return 1
        fail_key = _candle_data_key(NO_CANDLES_FAIL_PREFIX, symbol, timeframe)
        fail_count = int(r.incr(fail_key))
        r.expire(fail_key, NO_CANDLES_FAIL_TTL_SECONDS)
        if fail_count >= NO_CANDLES_FAIL_LIMIT:
            block_key = _candle_data_key(NO_CANDLES_BLOCK_PREFIX, symbol, timeframe)
            r.set(block_key, str(fail_count), ex=NO_CANDLES_BLOCK_TTL_SECONDS)
            logger.warning(
                f"DATA ERROR | {symbol} | no_candles blocked for {NO_CANDLES_BLOCK_TTL_SECONDS}s "
                f"after {fail_count} consecutive failures"
            )
        return fail_count
    except Exception as e:
        logger.warning(f"record_candle_fetch_failure failed for {symbol}: {e}")
        return 1

# =========================
# NEW HELPER: consolidated rejection logging
# =========================
LONG_REJECTION_REASON_COUNTER = {}
LONG_SOFT_WARNING_COUNTER = {}
LONG_LAYER_COUNTER = {"hard_reject": 0, "soft_penalty_only": 0, "candidate_built": 0}

def _reset_long_run_counters():
    try:
        LONG_REJECTION_REASON_COUNTER.clear()
        LONG_SOFT_WARNING_COUNTER.clear()
        LONG_LAYER_COUNTER.clear()
        LONG_LAYER_COUNTER.update({"hard_reject": 0, "soft_penalty_only": 0, "candidate_built": 0})
    except Exception:
        pass

def _bump_long_rejection_reason(reason: str):
    try:
        key = str(reason or "unknown")
        LONG_REJECTION_REASON_COUNTER[key] = int(LONG_REJECTION_REASON_COUNTER.get(key, 0) or 0) + 1
        LONG_LAYER_COUNTER["hard_reject"] = int(LONG_LAYER_COUNTER.get("hard_reject", 0) or 0) + 1
    except Exception:
        pass

def _bump_long_soft_warning(reason: str):
    try:
        key = str(reason or "unknown")
        LONG_SOFT_WARNING_COUNTER[key] = int(LONG_SOFT_WARNING_COUNTER.get(key, 0) or 0) + 1
    except Exception:
        pass

def _format_long_run_rejection_top(limit: int = 5) -> str:
    try:
        if not LONG_REJECTION_REASON_COUNTER:
            return "none"
        items = sorted(LONG_REJECTION_REASON_COUNTER.items(), key=lambda x: x[1], reverse=True)[:limit]
        return ", ".join(f"{k}:{v}" for k, v in items)
    except Exception:
        return "unavailable"

def _format_long_run_soft_warning_top(limit: int = 5) -> str:
    try:
        if not LONG_SOFT_WARNING_COUNTER:
            return "none"
        items = sorted(LONG_SOFT_WARNING_COUNTER.items(), key=lambda x: x[1], reverse=True)[:limit]
        return ", ".join(f"{k}:{v}" for k, v in items)
    except Exception:
        return "unavailable"

def log_long_rejection(
    symbol,
    reason,
    candle_time=None,
    score=None,
    raw_score=None,
    final_threshold=None,
    market_state="",
    current_mode="",
    setup_type="",
    entry_timing="",
    opportunity_type="",
    dist_ma=None,
    rsi_now=None,
    vol_ratio=None,
    vwap_distance=None,
    mtf_confirmed=None,
    breakout=None,
    pre_breakout=None,
    is_reverse=None,
    extra=None,
    secondary_reasons=None,
):
    try:
        _bump_long_rejection_reason(reason)
        extra_dict = extra if isinstance(extra, dict) else {}
        if secondary_reasons:
            extra_dict["secondary_reasons"] = secondary_reasons
        log_rejected_candidate(
            redis_client=r,
            symbol=symbol,
            reason=reason,
            candle_time=candle_time,
            score=score,
            raw_score=raw_score,
            final_threshold=final_threshold,
            market_state=market_state,
            current_mode=current_mode,
            setup_type=setup_type,
            entry_timing=entry_timing,
            opportunity_type=opportunity_type,
            dist_ma=dist_ma,
            rsi_now=rsi_now,
            vol_ratio=vol_ratio,
            vwap_distance=vwap_distance,
            mtf_confirmed=mtf_confirmed,
            breakout=breakout,
            pre_breakout=pre_breakout,
            is_reverse=is_reverse,
            extra=extra_dict or None,
        )
    except Exception as e:
        logger.warning(f"log_long_rejection failed for {symbol} reason={reason}: {e}")

def classify_hard_late_rejection_reason(
    entry_timing,
    entry_maturity_status,
    had_pullback,
    fib_position,
    wave_estimate,
    dist_ma,
    rsi_now,
    vol_ratio,
    vwap_distance,
    breakout,
    pre_breakout,
    mtf_confirmed,
):
    """Returns a more specific reason string for hard_late_entry block."""
    if wave_estimate >= 5 and not had_pullback:
        return "wave_5_no_pullback"
    if fib_position == "overextended" or "امتداد سعري" in str(entry_timing):
        return "overextended_late_entry"
    if dist_ma is not None and dist_ma < -2.0 and rsi_now is not None and rsi_now < 45 and not breakout and not pre_breakout and not mtf_confirmed:
        return "post_dump_weak_rebound"
    if dist_ma is not None and dist_ma < 0 and not mtf_confirmed and not breakout and not pre_breakout:
        return "weak_recovery_below_ma"
    if not breakout and not pre_breakout:
        return "no_structure_break"
    if vol_ratio is not None and vol_ratio < 1.0:
        return "low_volume_bounce"
    return "hard_late_entry"

# =========================
# NEW HELPER: HTF context extraction
# =========================
def get_htf_context_from_candles(candles) -> dict:
    ctx = {
        "valid": False,
        "close": 0.0,
        "ma": 0.0,
        "rsi": 50.0,
        "dist_ma": 0.0,
        "above_ma": False,
        "rsi_healthy": False,
        "rsi_hot": False,
        "overextended": False,
        "trend_healthy": False,
    }
    if not candles:
        return ctx
    df = to_dataframe(candles)
    if df is None or df.empty:
        return ctx
    signal_row = get_signal_row(df)
    if signal_row is None:
        return ctx
    close = _safe_float(signal_row.get("close"), 0.0)
    ma = _safe_float(signal_row.get("ma"), 0.0)
    rsi = _safe_float(signal_row.get("rsi"), 50.0)
    if close <= 0 or ma <= 0:
        return ctx
    dist_ma = round(((close - ma) / ma) * 100, 4)
    ctx["valid"] = True
    ctx["close"] = close
    ctx["ma"] = ma
    ctx["rsi"] = rsi
    ctx["dist_ma"] = dist_ma
    ctx["above_ma"] = close > ma
    ctx["rsi_healthy"] = 50.0 <= rsi <= 65.0
    ctx["rsi_hot"] = rsi >= 70.0
    ctx["overextended"] = dist_ma >= 5.0 or rsi >= 70.0
    ctx["trend_healthy"] = ctx["above_ma"] and ctx["rsi_healthy"] and dist_ma <= 4.0
    return ctx

def evaluate_wave5_htf_override(
    entry_timing,
    entry_maturity_data,
    dist_ma,
    htf_1h_context,
    htf_4h_context,
    breakout,
    pre_breakout,
    breakout_quality,
    mtf_confirmed,
    vol_ratio,
) -> dict:
    result = {
        "is_wave5_late": False,
        "can_override": False,
        "should_reject": False,
        "reason": "",
        "penalty": 0.0,
        "label": "",
    }
    entry_maturity_status = str(entry_maturity_data.get("entry_maturity", "unknown") or "unknown")
    had_pullback = bool(entry_maturity_data.get("had_pullback", False))
    fib_position = str(entry_maturity_data.get("fib_position", "unknown") or "unknown")
    wave_estimate = int(entry_maturity_data.get("wave_estimate", 0) or 0)

    is_wave5_late = (
        ("موجة خامسة" in str(entry_timing))
        or ("متأخر جدًا" in str(entry_timing))
        or ("نهاية موجة" in str(entry_timing))
        or (wave_estimate >= 5 and not had_pullback)
        or (fib_position == "overextended" and wave_estimate >= 5)
    )
    result["is_wave5_late"] = is_wave5_late
    if not is_wave5_late:
        return result

    htf_healthy = (
        htf_1h_context.get("trend_healthy", False)
        and not htf_4h_context.get("overextended", True)
    )
    breakout_exception = (
        (breakout or pre_breakout)
        and breakout_quality in ("strong", "ok")
        and mtf_confirmed
        and (vol_ratio or 0.0) >= 1.25
    )
    can_override = htf_healthy or breakout_exception
    result["can_override"] = can_override

    if can_override:
        result["should_reject"] = False
        if htf_healthy:
            result["penalty"] = 0.25
            result["label"] = "wave_5_15m_but_htf_healthy"
        else:
            result["penalty"] = 0.15
            result["label"] = "wave_5_15m_but_breakout_confirmed"
        result["reason"] = "wave5_override_by_htf"
    else:
        htf_confirm_late = (
            htf_1h_context.get("overextended", False)
            or htf_4h_context.get("overextended", False)
            or not htf_1h_context.get("trend_healthy", False)
        )
        if htf_confirm_late:
            result["should_reject"] = True
            result["reason"] = "wave5_confirmed_by_htf"
            result["label"] = "wave_5_confirmed_by_htf"
        else:
            result["should_reject"] = True
            result["reason"] = "wave5_default_reject"
            result["label"] = "wave_5_no_pullback"
    return result

# =========================
# NEW: Extra Strong Long Setup Detectors
# =========================
def detect_failed_breakdown_trap(df, vol_ratio: float, mtf_confirmed: bool = False) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "failed_breakdown_trap", "details": {}}
    try:
        if df is None or df.empty or len(df) < 30:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        idx = signal_row.name
        if idx is None or idx < 20:
            return result
        close = _safe_float(signal_row.get("close"), 0.0)
        low = _safe_float(signal_row.get("low"), 0.0)
        open_ = _safe_float(signal_row.get("open"), 0.0)
        if close <= 0 or low <= 0:
            return result
        lookback_start = max(0, idx - 20)
        prev_lows = df["low"].iloc[lookback_start:idx].astype(float)
        if prev_lows.empty:
            return result
        swing_low = float(prev_lows.min())
        if swing_low <= 0:
            return result
        breakdown_pct = round(((swing_low - low) / swing_low) * 100, 4)
        if not (0.2 <= breakdown_pct <= 1.5):
            return result
        if close <= swing_low:
            return result
        close_position = (close - low) / (_safe_float(signal_row.get("high"), close) - low) if (_safe_float(signal_row.get("high"), close) - low) > 0 else 0.5
        checks = 0
        if close > open_:
            checks += 1
        if close_position >= 0.55:
            checks += 1
        if vol_ratio >= 1.15:
            checks += 1
        if mtf_confirmed:
            checks += 1
        if checks >= 3:
            result["detected"] = True
            result["score_bonus"] = 0.35 if mtf_confirmed else 0.25
            result["details"] = {"swing_low": swing_low, "breakdown_pct": breakdown_pct, "close_position": close_position}
        return result
    except Exception:
        return result

def detect_retest_breakout_confirmed(df, vol_ratio: float, mtf_confirmed: bool = False) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "retest_breakout_confirmed", "details": {}}
    try:
        if df is None or df.empty or len(df) < 35:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        idx = signal_row.name
        if idx is None or idx < 25:
            return result
        close = _safe_float(signal_row.get("close"), 0.0)
        if close <= 0:
            return result
        resistance_start = max(0, idx - 23)
        resistance_end = max(0, idx - 3)
        if resistance_end <= resistance_start:
            return result
        resist_highs = df["high"].iloc[resistance_start:resistance_end].astype(float)
        if resist_highs.empty:
            return result
        resistance = float(resist_highs.max())
        last5 = df.iloc[max(0, idx - 4):idx + 1]
        broke_above = any(_safe_float(r.get("close"), 0.0) > resistance for _, r in last5.iterrows())
        if not broke_above:
            return result
        retest_bars = df.iloc[max(0, idx - 5):idx + 1]
        near_resistance = any(abs(_safe_float(r.get("low"), 0.0) - resistance) / resistance <= 0.003 for _, r in retest_bars.iterrows())
        if not near_resistance:
            return result
        if close <= resistance:
            return result
        close_position = (close - _safe_float(signal_row.get("low"), close)) / (_safe_float(signal_row.get("high"), close) - _safe_float(signal_row.get("low"), close)) if (_safe_float(signal_row.get("high"), close) - _safe_float(signal_row.get("low"), close)) > 0 else 0.5
        if close <= _safe_float(signal_row.get("open"), close) and close_position < 0.55:
            return result
        if vol_ratio < 1.05:
            return result
        result["detected"] = True
        result["score_bonus"] = 0.40 if (mtf_confirmed and vol_ratio >= 1.2) else 0.25
        result["details"] = {"resistance": resistance}
        return result
    except Exception:
        return result

def detect_vwap_reclaim(df, vol_ratio: float, mtf_confirmed: bool = False) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "vwap_reclaim", "details": {}}
    try:
        if df is None or df.empty or len(df) < 25:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        idx = signal_row.name
        if idx is None or idx < 2:
            return result
        vwap = _safe_float(signal_row.get("vwap"), 0.0)
        if vwap <= 0:
            return result
        close = _safe_float(signal_row.get("close"), 0.0)
        open_ = _safe_float(signal_row.get("open"), 0.0)
        rsi = _safe_float(signal_row.get("rsi"), 50.0)
        prev_close = _safe_float(df.iloc[idx - 1].get("close"), 0.0) if idx >= 1 else 0.0
        prev2_close = _safe_float(df.iloc[idx - 2].get("close"), 0.0) if idx >= 2 else 0.0
        below_before = (prev_close < vwap) or (prev2_close < vwap)
        if not below_before:
            return result
        if close <= vwap:
            return result
        if close <= open_:
            return result
        if rsi < 48:
            return result
        if vol_ratio < 1.10:
            return result
        result["detected"] = True
        result["score_bonus"] = 0.30 if mtf_confirmed else 0.20
        result["details"] = {"vwap": vwap, "rsi": rsi, "vol_ratio": vol_ratio}
        return result
    except Exception:
        return result

def detect_relative_strength_vs_btc(symbol: str, df, btc_df, mtf_confirmed: bool = False) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "relative_strength_vs_btc", "details": {}}
    try:
        if df is None or df.empty or btc_df is None or btc_df.empty:
            return result
        if len(df) < 8 or len(btc_df) < 8:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        close_now = _safe_float(signal_row.get("close"), 0.0)
        if close_now <= 0:
            return result
        coin_change_8 = get_change_8(df)
        btc_change_8 = get_change_8(btc_df)
        if abs(coin_change_8) < 1e-6 and abs(btc_change_8) < 1e-6:
            return result
        relative_strength = round(coin_change_8 - btc_change_8, 4)
        if relative_strength <= 1.0:
            return result
        dist_ma = get_distance_from_ma_percent(df)
        rsi_now = _safe_float(signal_row.get("rsi"), 50.0)
        if dist_ma <= -1.5:
            return result
        if rsi_now < 50 and not mtf_confirmed:
            return result
        result["detected"] = True
        result["score_bonus"] = 0.35 if relative_strength > 2.0 else 0.25
        result["details"] = {"coin_change_8": coin_change_8, "btc_change_8": btc_change_8, "relative_strength": relative_strength}
        return result
    except Exception:
        return result

def detect_higher_low_continuation(df, dist_ma: float, rsi_now: float, vol_ratio: float, mtf_confirmed: bool = False) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "higher_low_continuation", "details": {}}
    try:
        if df is None or df.empty or len(df) < 35:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        idx = signal_row.name
        if idx is None or idx < 20:
            return result
        lows = df["low"].iloc[max(0, idx - 30):idx].astype(float)
        if len(lows) < 10:
            return result
        swing_lows = []
        for i in range(2, len(lows) - 2):
            if lows.iloc[i] < lows.iloc[i-1] and lows.iloc[i] < lows.iloc[i-2] and lows.iloc[i] < lows.iloc[i+1] and lows.iloc[i] < lows.iloc[i+2]:
                swing_lows.append((i, float(lows.iloc[i])))
        if len(swing_lows) < 2:
            return result
        last_swing = swing_lows[-1]
        prev_swing = swing_lows[-2]
        if last_swing[1] <= prev_swing[1]:
            return result
        if dist_ma < -0.5 or dist_ma > 3.2:
            return result
        if rsi_now < 48 or rsi_now > 66:
            return result
        if vol_ratio < 1.0:
            return result
        result["detected"] = True
        result["score_bonus"] = 0.30 if mtf_confirmed else 0.20
        result["details"] = {"swing_low1": prev_swing[1], "swing_low2": last_swing[1], "rsi": rsi_now}
        return result
    except Exception:
        return result

def detect_support_bounce_confirmed(df, vol_ratio: float, mtf_confirmed: bool = False) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "support_bounce_confirmed", "details": {}}
    try:
        if df is None or df.empty or len(df) < 30:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        idx = signal_row.name
        if idx is None or idx < 25:
            return result
        close = _safe_float(signal_row.get("close"), 0.0)
        low = _safe_float(signal_row.get("low"), 0.0)
        open_ = _safe_float(signal_row.get("open"), 0.0)
        rsi = _safe_float(signal_row.get("rsi"), 50.0)
        if close <= 0 or low <= 0:
            return result
        support_start = max(0, idx - 30)
        support_lows = df["low"].iloc[support_start:idx].astype(float)
        if support_lows.empty:
            return result
        support = float(support_lows.min())
        proximity = abs(low - support) / support if support > 0 else 999
        if not (0.003 <= proximity <= 0.006):
            return result
        close_position = (close - low) / (_safe_float(signal_row.get("high"), close) - low) if (_safe_float(signal_row.get("high"), close) - low) > 0 else 0.5
        if close_position < 0.60:
            return result
        if close <= open_:
            return result
        if vol_ratio < 1.05:
            return result
        if rsi < 45:
            return result
        result["detected"] = True
        result["score_bonus"] = 0.35 if mtf_confirmed else 0.25
        result["details"] = {"support": support, "proximity": proximity, "close_position": close_position}
        return result
    except Exception:
        return result

# ==================== NEW CONTEXT SETUPS (analysis only) ====================

def detect_compression_before_expansion(df, vol_ratio, atr_value, rsi_now, dist_ma) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "compression_before_expansion", "details": {}}
    try:
        if df is None or df.empty or len(df) < 20:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        idx = signal_row.name
        if idx is None or idx < 12:
            return result

        start = max(0, idx - 12)
        end = idx
        range_high = df["high"].iloc[start:end].astype(float).max()
        range_low = df["low"].iloc[start:end].astype(float).min()
        range_pct = (range_high - range_low) / range_low * 100 if range_low > 0 else 100
        if range_pct > 8.0:
            return result

        atr_series = df["atr"].iloc[max(0, idx-12):idx].astype(float)
        if len(atr_series) < 8:
            return result
        avg_atr = atr_series.mean()
        if avg_atr <= 0 or atr_value <= 0:
            return result
        if atr_value > avg_atr * 1.15:
            return result

        close = _safe_float(signal_row.get("close"), 0.0)
        if range_high <= 0 or close <= 0:
            return result
        proximity = (range_high - close) / range_high * 100
        if proximity > 1.5:
            return result

        if vol_ratio < 1.10:
            return result
        if not (48 <= rsi_now <= 65):
            return result

        result["detected"] = True
        result["details"] = {"range_high": range_high, "range_low": range_low, "avg_atr": avg_atr, "atr": atr_value}
        return result
    except Exception:
        return result

def detect_bull_flag_breakout(df, vol_ratio, atr_value, rsi_now, mtf_confirmed) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "bull_flag_breakout", "details": {}}
    try:
        if df is None or df.empty or len(df) < 30:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        idx = signal_row.name
        if idx is None or idx < 20:
            return result

        lookback = 20
        start = max(0, idx - lookback)
        highs = df["high"].iloc[start:idx+1].astype(float)
        lows = df["low"].iloc[start:idx+1].astype(float)
        if highs.empty or lows.empty:
            return result
        peak = highs.max()
        trough = lows.min()
        if peak <= 0 or trough <= 0:
            return result
        move_pct = (peak - trough) / trough * 100
        if move_pct < 4.0:
            return result

        pullback_pct = (peak - _safe_float(signal_row.get("close"), peak)) / peak * 100 if peak > 0 else 0
        if not (20 <= pullback_pct <= 50):
            return result

        lows_series = df["low"].iloc[start:idx+1].astype(float)
        recent_min = lows_series.tail(8).min()
        prev_min = lows_series.iloc[:-8].min() if len(lows_series) > 8 else recent_min
        if recent_min <= prev_min:
            return result

        recent_high_flag = highs.tail(8).max()
        close = _safe_float(signal_row.get("close"), 0.0)
        if close <= recent_high_flag and close <= _safe_float(signal_row.get("ma"), 0.0) and close <= _safe_float(signal_row.get("vwap"), 0.0):
            return result

        if vol_ratio < 1.15:
            return result

        result["detected"] = True
        result["details"] = {"peak": peak, "trough": trough, "pullback_pct": pullback_pct}
        return result
    except Exception:
        return result

def detect_liquidity_sweep_reclaim(df, vol_ratio, atr_value, rsi_now) -> dict:
    result = {"detected": False, "score_bonus": 0.0, "reason": "liquidity_sweep_reclaim", "details": {}}
    try:
        if df is None or df.empty or len(df) < 25:
            return result
        signal_row = get_signal_row(df)
        if signal_row is None:
            return result
        idx = signal_row.name
        if idx is None or idx < 20:
            return result

        lookback = 20
        start = max(0, idx - lookback)
        previous_lows = df["low"].iloc[start:idx].astype(float)
        if previous_lows.empty:
            return result
        swing_low = previous_lows.min()
        current_low = _safe_float(signal_row.get("low"), 0.0)
        close = _safe_float(signal_row.get("close"), 0.0)
        if current_low <= 0 or swing_low <= 0 or close <= 0:
            return result
        if current_low >= swing_low:
            return result

        if close <= swing_low:
            return result

        high = _safe_float(signal_row.get("high"), close)
        low = current_low
        open_ = _safe_float(signal_row.get("open"), close)
        body = abs(close - open_)
        lower_wick = min(open_, close) - low
        if body <= 0 or lower_wick <= body * 0.4:
            return result

        if close <= open_:
            return result
        candle_range = high - low
        close_position = (close - low) / candle_range if candle_range > 0 else 0.5
        if close_position < 0.55:
            return result

        if vol_ratio < 1.10:
            return result

        result["detected"] = True
        result["details"] = {"swing_low": swing_low, "current_low": current_low, "close": close}
        return result
    except Exception:
        return result

# ----------------------------------------------------------------------------

def get_change_8(df) -> float:
    try:
        if df is None or df.empty or len(df) < 8:
            return 0.0
        signal_row = get_signal_row(df)
        if signal_row is None:
            return 0.0
        idx = signal_row.name
        if idx < 7:
            return 0.0
        close_now = _safe_float(df.iloc[idx]["close"], 0.0)
        close_8 = _safe_float(df.iloc[idx - 7]["close"], 0.0)
        if close_8 <= 0:
            return 0.0
        return round(((close_now - close_8) / close_8) * 100, 2)
    except Exception:
        return 0.0

def detect_extra_strong_long_setups(
    symbol: str,
    df,
    btc_df,
    dist_ma: float,
    rsi_now: float,
    vol_ratio: float,
    mtf_confirmed: bool,
    atr_value: float,              
) -> dict:
    setups = []
    total_bonus = 0.0
    primary_setup = ""
    details = {}
    context_setups = []          

    detectors = [
        detect_failed_breakdown_trap,
        detect_retest_breakout_confirmed,
        detect_vwap_reclaim,
        detect_higher_low_continuation,
        detect_support_bounce_confirmed,
    ]

    for detector in detectors:
        if detector == detect_higher_low_continuation:
            res = detector(df, dist_ma, rsi_now, vol_ratio, mtf_confirmed)
        elif detector == detect_support_bounce_confirmed:
            res = detector(df, vol_ratio, mtf_confirmed)
        else:
            res = detector(df, vol_ratio, mtf_confirmed)
        if res.get("detected"):
            setups.append(res["reason"])
            total_bonus += res.get("score_bonus", 0.0)
            details[res["reason"]] = res.get("details", {})
            if not primary_setup:
                primary_setup = res["reason"]

    rel_res = detect_relative_strength_vs_btc(symbol, df, btc_df, mtf_confirmed)
    if rel_res.get("detected"):
        setups.append(rel_res["reason"])
        total_bonus += rel_res.get("score_bonus", 0.0)
        details[rel_res["reason"]] = rel_res.get("details", {})
        if not primary_setup:
            primary_setup = rel_res["reason"]

    comp_res = detect_compression_before_expansion(df, vol_ratio, atr_value, rsi_now, dist_ma)
    if comp_res.get("detected"):
        context_setups.append(comp_res["reason"])
        details[comp_res["reason"]] = comp_res.get("details", {})

    flag_res = detect_bull_flag_breakout(df, vol_ratio, atr_value, rsi_now, mtf_confirmed)
    if flag_res.get("detected"):
        context_setups.append(flag_res["reason"])
        details[flag_res["reason"]] = flag_res.get("details", {})

    liq_res = detect_liquidity_sweep_reclaim(df, vol_ratio, atr_value, rsi_now)
    if liq_res.get("detected"):
        setups.append(liq_res["reason"])
        total_bonus += liq_res.get("score_bonus", 0.0)
        details[liq_res["reason"]] = liq_res.get("details", {})
        if not primary_setup:
            primary_setup = liq_res["reason"]

    total_bonus = min(total_bonus, 0.60)

    return {
        "has_extra_setup": len(setups) > 0,
        "setups": setups,
        "score_bonus": round(total_bonus, 2),
        "primary_setup": primary_setup,
        "details": details,
        "context_setups": context_setups,    
    }



def is_relaxed_execution_setup(
    setup_type: str = "",
    extra_setup_names=None,
    primary_extra_setup: str = "",
    execution_setup_tags=None,
    mtf_confirmed: bool = False,
    vol_ratio: float = 0.0,
    score: float = 0.0,
) -> bool:
    """
    Safe helper used by softening / resistance guards.
    Returns True only for strong/known setups in a supportive context.
    This prevents NameError and keeps weak setups strict.
    """
    try:
        names = []
        if setup_type:
            names.append(str(setup_type))
        if primary_extra_setup:
            names.append(str(primary_extra_setup))
        if isinstance(extra_setup_names, (list, tuple, set)):
            names.extend([str(x) for x in extra_setup_names if x])
        elif extra_setup_names:
            names.append(str(extra_setup_names))
        if isinstance(execution_setup_tags, (list, tuple, set)):
            names.extend([str(x) for x in execution_setup_tags if x])
        elif execution_setup_tags:
            names.append(str(execution_setup_tags))

        blob = "|".join(names).lower()
        strong_tokens = {
            "vwap_reclaim",
            "retest_breakout_confirmed",
            "wave_3",
            "liquidity_sweep_reclaim",
            "support_bounce_confirmed",
            "higher_low_continuation",
            "relative_strength_vs_btc",
            "failed_breakdown_trap",
        }
        has_strong_token = any(token in blob for token in strong_tokens)
        if not has_strong_token:
            return False

        try:
            v = float(vol_ratio or 0.0)
        except Exception:
            v = 0.0
        try:
            sc = float(score or 0.0)
        except Exception:
            sc = 0.0

        # Before final scoring, score may be 0; allow only if MTF or volume supports it.
        if sc <= 0:
            return bool(mtf_confirmed or v >= 1.15)
        return bool(sc >= 7.0 and (mtf_confirmed or v >= 1.15))
    except Exception:
        return False

# =========================
# ECONOMIC CALENDAR
# =========================
NEWS_CACHE_KEY = "cache:long:high_impact_news"
NEWS_CACHE_TTL = 300

def get_upcoming_high_impact_events(window_hours: int = NEWS_WINDOW_HOURS) -> list:
    """Fetch high-impact events with Redis or in-memory caching."""
    now = int(time.time())

    if r:
        try:
            cached = r.get(NEWS_CACHE_KEY)
            if cached:
                data = json.loads(cached)
                if isinstance(data, dict) and data.get("created_ts", 0) > now - NEWS_CACHE_TTL:
                    logger.info("News cache hit (Redis)")
                    return data.get("events", [])
        except Exception as e:
            logger.warning(f"News cache read error: {e}")

    if _local_news_cache.get("created_ts", 0) > now - NEWS_CACHE_TTL:
        logger.info("News in-memory cache hit")
        return _local_news_cache.get("events", [])

    try:
        events = _fetch_high_impact_events_http(now, window_hours)

        payload = {"created_ts": now, "events": events}
        if r:
            try:
                r.set(NEWS_CACHE_KEY, json.dumps(payload, ensure_ascii=False), ex=NEWS_CACHE_TTL + 60)
                logger.info("News cache refreshed via HTTP -> Redis")
            except Exception as e:
                logger.warning(f"News cache write error: {e}")
        _local_news_cache["created_ts"] = now
        _local_news_cache["events"] = events
        return events

    except Exception as e:
        logger.warning(f"News HTTP fetch failed: {e}")

        if r:
            try:
                cached = r.get(NEWS_CACHE_KEY)
                if cached:
                    data = json.loads(cached)
                    if isinstance(data, dict):
                        logger.info("News cache fallback from Redis after HTTP failure")
                        return data.get("events", [])
            except Exception:
                pass

        if _local_news_cache.get("events"):
            logger.info("News cache fallback from local memory after HTTP failure")
            return _local_news_cache["events"]

        return []

def _fetch_high_impact_events_http(now: int, window_hours: int) -> list:
    window_end = now + (window_hours * 3600)
    url = "https://economic-calendar.tradingview.com/events"
    params = {
        "from": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)),
        "to": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(window_end)),
        "countries": "US",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tradingview.com/",
    }
    res = requests.get(url, params=params, headers=headers, timeout=10)
    if res.status_code != 200:
        logger.warning(f"Economic calendar HTTP {res.status_code}")
        return []
    data = res.json()
    events = data if isinstance(data, list) else data.get("result", [])
    high_impact = []
    for event in events:
        importance = str(event.get("importance", "").lower())
        if importance in ("high", "3", "-1"):
            event_link = (
                event.get("url")
                or event.get("link")
                or event.get("source_url")
                or event.get("event_url")
                or ECONOMIC_CALENDAR_URL
            )
            high_impact.append({
                "title": event.get("title", "Unknown Event"),
                "date": event.get("date", ""),
                "country": event.get("country", ""),
                "impact": "High",
                "link": event_link,
            })
    logger.info(f"Economic calendar: {len(high_impact)} high-impact events in next {window_hours}h")
    return high_impact


def format_news_warning(events: list) -> str:
 calendar_link = html.escape(ECONOMIC_CALENDAR_URL, quote=True)
 if not events:
    return f'📰 <b>الأخبار:</b> لا توجد أخبار High-Impact قريبة | <a href="{calendar_link}">Economic Calendar</a>'
 parts = []
 for event in events[:2]:
    title = html.escape(event.get("title", "Unknown Event"))
    link = html.escape(event.get("link", ECONOMIC_CALENDAR_URL), quote=True)
    parts.append(f'<a href="{link}">{title}</a>')
 events_text = " | ".join(parts)
 return f'📰 <b>الأخبار:</b> {events_text} | <a href="{calendar_link}">Economic Calendar</a>'


# =========================
# TELEGRAM OFFSET
# =========================
def get_telegram_offset() -> int:
 if not r:
    return 0
 try:
    val = r.get(TELEGRAM_OFFSET_KEY)
    return int(val) if val else 0
 except Exception:
    return 0

def save_telegram_offset(offset: int) -> None:
 if not r:
    return
 try:
    r.set(TELEGRAM_OFFSET_KEY, str(offset))
 except Exception:
    pass

def is_telegram_bootstrap_done() -> bool:
 if not r:
    return False
 try:
    return bool(r.exists(TELEGRAM_BOOTSTRAP_DONE_KEY))
 except Exception:
    return False

def mark_telegram_bootstrap_done() -> None:
 if not r:
    return
 try:
    r.set(TELEGRAM_BOOTSTRAP_DONE_KEY, "1")
 except Exception:
    pass

def acquire_telegram_poll_lock() -> bool:
 if not r:
    return True
 try:
    locked = r.set(TELEGRAM_POLL_LOCK_KEY, "1", ex=TELEGRAM_POLL_LOCK_TTL, nx=True)
    return bool(locked)
 except Exception as e:
    logger.error(f"Telegram poll lock acquire error: {e}")
    return False

def release_telegram_poll_lock() -> None:
 if not r:
    return
 try:
    r.delete(TELEGRAM_POLL_LOCK_KEY)
 except Exception as e:
    logger.error(f"Telegram poll lock release error: {e}")

# =========================
# TELEGRAM
# =========================
def clear_webhook() -> None:
 if not BOT_TOKEN:
    return
 url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
 params = {"drop_pending_updates": False}
 try:
    response = requests.get(url, params=params, timeout=10)
    if response.status_code == 200:
        logger.info("Telegram webhook cleared")
    else:
        logger.error(f"Webhook clear HTTP error: {response.text}")
 except Exception as e:
    logger.error(f"Webhook clear error: {e}")

def telegram_api_call(method: str, payload: dict) -> dict:
 if not BOT_TOKEN:
    return {"ok": False, "error": "missing_bot_token"}
 url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
 try:
    response = requests.post(url, json=payload, timeout=20)
    if response.status_code != 200:
        logger.error(f"Telegram {method} HTTP Error: {response.text}")
        return {"ok": False, "error": response.text}
    data = response.json()
    if not data.get("ok"):
        logger.error(f"Telegram {method} API Error: {data}")
        return data
    return data
 except Exception as e:
    logger.error(f"Telegram {method} Exception: {e}")
    return {"ok": False, "error": str(e)}

def answer_callback_query(callback_query_id: str, text: str = " ") -> None:
 if not callback_query_id:
    return
 payload = {"callback_query_id": callback_query_id}
 if text:
    payload["text"] = text
 telegram_api_call("answerCallbackQuery", payload)

def send_telegram_message(message: str, reply_markup=None) -> dict:
 if not BOT_TOKEN or not CHAT_ID:
    logger.error("❌ Telegram config missing")
    return {"ok": False}
 payload = {
    "chat_id": CHAT_ID,
    "text": message,
    "parse_mode": "HTML",
    "disable_web_page_preview": True,
 }
 if reply_markup:
    payload["reply_markup"] = reply_markup
 return telegram_api_call("sendMessage", payload)

def send_telegram_reply(chat_id: str, message: str, reply_markup=None) -> bool:
 if not BOT_TOKEN or not chat_id:
    logger.error("❌ Telegram reply config missing")
    return False
 payload = {
    "chat_id": chat_id,
    "text": message,
    "parse_mode": "HTML",
    "disable_web_page_preview": True,
 }
 if reply_markup:
    payload["reply_markup"] = reply_markup
 data = telegram_api_call("sendMessage", payload)
 return bool(data.get("ok"))


def split_telegram_message(text: str, limit: int = 3600) -> list:
 try:
    text = str(text or "")
    if len(text) <= limit:
        return [text]
    chunks = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else current + "\n\n" + block
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(block) <= limit:
            current = block
        else:
            lines = block.split("\n")
            line_chunk = ""
            for line in lines:
                cand = line if not line_chunk else line_chunk + "\n" + line
                if len(cand) <= limit:
                    line_chunk = cand
                else:
                    if line_chunk:
                        chunks.append(line_chunk)
                    line_chunk = line[:limit]
            if line_chunk:
                current = line_chunk
    if current:
        chunks.append(current)
    return chunks or [text[:limit]]
 except Exception:
    return [str(text or "")[:limit]]


def split_open_trades_dashboard_message(text: str, limit: int = 3600) -> list:
 """Split /open_trades dashboard safely by lines/sections.

 The generic splitter uses blank blocks.  Open-trades dashboard is denser, so
 this function avoids cutting inside HTML tags and prefers section boundaries.
 """
 try:
    text = str(text or "")
    if len(text) <= limit:
        return [text]
    section_starts = (
        "⚠️ <b>Danger",
        "🔒 <b>TP1",
        "🟢 <b>Winners",
        "⏳ <b>Pending",
        "📌 <b>Other",
        "🧾 <b>Executive",
    )
    chunks = []
    current = []
    current_len = 0

    for line in text.split("\n"):
        line_len = len(line) + 1
        prefer_boundary = any(line.startswith(prefix) for prefix in section_starts)
        if current and (current_len + line_len > limit or (prefer_boundary and current_len > int(limit * 0.65))):
            chunks.append("\n".join(current).strip())
            current = []
            current_len = 0

        if line_len > limit:
            if current:
                chunks.append("\n".join(current).strip())
                current = []
                current_len = 0
            for i in range(0, len(line), limit):
                part = line[i:i + limit]
                if part:
                    chunks.append(part)
            continue

        current.append(line)
        current_len += line_len

    if current:
        chunks.append("\n".join(current).strip())

    return [c for c in chunks if c] or [text[:limit]]
 except Exception:
    return split_telegram_message(text, limit=limit)


def send_telegram_reply_chunks(chat_id: str, messages) -> bool:
 if isinstance(messages, str):
    messages = split_telegram_message(messages)
 ok = True
 total = len(messages or [])
 for idx, msg in enumerate(messages or [], start=1):
    final_msg = msg
    if total > 1:
        final_msg = f"{msg}\n\n📨 <b>جزء {idx}/{total}</b>"
    ok = send_telegram_reply(chat_id, final_msg) and ok
    time.sleep(0.25)
 return ok

def get_telegram_updates(offset: int = 0):
 if not BOT_TOKEN:
    return []
 url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
 params = {"timeout": 0, "offset": offset}
 try:
    response = requests.get(url, params=params, timeout=4)
    if response.status_code != 200:
        logger.error(f"❌ getUpdates HTTP Error: {response.text}")
        return []
    data = response.json()
    if not data.get("ok"):
        logger.error(f"❌ getUpdates API Error: {data}")
        return []
    return data.get("result", [])
 except Exception as e:
    logger.error(f"❌ getUpdates Exception: {e}")
    return []


def get_local_day_start_ts() -> int:
 try:
    now = time.localtime()
    return int(time.mktime((
        now.tm_year, now.tm_mon, now.tm_mday,
        0, 0, 0,
        now.tm_wday, now.tm_yday, now.tm_isdst
    )))
 except Exception:
    return int(time.time()) - 86400

def build_report_message(period: str) -> str:
 title_map = {
    "1h": "Long Report - Last 1H",
    "today": "Long Report - Today",
    "month": "Long Report - Last 30 Days",
    "30d": "Long Report - Last 30 Days",
    "all": "Long Report - All Time",
 }
 try:
    if period == "today":
        summary = get_trade_summary(
            redis_client=r,
            market_type="futures",
            side="long",
            since_ts=get_local_day_start_ts(),
        )
        return format_period_summary(title_map["today"], summary)
    if period == "month":
        summary = get_period_summary(
            redis_client=r,
            period="30d",
            market_type="futures",
            side="long",
        )
        return format_period_summary(title_map["month"], summary)
    summary = get_period_summary(
        redis_client=r,
        period=period,
        market_type="futures",
        side="long",
    )
    return format_period_summary(title_map.get(period, "Long Report"), summary)
 except Exception as e:
    logger.error(f"build_report_message error on period={period}: {e}")
    return "❌ حصل خطأ أثناء بناء التقرير"


def _limit_telegram_message(text: str, limit: int = 3900) -> str:
    try:
        text = str(text or "")
        if len(text) <= limit:
            return text
        return text[:limit - 200] + "\n\n⚠️ تم اختصار التقرير لأن حجمه أكبر من حد Telegram."
    except Exception:
        return "❌ فشل اختصار الرسالة"


def build_deep_report_message() -> str:
    try:
        try:
            report = build_deep_report(r, market_type="futures", side="long")
        except TypeError:
            logger.warning("build_deep_report does not accept market_type/side, falling back to build_deep_report(r)")
            report = build_deep_report(r)

        if not report:
            return "ℹ️ لا توجد بيانات كافية لبناء التقرير العميق"

        return _limit_telegram_message(report)

    except Exception as e:
        logger.exception(f"build_deep_report error: {e}")
        return (
            "❌ حصل خطأ أثناء بناء التقرير العميق\n"
            f"السبب: {html.escape(str(e))}\n\n"
            "راجع Logs لمعرفة الحقل أو الدالة التي سببت المشكلة."
        )


def get_execution_max_active_trades_display() -> int:
    """Display helper only: show the effective configured execution active-trade limit."""
    try:
        from execution.risk_manager import _configured_max_positions
        return int(_configured_max_positions())
    except Exception:
        pass
    try:
        from execution.config import MAX_OPEN_POSITIONS
        return int(os.getenv("EXECUTION_MAX_ACTIVE_TRADES", str(MAX_OPEN_POSITIONS)))
    except Exception:
        pass
    try:
        return int(os.getenv("EXECUTION_MAX_ACTIVE_TRADES", os.getenv("MAX_OPEN_POSITIONS", "100")))
    except Exception:
        return 100


def build_dynamic_help_note() -> str:
    """Dynamic help footer. UI only; does not change any execution rule."""
    max_trades = get_execution_max_active_trades_display()
    guard = get_execution_daily_guard_snapshot()
    equity = guard.get("start_equity_usd", EXECUTION_WALLET_FALLBACK_USD)
    limit_pct = guard.get("limit_pct", EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT)
    limit_usd = guard.get("limit_usd", 0.0)
    source = guard.get("equity_source", "config")
    source_note = "Simulation/config" if source != "okx" else "OKX"
    if is_execution_paused():
        return (
            "ℹ️ <b>التنفيذ الآن:</b> متوقف مؤقتًا ⏸\n"
            "الإشارات ستظهر عادي، لكن التنفيذ لن يقبل صفقات حتى /resume_trading.\n"
            f"💰 رأس مال بداية اليوم: <b>{equity:.2f}$</b> ({html.escape(source_note)})\n"
            f"🛑 حد الإيقاف اليومي: <b>-{limit_pct:.0f}%</b> من رأس مال بداية اليوم ≈ <b>-{abs(limit_usd):.2f}$</b>\n"
            f"عدد الصفقات المسموح بها: <b>{max_trades}</b>"
        )
    return (
        "ℹ️ <b>التنفيذ الآن:</b> مفعّل ✅\n"
        f"💰 رأس مال بداية اليوم: <b>{equity:.2f}$</b> ({html.escape(source_note)})\n"
        f"🛑 حد الإيقاف اليومي: <b>-{limit_pct:.0f}%</b> من رأس مال بداية اليوم ≈ <b>-{abs(limit_usd):.2f}$</b>\n"
        f"عدد الصفقات المسموح بها: <b>{max_trades}</b>"
    )

def build_help_message() -> str:
 mode = _get_current_market_mode_for_help()
 mode_icon = _market_mode_icon_for_help(mode)
 execution_status = _get_execution_status_value_for_help()
 protection = _get_protection_status_for_help()
 return f"""🚀 <b>OKX Scanner</b>
<b>Trading Command Center</b>
━━━━━━━━━━━━

⚡ <b>Quick Access</b>
/mood
/report_execution
/open_trades
/help

━━━━━━━━━━━━
🧭 <b>System Status</b>

📈 Market Mode: {mode_icon} <code>{html.escape(mode)}</code>
⚡ Execution Engine: <code>{html.escape(execution_status)}</code>
🛡 Risk Protection: <code>{html.escape(protection)}</code>

━━━━━━━━━━━━
📝 <b>ملاحظات</b>
• الأزرار بالأسفل تفتح أقسام الأوامر مباشرة.
• /open_trades يعرض كل الصفقات المتابعة.
• /report_execution خاص بصفقات التنفيذ / المرشحة.
• أوامر OKX تعتمد على وضع التنفيذ الحالي."""

def build_help_inline_keyboard() -> dict:
 return {
    "inline_keyboard": [
        [
            {"text": "🚀 Execution", "callback_data": "help:execution"},
            {"text": "📊 Normal Trades", "callback_data": "help:normal"},
        ],
        [
            {"text": "🧠 Exec Intelligence", "callback_data": "help:exec_intelligence"},
            {"text": "🧠 Market Intelligence", "callback_data": "help:market_intelligence"},
        ],
        [
            {"text": "🧠 Diagnostics", "callback_data": "help:diagnostics"},
            {"text": "🤖 OKX Control", "callback_data": "help:okx"},
        ],
        [
            {"text": "⚙️ Admin", "callback_data": "help:admin"},
            {"text": "📘 System Info", "callback_data": "help:info"},
        ],
    ]
 }


def _get_current_market_mode_for_help() -> str:
 try:
    if r:
        return normalize_market_mode(r.get(MARKET_MODE_KEY) or MODE_NORMAL_LONG)
 except Exception:
    pass
 return MODE_NORMAL_LONG


def _market_mode_icon_for_help(mode: str) -> str:
    mode = normalize_market_mode(mode or MODE_NORMAL_LONG)
    if mode == MODE_NORMAL_LONG:
        return "🟢"
    if mode == MODE_STRONG_LONG_ONLY:
        return "🟡"
    if mode == MODE_BLOCK_LONGS:
        return "🔴"
    if mode == MODE_RECOVERY_LONG:
        return "🔵"
    return "⚪"


def _get_execution_status_value_for_help() -> str:
 try:
    paused = bool(r and r.exists(EXECUTION_PAUSE_KEY))
    return "PAUSED" if paused else "ACTIVE"
 except Exception:
    return "UNKNOWN"


def _get_protection_status_for_help() -> str:
 try:
    mode = _get_current_market_mode_for_help()
    if mode == MODE_BLOCK_LONGS:
        return "STRICT"
    return "ENABLED"
 except Exception:
    return "ENABLED"


def build_help_execution_message() -> str:
 return """🚀 <b>صفقات التنفيذ</b>
📘 <code>/help_execution</code>
━━━━━━━━━━━━

📊 <b>التقرير العام</b>
/report_execution
/report_execution_1h
/report_execution_today
/report_execution_7d

📂 <b>الصفقات المفتوحة</b>
/report_execution_open
/report_execution_open_1h
/report_execution_open_today
/report_execution_open_7d

📈 <b>تحليل أسباب الأرباح</b>
/report_execution_profit_analysis
/report_execution_profit_analysis_1h
/report_execution_profit_analysis_today
/report_execution_profit_analysis_7d

📉 <b>تحليل أسباب الخسائر</b>
/report_execution_losses_analysis
/report_execution_losses_analysis_1h
/report_execution_losses_analysis_today
/report_execution_losses_analysis_7d

⚙️ <b>أداء التنفيذ</b>
/report_execution_analysis
/report_execution_setups
/report_execution_exits

🧠 <b>تشخيص التنفيذ</b>
/report_execution_diagnostics"""

def build_help_normal_message() -> str:
 return """📊 <b>الصفقات العادية</b>
📘 <code>/help_normal</code>
━━━━━━━━━━━━

📊 <b>التقرير العام</b>
/report_all
/report_1h
/report_today
/report_7d

📂 <b>الصفقات المفتوحة</b>
/open_trades
/open_trades_1h
/open_trades_today
/open_trades_7d

📈 <b>تحليل أسباب الأرباح</b>
/report_profit_analysis
/report_profit_analysis_1h
/report_profit_analysis_today
/report_profit_analysis_7d

📉 <b>تحليل أسباب الخسائر</b>
/report_losses_analysis
/report_losses_analysis_1h
/report_losses_analysis_today
/report_losses_analysis_7d

⚙️ <b>أداء الصفقات</b>
/report_setups
/report_scores
/report_exits
/report_market"""

def build_help_exec_intelligence_message() -> str:
 return """🧠 <b>Exec Intelligence</b>
📘 <code>/help_exec_intelligence</code>
━━━━━━━━━━━━

🚀 <b>ذكاء صفقات التنفيذ</b>
/report_execution_intelligence
/report_execution_intelligence_1h
/report_execution_intelligence_today
/report_execution_intelligence_7d

ℹ️ يعرض أقوى Setups، التحذيرات، جودة الخروج، ومراجعة الفلاتر بالأرقام."""

def build_help_market_intelligence_message() -> str:
 return """🧠 <b>Market Intelligence</b>
📘 <code>/help_market_intelligence</code>
━━━━━━━━━━━━

📊 <b>ذكاء الصفقات العادية</b>
/report_intelligence
/report_intelligence_1h
/report_intelligence_today
/report_intelligence_7d

ℹ️ يعرض جودة السوق، أفضل الأنماط، وملاحظات تحسين الأداء من الصفقات العادية."""

def build_help_diagnostics_message() -> str:
 return """🧠 <b>التشخيص</b>
📘 <code>/diagnostics</code>
━━━━━━━━━━━━

/report_diagnostics
/report_rejections
/report_filters
/report_market_guard
/report_recovery

📊 <b>التحليل</b>
/report_deep
/report_market
/report_mode_history
/report_top_setups"""


def build_help_okx_message() -> str:
 return """🤖 <b>التنفيذ و OKX</b>
📘 <code>/okx_execution</code>
━━━━━━━━━━━━

📊 <b>حالة التنفيذ</b>
/execution_status — حالة محرك التنفيذ
/execution_mode — وضع التنفيذ الحالي
/positions — المراكز المفتوحة على OKX
/open_orders — الأوامر المفتوحة

⚙️ <b>التحكم</b>
/stop_trading — إيقاف فتح صفقات جديدة
/resume_trading — استئناف التنفيذ

🧹 <b>إدارة الصفقات</b>
/cancel_all — إلغاء كل الأوامر المفتوحة
/close_all — إغلاق كل المراكز المفتوحة ⚠️

🛡 <b>معلومات النظام</b>
/max_positions — الحد الأقصى للصفقات
/daily_dd — حد الخسارة اليومي"""

def build_help_admin_message() -> str:
 return """⚙️ <b>الإدارة</b>
<code>/admin_help</code>

/status
/ping
/restart
/reload
/clear_cache
/reset_reports
/hard_reset"""


def build_help_info_message() -> str:
 return """📘 <b>معلومات النظام</b>

/how_it_work
/version
/changelog
/about"""

def build_how_it_work_message() -> str:
 return """📘 <b>كيف يعمل بوت اللونج؟</b>

🧠 <b>الفكرة العامة:</b>
البوت يبحث عن فرص <b>Long Futures</b> على OKX، ويرسل إشارات متابعة عادية، ثم يرشّح جزءًا منها فقط للتنفيذ التجريبي حسب جودة إضافية ومخاطر التنفيذ.

📡 <b>1) مرحلة البحث والفلترة الأولية:</b>
• جلب أزواج USDT-SWAP النشطة.
• استبعاد الأزواج الضعيفة أو قليلة السيولة.
• تحليل فريم 15m مع سياق BTC والسوق العام.
• استخدام حالة السوق: NORMAL_LONG / STRONG_LONG_ONLY / BLOCK_LONGS / RECOVERY_LONG.

📊 <b>2) مرحلة جودة الإشارة العادية:</b>
كل عملة تحصل على Score من 10 بناءً على:
• Trend / Breakout / Reclaim / Continuation.
• Volume وCandle Strength.
• RSI وMACD.
• موقع السعر من المتوسط.
• تأكيد فريم 1H.
• قوة السوق وBTC.

🧠 <b>3) Entry Maturity — منع المطاردة:</b>
يفحص هل الدخول مناسب أم متأخر جدًا:
• هل السعر ممتد؟
• هل الحركة قرب موجة خامسة؟
• هل يوجد Pullback صحي؟
• هل الدخول مطاردة بعد Pump؟
النتيجة قد تكون: Reject أو Penalty أو Warning فقط لو السوق/setup قويين.

🎯 <b>4) Smart TP/SL:</b>
يبني خطة الصفقة منطقياً:
• Entry.
• TP1 / TP2.
• SL.
• RR.
• دعم/مقاومة/ATR.
لو الهدف غير مجزٍ أو الوقف غير منطقي، الإشارة قد ترفض قبل Telegram.

🧱 <b>5) near_resistance_before_tp1:</b>
يفحص هل توجد مقاومة قريبة قبل TP1.
• في السوق الضعيف: التشدد أعلى.
• في السوق القوي: بعض مقاومات swing_high أو bb_upper تتحول إلى Warning فقط.
• المقاومة المؤكدة جدًا أو القريبة جدًا تظل سبب رفض.

📩 <b>6) إرسال الإشارة العادية:</b>
لو الإشارة عدّت الفلاتر السابقة، تصل Telegram كإشارة متابعة.
مهم: الإشارة العادية لا تعني تنفيذ تلقائي.

🚀 <b>7) مرحلة الترشيح للتنفيذ:</b>
بعد وصول الإشارة، يتم فحصها مرة أخرى للتنفيذ:
• هل setup ضمن Execution Tags / Whitelist؟
• هل خطة Entry/SL/TP كاملة؟
• هل Weak Drift يسمح؟
• هل السعر لم يبتعد بشكل غير مناسب؟
لو نجحت، تظهر علامة <b>Execution Candidate</b>.

🛡️ <b>8) مرحلة التنفيذ والمخاطر:</b>
Risk Manager يفحص:
• هل التنفيذ مفعّل أم متوقف؟
• Daily DD Lock محسوب من Wallet Impact الحقيقي على رأس مال بداية اليوم، وليس من مجموع نسب الصفقات بعد الرافعة.
• في Simulation يتم استخدام رأس المال من الإعدادات، وفي Live يُقرأ رأس مال بداية اليوم من OKX لاحقًا.
• عدد الصفقات المسموح بها.
• نفس الزوج مفتوح أم لا.
• حدود المخاطرة.
ثم تكون النتيجة: accepted_preview / pending_pullback_preview / rejected_limit / rejected_risk / execution_paused.

📌 <b>معنى الحالات:</b>
• Signal = إشارة متابعة فقط.
• Execution Candidate = مؤهلة للتنفيذ بعد فلاتر إضافية.
• accepted_preview = مقبولة للتنفيذ التجريبي.
• pending_pullback_preview = انتظار Pullback.
• execution_paused = التنفيذ متوقف، لكن الإشارة محفوظة للتقرير.

🛡️ <b>حماية BLOCK_LONGS:</b>
عند دخول السوق وضع BLOCK_LONGS لا يتم تحريك الصفقات فورًا.
يتم تقييم <b>صفقات التنفيذ فقط</b> عند أول Market Reminder داخل BLOCK.
الحماية الحالية <b>Tracking فقط</b> ولا ترسل أمر SL مباشر إلى OKX حتى تفعيل ربط Live SL لاحقًا.

📈 <b>Track:</b>
يعرض لاحقًا حالة الصفقة، المرحلة، أقصى صعود/هبوط، ونتيجة TP/SL."""

def reset_stats(chat_id: str):
 if not r:
    send_telegram_reply(chat_id, "❌ Redis غير متصل")
    return
 if ADMIN_CHAT_IDS and str(chat_id) not in ADMIN_CHAT_IDS:
    send_telegram_reply(chat_id, f"⛔ غير مسموح\nchat_id={chat_id}")
    logger.warning(f"reset_stats blocked for non-admin chat_id={chat_id}")
    return
 try:
    deleted = 0
    for key in r.scan_iter("trade:futures:long:*"):
        try:
            r.delete(key)
            deleted += 1
        except Exception:
            pass
    try:
        r.delete("open_trades:futures:long")
    except Exception:
        pass
    try:
        r.delete("stats:futures:long")
    except Exception:
        pass
    reset_ts = int(time.time())
    r.set(STATS_RESET_TS_KEY, str(reset_ts))
    saved_reset = r.get(STATS_RESET_TS_KEY)
    send_telegram_reply(
        chat_id,
        f"🧹 تم تصفير إحصائيات اللونج بنجاح\n"
        f"📊 عدد مفاتيح الصفقات المحذوفة: {deleted}\n"
        f"🕒 وقت التصفير: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(reset_ts))}\n"
        f"📌 Trade History للـ Hybrid Label محفوظ ✅\n"
        f"✅ reset_key={saved_reset}"
    )
    logger.info(
        f"RESET LONG STATS ONLY → deleted={deleted} | reset_ts={reset_ts} | "
        f"saved={saved_reset} | trade_history=PRESERVED"
    )
 except Exception as e:
    logger.error(f"Reset stats error: {e}")
    send_telegram_reply(chat_id, f"❌ حصل خطأ أثناء التصفير\n{html.escape(str(e))}")

def stats_since_reset(chat_id: str):
 if not r:
    send_telegram_reply(chat_id, "❌ Redis غير متصل")
    return
 try:
    reset_ts_raw = r.get(STATS_RESET_TS_KEY)
    if not reset_ts_raw:
        send_telegram_reply(chat_id, "ℹ️ لا يوجد Reset مسجل بعد")
        return
    reset_ts = int(reset_ts_raw)
    reset_time_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reset_ts))
    summary = get_trade_summary(
        redis_client=r,
        market_type="futures",
        side="long",
        since_ts=reset_ts,
    )
    msg = f"📊 <b>إحصائيات اللونج منذ {reset_time_text}</b>\n\n"
    msg += format_period_summary("منذ آخر تصفير", summary)
    send_telegram_reply(chat_id, msg)
 except Exception as e:
    logger.error(f"stats_since_reset error: {e}")
    send_telegram_reply(chat_id, f"❌ حصل خطأ\n{html.escape(str(e))}")

def _safe_summary_field(summary: dict, field: str, default=0.0):
 try:
    return summary.get(field, default)
 except Exception:
    return default

def _format_pct(value, decimals=2) -> str:
 try:
    return f"{float(value):+.{decimals}f}%"
 except Exception:
    return "N/A"

def _format_wallet_impact(summary: dict, side="long") -> str:
 try:
    if estimate_wallet_pnl is None:
        return "Wallet impact: غير متاح"
    impact = estimate_wallet_pnl(summary, side=side)
    if impact is None:
        return "Wallet impact: غير متاح"
    if isinstance(impact, (list, tuple)) and len(impact) >= 1:
        pct = float(impact[0] or 0)
        return f"💰 <b>تقدير أثر المحفظة:</b> {_format_pct(pct)}"
    if isinstance(impact, dict):
        pct = float(impact.get("impact_pct") or impact.get("wallet_pnl_pct") or impact.get("estimated_wallet_pnl_pct") or 0)
        return f"💰 <b>تقدير أثر المحفظة:</b> {_format_pct(pct)}"
    return "Wallet impact: غير متاح"
 except Exception:
    return "Wallet impact: غير متاح"


# =======================================================
# CENTRALIZED REGISTRATION PAYLOAD BUILDER
# =======================================================
def build_trade_registration_payload(candidate: dict) -> dict:
    entry_mode = candidate.get("entry_mode", "market")
    pullback_triggered = candidate.get("pullback_triggered", False)
    
    return {
        "redis_client": r,
        "symbol": candidate["symbol"],
        "market_type": "futures",
        "side": "long",
        "candle_time": candidate["candle_time"],
        "timeframe": TIMEFRAME,
        "entry": candidate["entry"],
        "sl": candidate["sl"],
        "tp1": candidate["tp1"],
        "tp2": candidate["tp2"],
        "score": candidate["score"],
        "setup_type": candidate.get("setup_type", ""),
        "reasons": candidate.get("reasons", []),
        "warning_reasons": candidate.get("warning_reasons", []),
        "btc_mode": candidate.get("btc_mode", ""),
        "funding_label": candidate.get("funding_label", "🟡 محايد"),
        "pre_breakout": candidate.get("pre_breakout", False),
        "breakout": candidate.get("breakout", False),
        "vol_ratio": candidate.get("vol_ratio", 1.0),
        "candle_strength": candidate.get("candle_strength", 0.0),
        "mtf_confirmed": candidate.get("mtf_confirmed", False),
        "is_new": candidate.get("is_new", False),
        "btc_dominance_proxy": candidate.get("btc_dominance_proxy", ""),
        "change_24h": candidate.get("change_24h", 0.0),
        "raw_score": candidate.get("raw_score", 0.0),
        "effective_score": candidate.get("score", 0.0),
        "dynamic_threshold": candidate.get("dynamic_threshold", 0.0),
        "required_min_score": candidate.get("required_min_score", 0.0),
        "final_threshold": candidate.get("final_threshold", 0.0),
        "dist_ma": candidate.get("dist_ma", 0.0),
        "entry_timing": candidate.get("entry_timing", ""),
        "opportunity_type": candidate.get("opportunity_type", ""),
        "market_state": candidate.get("market_state", ""),
        "market_state_label": candidate.get("market_state_label", ""),
        "market_bias_label": candidate.get("market_bias_label", ""),
        "alt_mode": candidate.get("alt_mode", ""),
        "early_priority": candidate.get("early_priority", "none"),
        "breakout_quality": candidate.get("breakout_quality", "none"),
        "risk_level": candidate.get("risk_level", ""),
        "fake_signal": candidate.get("fake_signal", False),
        "is_reverse_signal": candidate.get("is_reverse", False),
        "reversal_4h_confirmed": candidate.get("reversal_4h_confirmed", False),
        "rank_volume_24h": candidate.get("rank_volume_24h", 0.0),
        "alert_id": candidate.get("alert_id", ""),
        "has_high_impact_news": candidate.get("has_high_impact_news", False),
        "news_titles": candidate.get("news_titles", []),
        "warning_penalty": candidate.get("warning_penalty", 0.0),
        "warning_high_count": candidate.get("warning_high_count", 0),
        "warning_medium_count": candidate.get("warning_medium_count", 0),
        "warning_penalty_details": candidate.get("warning_penalty_details", []),
        "adjustments_log": candidate.get("adjustments_log", []),
        "pullback_entry": candidate.get("pullback_entry"),
        "pullback_low": candidate.get("pullback_low"),
        "pullback_high": candidate.get("pullback_high"),
        "rr1": candidate.get("rr1", 2.0),
        "rr2": candidate.get("rr2", 3.2),
        "setup_type_base": candidate.get("setup_type_base", ""),
        "fib_position": candidate.get("fib_position", "unknown"),
        "fib_position_ratio": candidate.get("fib_position_ratio", 0.0),
        "fib_label": candidate.get("fib_label", ""),
        "had_pullback": candidate.get("had_pullback", False),
        "pullback_pct": candidate.get("pullback_pct", 0.0),
        "pullback_label": candidate.get("pullback_label", ""),
        "wave_estimate": candidate.get("wave_estimate", 0),
        "wave_peaks": candidate.get("wave_peaks", 0),
        "wave_label": candidate.get("wave_label", ""),
        "entry_maturity": candidate.get("entry_maturity", "unknown"),
        "maturity_penalty": candidate.get("maturity_penalty", 0.0),
        "maturity_bonus": candidate.get("maturity_bonus", 0.0),
        "falling_knife_risk": candidate.get("falling_knife_risk", False),
        "falling_knife_reasons": candidate.get("falling_knife_reasons", []),
        "reversal_quality": candidate.get("reversal_quality", ""),
        "wave_context": candidate.get("wave_context", ""),
        "setup_context": candidate.get("setup_context", ""),
        "reversal_structure_confirmed": candidate.get("reversal_structure_confirmed", False),
        "strong_bull_pullback": candidate.get("strong_bull_pullback", False),
        "strong_breakout_exception": candidate.get("strong_breakout_exception", False),
        "target_method": candidate.get("target_method", "rr"),
        "nearest_resistance": candidate.get("nearest_resistance"),
        "nearest_support": candidate.get("nearest_support"),
        "resistance_warning": candidate.get("resistance_warning", ""),
        "support_warning": candidate.get("support_warning", ""),
        "target_notes": candidate.get("target_notes", []),
        "sl_method": candidate.get("sl_method", "atr"),
        "sl_notes": candidate.get("sl_notes", []),
        "tp1_close_pct": candidate.get("tp1_close_pct", TP1_CLOSE_PCT),
        "tp2_close_pct": candidate.get("tp2_close_pct", TP2_CLOSE_PCT),
        "trailing_pct": candidate.get("trailing_pct", TRAILING_PCT),
        "trailing_position_pct": candidate.get("trailing_position_pct", TRAILING_POSITION_PCT),
        "move_sl_to_entry_after_tp1": candidate.get("move_sl_to_entry_after_tp1", MOVE_SL_TO_ENTRY_AFTER_TP1),
        "has_extra_strong_setup": candidate.get("has_extra_strong_setup", False),
        "extra_setup_names": candidate.get("extra_setup_names", []),
        "extra_setup_bonus": candidate.get("extra_setup_bonus", 0.0),
        "primary_extra_setup": candidate.get("primary_extra_setup", ""),
        "extra_setups_details": candidate.get("extra_setups_details", {}),
        "has_pullback_plan": candidate.get("has_pullback_plan", False),
        "entry_mode": entry_mode,
        "pullback_triggered": pullback_triggered,
        "market_entry": candidate.get("market_entry", None),
        "recommended_entry": candidate.get("recommended_entry", None),
        "execution_entry": candidate.get("execution_entry"),
        "execution_sl": candidate.get("execution_sl"),
        "execution_tp1": candidate.get("execution_tp1"),
        "execution_tp2": candidate.get("execution_tp2"),
        "execution_candidate_badged": bool(candidate.get("execution_candidate_badged", False)),
        "execution_candidate_badge_sent": bool(candidate.get("execution_candidate_badge_sent", candidate.get("execution_candidate_badged", False))),
        "execution_status": candidate.get("execution_status", "candidate_only" if candidate.get("execution_candidate_badged") else "not_candidate"),
        "execution_reject_reason": candidate.get("execution_reject_reason", ""),
        "execution_message_sent": candidate.get("execution_message_sent", False),
        "execution_result_status": candidate.get("execution_result_status", ""),
    }


def register_trade_from_candidate(candidate: dict) -> bool:
    try:
        payload = build_trade_registration_payload(candidate)
        result = register_trade(**payload)
        return interpret_register_trade_result(result, candidate)
    except Exception as e:
        error_msg = (
            f"register_trade failed exception | symbol={candidate.get('symbol', '?')} | "
            f"alert_id={candidate.get('alert_id', '?')} | setup={candidate.get('setup_type', '?')} | "
            f"current_mode={candidate.get('current_mode', '?')} | error={e}"
        )
        logger.error(error_msg)
        return False

def interpret_register_trade_result(result, candidate: dict) -> bool:
    symbol = candidate.get('symbol', '?')
    alert_id = candidate.get('alert_id', '?')
    setup = candidate.get('setup_type', '?')
    current_mode = candidate.get('current_mode', '?')
    if result is True:
        logger.info(f"register_trade success_true | symbol={symbol} | alert_id={alert_id} | setup={setup} | current_mode={current_mode}")
        return True
    if result is False:
        logger.error(f"register_trade failed_false | symbol={symbol} | alert_id={alert_id} | setup={setup} | current_mode={current_mode}")
        return False
    if result is None:
        logger.error(f"register_trade returned None -> treated as failure | symbol={symbol} | alert_id={alert_id} | setup={setup} | current_mode={current_mode}")
        return False
    logger.error(f"register_trade unexpected_result={result!r} | symbol={symbol} | alert_id={alert_id} | setup={setup} | current_mode={current_mode}")
    return False


# ----------- Helper functions for exits report -----------
def _load_long_trades_from_redis(limit: int = 700) -> list:
    trades = []
    if not r:
        return trades
    try:
        trade_count = 0
        history_count = 0
        seen_ids = set()

        for key in r.scan_iter("trade:futures:long:*"):
            try:
                raw = r.get(key)
                if not raw:
                    continue
                data = json.loads(raw)
                if not isinstance(data, dict):
                    continue
                uid = data.get("alert_id") or f"{data.get('symbol','')}:{data.get('candle_time','')}"
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)
                data["_redis_key"] = key
                trades.append(data)
                trade_count += 1
            except Exception:
                continue

        for key in r.scan_iter("trade_history:futures:long:*"):
            try:
                raw = r.get(key)
                if not raw:
                    continue
                data = json.loads(raw)
                if not isinstance(data, dict):
                    continue
                uid = data.get("alert_id") or f"{data.get('symbol','')}:{data.get('candle_time','')}"
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)
                data["_redis_key"] = key
                trades.append(data)
                history_count += 1
            except Exception:
                continue

        logger.info(f"Loaded trades: trade={trade_count} history={history_count} after dedupe={len(trades)}")
    except Exception as e:
        logger.error(f"_load_long_trades_from_redis error: {e}")
        return trades

    trades.sort(
        key=lambda x: int(float(x.get("created_ts") or x.get("candle_time") or 0)),
        reverse=True
    )
    return trades[:limit]


def _pct_safe(value, decimals=2):
    try:
        return f"{float(value):+.{decimals}f}%"
    except Exception:
        return "N/A"


def _avg(values):
    try:
        values = [float(v) for v in values if v is not None]
        if not values:
            return 0.0
        return sum(values) / len(values)
    except Exception:
        return 0.0


def _trade_exit_bucket(trade: dict) -> str:
    try:
        status = str(trade.get("status", "") or "").lower()
        result = str(trade.get("result", "") or "").lower()

        if bool(trade.get("protected_breakeven_exit", False)):
            return "breakeven_protected"
        if result == "tp2_win":
            return "tp2"
        if result == "trailing_win":
            return "trailing"
        if result == "tp1_win":
            return "tp1_only"
        if result == "loss":
            return "loss"
        if result == "expired":
            return "expired"
        if status == "trailing":
            return "trailing_open"
        if status == "partial":
            return "partial"
        if status == "open":
            return "open"
        if bool(trade.get("tp1_hit", False)):
            return "tp1_hit_open_or_unknown"
        return "unknown"
    except Exception:
        return "unknown"


def _get_trade_pnl_pct(trade: dict) -> float:
    from tracking.summary_helpers import calc_trade_result_pct
    raw = calc_trade_result_pct(trade)
    if raw is None:
        return 0.0
    return round(raw * TRACK_LEVERAGE, 4)


def _is_trade_win_bucket(bucket: str) -> bool:
    return bucket in ("tp2", "trailing", "tp1_only", "partial", "breakeven_protected", "tp1_hit_open_or_unknown")


def _build_group_exit_stats(trades: list, group_field: str, max_items: int = 6) -> dict:
    from collections import defaultdict
    groups = defaultdict(list)
    for t in trades:
        bucket = _trade_exit_bucket(t)
        if bucket in ("open", "unknown"):
            continue
        key = str(t.get(group_field, "unknown"))[:60]
        groups[key].append((t, bucket))
    stats = {}
    for key, items in groups.items():
        total = len(items)
        wins = sum(1 for _, b in items if _is_trade_win_bucket(b))
        losses = sum(1 for _, b in items if b == "loss")
        expired = sum(1 for _, b in items if b == "expired")
        tp1_count = sum(1 for _, b in items if b in ("tp2", "tp1_only", "partial", "breakeven_protected", "tp1_hit_open_or_unknown"))
        tp2_count = sum(1 for _, b in items if b == "tp2")
        breakeven_count = sum(1 for _, b in items if b == "breakeven_protected")
        winrate = (wins / total * 100) if total else 0.0
        tp1_rate = (tp1_count / total * 100) if total else 0.0
        tp2_rate = (tp2_count / total * 100) if total else 0.0
        pnls = [_get_trade_pnl_pct(t) for t, _ in items]
        avg_pnl = _avg(pnls) if pnls else 0.0
        stats[key] = {
            "total": total,
            "wins": wins,
            "losses": losses,
            "expired": expired,
            "tp1_count": tp1_count,
            "tp2_count": tp2_count,
            "breakeven_count": breakeven_count,
            "winrate": winrate,
            "tp1_rate": tp1_rate,
            "tp2_rate": tp2_rate,
            "avg_pnl": avg_pnl,
        }
    sorted_items = sorted(stats.items(), key=lambda x: x[1]["total"], reverse=True)[:max_items]
    return dict(sorted_items)


def _format_group_exit_stats(title: str, stats: dict) -> str:
    lines = [f"<b>{title}</b>"]
    for key, info in stats.items():
        lines.append(
            f"• {html.escape(str(key))} | total={info['total']} | "
            f"winrate={info['winrate']:.1f}% | "
            f"TP1={info['tp1_count']}/{info['tp1_rate']:.1f}% | "
            f"TP2={info['tp2_count']}/{info['tp2_rate']:.1f}% | "
            f"SL={info['losses']} | Exp={info['expired']} | "
            f"AvgPnL={_pct_safe(info['avg_pnl'])}"
        )
    return "\n".join(lines)


def _format_last_closed_trades(trades: list, max_items: int = 8) -> str:
    lines = []
    closed = [
        t for t in trades
        if _trade_exit_bucket(t) not in ("open", "unknown")
    ][:max_items]

    if not closed:
        return "• لا توجد صفقات مغلقة كافية بعد"

    for t in closed:
        symbol = clean_symbol_for_message(str(t.get("symbol", "UNKNOWN")))
        bucket = _trade_exit_bucket(t)
        score = _safe_float(t.get("score"), 0.0)
        setup_type = str(t.get("setup_type", "unknown"))
        entry_timing = str(t.get("entry_timing", ""))

        pnl = _get_trade_pnl_pct(t)

        label = {
            "tp2": "🎯 TP2",
            "trailing": "🔄 Trailing Win",
            "trailing_open": "🔄 Trailing مفعّل",
            "tp1_only": "✅ TP1 فقط",
            "loss": "❌ SL",
            "expired": "⏳ Expired",
            "partial": "✅ Partial",
            "breakeven_protected": "🛡 Breakeven",
            "tp1_hit_open_or_unknown": "✅ TP1 Hit",
        }.get(bucket, bucket)

        lines.append(
            f"• {html.escape(symbol)} | {label} | Score {score:.2f} | "
            f"{_pct_safe(pnl)} | {html.escape(setup_type[:45])} | {html.escape(entry_timing[:25])}"
        )

    return "\n".join(lines)


def build_exits_report_message() -> str:
    try:
        trades = _load_long_trades_from_redis()
        if not trades:
            return "ℹ️ لا توجد بيانات بعد"

        total = len(trades)
        open_trades = [t for t in trades if _trade_exit_bucket(t) == "open"]
        closed = [t for t in trades if _trade_exit_bucket(t) not in ("open", "unknown")]
        tp2_wins = [t for t in closed if _trade_exit_bucket(t) == "tp2"]
        tp1_only = [t for t in closed if _trade_exit_bucket(t) == "tp1_only"]
        partial = [t for t in closed if _trade_exit_bucket(t) == "partial"]
        breakeven = [t for t in closed if _trade_exit_bucket(t) == "breakeven_protected"]
        losses = [t for t in closed if _trade_exit_bucket(t) == "loss"]
        expired = [t for t in closed if _trade_exit_bucket(t) == "expired"]
        tp1_hit_other = [t for t in closed if _trade_exit_bucket(t) == "tp1_hit_open_or_unknown"]

        closed_count = len(closed)
        tp2_wins_list = [t for t in closed if _trade_exit_bucket(t) == "tp2"]
        trailing_wins_list = [t for t in closed if _trade_exit_bucket(t) == "trailing"]
        trailing_open_list = [t for t in trades if _trade_exit_bucket(t) == "trailing_open"]
        tp1_effective = len(tp2_wins) + len(tp1_only) + len(partial) + len(breakeven) + len(tp1_hit_other) + len(trailing_wins_list)
        tp2_effective = len(tp2_wins) + len(trailing_wins_list)
        sl_count = len(losses)
        expired_count = len(expired)

        tp1_rate = (tp1_effective / closed_count * 100) if closed_count else 0
        tp2_rate = (tp2_effective / closed_count * 100) if closed_count else 0
        sl_rate = (sl_count / closed_count * 100) if closed_count else 0

        tp1_to_tp2 = (tp2_effective / tp1_effective * 100) if tp1_effective else 0

        all_pnls = [_get_trade_pnl_pct(t) for t in closed]
        avg_pnl = _avg(all_pnls) if all_pnls else 0.0
        win_pnls = [p for i, p in enumerate(all_pnls) if _is_trade_win_bucket(_trade_exit_bucket(closed[i]))]
        loss_pnls = [p for i, p in enumerate(all_pnls) if _trade_exit_bucket(closed[i]) == "loss"]
        avg_win = _avg(win_pnls) if win_pnls else 0.0
        avg_loss = _avg(loss_pnls) if loss_pnls else 0.0

        setup_stats = _build_group_exit_stats(closed, "setup_type", max_items=6)
        timing_stats = _build_group_exit_stats(closed, "entry_timing", max_items=6)

        trailing_block = ""
        if trailing_open_list or trailing_wins_list:
            trailing_lines = [f"\n<b>🔄 Trailing (20% مفتوح):</b>"]
            if trailing_open_list:
                for t in trailing_open_list[:5]:
                    sym = clean_symbol_for_message(t.get("symbol", ""))
                    t_high = _safe_float(t.get("trailing_high"), 0.0)
                    t_sl = _safe_float(t.get("trailing_sl"), 0.0)
                    entry_p = _safe_float(t.get("entry"), 0.0)
                    gain = ((t_high - entry_p) / entry_p * 100) if entry_p > 0 and t_high > 0 else 0.0
                    trailing_lines.append(
                        f"• {html.escape(sym)} | 📈 High: {fmt_num(t_high, 6)} "
                        f"(+{gain:.2f}%) | 🛑 Trailing SL: {fmt_num(t_sl, 6)}"
                    )
            if trailing_wins_list:
                trailing_lines.append(f"• ✅ مغلق بـ trailing: {len(trailing_wins_list)}")
            trailing_block = "\n".join(trailing_lines)

        lines = [
            "📊 <b>تقرير جودة الخروج الشامل - LONG</b>",
            "",
            f"• إجمالي الصفقات: {total}",
            f"• مفتوح: {len(open_trades)}",
            f"• 🔄 Trailing مفعّل: {len(trailing_open_list)}",
            f"• مغلق: {closed_count}",
            f"• 🎯 TP2: {len(tp2_wins_list)}",
            f"• 🔄 Trailing Win: {len(trailing_wins_list)}",
            f"• ✅ TP1 فقط: {len(tp1_only)}",
            f"• ✅ Partial: {len(partial)}",
            f"• 🛡 Breakeven: {len(breakeven)}",
            f"• ❌ SL: {sl_count}",
            f"• ⏳ Expired: {expired_count}",
            "",
            "<b>📈 نسب:</b>",
            f"• TP1 Rate: {tp1_rate:.1f}%",
            f"• TP2 Rate: {tp2_rate:.1f}%",
            f"• TP1 → TP2: {tp1_to_tp2:.1f}%",
            f"• SL Rate: {sl_rate:.1f}%",
            "",
            "<b>💰 مالي:</b>",
            f"• Avg PnL: {_pct_safe(avg_pnl)}",
            f"• Avg Win: {_pct_safe(avg_win)}",
            f"• Avg Loss: {_pct_safe(avg_loss)}",
            trailing_block,
            "",
            _format_group_exit_stats("📌 أداء setup_type", setup_stats),
            "",
            _format_group_exit_stats("📍 أداء entry_timing", timing_stats),
            "",
            "<b>🔹 آخر الصفقات المغلقة:</b>",
            _format_last_closed_trades(trades),
        ]

        return _limit_telegram_message("\n".join(lines))

    except Exception as e:
        logger.exception(f"build_exits_report_message error: {e}")
        return "❌ حصل خطأ أثناء بناء تقرير الخروج"


def build_daily_report_message() -> str:
 try:
    summary = get_trade_summary(
        redis_client=r,
        market_type="futures",
        side="long",
        since_ts=get_local_day_start_ts(),
    )
 except Exception as e:
    logger.error(f"build_daily_report_message error: {e}")
    return "❌ حصل خطأ أثناء بناء التقرير اليومي"
 if not summary:
    return "ℹ️ لا توجد بيانات لليوم الحالي"
 signals = int(_safe_summary_field(summary, "signals", 0))
 closed = int(_safe_summary_field(summary, "closed", 0))
 wins = int(_safe_summary_field(summary, "wins", 0))
 tp1_wins = int(_safe_summary_field(summary, "tp1_wins", 0))
 tp2_wins = int(_safe_summary_field(summary, "tp2_wins", 0))
 losses = int(_safe_summary_field(summary, "losses", 0))
 expired = int(_safe_summary_field(summary, "expired", 0))
 open_ = int(_safe_summary_field(summary, "open", 0))
 tp1_hits = int(_safe_summary_field(summary, "tp1_hits", tp1_wins + tp2_wins))
 tp2_hits = int(_safe_summary_field(summary, "tp2_hits", tp2_wins))
 winrate = float(_safe_summary_field(summary, "winrate", 0.0))
 tp1_rate = float(_safe_summary_field(summary, "tp1_rate", 0.0))
 tp2_rate = float(_safe_summary_field(summary, "tp2_rate", 0.0))
 tp1_to_tp2_rate = round((tp2_hits / tp1_hits) * 100, 2) if tp1_hits > 0 else 0.0
 pnl_pct = float(_safe_summary_field(
    summary,
    "realized_leveraged_pnl_pct",
    summary.get("realized_pnl_pct", 0.0)
 ))
 raw_pnl = float(_safe_summary_field(summary, "realized_raw_pnl_pct", 0.0))
 wallet_line = _format_wallet_impact(summary, side="long")
 lines = [
    "<b>📅 Daily Performance - LONG</b>",
    "",
    f"Signals: {signals}",
    f"Closed: {closed}",
    f"Wins: {wins}",
    f"TP2: {tp2_wins}",
    f"TP1 Only: {tp1_wins}",
    f"Losses: {losses}",
    f"Expired: {expired}",
    f"Open: {open_}",
    f"Win rate: {winrate:.1f}%",
    "",
    "🎯 <b>جودة الخروج:</b>",
    f"• TP1 Hits: {tp1_hits}",
    f"• TP2 Hits: {tp2_hits}",
    f"• TP1 Rate: {tp1_rate:.1f}%",
    f"• TP2 Rate: {tp2_rate:.1f}%",
    f"• TP1 → TP2 Rate: {tp1_to_tp2_rate:.1f}%",
    "",
    "💰 <b>النتيجة المالية:</b>",
    f"• Net after leverage: {_format_pct(pnl_pct)}",
    f"• Raw price move: {_format_pct(raw_pnl)}",
    f"• {wallet_line}",
 ]
 return "\n".join(lines)

def build_7d_report_message() -> str:
 try:
    since_ts = int(time.time()) - 7 * 86400
    summary = get_trade_summary(
        redis_client=r,
        market_type="futures",
        side="long",
        since_ts=since_ts,
    )
    if not summary:
        return "ℹ️ لا توجد بيانات لآخر 7 أيام"
    signals = int(_safe_summary_field(summary, "signals", 0))
    closed = int(_safe_summary_field(summary, "closed", 0))
    wins = int(_safe_summary_field(summary, "wins", 0))
    losses = int(_safe_summary_field(summary, "losses", 0))
    open_ = int(_safe_summary_field(summary, "open", 0))
    tp1_hits = int(_safe_summary_field(summary, "tp1_hits", 0))
    tp2_hits = int(_safe_summary_field(summary, "tp2_hits", 0))
    tp1_rate = float(_safe_summary_field(summary, "tp1_rate", 0.0))
    tp2_rate = float(_safe_summary_field(summary, "tp2_rate", 0.0))
    tp1_to_tp2_rate = round((tp2_hits / tp1_hits) * 100, 2) if tp1_hits > 0 else 0.0
    winrate = float(_safe_summary_field(summary, "winrate", 0.0))
    pnl_pct = float(_safe_summary_field(summary, "realized_leveraged_pnl_pct", 0.0))
    avg_win = float(_safe_summary_field(summary, "avg_win_pct", 0.0))
    avg_loss = float(_safe_summary_field(summary, "avg_loss_pct", 0.0))
    best = float(_safe_summary_field(summary, "best_trade_pct", 0.0))
    worst = float(_safe_summary_field(summary, "worst_trade_pct", 0.0))
    wallet_line = _format_wallet_impact(summary, side="long")
    lines = [
        "📅 <b>7-Day Performance - LONG</b>",
        "",
        f"Signals: {signals}",
        f"Closed: {closed}",
        f"Open: {open_}",
        f"Wins: {wins}",
        f"Losses: {losses}",
        f"Win rate: {winrate:.1f}%",
        "",
        "🎯 <b>أداء الأهداف:</b>",
        f"• TP1 Hits: {tp1_hits}",
        f"• TP2 Hits: {tp2_hits}",
        f"• TP1 Rate: {tp1_rate:.1f}%",
        f"• TP2 Rate: {tp2_rate:.1f}%",
        f"• TP1 → TP2 Rate: {tp1_to_tp2_rate:.1f}%",
        "",
        "💰 <b>النتيجة المالية:</b>",
        f"• Net after leverage: {_format_pct(pnl_pct)}",
        f"• {wallet_line}",
        f"• Avg win: {_format_pct(avg_win)}",
        f"• Avg loss: {_format_pct(avg_loss)}",
        f"• Best trade: {_format_pct(best)}",
        f"• Worst trade: {_format_pct(worst)}",
    ]
    return "\n".join(lines)
 except Exception as e:
    logger.error(f"build_7d_report_message error: {e}")
    return "❌ حصل خطأ أثناء بناء تقرير 7 أيام"

# =========================
# MARKET STATUS SNAPSHOT FUNCTIONS
# =========================
def save_market_status_snapshot(snapshot: dict) -> None:
 if not r or not snapshot:
    return
 try:
    r.set(MARKET_STATUS_SNAPSHOT_KEY, json.dumps(snapshot, ensure_ascii=False), ex=MARKET_STATUS_SNAPSHOT_TTL)
 except Exception as e:
    logger.warning(f"Failed to save market status snapshot: {e}")

def load_market_status_snapshot(max_age_seconds: int = 240):
 if not r:
    return None
 try:
    raw = r.get(MARKET_STATUS_SNAPSHOT_KEY)
    if not raw:
        return None
    snapshot = json.loads(raw)
    if not isinstance(snapshot, dict):
        return None
    created_ts = int(snapshot.get("created_ts", 0))
    if created_ts <= 0:
        return None
    age = int(time.time()) - created_ts
    if age > max_age_seconds:
        return None
    return snapshot
 except Exception as e:
    logger.warning(f"Failed to load market status snapshot: {e}")
    return None

# =========================
# MARKET MODE ARABIC DESCRIPTION
# =========================
def get_market_mode_arabic_description(mode: str) -> str:
    mode = normalize_market_mode(mode)
    mapping = {
        MODE_NORMAL_LONG: "السوق طبيعي، الإشارات العادية مسموحة حسب الفلاتر.",
        MODE_STRONG_LONG_ONLY: "السوق فيه ضعف أو تذبذب نسبي، لذلك يتم التركيز على الفرص الأقوى فقط.",
        MODE_BLOCK_LONGS: "السوق تحت ضغط أو هبوط جماعي، لذلك يتم تفعيل وضع الحماية وتشديد الدخول.",
        MODE_RECOVERY_LONG: "السوق يحاول التعافي بعد ضغط، لذلك يسمح بفرص Recovery محدودة وبحذر.",
    }
    return mapping.get(mode, "وضع غير معروف")


def get_market_mode_reason_text(mode: str, suggested_reason: str = "") -> str:
    mode = normalize_market_mode(mode)
    if suggested_reason:
        reason = str(suggested_reason)
        # Keep user-facing text clean while preserving the calculated reason.
        if reason in ("السوق ضعيف/مختلط لكن ليس كراش", "market weak/mixed but not crash"):
            return "السوق متماسك لكن الزخم غير كافي لفتح التنفيذ بحرية كاملة."
        if reason in ("السوق طبيعي", "normal market"):
            return "السوق مستقر بما يكفي للسماح بالإشارات العادية مع فلاتر الجودة."
        if "block" in reason.lower() or "كراش" in reason or "ضغط" in reason:
            return "السوق يظهر ضغط واضح، لذلك يتم تشديد الدخول وحماية الصفقات المفتوحة."
    mapping = {
        MODE_NORMAL_LONG: "السوق مستقر بما يكفي للسماح بالإشارات العادية مع فلاتر الجودة.",
        MODE_STRONG_LONG_ONLY: "السوق متماسك لكن الزخم غير كافي لفتح التنفيذ بحرية كاملة.",
        MODE_BLOCK_LONGS: "السوق يظهر ضغط واضح، لذلك يتم تشديد الدخول وحماية الصفقات المفتوحة.",
        MODE_RECOVERY_LONG: "السوق في مرحلة تعافي محتملة بعد ضغط، ويحتاج تأكيد أقوى قبل التنفيذ.",
    }
    return mapping.get(mode, str(suggested_reason or "لا يوجد سبب واضح"))


def get_market_mode_allowed_lines(mode: str) -> list:
    mode = normalize_market_mode(mode)
    if mode == MODE_NORMAL_LONG:
        return [
            "• الإشارات العادية مسموحة حسب الفلاتر",
            "• التنفيذ التجريبي يخضع للـ Whitelist + Quality Filters",
            "• Weak Drift يمنع التنفيذ الضعيف فقط ولا يمنع الإشارة",
        ]
    if mode == MODE_STRONG_LONG_ONLY:
        return [
            f"• Score ≥ {STRONG_ONLY_MIN_SCORE}",
            f"• Volume ≥ {STRONG_ONLY_MIN_VOL_RATIO}",
            "• تأكيد فريم الساعة مفضل",
            "• تجنب المطاردة والدخول المتأخر",
        ]
    if mode == MODE_BLOCK_LONGS:
        return [
            "• الإشارات العادية ممنوعة أو مشددة جدًا",
            "• التنفيذ فقط لاستثناءات قوية جدًا حسب قواعد التنفيذ",
            "• حماية الصفقات المفتوحة الرابحة إن وجدت",
            "• Weak Drift ثانوي لأن BLOCK أصلاً متشدد",
        ]
    if mode == MODE_RECOVERY_LONG:
        return [
            "• فرص Recovery محدودة وبحذر",
            "• تأكيد التعافي مطلوب",
            "• التنفيذ: Recovery confirmed + Whitelist",
        ]
    return ["• مراقبة السوق قبل الدخول"]

# =========================
# MARKET STATUS MESSAGE
# =========================
def build_market_status_message() -> str:
 try:
    snapshot = load_market_status_snapshot(max_age_seconds=300)
    if snapshot:
        current_mode = normalize_market_mode(snapshot.get("current_mode", MODE_NORMAL_LONG))
        mode_reason = snapshot.get("mode_reason", "")
        btc_mode = snapshot.get("btc_mode", "🟡 محايد")
        alt_snapshot = snapshot.get("alt_snapshot", {}) or {}
        alt_mode = alt_snapshot.get("alt_mode", "🟡 محايد")
        market_info = snapshot.get("market_info", {}) or {}
        market_state_label = market_info.get("market_state_label", "Mixed")
        market_bias_label = market_info.get("market_bias_label", "السوق مختلط")
        market_guard = snapshot.get("market_guard", {}) or {}
        red_ratio = float(market_guard.get("red_ratio_15m", 0.0) or 0.0)
        avg_change = float(market_guard.get("avg_change_15m", 0.0) or 0.0)
        btc_change = float(market_guard.get("btc_change_15m", 0.0) or 0.0)
        guard_level = str(market_guard.get("level", "normal"))
        suggested_mode = normalize_market_mode(snapshot.get("suggested_mode", snapshot.get("current_mode", MODE_NORMAL_LONG)))
        suggested_reason = snapshot.get("suggested_reason", mode_reason)
    else:
        current_mode = normalize_market_mode(r.get(MARKET_MODE_KEY) if r else MODE_NORMAL_LONG)
        if not current_mode:
            current_mode = MODE_NORMAL_LONG
        btc_mode = get_btc_mode()
        ranked_pairs = get_ranked_pairs()
        alt_snapshot = get_alt_market_snapshot(ranked_pairs)
        alt_mode = alt_snapshot.get("alt_mode", "🟡 محايد")
        market_info = get_market_state(btc_mode, alt_snapshot)
        market_state_label = market_info.get("market_state_label", "Mixed")
        market_bias_label = market_info.get("market_bias_label", "السوق مختلط")
        btc_zone = get_btc_range_zone(timeframe="1H", lookback=50)
        market_guard = get_market_guard_snapshot(ranked_pairs, btc_mode, alt_snapshot, btc_zone=btc_zone)
        red_ratio = float(market_guard.get("red_ratio_15m", 0.0) or 0.0)
        avg_change = float(market_guard.get("avg_change_15m", 0.0) or 0.0)
        btc_change = float(market_guard.get("btc_change_15m", 0.0) or 0.0)
        guard_level = str(market_guard.get("level", "normal"))
        mode_result = determine_long_market_mode(
            market_guard=market_guard,
            market_state=market_info.get("market_state", "mixed"),
            btc_mode=btc_mode,
            alt_snapshot=alt_snapshot,
            current_mode=current_mode,
            allow_state_writes=False,
        )
        suggested_mode = mode_result.get("mode", current_mode)
        suggested_reason = mode_result.get("reason", "")

    mode_ar = _market_mode_label(current_mode)
    mode_icon = str(mode_ar).split(" ", 1)[0] if mode_ar else "🧭"
    suggested_mode_ar = _market_mode_label(suggested_mode)
    mode_desc = get_market_mode_arabic_description(current_mode)
    reason_text = get_market_mode_reason_text(current_mode, suggested_reason)
    action = get_market_mode_action_text(current_mode)
    allowed_lines = get_market_mode_allowed_lines(current_mode)

    last_mode = MODE_NORMAL_LONG
    try:
        if r:
            last_mode = normalize_market_mode(r.get(MARKET_MODE_LAST_KEY) or current_mode)
    except Exception:
        last_mode = current_mode
    transition = f"{_market_mode_label(last_mode)} → {mode_ar}" if last_mode != current_mode else f"{mode_ar}"

    lines = [
        f"{mode_icon} <b>Market Mood - {current_mode}</b>",
        "",
        f"⚙️ <b>المود الحالي:</b> {mode_ar}",
        f"📋 <b>الوصف:</b> {html.escape(mode_desc)}",
        "",
        f"🔄 <b>الانتقال:</b> {transition}",
        "",
        f"🧠 <b>السبب:</b> {html.escape(reason_text)}",
        "",
        "🧪 <b>Weak Drift Block:</b>",
        f"{html.escape(get_weak_drift_display_status(current_mode, btc_mode, market_state_label, alt_mode, market_bias_label).get('label', '🟢 Weak Drift: OFF'))} — {html.escape(get_weak_drift_display_status(current_mode, btc_mode, market_state_label, alt_mode, market_bias_label).get('note', 'التنفيذ يعمل طبيعيًا.'))}",
        "",
        "🌍 <b>السوق:</b>",
        f"• BTC: {html.escape(str(btc_mode))}",
        f"• Alt Mode: {html.escape(str(alt_mode))}",
        f"• State: {html.escape(str(market_state_label))}",
        f"• Flow: {html.escape(str(market_bias_label))}",
        "",
        "🛡 <b>Market Guard 15m:</b>",
        f"• Level: {html.escape(guard_level)}",
        f"• Red Ratio: {red_ratio * 100:.1f}%",
        f"• Avg 15m: {avg_change:+.2f}%",
        f"• BTC 15m: {btc_change:+.2f}%",
        "",
        "🎯 <b>التصرف:</b>",
        html.escape(action),
        "",
        "✅ <b>المسموح:</b>",
    ]
    lines.extend([html.escape(x) for x in allowed_lines])
    if current_mode == MODE_BLOCK_LONGS:
        lines.extend([
            "",
            "🛡️ <b>حماية BLOCK:</b>",
            "السوق دخل وضع حماية. سيتم تقييم صفقات التنفيذ المفتوحة عند أول Market Reminder داخل BLOCK_LONGS.",
            "⚠️ الحماية الحالية Tracking فقط وليست أمر SL مباشر على OKX.",
        ])
    lines.extend([
        "",
        "📌 <b>ملاحظة:</b> قد تظهر إشارات قوية على اللوحة، لكن التنفيذ التجريبي يخضع لقواعد جودة الحركة والزخم.",
    ])
    if current_mode in (MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG):
        lines.append("🧩 <b>Weak Drift:</b> يمنع التنفيذ الضعيف فقط ولا يمنع الإشارة العادية.")
    if suggested_mode != current_mode:
        lines.extend([
            "",
            f"🔮 <b>المود المحسوب الآن:</b> {suggested_mode_ar}",
            "ℹ️ سيتم تطبيقه مع دورة الفحص القادمة إذا استمر الشرط.",
        ])
    return "\n".join(lines)
 except Exception as e:
    logger.error(f"build_market_status_message error: {e}")
    return f"❌ حصل خطأ أثناء بناء حالة السوق\n{html.escape(str(e))}"


def is_strong_exception(candidate: dict) -> bool:
    """Allow only very strong / relatively strong coins during BLOCK_LONGS.

    v204: use canonical execution_setup_tags first, then fall back to setup_type
    text for backward compatibility.
    """
    try:
        candidate = _ensure_execution_setup_tags(candidate or {})
        setup_type = str(candidate.get("setup_type", "") or "")
        tags = set(_collect_execution_setup_tags(candidate))
        score = float(candidate.get("effective_score", candidate.get("score", 0.0)) or 0.0)
        vol_ratio = float(candidate.get("vol_ratio", 0.0) or 0.0)
        mtf = bool(candidate.get("mtf_confirmed", False))
        relative_strength_short = float(candidate.get("relative_strength_short", 0.0) or 0.0)
        relative_strength_24 = float(candidate.get("relative_strength_24", 0.0) or 0.0)
        is_rel = (
            relative_strength_short >= 1.5
            or relative_strength_24 >= 2.0
            or bool(candidate.get("relative_strength_vs_btc", False))
            or "relative_strength_vs_btc" in tags
        )
        strong_tag_names = {
            "vwap_reclaim",
            "retest_breakout_confirmed",
            "wave_3",
            "liquidity_sweep_reclaim",
            "support_bounce_confirmed",
            "higher_low_continuation",
            "failed_breakdown_trap",
        }
        strong_setup = (
            bool(tags.intersection(strong_tag_names))
            or "breakout|mtf_yes|vol_high" in setup_type
            or any(tag in setup_type for tag in strong_tag_names)
        )
        return (
            strong_setup
            and score >= 8.3
            and vol_ratio >= 1.2
            and (mtf or is_rel)
        ) or (
            is_rel
            and score >= 8.0
            and vol_ratio >= 1.25
        )
    except Exception:
        return False


# =====================
# EXECUTION REPORT / PLAN SAFETY
# =====================
def _trade_field(trade: dict, key: str, default=None):
    diagnostics = trade.get("diagnostics", {}) or {}
    return trade.get(key, diagnostics.get(key, default))


def _safe_trade_float_value(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _is_late_risky_execution_context(data: dict) -> bool:
    """Return True when a signal is allowed for tracking but should not be an execution candidate."""
    try:
        diagnostics = data.get("diagnostics", {}) or {}
        setup_type = str(data.get("setup_type") or diagnostics.get("setup_type") or "").lower()
        entry_maturity = str(data.get("entry_maturity") or diagnostics.get("entry_maturity") or "").lower()
        wave_label = str(data.get("wave_label") or diagnostics.get("wave_label") or "").lower()
        fib_position = str(data.get("fib_position") or diagnostics.get("fib_position") or "").lower()
        maturity_label = str(data.get("entry_maturity_label") or diagnostics.get("entry_maturity_label") or "").lower()
        risky_tokens = (
            "wave_5_late",
            "danger_late",
            "overextended",
            "late_without_mtf",
            "hard_late_entry",
        )
        joined = "|".join([setup_type, entry_maturity, wave_label, fib_position, maturity_label])
        return any(token in joined for token in risky_tokens)
    except Exception:
        return False


try:
    from execution.config import EXECUTION_SETUP_WHITELIST
except Exception:
    EXECUTION_SETUP_WHITELIST = {
        "vwap_reclaim",
        "retest_breakout_confirmed",
        "wave_3",
        "relative_strength_vs_btc",
    }

# v205: keep main/executor whitelist aligned with the execution tag system.
EXECUTION_SETUP_WHITELIST = set(EXECUTION_SETUP_WHITELIST) | {
    "higher_low_continuation",
    "support_bounce_confirmed",
    "failed_breakdown_trap",
}
EXECUTION_WHITELIST_KEYWORDS = tuple(EXECUTION_SETUP_WHITELIST)

# NORMAL_LONG-only execution gate expansion.
# These tags are produced by detect_extra_strong_long_setups(), but may not exist
# in the global execution config whitelist yet. Keep them local to NORMAL_LONG so
# STRONG/BLOCK/RECOVERY behaviour stays unchanged.
NORMAL_LONG_EXECUTION_EXTRA_WHITELIST = {
    "failed_breakdown_trap",
    "higher_low_continuation",
    "support_bounce_confirmed",
    "liquidity_sweep_reclaim",
}


def _normalize_execution_tag(value) -> str:
    """Normalize setup/context tags for execution whitelist matching."""
    try:
        tag = str(value or "").strip().lower()
        tag = tag.replace(" ", "_").replace("-", "_")
        while "__" in tag:
            tag = tag.replace("__", "_")
        return tag.strip("_|")
    except Exception:
        return ""


def _collect_execution_setup_tags(data: dict) -> list:
    """Build one canonical execution tag list from every setup source.

    This prevents mismatch between what Telegram displays as Setup إضافي and
    what the execution whitelist sees.
    """
    if not isinstance(data, dict):
        return []
    diagnostics = data.get("diagnostics", {}) or {}
    keys = (
        "setup_type",
        "setup_type_base",
        "primary_extra_setup",
        "extra_setup",
        "setup_extra",
        "extra_setup_name",
        "extra_setup_names",
        "extra_setups",
        "extra_setups_details",
        "context",
        "context_setup",
        "context_setups",
        "setup_context",
        "wave_context",
        "wave_estimate",
        "wave_label",
        "wave",
        "execution_setup_tags",
        "early_execution_setup_tags",
        "entry_maturity",
        "entry_maturity_label",
        "relative_strength_vs_btc",
    )
    tags = set()

    def add_value(value, source_key=""):
        if value is None or value == "":
            return
        if isinstance(value, dict):
            for k, v in value.items():
                add_value(k, source_key)
                add_value(v, source_key)
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                add_value(item, source_key)
            return
        if isinstance(value, bool):
            if value and source_key:
                tags.add(_normalize_execution_tag(source_key))
            return
        text = str(value)
        for part in text.replace(",", "|").replace(";", "|").split("|"):
            tag = _normalize_execution_tag(part)
            if tag:
                tags.add(tag)

    for key in keys:
        add_value(data.get(key), key)
        add_value(diagnostics.get(key), key)

    return sorted(tags)


def build_execution_setup_tags(data: dict = None, **sources) -> list:
    """Build canonical execution setup tags from early or final scan data.

    Use this once early after detect_extra_strong_long_setups() and again on the
    final candidate.  The goal is to keep whitelist matching, Weak Drift weight,
    Smart Resistance relaxation, and execution badge decisions reading the same
    normalized setup source without changing strategy rules.
    """
    try:
        merged = {}
        if isinstance(data, dict):
            merged.update(data)
        for key, value in (sources or {}).items():
            if value is not None:
                merged[key] = value
        return _collect_execution_setup_tags(merged)
    except Exception:
        return []


def _ensure_execution_setup_tags(candidate: dict) -> dict:
    """Attach canonical execution_setup_tags to candidate in-place."""
    try:
        if not isinstance(candidate, dict):
            return candidate
        candidate["execution_setup_tags"] = build_execution_setup_tags(candidate)
        return candidate
    except Exception:
        return candidate


def _has_strict_execution_setup(data: dict) -> bool:
    """Execution whitelist: only detector-approved setup tags become Execution Candidates.

    Keep execution matching intentionally strict:
    - Use primary_extra_setup / extra_setup_names from detect_extra_strong_long_setups().
    - Allow relative_strength_vs_btc only when the explicit bool flag is True.
    - Do not whitelist based on setup_type, wave_context, wave_label, wave_estimate,
      entry_maturity, or other descriptive/context fields.
    """
    try:
        if not isinstance(data, dict):
            return False
        diagnostics = data.get("diagnostics", {}) or {}
        allowed = {_normalize_execution_tag(k) for k in EXECUTION_WHITELIST_KEYWORDS}
        tags = set()

        def add_detector_value(value):
            if value is None or value == "":
                return
            if isinstance(value, dict):
                for v in value.values():
                    add_detector_value(v)
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    add_detector_value(item)
                return
            if isinstance(value, bool):
                return
            for part in str(value).replace(",", "|").replace(";", "|").split("|"):
                tag = _normalize_execution_tag(part)
                if tag:
                    tags.add(tag)

        for key in ("primary_extra_setup", "extra_setup_names", "execution_setup_tags", "early_execution_setup_tags"):
            add_detector_value(data.get(key))
            add_detector_value(diagnostics.get(key))

        rel_strength = bool(data.get("relative_strength_vs_btc")) or bool(diagnostics.get("relative_strength_vs_btc"))
        if rel_strength:
            tags.add("relative_strength_vs_btc")

        return any(tag in allowed for tag in tags)
    except Exception:
        return False


def _has_normal_long_execution_setup(data: dict) -> bool:
    """NORMAL_LONG-only extension for execution candidate detection.

    This deliberately does not alter the global strict whitelist used by other
    market modes. It only accepts detector-produced extra setups that are
    already calculated by detect_extra_strong_long_setups(), plus wave_3 when
    entry_maturity explicitly estimates wave 3.
    """
    try:
        if not isinstance(data, dict):
            return False
        diagnostics = data.get("diagnostics", {}) or {}
        allowed = {_normalize_execution_tag(k) for k in NORMAL_LONG_EXECUTION_EXTRA_WHITELIST}
        tags = set()

        def add_detector_value(value):
            if value is None or value == "":
                return
            if isinstance(value, dict):
                for v in value.values():
                    add_detector_value(v)
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    add_detector_value(item)
                return
            if isinstance(value, bool):
                return
            for part in str(value).replace(",", "|").replace(";", "|").split("|"):
                tag = _normalize_execution_tag(part)
                if tag:
                    tags.add(tag)

        for key in ("primary_extra_setup", "extra_setup_names", "execution_setup_tags", "early_execution_setup_tags"):
            add_detector_value(data.get(key))
            add_detector_value(diagnostics.get(key))

        if any(tag in allowed for tag in tags):
            return True

        # Allow wave_3 only from the explicit numeric wave estimate, not from
        # descriptive setup_type/wave_context text.
        wave_values = (
            data.get("wave_estimate"),
            diagnostics.get("wave_estimate"),
        )
        for value in wave_values:
            try:
                if int(value or 0) == 3:
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False


def get_weak_trend_drift_status(candidate: dict) -> dict:
    """Execution-only weak drift detector.

    Weak Drift means the market/signal is drifting upward without enough
    confirmation. It must never block the normal Telegram signal; it only
    prevents weak alerts from receiving the execution badge / preview.
    """
    result = {"active": False, "reason": "", "details": {}}
    try:
        data = candidate or {}
        mode = normalize_market_mode(
            _trade_field(data, "current_mode", "")
            or _trade_field(data, "market_mode", "")
            or _trade_field(data, "mode", "")
            or MODE_NORMAL_LONG
        )
        if mode == MODE_BLOCK_LONGS:
            return result

        vol_ratio = _safe_trade_float_value(_trade_field(data, "vol_ratio", 0.0), 0.0) or 0.0
        score = _safe_trade_float_value(
            _trade_field(data, "effective_score", None)
            or _trade_field(data, "score", 0.0),
            0.0,
        ) or 0.0
        dist_ma = _safe_trade_float_value(_trade_field(data, "dist_ma", 0.0), 0.0) or 0.0
        mtf_confirmed = bool(_trade_field(data, "mtf_confirmed", False))
        breakout = bool(_trade_field(data, "breakout", False))
        pre_breakout = bool(_trade_field(data, "pre_breakout", False))
        setup_tags = set(_collect_execution_setup_tags(data))
        has_whitelist = _has_strict_execution_setup(data)

        entry_text = "|".join([
            str(_trade_field(data, "entry_timing", "") or ""),
            str(_trade_field(data, "entry_maturity", "") or ""),
            str(_trade_field(data, "wave_label", "") or ""),
            str(_trade_field(data, "fib_position", "") or ""),
        ]).lower()
        late_or_danger = any(token in entry_text for token in (
            "danger", "danger_late", "hard_late", "overextended",
            "متأخر جدًا", "امتداد سعري", "نهاية موجة", "موجة خامسة"
        ))

        btc_mode = str(_trade_field(data, "btc_mode", "") or "")
        alt_mode = str(_trade_field(data, "alt_mode", "") or "")
        market_state = str(_trade_field(data, "market_state", "") or "").lower()
        market_state_label = str(_trade_field(data, "market_state_label", "") or "").lower()
        market_bias_label = str(_trade_field(data, "market_bias_label", "") or "").lower()
        resistance_warning = str(_trade_field(data, "resistance_warning", "") or "")

        weak_market_context = (
            "risk_off" in market_state
            or "btc_leading" in market_state
            or "weak" in market_state_label
            or "ضعيف" in market_state_label
            or "weak" in market_bias_label
            or "ضعيف" in market_bias_label
            or btc_mode in ("🔴 هابط",)
            or alt_mode in ("🔴 ضعيف",)
        )

        low_momentum = (not mtf_confirmed and vol_ratio < 1.05)
        drifting_range = (weak_market_context and vol_ratio < 1.15 and not mtf_confirmed)
        soft_chase = (dist_ma > 3.2 and vol_ratio < 1.30 and not (breakout or pre_breakout))
        near_resistance_without_force = bool(resistance_warning) and vol_ratio < 1.25 and not mtf_confirmed

        active = bool(low_momentum or drifting_range or soft_chase or near_resistance_without_force or late_or_danger)
        if not active:
            return result

        reason_parts = []
        if low_momentum:
            reason_parts.append("low_momentum")
        if drifting_range:
            reason_parts.append("weak_market_drift")
        if soft_chase:
            reason_parts.append("soft_chase")
        if near_resistance_without_force:
            reason_parts.append("near_resistance_without_force")
        if late_or_danger:
            reason_parts.append("late_or_danger")

        result.update({
            "active": True,
            "reason": ",".join(reason_parts),
            "details": {
                "mode": mode,
                "score": score,
                "vol_ratio": vol_ratio,
                "dist_ma": dist_ma,
                "mtf_confirmed": mtf_confirmed,
                "has_whitelist": has_whitelist,
                "tags": sorted(setup_tags),
            },
        })
        return result
    except Exception as e:
        logger.warning(f"get_weak_trend_drift_status error: {e}")
        return result


def _get_strict_execution_setup_weight(data: dict) -> int:
    """Return setup strength using only strict detector-approved execution sources.

    This intentionally ignores setup_type / wave_context / wave_label / wave_estimate
    so descriptive analysis cannot accidentally relax Weak Drift.
    """
    weights = {
        "vwap_reclaim": 3,
        "retest_breakout_confirmed": 3,
        "liquidity_sweep_reclaim": 2,
        "relative_strength_vs_btc": 2,
        "wave_3": 2,
        "support_bounce_confirmed": 2,
        "failed_breakdown_trap": 2,
        "higher_low_continuation": 2,
    }
    try:
        if not isinstance(data, dict):
            return 0
        diagnostics = data.get("diagnostics", {}) or {}
        tags = set()

        def add_detector_value(value):
            if value is None or value == "":
                return
            if isinstance(value, dict):
                for v in value.values():
                    add_detector_value(v)
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    add_detector_value(item)
                return
            if isinstance(value, bool):
                return
            for part in str(value).replace(",", "|").replace(";", "|").split("|"):
                tag = _normalize_execution_tag(part)
                if tag:
                    tags.add(tag)

        for key in ("primary_extra_setup", "extra_setup_names", "execution_setup_tags", "early_execution_setup_tags"):
            add_detector_value(data.get(key))
            add_detector_value(diagnostics.get(key))

        rel_strength = bool(data.get("relative_strength_vs_btc")) or bool(diagnostics.get("relative_strength_vs_btc"))
        if rel_strength:
            tags.add("relative_strength_vs_btc")

        return max((weights.get(tag, 0) for tag in tags), default=0)
    except Exception:
        return 0


def _candidate_passes_weak_drift_execution_quality(candidate: dict) -> bool:
    """Smart Weak Drift gate for execution only.

    Normal Telegram signals remain allowed. Execution gets a controlled relaxation only
    when the candidate has a strict detector-approved setup plus enough score/volume/MTF.
    """
    try:
        drift = get_weak_trend_drift_status(candidate)
        if not drift.get("active"):
            return True

        data = candidate or {}
        score = _safe_trade_float_value(
            _trade_field(data, "effective_score", None)
            or _trade_field(data, "score", 0.0),
            0.0,
        ) or 0.0
        vol_ratio = _safe_trade_float_value(_trade_field(data, "vol_ratio", 0.0), 0.0) or 0.0
        mtf_confirmed = bool(_trade_field(data, "mtf_confirmed", False))
        breakout_quality = str(_trade_field(data, "breakout_quality", "") or "").strip().lower()
        resistance_warning = bool(_trade_field(data, "resistance_warning", ""))
        mode = normalize_market_mode(
            _trade_field(data, "current_mode", "")
            or _trade_field(data, "market_mode", "")
            or _trade_field(data, "mode", "")
            or MODE_NORMAL_LONG
        )
        has_whitelist = _has_strict_execution_setup(data)
        if mode == MODE_NORMAL_LONG and not has_whitelist:
            has_whitelist = _has_normal_long_execution_setup(data)
        setup_weight = _get_strict_execution_setup_weight(data)
        if mode == MODE_NORMAL_LONG and setup_weight <= 0 and _has_normal_long_execution_setup(data):
            setup_weight = 2

        entry_text = "|".join([
            str(_trade_field(data, "entry_timing", "") or ""),
            str(_trade_field(data, "entry_maturity", "") or ""),
            str(_trade_field(data, "wave_label", "") or ""),
            str(_trade_field(data, "fib_position", "") or ""),
        ]).lower()
        # NORMAL_LONG execution quality relaxation:
        # - Keep true danger / hard-late / overextended / wave-5 entries blocked.
        # - Do not let ordinary late wording (for example extension/near end of wave)
        #   automatically kill a detector-approved whitelist setup when score, MTF and
        #   volume are supportive. Normal alerts are unaffected; this is execution-only.
        hard_late_or_danger = any(token in entry_text for token in (
            "danger", "danger_late", "hard_late", "overextended", "متأخر جدًا", "موجة خامسة"
        ))
        soft_late_warning = any(token in entry_text for token in (
            "late", "متأخر", "امتداد سعري", "نهاية موجة"
        ))
        late_or_danger = hard_late_or_danger or (soft_late_warning and mode != MODE_NORMAL_LONG)

        allowed = False
        allow_reason = ""

        if has_whitelist and not hard_late_or_danger:
            if breakout_quality == "strong" and mtf_confirmed and vol_ratio >= 1.10 and score >= 7.2:
                allowed = True
                allow_reason = "strong_breakout_mtf"
            elif setup_weight >= 3 and score >= (6.5 if mode == MODE_NORMAL_LONG else 7.3) and mtf_confirmed and vol_ratio >= (1.05 if mode == MODE_NORMAL_LONG else 1.10):
                allowed = True
                allow_reason = "normal_whitelist_flow_weak_drift_warning" if mode == MODE_NORMAL_LONG and score < 7.3 else "tier3_setup_mtf"
            elif setup_weight >= 3 and score >= 7.7 and vol_ratio >= 1.25:
                allowed = True
                allow_reason = "tier3_setup_force"
            elif setup_weight >= 2 and score >= 7.5 and mtf_confirmed and vol_ratio >= 1.15:
                allowed = True
                allow_reason = "tier2_setup_mtf"
            elif setup_weight >= 2 and score >= 7.9 and vol_ratio >= 1.30:
                allowed = True
                allow_reason = "tier2_setup_force"
            elif (
                mode == MODE_NORMAL_LONG
                and soft_late_warning
                and setup_weight >= 3
                and score >= 6.5
                and mtf_confirmed
                and vol_ratio >= 1.05
                and breakout_quality in ("strong", "good", "ok", "")
            ):
                allowed = True
                allow_reason = "normal_whitelist_soft_late_allowed"
            else:
                dynamic_score = 0
                if mtf_confirmed:
                    dynamic_score += 2
                if vol_ratio >= 1.50:
                    dynamic_score += 3
                elif vol_ratio >= 1.25:
                    dynamic_score += 2
                elif vol_ratio >= 1.15:
                    dynamic_score += 1
                if breakout_quality == "strong":
                    dynamic_score += 2
                elif breakout_quality in ("ok", "good"):
                    dynamic_score += 1
                dynamic_score += setup_weight
                if soft_late_warning:
                    dynamic_score -= 1
                if resistance_warning:
                    dynamic_score -= 1

                if dynamic_score >= 5 and score >= 7.6 and vol_ratio >= 1.15:
                    allowed = True
                    allow_reason = f"dynamic_{dynamic_score}"

        if allowed:
            logger.info(
                "WEAK DRIFT EXEC ALLOW | "
                f"symbol={data.get('symbol', '?')} | allow_reason={allow_reason} | drift={drift.get('reason')} | "
                f"score={score:.2f} | vol={vol_ratio:.2f} | mtf={mtf_confirmed} | setup_weight={setup_weight}"
            )
            return True

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "WEAK DRIFT EXEC BLOCK | "
                f"symbol={data.get('symbol', '?')} | reason={drift.get('reason')} | "
                f"score={score:.2f} | vol={vol_ratio:.2f} | mtf={mtf_confirmed} | "
                f"whitelist={has_whitelist} | setup_weight={setup_weight} | hard_late={hard_late_or_danger} | soft_late={soft_late_warning} | "
                f"tags={data.get('execution_setup_tags', [])}"
            )
        return False
    except Exception as e:
        logger.warning(f"_candidate_passes_weak_drift_execution_quality error: {e}")
        return False


def _is_block_mode_execution_candidate(data: dict) -> bool:
    """Any alert that actually passed while BLOCK_LONGS is active becomes an execution candidate."""
    mode_value = (
        _trade_field(data, "current_mode", "")
        or _trade_field(data, "market_mode", "")
        or _trade_field(data, "mode", "")
    )
    mode = normalize_market_mode(mode_value)
    return (
        mode == MODE_BLOCK_LONGS
        or bool(_trade_field(data, "block_exception", False))
        or bool(_trade_field(data, "block_longs_execution_candidate", False))
    )


def _execution_plan_for_trade(trade: dict) -> dict:
    """Return complete execution plan values. Market entries can fall back to signal values."""
    entry_mode = str(_trade_field(trade, "entry_mode", "") or "").lower()
    has_pullback = bool(_trade_field(trade, "has_pullback_plan", False)) or entry_mode in ("pullback_pending", "pullback_triggered")

    execution_entry = _trade_field(trade, "execution_entry")
    execution_sl = _trade_field(trade, "execution_sl")
    execution_tp1 = _trade_field(trade, "execution_tp1")
    execution_tp2 = _trade_field(trade, "execution_tp2")

    # Market entries do not always store execution_* fields; use the signal plan as the execution plan.
    if not has_pullback and entry_mode in ("", "market", "market_entry"):
        execution_entry = execution_entry if execution_entry is not None else (_trade_field(trade, "recommended_entry") or _trade_field(trade, "market_entry") or _trade_field(trade, "entry"))
        execution_sl = execution_sl if execution_sl is not None else trade.get("sl")
        execution_tp1 = execution_tp1 if execution_tp1 is not None else trade.get("tp1")
        execution_tp2 = execution_tp2 if execution_tp2 is not None else trade.get("tp2")

    complete = all(_safe_trade_float_value(v) is not None for v in (execution_entry, execution_sl, execution_tp1))
    return {
        "entry": execution_entry,
        "sl": execution_sl,
        "tp1": execution_tp1,
        "tp2": execution_tp2,
        "complete": complete,
        "has_pullback": has_pullback,
        "entry_mode": entry_mode or "market",
    }


def is_execution_candidate_trade(trade: dict) -> bool:
    """Report/analytics classification for execution candidates.

    Badge-only rule:
    - New trades are counted in /report_execution only when the main signal
      message actually displayed the execution badge.
    - This prevents whitelist-like normal alerts (for example vwap_reclaim)
      from entering the execution report without a visible badge.
    - Legacy trades that do not yet have the badge flag can still be counted
      only if they already have a real execution status/message from the
      execution flow, not merely a whitelist setup.
    """
    try:
        badged_value = _trade_field(trade, "execution_candidate_badged", None)
        if isinstance(badged_value, str):
            badged_value = badged_value.strip().lower() in ("1", "true", "yes", "y", "on")
        if badged_value is True:
            plan = _execution_plan_for_trade(trade)
            return bool(plan.get("complete"))
        if badged_value is False:
            return False

        # Legacy fallback: before this flag existed, the safest proof that a
        # trade really passed the execution path is a concrete execution status
        # or an execution message. Do not count candidate_only/not_candidate.
        status = str(_trade_field(trade, "execution_status", "") or _trade_field(trade, "execution_result_status", "") or "").strip()
        if status in ("", "not_candidate", "candidate_only"):
            return False
        if bool(_trade_field(trade, "execution_message_sent", False)) or status not in ("preview_rejected",):
            plan = _execution_plan_for_trade(trade)
            return bool(plan.get("complete"))
        return False
    except Exception:
        return False


def _execution_report_since_ts(period: str):
    period = str(period or "all").lower()
    now = int(time.time())
    if period in ("1h", "hour"):
        return now - 3600
    if period == "today":
        return get_local_day_start_ts()
    if period in ("7d", "week"):
        return now - 7 * 86400
    if period in ("30d", "month"):
        return now - 30 * 86400
    return None


def _period_since_ts(period: str):
    """Compatibility alias for UI/report helpers."""
    return _execution_report_since_ts(period)


def _format_report_range_label(period: str = "all", trades: list = None, since_ts: int = None) -> str:
    """Human-readable report period/range for Telegram reports. UI only."""
    try:
        period_key = str(period or "all").lower()
        now_ts = int(time.time())
        if since_ts is None:
            since_ts = _execution_report_since_ts(period_key)
        if period_key in ("1h", "hour"):
            return f"آخر ساعة | {time.strftime('%Y-%m-%d %H:%M', time.localtime(since_ts or now_ts - 3600))} → {time.strftime('%H:%M', time.localtime(now_ts))}"
        if period_key == "today":
            return f"اليوم | {time.strftime('%Y-%m-%d', time.localtime(now_ts))}"
        if period_key in ("7d", "week"):
            return f"آخر 7 أيام | {time.strftime('%Y-%m-%d', time.localtime(since_ts or now_ts - 7*86400))} → {time.strftime('%Y-%m-%d', time.localtime(now_ts))}"
        if period_key in ("30d", "month"):
            return f"آخر 30 يوم | {time.strftime('%Y-%m-%d', time.localtime(since_ts or now_ts - 30*86400))} → {time.strftime('%Y-%m-%d', time.localtime(now_ts))}"
        # All time: use earliest trade timestamp if available; otherwise keep classic wording.
        if trades:
            timestamps = []
            for item in trades:
                try:
                    ts = _trade_created_ts_for_exec(item)
                    if ts > 0:
                        timestamps.append(ts)
                except Exception:
                    pass
            if timestamps:
                return f"منذ البداية | {time.strftime('%Y-%m-%d', time.localtime(min(timestamps)))} → {time.strftime('%Y-%m-%d', time.localtime(now_ts))}"
        return "منذ البداية"
    except Exception:
        return "منذ البداية"


def _avg_open_age_for_report(open_items: list) -> str:
    """Average open trade age used by open-report formatters. UI only."""
    try:
        if not open_items:
            return "0m"
        now = int(time.time())
        ages = []
        for item in open_items:
            ts = _trade_created_ts_for_exec(item)
            if ts > 0:
                ages.append(max(0, now - ts))
        if not ages:
            return "0m"
        avg = int(sum(ages) / len(ages))
        mins = avg // 60
        if mins < 60:
            return f"{mins}m"
        if mins < 1440:
            return f"{mins // 60}h {mins % 60}m"
        return f"{mins // 1440}d {(mins % 1440) // 60}h"
    except Exception:
        return "0m"


def _append_sampled_trade_cards(lines: list, pairs: list, card_func, icon: str, more_label: str, limit: int = 5, separator: str = "┄┄┄┄┄┄", status_func=None) -> None:
    """Append representative trade cards only, preserving counters for the rest. UI only."""
    selected = pairs[:max(0, int(limit or 0))]
    if not selected:
        lines.append("لا توجد بيانات حاليًا.")
        return
    for idx, pair in enumerate(selected):
        trade, pnl = pair
        if idx > 0:
            lines.append(separator)
        status_text = status_func(trade) if status_func else None
        try:
            lines.extend(card_func(trade, pnl, icon, status_text=status_text))
        except TypeError:
            lines.extend(card_func(trade, is_open=True))
    remaining = len(pairs) - len(selected)
    if remaining > 0:
        lines.append(f"📂 +{remaining} {more_label}")

def _format_exec_num(value, decimals=6):
    """Format execution-report prices without hiding micro-price differences.

    Very small coins such as PEPE can have entry/TP/SL values that look
    identical when rounded to 6 decimals. Keep the classic report layout,
    but use more decimals only when the price is tiny.
    """
    try:
        v = float(value)
        av = abs(v)
        if av <= 0:
            return f"{v:.2f}"
        if av >= 100:
            return f"{v:.2f}"
        if av >= 1:
            return f"{v:.4f}"
        if av >= 0.01:
            return f"{v:.5f}"
        if av >= 0.001:
            return f"{v:.6f}"
        # Micro-price assets: show enough precision so TP1/TP2/SL do not
        # collapse visually into the same displayed number.
        return f"{v:.8f}"
    except Exception:
        return "N/A"


def _execution_current_price_for_trade(trade: dict):
    for key in ("current_price", "last_price", "market_price", "last_tracked_price", "price"):
        current = _safe_trade_float_value(_trade_field(trade, key), None)
        if current and current > 0:
            return current
    try:
        current = _safe_trade_float_value(get_last_price(str(trade.get("symbol") or "")), None)
        if current and current > 0:
            return current
    except Exception:
        pass
    return None


def _execution_prices_line(trade: dict) -> str:
    plan = _execution_plan_for_trade(trade)
    current = _execution_current_price_for_trade(trade)
    tp1 = _safe_trade_float_value(plan.get("tp1") or trade.get("tp1"), None)
    tp2 = _safe_trade_float_value(plan.get("tp2") or trade.get("tp2"), None)
    sl = _safe_trade_float_value(plan.get("sl") or trade.get("sl"), None)
    return (
        f"💰 الحالي: {_format_exec_num(current)} | "
        f"🎯 TP1: {_format_exec_num(tp1)} | "
        f"🏁 TP2: {_format_exec_num(tp2)} | "
        f"🛑 SL: {_format_exec_num(sl)}"
    )


def _trade_created_ts_for_exec(trade: dict) -> int:
    for key in ("created_ts", "created_at", "candle_time"):
        try:
            v = int(float(trade.get(key) or 0))
            if v > 0:
                return v
        except Exception:
            pass
    return 0


def _execution_status_for_trade(trade: dict) -> str:
    status = str(_trade_field(trade, "execution_status", "") or "").strip()
    if status:
        return status
    result_status = str(_trade_field(trade, "execution_result_status", "") or "").strip()
    if result_status:
        return result_status
    if is_execution_candidate_trade(trade):
        return "candidate_only"
    return "not_candidate"


def _execution_trade_reached_tp1_for_display(trade: dict) -> bool:
    """Conservative TP1 phase proof for execution reports.

    Do not trust status == "partial" alone because legacy/stale status can mark
    a trade as partial before the actual TP1 flags are updated.
    """
    status = str(trade.get("status", "") or "").lower()
    return bool(
        trade.get("tp1_hit", False)
        or trade.get("sl_moved_to_entry", False)
        or trade.get("protected_breakeven", False)
        or trade.get("tp2_hit", False)
        or trade.get("trailing_active", False)
        or status in ("trailing", "trailing_open", "tp2_partial")
    )


def _execution_phase_for_trade(trade: dict) -> str:
    status = str(trade.get("status", "") or "").lower()
    result = str(trade.get("result", "") or "").lower()
    tp1_hit = _execution_trade_reached_tp1_for_display(trade)
    tp2_hit = bool(trade.get("tp2_hit", False))
    trailing_active = bool(trade.get("trailing_active", False)) or status in ("trailing", "trailing_open", "tp2_partial")
    if status == "pending_pullback":
        return "Pending Pullback"
    if trailing_active or tp2_hit:
        return "Trailing Active" if status not in ("closed", "expired") else "TP2 Hit"
    if tp1_hit:
        return "TP1 Hit"
    return "Before TP1"


def _execution_close_type_for_trade(trade: dict) -> str:
    result = str(trade.get("result", "") or "").lower()
    status = str(trade.get("status", "") or "").lower()
    tp1_hit = bool(trade.get("tp1_hit", False))
    tp2_hit = bool(trade.get("tp2_hit", False))
    if result == "trailing_win":
        return "TP2 + Trail Win"
    if result == "tp2_win" or tp2_hit:
        return "TP2 Win"
    if result == "tp1_win":
        return "TP1 Only"
    if result in ("breakeven", "protected_breakeven") or bool(trade.get("protected_breakeven_exit", False)):
        return "TP1 → BE" if tp1_hit else "Breakeven"
    if result == "loss":
        return "Direct SL"
    if result == "pending_expired":
        return "Pending Expired"
    if result == "expired" or status == "expired":
        return "Expired"
    return result or status or "Closed"


def _execution_final_pnl_pct(trade: dict):
    try:
        from tracking.summary_helpers import calc_trade_result_pct
        raw = calc_trade_result_pct(trade)
        if raw is None:
            return None
        leverage = float(_trade_field(trade, "leverage", TRACK_LEVERAGE) or TRACK_LEVERAGE)
        return float(raw) * leverage
    except Exception:
        return None


def _execution_floating_pnl_pct(trade: dict):
    try:
        plan = _execution_plan_for_trade(trade)
        entry = _safe_trade_float_value(plan.get("entry") or _trade_field(trade, "entry"), 0.0)
        if entry <= 0:
            return None
        current = None
        for key in ("current_price", "last_price", "market_price", "last_tracked_price"):
            current = _safe_trade_float_value(_trade_field(trade, key), None)
            if current and current > 0:
                break
        if not current or current <= 0:
            try:
                current = _safe_trade_float_value(get_last_price(str(trade.get("symbol") or "")), None)
            except Exception:
                current = None
        if not current or current <= 0:
            return None
        tp1 = _safe_trade_float_value(plan.get("tp1") or trade.get("tp1"), 0.0)
        tp2 = _safe_trade_float_value(plan.get("tp2") or trade.get("tp2"), 0.0)
        tp1_hit = bool(trade.get("tp1_hit", False))
        tp2_hit = bool(trade.get("tp2_hit", False))
        raw = 0.0
        if not tp1_hit:
            raw = (current - entry) / entry * 100.0
        elif not tp2_hit:
            tp1_part = ((tp1 - entry) / entry * 100.0) * 0.40 if tp1 > 0 else 0.0
            float_part = ((current - entry) / entry * 100.0) * 0.60
            raw = tp1_part + float_part
        else:
            tp1_part = ((tp1 - entry) / entry * 100.0) * 0.40 if tp1 > 0 else 0.0
            tp2_part = ((tp2 - entry) / entry * 100.0) * 0.40 if tp2 > 0 else 0.0
            trail_part = ((current - entry) / entry * 100.0) * 0.20
            raw = tp1_part + tp2_part + trail_part
        leverage = float(_trade_field(trade, "leverage", TRACK_LEVERAGE) or TRACK_LEVERAGE)
        return raw * leverage
    except Exception:
        return None


def _execution_duration_text(trade: dict) -> str:
    try:
        start_ts = _trade_created_ts_for_exec(trade)
        end_ts = int(float(trade.get("closed_ts") or trade.get("exit_ts") or trade.get("updated_ts") or time.time()))
        if start_ts <= 0 or end_ts <= start_ts:
            return "N/A"
        minutes = int((end_ts - start_ts) / 60)
        h, m = divmod(minutes, 60)
        if h >= 24:
            d, h = divmod(h, 24)
            return f"{d}d {h}h"
        return f"{h}h {m}m"
    except Exception:
        return "N/A"


def _is_execution_trade_open(trade: dict) -> bool:
    status = str(trade.get("status", "") or "").lower()
    result = str(trade.get("result", "") or "").lower()
    return status in ("open", "partial", "tp2_partial", "trailing", "trailing_open", "pending_pullback") or result in ("", "open", "partial")


def _format_execution_trade_card(trade: dict, is_open: bool) -> list:
    """Compact execution trade card aligned with the official report UI style."""
    plan = _execution_plan_for_trade(trade)
    raw_symbol = str(trade.get("symbol", "?") or "?")
    symbol = html.escape(raw_symbol)
    try:
        tv_link = build_tradingview_link(raw_symbol)
    except Exception:
        tv_link = ""

    def _compact_setup() -> str:
        setup_raw = str(
            _trade_field(trade, "primary_extra_setup")
            or _trade_field(trade, "extra_setup")
            or _trade_field(trade, "setup_type")
            or "Setup N/A"
        )
        parts = [p.strip() for p in setup_raw.replace(",", "|").split("|") if p.strip()]
        preferred = [
            "vwap_reclaim", "retest_breakout_confirmed", "higher_low_continuation",
            "relative_strength_vs_btc", "wave_3", "support_bounce_confirmed",
            "failed_breakdown_trap", "liquidity_sweep_reclaim",
        ]
        setup = next((p for p in preferred if p in parts), parts[-1] if parts else setup_raw[:40])
        mapping = {
            "vwap_reclaim": "VWAP Reclaim",
            "retest_breakout_confirmed": "Retest Breakout",
            "higher_low_continuation": "Higher Low",
            "relative_strength_vs_btc": "RS vs BTC",
            "wave_3": "Wave 3",
            "support_bounce_confirmed": "Support Bounce",
            "failed_breakdown_trap": "Failed Breakdown Trap",
            "liquidity_sweep_reclaim": "Liquidity Sweep",
        }
        return html.escape(mapping.get(str(setup), str(setup or "Setup N/A").replace("_", " ").title()))

    score = html.escape(str(_trade_field(trade, "score", "N/A")))
    duration = html.escape(_execution_duration_text(trade))
    phase = html.escape(_execution_phase_for_trade(trade) if is_open else _execution_close_type_for_trade(trade))
    pnl = _execution_floating_pnl_pct(trade) if is_open else _execution_final_pnl_pct(trade)
    pnl_txt = _pct_safe(pnl) if pnl is not None else "N/A"
    if pnl is None:
        result_for_icon = str(trade.get("result", "") or "").lower()
        icon = "🟢" if result_for_icon in ("tp1_win", "tp2_win", "trailing_win") else ("🔴" if result_for_icon == "loss" else "⚪")
    elif pnl > 0:
        icon = "🟢"
    elif pnl < 0:
        icon = "🔴"
    else:
        icon = "🟡"
    if _execution_trade_reached_tp1_for_display(trade) and is_open and (pnl or 0) >= 0:
        icon = "🟡"

    sl_text = "SL Entry" if bool(trade.get("sl_moved_to_entry", False)) or bool(trade.get("sl_is_entry", False)) else f"SL: {_format_exec_num(plan.get('sl'))}"
    lines = [
        f"{icon} <b>{symbol}</b> | {pnl_txt}",
        f"⏱️ {duration} | 📍 {phase}",
        f"🎯 TP1: {_format_exec_num(plan.get('tp1'))} | 🏁 TP2: {_format_exec_num(plan.get('tp2'))}",
        f"🛡 {html.escape(sl_text)} | ⭐ {score}",
        f"🧠 {_compact_setup()}",
    ]
    if tv_link:
        lines.append(f'🔗 <a href="{html.escape(tv_link, quote=True)}">TradingView</a>')
    return lines

def build_execution_open_report_message(period: str = "all") -> str:
    """Open-only report for execution-candidate trades using the official compact UI style."""
    try:
        since_ts = _execution_report_since_ts(period)
        try:
            trades = load_all_trades_for_report(
                r, market_type="futures", side="long", since_ts=since_ts, include_open=True
            )
        except Exception:
            trades = _load_long_trades_from_redis(limit=1500)
            if since_ts:
                trades = [t for t in trades if _trade_created_ts_for_exec(t) >= since_ts]
        trades = [t for t in trades if is_execution_candidate_trade(t) and _is_execution_trade_open(t)]
        title_map = {
            "all": "منذ البداية",
            "1h": "آخر ساعة",
            "hour": "آخر ساعة",
            "today": "آخر يوم",
            "7d": "آخر 7 أيام",
            "30d": "آخر 30 يوم",
            "month": "آخر 30 يوم",
        }
        title_period = _format_report_range_label(period, trades, since_ts)
        if not trades:
            return f"📭 لا توجد صفقات تنفيذ مفتوحة ({html.escape(title_period)})."

        pairs = []
        for t in trades:
            p = _execution_floating_pnl_pct(t)
            pairs.append((t, 0.0 if p is None else float(p)))
        winners = [(t, p) for t, p in pairs if p >= 0]
        losers = [(t, p) for t, p in pairs if p < 0]
        avg_trade_time = _avg_open_age_for_report(trades)
        net = sum(p for _, p in pairs)
        win_rate = (len(winners) / len(pairs) * 100.0) if pairs else 0.0

        tp1_hit_count = sum(1 for t in trades if _execution_trade_reached_tp1_for_display(t))
        tp2_active_count = sum(1 for t in trades if bool(t.get("tp2_hit", False)) or str(t.get("status", "") or "").lower() in ("tp2_partial", "trailing", "trailing_open"))
        trailing_count = sum(1 for t in trades if bool(t.get("trailing_active", False)) or str(t.get("status", "") or "").lower() in ("trailing", "trailing_open"))
        protected_count = sum(1 for t in trades if bool(t.get("sl_moved_to_entry", False)) or bool(t.get("protected_breakeven", False)) or _execution_trade_reached_tp1_for_display(t))
        danger_count = sum(1 for _, p in pairs if p <= -10.0)
        scores = [_safe_trade_float_value(_trade_field(t, "score", 0.0), 0.0) or 0.0 for t in trades]
        avg_score = _avg(scores) if scores else 0.0
        best_pair = max(pairs, key=lambda x: x[1]) if pairs else None
        worst_pair = min(pairs, key=lambda x: x[1]) if pairs else None
        best_txt = f"{str(best_pair[0].get('symbol','?')).replace('-SWAP','')} {_pct_safe(best_pair[1])}" if best_pair else "—"
        worst_txt = f"{str(worst_pair[0].get('symbol','?')).replace('-SWAP','')} {_pct_safe(worst_pair[1])}" if worst_pair else "—"
        lines = [
            "🚀 <b>صفقات التنفيذ المفتوحة</b>",
            f"📅 {html.escape(title_period)}",
            "━━━━━━━━━━━━",
            "⚡ جميع نسب الأداء محسوبة على رافعة 15x",
            "📊 <b>Quick Stats</b>",
            f"• Open: {len(trades)}",
            f"• Winners: {len(winners)}",
            f"• Losers: {len(losers)}",
            f"• Win Rate: <b>{win_rate:.1f}%</b>",
            f"• Net Floating: <b>{_pct_safe(net)}</b>",
            f"🎯 TP1: {tp1_hit_count}",
            f"🏁 TP2: {tp2_active_count}",
            f"📍 Trailing: {trailing_count}",
            f"🛡 Protected: {protected_count}",
            f"⚠️ Danger: {danger_count}",
            f"⏱ Avg Time: {html.escape(avg_trade_time)}",
            f"⭐ Avg Score: {avg_score:.2f}",
            f"🔥 Best: <b>{html.escape(best_txt)}</b>",
            f"⚠️ Worst: <b>{html.escape(worst_txt)}</b>",
            "━━━━━━━━━━━━",
            "📂 <b>Open Trades</b>",
            f"🟢 Open Winners: {len(winners)}",
            f"🔴 Open Losers: {len(losers)}",
        ]
        sep = "┄┄┄┄┄┄"
        winners_sorted = sorted(winners, key=lambda x: x[1], reverse=True)
        losers_sorted = sorted(losers, key=lambda x: x[1])
        if winners_sorted:
            lines.append("")
            for idx, (trade, _) in enumerate(winners_sorted[:5]):
                if idx > 0:
                    lines.append(sep)
                lines.extend(_format_execution_trade_card(trade, is_open=True))
            if len(winners_sorted) > 5:
                lines.append(f"📂 +{len(winners_sorted) - 5} صفقات رابحة مفتوحة أخرى")
        else:
            lines.append("لا توجد صفقات مفتوحة رابحة حاليًا.")
        lines.extend(["━━━━━━━━━━━━", "🔴 <b>Open Losers</b>"])
        if losers_sorted:
            for idx, (trade, _) in enumerate(losers_sorted[:5]):
                if idx > 0:
                    lines.append(sep)
                lines.extend(_format_execution_trade_card(trade, is_open=True))
            if len(losers_sorted) > 5:
                lines.append(f"📂 +{len(losers_sorted) - 5} صفقات خاسرة مفتوحة أخرى")
        else:
            lines.append("لا توجد صفقات مفتوحة خاسرة حاليًا.")
        lines.extend(["━━━━━━━━━━━━", "💡 يعتمد على نظام إدارة 40/40/20"])
        return "\n".join(lines).strip()
    except Exception as e:
        logger.error(f"build_execution_open_report_message error: {e}", exc_info=True)
        return f"❌ خطأ في تقرير صفقات التنفيذ المفتوحة: {html.escape(str(e))}"


def build_execution_report_message(period: str = "all") -> str:
    """Final execution-candidates report: compact wallet impact + behavior analytics."""
    try:
        since_ts = _execution_report_since_ts(period)
        try:
            trades = load_all_trades_for_report(
                r, market_type="futures", side="long", since_ts=since_ts, include_open=True
            )
        except Exception:
            trades = _load_long_trades_from_redis(limit=1500)
            if since_ts:
                trades = [t for t in trades if _trade_created_ts_for_exec(t) >= since_ts]

        trades = [
            t for t in trades
            if is_execution_candidate_trade(t)
        ]
        trades.sort(key=_trade_created_ts_for_exec, reverse=True)
        if not trades:
            return "📭 لا توجد صفقات مرشحة للتنفيذ حتى الآن."

        open_trades = [t for t in trades if _is_execution_trade_open(t)]
        closed_trades = [t for t in trades if not _is_execution_trade_open(t)]
        open_pnls = [p for p in (_execution_floating_pnl_pct(t) for t in open_trades) if p is not None]
        closed_pairs = [(t, _execution_final_pnl_pct(t)) for t in closed_trades]
        closed_pairs = [(t, p) for t, p in closed_pairs if p is not None]
        closed_pnls = [p for _, p in closed_pairs]
        winners_pairs = [(t, p) for t, p in closed_pairs if p > 0]
        losers_pairs = [(t, p) for t, p in closed_pairs if p < 0]

        total = max(1, len(trades))
        tp1_hits = sum(1 for t in trades if bool(t.get("tp1_hit", False)))
        tp2_hits = sum(
            1 for t in trades
            if bool(t.get("tp2_hit", False))
            or str(t.get("result", "") or "").lower() in ("tp2_win", "trailing_win")
        )
        trailing_wins = sum(1 for t in trades if str(t.get("result", "") or "").lower() == "trailing_win")
        direct_sl = sum(1 for t in closed_trades if _execution_close_type_for_trade(t) == "Direct SL")
        breakeven_exits = sum(
            1 for t in closed_trades
            if str(t.get("result", "") or "").lower() in ("breakeven", "protected_breakeven")
            or bool(t.get("protected_breakeven_exit", False))
            or "Breakeven" in _execution_close_type_for_trade(t)
        )
        tp1_to_tp2_pct = (tp2_hits / tp1_hits * 100.0) if tp1_hits else 0.0

        title_map = {
            "all": "منذ البداية",
            "1h": "آخر ساعة",
            "hour": "آخر ساعة",
            "today": "آخر يوم",
            "7d": "آخر 7 أيام",
            "30d": "آخر 30 يوم",
            "month": "آخر 30 يوم",
        }
        title_period = _format_report_range_label(period, trades, since_ts)

        realized_profit_pct = sum(p for p in closed_pnls if p > 0)
        realized_loss_pct = sum(p for p in closed_pnls if p < 0)
        realized_net_pct = realized_profit_pct + realized_loss_pct
        floating_profit_pct = sum(p for p in open_pnls if p > 0)
        floating_loss_pct = sum(p for p in open_pnls if p < 0)
        floating_net_pct = floating_profit_pct + floating_loss_pct
        portfolio_net_pct = realized_net_pct + floating_net_pct

        wallet_capital_usd = float(os.getenv("EXEC_REPORT_WALLET_USD", "1000") or 1000)
        trade_margin_usd = float(os.getenv("EXEC_REPORT_TRADE_MARGIN_USD", "35") or 35)
        pct_to_usd = trade_margin_usd / 100.0
        realized_profit_usd = realized_profit_pct * pct_to_usd
        realized_loss_usd = realized_loss_pct * pct_to_usd
        realized_net_usd = realized_net_pct * pct_to_usd
        floating_profit_usd = floating_profit_pct * pct_to_usd
        floating_loss_usd = floating_loss_pct * pct_to_usd
        floating_net_usd = floating_net_pct * pct_to_usd
        portfolio_net_usd = portfolio_net_pct * pct_to_usd
        wallet_pct = (portfolio_net_usd / wallet_capital_usd * 100.0) if wallet_capital_usd else 0.0

        def money(v: float) -> str:
            # LRM keeps Telegram RTL rendering stable: +0.00$ instead of $0.00+
            return f"\u200e{v:+.2f}$"

        def exposure(v: float) -> str:
            # LRM keeps Telegram RTL rendering stable: +0.00% Exposure
            return f"\u200e{_pct_safe(v)} Exposure"

        def compact_setup(trade: dict) -> str:
            setup = str(
                _trade_field(trade, "primary_extra_setup")
                or _trade_field(trade, "extra_setup")
                or _trade_field(trade, "setup_type")
                or "unknown"
            )
            parts = [x.strip() for x in setup.replace(",", "|").split("|") if x.strip()]
            preferred = [
                "vwap_reclaim", "retest_breakout_confirmed", "higher_low_continuation",
                "relative_strength_vs_btc", "wave_3", "support_bounce_confirmed",
                "failed_breakdown_trap", "liquidity_sweep_reclaim",
            ]
            for tag in preferred:
                if tag in parts:
                    return tag
            return (parts[-1] if parts else setup[:40]) or "unknown"

        def market_context_line(trade: dict) -> str:
            mode = str(_trade_field(trade, "current_mode") or _trade_field(trade, "market_mode") or _trade_field(trade, "mode") or "NORMAL_LONG")
            btc = str(_trade_field(trade, "btc_mode") or _trade_field(trade, "btc") or "BTC N/A")
            state = str(_trade_field(trade, "market_state_label") or _trade_field(trade, "market_state") or "")
            line = f"🌍 {html.escape(mode)} | {html.escape(btc)}"
            if state:
                line += f" | {html.escape(state[:24])}"
            return line

        def concise_trade_reason(trade: dict, is_win: bool) -> str:
            setup = compact_setup(trade)
            close_type = _execution_close_type_for_trade(trade)
            warning = str(_trade_field(trade, "resistance_warning") or _trade_field(trade, "support_warning") or "")
            entry_timing = str(_trade_field(trade, "entry_timing") or "")
            target_method = str(_trade_field(trade, "target_method") or "")
            mtf = bool(_trade_field(trade, "mtf_confirmed", False))
            vol = _safe_trade_float_value(_trade_field(trade, "vol_ratio", 0.0), 0.0) or 0.0
            pieces = []
            if setup and setup != "unknown":
                pieces.append(setup)
            if is_win:
                if mtf:
                    pieces.append("MTF مؤكد")
                if vol >= 1.2:
                    pieces.append("زخم قوي")
                if close_type:
                    pieces.append(str(close_type))
                prefix = "⚡"
            else:
                if "late" in entry_timing.lower() or "متأخر" in entry_timing:
                    pieces.append("دخول متأخر")
                if warning:
                    pieces.append(warning[:35])
                elif target_method:
                    pieces.append(target_method[:35])
                if close_type:
                    pieces.append(str(close_type))
                prefix = "⚠️"
            if not pieces:
                pieces = [str(close_type or "سياق غير واضح")]
            return f"{prefix} {html.escape(' + '.join(str(x) for x in pieces[:3]))}"

        def short_trade_line(trade: dict, pnl: float, icon: str, is_win: bool, label: str = None, compact_open: bool = False) -> list:
            raw_symbol = str(trade.get("symbol", "?") or "?")
            symbol = html.escape(raw_symbol)
            try:
                tv_link = build_tradingview_link(raw_symbol)
            except Exception:
                tv_link = ""
            score = _trade_field(trade, "score", "N/A")
            result_label = label or _execution_close_type_for_trade(trade)
            tv = f' | <a href="{html.escape(tv_link, quote=True)}">TradingView</a>' if tv_link else ""
            if compact_open:
                status = str(trade.get("status", "") or "").lower()
                phase_text = str(result_label or "")
                tp1_hit = _execution_trade_reached_tp1_for_display(trade)
                tp2_hit = bool(trade.get("tp2_hit", False)) or status in ("tp2_partial", "trailing_open", "trailing")
                trailing_active = bool(trade.get("trailing_active", False)) or status in ("tp2_partial", "trailing_open", "trailing")
                sl_entry = bool(trade.get("sl_moved_to_entry", False)) or bool(trade.get("protected_breakeven", False))
                if trailing_active or tp2_hit or "tp2" in phase_text.lower() or "trailing" in phase_text.lower():
                    status_line = "🔵 قرب TP2"
                elif tp1_hit:
                    status_line = "🟡 جزئية | بعد TP1"
                else:
                    status_line = "🟢 مفتوحة | قبل TP1"
                if sl_entry and "SL Entry" not in status_line:
                    status_line += " | 🔒 SL Entry"
                return [
                    f"{icon} <b>{symbol}</b> | {exposure(pnl)}",
                    _execution_prices_line(trade),
                    f"📌 الحالة: {html.escape(status_line)}",
                    market_context_line(trade),
                    concise_trade_reason(trade, is_win=is_win),
                    f"⭐ {html.escape(str(score))}{tv}",
                ]
            return [
                f"{icon} <b>{symbol}</b>",
                f"{exposure(pnl)} | {html.escape(str(result_label))}",
                _execution_prices_line(trade),
                market_context_line(trade),
                concise_trade_reason(trade, is_win=is_win),
                f"⭐ {html.escape(str(score))}{tv}",
            ]

        avg_winner = _avg([p for _, p in winners_pairs]) if winners_pairs else 0.0
        avg_loser = _avg([p for _, p in losers_pairs]) if losers_pairs else 0.0
        avg_open_floating = _avg(open_pnls) if open_pnls else 0.0
        rr_quality = "إيجابي ✔️" if winners_pairs and abs(avg_winner) >= abs(avg_loser) else "يحتاج متابعة ⚠️"
        impact_icon = "🟢" if portfolio_net_usd >= 0 else "🔴"

        # Official compact Telegram report UI style.
        # Replaces the older execution-report block to avoid duplicate/noisy UI sections.
        def _duration_from_ts(ts: int) -> str:
            try:
                age = max(0, int(time.time()) - int(ts or 0))
            except Exception:
                age = 0
            mins = age // 60
            if mins < 60:
                return f"{mins}m"
            if mins < 1440:
                return f"{mins // 60}h {mins % 60}m"
            return f"{mins // 1440}d {(mins % 1440) // 60}h"

        def _avg_open_age(open_items: list) -> str:
            if not open_items:
                return "0m"
            now = int(time.time())
            ages = []
            for item in open_items:
                ts = _trade_created_ts_for_exec(item)
                if ts > 0:
                    ages.append(max(0, now - ts))
            if not ages:
                return "0m"
            avg = int(sum(ages) / len(ages))
            mins = avg // 60
            if mins < 60:
                return f"{mins}m"
            if mins < 1440:
                return f"{mins // 60}h {mins % 60}m"
            return f"{mins // 1440}d {(mins % 1440) // 60}h"

        def _clean_setup_name(trade: dict) -> str:
            setup = compact_setup(trade)
            mapping = {
                "vwap_reclaim": "VWAP Reclaim",
                "retest_breakout_confirmed": "Retest Breakout Confirmed",
                "higher_low_continuation": "Higher Low Continuation",
                "relative_strength_vs_btc": "Relative Strength vs BTC",
                "wave_3": "Wave 3",
                "support_bounce_confirmed": "Support Bounce Confirmed",
                "failed_breakdown_trap": "Failed Breakdown Trap",
                "liquidity_sweep_reclaim": "Liquidity Sweep Reclaim",
            }
            return mapping.get(str(setup), str(setup).replace("_", " ").title())

        def _trade_prices_for_card(trade: dict) -> tuple:
            plan = _execution_plan_for_trade(trade)
            tp1 = _safe_trade_float_value(plan.get("tp1") or trade.get("tp1"), None)
            tp2 = _safe_trade_float_value(plan.get("tp2") or trade.get("tp2"), None)
            sl = _safe_trade_float_value(plan.get("sl") or trade.get("sl"), None)
            return (_format_exec_num(tp1), _format_exec_num(tp2), _format_exec_num(sl))

        def _trade_card_lines(trade: dict, pnl: float, icon: str, status_text: str = None) -> list:
            raw_symbol = str(trade.get("symbol", "?") or "?")
            symbol = html.escape(raw_symbol)
            try:
                tv_link = build_tradingview_link(raw_symbol)
            except Exception:
                tv_link = ""
            tv = f'<a href="{html.escape(tv_link, quote=True)}">TradingView</a>' if tv_link else "TradingView"
            score = _trade_field(trade, "score", "N/A")
            age = _duration_from_ts(_trade_created_ts_for_exec(trade))
            phase = status_text or _execution_phase_for_trade(trade)
            tp1, tp2, sl = _trade_prices_for_card(trade)
            setup = html.escape(_clean_setup_name(trade))
            return [
                f"{icon} <b>{symbol}</b> | {exposure(pnl)}",
                f"⏱️ {html.escape(age)} | 📍 {html.escape(str(phase))}",
                f"🎯 TP1: {tp1} | 🏁 TP2: {tp2}",
                f"🛡 SL: {sl} | ⭐ {html.escape(str(score))}",
                f"🧠 {setup}",
                f"🔗 {tv}",
            ]

        closed_decisions = len(winners_pairs) + len(losers_pairs)
        win_rate = (len(winners_pairs) / closed_decisions * 100.0) if closed_decisions else 0.0
        open_pairs = []
        for t in open_trades:
            p = _execution_floating_pnl_pct(t)
            if p is None:
                p = 0.0
            open_pairs.append((t, p))
        open_winners = [(t, p) for t, p in open_pairs if p >= 0]
        open_losers = [(t, p) for t, p in open_pairs if p < 0]
        avg_trade_time = _avg_open_age(open_trades)

        impact_icon = "🟢" if portfolio_net_usd >= 0 else "🔴"
        net_icon = "🟢" if realized_net_usd >= 0 else "🔴"
        floating_icon = "🟢" if floating_net_usd >= 0 else "🔴"

        lines = [
            "🚀 <b>تقرير أداء التنفيذ</b>",
            f"📅 {html.escape(title_period)}",
            "━━━━━━━━━━━━",
            "⚡ جميع نسب الأداء محسوبة على رافعة 15x",
            "📊 <b>Quick Stats</b>",
            f"• Candidates: {len(trades)}",
            f"• Open: {len(open_trades)}",
            f"• Closed: {len(closed_trades)}",
            f"🏆 Win Rate: <b>{win_rate:.1f}%</b>",
            f"🟢 Winners: {len(winners_pairs)}",
            f"🔴 Losers: {len(losers_pairs)}",
            "━━━━━━━━━━━━",
            "💰 <b>Wallet Impact</b>",
            f"📌 رأس المال: {wallet_capital_usd:.0f}$",
            "✅ <b>الصفقات المغلقة</b>",
            "📈 الأرباح",
            f"{money(realized_profit_usd)}",
            f"{exposure(realized_profit_pct)}",
            "📉 الخسائر",
            f"{money(realized_loss_usd)}",
            f"{exposure(realized_loss_pct)}",
            "⚖️ الصافي",
            f"<b>{net_icon} {money(realized_net_usd)}</b>",
            f"<b>{exposure(realized_net_pct)}</b>",
            "🔄 <b>الصفقات المفتوحة</b>",
            "📈 الأرباح العائمة",
            f"{money(floating_profit_usd)}",
            f"{exposure(floating_profit_pct)}",
            "📉 الخسائر العائمة",
            f"{money(floating_loss_usd)}",
            f"{exposure(floating_loss_pct)}",
            "⚖️ الصافي العائم",
            f"<b>{floating_icon} {money(floating_net_usd)}</b>",
            f"<b>{exposure(floating_net_pct)}</b>",
            "💼 <b>التأثير الحالي على المحفظة</b>",
            f"<b>{impact_icon} {money(portfolio_net_usd)}</b>",
            "━━━━━━━━━━━━",
            "🧠 <b>Execution Behavior Summary</b>",
            "📦 Model: 40/40/20",
            f"📈 Avg Winner: {_pct_safe(avg_winner)}",
            f"📉 Avg Loser: {_pct_safe(avg_loser)}",
            f"🎯 TP1 Rate: {tp1_hits / total * 100:.1f}%",
            f"🏁 TP2 Rate: {tp2_hits / total * 100:.1f}%",
            f"🔁 TP1 → TP2: {tp1_to_tp2_pct:.1f}%",
            f"🔄 Trailing Exit: {trailing_wins / total * 100:.1f}%",
            f"🔒 Breakeven Exit: {breakeven_exits / total * 100:.1f}%",
            f"🛑 Direct SL: {direct_sl / total * 100:.1f}%",
            f"⚡ Avg Floating Profit: {exposure(avg_open_floating)}",
            f"💡 Risk / Reward Quality: {html.escape(rr_quality)}",
            "━━━━━━━━━━━━",
            "📂 <b>Open Trades</b>",
            f"🟢 Open Winners: {len(open_winners)}",
            f"🔴 Open Losers: {len(open_losers)}",
            f"⏱️ Avg Trade Time: {html.escape(avg_trade_time)}",
        ]

        trade_separator = "┄┄┄┄┄┄"

        def _append_trade_group(title: str, pairs: list, icon: str, more_label: str, limit: int, reverse: bool = True, status_override: str = None):
            lines.extend(["", "━━━━━━━━━━━━" if title.startswith(("🔴", "🏆", "🛑")) else "", title])
            # Remove accidental blank separator line for the first embedded title under Open Trades.
            while "" in lines[-3:-2]:
                break
            selected = pairs[:limit]
            if not selected:
                lines.append("لا توجد بيانات حاليًا.")
                return
            for idx, (trade, pnl) in enumerate(selected):
                if idx > 0:
                    lines.append(trade_separator)
                lines.extend(_trade_card_lines(trade, pnl, icon, status_text=status_override))
            if len(pairs) > limit:
                lines.append(f"📂 +{len(pairs) - limit} {more_label}")

        # Open winners: highest floating PnL first. Open losers: worst floating PnL first.
        open_winners_sorted = sorted(open_winners, key=lambda x: x[1], reverse=True)
        open_losers_sorted = sorted(open_losers, key=lambda x: x[1])
        closed_winners_sorted = sorted(winners_pairs, key=lambda x: _trade_created_ts_for_exec(x[0]), reverse=True)
        closed_losers_sorted = sorted(losers_pairs, key=lambda x: _trade_created_ts_for_exec(x[0]), reverse=True)

        # Keep winners immediately under the Open Trades header without an additional section break.
        if open_winners_sorted:
            lines.append("")
            for idx, (trade, pnl) in enumerate(open_winners_sorted[:5]):
                if idx > 0:
                    lines.append(trade_separator)
                icon = "🟡" if _execution_trade_reached_tp1_for_display(trade) else "🟢"
                lines.extend(_trade_card_lines(trade, pnl, icon))
            if len(open_winners_sorted) > 5:
                lines.append(f"📂 +{len(open_winners_sorted) - 5} صفقات رابحة مفتوحة أخرى")
        else:
            lines.append("لا توجد صفقات مفتوحة رابحة حاليًا.")

        lines.extend(["━━━━━━━━━━━━", "🔴 <b>Open Losers</b>"])
        if open_losers_sorted:
            for idx, (trade, pnl) in enumerate(open_losers_sorted[:5]):
                if idx > 0:
                    lines.append(trade_separator)
                lines.extend(_trade_card_lines(trade, pnl, "🔴"))
            if len(open_losers_sorted) > 5:
                lines.append(f"📂 +{len(open_losers_sorted) - 5} صفقات خاسرة مفتوحة أخرى")
        else:
            lines.append("لا توجد صفقات مفتوحة خاسرة حاليًا.")

        lines.extend(["━━━━━━━━━━━━", "🏆 <b>Closed Winners</b>"])
        if closed_winners_sorted:
            for idx, (trade, pnl) in enumerate(closed_winners_sorted[:5]):
                if idx > 0:
                    lines.append(trade_separator)
                lines.extend(_trade_card_lines(trade, pnl, "🏆", status_text=_execution_close_type_for_trade(trade)))
            if len(closed_winners_sorted) > 5:
                lines.append(f"📂 +{len(closed_winners_sorted) - 5} صفقات رابحة مغلقة أخرى")
        else:
            lines.append("لا توجد صفقات رابحة مغلقة حتى الآن.")

        lines.extend(["━━━━━━━━━━━━", "🛑 <b>Closed Losers</b>"])
        if closed_losers_sorted:
            for idx, (trade, pnl) in enumerate(closed_losers_sorted[:5]):
                if idx > 0:
                    lines.append(trade_separator)
                lines.extend(_trade_card_lines(trade, pnl, "🛑", status_text=_execution_close_type_for_trade(trade)))
            if len(closed_losers_sorted) > 5:
                lines.append(f"📂 +{len(closed_losers_sorted) - 5} صفقات خاسرة مغلقة أخرى")
        else:
            lines.append("لا توجد صفقات خاسرة مغلقة حتى الآن.")

        lines.extend(["━━━━━━━━━━━━", "💡 يعتمد على نظام إدارة 40/40/20"])

        msg = "\n".join(lines).strip()
        return msg
    except Exception as e:
        logger.error(f"build_execution_report_message error: {e}", exc_info=True)
        return f"❌ خطأ في تقرير التنفيذ: {html.escape(str(e))}"


def _intelligence_setup_name(trade: dict) -> str:
    raw = str(
        _trade_field(trade, "primary_extra_setup")
        or _trade_field(trade, "extra_setup")
        or _trade_field(trade, "setup_type")
        or "unknown"
    )
    parts = [p.strip() for p in raw.replace(",", "|").split("|") if p.strip()]
    preferred = [
        "vwap_reclaim", "retest_breakout_confirmed", "higher_low_continuation",
        "relative_strength_vs_btc", "wave_3", "support_bounce_confirmed",
        "failed_breakdown_trap", "liquidity_sweep_reclaim",
    ]
    setup = next((p for p in preferred if p in parts), parts[-1] if parts else raw[:40])
    mapping = {
        "vwap_reclaim": "VWAP Reclaim",
        "retest_breakout_confirmed": "Retest Breakout",
        "higher_low_continuation": "Higher Low",
        "relative_strength_vs_btc": "RS vs BTC",
        "wave_3": "Wave 3",
        "support_bounce_confirmed": "Support Bounce",
        "failed_breakdown_trap": "Failed Breakdown Trap",
        "liquidity_sweep_reclaim": "Liquidity Sweep",
    }
    return mapping.get(str(setup), str(setup or "Unknown").replace("_", " ").title())


def _intelligence_exit_stats(trades: list) -> dict:
    total = max(1, len(trades))
    tp1 = sum(1 for t in trades if bool(t.get("tp1_hit", False)) or _execution_trade_reached_tp1_for_display(t))
    tp2 = sum(1 for t in trades if bool(t.get("tp2_hit", False)) or str(t.get("result", "") or "").lower() in ("tp2_win", "trailing_win"))
    trailing = sum(1 for t in trades if str(t.get("result", "") or "").lower() == "trailing_win" or bool(t.get("trailing_active", False)))
    direct_sl = sum(1 for t in trades if _execution_close_type_for_trade(t) == "Direct SL" or str(t.get("result", "") or "").lower() == "loss")
    return {
        "tp1_rate": tp1 / total * 100.0,
        "tp2_rate": tp2 / total * 100.0,
        "tp1_to_tp2": (tp2 / tp1 * 100.0) if tp1 else 0.0,
        "trailing_rate": trailing / total * 100.0,
        "direct_sl_rate": direct_sl / total * 100.0,
    }




def _intelligence_quality_icon(row: dict) -> str:
    """Visual quality marker for Intelligence rows. UI/reporting only."""
    try:
        wr = float(row.get("wr", 0.0) or 0.0)
        avg = float(row.get("avg", 0.0) or 0.0)
        direct_sl = float(row.get("direct_sl_rate", 0.0) or 0.0)
        closed = int(row.get("closed", 0) or 0)
        n = int(row.get("n", 0) or 0)
    except Exception:
        return "🟡"
    if closed < 2 and n < 4:
        return "🟡"
    if wr >= 55.0 and avg >= 0 and direct_sl <= 40.0:
        return "🟢"
    if wr < 42.0 or avg < 0 or direct_sl >= 50.0:
        return "🔴"
    return "🟡"

def build_intelligence_report_message(kind: str = "execution", period: str = "all") -> str:
    """Data-backed Intelligence report. Reporting/UI only; does not change strategy."""
    try:
        kind = str(kind or "execution").lower()
        since_ts = _execution_report_since_ts(period)
        try:
            trades = load_all_trades_for_report(
                r, market_type="futures", side="long", since_ts=since_ts, include_open=True
            )
        except Exception:
            trades = _load_long_trades_from_redis(limit=1500)
            if since_ts:
                trades = [t for t in trades if _trade_created_ts_for_exec(t) >= since_ts]
        if kind == "execution":
            trades = [t for t in trades if is_execution_candidate_trade(t)]
            title = "🧠 <b>Execution Intelligence</b>"
        else:
            trades = [t for t in trades if not is_execution_candidate_trade(t)]
            title = "🧠 <b>Market Intelligence</b>"
        trades.sort(key=_trade_created_ts_for_exec, reverse=True)
        period_label = _format_report_range_label(period, trades, since_ts)
        if not trades:
            return f"{title}\n📅 {html.escape(period_label)}\n━━━━━━━━━━━━\n📭 لا توجد بيانات كافية لهذا التقرير."

        closed_pairs = []
        for t in trades:
            if _is_execution_trade_open(t):
                continue
            pnl = _execution_final_pnl_pct(t)
            if pnl is not None:
                closed_pairs.append((t, float(pnl)))
        setup_map = {}
        for t in trades:
            name = _intelligence_setup_name(t)
            bucket = setup_map.setdefault(name, {"all": [], "closed": []})
            bucket["all"].append(t)
        for t, pnl in closed_pairs:
            name = _intelligence_setup_name(t)
            setup_map.setdefault(name, {"all": [], "closed": []})["closed"].append((t, pnl))

        setup_rows = []
        for name, data in setup_map.items():
            all_items = data.get("all", [])
            closed = data.get("closed", [])
            closed_n = len(closed)
            wins = sum(1 for _, p in closed if p > 0)
            avg = _avg([p for _, p in closed]) if closed else 0.0
            exit_stats = _intelligence_exit_stats(all_items)
            wr = (wins / closed_n * 100.0) if closed_n else 0.0
            setup_rows.append({
                "name": name,
                "n": len(all_items),
                "closed": closed_n,
                "wr": wr,
                "avg": avg,
                **exit_stats,
            })
        reliable_rows = [row for row in setup_rows if row["closed"] >= 2 or row["n"] >= 4]
        strongest = sorted(reliable_rows, key=lambda x: (x["wr"], x["tp2_rate"], x["avg"], x["n"]), reverse=True)[:3]
        caution = sorted(reliable_rows, key=lambda x: (x["wr"], -x["direct_sl_rate"], x["avg"]))[:3]

        closed_total = len(closed_pairs)
        wins_total = sum(1 for _, p in closed_pairs if p > 0)
        overall_wr = (wins_total / closed_total * 100.0) if closed_total else 0.0
        exit_all = _intelligence_exit_stats(trades)

        from collections import Counter
        filters = Counter()
        for t in trades:
            for key in ("reasons", "warning_reasons", "reject_reason", "rejection_reason"):
                vals = _trade_field(t, key, [])
                if isinstance(vals, str):
                    vals = [vals]
                if not isinstance(vals, (list, tuple)):
                    vals = []
                for val in vals:
                    txt = str(val or "").strip()
                    if txt:
                        filters[txt[:42]] += 1
        filter_rows = filters.most_common(3)

        lines = [
            title,
            f"📅 {html.escape(period_label)}",
            "━━━━━━━━━━━━",
            "⚡ جميع نسب الأداء محسوبة على رافعة 15x",
            f"📊 Sample: {len(trades)} | Closed: {closed_total} | WR: <b>{overall_wr:.1f}%</b>",
        ]
        if len(trades) < 10 or closed_total < 5:
            lines.append("⚠️ العينة صغيرة — القرار غير مؤكد")
        lines.extend(["━━━━━━━━━━━━", "✅ <b>أقوى Setups</b>"])
        if strongest:
            for row in strongest:
                lines.append(
                    f"• {_intelligence_quality_icon(row)} {html.escape(row['name'])} — WR {row['wr']:.1f}% | TP1 {row['tp1_rate']:.1f}% | TP2 {row['tp2_rate']:.1f}% | Avg {_pct_safe(row['avg'])}"
                )
        else:
            lines.append("• لا توجد عينة كافية لتحديد أقوى Setup.")
        lines.extend(["", "⚠️ <b>Setups تحتاج حذر</b>"])
        if caution:
            for row in caution:
                lines.append(
                    f"• {_intelligence_quality_icon(row)} {html.escape(row['name'])} — WR {row['wr']:.1f}% | Direct SL {row['direct_sl_rate']:.1f}% | Avg {_pct_safe(row['avg'])}"
                )
        else:
            lines.append("• لا توجد عينة كافية للتحذير.")
        lines.extend(["", "🧱 <b>Filters Review</b>"])
        if filter_rows:
            for reason, count in filter_rows:
                lines.append(f"• {html.escape(reason)} — {count} مرة")
        else:
            lines.append("• لا توجد أسباب فلترة كافية في العينة الحالية.")
        lines.extend([
            "",
            "🎯 <b>Exit Quality</b>",
            f"• TP1 Rate: {exit_all['tp1_rate']:.1f}%",
            f"• TP2 Conversion: {exit_all['tp1_to_tp2']:.1f}%",
            f"• Trailing Exit: {exit_all['trailing_rate']:.1f}%",
            f"• Direct SL: {exit_all['direct_sl_rate']:.1f}%",
            "━━━━━━━━━━━━",
            "🧠 <b>توصية مؤقتة</b>",
        ])
        if strongest:
            focus = " / ".join(row["name"] for row in strongest[:3])
            lines.append(f"ركز المتابعة على: {html.escape(focus)}")
        if caution:
            avoid = " / ".join(row["name"] for row in caution[:2])
            lines.append(f"راقب بحذر: {html.escape(avoid)}")
        lines.append("لا يتم تغيير أي فلتر تلقائيًا — التقرير للقياس فقط.")
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"build_intelligence_report_message error: {e}", exc_info=True)
        return f"❌ خطأ في تقرير Intelligence: {html.escape(str(e))}"


def build_execution_guard_report_message() -> str:
    try:
        snap = get_execution_daily_guard_snapshot()
        start_ts = int(snap.get("day_start_ts", get_local_day_start_ts()) or get_local_day_start_ts())
        start_text = time.strftime("%Y-%m-%d %H:%M", time.localtime(start_ts))
        locked = bool(snap.get("locked", False))
        status = "🔒 LOCKED" if locked else "✅ ACTIVE"
        raw_reason = str(snap.get("reason") or "").strip()
        reason = html.escape(raw_reason if raw_reason and raw_reason not in ("—", "-") else "لا يوجد")
        equity = float(snap.get("start_equity_usd", 0.0) or 0.0)
        net_usd = float(snap.get("daily_net_usd", 0.0) or 0.0)
        dd_pct = float(snap.get("daily_dd_pct", 0.0) or 0.0)
        limit_pct = float(snap.get("limit_pct", EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT) or EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT)
        limit_usd = float(snap.get("limit_usd", 0.0) or 0.0)
        source = html.escape(str(snap.get("equity_source", "config")))
        margin = float(snap.get("trade_margin_usd", EXECUTION_TRADE_MARGIN_USD) or EXECUTION_TRADE_MARGIN_USD)
        return "\n".join([
            "🛡️ <b>Execution Guard / Daily DD</b>",
            "━━━━━━━━━━━━",
            f"📌 الحالة: <b>{status}</b>",
            f"🕛 بداية اليوم: {html.escape(start_text)}",
            f"💰 رأس مال بداية اليوم: <b>{equity:.2f}$</b> ({source})",
            f"📍 Margin لكل صفقة في الحساب: {margin:.2f}$",
            "",
            f"📊 صافي تأثير اليوم: <b>{net_usd:+.2f}$</b>",
            f"📉 Daily DD الحالي: {'🔻' if dd_pct < 0 else '🟢'} <b>{dd_pct:+.2f}%</b>",
            f"🛑 حد الإيقاف: <b>-{limit_pct:.0f}%</b> ≈ <b>{limit_usd:.2f}$</b>",
            f"🧾 سبب القفل: {reason}",
            "",
            "ℹ️ الحساب الحالي يعتمد على Wallet Impact الحقيقي وليس مجموع نسب الصفقات بعد الرافعة.",
            "⚠️ في Simulation الرصيد من الإعدادات، وفي Live سيتم ربط رأس مال بداية اليوم من OKX لاحقًا.",
        ])
    except Exception as e:
        logger.error(f"build_execution_guard_report_message error: {e}", exc_info=True)
        return f"❌ خطأ في تقرير Execution Guard: {html.escape(str(e))}"


def build_execution_losses_report_message(period: str = "all") -> str:
    try:
        period = (period or "all").lower()
        since_ts = _period_since_ts(period)
        try:
            trades = load_all_trades_for_report(r, market_type="futures", side="long", since_ts=since_ts, include_open=True)
        except Exception:
            trades = _load_long_trades_from_redis(limit=1500)
            if since_ts:
                trades = [t for t in trades if _trade_created_ts_for_exec(t) >= since_ts]
        trades = [
            t for t in trades
            if is_execution_candidate_trade(t)
        ]
        losses = []
        for t in trades:
            if _is_execution_trade_open(t):
                continue
            pnl = _execution_final_pnl_pct(t)
            result = str(t.get("result", "") or "").lower()
            if (pnl is not None and pnl < 0) or result == "loss":
                losses.append((t, pnl if pnl is not None else 0.0))
        if not losses:
            return "📭 لا توجد خسائر لصفقات التنفيذ حتى الآن."
        from collections import Counter
        reasons = Counter()
        setups = Counter()
        timings = Counter()
        modes = Counter()
        scores = Counter()
        for t, pnl in losses:
            for key in ("reasons", "warning_reasons"):
                vals = _trade_field(t, key, []) or []
                if isinstance(vals, str):
                    vals = [vals]
                for reason in vals:
                    txt = str(reason or "").strip()
                    if txt:
                        reasons[txt] += 1
            setup = str(_trade_field(t, "setup_type", "unknown") or "unknown").strip() or "unknown"
            setups[setup] += 1
            timing = str(_trade_field(t, "entry_timing", "unknown") or "unknown").strip() or "unknown"
            timings[timing] += 1
            mode = str(_trade_field(t, "market_state_label", _trade_field(t, "market_state", "unknown")) or "unknown").strip() or "unknown"
            modes[mode] += 1
            score = _safe_trade_float_value(_trade_field(t, "score", None), None)
            if score is None:
                scores["N/A"] += 1
            elif score < 7:
                scores["<7"] += 1
            elif score < 8:
                scores["7-7.9"] += 1
            elif score < 9:
                scores["8-8.9"] += 1
            else:
                scores["9+"] += 1

        total_loss_pct = sum(float(p or 0.0) for _, p in losses)
        avg_loss_pct = total_loss_pct / len(losses) if losses else 0.0
        pct_to_usd = float(EXECUTION_TRADE_MARGIN_USD or 35.0) / 100.0
        total_loss_usd = total_loss_pct * pct_to_usd

        def top_lines(counter, limit=8):
            if not counter:
                return ["• لا توجد بيانات"]
            return [f"• {html.escape(str(k))}: {v}" for k, v in counter.most_common(limit)]

        lines = [
            "📉 <b>Execution Losses Report</b>",
            "━━━━━━━━━━━━",
            f"إجمالي صفقات التنفيذ: <b>{len(trades)}</b>",
            f"الخسائر المغلقة: <b>{len(losses)}</b>",
            f"متوسط الخسارة بعد الرافعة: <b>{avg_loss_pct:.2f}%</b>",
            f"تأثير الخسائر التقريبي: <b>{total_loss_usd:.2f}$</b>",
            "",
            "🔴 <b>أكثر الأسباب تكرارًا:</b>",
            *top_lines(reasons, 10),
            "",
            "🧩 <b>الخسائر حسب setup:</b>",
            *top_lines(setups, 8),
            "",
            "⏱ <b>الخسائر حسب توقيت الدخول:</b>",
            *top_lines(timings, 8),
            "",
            "🌍 <b>الخسائر حسب السوق:</b>",
            *top_lines(modes, 6),
            "",
            "⭐ <b>الخسائر حسب السكور:</b>",
            *top_lines(scores, 6),
            "",
            "📌 <b>آخر 5 خسائر تنفيذ:</b>",
        ]
        for t, pnl in sorted(losses, key=lambda x: _trade_created_ts_for_exec(x[0]), reverse=True)[:5]:
            symbol = html.escape(str(t.get("symbol", "?") or "?"))
            setup = html.escape(str(_trade_field(t, "setup_type", "") or ""))
            status = html.escape(_execution_status_for_trade(t))
            lines.append(f"• 🔴 <b>{symbol}</b> | {pnl:.2f}% | {status}")
            if setup:
                lines.append(f"  🧠 {setup[:90]}")
        return _limit_telegram_message("\n".join(lines))
    except Exception as e:
        logger.error(f"build_execution_losses_report_message error: {e}", exc_info=True)
        return f"❌ خطأ في تقرير خسائر التنفيذ: {html.escape(str(e))}"

def build_setup_performance_report_message() -> str:
    try:
        trades = _load_long_trades_from_redis(limit=1500)
        if not trades:
            return "📭 لا توجد صفقات لتحليل أداء الـ setups."
        buckets = {}
        for trade in trades:
            setup = str(_trade_field(trade, "primary_extra_setup") or _trade_field(trade, "setup_type") or "unknown")
            b = buckets.setdefault(setup, {"total":0,"wins":0,"losses":0,"open":0,"pending":0,"tp1":0,"tp2":0,"breakeven":0,"expired":0,"scores":[],"pnls":[]})
            b["total"] += 1
            status = str(trade.get("status", "") or "").lower()
            result = str(trade.get("result", "") or "").lower()
            if status == "open": b["open"] += 1
            if status == "pending_pullback": b["pending"] += 1
            if trade.get("tp1_hit"): b["tp1"] += 1
            if trade.get("tp2_hit") or result == "tp2_win": b["tp2"] += 1
            if "breakeven" in result or trade.get("protected_breakeven_exit"): b["breakeven"] += 1
            if result == "expired": b["expired"] += 1
            if result in ("tp1_win", "tp2_win", "trailing_win", "win") or trade.get("tp1_hit"):
                b["wins"] += 1
            if result == "loss": b["losses"] += 1
            try: b["scores"].append(float(_trade_field(trade, "score", 0) or 0))
            except Exception: pass
            try:
                pnl = _get_trade_pnl_pct(trade)
                if pnl != 0: b["pnls"].append(float(pnl))
            except Exception: pass
        def enrich(item):
            setup, b = item
            total = max(1, b["total"])
            closed = max(1, b["wins"] + b["losses"] + b["breakeven"] + b["expired"])
            return {
                "setup": setup, **b,
                "winrate": b["wins"] / closed * 100,
                "tp1_rate": b["tp1"] / total * 100,
                "tp2_rate": b["tp2"] / total * 100,
                "avg_score": _avg(b["scores"]),
                "avg_pnl": _avg(b["pnls"]) if b["pnls"] else None,
            }
        rows = [enrich(x) for x in buckets.items()]
        rows.sort(key=lambda x: (x["winrate"], x["tp2_rate"], x["total"]), reverse=True)
        def fmt_row(x):
            pnl = f"{x['avg_pnl']:+.2f}%" if x["avg_pnl"] is not None else "N/A"
            name = html.escape(x["setup"][:55])
            return (f"• {name}\n  total={x['total']} | W/L={x['wins']}/{x['losses']} | open={x['open']} | pending={x['pending']}\n"
                    f"  WR={x['winrate']:.1f}% | TP1={x['tp1_rate']:.1f}% | TP2={x['tp2_rate']:.1f}% | BE={x['breakeven']} | Exp={x['expired']}\n"
                    f"  avg_score={x['avg_score']:.2f} | avg_pnl={pnl}")
        lines = ["🧠 <b>Setup Performance</b>", f"إجمالي الصفقات: {len(trades)}", "", "✅ <b>أفضل 8 Setups</b>"]
        lines += [fmt_row(x) for x in rows[:8]]
        lines += ["", "⚠️ <b>أضعف 8 Setups</b>"]
        weak = sorted(rows, key=lambda x: (x["winrate"], x["tp2_rate"], -x["losses"]))[:8]
        lines += [fmt_row(x) for x in weak]
        return _limit_telegram_message("\n\n".join(lines))
    except Exception as e:
        logger.error(f"build_setup_performance_report_message error: {e}", exc_info=True)
        return f"❌ خطأ في تقرير setups: {html.escape(str(e))}"

# =========================
# COMMAND HANDLERS
# =========================
def handle_hard_reset(chat_id: str):
    if not r:
        send_telegram_reply(chat_id, "❌ Redis غير متصل")
        return
    if ADMIN_CHAT_IDS and str(chat_id) not in ADMIN_CHAT_IDS:
        send_telegram_reply(chat_id, f"⛔ غير مسموح\nchat_id={chat_id}")
        logger.warning(f"hard_reset blocked for non-admin chat_id={chat_id}")
        return
    try:
        del_trade = 0
        del_history = 0
        del_alert = 0
        del_cooldown = 0
        del_candle = 0
        del_mode_cache = 0

        removed_trade_keys = []

        for key in r.scan_iter("trade:futures:long:*"):
            r.delete(key)
            del_trade += 1
            removed_trade_keys.append(key)
        for key in r.scan_iter("trade_history:futures:long:*"):
            r.delete(key)
            del_history += 1
            removed_trade_keys.append(key)

        if removed_trade_keys:
            try:
                r.srem("trades:all", *removed_trade_keys)
            except Exception as e:
                logger.warning(f"Failed to srem trade keys from trades:all: {e}")

        r.delete("open_trades:futures:long")
        r.delete("stats:futures:long")
        r.delete("stats:last_reset_ts:long")
        for key in r.scan_iter("alert:long:*"):
            r.delete(key)
            del_alert += 1
        for key in r.scan_iter("alertmsg:long:*"):
            r.delete(key)
            del_alert += 1
        for key in r.scan_iter("sent:long:*"):
            r.delete(key)
            del_cooldown += 1
        for key in r.scan_iter("cooldown:long:*"):
            r.delete(key)
            del_cooldown += 1
        r.delete("global_cooldown:long")
        r.delete("scan:long:running")
        mode_keys = [
            MARKET_MODE_KEY,
            MARKET_MODE_LAST_KEY,
            MARKET_MODE_LAST_TRANSITION_KEY,
            MARKET_MODE_LAST_RECOVERY_CHECK_KEY,
            MARKET_MODE_NORMAL_CANDIDATE_KEY,
            MARKET_MODE_BLOCK_STARTED_KEY,
            MARKET_MODE_LAST_SAFE_SEEN_KEY,
            MARKET_MODE_LAST_REMINDER_KEY,
            MARKET_MODE_REMINDER_COUNT_KEY,
            MARKET_MODE_REMINDER_MODE_KEY,
        ]
        for k in mode_keys:
            if r.delete(k):
                del_mode_cache += 1
        r.delete(ALT_SNAPSHOT_CACHE_KEY)
        r.delete(MARKET_STATUS_SNAPSHOT_KEY)
        r.delete(NEWS_CACHE_KEY)
        r.delete(TELEGRAM_OFFSET_KEY)
        r.delete(TELEGRAM_BOOTSTRAP_DONE_KEY)
        r.delete(TELEGRAM_POLL_LOCK_KEY)
        del_mode_cache += 3

        # Reset execution stop/daily drawdown state as part of hard reset.
        # This does not close or modify trades; it only clears the execution pause flags.
        try:
            r.delete(EXECUTION_PAUSE_KEY)
            r.delete(EXECUTION_DRAWDOWN_LOCK_REASON_KEY)
            del_mode_cache += 2
        except Exception as e:
            logger.warning(f"Failed to clear execution pause/DD state on hard reset: {e}")

        for key in r.scan_iter("candles:long:*"):
            r.delete(key)
            del_candle += 1

        msg = (
            "🔥 <b>Hard Reset LONG تم بنجاح</b>\n"
            f"- trade keys: {del_trade}\n"
            f"- trade_history keys: {del_history}\n"
            f"- alerts: {del_alert}\n"
            f"- cooldown/sent keys: {del_cooldown}\n"
            f"- mode/cache keys: {del_mode_cache}\n"
            f"- candle cache keys: {del_candle}\n"
            f"- time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"
        )
        send_telegram_reply(chat_id, msg)
        logger.info(f"HARD RESET LONG executed by {chat_id}: {msg}")
    except Exception as e:
        logger.error(f"Hard reset error: {e}")
        send_telegram_reply(chat_id, f"❌ Hard reset failed: {html.escape(str(e))}")


def is_execution_paused() -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(EXECUTION_PAUSE_KEY))
    except Exception:
        return False


def build_exec_pause_message() -> str:
    if not r:
        return "⚠️ Redis غير متصل، لا يمكن إيقاف التداول الآن."
    try:
        r.set(EXECUTION_PAUSE_KEY, "1")
        return (
            "⛔ <b>تم إيقاف الدخول في صفقات جديدة</b>\n\n"
            "📌 الصفقات المفتوحة ستظل كما هي.\n"
            "📌 لن يتم فتح صفقات جديدة حتى إعادة التشغيل.\n"
            "✅ هذا لا يغلق أي مركز مفتوح."
        )
    except Exception as e:
        logger.exception(f"build_exec_pause_message error: {e}")
        return f"❌ فشل إيقاف التداول\nالسبب: {html.escape(str(e))}"


def build_exec_resume_message() -> str:
    if not r:
        return "⚠️ Redis غير متصل، لا يمكن إعادة التداول الآن."
    try:
        r.delete(EXECUTION_PAUSE_KEY)
        r.delete(EXECUTION_DRAWDOWN_LOCK_REASON_KEY)
        return (
            "✅ <b>تمت إعادة تفعيل الدخول في صفقات جديدة</b>\n\n"
            "📌 تم تصفير حالة إيقاف التنفيذ وDaily DD Lock يدويًا.\n"
            "📌 البوت يستطيع الآن قبول فرص جديدة حسب شروط التنفيذ.\n"
            "📌 هذا لا يغير حالة الصفقات المفتوحة."
        )
    except Exception as e:
        logger.exception(f"build_exec_resume_message error: {e}")
        return f"❌ فشل إعادة التداول\nالسبب: {html.escape(str(e))}"


def _send_open_trades(chat_id: str, period: str = "all"):
 try:
    msg = build_open_trades_message(period)
    chunks = split_open_trades_dashboard_message(msg, limit=3600)
    total = len(chunks)
    for idx, chunk in enumerate(chunks, start=1):
        final_chunk = chunk
        if total > 1:
            final_chunk = f"{chunk}\n\n📨 <b>جزء {idx}/{total}</b>"
        send_telegram_reply(chat_id, final_chunk)
        time.sleep(0.20)
 except Exception as e:
    logger.error(f"_send_open_trades error: {e}", exc_info=True)
    send_telegram_reply(chat_id, f"❌ خطأ في جلب الصفقات: {html.escape(str(e))}")


def _build_open_trades_report_context() -> dict:
    try:
        snapshot = load_market_status_snapshot(max_age_seconds=600) or {}
        current_mode = normalize_market_mode(
            snapshot.get("current_mode") or (r.get(MARKET_MODE_KEY) if r else MODE_NORMAL_LONG) or MODE_NORMAL_LONG
        )
        btc_mode = snapshot.get("btc_mode", "")
        alt_snapshot = snapshot.get("alt_snapshot", {}) or {}
        alt_mode = alt_snapshot.get("alt_mode", "")
        market_info = snapshot.get("market_info", {}) or {}
        market_state_label = market_info.get("market_state_label", "")
        market_bias_label = market_info.get("market_bias_label", "")
        drift = get_weak_drift_display_status(current_mode, btc_mode, market_state_label, alt_mode, market_bias_label)
        execution_status = "PAUSED" if bool(r and r.exists(EXECUTION_PAUSE_KEY)) else "ACTIVE"
        return {
            "market_mode": current_mode,
            "weak_drift": drift.get("label", "Weak Drift: OFF"),
            "execution_status": execution_status,
        }
    except Exception:
        return {"market_mode": MODE_NORMAL_LONG, "weak_drift": "Weak Drift: OFF", "execution_status": "UNKNOWN"}


def build_open_trades_message(period: str = "all") -> str:
 try:
    if not r:
        return "❌ لا يوجد اتصال بقاعدة البيانات"
    trades = get_open_trades_summary(r, market_type="futures", side="long")
    since_ts = _execution_report_since_ts(period)
    if since_ts:
        def _open_created_ts(item):
            for key in ("created_at", "created_ts", "candle_time"):
                try:
                    v = int(float(item.get(key) or 0))
                    if v > 0:
                        return v
                except Exception:
                    pass
            return 0
        trades = [t for t in trades if _open_created_ts(t) >= since_ts]
    ctx = _build_open_trades_report_context()
    return format_open_trades_message(
        trades,
        side="long",
        market_mode=ctx.get("market_mode"),
        weak_drift=ctx.get("weak_drift"),
        execution_status=ctx.get("execution_status"),
        period_label=_format_report_range_label(period, trades, since_ts),
    )
 except Exception as e:
    logger.error(f"build_open_trades_message error: {e}", exc_info=True)
    return f"❌ خطأ في جلب الصفقات المفتوحة: {html.escape(str(e))}"


COMMAND_HANDLERS = {
 "/help": lambda chat_id: send_telegram_reply(chat_id, build_help_message(), reply_markup=build_help_inline_keyboard()),
 "/start": lambda chat_id: send_telegram_reply(chat_id, build_help_message(), reply_markup=build_help_inline_keyboard()),
 "/help_execution": lambda chat_id: send_telegram_reply(chat_id, build_help_execution_message()),
 "/execution_reports": lambda chat_id: send_telegram_reply(chat_id, build_help_execution_message()),
 "/help_normal": lambda chat_id: send_telegram_reply(chat_id, build_help_normal_message()),
 "/normal_reports": lambda chat_id: send_telegram_reply(chat_id, build_help_normal_message()),
 "/help_exec_intelligence": lambda chat_id: send_telegram_reply(chat_id, build_help_exec_intelligence_message()),
 "/help_market_intelligence": lambda chat_id: send_telegram_reply(chat_id, build_help_market_intelligence_message()),
 "/diagnostics": lambda chat_id: send_telegram_reply(chat_id, build_help_diagnostics_message()),
 "/help_analysis": lambda chat_id: send_telegram_reply(chat_id, build_help_diagnostics_message()),
 "/okx_execution": lambda chat_id: send_telegram_reply(chat_id, build_help_okx_message()),
 "/help_okx": lambda chat_id: send_telegram_reply(chat_id, build_help_okx_message()),
 "/admin_help": lambda chat_id: send_telegram_reply(chat_id, build_help_admin_message()),
 "/help_admin": lambda chat_id: send_telegram_reply(chat_id, build_help_admin_message()),
 "/system_info": lambda chat_id: send_telegram_reply(chat_id, build_help_info_message()),
 "/mood": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/status": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/market": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/open_trades": lambda chat_id: _send_open_trades(chat_id, "all"),
 "/exec_status": lambda chat_id: send_telegram_reply(chat_id, build_exec_status_message()),
 "/exec_mode": lambda chat_id: send_telegram_reply(chat_id, build_exec_mode_message()),
 "/stop_trading": lambda chat_id: send_telegram_reply(chat_id, build_exec_pause_message()),
 "/resume_trading": lambda chat_id: send_telegram_reply(chat_id, build_exec_resume_message()),
 "/market_status": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/market_mode": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/how_it_work": lambda chat_id: send_telegram_reply(chat_id, build_how_it_work_message()),
 "/report_1h": lambda chat_id: send_telegram_reply(chat_id, build_report_message("1h")),
 "/report_today": lambda chat_id: send_telegram_reply(chat_id, build_report_message("today")),
 "/report_month": lambda chat_id: send_telegram_reply(chat_id, build_report_message("month")),
 "/report_30d": lambda chat_id: send_telegram_reply(chat_id, build_report_message("month")),
 "/report_all": lambda chat_id: send_telegram_reply(chat_id, build_report_message("all")),
 "/report_deep": lambda chat_id: send_telegram_reply(chat_id, build_deep_report_message()),
 "/report_setups": lambda chat_id: send_telegram_reply(chat_id, build_setup_performance_report_message()),
 "/report_execution": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("all"))),
 "/report_execution_1h": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("1h"))),
 "/report_execution_today": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("today"))),
 "/report_execution_7d": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("7d"))),
 "/report_execution_30d": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("30d"))),
 "/report_execution_open": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_open_report_message("all"))),
 "/report_execution_open_1h": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_open_report_message("1h"))),
 "/report_execution_open_today": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_open_report_message("today"))),
 "/report_execution_open_7d": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_open_report_message("7d"))),
 "/report_execution_profit_analysis": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("all"))),
 "/report_execution_profit_analysis_1h": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("1h"))),
 "/report_execution_profit_analysis_today": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("today"))),
 "/report_execution_profit_analysis_7d": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("7d"))),
 "/report_execution_losses_analysis": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_losses_report_message())),
 "/report_execution_losses_analysis_1h": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_losses_report_message())),
 "/report_execution_losses_analysis_today": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_losses_report_message())),
 "/report_execution_losses_analysis_7d": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_losses_report_message())),
 "/report_execution_losses": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_losses_report_message())),
 "/report_execution_guard": lambda chat_id: send_telegram_reply(chat_id, build_execution_guard_report_message()),
 "/report_execution_profit": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("all"))),
 "/report_execution_profit_today": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("today"))),
 "/report_execution_profit_7d": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("7d"))),
 "/report_execution_losses_today": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_losses_report_message())),
 "/report_execution_losses_7d": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_losses_report_message())),
 "/report_execution_analysis": lambda chat_id: send_telegram_reply_chunks(chat_id, split_telegram_message(build_execution_report_message("all"))),
 "/report_execution_setups": lambda chat_id: send_telegram_reply(chat_id, build_setup_performance_report_message()),
 "/report_execution_exits": lambda chat_id: send_telegram_reply(chat_id, build_exits_report_message()),
 "/report_execution_diagnostics": lambda chat_id: send_telegram_reply(chat_id, build_full_diagnostics_report(r, market_type="futures", side="long", period="all")),
 "/report_execution_intelligence": lambda chat_id: send_telegram_reply(chat_id, build_intelligence_report_message("execution", "all")),
 "/report_execution_intelligence_1h": lambda chat_id: send_telegram_reply(chat_id, build_intelligence_report_message("execution", "1h")),
 "/report_execution_intelligence_today": lambda chat_id: send_telegram_reply(chat_id, build_intelligence_report_message("execution", "today")),
 "/report_execution_intelligence_7d": lambda chat_id: send_telegram_reply(chat_id, build_intelligence_report_message("execution", "7d")),
 "/report_intelligence": lambda chat_id: send_telegram_reply(chat_id, build_intelligence_report_message("normal", "all")),
 "/report_intelligence_1h": lambda chat_id: send_telegram_reply(chat_id, build_intelligence_report_message("normal", "1h")),
 "/report_intelligence_today": lambda chat_id: send_telegram_reply(chat_id, build_intelligence_report_message("normal", "today")),
 "/report_intelligence_7d": lambda chat_id: send_telegram_reply(chat_id, build_intelligence_report_message("normal", "7d")),
 "/report_profit": lambda chat_id: send_telegram_reply(chat_id, build_report_message("all")),
 "/report_profit_today": lambda chat_id: send_telegram_reply(chat_id, build_report_message("today")),
 "/report_profit_7d": lambda chat_id: send_telegram_reply(chat_id, build_7d_report_message()),
 "/open_trades_1h": lambda chat_id: _send_open_trades(chat_id, "1h"),
 "/open_trades_today": lambda chat_id: _send_open_trades(chat_id, "today"),
 "/open_trades_7d": lambda chat_id: _send_open_trades(chat_id, "7d"),
 "/report_profit_analysis": lambda chat_id: send_telegram_reply(chat_id, build_report_message("all")),
 "/report_profit_analysis_1h": lambda chat_id: send_telegram_reply(chat_id, build_report_message("1h")),
 "/report_profit_analysis_today": lambda chat_id: send_telegram_reply(chat_id, build_report_message("today")),
 "/report_profit_analysis_7d": lambda chat_id: send_telegram_reply(chat_id, build_7d_report_message()),
 "/report_losses_analysis": lambda chat_id: send_telegram_reply(chat_id, build_losses_report(r, market_type="futures", side="long", period="all")),
 "/report_losses_analysis_1h": lambda chat_id: send_telegram_reply(chat_id, build_losses_report(r, market_type="futures", side="long", period="1h")),
 "/report_losses_analysis_today": lambda chat_id: send_telegram_reply(chat_id, build_losses_report(r, market_type="futures", side="long", period="today")),
 "/report_losses_analysis_7d": lambda chat_id: send_telegram_reply(chat_id, build_losses_report(r, market_type="futures", side="long", period="7d")),
 "/report_losses_today": lambda chat_id: send_telegram_reply(chat_id, build_losses_report(r, market_type="futures", side="long", period="today")),
 "/report_losses_7d": lambda chat_id: send_telegram_reply(chat_id, build_losses_report(r, market_type="futures", side="long", period="7d")),
 "/report_scores": lambda chat_id: send_telegram_reply(chat_id, build_scores_report(r, market_type="futures", side="long", period="all")),
 "/report_market": lambda chat_id: send_telegram_reply(chat_id, build_market_report(r, market_type="futures", side="long", period="all")),
 "/report_losses": lambda chat_id: send_telegram_reply(chat_id, build_losses_report(r, market_type="futures", side="long", period="all")),
 "/report_diagnostics": lambda chat_id: send_telegram_reply(chat_id, build_full_diagnostics_report(r, market_type="futures", side="long", period="all")),
 "/report_exits": lambda chat_id: send_telegram_reply(chat_id, build_exits_report_message()),
 "/report_rejections": lambda chat_id: send_telegram_reply(
    chat_id,
    build_rejections_report_message(r)
),
 "/report_daily": lambda chat_id: send_telegram_reply(chat_id, build_daily_report_message()),
 "/report_7d": lambda chat_id: send_telegram_reply(chat_id, build_7d_report_message()),
 "/reset_stats": lambda chat_id: reset_stats(chat_id),
 "/stats_since_reset": lambda chat_id: stats_since_reset(chat_id),
 "/hard_reset": handle_hard_reset,
}

# =========================
# ALERT TRACKING
# =========================
def build_alert_id(symbol: str, candle_time: int) -> str:
 return f"{clean_symbol_for_message(symbol)}:{int(candle_time)}"

def save_alert_snapshot(alert_data: dict, message_id=None) -> None:
 if not r or not alert_data:
    return
 try:
    alert_id = alert_data.get("alert_id")
    if not alert_id:
        logger.warning("save_alert_snapshot: missing alert_id")
        return
    payload = dict(alert_data)
    if message_id is not None:
        payload["message_id"] = str(message_id)
    r.set(get_alert_key(alert_id), json.dumps(payload, ensure_ascii=False), ex=ALERT_TTL_SECONDS)
    logger.info(f"✅ Alert snapshot saved: {alert_id}")
    if message_id:
        msg_key = get_alert_by_message_key(str(message_id))
        r.set(msg_key, alert_id, ex=ALERT_TTL_SECONDS)
        logger.info(f"🔗 Linked message {message_id} to alert {alert_id}")
 except Exception as e:
    logger.error(f"❌ save_alert_snapshot error: {e}")

def load_alert_snapshot(alert_id: str):
 if not r or not alert_id:
    return None
 try:
    raw = r.get(get_alert_key(alert_id))
    if not raw:
        logger.info(f"load_alert_snapshot: no data for {alert_id}")
        return None
    data = json.loads(raw)
    return data if isinstance(data, dict) else None
 except Exception as e:
    logger.error(f"load_alert_snapshot error: {e}")
    return None

def load_alert_snapshot_by_message_id(message_id: str):
 if not r or not message_id:
    return None
 try:
    msg_key = get_alert_by_message_key(str(message_id))
    alert_id = r.get(msg_key)
    if not alert_id:
        logger.info(f"load_alert_snapshot_by_message_id: no alert_id for message {message_id}")
        return None
    logger.info(f"Found alert_id {alert_id} via message {message_id}")
    return load_alert_snapshot(alert_id)
 except Exception as e:
    logger.error(f"load_alert_snapshot_by_message_id error: {e}")
    return None

def set_alert_registration_status(alert_id: str, success: bool) -> None:
    if not r or not alert_id:
        return
    try:
        raw = r.get(get_alert_key(alert_id))
        if raw:
            data = json.loads(raw)
            data["register_trade_failed"] = not success
            data["register_trade_status"] = "ok" if success else "failed"
            if not success:
                data["register_trade_failed_ts"] = int(time.time())
            else:
                data.pop("register_trade_failed_ts", None)
            r.set(get_alert_key(alert_id), json.dumps(data, ensure_ascii=False), ex=ALERT_TTL_SECONDS)
    except Exception as e:
        logger.error(f"set_alert_registration_status failed for {alert_id}: {e}")

def should_ignore_track_callback(chat_id: str, message_id: str, alert_id: str) -> bool:
 if not r:
    return False
 try:
    lock_key = f"track:callback:lock:long:{chat_id}:{message_id}:{alert_id}"
    locked = r.set(lock_key, "1", ex=8, nx=True)
    if locked:
        return False
    return True
 except Exception as e:
    logger.error(f"should_ignore_track_callback error: {e}")
    return False

def get_last_price(symbol: str) -> float:
 try:
    res = requests.get(OKX_TICKER_SINGLE_URL, params={"instId": symbol}, timeout=10).json()
    data = res.get("data", [])
    if not data:
        return 0.0
    return _safe_float(data[0].get("last"), 0.0)
 except Exception as e:
    logger.error(f"get_last_price error on {symbol}: {e}")
    return 0.0

def get_max_move_since_alert(symbol: str, since_ts: int, entry: float, side: str = "long"):
    try:
        age_seconds = max(0, int(time.time()) - since_ts)
        if age_seconds <= 24 * 3600:
            timeframe = TIMEFRAME
            limit = 120
        elif age_seconds <= 72 * 3600:
            timeframe = "1H"
            limit = 100
        else:
            timeframe = "1H"
            limit = 200

        candles = get_candles(symbol, timeframe, limit)
        df = to_dataframe(candles)
        if df is None or df.empty:
            return entry, 0.0, entry, 0.0

        ts_col = df["ts"].astype(float)
        since_ms = since_ts * 1000 if since_ts < 10_000_000_000 else since_ts
        work = df[ts_col > since_ms].copy()
        if work.empty:
            work = df.tail(20).copy()

        lows = work["low"].astype(float)
        highs = work["high"].astype(float)
        if lows.empty or highs.empty or entry <= 0:
            return entry, 0.0, entry, 0.0

        if side == "long":
            favorable_price = float(highs.max())
            favorable_pct = round(((favorable_price - entry) / entry) * 100, 2)
            adverse_price = float(lows.min())
            adverse_pct = round(((entry - adverse_price) / entry) * 100, 2)
            return favorable_price, favorable_pct, adverse_price, adverse_pct
        else:
            favorable_price = float(lows.min())
            favorable_pct = round(((entry - favorable_price) / entry) * 100, 2)
            adverse_price = float(highs.max())
            adverse_pct = round(((adverse_price - entry) / entry) * 100, 2)
            return favorable_price, favorable_pct, adverse_price, adverse_pct
    except Exception as e:
        logger.error(f"get_max_move_since_alert error on {symbol}: {e}")
        return entry, 0.0, entry, 0.0

def resolve_alert_official_or_estimated_status(alert: dict) -> dict:
    symbol = alert.get("symbol", "Unknown")
    official_status = ""
    trade = load_registered_trade_for_alert(alert)
    if trade:
        official_status = format_official_trade_status(trade)
        if official_status:
            return {
                "official_status": official_status,
                "estimated_status": "",
                "display_status": official_status,
                "is_official": True
            }
    estimated_status = get_alert_status(alert)
    return {
        "official_status": "",
        "estimated_status": estimated_status,
        "display_status": estimated_status,
        "is_official": False
    }

def get_alert_status(alert: dict) -> str:
 try:
    symbol = alert["symbol"]
    mode = alert.get("mode") or alert.get("market_mode", "")
    avg_planned = _safe_float(alert.get("average_planned_entry"), 0.0)
    entry = _safe_float(alert.get("entry"), 0)
    recommended_entry = _safe_float(alert.get("recommended_entry"), 0.0)
    pullback_entry = _safe_float(alert.get("pullback_entry"), 0.0)
    entry_mode = alert.get("entry_mode", "market")
    pullback_triggered = bool(alert.get("pullback_triggered", False))
    market_entry = _safe_float(alert.get("market_entry"), 0.0)

    if mode == MODE_RECOVERY_LONG and avg_planned > 0:
        effective_entry = avg_planned
    elif entry_mode == "pullback_pending" and not pullback_triggered:
        return "⏳ Pending Pullback"
    elif entry_mode == "pullback_pending":
        effective_entry = pullback_entry if pullback_entry > 0 else entry
    else:
        effective_entry = market_entry if market_entry > 0 else entry

    sl = _safe_float(alert.get("sl"), 0)
    tp1 = _safe_float(alert.get("tp1"), 0)
    tp2 = _safe_float(alert.get("tp2"), 0)
    candle_time = int(_safe_float(alert.get("candle_time"), 0))
    favorable_price, favorable_pct, adverse_price, adverse_pct = get_max_move_since_alert(
        symbol=symbol,
        since_ts=candle_time,
        entry=effective_entry,
        side="long",
    )
    if effective_entry <= 0:
        return "🔴 غير معروف"
    sl_pct = round(((effective_entry - sl) / effective_entry) * 100, 4) if sl > 0 else 0
    tp1_pct = round(((tp1 - effective_entry) / effective_entry) * 100, 4) if tp1 > 0 else 0
    tp2_pct = round(((tp2 - effective_entry) / effective_entry) * 100, 4) if tp2 > 0 else 0
    if adverse_pct >= sl_pct > 0:
        return "❌ SL Hit"
    if favorable_pct >= tp2_pct > 0:
        return "🎯 TP2 Hit"
    if favorable_pct >= tp1_pct > 0:
        return "✅ TP1 Hit"
    return "⏳ Open"
 except Exception as e:
    logger.error(f"get_alert_status error: {e}")
    return "🔴 خطأ"

def get_track_state_badge(status: str, current_move: float) -> str:
 if "Pending Pullback" in status or "انتظار Pullback" in status:
    return "⏳ Pending Pullback"
 if "Trailing Win" in status or "trailing_win" in status.lower():
    return "🔄 Trailing Win ✅"
 if "Trailing شغال" in status or "trailing_open" in status.lower():
    return "🔄 Trailing شغال"
 if "TP2" in status:
    return "🏁 TP2 Hit"
 if "TP1" in status and "مغلقة" in status:
    return "✅ TP1 فقط"
 if "TP1" in status:
    return "✅ TP1 Hit"
 if "SL" in status and "مغلقة" in status:
    return "❌ SL Hit"
 if "Breakeven" in status or "breakeven" in status.lower():
    return "🔒 Breakeven"
 if "Expired" in status or "expired" in status.lower():
    return "⏳ Expired"
 if current_move > 0.30:
    return "🟢 ربح"
 if current_move < -0.30:
    return "🔴 خسارة"
 return "🟡 تعادل"

def build_track_tradingview_link(symbol: str) -> str:
 base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
 tv_symbol = f"OKX:{base}USDT.P"
 return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"

def load_registered_trade_for_alert(alert: dict):
 if not r or not alert:
    return None
 try:
    symbol = alert.get("symbol", "")
    candle_time = int(_safe_float(alert.get("candle_time"), 0))
    trade_key = f"trade:futures:long:{symbol}:{candle_time}"
    raw = r.get(trade_key)
    if raw:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    history_key = f"trade_history:futures:long:{symbol}:{candle_time}"
    raw = r.get(history_key)
    if raw:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    return None
 except Exception as e:
    logger.warning(f"load_registered_trade_for_alert error: {e}")
    return None

def format_official_trade_status(trade: dict) -> str:
 if not trade:
    return ""
 try:
    status = str(trade.get("status", "") or "").lower()
    result = str(trade.get("result", "") or "").lower()
    tp1_hit = bool(trade.get("tp1_hit", False))
    tp2_hit = bool(trade.get("tp2_hit", False))
    trailing_active = bool(trade.get("trailing_active", False))

    if status == "pending_pullback":
        return "⏳ انتظار Pullback"
    if status in ("tp2_partial", "trailing_open", "trailing") or (tp2_hit and trailing_active):
        return "🔄 Trailing شغال (20% متبقي)"
    if status == "partial":
        return "✅ TP1 Hit / الصفقة جزئية (40% مغلق)"
    if result == "trailing_win":
        return "🔄 Trailing Win ✅ (TP1+TP2+Trailing)"
    if result == "tp2_win":
        return "🏁 TP2 Hit ✅ (TP1+TP2+Trailing)"
    if result == "tp1_win":
        return "✅ TP1 فقط (40%) | الباقي Breakeven"
    if result == "loss":
        return "❌ SL Hit"
    if result == "breakeven":
        return "🔒 Breakeven (TP1 ثم Entry)"
    if result == "expired":
        if tp1_hit:
            return "⏳ Expired بعد TP1"
        return "⏳ Expired"
    if result == "pending_expired":
        return "⚫ Pullback لم يتفعل"
    if status == "open":
        return "⏳ Open"
    if tp1_hit:
        return "✅ TP1 Hit"
    return ""
 except Exception:
    return ""

def format_price_dynamic(price) -> str:
    try:
        v = float(price)
        if v <= 0:
            return "—"
        if v >= 100:
            return f"{v:.4f}"
        if v >= 1:
            return f"{v:.6f}"
        if v >= 0.01:
            return f"{v:.8f}"
        if v >= 0.0001:
            return f"{v:.10f}"
        return f"{v:.12f}"
    except (TypeError, ValueError):
        return "—"


def validate_signal_prices(candidate: dict) -> bool:
    symbol = candidate.get("symbol", "?")
    entry        = _safe_float(candidate.get("entry"), 0.0)
    sl           = _safe_float(candidate.get("sl"), 0.0)
    tp1          = _safe_float(candidate.get("tp1"), 0.0)
    tp2          = _safe_float(candidate.get("tp2"), 0.0)
    market_price = _safe_float(
        candidate.get("market_entry", candidate.get("entry")), 0.0
    )
    if entry <= 0:
        logger.warning(f"validate_signal_prices: invalid_price_data | {symbol} | entry={entry}")
        return False
    if sl <= 0:
        logger.warning(f"validate_signal_prices: invalid_price_data | {symbol} | sl={sl}")
        return False
    if tp1 <= 0:
        logger.warning(f"validate_signal_prices: invalid_price_data | {symbol} | tp1={tp1}")
        return False
    if tp2 <= 0:
        logger.warning(f"validate_signal_prices: invalid_price_data | {symbol} | tp2={tp2}")
        return False
    if market_price <= 0:
        logger.warning(f"validate_signal_prices: zero_price_signal | {symbol} | market_price={market_price}")
        return False
    return True


def _fmt_price(value) -> str:
    v = _safe_float(value, 0.0)
    if v <= 0:
        return "—"
    if v >= 100:
        return f"{v:.4f}"
    if v >= 1:
        return f"{v:.6f}"
    return f"{v:.8f}"


# =========================
# NEW HELPER: format_trade_status_line
# =========================
def format_trade_status_line(trade: dict) -> str:
    if not trade:
        return "📌 حالة الصفقة: ⚪ غير معروفة"

    status = str(trade.get("status", "") or "").lower()
    result = str(trade.get("result", "") or "").lower()
    tp1_hit = bool(trade.get("tp1_hit", False))
    tp2_hit = bool(trade.get("tp2_hit", False))
    trailing_active = bool(trade.get("trailing_active", False))
    sl_moved_to_entry = bool(trade.get("sl_moved_to_entry", False))
    trailing_high = _safe_float(trade.get("trailing_high"), 0.0)
    trailing_sl  = _safe_float(trade.get("trailing_sl"), 0.0)

    if status == "pending_pullback":
        return "📌 حالة الصفقة: ⏳ معلّقة | انتظار Pullback"

    if status == "open":
        return "📌 حالة الصفقة: 🟢 مفتوحة"

    if status == "partial":
        line = "📌 حالة الصفقة: 🟡 جزئية | ✅ TP1 (40% مغلق)"
        if sl_moved_to_entry:
            line += " | 🔒 SL → Entry"
        return line

    if status in ("tp2_partial", "trailing_open", "trailing") or (tp2_hit and trailing_active):
        line = "📌 حالة الصفقة: 🔵 جزئية | 🏁 TP2 (40% مغلق) | 🔄 Trailing شغال (20%)"
        if trailing_high > 0:
            line += f"\n   📈 أعلى سعر: {_fmt_price(trailing_high)}"
        if trailing_sl > 0:
            line += f" | 🛑 Trailing SL: {_fmt_price(trailing_sl)}"
        return line

    if status == "closed":
        if result == "trailing_win":
            t_exit = _safe_float(trade.get("trailing_exit_price"), 0.0)
            line = "📌 حالة الصفقة: 🟢 مغلقة | 🔄 Trailing Win (TP1+TP2+Trailing)"
            if t_exit > 0:
                line += f" | خروج: {_fmt_price(t_exit)}"
            return line
        if result == "tp2_win":
            return "📌 حالة الصفقة: 🟢 مغلقة | 🏁 TP2 (TP1+TP2+Trailing)"
        if result == "tp1_win":
            if bool(trade.get("protected_breakeven_exit", False)):
                return "📌 حالة الصفقة: 🟢 مغلقة | ✅ TP1 | الباقي Breakeven على Entry"
            return "📌 حالة الصفقة: 🟢 مغلقة | ✅ TP1 فقط (40%)"
        if result == "loss":
            return "📌 حالة الصفقة: 🔴 مغلقة | ❌ SL"
        if result == "breakeven":
            return "📌 حالة الصفقة: ⚪ مغلقة | 🔒 Breakeven (TP1 ثم Entry)"
        if result == "expired":
            if tp1_hit:
                return "📌 حالة الصفقة: ⚪ مغلقة | ⏳ Expired بعد TP1"
            return "📌 حالة الصفقة: ⚫ مغلقة | ⏳ Expired"
        if result == "pending_expired":
            return "📌 حالة الصفقة: ⚫ مغلقة | Pullback لم يتفعل"
        return "📌 حالة الصفقة: ⚫ مغلقة"

    return "📌 حالة الصفقة: ⚪ غير معروفة"


def calculate_trade_lifecycle_pnl_for_track(trade: dict, current_price: float = None) -> dict:
    """Display-only weighted PnL using the 40/40/20 exit plan.

    This does not change tracking decisions. It only reports the effective PnL:
    - before TP1: 100% of position moves with current price
    - after TP1: 40% locked at TP1 + 60% live
    - after TP2/trailing: 40% TP1 + 40% TP2 + 20% live/trailing exit
    """
    try:
        if not isinstance(trade, dict):
            return {"available": False}

        side = str(trade.get("side", "long") or "long").lower()
        entry = _safe_float(
            trade.get("effective_entry")
            or trade.get("execution_entry")
            or trade.get("recommended_entry")
            or trade.get("pullback_entry")
            or trade.get("entry"),
            0.0,
        )
        tp1 = _safe_float(trade.get("tp1"), 0.0)
        tp2 = _safe_float(trade.get("tp2"), 0.0)
        price = _safe_float(current_price, 0.0)
        if price <= 0:
            price = _safe_float(
                trade.get("current_price")
                or trade.get("last_price")
                or trade.get("trailing_exit_price")
                or trade.get("exit_price"),
                0.0,
            )

        leverage = _safe_float(trade.get("leverage"), TRACK_LEVERAGE) or TRACK_LEVERAGE
        status = str(trade.get("status", "") or "").lower()
        result = str(trade.get("result", "") or "").lower()
        tp1_hit = bool(trade.get("tp1_hit", False)) or result in ("tp1_win", "tp2_win", "trailing_win")
        tp2_hit = (
            bool(trade.get("tp2_hit", False))
            or result in ("tp2_win", "trailing_win")
            or status in ("tp2_partial", "trailing_open", "trailing")
        )
        trailing_active = bool(trade.get("trailing_active", False)) or status in ("trailing_open", "trailing")

        if status == "pending_pullback" or entry <= 0:
            return {"available": False, "phase": "pending_pullback"}

        def pct_to(target):
            target = _safe_float(target, 0.0)
            if target <= 0 or entry <= 0:
                return 0.0
            if side == "short":
                return ((entry - target) / entry) * 100.0
            return ((target - entry) / entry) * 100.0

        current_raw = pct_to(price) if price > 0 else 0.0
        tp1_raw = pct_to(tp1)
        tp2_raw = pct_to(tp2)

        if result == "loss":
            raw = pct_to(trade.get("sl") or trade.get("exit_price"))
            phase = "loss"
        elif result == "breakeven":
            raw = 0.0
            phase = "breakeven"
        elif result == "tp1_win":
            raw = tp1_raw * 0.40
            phase = "tp1_closed"
        elif result == "tp2_win":
            raw = (tp1_raw * 0.40) + (tp2_raw * 0.40) + (tp2_raw * 0.20)
            phase = "tp2_closed"
        elif result == "trailing_win":
            trailing_exit = _safe_float(trade.get("trailing_exit_price") or trade.get("exit_price"), price)
            raw = (tp1_raw * 0.40) + (tp2_raw * 0.40) + (pct_to(trailing_exit) * 0.20)
            phase = "trailing_closed"
        elif tp2_hit or trailing_active:
            raw = (tp1_raw * 0.40) + (tp2_raw * 0.40) + (current_raw * 0.20)
            phase = "trailing_live"
        elif tp1_hit or status == "partial":
            raw = (tp1_raw * 0.40) + (current_raw * 0.60)
            phase = "tp1_live"
        else:
            raw = current_raw
            phase = "open_before_tp1"

        return {
            "available": True,
            "phase": phase,
            "raw_pct": round(raw, 4),
            "leveraged_pct": round(raw * leverage, 4),
            "current_raw_pct": round(current_raw, 4),
            "current_leveraged_pct": round(current_raw * leverage, 4),
            "leverage": leverage,
        }
    except Exception as e:
        logger.warning(f"calculate_trade_lifecycle_pnl_for_track error: {e}")
        return {"available": False}


def _is_track_trade_closed(trade: dict) -> bool:
    """Display-only helper: determine whether Track should show final realized outcome."""
    try:
        if not isinstance(trade, dict) or not trade:
            return False
        status = str(trade.get("status", "") or "").lower()
        result = str(trade.get("result", "") or "").lower()
        if status in ("closed", "expired"):
            return True
        return result in (
            "loss", "tp1_win", "tp2_win", "trailing_win",
            "breakeven", "protected_breakeven", "expired", "pending_expired",
        )
    except Exception:
        return False


def _format_track_tp1_result_line(trade: dict) -> str:
    try:
        result = str(trade.get("result", "") or "").lower()
        tp1_hit = bool(trade.get("tp1_hit", False)) or result in ("tp1_win", "tp2_win", "trailing_win")
        return "تحقق" if tp1_hit else "لم يتحقق"
    except Exception:
        return "غير معروف"


def format_final_result_block_for_track(trade: dict, favorable_pct: float = 0.0, adverse_pct: float = 0.0) -> str:
    """Final-result Track block for closed trades.

    Closed trades should not show floating/current PnL. This block separates:
    - final realized result
    - max favorable move during the trade
    - max adverse move during the trade
    """
    try:
        close_type = _execution_close_type_for_trade(trade) if isinstance(trade, dict) else "Closed"
        close_icon = "❌" if close_type in ("Direct SL", "SL Hit") else "🏁" if "TP2" in close_type else "🎯" if "TP1" in close_type else "🛡️" if "BE" in close_type or "Breakeven" in close_type else "⌛" if "Expired" in close_type else "📌"

        final_pct = _execution_final_pnl_pct(trade) if isinstance(trade, dict) else None
        if final_pct is None:
            lifecycle = calculate_trade_lifecycle_pnl_for_track(trade, None) if isinstance(trade, dict) else {"available": False}
            if lifecycle.get("available"):
                final_pct = _safe_float(lifecycle.get("leveraged_pct"), 0.0)

        final_line = f"{final_pct:+.2f}%" if final_pct is not None else "—"
        max_up = _safe_float(favorable_pct, 0.0) * TRACK_LEVERAGE
        max_down = _safe_float(adverse_pct, 0.0) * TRACK_LEVERAGE
        tp1_line = _format_track_tp1_result_line(trade if isinstance(trade, dict) else {})

        return (
            "📌 <b>النتيجة النهائية:</b>\n"
            f"{close_icon} {html.escape(str(close_type))}\n\n"
            "💰 <b>Final Result 40/40/20:</b>\n"
            f"{final_line}\n\n"
            "📉 <b>أقصى صعود أثناء الصفقة:</b>\n"
            f"{max_up:+.2f}%\n\n"
            "📉 <b>أقصى هبوط:</b>\n"
            f"-{abs(max_down):.2f}%\n\n"
            "🎯 <b>TP1:</b>\n"
            f"{tp1_line}"
        )
    except Exception as e:
        logger.warning(f"format_final_result_block_for_track error: {e}")
        return "📌 <b>النتيجة النهائية:</b>\n⚠️ تعذر حساب النتيجة النهائية"

def format_lifecycle_pnl_block_for_track(trade: dict, lifecycle_pnl: dict, current_price: float) -> str:
    """Clear display-only 40/40/20 financial status for Track messages."""
    try:
        if not isinstance(trade, dict) or not trade:
            return (
                "📊 <b>الحالة المالية الفعلية 40/40/20</b>\n"
                "⚠️ لا توجد صفقة مسجلة — الحساب تقديري فقط"
            )

        status = str(trade.get("status", "") or "").lower()
        result = str(trade.get("result", "") or "").lower()
        if status == "pending_pullback":
            return (
                "📊 <b>الحالة المالية الفعلية 40/40/20</b>\n"
                "⏳ لم يتم تفعيل الدخول بعد — لا يوجد PnL فعلي"
            )

        tp1_hit = bool(trade.get("tp1_hit", False)) or result in ("tp1_win", "tp2_win", "trailing_win")
        tp2_hit = bool(trade.get("tp2_hit", False)) or result in ("tp2_win", "trailing_win") or status in ("tp2_partial", "trailing_open", "trailing")
        trailing_active = bool(trade.get("trailing_active", False)) or status in ("trailing_open", "trailing")
        sl_moved_to_entry = bool(trade.get("sl_moved_to_entry", False))
        protected_exit = bool(trade.get("protected_breakeven_exit", False))

        tp1_line = "✅ اتضرب | 40% اتقفل" if tp1_hit else "⏳ لم يضرب"
        tp2_line = "✅ اتضرب | 40% إضافي اتقفل" if tp2_hit else "⏳ لم يضرب"

        if result == "trailing_win":
            trailing_line = "✅ مغلق على Trailing"
        elif trailing_active:
            trailing_line = "🔄 شغال Trailing"
        elif tp2_hit:
            trailing_line = "🔄 بدأ بعد TP2"
        else:
            trailing_line = "⏳ لم يبدأ"

        if result == "loss":
            sl_line = "✅ اتضرب"
        elif result == "breakeven" or protected_exit:
            sl_line = "🔒 خرج على Entry بعد TP1"
        elif sl_moved_to_entry:
            sl_line = "🔒 على Entry"
        else:
            sl_line = "⏳ لم يضرب"

        if lifecycle_pnl.get("available"):
            raw = _safe_float(lifecycle_pnl.get("raw_pct"), 0.0)
            leveraged = _safe_float(lifecycle_pnl.get("leveraged_pct"), 0.0)
            pnl_line = f"{raw:+.2f}%"
            lev_line = f"{leveraged:+.2f}%"
        else:
            pnl_line = "—"
            lev_line = "—"

        return (
            "📊 <b>الحالة المالية الفعلية 40/40/20</b>\n"
            f"• 🎯 TP1: {tp1_line}\n"
            f"• 🏁 TP2: {tp2_line}\n"
            f"• 🔄 الجزء المتبقي 20%: {trailing_line}\n"
            f"• 🛑 SL: {sl_line}\n"
            f"• 💵 السعر الحالي: {_fmt_price(current_price)}\n"
            f"• 💰 الربح/الخسارة الفعلي: {pnl_line}\n"
            f"• ⚡ بعد الرافعة: {lev_line}"
        )
    except Exception as e:
        logger.warning(f"format_lifecycle_pnl_block_for_track error: {e}")
        return "📊 <b>الحالة المالية الفعلية 40/40/20</b>\n⚠️ تعذر حساب الربح الفعلي"

def build_track_message(alert: dict) -> str:
 try:
    symbol = clean_symbol_for_message(alert.get("symbol", "Unknown"))
    mode = alert.get("mode") or alert.get("market_mode", "")
    avg_planned = _safe_float(alert.get("average_planned_entry"), 0.0)
    entry = _safe_float(alert.get("entry"), 0.0)
    recommended_entry = _safe_float(alert.get("recommended_entry"), 0.0)
    pullback_entry = _safe_float(alert.get("pullback_entry"), 0.0)
    market_entry = _safe_float(alert.get("market_entry"), 0.0)
    entry_mode = alert.get("entry_mode", "market")
    pullback_triggered = bool(alert.get("pullback_triggered", False))
    sl = _safe_float(alert.get("sl"), 0.0)
    tp1 = _safe_float(alert.get("tp1"), 0.0)
    tp2 = _safe_float(alert.get("tp2"), 0.0)
    candle_time = int(_safe_float(alert.get("candle_time"), 0))
    created_ts = int(_safe_float(alert.get("created_ts"), candle_time))
    current_price = get_last_price(alert.get("symbol", ""))

    if mode == MODE_RECOVERY_LONG and avg_planned > 0:
        effective_entry = avg_planned
    elif entry_mode == "pullback_pending":
        if pullback_triggered:
            effective_entry = pullback_entry if pullback_entry > 0 else entry
        else:
            effective_entry = pullback_entry if pullback_entry > 0 else recommended_entry if recommended_entry > 0 else entry
    else:
        effective_entry = market_entry if market_entry > 0 else entry

    favorable_price, favorable_pct, adverse_price, adverse_pct = get_max_move_since_alert(
        symbol=alert.get("symbol", ""),
        since_ts=candle_time,
        entry=effective_entry,
        side="long",
    )

    trade = load_registered_trade_for_alert(alert)
    status_line = format_trade_status_line(trade) if trade else format_trade_status_line(None)

    status_info = resolve_alert_official_or_estimated_status(alert)
    display_status = status_info["display_status"]
    is_official = status_info["is_official"]

    if entry_mode == "pullback_pending" and not pullback_triggered:
        display_status = "⏳ Pending Pullback"
        is_official = False

    if is_official:
        logger.info(f"Track using official status for {alert.get('alert_id')}: {display_status}")
    else:
        logger.info(f"Track using estimated status for {alert.get('alert_id')}: {display_status}")

    duration_seconds = max(0, int(time.time()) - created_ts)
    duration_h = duration_seconds // 3600
    duration_m = (duration_seconds % 3600) // 60
    current_move = 0.0
    if effective_entry > 0 and current_price > 0:
        current_move = round(((current_price - effective_entry) / effective_entry) * 100, 2)
    state_badge = get_track_state_badge(display_status, current_move)
    tv_link = build_track_tradingview_link(alert.get("symbol", ""))
    leveraged_current = round(current_move * TRACK_LEVERAGE, 2)
    lifecycle_pnl = calculate_trade_lifecycle_pnl_for_track(trade, current_price) if trade else {"available": False}
    track_trade_closed = _is_track_trade_closed(trade) if trade else False
    leveraged_favorable = round(favorable_pct * TRACK_LEVERAGE, 2)
    leveraged_adverse = round(adverse_pct * TRACK_LEVERAGE, 2)
    recovery_extra = ""
    if mode == MODE_RECOVERY_LONG:
        entry1 = _safe_float(alert.get("entry1"), entry)
        entry2 = _safe_float(alert.get("entry2"), 0.0)
        recovery_extra = (
            f"\n🔄 Mode: Recovery Long\n"
            f"• Entry 1: {entry1:.6f}\n"
            f"• Entry 2: {entry2:.6f}\n"
            f"• Avg Planned Entry: {avg_planned:.6f}"
        )
    msg = (
        f"📌 <b>Alert Track</b>\n\n"
        f"🪙 {html.escape(symbol)}\n"
        f"📈 Long\n"
        f"⏱ {html.escape(str(alert.get('timeframe', TIMEFRAME)))}\n"
        f"{status_line}\n"
        f"{recovery_extra}\n"
        f"📍 <b>Entry Mode:</b> {'Market' if entry_mode == 'market' else 'Pullback Pending'}\n"
        f"{format_final_result_block_for_track(trade, favorable_pct, adverse_pct) if track_trade_closed else format_lifecycle_pnl_block_for_track(trade, lifecycle_pnl, current_price)}\n"
    )
    if entry_mode == "pullback_pending" and not pullback_triggered:
        msg += "⏳ <b>لم يتم تفعيل دخول البول باك بعد</b>، الحساب تقديري على سعر البول باك المخطط.\n"
    msg += (
        f"💰 Signal Entry: {entry:.6f}\n"
    )
    if entry_mode == "pullback_pending" and market_entry > 0:
        msg += f"💵 سعر السوق عند الإرسال: {market_entry:.6f}\n"
    if recommended_entry > 0 and recommended_entry != entry:
        msg += f"📌 Recommended Entry: {recommended_entry:.6f}\n"
    if effective_entry != entry and mode != MODE_RECOVERY_LONG and entry_mode != "market":
        msg += f"⚡ Effective Entry: {effective_entry:.6f}\n"
    msg += (
        f"🛑 SL: {sl:.6f}\n"
        f"🎯 TP1: {tp1:.6f} | إغلاق 40%\n"
        f"🏁 TP2: {tp2:.6f} | إغلاق 40%\n"
        f"🔄 بعد TP2: Trailing Stop 20% ({TRAILING_PCT}% تحت الـ High)\n"
        f"🛡 بعد TP1: نقل SL إلى Entry\n\n"
        f"{state_badge}\n"
        f"📊 {html.escape(display_status)}"
    )
    if is_official:
        msg += "\n🏛️ <b>حالة رسمية</b> (مستندة إلى سجل الصفقة)"
    else:
        msg += "\n⚠️ <b>حالة تقديرية</b> لعدم توفر صفقة مسجلة"

    if trade:
        trailing_active = bool(trade.get("trailing_active", False))
        tp2_hit_flag = bool(trade.get("tp2_hit", False))
        t_high = _safe_float(trade.get("trailing_high"), 0.0)
        t_sl   = _safe_float(trade.get("trailing_sl"), 0.0)
        t_pct  = _safe_float(trade.get("trailing_pct"), TRAILING_PCT)
        if trailing_active and tp2_hit_flag:
            gain_from_entry = 0.0
            if effective_entry > 0 and t_high > 0:
                gain_from_entry = ((t_high - effective_entry) / effective_entry) * 100
            msg += (
                f"\n\n🔄 <b>Trailing Stop شغال (20%)</b>\n"
                f"• أعلى سعر وصله: {_fmt_price(t_high)} (+{gain_from_entry:.2f}%)\n"
                f"• Trailing SL الحالي: {_fmt_price(t_sl)} ({t_pct:.1f}% تحت الـ High)"
            )
    if trade and bool(trade.get("protected_on_block", False)):
        protected_sl = _safe_float(trade.get("protected_sl") or trade.get("sl"), 0.0)
        protection_type = str(trade.get("block_protection_type", "") or "")
        if protection_type == "risk_compression":
            msg += (
                f"\n\n🛡️ <b>حماية BLOCK مفعّلة</b>\n"
                f"🟡 تم تقليل المخاطرة بسبب ضغط السوق\n"
                f"🔒 SL الحالي: {_fmt_price(protected_sl)}\n"
                f"⚠️ Tracking فقط، لم يتم تحديث OKX مباشرة."
            )
        else:
            msg += (
                f"\n\n🛡️ <b>حماية BLOCK مفعّلة</b>\n"
                f"🔒 SL الحالي: {_fmt_price(protected_sl)} | Entry +{PROTECT_ON_BLOCK_BUFFER_PCT:.2f}%\n"
                f"⚠️ Tracking فقط، لم يتم تحديث OKX مباشرة."
            )

    if not track_trade_closed:
        msg += (
            f"\n💵 السعر الحالي: {current_price:.6f}\n"
            f"🔢 الرافعة: {TRACK_LEVERAGE:.0f}x\n"
            f"📈 التغير الحالي: {current_move:.2f}% | بعد الرافعة: {leveraged_current:.2f}%\n"
            f"🚀 أقصى صعود: {favorable_price:.6f} | +{favorable_pct:.2f}% | بعد الرافعة: +{leveraged_favorable:.2f}%\n"
            f"📉 أقصى هبوط ضدك: {adverse_price:.6f} | -{adverse_pct:.2f}% | بعد الرافعة: -{leveraged_adverse:.2f}%\n"
        )
    msg += (
        f"⏳ المدة: {duration_h}h {duration_m}m\n\n"
        f'🔗 <a href="{html.escape(tv_link, quote=True)}">فتح الشارت على TradingView - 15m / 1H</a>'
    )
    return msg
 except Exception as e:
    logger.error(f"build_track_message error: {e}")
    return "❌ حصل خطأ أثناء متابعة الإشارة"

def build_track_reply_markup(alert_id: str) -> dict:
 return {
    "inline_keyboard": [
        [
            {
                "text": "📌 Track",
                "callback_data": f"track_long:{alert_id}"
            }
        ]
    ]
 }

def handle_callback_query(callback_query: dict):
 try:
    callback_id = callback_query.get("id", "")
    data = callback_query.get("data", "") or ""
    message = callback_query.get("message") or {}
    message_id = str(message.get("message_id", ""))
    chat_id = str((message.get("chat") or {}).get("id", "") or "")
    if data.startswith("help:"):
        section = data.split(":", 1)[1].strip().lower()
        answer_callback_query(callback_id, "فتح القسم")
        if not chat_id:
            return
        section_map = {
            "execution": build_help_execution_message,
            "normal": build_help_normal_message,
            "exec_intelligence": build_help_exec_intelligence_message,
            "market_intelligence": build_help_market_intelligence_message,
            "diagnostics": build_help_diagnostics_message,
            "okx": build_help_okx_message,
            "admin": build_help_admin_message,
            "info": build_help_info_message,
        }
        builder = section_map.get(section)
        if builder:
            send_telegram_reply(chat_id, builder())
        else:
            send_telegram_reply(chat_id, build_help_message(), reply_markup=build_help_inline_keyboard())
        return
    if not data.startswith("track_long:"):
        answer_callback_query(callback_id, "زر غير مدعوم")
        return
    alert_id = data.split(":", 1)[1].strip()
    if not r:
        answer_callback_query(callback_id, "Redis غير متصل الآن")
        logger.warning("handle_callback_query: Redis not available")
        return
    if should_ignore_track_callback(chat_id, message_id, alert_id):
        answer_callback_query(callback_id, "تم استلام الطلب بالفعل")
        logger.info(f"Ignored duplicate track: {alert_id}")
        return
    alert = load_alert_snapshot(alert_id)
    if not alert and message_id:
        alert = load_alert_snapshot_by_message_id(message_id)
    if not alert:
        answer_callback_query(callback_id, "بيانات الإشارة غير متاحة أو انتهت صلاحيتها")
        logger.warning(f"No alert data for {alert_id} (message {message_id})")
        return
    answer_callback_query(callback_id, "جاري جلب نتيجة الإشارة")
    if chat_id:
        send_telegram_reply(chat_id, build_track_message(alert))
        logger.info(f"Track sent for {alert_id} to chat {chat_id}")
 except Exception as e:
    logger.error(f"handle_callback_query error: {e}")
    try:
        callback_id = callback_query.get("id", "")
        answer_callback_query(callback_id, "حصل خطأ")
    except Exception:
        pass

# =====================
# OKX DATA
# =====================
def is_excluded_symbol(symbol: str) -> bool:
 excluded_prefixes = ("USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP", "USD0")
 if symbol.startswith(excluded_prefixes):
    return True
 base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "")
 if len(base) > 20:
    return True
 return False

def extract_24h_quote_volume(ticker: dict) -> float:
 fields = ["volCcy24h", "turnover24h", "quoteVolume", "vol24h"]
 for field in fields:
    value = ticker.get(field)
    if value is None:
        continue
    try:
        return float(value)
    except Exception:
        continue
 return 0.0

def extract_24h_change_percent(ticker: dict) -> float:
 try:
    last = float(ticker.get("last", 0) or 0)
    prev = ticker.get("prevPx")
    if prev is not None:
        prev = float(prev)
        if prev > 0:
            return round(((last - prev) / prev) * 100, 2)
    open24h = ticker.get("open24h")
    if open24h is not None:
        open24h = float(open24h)
        if open24h > 0:
            return round(((last - open24h) / open24h) * 100, 2)
    sod = ticker.get("sodUtc0") or ticker.get("sodUtc8")
    if sod is not None:
        sod = float(sod)
        if sod > 0:
            return round(((last - sod) / sod) * 100, 2)
    return 0.0
 except Exception:
    return 0.0

def _safe_float(value, default=0.0):
 try:
    if value is None:
        return default
    value = float(value)
    if value != value:
        return default
    if value in (float("inf"), float("-inf")):
        return default
    return value
 except Exception:
    return default

safe_float = _safe_float  # compatibility alias for older report helpers

def get_signal_row(df):
 try:
    if df is None or df.empty:
        return None
    if len(df) == 1:
        return df.iloc[-1]
    if "confirm" in df.columns:
        last = df.iloc[-1]
        confirm_value = _safe_float(last.get("confirm"), 0.0)
        if int(confirm_value) == 1:
            return last
        return df.iloc[-2]
    return df.iloc[-2]
 except Exception as e:
    logger.warning(f"get_signal_row error: {e}")
    try:
        if df is not None and not df.empty and len(df) >= 2:
            return df.iloc[-2]
        if df is not None and not df.empty:
            return df.iloc[-1]
    except Exception:
        pass
    return None

def get_signal_candle_time(df):
 try:
    if df is None or df.empty:
        return int(time.time() // (15 * 60))
    signal_row = get_signal_row(df)
    if signal_row is None:
        return int(time.time() // (15 * 60))
    ts = int(signal_row["ts"])
    if ts > 10_000_000_000:
        return ts // 1000
    return ts
 except Exception:
    return int(time.time() // (15 * 60))

def get_ranked_pairs():
 try:
    res = requests.get(
        OKX_TICKERS_URL, params={"instType": "SWAP"}, timeout=20,
    ).json()
    data = res.get("data", [])
    logger.info(f"Fetched {len(data)} futures pairs")
    filtered = []
    for item in data:
        symbol = item.get("instId", "")
        if "USDT" not in symbol:
            continue
        if not symbol.endswith("-SWAP"):
            continue
        if is_excluded_symbol(symbol):
            continue
        vol_24h = extract_24h_quote_volume(item)
        if vol_24h < MIN_24H_QUOTE_VOLUME:
            continue
        change_24h = extract_24h_change_percent(item)
        item["_rank_volume_24h"] = vol_24h
        item["_rank_change_24h"] = change_24h
        filtered.append(item)
    by_volume = sorted(filtered, key=lambda x: x.get("_rank_volume_24h", 0), reverse=True)
    by_momentum = sorted(filtered, key=lambda x: x.get("_rank_change_24h", 0), reverse=True)
    by_reversal = sorted(filtered, key=lambda x: x.get("_rank_change_24h", 0), reverse=False)
    n_vol = int(SCAN_LIMIT * 0.35)
    n_momentum = int(SCAN_LIMIT * 0.25)
    n_reversal = int(SCAN_LIMIT * 0.25)
    seen = set()
    merged = []
    for item in by_volume[:n_vol]:
        sid = item.get("instId", "")
        if sid not in seen:
            seen.add(sid)
            merged.append(item)
    positive_momentum_count = 0
    for item in by_momentum[:n_momentum * 2]:
        if positive_momentum_count >= n_momentum:
            break
        sid = item.get("instId", "")
        if sid not in seen and item.get("_rank_change_24h", 0) > 0:
            seen.add(sid)
            merged.append(item)
            positive_momentum_count += 1
    negative_reversal_count = 0
    for item in by_reversal[:n_reversal * 2]:
        if negative_reversal_count >= n_reversal:
            break
        sid = item.get("instId", "")
        change = float(item.get("_rank_change_24h", 0) or 0)
        if sid not in seen and change < 0:
            seen.add(sid)
            merged.append(item)
            negative_reversal_count += 1
    for item in by_volume:
        if len(merged) >= SCAN_LIMIT:
            break
        sid = item.get("instId", "")
        if sid not in seen:
            seen.add(sid)
            merged.append(item)
    merged = merged[:SCAN_LIMIT]
    logger.info(f"After liquidity filter: {len(filtered)}")
    logger.info(f"Using merged ranked pairs for long scan: {len(merged)}")
    return merged
 except Exception as e:
    logger.error(f"get_ranked_pairs error: {e}")
    return []

def compute_rsi(series, period=14):
 delta = series.diff()
 gain = delta.clip(lower=0)
 loss = (-delta).clip(lower=0)
 avg_gain = gain.ewm(com=period - 1, adjust=False, min_periods=period).mean()
 avg_loss = loss.ewm(com=period - 1, adjust=False, min_periods=period).mean()
 rs = avg_gain / avg_loss.replace(0, pd.NA)
 rsi = 100 - (100 / (1 + rs))
 return rsi.fillna(50)

def compute_atr(df, period=14):
 high_low = df["high"] - df["low"]
 high_close = (df["high"] - df["close"].shift()).abs()
 low_close = (df["low"] - df["close"].shift()).abs()
 tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
 return tr.rolling(period).mean()

def compute_bollinger_bands(series, period=20, std_mult=2):
 ma = series.rolling(period).mean()
 std = series.rolling(period).std()
 upper = ma + (std * std_mult)
 lower = ma - (std * std_mult)
 return ma, upper, lower

def to_dataframe(data):
 if not data:
    return None
 df = pd.DataFrame(data, columns=[
        "ts", "open", "high", "low", "close", "volume",
        "volCcy", "volCcyQuote", "confirm"
 ])
 numeric_cols = [
        "ts", "open", "high", "low", "close",
        "volume", "volCcy", "volCcyQuote", "confirm"
 ]
 for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")
 df = df.sort_values("ts").reset_index(drop=True)
 df["ma"] = df["close"].rolling(20).mean()
 df["rsi"] = compute_rsi(df["close"])
 df["atr"] = compute_atr(df)
 df["bb_mid"], df["bb_upper"], df["bb_lower"] = compute_bollinger_bands(df["close"])
 typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
 vol = df["volume"].fillna(0).replace([float("inf"), -float("inf")], 0)
 tp_vol = typical_price * vol
 sum_tp_vol = tp_vol.rolling(20, min_periods=1).sum()
 sum_vol = vol.rolling(20, min_periods=1).sum()
 vwap = (sum_tp_vol / sum_vol.replace(0, pd.NA)).fillna(0)
 df["vwap"] = vwap.astype(float)
 ema12 = df["close"].ewm(span=12, adjust=False).mean()
 ema26 = df["close"].ewm(span=26, adjust=False).mean()
 macd_line = ema12 - ema26
 macd_signal = macd_line.ewm(span=9, adjust=False).mean()
 macd_hist = macd_line - macd_signal
 df["macd"] = macd_line.astype(float)
 df["macd_signal"] = macd_signal.astype(float)
 df["macd_hist"] = macd_hist.astype(float)
 return df

def get_candle_cache_key(symbol: str, timeframe: str, limit: int) -> str:
 return f"candles:long:{symbol}:{timeframe}:{limit}"

def get_candle_cache_ttl(timeframe: str) -> int:
 tf = str(timeframe).strip().lower()
 if tf == "15m":
    return CANDLE_CACHE_TTL_15M
 if tf == "1h":
    return CANDLE_CACHE_TTL_1H
 if tf == "4h":
    return CANDLE_CACHE_TTL_4H
 return CANDLE_CACHE_TTL_DEFAULT

def get_candles(symbol, timeframe="15m", limit=100):
 cache_key = get_candle_cache_key(symbol, timeframe, limit)
 cache_ttl = get_candle_cache_ttl(timeframe)
 if r:
    try:
        cached = r.get(cache_key)
        if cached:
            data = json.loads(cached)
            if isinstance(data, list) and data:
                logger.debug(f"{symbol} {timeframe} -> candles cache hit")
                return data
    except Exception as e:
        logger.warning(f"Candle cache read error on {symbol} {timeframe}: {e}")
 try:
    params = {"instId": symbol, "bar": timeframe, "limit": limit}
    res = requests.get(OKX_CANDLES_URL, params=params, timeout=20).json()
    data = res.get("data", [])
    if data and r:
        try:
            r.set(cache_key, json.dumps(data), ex=cache_ttl)
        except Exception as e:
            logger.warning(f"Candle cache write error on {symbol} {timeframe}: {e}")
    return data
 except Exception as e:
    logger.error(f"get_candles error on {symbol} {timeframe}: {e}")
    return []

def get_funding_rate(symbol):
 try:
    params = {"instId": symbol}
    res = requests.get(OKX_FUNDING_URL, params=params, timeout=10).json()
    data = res.get("data", [])
    if not data:
        return 0.0
    return float(data[0].get("fundingRate", 0))
 except Exception as e:
    logger.error(f"Funding rate error on {symbol}: {e}")
    return 0.0

# =========================
# PARALLEL CANDLE FETCH
# =========================
def fetch_candles_parallel(pairs, timeframe=TIMEFRAME, limit=100, max_workers=MAX_CANDLE_FETCH_WORKERS):
 result = {}
 if not pairs:
    return result
 def _fetch(pair):
    symbol = pair.get("instId", "")
    if not symbol:
        return symbol, []
    try:
        return symbol, get_candles(symbol, timeframe, limit)
    except Exception as e:
        logger.warning(f"parallel candle fetch error on {symbol}: {e}")
        return symbol, []
 workers = max(1, int(max_workers or 1))
 with ThreadPoolExecutor(max_workers=workers) as executor:
    futures = [executor.submit(_fetch, p) for p in pairs]
    for future in as_completed(futures):
        try:
            symbol, candles = future.result()
            if symbol:
                result[symbol] = candles or []
        except Exception as e:
            logger.warning(f"parallel candle future error: {e}")
 return result

# ==========================================
# PRE-FILTER BEFORE CANDLE FETCH
# ==========================================
def prefilter_pair_before_candles(pair_data: dict, current_mode: str) -> bool:
 try:
    symbol = pair_data.get("instId", "")
    if is_excluded_symbol(symbol):
        return False
    vol_24h = float(pair_data.get("_rank_volume_24h", 0) or 0)
    change_24h = float(pair_data.get("_rank_change_24h", 0) or 0)
    if vol_24h < MIN_24H_QUOTE_VOLUME:
        return False
    if current_mode == MODE_RECOVERY_LONG:
        if change_24h > 2.0:
            return False
        return True
    if change_24h >= 35.0:
        return False
    return True
 except Exception as e:
    logger.warning(f"prefilter_pair_before_candles error: {e}")
    return True

# ==========================================
# INDICATOR HELPERS
# ==========================================
def is_above_upper_bollinger(df) -> bool:
 try:
    signal_row = get_signal_row(df)
    if signal_row is None:
        return False
    close = _safe_float(signal_row["close"])
    upper = _safe_float(signal_row.get("bb_upper"), 0)
    if upper <= 0:
        return False
    return close > upper
 except Exception:
    return False

def get_change_4h(df) -> float:
 try:
    signal_row = get_signal_row(df)
    if signal_row is None:
        return 0.0
    idx = signal_row.name
    if idx is None or idx < 16:
        return 0.0
    current = _safe_float(df.iloc[idx]["close"])
    prev = _safe_float(df.iloc[idx - 16]["close"])
    if prev <= 0:
        return 0.0
    return round(((current - prev) / prev) * 100, 2)
 except Exception:
    return 0.0

def is_late_long_entry(dist_ma: float, breakout: bool, pre_breakout: bool) -> bool:
 try:
    if breakout or pre_breakout:
        return False
    return dist_ma > 6.2
 except Exception:
    return False

def is_exhausted_long_move(
 dist_ma: float,
 vol_ratio: float,
 candle_strength: float,
 breakout: bool,
 pre_breakout: bool,
) -> bool:
 try:
    if breakout or pre_breakout:
        return False
    if dist_ma > 5.5 and vol_ratio < 1.15:
        return True
    if dist_ma > 5.0 and candle_strength < 0.50:
        return True
    if dist_ma > 5.5 and vol_ratio < 1.45 and candle_strength < 0.55:
        return True
    return False
 except Exception:
    return False

# ==========================================
# NEW INDICATOR HELPER FUNCTIONS
# ==========================================
def get_vwap_distance_percent(df) -> float:
 try:
    row = get_signal_row(df)
    if row is None:
        return 0.0
    close = _safe_float(row.get("close"), 0.0)
    vwap = _safe_float(row.get("vwap"), 0.0)
    if vwap <= 0:
        return 0.0
    return round(((close - vwap) / vwap) * 100, 4)
 except Exception:
    return 0.0

def get_rsi_slope(df, bars=3) -> float:
 try:
    row = get_signal_row(df)
    if row is None or row.name is None:
        return 0.0
    idx = row.name
    if idx < bars:
        return 0.0
    rsi_now = _safe_float(row.get("rsi"), 50)
    rsi_prev = _safe_float(df.iloc[idx - bars].get("rsi"), rsi_now)
    return round(rsi_now - rsi_prev, 4)
 except Exception:
    return 0.0

def get_macd_hist_slope(df, bars=3) -> float:
 try:
    row = get_signal_row(df)
    if row is None or row.name is None:
        return 0.0
    idx = row.name
    if idx < bars:
        return 0.0
    now = _safe_float(row.get("macd_hist"), 0.0)
    prev = _safe_float(df.iloc[idx - bars].get("macd_hist"), now)
    return round(now - prev, 6)
 except Exception:
    return 0.0

def is_momentum_exhaustion_trap(
 market_state: str,
 opportunity_type: str,
 dist_ma: float,
 vwap_distance: float,
 rsi_now: float,
 rsi_slope: float,
 vol_ratio: float,
 candle_strength: float,
 macd_hist: float,
 macd_hist_slope: float,
 breakout: bool,
 pre_breakout: bool,
 breakout_quality: str,
 is_reverse: bool,
) -> dict:
 if is_reverse:
    return {"is_trap": False, "soft_trap": False, "reasons": [], "checks": 0}
 reasons = []
 checks = 0
 if dist_ma >= 3.2:
    reasons.append("far_from_ma")
    checks += 1
 if vwap_distance >= 2.0:
    reasons.append("far_from_vwap")
    checks += 1
 if rsi_now >= 64:
    reasons.append("rsi_hot")
    checks += 1
 if rsi_slope <= 0:
    reasons.append("rsi_slope_weak")
    checks += 1
 if vol_ratio >= 1.8:
    reasons.append("volume_spike")
    checks += 1
 if candle_strength >= 0.60:
    reasons.append("big_candle")
    checks += 1
 if macd_hist_slope < 0:
    reasons.append("macd_hist_falling")
    checks += 1
 if macd_hist < 0:
    reasons.append("macd_hist_negative")
    checks += 1
 is_trap = checks >= 5 and not pre_breakout and breakout_quality != "strong"
 soft_trap = checks >= 4
 return {"is_trap": is_trap, "soft_trap": soft_trap, "reasons": reasons, "checks": checks}

# ==========================================
# LATE BREAKOUT GUARD
# ==========================================
def evaluate_late_breakout_guard(
 df,
 breakout: bool,
 entry_timing: str,
 dist_ma: float,
 vol_ratio: float,
 rsi_now: float,
 mtf_confirmed: bool,
 market_state: str,
 breakout_quality: str = "",
 pre_breakout: bool = False,
 is_reverse: bool = False,
) -> dict:
 result = {
    "blocked": False,
    "retest_required": False,
    "upper_wick_ratio": 0.0,
    "reason": "",
    "warning": "",
 }
 if not breakout or is_reverse:
    return result
 strong_exception = (breakout_quality == "strong")
 signal_row = get_signal_row(df)
 if signal_row is None:
    return result
 high = _safe_float(signal_row["high"])
 low = _safe_float(signal_row["low"])
 open_ = _safe_float(signal_row["open"])
 close = _safe_float(signal_row["close"])
 candle_range = high - low
 if candle_range <= 0:
    return result
 upper_wick = high - max(open_, close)
 upper_wick_ratio = round(upper_wick / candle_range, 4) if candle_range > 0 else 0.0
 result["upper_wick_ratio"] = upper_wick_ratio
 is_late_entry = ("متأخر" in str(entry_timing)) or (dist_ma >= LATE_PUMP_DIST_MA) or (rsi_now >= 72)
 if not breakout:
    return result
 if pre_breakout and not is_late_entry:
    return result
 if market_state in ("bull_market", "alt_season"):
    if is_late_entry:
        if strong_exception and upper_wick_ratio < 0.35 and vol_ratio >= 1.8 and mtf_confirmed:
            result["warning"] = "اختراق متأخر لكن مؤكد بقوة في سوق صاعدة"
        else:
            result["retest_required"] = True
            result["reason"] = "اختراق متأخر في سوق صاعدة، يحتاج Retest"
        return result
    if upper_wick_ratio >= 0.45:
        if strong_exception and upper_wick_ratio < 0.55 and vol_ratio >= 1.6:
            result["warning"] = "رفض سعري علوي بعد الاختراق (مستوى خطر متوسط)"
        else:
            result["retest_required"] = True
            result["reason"] = "رفض سعري علوي كبير بعد الاختراق في سوق صاعدة"
        if upper_wick_ratio >= 0.65:
            result["blocked"] = True
            result["reason"] = "رفض سعري علوي حاد جداً، إشارة ملغية"
        return result
 elif market_state == "btc_leading":
    if is_late_entry:
        if vol_ratio >= 1.8 and mtf_confirmed and upper_wick_ratio < 0.35:
            result["warning"] = "اختراق متأخر في بيئة BTC Leading، لكن العوامل داعمة"
        else:
            result["retest_required"] = True
            result["reason"] = "اختراق متأخر في BTC Leading، يحتاج Retest"
        return result
    if upper_wick_ratio >= 0.55:
        result["retest_required"] = True
        result["reason"] = "wick رفض علوي كبير"
        if upper_wick_ratio >= 0.65 and not strong_exception:
            result["blocked"] = True
            result["reason"] = "رفض سعري علوي حاد"
    return result
 elif market_state == "mixed":
    if is_late_entry:
        if upper_wick_ratio >= 0.45 or rsi_now >= 70 or dist_ma >= 4.5:
            result["retest_required"] = True
            result["reason"] = "اختراق متأخر في سوق مختلط"
        return result
    else:
        if upper_wick_ratio >= 0.55:
            result["retest_required"] = True
            result["reason"] = "رفض سعري كبير"
        return result
 elif market_state == "risk_off":
    if is_late_entry and dist_ma >= 5.0 and upper_wick_ratio >= 0.55:
        result["retest_required"] = True
        result["reason"] = "اختراق متأخر في risk_off"
    return result
 if not result["blocked"] and not result["retest_required"]:
    if is_late_entry:
        result["warning"] = "اختراق متأخر، يُنصح بالحذر"
    if upper_wick_ratio >= 0.40:
        result["warning"] = "رفض سعري علوي بعد الاختراق"
 return result

# ==========================================
# LATE PUMP / BULL CONTINUATION GUARD
# ==========================================
def get_late_pump_risk(
 market_state: str,
 opportunity_type: str,
 dist_ma: float,
 rsi_now: float,
 vol_ratio: float,
 candle_strength: float,
 breakout: bool,
 pre_breakout: bool,
 breakout_quality: str,
 change_4h: float = 0.0,
 vwap_distance: float = 0.0,
 rsi_slope: float = 0.0,
 macd_hist_slope: float = 0.0,
 entry_timing: str = "",
 alt_mode: str = "",
) -> dict:
 _is_simple_late_et = False
 try:
    reasons = []
    is_continuation = str(opportunity_type or "").strip() in ("استمرار", "continuation")
    over_ma = dist_ma >= LATE_PUMP_DIST_MA
    hot_rsi = rsi_now >= LATE_PUMP_RSI
    pump_volume = vol_ratio >= LATE_PUMP_VOL_RATIO
    big_candle = candle_strength >= LATE_PUMP_CANDLE_STRENGTH
    fast_4h_move = change_4h >= 3.0
    if over_ma:
        reasons.append("overextended_from_ma")
    if hot_rsi:
        reasons.append("rsi_overheated")
    if pump_volume:
        reasons.append("volume_pump")
    if big_candle:
        reasons.append("strong_candle_chase")
    if fast_4h_move:
        reasons.append("fast_4h_move")
    if vwap_distance >= 2.4:
        reasons.append("far_from_vwap")
    if rsi_slope <= 0 and rsi_now >= 65:
        reasons.append("rsi_slope_weak")
    if macd_hist_slope < 0:
        reasons.append("macd_hist_falling")
    checks = sum([over_ma, hot_rsi, pump_volume, big_candle, fast_4h_move, (vwap_distance >= 2.4), (rsi_slope <= 0 and rsi_now >= 65), (macd_hist_slope < 0)])
    late_pump_risk = checks >= 3

    _et = str(entry_timing or "")
    _alt = str(alt_mode or "")

    _is_chase_or_very_late = "مطاردة حركة" in _et or "متأخر جدًا" in _et
    if _is_chase_or_very_late:
        _et_conds = sum([
            dist_ma >= 4.2,
            rsi_now >= 67,
            vol_ratio >= 1.8,
            candle_strength >= 0.62,
            "ضعيف" in _alt,
        ])
        if _et_conds >= 2:
            late_pump_risk = True
            if "late_entry_strong_conditions" not in reasons:
                reasons.append("late_entry_strong_conditions")

    _is_simple_late_et = (
        "متأخر" in _et
        and not _is_chase_or_very_late
        and "مطاردة" not in _et
    )
    if _is_simple_late_et:
        if "late_entry_soft_penalty" not in reasons:
            reasons.append("late_entry_soft_penalty")

    extreme_late_pump = (
        dist_ma >= 5.2
        and rsi_now >= 70
        and vol_ratio >= 2.0
        and candle_strength >= 0.65
    )

    if (breakout or pre_breakout) and dist_ma >= 4.8 and vol_ratio >= 2.0:
        extreme_late_pump = True
        if "breakout_extreme_stretch" not in reasons:
            reasons.append("breakout_extreme_stretch")

    bull_continuation_risk = (
        market_state == "bull_market"
        and is_continuation
        and not pre_breakout
        and (
            dist_ma > BULL_CONTINUATION_MAX_DIST_MA
            or rsi_now > BULL_CONTINUATION_MAX_RSI
            or vol_ratio > BULL_CONTINUATION_MAX_VOL_RATIO
        )
    )
    weak_breakout_exception = (
        breakout
        and breakout_quality in ("none", "weak")
        and late_pump_risk
    )
    should_block = False
    if bull_continuation_risk and late_pump_risk:
        should_block = True
    if extreme_late_pump and not pre_breakout and breakout_quality != "strong":
        should_block = True
    if weak_breakout_exception:
        should_block = True
    return {
        "late_pump_risk": bool(late_pump_risk),
        "extreme_late_pump": bool(extreme_late_pump),
        "bull_continuation_risk": bool(bull_continuation_risk),
        "should_block": bool(should_block),
        "reasons": reasons,
        "checks": checks,
        "soft_late_penalty": 0.25 if _is_simple_late_et else 0.0,
    }
 except Exception:
    return {
        "late_pump_risk": False,
        "extreme_late_pump": False,
        "bull_continuation_risk": False,
        "should_block": False,
        "reasons": [],
        "checks": 0,
        "soft_late_penalty": 0.0,
    }

def append_late_pump_warnings(score_result: dict, late_guard: dict) -> dict:
 try:
    if "warning_reasons" not in score_result or score_result["warning_reasons"] is None:
        score_result["warning_reasons"] = []
    warning_map = {
        "overextended_from_ma": "دخول متأخر",
        "rsi_overheated": "RSI تشبع شراء",
        "volume_pump": "فوليوم انفجاري",
        "strong_candle_chase": "شمعة قوية لكن احتمال مطاردة",
        "fast_4h_move": "صعود سريع خلال 4 ساعات",
        "far_from_vwap": "بعيد عن VWAP",
        "rsi_slope_weak": "RSI بدأ يضعف",
        "macd_hist_falling": "زخم MACD يتراجع",
    }
    if late_guard.get("late_pump_risk"):
        score_result["warning_reasons"].append("خطر مطاردة")
    if late_guard.get("bull_continuation_risk"):
        score_result["warning_reasons"].append("خطر مطاردة Pump متأخر")
    for reason in late_guard.get("reasons", []):
        label = warning_map.get(reason)
        if label:
            score_result["warning_reasons"].append(label)
    score_result["warning_reasons"] = list(dict.fromkeys(score_result["warning_reasons"]))
    return score_result
 except Exception:
    return score_result

def calculate_warning_penalty(warning_reasons: list) -> tuple:
    penalty = 0.0
    penalty_reasons = []

    high_risk_warnings = {
        "خطر مطاردة": 0.20,
        "خطر مطاردة Pump متأخر": 0.20,
        "خطر نهاية الزخم": 0.20,
        "موجة خامسة بدون Pullback واضح": 0.20,
        "RSI بدأ يضعف": 0.20,
        "اختراق متأخر": 0.20,
        "اختراق متأخر، يُنصح بالحذر": 0.20,
        "رفض سعري علوي بعد الاختراق": 0.20,
    }

    medium_risk_warnings = {
        "بعيد عن VWAP": 0.08,
        "زخم MACD يتراجع": 0.08,
        "MACD سلبي": 0.08,
        "رفض سعري علوي": 0.08,
        "شمعة قوية لكن احتمال مطاردة": 0.08,
        "صعود سريع خلال 4 ساعات": 0.08,
    }

    high_risk_count = 0
    medium_risk_count = 0

    for warning in warning_reasons or []:
        normalized = normalize_reason(str(warning))
        if normalized in high_risk_warnings:
            value = high_risk_warnings[normalized]
            penalty += value
            high_risk_count += 1
            penalty_reasons.append({
                "warning": normalized,
                "penalty": value,
                "level": "high"
            })
        elif normalized in medium_risk_warnings:
            value = medium_risk_warnings[normalized]
            penalty += value
            medium_risk_count += 1
            penalty_reasons.append({
                "warning": normalized,
                "penalty": value,
                "level": "medium"
            })

    if high_risk_count >= 2:
        penalty += 0.15
        penalty_reasons.append({
            "warning": "multiple_high_risk",
            "penalty": 0.15,
            "level": "extra"
        })

    penalty = min(penalty, 0.80)
    return round(penalty, 2), penalty_reasons, high_risk_count, medium_risk_count

# ==========================================
# FALLING KNIFE DETECTION
# ==========================================
def detect_falling_knife_risk(df, dist_ma, change_24h, vol_ratio) -> dict:
    result = {
        "falling_knife_risk": False,
        "checks": 0,
        "reasons": [],
    }

    try:
        if df is None or df.empty or len(df) < 8:
            return result

        signal_row = get_signal_row(df)
        if signal_row is None:
            return result

        idx = signal_row.name
        if idx is None or idx < 4:
            return result

        last = df.iloc[idx]
        prev = df.iloc[idx - 1]

        open_ = _safe_float(last.get("open"), 0.0)
        close_ = _safe_float(last.get("close"), 0.0)
        high_ = _safe_float(last.get("high"), 0.0)
        low_ = _safe_float(last.get("low"), 0.0)

        prev_close = _safe_float(prev.get("close"), close_)
        rsi_now = _safe_float(last.get("rsi"), 50.0)
        rsi_prev = _safe_float(prev.get("rsi"), rsi_now)

        candle_range = high_ - low_
        close_position = ((close_ - low_) / candle_range) if candle_range > 0 else 0.5

        recent = df.iloc[max(0, idx - 3):idx + 1]
        red_candles = 0
        for _, row in recent.iterrows():
            if _safe_float(row.get("close"), 0.0) < _safe_float(row.get("open"), 0.0):
                red_candles += 1

        checks = 0
        reasons = []

        if close_ < open_:
            checks += 1
            reasons.append("current_candle_red")

        if close_ < prev_close:
            checks += 1
            reasons.append("lower_close")

        if rsi_now < rsi_prev:
            checks += 1
            reasons.append("rsi_not_improving")

        if close_position <= 0.35:
            checks += 1
            reasons.append("weak_close_position")

        if red_candles >= 3:
            checks += 1
            reasons.append("three_or_more_recent_red_candles")

        if vol_ratio >= 1.4 and close_ < open_:
            checks += 1
            reasons.append("high_volume_selling")

        if change_24h <= -18.0 and dist_ma <= -7.0:
            checks += 1
            reasons.append("deep_24h_drop_and_far_below_ma")

        result["checks"] = checks
        result["reasons"] = reasons
        result["falling_knife_risk"] = checks >= 5

        return result

    except Exception as e:
        logger.warning(f"detect_falling_knife_risk error: {e}")
        return result

def is_oversold_reversal_long(
 df,
 dist_ma: float,
 change_24h: float,
 vol_ratio: float,
 funding: float = 0.0,
) -> bool:
 try:
    if not OVERSOLD_REVERSAL_ENABLED:
        return False
    if df is None or df.empty or len(df) < 25:
        return False
    if dist_ma >= 0:
        return False
    if change_24h > -5.0:
        return False
    signal_row = get_signal_row(df)
    if signal_row is None:
        return False
    idx = signal_row.name
    if idx is None or idx < 2:
        return False
    last = df.iloc[idx]
    prev = df.iloc[idx - 1]
    open_ = _safe_float(last["open"])
    close_ = _safe_float(last["close"])
    high_ = _safe_float(last["high"])
    low_ = _safe_float(last["low"])
    prev_close_ = _safe_float(prev["close"])
    prev_rsi_ = _safe_float(prev.get("rsi"), 50)
    rsi_now_ = _safe_float(last.get("rsi"), 50)
    candle_range_ = high_ - low_
    body_ = abs(close_ - open_)
    body_ratio_ = (body_ / candle_range_) if candle_range_ > 0 else 0.0
    upper_wick_ = high_ - max(open_, close_)
    exhaustion_wick_ = (
        candle_range_ > 0
        and upper_wick_ > body_ * 2.0
        and upper_wick_ > candle_range_ * 0.4
    )
    if exhaustion_wick_:
        return False
    falling = detect_falling_knife_risk(df, dist_ma, change_24h, vol_ratio)
    if falling.get("falling_knife_risk"):
        logger.info(f"Oversold reversal blocked by falling knife: {falling.get('reasons', [])}")
        return False
    bullish_close_ = close_ > open_
    gained_momentum_ = close_ >= prev_close_
    rsi_turning_ = rsi_now_ <= OVERSOLD_REVERSAL_MAX_RSI and rsi_now_ >= prev_rsi_
    strong_close_position_ = candle_range_ > 0 and ((close_ - low_) / candle_range_) >= 0.55
    decent_body_ = body_ratio_ >= 0.28
    negative_funding_ = funding < 0
    checks_ = 0
    if dist_ma <= -OVERSOLD_REVERSAL_MIN_DIST_MA:
        checks_ += 1
    if change_24h <= OVERSOLD_REVERSAL_MIN_24H_DROP:
        checks_ += 1
    if vol_ratio >= OVERSOLD_REVERSAL_MIN_VOL_RATIO:
        checks_ += 1
    if bullish_close_:
        checks_ += 1
    if gained_momentum_:
        checks_ += 1
    if rsi_turning_:
        checks_ += 1
    if strong_close_position_:
        checks_ += 1
    if decent_body_:
        checks_ += 1
    if negative_funding_:
        checks_ += 1
    return checks_ >= 5
 except Exception:
    return False

def get_reverse_banner_long(is_reverse: bool) -> str:
 if is_reverse:
    return "🔄 <b>OVERSOLD REVERSAL</b>"
 return ""

def get_reverse_style_note_long(is_reverse: bool) -> str:
 if is_reverse:
    return "⚠️ <b>تنبيه:</b> هذه إشارة ارتداد من تشبع بيعي، مخاطرتها أعلى من العادي"
 return ""

def get_effective_min_score_with_reverse(
 base_min_score: float,
 is_reverse: bool,
) -> float:
 try:
    if is_reverse:
        return round(min(base_min_score, OVERSOLD_REVERSAL_MIN_SCORE), 2)
    return round(base_min_score, 2)
 except Exception:
    return round(base_min_score, 2)

# =========================
# STRATEGY HELPERS
# =========================
def early_bullish_signal(df):
 try:
    if df is None or df.empty or len(df) < 25:
        return False
    signal_row = get_signal_row(df)
    if signal_row is None:
        return False
    idx = signal_row.name
    if idx is None or idx < 2:
        return False
    last = df.iloc[idx]
    prev = df.iloc[idx - 1]
    open_ = _safe_float(last["open"])
    close_ = _safe_float(last["close"])
    high_ = _safe_float(last["high"])
    low_ = _safe_float(last["low"])
    rsi_now_ = _safe_float(last.get("rsi"), 50)
    rsi_prev_ = _safe_float(prev.get("rsi"), 50)
    candle_range = high_ - low_
    body = abs(close_ - open_)
    body_ratio = (body / candle_range) if candle_range > 0 else 0.0
    avg_vol_ = _safe_float(df.iloc[max(0, idx - 10):idx]["volume"].mean(), 0)
    vol_ok = avg_vol_ > 0 and _safe_float(last["volume"]) >= avg_vol_ * 1.08
    bullish_close = close_ > open_
    strong_close_position = candle_range > 0 and ((close_ - low_) / candle_range) >= 0.55
    rsi_strengthening = rsi_now_ > 48 and rsi_now_ >= rsi_prev_
    real_body = body_ratio >= 0.32
    checks = sum([bullish_close, strong_close_position, rsi_strengthening, vol_ok, real_body])
    return checks >= 3
 except Exception:
    return False

def is_higher_timeframe_confirmed(symbol):
 try:
    candles = get_candles(symbol, HTF_TIMEFRAME, 100)
    df = to_dataframe(candles)
    if df is None or df.empty or len(df) < 10:
        return False
    signal_row = get_signal_row(df)
    if signal_row is None:
        return False
    idx = signal_row.name
    if idx is None or idx < 3:
        return False
    checks = 0
    ma_value = signal_row.get("ma", None)
    if ma_value is not None and _safe_float(signal_row["close"]) > _safe_float(ma_value):
        checks += 1
    high_rsi = _safe_float(signal_row.get("rsi"), 50) >= 50
    if high_rsi:
        checks += 1
    last_3 = df.iloc[idx - 3:idx]
    green_candles = sum(
        1 for _, row in last_3.iterrows()
        if _safe_float(row["close"]) > _safe_float(row["open"])
    )
    if green_candles >= 2:
        checks += 1
    return checks >= 2
 except Exception as e:
    logger.error(f"MTF error on {symbol}: {e}")
    return False

def is_higher_timeframe_confirmed_from_candles(candles) -> bool:
    try:
        df = to_dataframe(candles)
        if df is None or df.empty or len(df) < 10:
            return False

        signal_row = get_signal_row(df)
        if signal_row is None:
            return False

        idx = signal_row.name
        if idx is None or idx < 3:
            return False

        checks = 0

        ma_value = signal_row.get("ma", None)
        if ma_value is not None and _safe_float(signal_row["close"]) > _safe_float(ma_value):
            checks += 1

        high_rsi = _safe_float(signal_row.get("rsi"), 50) >= 50
        if high_rsi:
            checks += 1

        last_3 = df.iloc[idx - 3:idx]
        green_candles = sum(
            1 for _, row in last_3.iterrows()
            if _safe_float(row["close"]) > _safe_float(row["open"])
        )

        if green_candles >= 2:
            checks += 1

        return checks >= 2

    except Exception as e:
        logger.error(f"MTF from candles error: {e}")
        return False

def is_4h_oversold_confirmed(symbol: str) -> dict:
 try:
    candles = get_candles(symbol, "4H", 60)
    df = to_dataframe(candles)
    if df is None or df.empty or len(df) < 20:
        return {
            "confirmed": False,
            "checks": 0,
            "details": f"• فريم 4H: البيانات غير كافية"
        }
    signal_row = get_signal_row(df)
    if signal_row is None:
        return {
            "confirmed": False,
            "checks": 0,
            "details": f"• فريم 4H: تعذر الحصول على صف الإشارة"
        }
    idx = signal_row.name
    if idx is None or idx < 3:
        return {
            "confirmed": False,
            "checks": 0,
            "details": f"• فريم 4H: المؤشر الزمني غير كاف"
        }
    close = _safe_float(signal_row["close"])
    open_ = _safe_float(signal_row["open"])
    high = _safe_float(signal_row["high"])
    low = _safe_float(signal_row["low"])
    rsi_4h = _safe_float(signal_row.get("rsi"), 50)
    ma_4h = _safe_float(signal_row.get("ma"), close)
    candle_range = high - low
    body = abs(close - open_)
    lower_wick = min(open_, close) - low
    upper_wick = high - max(open_, close)
    checks = 0
    rsi_ok = rsi_4h <= 35
    if rsi_ok:
        checks += 1
    below_ma = close < ma_4h
    if below_ma:
        checks += 1
    hammer = (
        candle_range > 0
        and lower_wick >= body * 1.8
        and upper_wick <= body * 0.6
        and close > open_
    )
    bullish_engulf = (
        close > open_
        and candle_range > 0
        and body >= candle_range * 0.60
    )
    reversal_candle = hammer or bullish_engulf
    if reversal_candle:
        checks += 1
    confirmed = checks >= 2
    details_lines = [
        f"• 4H RSI: {fmt_num(rsi_4h, 0)} {'✅' if rsi_ok else '❌'}",
        f"• 4H Below MA: {'✅' if below_ma else '❌'}",
        f"• 4H Reversal Candle: {'✅' if reversal_candle else '❌'}",
    ]
    details = "\n".join(details_lines)
    logger.info(
        f"4H OVERSOLD CHECK | {symbol} | "
        f"rsi={rsi_4h:.1f} | below_ma={below_ma} | "
        f"reversal_candle={reversal_candle} | checks={checks}/3 | confirmed={confirmed}"
    )
    return {"confirmed": confirmed, "checks": checks, "details": details}
 except Exception as e:
    logger.error(f"is_4h_oversold_confirmed error on {symbol}: {e}")
    return {
        "confirmed": False,
        "checks": 0,
        "details": f"• فريم 4H: حدث خطأ أثناء التحقق"
    }

def get_btc_mode():
 try:
    candles = get_candles("BTC-USDT-SWAP", "1H", 100)
    df = to_dataframe(candles)
    if df is None or df.empty:
        return "🟡 محايد"
    signal_row = get_signal_row(df)
    if signal_row is None:
        return "🟡 محايد"
    ma_value = signal_row.get("ma", None)
    rsi_value = _safe_float(signal_row.get("rsi"), 50)
    if ma_value is not None:
        if _safe_float(signal_row["close"]) > _safe_float(ma_value) and rsi_value >= 55:
            return "🟢 صاعد"
        if _safe_float(signal_row["close"]) < _safe_float(ma_value) and rsi_value <= 45:
            return "🔴 هابط"
    return "🟡 محايد"
 except Exception as e:
    logger.error(f"BTC mode error: {e}")
    return "🟡 محايد"


def get_btc_range_zone(timeframe: str = "1H", lookback: int = 50) -> dict:
 """Classify BTC position inside its recent range.

 This is a market-context helper only. It does not change execution rules,
 whitelist, TP/SL, tracking, or risk-manager logic.
 """
 try:
    candles = get_candles("BTC-USDT-SWAP", timeframe, max(lookback + 5, 60))
    df = to_dataframe(candles)
    if df is None or df.empty or len(df) < 20:
        return {
            "zone": "unknown",
            "label": "⚪ BTC Zone غير متاح",
            "score_adjustment": 0.0,
            "position_pct": None,
            "breakdown": False,
            "reason": "not_enough_data",
        }

    recent = df.tail(lookback).copy()
    signal_row = get_signal_row(df)
    if signal_row is None:
        signal_row = df.iloc[-1]

    close = _safe_float(signal_row.get("close"), 0.0)
    high_range = _safe_float(recent["high"].max(), 0.0)
    low_range = _safe_float(recent["low"].min(), 0.0)
    if close <= 0 or high_range <= 0 or low_range <= 0 or high_range <= low_range:
        return {
            "zone": "unknown",
            "label": "⚪ BTC Zone غير واضح",
            "score_adjustment": 0.0,
            "position_pct": None,
            "breakdown": False,
            "reason": "invalid_range",
        }

    position_pct = max(0.0, min(100.0, ((close - low_range) / (high_range - low_range)) * 100.0))
    prev_range = df.tail(lookback + 1).head(lookback)
    prev_low = _safe_float(prev_range["low"].min(), low_range) if prev_range is not None and not prev_range.empty else low_range
    last_change = get_last_candle_change_pct(df)
    breakdown = bool(close < prev_low * 0.997 and last_change <= -0.35)

    if breakdown:
        zone = "breakdown"
        label = "🔴 كسر أسفل رينج BTC"
        score_adjustment = -0.45
        reason = "confirmed_breakdown"
    elif position_pct <= 35:
        zone = "lower_range"
        label = "🟢 أسفل رينج BTC / ارتداد محتمل"
        score_adjustment = 0.35
        reason = "lower_range_rebound_zone"
    elif position_pct >= 78:
        zone = "upper_range"
        label = "🟠 أعلى رينج BTC / احتمال جني أرباح"
        score_adjustment = -0.25
        reason = "upper_range_extended"
    elif position_pct >= 58:
        zone = "upper_mid"
        label = "🟡 BTC قرب أعلى النطاق"
        score_adjustment = -0.10
        reason = "upper_mid_caution"
    else:
        zone = "middle_range"
        label = "🟡 BTC منتصف الرينج"
        score_adjustment = 0.0
        reason = "neutral_range"

    return {
        "zone": zone,
        "label": label,
        "score_adjustment": round(float(score_adjustment), 2),
        "position_pct": round(float(position_pct), 1),
        "range_low": low_range,
        "range_high": high_range,
        "close": close,
        "breakdown": breakdown,
        "timeframe": timeframe,
        "lookback": lookback,
        "reason": reason,
    }
 except Exception as e:
    logger.error(f"BTC range zone error: {e}")
    return {
        "zone": "unknown",
        "label": "⚪ BTC Zone error",
        "score_adjustment": 0.0,
        "position_pct": None,
        "breakdown": False,
        "reason": f"error: {e}",
    }

def is_gaining_intraday_strength(df) -> bool:
 try:
    if df is None or df.empty or len(df) < 5:
        return False
    signal_row = get_signal_row(df)
    if signal_row is None:
        return False
    idx = signal_row.name
    if idx is None or idx < 2:
        return False
    last = df.iloc[idx]
    prev = df.iloc[idx - 1]
    higher_close = _safe_float(last["close"]) >= _safe_float(prev["close"])
    stronger_rsi = _safe_float(last.get("rsi"), 50) >= _safe_float(prev.get("rsi"), 50)
    near_high = _safe_float(last["close"]) > (_safe_float(last["high"]) * 0.994)
    checks = sum([higher_close, stronger_rsi, near_high])
    return checks >= 2
 except Exception:
    return False

def get_alt_market_snapshot(ranked_pairs, sample_size=ALT_MARKET_SAMPLE_SIZE):
 try:
    if not ranked_pairs:
        return {
            "sample_size": 0,
            "valid_count": 0,
            "above_ma_ratio": 0.0,
            "rsi_support_ratio": 0.0,
            "positive_24h_ratio": 0.0,
            "alt_strength_score": 0.0,
            "alt_mode": "🔴 ضعيف",
        }
    sampled = sorted(
        ranked_pairs,
        key=lambda x: x.get("_rank_volume_24h", 0),
        reverse=True
    )[:sample_size]
    above_ma_count = 0
    rsi_support_count = 0
    positive_24h_count = 0
    valid = 0
    for item in sampled:
        symbol = item.get("instId", "")
        if not symbol:
            continue
        candles = get_candles(symbol, ALT_MARKET_TIMEFRAME, ALT_MARKET_CANDLE_LIMIT)
        df = to_dataframe(candles)
        if df is None or df.empty:
            continue
        signal_row = get_signal_row(df)
        if signal_row is None:
            continue
        close = _safe_float(signal_row["close"], 0)
        if close <= 0:
            continue
        change_24h = float(item.get("_rank_change_24h", 0) or 0)
        ma_value = _safe_float(signal_row.get("ma"), 0)
        rsi_value = _safe_float(signal_row.get("rsi"), 50)
        valid += 1
        if ma_value > 0 and close > ma_value:
            above_ma_count += 1
        if rsi_value >= 52:
            rsi_support_count += 1
        if change_24h > 0:
            positive_24h_count += 1
    if valid == 0:
        return {
            "sample_size": len(sampled),
            "valid_count": 0,
            "above_ma_ratio": 0.0,
            "rsi_support_ratio": 0.0,
            "positive_24h_ratio": 0.0,
            "alt_strength_score": 0.0,
            "alt_mode": "🔴 ضعيف",
        }
    above_ma_ratio = round(above_ma_count / valid, 4)
    rsi_support_ratio = round(rsi_support_count / valid, 4)
    positive_24h_ratio = round(positive_24h_count / valid, 4)
    alt_strength_score = round(
        (above_ma_ratio * 0.45) +
        (rsi_support_ratio * 0.35) +
        (positive_24h_ratio * 0.20),
        4
    )
    if valid < ALT_MARKET_MIN_VALID:
        alt_mode = "🔴 ضعيف"
    elif alt_strength_score >= 0.68 and above_ma_ratio >= 0.58 and rsi_support_ratio >= 0.50:
        alt_mode = "🟢 قوي"
    elif (
        alt_strength_score < 0.38
        and above_ma_ratio < 0.38
        and rsi_support_ratio < 0.38
        and positive_24h_ratio < 0.42
    ):
        alt_mode = "🔴 ضعيف"
    else:
        alt_mode = "🟡 محايد"
    snapshot = {
        "sample_size": len(sampled),
        "valid_count": valid,
        "above_ma_ratio": above_ma_ratio,
        "rsi_support_ratio": rsi_support_ratio,
        "positive_24h_ratio": positive_24h_ratio,
        "alt_strength_score": alt_strength_score,
        "alt_mode": alt_mode,
    }
    logger.info(
        f"ALT SNAPSHOT | valid={valid}/{len(sampled)} | "
        f"above_ma={above_ma_ratio:.2f} | rsi={rsi_support_ratio:.2f} | "
        f"pos24h={positive_24h_ratio:.2f} | strength={alt_strength_score:.2f} | "
        f"mode={alt_mode}"
    )
    return snapshot
 except Exception as e:
    logger.error(f"Alt market snapshot error: {e}")
    return {
        "sample_size": 0,
        "valid_count": 0,
        "above_ma_ratio": 0.0,
        "rsi_support_ratio": 0.0,
        "positive_24h_ratio": 0.0,
        "alt_strength_score": 0.0,
        "alt_mode": "🟡 محايد",
    }

def get_market_state(btc_mode: str, alt_snapshot: dict):
 alt_mode = alt_snapshot.get("alt_mode", "🟡 محايد")
 if "هابط" in btc_mode and "ضعيف" in alt_mode:
    return {
        "market_state": "risk_off",
        "market_state_label": "🔴 Risk-Off",
        "market_bias_label": "⚠️ هروب من المخاطرة",
        "btc_dominance_proxy": "🔴 ضد الألت",
    }
 if "صاعد" in btc_mode and "ضعيف" in alt_mode:
    return {
        "market_state": "btc_leading",
        "market_state_label": "🟠 BTC Leading",
        "market_bias_label": "🟠 BTC يصعد والألت ضعيفة",
        "btc_dominance_proxy": "🟠 ضغط على الألت",
    }
 if "صاعد" in btc_mode and ("قوي" in alt_mode or "متماسك" in alt_mode or "محايد" in alt_mode):
    return {
        "market_state": "bull_market",
        "market_state_label": "🟢 Bull Market",
        "market_bias_label": "🟢 داعم للألت",
        "btc_dominance_proxy": "🟢 داعم للألت",
    }
 if ("محايد" in btc_mode or "هابط" in btc_mode) and "قوي" in alt_mode:
    return {
        "market_state": "alt_season",
        "market_state_label": "🔥 Alt Season",
        "market_bias_label": "🟢 الألت أقوى من BTC حالياً",
        "btc_dominance_proxy": "🟢 داعم للألت",
    }
 return {
    "market_state": "mixed",
    "market_state_label": "🟡 Mixed",
    "market_bias_label": "🟡 السوق مختلط والسيولة غير محسومة",
    "btc_dominance_proxy": "🟡 محايد",
 }

def get_dynamic_entry_threshold(
 market_state: str,
 score_result: dict,
 vol_ratio: float,
 mtf_confirmed: bool,
 is_new: bool,
 gaining_strength: bool,
) -> float:
 if market_state == "risk_off":
    threshold = 6.7
 elif market_state == "btc_leading":
    threshold = 6.8
 elif market_state == "mixed":
    threshold = 6.5
 elif market_state == "bull_market":
    threshold = 6.35
 elif market_state == "alt_season":
    threshold = 6.0
 else:
    threshold = 6.4
 if mtf_confirmed:
    threshold -= 0.10
 if market_state == "bull_market":
    if vol_ratio >= 2.0:
        threshold += 0.10
    elif vol_ratio >= 1.5:
        threshold += 0.05
 else:
    if vol_ratio >= 2.0:
        threshold -= 0.10
    elif vol_ratio >= 1.5:
        threshold -= 0.05
 if is_new:
    threshold += 0.10
 if not gaining_strength:
    threshold += 0.10
 if score_result.get("fake_signal"):
    threshold += 0.15
 threshold = max(5.6, min(6.8, threshold))
 return round(threshold, 2)

def calculate_stop_loss(price, atr_value, signal_type="standard"):
 multipliers = {
    "breakout": 2.5,
    "pre_breakout": 3.0,
    "new_listing": 3.2,
    "standard": 2.8,
 }
 multiplier = multipliers.get(signal_type, 2.8)
 try:
    sl = round(float(price) - (float(atr_value) * multiplier), 6)
    min_sl = round(float(price) * 0.985, 6)
    max_sl = round(float(price) * 0.965, 6)
    return max(max_sl, min(min_sl, sl))
 except Exception:
    return round(float(price) * 0.978, 6)

def calculate_sl_percent(entry, sl):
 try:
    return round(((float(entry) - float(sl)) / float(entry)) * 100, 2)
 except Exception:
    return 0.0

def is_new_listing_by_candles(candles) -> bool:
 try:
    return len(candles) < NEW_LISTING_MAX_CANDLES
 except Exception:
    return False

def build_tradingview_link(symbol):
 base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
 tv_symbol = f"OKX:{base}USDT.P"
 return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"

def get_candle_strength_ratio(df) -> float:
 try:
    signal_row = get_signal_row(df)
    if signal_row is None:
        return 0.0
    high = _safe_float(signal_row["high"])
    low = _safe_float(signal_row["low"])
    open_ = _safe_float(signal_row["open"])
    close = _safe_float(signal_row["close"])
    full = high - low
    if full <= 0:
        return 0.0
    body = abs(close - open_)
    return round(body / full, 4)
 except Exception:
    return 0.0

def get_volume_ratio(df) -> float:
 try:
    signal_row = get_signal_row(df)
    if signal_row is None:
        return 1.0
    idx = signal_row.name
    if idx is None or idx <= 1:
        return 1.0
    start_idx = max(0, idx - 20)
    avg_volume = _safe_float(df.iloc[start_idx:idx]["volume"].mean(), 0)
    last_volume = _safe_float(signal_row["volume"], 0)
    if avg_volume <= 0:
        return 1.0
    return round(last_volume / avg_volume, 4)
 except Exception:
    return 1.0

def get_distance_from_ma_percent(df) -> float:
 try:
    signal_row = get_signal_row(df)
    if signal_row is None:
        return 0.0
    close = _safe_float(signal_row["close"], 0)
    ma_value = _safe_float(signal_row.get("ma"), 0)
    if ma_value <= 0:
        return 0.0
    return round(((close - ma_value) / ma_value) * 100, 4)
 except Exception:
    return 0.0

def is_pre_breakout(df, lookback=PRE_BREAKOUT_LOOKBACK) -> bool:
 try:
    min_len = max(
        lookback + 6,
        PRE_BREAKOUT_BASELINE_VOL_BARS + PRE_BREAKOUT_RECENT_VOL_BARS + 2,
    )
    if df is None or df.empty or len(df) < min_len:
        return False
    signal_row = get_signal_row(df)
    if signal_row is None:
        return False
    idx = signal_row.name
    if idx is None or idx < max(lookback, PRE_BREAKOUT_BASELINE_VOL_BARS + PRE_BREAKOUT_RECENT_VOL_BARS):
        return False
    close = _safe_float(signal_row["close"])
    ma_value = _safe_float(signal_row.get("ma"), close)
    recent_high = _safe_float(df["high"].iloc[idx - lookback:idx].max())
    if recent_high <= 0 or close <= 0:
        return False
    proximity = close / recent_high
    if not (PRE_BREAKOUT_PROXIMITY_MIN <= proximity < 1.0):
        return False
    recent_vols = df["volume"].iloc[idx - PRE_BREAKOUT_RECENT_VOL_BARS:idx].astype(float).tolist()
    vol_increasing = (
        len(recent_vols) == PRE_BREAKOUT_RECENT_VOL_BARS
        and recent_vols[1] >= recent_vols[0]
        and recent_vols[2] >= recent_vols[1]
    )
    baseline_start = idx - (PRE_BREAKOUT_BASELINE_VOL_BARS + PRE_BREAKOUT_RECENT_VOL_BARS)
    baseline_end = idx - PRE_BREAKOUT_RECENT_VOL_BARS
    baseline_vols = df["volume"].iloc[baseline_start:baseline_end].astype(float)
    if baseline_vols.empty:
        return False
    recent_avg_vol = sum(recent_vols) / len(recent_vols)
    baseline_avg_vol = float(baseline_vols.mean())
    if baseline_avg_vol <= 0:
        return False
    volume_significant = recent_avg_vol >= baseline_avg_vol * PRE_BREAKOUT_VOLUME_SIGNIFICANCE
    recent_atr = _safe_float(signal_row.get("atr"), 0)
    prev_atr = _safe_float(df["atr"].iloc[idx - 5:idx].mean(), 0)
    compressed = prev_atr > 0 and recent_atr > 0 and recent_atr < prev_atr * 0.90
    above_ma = close > ma_value
    return vol_increasing and volume_significant and compressed and above_ma
 except Exception:
    return False

def is_valid_candle_timing(df) -> bool:
 try:
    now = int(time.time())
    candle_seconds = 15 * 60
    last_completed_ts = (now // candle_seconds) * candle_seconds
    signal_row = get_signal_row(df)
    if signal_row is None:
        return False
    ts = int(signal_row["ts"])
    if ts > 10_000_000_000:
        ts = ts // 1000
    candle_age = last_completed_ts - ts
    return 0 <= candle_age <= (candle_seconds * 2)
 except Exception:
    return False

def passes_new_listing_filter(score: float, breakout: bool, vol_ratio: float, candle_strength: float) -> bool:
 checks = 0
 if breakout:
    checks += 1
 if vol_ratio >= NEW_LISTING_MIN_VOL_RATIO:
    checks += 1
 if candle_strength >= NEW_LISTING_MIN_CANDLE_STRENGTH:
    checks += 1
 if score >= TOP_MOMENTUM_NEW_MIN_SCORE:
    checks += 1
 return checks >= 3

def get_effective_min_score(is_new: bool, is_reverse: bool = False) -> float:
 if is_reverse:
    return OVERSOLD_REVERSAL_MIN_SCORE
 return TOP_MOMENTUM_NEW_MIN_SCORE if is_new else TOP_MOMENTUM_MIN_SCORE

def classify_early_priority_long(
 early_signal: bool,
 breakout: bool,
 pre_breakout: bool,
 dist_ma: float,
 vol_ratio: float,
 candle_strength: float,
 mtf_confirmed: bool,
 gaining_strength: bool,
 market_state: str,
) -> str:
 try:
    if not early_signal or breakout or pre_breakout:
        return "none"
    score = 0
    if dist_ma <= 2.8:
        score += 2
    elif dist_ma <= 3.5:
        score += 1
    if vol_ratio >= 1.35:
        score += 2
    elif vol_ratio >= 1.15:
        score += 1
    if candle_strength >= 0.55:
        score += 2
    elif candle_strength >= 0.42:
        score += 1
    if mtf_confirmed:
        score += 2
    if gaining_strength:
        score += 1
    if market_state in ("bull_market", "alt_season"):
        score += 1
    if score >= 7:
        return "strong"
    if score >= 4:
        return "medium"
    if score >= 2:
        return "weak"
    return "none"
 except Exception:
    return "none"

def get_early_priority_score_bonus(priority: str) -> float:
 if priority == "strong":
    return 0.50
 if priority == "medium":
    return 0.25
 if priority == "weak":
    return 0.10
 return 0.0

def get_early_priority_momentum_bonus(priority: str) -> float:
 if priority == "strong":
    return 0.35
 if priority == "medium":
    return 0.15
 if priority == "weak":
    return -0.10
 return 0.0

def get_early_priority_threshold_adjustment(priority: str) -> float:
 if priority == "strong":
    return -0.15
 if priority == "medium":
    return -0.05
 return 0.0

# =========================
# SETUP TYPE SYSTEM
# =========================
def get_setup_family(candidate: dict) -> str:
 if candidate.get("is_reverse"):
    return "reverse"
 if candidate.get("breakout"):
    return "breakout"
 if candidate.get("pre_breakout"):
    return "pre_breakout"
 return "continuation"

def get_setup_volume_band(vol_ratio: float) -> str:
 try:
    v = float(vol_ratio or 0)
    if v >= 1.80:
        return "vol_high"
    if v >= 1.25:
        return "vol_mid"
    return "vol_low"
 except Exception:
    return "vol_low"

def get_setup_market_regime(market_state: str) -> str:
 allowed = {"bull_market", "alt_season", "mixed", "btc_leading", "risk_off"}
 value = str(market_state or "").strip()
 return value if value in allowed else "mixed"

def infer_wave_context(entry_maturity_data: dict, is_reverse: bool, dist_ma: float, breakout: bool, pre_breakout: bool) -> str:
    try:
        if not entry_maturity_data:
            return "unknown"

        wave_estimate = int(entry_maturity_data.get("wave_estimate", 0) or 0)
        fib_position = str(entry_maturity_data.get("fib_position", "unknown") or "unknown")
        entry_maturity = str(entry_maturity_data.get("entry_maturity", "unknown") or "unknown")
        had_pullback = bool(entry_maturity_data.get("had_pullback", False))

        if is_reverse:
            if wave_estimate >= 5 or dist_ma <= -OVERSOLD_REVERSAL_MIN_DIST_MA:
                return "wave_5_down"
            return "reverse_unknown"

        if entry_maturity == "healthy" and had_pullback:
            return "golden_pullback"

        if wave_estimate == 3:
            return "wave_3"

        if wave_estimate >= 5 and fib_position == "overextended":
            return "wave_5_late"

        if wave_estimate >= 5 and not had_pullback:
            return "wave_5_late"

        if breakout or pre_breakout:
            return "breakout_context"

        return "unknown"

    except Exception:
        return "unknown"

def build_setup_type(candidate: dict) -> str:
    try:
        family = get_setup_family(candidate)
        mtf = "mtf_yes" if candidate.get("mtf_confirmed") else "mtf_no"
        vol_band = get_setup_volume_band(candidate.get("vol_ratio", 1.0))
        market_regime = get_setup_market_regime(candidate.get("market_state"))
        wave_context = str(candidate.get("wave_context", "") or "").strip()

        base = f"{family}|{mtf}|{vol_band}|{market_regime}"

        if wave_context and wave_context != "unknown":
            return f"{base}|{wave_context}"

        return base
    except Exception:
        return "unknown"

def get_hybrid_label_from_stats(setup_stats: dict) -> str:
 try:
    closed = int(setup_stats.get("closed", 0) or 0)
    winrate = float(setup_stats.get("winrate", 0) or 0)
    if closed < 8:
        return f"📊 No Data ({closed} trades)"
    if winrate >= 70 and closed >= 15:
        return f"🏆 ELITE ({winrate:.0f}% | {closed} trades)"
    if winrate >= 55 and closed >= 8:
        return f"✅ GOOD ({winrate:.0f}% | {closed} trades)"
    return f"⚠️ WEAK ({winrate:.0f}% | {closed} trades)"
 except Exception:
    return "📊 No Data (0 trades)"

def get_momentum_priority(
 score: float,
 breakout: bool,
 vol_ratio: float,
 is_new: bool,
 pre_breakout: bool = False,
 dist_ma: float = 0.0,
 gaining_strength: bool = False,
 early_priority: str = "none",
 is_reverse: bool = False,
) -> float:
 priority = float(score)
 if breakout:
    priority += 0.9
 elif pre_breakout:
    priority += 0.6
 if vol_ratio >= 1.8:
    priority += 0.8
 elif vol_ratio >= 1.35:
    priority += 0.4
 if is_new and vol_ratio >= NEW_LISTING_MIN_VOL_RATIO:
    priority += 0.4
 if not is_reverse:
    if dist_ma < -5.2:
        priority -= 0.7
    elif dist_ma < -4.2:
        priority -= 0.25
 else:
    if dist_ma <= -OVERSOLD_REVERSAL_MIN_DIST_MA:
        priority += 0.35
 if gaining_strength:
    priority += 0.20
 priority += get_early_priority_momentum_bonus(early_priority)
 return round(priority, 2)

def get_candidate_bucket(candidate: dict) -> str:
 if candidate.get("is_reverse"):
    return "reverse"
 if candidate["is_new"] and candidate["breakout"]:
    return "new_breakout"
 if candidate.get("pre_breakout") and not candidate["breakout"]:
    return "pre_breakout"
 if candidate["breakout"]:
    return "breakout"
 if candidate.get("early_priority") == "strong":
    return "early_strong"
 if candidate["vol_ratio"] >= 1.8:
    return "volume"
 return "standard"

def apply_top_momentum_filter(candidates):
 if not candidates:
    return []

 def _log_top_momentum_rejection(c, reason="top_momentum_filter"):
    try:
        log_rejected_candidate(
            redis_client=r,
            symbol=c.get("symbol", "UNKNOWN"),
            reason=reason,
            candle_time=c.get("candle_time"),
            score=c.get("score"),
            raw_score=c.get("raw_score"),
            final_threshold=c.get("final_threshold"),
            market_state=c.get("market_state", ""),
            current_mode=c.get("current_mode", ""),
            setup_type=c.get("setup_type", ""),
            entry_timing=c.get("entry_timing", ""),
            opportunity_type=c.get("opportunity_type", ""),
            dist_ma=c.get("dist_ma"),
            rsi_now=c.get("rsi_now"),
            vol_ratio=c.get("vol_ratio"),
            vwap_distance=c.get("vwap_distance"),
            mtf_confirmed=c.get("mtf_confirmed"),
            breakout=c.get("breakout"),
            pre_breakout=c.get("pre_breakout"),
            is_reverse=c.get("is_reverse"),
            extra={
                "momentum_priority": c.get("momentum_priority"),
                "bucket": c.get("bucket"),
                "change_24h": c.get("change_24h"),
                "rank_volume_24h": c.get("rank_volume_24h"),
                "extra_setup_names": c.get("extra_setup_names"),
                "setup_type_base": c.get("setup_type_base"),
                "execution_setup_tags": c.get("execution_setup_tags", []),
            },
        )
    except Exception:
        pass

 def _is_top_momentum_soft_exception(c):
    """Keep strong detector-approved setups from being silenced by rank/momentum filters.

    This is alert-flow only. It does not bypass TP/SL, execution limits, late danger,
    or true near-resistance blocks. It mainly prevents STRONG_LONG_ONLY from producing
    zero normal messages when the market is selective but a whitelisted setup is present.
    """
    try:
        if not isinstance(c, dict):
            return False
        mode = normalize_market_mode(c.get("current_mode") or c.get("market_mode") or MODE_NORMAL_LONG)
        if mode not in (MODE_STRONG_LONG_ONLY, MODE_NORMAL_LONG):
            return False

        tags = set(_collect_execution_setup_tags(c))
        strong_tags = {
            "retest_breakout_confirmed",
            "vwap_reclaim",
            "wave_3",
            "higher_low_continuation",
            "relative_strength_vs_btc",
            "liquidity_sweep_reclaim",
            "support_bounce_confirmed",
            "failed_breakdown_trap",
        }
        has_strong_setup = bool(tags & strong_tags) or _has_strict_execution_setup(c)
        if mode == MODE_NORMAL_LONG and not has_strong_setup:
            has_strong_setup = _has_normal_long_execution_setup(c)
        if not has_strong_setup:
            return False

        entry_text = "|".join(str(c.get(k, "") or "") for k in (
            "entry_timing", "entry_maturity", "entry_maturity_label", "wave_label", "fib_position"
        )).lower()
        hard_late_or_danger = any(token in entry_text for token in (
            "danger", "danger_late", "hard_late", "overextended", "wave_5",
            "متأخر جدًا", "موجة خامسة", "نهاية الحركة"
        ))
        if hard_late_or_danger:
            return False

        score = _safe_float(c.get("score", c.get("effective_score", 0.0)), 0.0)
        vol_ratio = _safe_float(c.get("vol_ratio", 0.0), 0.0)
        mtf_confirmed = bool(c.get("mtf_confirmed"))
        market_state = str(c.get("market_state", "") or "").lower()
        btc_mode = str(c.get("btc_mode", "") or "")
        alt_mode = str(c.get("alt_mode", "") or "")
        market_supportive = (
            market_state in ("bull_market", "alt_season")
            or "صاعد" in btc_mode
            or "قوي" in alt_mode
            or "🟢" in btc_mode
            or "🟢" in alt_mode
        )

        min_score = 6.7 if mode == MODE_STRONG_LONG_ONLY else 6.4
        return bool(score >= min_score and vol_ratio >= 1.0 and (mtf_confirmed or market_supportive))
    except Exception:
        return False

 strong_candidates = []

 if len(candidates) <= 3:
    for c in candidates:
        if c.get("is_reverse"):
            min_sc = OVERSOLD_REVERSAL_MIN_SCORE
        elif c.get("is_new"):
            min_sc = TOP_MOMENTUM_NEW_MIN_SCORE
        elif (c.get("breakout") or c.get("pre_breakout") or c.get("early_priority") == "strong"
              or c.get("strong_bull_pullback") or c.get("strong_breakout_exception")):
            min_sc = max(6.2, TOP_MOMENTUM_MIN_SCORE)
        else:
            min_sc = 6.8

        if c["score"] >= min_sc:
            strong_candidates.append(c)
        elif _is_top_momentum_soft_exception(c):
            c.setdefault("warning_reasons", [])
            if isinstance(c.get("warning_reasons"), list):
                c["warning_reasons"].append("Top momentum ضعيف لكن setup قوي/موحد؛ تم السماح كتحذير")
            c["top_momentum_soft_exception"] = True
            strong_candidates.append(c)
            logger.info(f"TOP MOMENTUM SOFT ALLOW | {c.get('symbol')} | score={c.get('score')} | tags={c.get('execution_setup_tags', [])}")
        else:
            _log_top_momentum_rejection(c, reason="top_momentum_min_score")

    strong_candidates.sort(
        key=lambda x: (x["momentum_priority"], x["score"], x["change_24h"], x["rank_volume_24h"]),
        reverse=True
    )
    logger.info(f"Top momentum small-candidate mode: kept {len(strong_candidates)} of {len(candidates)}")
    return strong_candidates

 min_score = get_effective_min_score(False, False)
 for c in candidates:
    soft_exception = _is_top_momentum_soft_exception(c)
    if c["score"] >= min_score or soft_exception:
        if not (c.get("breakout") or c.get("pre_breakout") or c.get("early_priority") == "strong"
                or c.get("strong_bull_pullback") or c.get("strong_breakout_exception") or c.get("is_reverse")):
            if c["score"] < 6.8 and not soft_exception:
                _log_top_momentum_rejection(c, reason="top_momentum_plain_continuation_score")
                continue
        if soft_exception and c["score"] < min_score:
            c.setdefault("warning_reasons", [])
            if isinstance(c.get("warning_reasons"), list):
                c["warning_reasons"].append("Top momentum أقل من الحد لكن setup قوي/موحد؛ تم السماح كتحذير")
            c["top_momentum_soft_exception"] = True
            logger.info(f"TOP MOMENTUM SOFT ALLOW | {c.get('symbol')} | score={c.get('score')} | min={min_score} | tags={c.get('execution_setup_tags', [])}")
        strong_candidates.append(c)
    else:
        _log_top_momentum_rejection(c, reason="top_momentum_min_score")

 if not strong_candidates:
    logger.info("Top momentum filter: no candidates above threshold")
    return []

 strong_candidates.sort(
    key=lambda x: (
        x["momentum_priority"],
        x["score"],
        x["change_24h"],
        x["rank_volume_24h"],
    ),
    reverse=True,
 )

 top_n = max(4, int(len(strong_candidates) * TOP_MOMENTUM_PERCENT))
 filtered = strong_candidates[:top_n]
 soft_rank_exceptions = [c for c in strong_candidates[top_n:] if c.get("top_momentum_soft_exception")]
 if soft_rank_exceptions:
    # Keep at most two soft exceptions so STRONG mode does not go silent, without flooding alerts.
    filtered.extend(soft_rank_exceptions[:2])
 filtered_ids = set(id(x) for x in filtered)

 for c in strong_candidates:
    if id(c) not in filtered_ids:
        _log_top_momentum_rejection(c, reason="top_momentum_rank_cut")

 final_candidates = []
 new_count = 0

 for c in filtered:
    if c["is_new"]:
        if new_count >= NEW_LISTING_MAX_PER_RUN:
            _log_top_momentum_rejection(c, reason="top_momentum_new_listing_limit")
            continue
        new_count += 1
    final_candidates.append(c)

 logger.info(
    f"Top momentum filter: kept {len(final_candidates)} of {len(strong_candidates)} "
    f"(from total {len(candidates)})"
 )
 return final_candidates

def diversify_candidates(candidates, max_alerts=3):
 if not candidates:
    return []
 buckets = {}
 for candidate in candidates:
    bucket = get_candidate_bucket(candidate)
    buckets.setdefault(bucket, []).append(candidate)
 for bucket in buckets:
    buckets[bucket].sort(
        key=lambda x: (
            x["momentum_priority"],
            x["score"],
            x["change_24h"],
            x["rank_volume_24h"],
        ),
        reverse=True,
    )
 diversified = []
 used_patterns = set()
 for bucket_name in ["reverse", "new_breakout", "pre_breakout", "breakout", "early_strong", "volume", "standard"]:
    if bucket_name not in buckets or not buckets[bucket_name]:
        continue
    candidate = buckets[bucket_name][0]
    pattern = (
        candidate["breakout"],
        candidate.get("pre_breakout", False),
        round(candidate["vol_ratio"], 1),
        candidate["is_new"],
        candidate.get("early_priority", "none"),
    )
    if pattern not in used_patterns:
        diversified.append(candidate)
        used_patterns.add(pattern)
    if len(diversified) >= max_alerts:
        break
 if len(diversified) < max_alerts:
    remaining = []
    for items in buckets.values():
        remaining.extend(items)
    remaining.sort(
        key=lambda x: (
            x["momentum_priority"],
            x["score"],
            x["change_24h"],
            x["rank_volume_24h"],
        ),
        reverse=True,
    )
    for candidate in remaining:
        if len(diversified) >= max_alerts:
            break
        pattern = (
            candidate["breakout"],
            candidate.get("pre_breakout", False),
            round(candidate["vol_ratio"], 1),
            candidate["is_new"],
            candidate.get("early_priority", "none"),
        )
        if pattern in used_patterns:
            continue
        if candidate in diversified:
            continue
        diversified.append(candidate)
        used_patterns.add(pattern)
 logger.info(
    "Diversified selection: "
    + ", ".join(
        f"{c['symbol']}({get_candidate_bucket(c)}|{c['momentum_priority']})"
        for c in diversified
    )
 )
 return diversified[:max_alerts]

def normalize_reason(reason: str) -> str:
 mapping = {
    "RSI صحي": "RSI في منطقة صحية",
    "RSI جيد": "RSI جيد",
    "RSI صاعد بقوة": "RSI صاعد بقوة",
    "RSI مرتفع لكن بزخم": "RSI مرتفع بزخم",
    "RSI عالي": "RSI عالي (تشبع شراء)",
    "فوليوم داعم": "فوليوم داعم",
    "فوليوم قوي": "فوليوم قوي",
    "فوليوم انفجاري": "فوليوم انفجاري",
    "فوق MA": "فوق المتوسط",
    "شمعة جيدة": "شمعة جيدة",
    "شمعة قوية": "شمعة قوية",
    "اختراق": "اختراق",
    "اختراق مبكر جدا": "اختراق مبكر",
    "اختراق متأخر": "اختراق متأخر",
    "اختراق قوي مؤكد": "اختراق قوي مؤكد",
    "تأكيد فريم الساعة": "تأكيد فريم الساعة",
    "BTC داعم": "BTC داعم",
    "BTC غير داعم": "BTC غير داعم",
    "هيمنة داعمة للألت": "هيمنة داعمة",
    "هيمنة ضد الألت (ضغط على العملات)": "هيمنة ضد الألت",
    "تمويل سلبي (داعم للشراء)": "تمويل سلبي",
    "تمويل إيجابي (ضغط محتمل)": "تمويل إيجابي",
    "عملة جديدة": "عملة جديدة",
    "بداية ترند مبكرة": "بداية ترند مبكرة",
    "زخم مبكر تحت المقاومة": "زخم مبكر تحت المقاومة",
    "بعد عن المتوسط (دخول متأخر)": "MA بعد عن",
    "ممتد زيادة": "ممتد زيادة",
    "أسفل المتوسط": "أسفل المتوسط",
    "رفض سعري علوي": "رفض سعري علوي",
    "اخبار اقتصادية مهمة قريبة": "اخبار اقتصادية مهمة قريبة",
    "Late Pump Risk": "خطر مطاردة Pump متأخر",
    "Bull Market Continuation Risk": "خطر مطاردة Pump متأخر",
    "شمعة قوية لكن احتمال مطاردة": "شمعة قوية لكن احتمال مطاردة",
    "صعود سريع خلال 4 ساعات": "صعود سريع خلال 4 ساعات",
    "Momentum Exhaustion Trap": "خطر نهاية الزخم",
    "far_from_vwap": "بعيد عن VWAP",
    "rsi_slope_weak": "RSI بدأ يضعف",
    "macd_hist_falling": "زخم MACD يتراجع",
    "macd_hist_negative": "MACD سلبي",
    "Weak Historical Setup": "نوع إشارة ضعيف تاريخياً",
    "Late Breakout Warning": "اختراق متأخر، يُنصح بالحذر",
    "رفض سعري علوي بعد الاختراق": "رفض سعري علوي بعد الاختراق",
    "Entry Maturity: موجة متأخرة + امتداد فيبوناتشي": "موجة متأخرة + امتداد فيبوناتشي",
    "Entry Maturity: السعر قريب من نهاية الموجة": "السعر قريب من نهاية الموجة",
    "Entry Maturity: موجة خامسة بدون Pullback واضح": "موجة خامسة بدون Pullback واضح",
    "Entry Maturity: مبكر جدًا، يحتاج تأكيد": "مبكر جدًا ويحتاج تأكيد",
 }
 return mapping.get(reason, reason)

def sort_reasons(reasons):
 priority = {
    "فوق المتوسط": 1,
    "بداية ترند مبكرة": 2,
    "زخم مبكر تحت المقاومة": 3,
    "اختراق": 4,
    "اختراق مبكر": 5,
    "اختراق قوي مؤكد": 6,
    "فوليوم داعم": 7,
    "فوليوم قوي": 8,
    "فوليوم انفجاري": 9,
    "شمعة جيدة": 10,
    "شمعة قوية": 11,
    "RSI في منطقة صحية": 12,
    "RSI جيد": 13,
    "RSI صاعد بقوة": 14,
    "RSI مرتفع بزخم": 15,
    "تأكيد فريم الساعة": 16,
    "BTC داعم": 17,
    "هيمنة داعمة للألت": 18,
    "تمويل سلبي (داعم للشراء)": 19,
    "عملة جديدة": 20,
    "RSI عالي (تشبع شراء)": 101,
    "أسفل المتوسط": 102,
    "بعد عن المتوسط (دخول متأخر)": 103,
    "ممتد زيادة": 104,
    "اختراق متأخر": 105,
    "هيمنة ضد الألت (ضغط على العملات)": 106,
    "BTC غير داعم": 107,
    "تمويل إيجابي (ضغط محتمل)": 108,
    "رفض سعري علوي": 109,
    "اخبار اقتصادية مهمة قريبة": 110,
    "خطر مطاردة": 111,
    "خطر نهاية الزخم": 112,
    "نوع إشارة ضعيف تاريخياً": 113,
    "اختراق متأخر، يُنصح بالحذر": 114,
    "رفض سعري علوي بعد الاختراق": 115,
 }
 return sorted(reasons, key=lambda x: priority.get(x, 200))

def classify_reasons(reasons):
 bullish = []
 warnings = []
 bullish_keywords = ["فوق", "اختراق", "جيدة", "قوية", "داعم", "صحي", "جيد", "صاعد", "مبكر", "تأكيد", "سلبي", "تمويل سلبي", "عملة جديدة", "بداية ترند", "زخم مبكر"]
 warning_keywords = ["Entry Maturity", "موجة", "متأخر", "overextended", "امتداد", "Pullback عميق", "بدون Pullback", "رفض سعري", "خطر", "مطاردة", "RSI عالي", "بعيد عن VWAP", "زخم MACD يتراجع", "Weak Historical Setup", "MACD سلبي"]
 for r in reasons:
    normalized = normalize_reason(r)
    if any(kw in normalized for kw in warning_keywords):
        warnings.append(normalized)
    elif any(kw in normalized for kw in bullish_keywords) and "متأخر" not in normalized and "عالي" not in normalized:
        bullish.append(normalized)
    else:
        warnings.append(normalized)
 return bullish, warnings

def format_bullish_reasons(bullish):
 if not bullish:
    return "• زخم مبكر"
 return "\n".join(f"• {html.escape(r)}" for r in sort_reasons(bullish)[:8])

def classify_opportunity_type_long(
 is_reverse: bool,
 pre_breakout: bool,
 breakout: bool,
 dist_ma: float,
 mtf_confirmed: bool,
) -> str:
 try:
    if is_reverse:
        return "Oversold Reversal"
    if pre_breakout and not breakout:
        return "Breakout مبكر"
    if breakout:
        return "Breakout"
    if dist_ma <= 1.2 and mtf_confirmed:
        return "Pullback"
    return "استمرار"
 except Exception:
    return "استمرار"

def classify_entry_timing_long(
 dist_ma: float,
 breakout: bool,
 pre_breakout: bool,
 vol_ratio: float,
 rsi_now: float = 50.0,
 candle_strength: float = 0.0,
 late_pump_risk: bool = False,
 entry_maturity_data: dict = None,
) -> str:
 try:
    if entry_maturity_data is None:
        entry_maturity_data = {}
    entry_maturity = entry_maturity_data.get("entry_maturity", "unknown")
    fib_position = entry_maturity_data.get("fib_position", "unknown")
    wave_estimate = int(entry_maturity_data.get("wave_estimate", 0) or 0)
    had_pullback = bool(entry_maturity_data.get("had_pullback", False))

    if entry_maturity == "danger_late":
        return "🔴 متأخر جدًا (نهاية موجة)"
    if fib_position == "overextended" and wave_estimate == 5:
        return "🔴 متأخر جدًا (موجة خامسة)"
    if fib_position == "overextended":
        return "🔴 متأخر (امتداد سعري)"
    if wave_estimate == 5 and not had_pullback:
        return "🔴 متأخر (موجة خامسة بلا Pullback)"
    if late_pump_risk:
        return "🔴 متأخر (مطاردة حركة)"
    if entry_maturity == "healthy":
        return "🟢 صحي (Golden Zone + Pullback)"
    if entry_maturity == "early":
        return "🟢 مبكر يحتاج تأكيد"

    if dist_ma > 5.0:
        return "🔴 قرب النهاية"
    if rsi_now >= 68 and dist_ma > 3.2:
        return "🔴 متأخر (RSI مرتفع)"
    if vol_ratio >= 1.9 and candle_strength >= 0.62 and dist_ma > 3.5:
        return "🔴 متأخر (Pump محتمل)"
    if (pre_breakout or breakout) and dist_ma <= 2.6 and 1.10 <= vol_ratio <= 1.85 and rsi_now <= 66:
        return "🟢 مبكر (بداية الحركة)"
    if breakout and 2.6 < dist_ma <= 4.2 and vol_ratio >= 1.20 and rsi_now <= 68:
        return "🟡 متوسط (نص الحركة)"
    if dist_ma <= 3.2 and vol_ratio >= 1.05 and rsi_now <= 66:
        return "🟡 متوسط (نص الحركة)"
    if 3.2 < dist_ma <= 5.0:
        return "🟡 متوسط (نص الحركة)"
    return "🟡 متوسط (نص الحركة)"
 except Exception:
    return "🟡 متوسط (نص الحركة)"

def get_entry_timing_penalty(entry_timing: str) -> float:
 try:
    if "🔴" in entry_timing:
        return 0.25
    if "🟡" in entry_timing:
        return 0.10
    return 0.0
 except Exception:
    return 0.0

def get_base_risk_label(score_result: dict, warnings_count: int) -> str:
 risk_level = score_result.get("risk_level")
 if risk_level:
    return risk_level
 if warnings_count == 0:
    return "🟢 منخفض"
 if warnings_count == 1:
    return "🟡 متوسط"
 return "🔴 مرتفع"

def adjust_risk_with_entry_timing(base_risk: str, entry_timing: str) -> str:
 try:
    if "🔴" in entry_timing:
        return "🔴 مرتفع"
    if "🟡" in entry_timing and base_risk == "🟢 منخفض":
        return "🟡 متوسط"
    return base_risk
 except Exception:
    return base_risk

def build_market_summary(btc_mode: str, alt_mode: str) -> str:
 safe_alt = alt_mode if alt_mode else "🟡 متماسك"
 safe_btc = btc_mode if btc_mode else "🟡 محايد"
 return f"{safe_alt} | BTC: {safe_btc}"

# ===============
# RTL / FORMAT HELPERS
# ================
def rtl_fix(text: str) -> str:
 try:
    if text is None:
        return ""
    return f"\u200F{text}"
 except Exception:
    return str(text)

def fmt_num(value, decimals=2) -> str:
 try:
    return rtl_fix(f"{float(value):.{int(decimals)}f}")
 except Exception:
    return rtl_fix(str(value))

def fmt_pct(value, decimals=2) -> str:
 try:
    return rtl_fix(f"{float(value):+.{int(decimals)}f}%")
 except Exception:
    return rtl_fix(str(value))

def get_breakout_quality(df, vol_ratio: float) -> str:
 try:
    if df is None or df.empty or len(df) < 5:
        return "none"
    signal_row = get_signal_row(df)
    if signal_row is None:
        return "none"
    idx = signal_row.name
    if idx is None or idx < 3:
        return "none"
    close = _safe_float(signal_row["close"])
    open_ = _safe_float(signal_row["open"])
    high = _safe_float(signal_row["high"])
    low = _safe_float(signal_row["low"])
    candle_range = high - low
    if candle_range <= 0:
        return "none"
    body = abs(close - open_)
    upper_wick = high - max(open_, close)
    close_position = (close - low) / candle_range
    lookback_start = max(0, idx - 20)
    recent_high = float(df["high"].iloc[lookback_start:idx].max())
    bullish_close = close > open_
    broke_above = close > recent_high
    strong_close = close_position >= 0.65
    ok_close = close_position >= 0.50
    small_wick = upper_wick <= body * 0.6
    vol_ok = vol_ratio >= 1.3
    if not bullish_close or not broke_above:
        return "none"
    score = 0
    if strong_close:
        score += 2
    elif ok_close:
        score += 1
    if small_wick:
        score += 1
    if vol_ok:
        score += 1
    if score >= 4:
        return "strong"
    if score >= 2:
        return "ok"
    return "weak"
 except Exception:
    return "none"

# =========================
# SL / TP LOGIC
# =========================
SMART_TP1_ENABLED = True
SMART_TP1_MIN_RR = 1.2
SMART_TP1_DEFAULT_RR_FALLBACK = 2.0
SMART_TP1_RESISTANCE_BUFFER_ATR = 0.20
SMART_TP1_NEAR_RESISTANCE_RR = 1.2
SMART_TP1_LOOKBACK_SWING = 50
SMART_TP1_ROUND_LEVELS_ENABLED = True

# Smart SL Settings
SMART_SL_ENABLED = True
SMART_SL_SUPPORT_LOOKBACK = 50
SMART_SL_ATR_BUFFER = 0.35
SMART_SL_MIN_PCT = 1.85
SMART_SL_MAX_PCT = 3.50
SMART_SL_FALLBACK_ATR_MULT = 2.8

def get_rr_targets_long(signal_type="standard", entry_timing=""):
 if signal_type == "breakout":
    return 2.2, 3.5
 if signal_type == "pre_breakout":
    return 2.3, 3.8
 if signal_type == "new_listing":
    return 2.5, 4.0
 if "🔴 متأخر" in entry_timing:
    return 2.0, 3.2
 return 2.0, 3.2

def calc_tp_long(entry: float, sl: float, rr: float) -> float:
 risk = float(entry) - float(sl)
 return round(float(entry) + (risk * rr), 6)

def _round_price_dynamic(value: float) -> float:
    try:
        value = float(value)
        if value <= 0:
            return 0.0
        if value >= 100:
            return round(value, 2)
        if value >= 1:
            return round(value, 4)
        if value >= 0.01:
            return round(value, 6)
        return round(value, 8)
    except Exception:
        return 0.0

def is_major_round_level(price: float) -> bool:
    if price <= 0:
        return False
    if price in (100, 500, 1000, 5000, 10000, 50000):
        return True
    if price >= 100 and price % 100 == 0:
        return True
    if 10 <= price < 100 and price % 10 == 0:
        return True
    if 1 <= price < 10 and price % 1 == 0:
        return True
    return False

def is_valid_round_resistance(df, level: float, entry: float, lookback: int = 50) -> bool:
    """Accept round numbers as resistance only when recent price action rejected them."""
    try:
        if df is None or getattr(df, "empty", True) or level <= entry:
            return False

        recent = df.tail(lookback)
        touches = 0
        rejections = 0

        for _, row in recent.iterrows():
            high = float(row.get("high", 0) or 0)
            close = float(row.get("close", 0) or 0)
            open_ = float(row.get("open", 0) or 0)

            if high <= 0 or close <= 0 or level <= 0:
                continue

            near_level = abs(high - level) / level <= 0.003
            rejected = near_level and close < level and close <= open_

            if near_level:
                touches += 1
            if rejected:
                rejections += 1

        return touches >= 1 and rejections >= 1
    except Exception as e:
        logger.warning(f"is_valid_round_resistance error: {e}")
        return False


def collect_resistance_candidates_long(df, entry: float, lookback: int = 50) -> list:
    """Collect real resistance candidates above entry from structure, bands and confirmed round levels."""
    candidates = []
    try:
        if df is None or getattr(df, "empty", True) or entry <= 0:
            return candidates

        recent = df.tail(lookback).copy()
        if recent.empty:
            return candidates

        # 1) Real swing highs: local pivot high, not any previous high.
        highs = recent["high"].astype(float).tolist()
        for i in range(2, len(highs) - 2):
            level = highs[i]
            if (
                level > entry
                and level > highs[i - 1]
                and level > highs[i - 2]
                and level > highs[i + 1]
                and level > highs[i + 2]
            ):
                candidates.append({"price": level, "level": level, "source": "swing_high", "strength": 3})

        # 2) Recent high from the last 20 candles.
        try:
            recent20 = df.tail(20)
            level = float(recent20["high"].astype(float).max())
            if level > entry:
                candidates.append({"price": level, "level": level, "source": "recent_20_high", "strength": 2})
        except Exception:
            pass

        # 3) Bollinger upper band if available.
        for col in ("bb_upper", "bollinger_upper", "upper_band"):
            if col in df.columns:
                try:
                    level = float(df.iloc[-1].get(col, 0) or 0)
                    if level > entry:
                        candidates.append({"price": level, "level": level, "source": "bb_upper", "strength": 2})
                    break
                except Exception:
                    pass

        # 4) Previous rejection levels: upper wick rejection above entry.
        for _, row in recent.iterrows():
            try:
                high = float(row.get("high", 0) or 0)
                close = float(row.get("close", 0) or 0)
                open_ = float(row.get("open", 0) or 0)
                low = float(row.get("low", 0) or 0)
                if high <= entry or high <= 0:
                    continue
                candle_range = high - low
                if candle_range <= 0:
                    continue
                upper_wick = high - max(open_, close)
                rejected = (upper_wick / candle_range) >= 0.35 and close < high
                if rejected:
                    candidates.append({"price": high, "level": high, "source": "previous_rejection", "strength": 3})
            except Exception:
                continue

        # 5) Confirmed round levels only.
        if SMART_TP1_ROUND_LEVELS_ENABLED:
            try:
                if entry >= 100:
                    steps = [1, 5, 10]
                elif entry >= 10:
                    steps = [0.1, 0.5, 1]
                elif entry >= 1:
                    steps = [0.01, 0.05, 0.1]
                elif entry >= 0.1:
                    steps = [0.001, 0.005, 0.01]
                elif entry >= 0.01:
                    steps = [0.0001, 0.0005, 0.001]
                else:
                    steps = [entry * 0.01, entry * 0.05, entry * 0.1]

                for step in steps:
                    if step <= 0:
                        continue
                    next_level = round(((entry // step) + 1) * step, 12)
                    if next_level > entry and is_valid_round_resistance(df, next_level, entry, lookback=lookback):
                        candidates.append({
                            "price": next_level,
                            "level": next_level,
                            "source": "round_level_confirmed",
                            "strength": 1,
                        })
            except Exception:
                pass

        # Merge duplicates/near-duplicates, keeping the stronger source.
        cleaned = []
        for c in candidates:
            try:
                level = float(c.get("price", c.get("level", 0)) or 0)
                if level <= entry:
                    continue
                duplicate = False
                for old in cleaned:
                    old_level = float(old.get("price", old.get("level", 0)) or 0)
                    if old_level > 0 and abs(level - old_level) / old_level <= 0.002:
                        duplicate = True
                        if c.get("strength", 0) > old.get("strength", 0):
                            old.update(c)
                            old["price"] = level
                            old["level"] = level
                        break
                if not duplicate:
                    c["price"] = level
                    c["level"] = level
                    cleaned.append(c)
            except Exception:
                continue

        cleaned.sort(key=lambda x: float(x.get("price", 0) or 0))
        return cleaned
    except Exception as e:
        logger.warning(f"collect_resistance_candidates_long error: {e}")
        return candidates


# Backward-compatible alias for older calls in this file.
def _collect_resistance_candidates_long(df, entry: float) -> list:
    return collect_resistance_candidates_long(df, entry, lookback=SMART_TP1_LOOKBACK_SWING)


def find_nearest_resistance_long(df, entry: float) -> dict:
    try:
        candidates = collect_resistance_candidates_long(df, entry, lookback=SMART_TP1_LOOKBACK_SWING)
        valid = []
        for c in candidates:
            try:
                level = float(c.get("price", c.get("level", 0)) or 0)
                if level > entry:
                    valid.append(c)
            except Exception:
                pass

        if not valid:
            return None

        source_priority = {
            "swing_high": 1,
            "previous_rejection": 2,
            "recent_20_high": 3,
            "bb_upper": 4,
            "round_level_confirmed": 5,
        }
        valid.sort(
            key=lambda c: (
                float(c.get("price", c.get("level", 0)) or 0),
                source_priority.get(c.get("source", ""), 99),
            )
        )
        return valid[0]
    except Exception as e:
        logger.warning(f"find_nearest_resistance_long error: {e}")
        return None


def find_nearest_support_long(df, entry: float):
    try:
        if df is None or df.empty or entry <= 0:
            return None

        signal_row = get_signal_row(df)
        if signal_row is None:
            return None

        idx = signal_row.name
        if idx is None:
            return None

        start = max(0, idx - 50)
        work = df.iloc[start:idx + 1].copy()
        if work.empty:
            return None

        lows = work["low"].astype(float)
        supports = [float(x) for x in lows.tail(40).tolist() if _safe_float(x, 0.0) < entry]

        ma_value = _safe_float(signal_row.get("ma"), 0.0)
        if 0 < ma_value < entry:
            supports.append(ma_value)

        vwap_value = _safe_float(signal_row.get("vwap"), 0.0)
        if 0 < vwap_value < entry:
            supports.append(vwap_value)

        if not supports:
            return None

        nearest = max(supports)
        return {
            "price": nearest,
            "source": "nearest_support"
        }

    except Exception as e:
        logger.warning(f"find_nearest_support_long error: {e}")
        return None


def build_smart_tp1_long(
    df,
    entry: float,
    sl: float,
    rr1: float,
    rr2: float,
    atr_value: float,
    market_state: str,
    breakout: bool,
    pre_breakout: bool,
) -> dict:
    result = {
        "tp1": calc_tp_long(entry, sl, rr=rr1),
        "tp2": calc_tp_long(entry, sl, rr=rr2),
        "nearest_resistance": None,
        "nearest_resistance_source": None,
        "nearest_support": None,
        "target_method": "rr",
        "target_notes": [],
        "resistance_warning": "",
        "support_warning": "",
        "rr1_effective": rr1,
        "rr2_effective": rr2,
    }

    try:
        if not SMART_TP1_ENABLED:
            return result

        entry = float(entry)
        sl = float(sl)
        atr_value = float(atr_value or 0.0)

        risk = entry - sl
        if entry <= 0 or sl <= 0 or risk <= 0:
            result["target_notes"].append("invalid_risk_fallback_rr")
            return result

        min_tp1 = entry + (risk * SMART_TP1_MIN_RR)
        rr_tp1 = entry + (risk * rr1)

        nearest_resistance_data = find_nearest_resistance_long(df, entry)
        nearest_support_data = find_nearest_support_long(df, entry)

        if nearest_resistance_data:
            nearest_resistance = _safe_float(nearest_resistance_data.get("price"), 0.0)
            result["nearest_resistance"] = _round_price_dynamic(nearest_resistance)
            result["nearest_resistance_source"] = nearest_resistance_data.get("source", "unknown")
        else:
            nearest_resistance = 0.0

        if nearest_support_data:
            nearest_support = _safe_float(nearest_support_data.get("price"), 0.0)
            result["nearest_support"] = _round_price_dynamic(nearest_support)

        if nearest_resistance <= 0:
            result["tp1"] = _round_price_dynamic(rr_tp1)
            result["tp2"] = _round_price_dynamic(calc_tp_long(entry, sl, rr=rr2))
            result["target_method"] = "rr_no_resistance"
            result["target_notes"].append("no_nearest_resistance_found")
            return result

        buffer = atr_value * SMART_TP1_RESISTANCE_BUFFER_ATR if atr_value > 0 else risk * 0.10
        resistance_before_buffer = nearest_resistance - buffer

        resistance_rr = (nearest_resistance - entry) / risk if risk > 0 else 0.0
        buffered_rr = (resistance_before_buffer - entry) / risk if risk > 0 else 0.0

        source_label = nearest_resistance_data.get("source", "unknown")

        if resistance_rr < SMART_TP1_MIN_RR:
            result["tp1"] = _round_price_dynamic(min_tp1)
            result["tp2"] = _round_price_dynamic(calc_tp_long(entry, sl, rr=rr2))
            result["target_method"] = "rr_min_due_close_resistance"
            result["resistance_warning"] = "مقاومة قريبة جدًا قبل TP1"
            result["target_notes"].append(
                f"nearest_resistance_before_min_rr rr={resistance_rr:.2f} source={source_label}"
            )
            result["rr1_effective"] = SMART_TP1_MIN_RR
            return result

        if SMART_TP1_MIN_RR <= buffered_rr < rr1:
            smart_tp1 = max(min_tp1, resistance_before_buffer)
            result["tp1"] = _round_price_dynamic(smart_tp1)
            result["tp2"] = _round_price_dynamic(calc_tp_long(entry, sl, rr=rr2))
            result["target_method"] = "structure_before_resistance"
            result["target_notes"].append(
                f"tp1_before_resistance source={source_label} rr={buffered_rr:.2f}"
            )
            result["rr1_effective"] = round((smart_tp1 - entry) / risk, 2)
            return result

        result["tp1"] = _round_price_dynamic(rr_tp1)
        tp2_rr = rr2
        if market_state in ("bull_market", "alt_season") and (breakout or pre_breakout):
            tp2_rr = max(rr2, rr1 + 1.2)

        result["tp2"] = _round_price_dynamic(calc_tp_long(entry, sl, rr=tp2_rr))
        result["target_method"] = "rr_with_structure_check"
        result["target_notes"].append(
            f"resistance_ok source={source_label} rr={resistance_rr:.2f}"
        )
        result["rr1_effective"] = rr1
        result["rr2_effective"] = tp2_rr
        return result

    except Exception as e:
        logger.warning(f"build_smart_tp1_long error: {e}")
        result["target_notes"].append("smart_tp1_error_fallback_rr")
        return result

def near_resistance_guard_long(
    resistance_warning: str,
    nearest_resistance: float,
    market_state: str,
    btc_mode: str,
    alt_mode: str,
    current_mode: str,
    display_risk: str,
    late_guard: dict,
    vol_ratio: float,
    candle_strength: float,
    upper_wick_ratio: float,
    breakout: bool,
    breakout_quality: str,
    mtf_confirmed: bool,
    rsi_now: float,
    dist_ma: float,
    vwap_distance: float,
    score_after_penalties: float,
) -> tuple:
    if not resistance_warning or resistance_warning != "مقاومة قريبة جدًا قبل TP1":
        return False, 0.0

    if market_state in ("risk_off",):
        base_penalty = 0.75
    elif market_state == "btc_leading":
        base_penalty = 0.60
    elif market_state == "mixed":
        base_penalty = 0.45
    else:
        base_penalty = 0.25

    weak_market_conditions = (
        market_state in ("risk_off", "btc_leading", "mixed")
        or "هابط" in btc_mode
        or "ضعيف" in alt_mode
        or "مرتفع" in display_risk
        or current_mode == MODE_STRONG_LONG_ONLY
        or late_guard.get("late_pump_risk", False)
        or (vol_ratio >= 1.8 and candle_strength >= 0.60)
        or upper_wick_ratio >= 0.35
    )

    if weak_market_conditions:
        strong_exception = (
            breakout
            and breakout_quality == "strong"
            and mtf_confirmed
            and 1.30 <= vol_ratio <= 1.90
            and rsi_now <= 66
            and dist_ma <= 3.2
            and vwap_distance <= 2.0
            and upper_wick_ratio < 0.35
            and score_after_penalties >= 7.8
            and market_state != "risk_off"
        )
        if not strong_exception:
            return True, 0.0

    return False, base_penalty

# =====================
# FORMAT ENTRY MATURITY BLOCK
# =====================
def format_entry_maturity_block(entry_maturity_data: dict) -> str:
 if not entry_maturity_data or entry_maturity_data.get("entry_maturity", "unknown") == "unknown":
    return ""
 try:
    fib_label = html.escape(str(entry_maturity_data.get("fib_label", "غير معروف")))
    pullback_label = html.escape(str(entry_maturity_data.get("pullback_label", "غير معروف")))
    wave_label = html.escape(str(entry_maturity_data.get("wave_label", "غير معروف")))
    status = html.escape(str(entry_maturity_data.get("entry_maturity", "unknown")))
    return f"""🧬 <b>نضج الدخول:</b>
• Fib: {fib_label}
• Pullback: {pullback_label}
• Wave: {wave_label}
• Status: {status}"""
 except Exception:
    return ""

# =====================
# EXECUTION BADGE
# =====================
def _candidate_has_complete_execution_plan(candidate: dict) -> bool:
    try:
        entry_mode = str(candidate.get("entry_mode", "market") or "market").lower()
        has_pullback = bool(candidate.get("has_pullback_plan")) or entry_mode in ("pullback_pending", "pullback_triggered")
        if has_pullback:
            required = (candidate.get("execution_entry"), candidate.get("execution_sl"), candidate.get("execution_tp1"))
            return all(_safe_trade_float_value(v) is not None for v in required)
        # Market execution can use the normal signal plan.
        required = (candidate.get("entry"), candidate.get("sl"), candidate.get("tp1"))
        return all(_safe_trade_float_value(v) is not None for v in required)
    except Exception:
        return False


def _apply_market_execution_fallback(candidate: dict) -> dict:
    """Fill execution_* for market entries so preview/report never shows None."""
    try:
        entry_mode = str(candidate.get("entry_mode", "market") or "market").lower()
        has_pullback = bool(candidate.get("has_pullback_plan")) or entry_mode in ("pullback_pending", "pullback_triggered")
        if not has_pullback:
            candidate["execution_entry"] = candidate.get("execution_entry") or candidate.get("recommended_entry") or candidate.get("market_entry") or candidate.get("entry")
            candidate["execution_sl"] = candidate.get("execution_sl") or candidate.get("sl")
            candidate["execution_tp1"] = candidate.get("execution_tp1") or candidate.get("tp1")
            candidate["execution_tp2"] = candidate.get("execution_tp2") or candidate.get("tp2")
        return candidate
    except Exception:
        return candidate



def _decide_long_execution_candidate(candidate: dict, mutate: bool = False) -> dict:
    """Central long execution gate.

    NORMAL_LONG intentionally preserves the previous behaviour:
    strict execution whitelist/block exception + complete plan + Weak Drift quality.
    STRONG_LONG_ONLY can additionally pass through the Elite path in
    execution.long_execution_gate.
    """
    try:
        if not isinstance(candidate, dict):
            return {"allowed": False, "path": "blocked", "reason": "invalid_candidate"}
        planned = candidate if mutate else dict(candidate or {})
        planned = _apply_market_execution_fallback(planned)
        _ensure_execution_setup_tags(planned)
        mode = normalize_market_mode(
            _trade_field(planned, "current_mode", "")
            or _trade_field(planned, "market_mode", "")
            or _trade_field(planned, "mode", "")
            or MODE_NORMAL_LONG
        )
        strict_setup_allowed = _has_strict_execution_setup(planned)
        if mode == MODE_NORMAL_LONG and not strict_setup_allowed:
            strict_setup_allowed = _has_normal_long_execution_setup(planned)
        block_mode_allowed = _is_block_mode_execution_candidate(planned)
        has_complete_plan = _candidate_has_complete_execution_plan(planned)
        weak_drift_passed = _candidate_passes_weak_drift_execution_quality(planned)
        base_execution_allowed = bool(
            (strict_setup_allowed or block_mode_allowed)
            and has_complete_plan
            and weak_drift_passed
        )
        gate = decide_long_execution_gate(
            planned,
            mode,
            base_execution_allowed=base_execution_allowed,
            strict_setup_allowed=strict_setup_allowed,
            block_mode_allowed=block_mode_allowed,
            has_complete_plan=has_complete_plan,
            weak_drift_passed=weak_drift_passed,
            mutate=mutate,
        )
        if (not gate.get("allowed")) and logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "EXEC REJECT | "
                f"symbol={planned.get('symbol', '?')} | mode={mode} | reason={gate.get('reason')} | "
                f"path={gate.get('path')} | tags={planned.get('execution_setup_tags', [])} | "
                f"strict={strict_setup_allowed} | block={block_mode_allowed} | plan={has_complete_plan} | weak_drift={weak_drift_passed}"
            )
        if mutate:
            candidate.update(planned)
            candidate["execution_gate_path"] = gate.get("path")
            candidate["execution_gate_reason"] = gate.get("reason")
            elite = gate.get("elite") or {}
            candidate["elite_execution_passed"] = bool(elite.get("passed") or candidate.get("elite_execution_passed"))
            if elite.get("score") is not None:
                candidate["elite_execution_score"] = elite.get("score")
            if elite.get("level"):
                candidate["elite_execution_level"] = elite.get("level")
            if elite.get("reasons"):
                candidate["elite_execution_reasons"] = elite.get("reasons")
            if elite.get("warnings"):
                candidate["elite_execution_warnings"] = elite.get("warnings")
        return gate
    except Exception as e:
        logger.warning(f"_decide_long_execution_candidate error: {e}")
        return {"allowed": False, "path": "blocked", "reason": "execution_gate_error"}


def is_candidate_for_execution(candidate: dict) -> bool:
    """True only for alerts that pass the long execution gate plus a complete plan.

    NORMAL_LONG keeps the previous execution behaviour.
    STRONG_LONG_ONLY may pass through whitelist OR Elite.
    """
    try:
        gate = _decide_long_execution_candidate(candidate, mutate=False)
        return bool(gate.get("allowed"))
    except Exception:
        return False

def build_execution_badge_line(candidate: dict) -> str:
    _ensure_execution_setup_tags(candidate)
    if not is_candidate_for_execution(candidate):
        return ""
    return (
        "🚀🔥 <b>EXECUTION PRIORITY</b>\n"
        "══════════════\n\n"
        "🔥 <b>مرشحة للتنفيذ التجريبي</b>\n"
        "✅ <b>Whitelist ACTIVE</b>\n"
        "🧠 <b>Quality Filters: PASS</b>\n"
        "⚡ <b>Preview Ready</b>"
    )


# =====================
# BUILD MESSAGE (normal)
# =====================
def build_message(
 symbol,
 price,
 score_result,
 stop_loss,
 tp1,
 tp2,
 rr1,
 rr2,
 btc_mode,
 btc_dominance_proxy,
 tv_link,
 is_new,
 change_24h=0.0,
 market_state_label=None,
 market_bias_label=None,
 alt_mode=None,
 news_warning="",
 opportunity_type="",
 entry_timing="",
 display_risk="",
 setup_stats=None,
 is_reverse=False,
 reversal_4h_confirmed=False,
 reversal_4h_details="",
 breakout_quality="none",
 pullback_low=None,
 pullback_high=None,
 entry_maturity_data=None,
 warning_penalty=0.0,
 resistance_warning="",
 target_method="rr",
 nearest_resistance=None,
 wave_context="",
 extra_setup_names=None,
 primary_extra_setup="",
 has_pullback_plan=False,
 market_price=None,
 sl_method="",
 context_setups=None,
 execution_entry=None,         # NEW
 execution_sl=None,
 execution_tp1=None,
 execution_tp2=None,
):
 symbol_clean = clean_symbol_for_message(symbol)
 bullish, inferred_warnings = classify_reasons(score_result.get("reasons", []))
 explicit_warnings = [
    normalize_reason(w)
    for w in (score_result.get("warning_reasons") or [])
 ]
 warnings = explicit_warnings if explicit_warnings else inferred_warnings
 warnings = list(dict.fromkeys(warnings))
 warnings = sort_reasons(warnings)
 if is_reverse:
    reverse_reason = "Oversold reversal بعد هبوط/امتداد مبالغ فيه"
    if reverse_reason not in bullish:
        bullish = [reverse_reason] + bullish
 bullish_text = format_bullish_reasons(bullish) if bullish else "• زخم مبكر"
 warnings_text = "\n".join(f"• {html.escape(w)}" for w in warnings) if warnings else ""
 funding_text = score_result.get("funding_label", "🟡 محايد")
 signal_rating = score_result.get("signal_rating", "⚡ عادي")
 sl_pct = calculate_sl_percent(price, stop_loss)
 tp1_pct = round(((tp1 - price) / price) * 100, 2) if price else 0.0
 tp2_pct = round(((tp2 - price) / price) * 100, 2) if price else 0.0
 new_tag = "\n🆕 <b>عملة جديدة</b>" if is_new else ""
 reverse_banner = get_reverse_banner_long(is_reverse)
 reverse_note = get_reverse_style_note_long(is_reverse)
 safe_4h = rtl_fix("4H")
 safe_15m = rtl_fix("15m")
 safe_1h = rtl_fix("1H")
 safe_24h = rtl_fix("24H")
 pullback_text = ""
 if pullback_low is not None and pullback_high is not None:
    if has_pullback_plan and market_price is not None:
        pullback_text = ""
        if execution_entry is not None:
            pullback_text += (
                f"📌 <b>Pullback Entry:</b> {fmt_num(pullback_low, 6)} → {fmt_num(pullback_high, 6)}\n"
                f"🎯 <b>Execution Entry:</b> {fmt_num(execution_entry, 6)}\n"
                f"💰 <b>سعر السوق الحالي:</b> {fmt_num(market_price, 6)}\n"
            )
        # Execution SL/TP lines
        if execution_sl is not None:
            pullback_text += f"🛑 <b>Execution SL:</b> {fmt_num(execution_sl, 6)}\n"
        if execution_tp1 is not None:
            pullback_text += f"🎯 <b>Execution TP1:</b> {fmt_num(execution_tp1, 6)}\n"
        if execution_tp2 is not None:
            pullback_text += f"🏁 <b>Execution TP2:</b> {fmt_num(execution_tp2, 6)}\n"
    else:
        pullback_text = (
            f"📥 <b>منطقة بول باك مقترحة للمراقبة:</b> "
            f"من {fmt_num(pullback_low, 6)} إلى {fmt_num(pullback_high, 6)}\n"
        )
        if execution_entry is not None:
            pullback_text += f"🎯 <b>Execution Entry:</b> {fmt_num(execution_entry, 6)}\n"

 if is_reverse:
    if reversal_4h_confirmed:
        reversal_4h_block = (
            f"\n✅ <b>تأكيد فريم {safe_4h}:</b>\n"
            f"{reversal_4h_details}"
        )
    else:
        reversal_4h_block = (
            f"\n🔴 <b>تحذير فريم {safe_4h}:</b> غير مؤكد\n"
            f"{reversal_4h_details}\n"
            f"• مخاطرة أعلى --- راجع شارت {safe_4h} قبل الدخول"
        )
 else:
    reversal_4h_block = ""
 bq_map = {
    "strong": "🟢 كسر قوي",
    "ok": "🟡 كسر مقبول",
    "weak": "🔴 كسر ضعيف --- تحقق قبل الدخول",
 }
 bq_label = bq_map.get(breakout_quality, "")
 breakout_quality_block = f"\n🧩 <b>جودة الكسر:</b> {bq_label}" if bq_label else ""
 entry_maturity_block = format_entry_maturity_block(entry_maturity_data)
 safe_symbol = html.escape(symbol_clean)
 safe_market = html.escape(build_market_summary(btc_mode=btc_mode, alt_mode=alt_mode or "🟡 متماسك"))
 safe_funding = html.escape(funding_text)
 safe_rating = html.escape(signal_rating)
 safe_tv_link = html.escape(tv_link, quote=True)
 safe_opportunity_type = html.escape(opportunity_type)
 safe_entry_timing = html.escape(entry_timing)
 safe_display_risk = html.escape(display_risk)
 warnings_block = f"\n\n⚠️ <b>ملاحظات:</b>\n{warnings_text}" if warnings_text else ""
 news_block = f"\n\n{news_warning}" if news_warning else ""
 reverse_block = f"\n{reverse_note}" if reverse_note else ""
 hybrid_label = html.escape(
    get_hybrid_label_from_stats(setup_stats or {})
 )
 header_block = f"{hybrid_label}\n\n" if hybrid_label else ""
 if reverse_banner:
    header_block += f"{reverse_banner}\n\n"

 penalty_text = ""
 if warning_penalty > 0:
    penalty_text = f"\n🧮 <b>تأثير التحذيرات على السكور:</b> {fmt_num(-warning_penalty, 2)}"

 resistance_text = ""
 if resistance_warning:
    resistance_text = f"\n⚠️ <b>{html.escape(resistance_warning)}</b>"

 target_text = ""
 if target_method and target_method != "rr":
    target_text += f"\n🎯 <b>Target Method:</b> {html.escape(str(target_method))}"
 if nearest_resistance:
    target_text += f"\n🧱 <b>أقرب مقاومة:</b> {fmt_num(nearest_resistance, 6)}"

 wave_text = ""
 if wave_context:
    wave_text = f"\n🌊 <b>Wave:</b> {html.escape(wave_context)}"

 extra_setup_text = ""
 if primary_extra_setup:
    extra_setup_text = f"\n🧩 <b>Setup إضافي:</b> {html.escape(primary_extra_setup)}"

 context_text = ""
 if context_setups:
    context_joined = " | ".join(html.escape(s) for s in context_setups)
    context_text = f"\n🧩 <b>Context:</b> {context_joined}"

 sl_method_text = ""
 if sl_method:
    sl_method_text = f"\n🛡 <b>SL Method:</b> {html.escape(sl_method)}"

 if has_pullback_plan and market_price is not None:
    price_line = f"🎯 <b>الدخول المخطط:</b> {fmt_num(price, 6)} | 💰 <b>السوق:</b> {fmt_num(market_price, 6)} | ⏱ <b>الفريم:</b> {safe_15m}"
 else:
    price_line = f"💰 <b>السعر:</b> {fmt_num(price, 6)} | ⏱ <b>الفريم:</b> {safe_15m}"

 return f"""{header_block}📈 <b>LONG SIGNAL</b>
┄┄┄┄┄┄┄┄┄┄┄┄

📊 <b>لونج فيوتشر | {safe_symbol}</b>
{price_line}
⭐ <b>السكور:</b> {rtl_fix(f"{float(score_result['score']):.1f} / 10")}
🏷 <b>التصنيف:</b> {safe_rating}
{pullback_text}
🎯 <b>TP1:</b> {fmt_num(tp1, 6)} ({fmt_pct(tp1_pct)} | {rtl_fix(f"{rr1}R")} | إغلاق 40%)
🏁 <b>TP2:</b> {fmt_num(tp2, 6)} ({fmt_pct(tp2_pct)} | {rtl_fix(f"{rr2}R")} | إغلاق 40%)
🔄 <b>بعد TP2:</b> 20% trailing stop ({TRAILING_PCT}% تحت الـ high)
🛡 <b>بعد TP1:</b> نقل SL إلى Entry
🛑 <b>SL:</b> {fmt_num(stop_loss, 6)} ({rtl_fix(f"-{abs(float(sl_pct)):.2f}%")}){sl_method_text}
🧠 <b>نوع الفرصة:</b> {safe_opportunity_type}{reverse_block}{reversal_4h_block}{breakout_quality_block}{extra_setup_text}{context_text}
🌍 <b>السوق:</b> {safe_market}
💸 <b>التمويل:</b> {safe_funding}
📈 <b>تغير {safe_24h}:</b> {fmt_pct(change_24h)}{new_tag}
📊 <b>أسباب الدخول:</b>
{bullish_text}{warnings_block}{news_block}{penalty_text}{resistance_text}{target_text}{wave_text}
📍 <b>الدخول:</b> {safe_entry_timing}
{entry_maturity_block}
⚖️ <b>المخاطرة:</b> {safe_display_risk}
🔗 <a href="{safe_tv_link}">Open Chart ({safe_15m} / {safe_1h})</a>"""

# =========================
# BUILD RECOVERY LONG MESSAGE
# =========================
def build_recovery_long_message(
 symbol,
 entry1,
 entry2,
 sl,
 tp1,
 tp2,
 rr1,
 rr2,
 atr_value,
 red_ratio,
 avg_change,
 btc_change,
 alt_mode,
 tv_link,
):
 symbol_clean = clean_symbol_for_message(symbol)
 avg_entry = round((entry1 + entry2) / 2, 6)
 sl_pct = round(((avg_entry - sl) / avg_entry) * 100, 2) if avg_entry > 0 else 0.0
 tp1_pct = round(((tp1 - avg_entry) / avg_entry) * 100, 2) if avg_entry > 0 else 0.0
 tp2_pct = round(((tp2 - avg_entry) / avg_entry) * 100, 2) if avg_entry > 0 else 0.0
 red_ratio_pct = round(float(red_ratio or 0) * 100, 1)
 safe_symbol = html.escape(symbol_clean)
 safe_tv_link = html.escape(tv_link, quote=True)
 return f"""🔄 <b>RECOVERY LONG MODE</b>
🪙 <b>{safe_symbol}</b>

📉 <b>السياق:</b>
السوق خارج من ضغط/كراش، والفرصة محاولة صيد ارتداد بحجم صغير

📊 <b>حجم الصفقة:</b> {RECOVERY_TOTAL_SIZE_PCT}% من الصفقة العادية
• Entry 1: {RECOVERY_ENTRY1_SIZE_PCT}% عند {fmt_num(entry1, 6)}
• Entry 2: {RECOVERY_ENTRY2_SIZE_PCT}% عند {fmt_num(entry2, 6)}
• متوسط الدخول المخطط: {fmt_num(avg_entry, 6)}

🎯 <b>الأهداف:</b>
• TP1: {fmt_num(tp1, 6)} ({fmt_pct(tp1_pct)} | {rtl_fix(f"{rr1}R")} | إغلاق 40%)
• TP2: {fmt_num(tp2, 6)} ({fmt_pct(tp2_pct)} | {rtl_fix(f"{rr2}R")} | إغلاق 40%)
• بعد TP2: 20% trailing ({TRAILING_PCT}% تحت الـ high)
• بعد TP1: نقل SL إلى Entry

🛑 <b>وقف الخسارة:</b>
• SL: {fmt_num(sl, 6)} ({rtl_fix(f"-{abs(float(sl_pct)):.2f}%")})
• ملاحظة: SL أوسع لأن المود Recovery بعد هبوط قوي (ATR × {RECOVERY_SL_ATR_MULT})

📈 <b>سبب الدخول:</b>
• هبوط/امتداد زائد
• RSI منخفض أو بدأ يرتد
• تحسن شمعة 15m
• الفوليوم يدعم الارتداد
• BTC لم يعد ينهار

🌍 <b>حالة السوق:</b>
• Red Ratio 15m: {red_ratio_pct}%
• Avg Market Change 15m: {fmt_pct(avg_change)}
• BTC 15m: {fmt_pct(btc_change)}
• Alt Mode: {html.escape(alt_mode)}

⚠️ <b>تحذير:</b>
هذه ليست إشارة Long عادية.
هذه محاولة Recovery عالية المخاطر بحجم صغير.

🔗 <a href="{safe_tv_link}">Open Chart</a>"""

# =========================
# MARKET GUARD
# =========================
def get_last_candle_change_pct(df) -> float:
 try:
    signal_row = get_signal_row(df)
    if signal_row is None:
        return 0.0
    open_ = _safe_float(signal_row["open"], 0.0)
    close = _safe_float(signal_row["close"], 0.0)
    if open_ <= 0:
        return 0.0
    return round(((close - open_) / open_) * 100, 4)
 except Exception:
    return 0.0

def get_market_guard_snapshot(ranked_pairs, btc_mode: str, alt_snapshot: dict, candles_map_15m: dict = None, btc_zone: dict = None) -> dict:
 if not MARKET_GUARD_ENABLED:
    return {
        "active": False,
        "block_longs": False,
        "level": "normal",
        "valid_count": 0,
        "red_ratio_15m": 0.0,
        "avg_change_15m": 0.0,
        "btc_change_15m": 0.0,
        "reason": "disabled",
        "alt_weak_cautious": False,
    }
 if not ranked_pairs:
    return {
        "active": False,
        "block_longs": False,
        "level": "normal",
        "valid_count": 0,
        "red_ratio_15m": 0.0,
        "avg_change_15m": 0.0,
        "btc_change_15m": 0.0,
        "reason": "no ranked pairs",
        "alt_weak_cautious": False,
    }
 try:
    btc_zone = btc_zone or {}
    btc_zone_name = str(btc_zone.get("zone", "") or "")
    btc_zone_breakdown = bool(btc_zone.get("breakdown", False))
    btc_lower_range = btc_zone_name == "lower_range" and not btc_zone_breakdown
    sample = sorted(
        ranked_pairs,
        key=lambda x: x.get("_rank_volume_24h", 0),
        reverse=True
    )[:MARKET_GUARD_SAMPLE_SIZE]
    changes = []
    red_count = 0
    valid = 0
    for item in sample:
        symbol = item.get("instId", "")
        if not symbol:
            continue
        if candles_map_15m and symbol in candles_map_15m:
            candles = candles_map_15m[symbol]
        else:
            candles = get_candles(symbol, MARKET_GUARD_TIMEFRAME, MARKET_GUARD_CANDLE_LIMIT)
        df = to_dataframe(candles)
        if df is None or df.empty:
            continue
        change = get_last_candle_change_pct(df)
        changes.append(change)
        valid += 1
        if change < 0:
            red_count += 1
    if valid < MARKET_GUARD_MIN_VALID:
        return {
            "active": False,
            "block_longs": False,
            "level": "normal",
            "valid_count": valid,
            "red_ratio_15m": 0.0,
            "avg_change_15m": 0.0,
            "btc_change_15m": 0.0,
            "reason": f"valid={valid} < {MARKET_GUARD_MIN_VALID}",
            "alt_weak_cautious": False,
        }
    red_ratio = round(red_count / valid, 4)
    avg_change = round(sum(changes) / len(changes), 4) if changes else 0.0
    if candles_map_15m and "BTC-USDT-SWAP" in candles_map_15m:
        btc_candles = candles_map_15m["BTC-USDT-SWAP"]
    else:
        btc_candles = get_candles("BTC-USDT-SWAP", MARKET_GUARD_TIMEFRAME, 5)
    btc_df = to_dataframe(btc_candles)
    btc_change = 0.0
    if btc_df is not None and not btc_df.empty:
        btc_change = get_last_candle_change_pct(btc_df)
    block = False
    reason = ""
    alt_mode_str = str(alt_snapshot.get("alt_mode", ""))
    alt_weak_cautious = False
    if red_ratio >= MARKET_GUARD_RED_RATIO_BLOCK and avg_change <= MARKET_GUARD_AVG_CHANGE_15M_BLOCK:
        block = True
        reason = f"red_ratio={red_ratio:.2f} & avg_change={avg_change:.2f}"
    elif btc_change <= MARKET_GUARD_BTC_CHANGE_15M_BLOCK and red_ratio >= 0.55:
        block = True
        reason = f"btc_change={btc_change:.2f} & red_ratio={red_ratio:.2f}"
    elif MARKET_GUARD_ALT_WEAK_BLOCK and "ضعيف" in alt_mode_str:
        if btc_zone_breakdown and red_ratio >= 0.55:
            block = True
            reason = f"alt_weak + BTC range breakdown & red_ratio={red_ratio:.2f}"
        else:
            # Alt weakness alone is no longer a BLOCK trigger.
            # If BTC is still inside the range (especially lower range without breakdown),
            # treat it as STRONG_LONG_ONLY / cautious instead of full panic block.
            alt_weak_cautious = True
            if btc_lower_range:
                reason = "alt_weak but BTC lower range rebound zone -> STRONG_LONG_ONLY"
            elif "صاعد" in btc_mode:
                reason = "alt_weak & btc_up -> STRONG_LONG_ONLY"
            else:
                reason = f"alt_weak & btc not up -> STRONG_LONG_ONLY (red_ratio={red_ratio:.2f})"
    return {
        "active": True,
        "block_longs": bool(block),
        "level": "danger" if block else "normal",
        "valid_count": valid,
        "red_ratio_15m": red_ratio,
        "avg_change_15m": avg_change,
        "btc_change_15m": btc_change,
        "reason": reason,
        "alt_weak_cautious": alt_weak_cautious,
        "btc_zone": btc_zone,
    }
 except Exception as e:
    logger.error(f"Market guard snapshot error: {e}")
    return {
        "active": False,
        "block_longs": False,
        "level": "normal",
        "valid_count": 0,
        "red_ratio_15m": 0.0,
        "avg_change_15m": 0.0,
        "btc_change_15m": 0.0,
        "reason": f"error: {e}",
        "alt_weak_cautious": False,
    }

# =====================
# MODE HELPER FUNCTIONS
# =====================
def normalize_market_mode(mode: str) -> str:
 allowed = {
    MODE_NORMAL_LONG,
    MODE_STRONG_LONG_ONLY,
    MODE_BLOCK_LONGS,
    MODE_RECOVERY_LONG,
 }

 # Normalize stale/empty Redis values safely.
 # Empty mode should silently fallback to NORMAL_LONG without warning spam.
 mode = str(mode or "").strip().upper()
 if not mode:
    return MODE_NORMAL_LONG

 if mode in allowed:
    return mode

 logger.warning(f"Invalid market mode '{mode}', fallback to {MODE_NORMAL_LONG}")
 return MODE_NORMAL_LONG

def is_market_no_longer_crashing(red_ratio, avg_change, btc_change, alt_mode) -> bool:
 try:
    red_ratio = float(red_ratio or 0.0)
    avg_change = float(avg_change or 0.0)
    btc_change = float(btc_change or 0.0)
    return (
        red_ratio < 0.62
        and avg_change > -0.75
        and btc_change > -0.45
    )
 except Exception:
    return False

def is_market_recovery_ready(red_ratio, avg_change, btc_change, alt_mode) -> bool:
 try:
    return (
        float(red_ratio or 0.0) < 0.58
        and float(avg_change or 0.0) > -0.55
        and float(btc_change or 0.0) > -0.32
        and alt_mode != "🔴 ضعيف"
    )
 except Exception:
    return False

def is_market_normal_ready(red_ratio, avg_change, btc_change, market_state) -> bool:
 try:
    red_ratio = float(red_ratio or 0.0)
    avg_change = float(avg_change or 0.0)
    btc_change = float(btc_change or 0.0)

    if market_state in ("bull_market", "alt_season"):
        return (
            red_ratio < 0.62
            and avg_change > -0.50
            and btc_change > -0.30
        )

    if market_state == "mixed":
        return (
            red_ratio < 0.56
            and avg_change > -0.40
            and btc_change > -0.25
        )

    return False
 except Exception:
    return False

def is_selective_market_normal_ready(
 red_ratio,
 avg_change,
 btc_change,
 btc_mode: str = "",
 alt_snapshot: dict | None = None,
 market_state: str = "",
) -> bool:
 """
 Allow STRONG_LONG_ONLY to return to NORMAL when the market is rotational,
 not genuinely weak: BTC is strong/stable, Market Guard is normal, and
 enough alts show constructive breadth. This does not affect BLOCK logic.
 """
 try:
    alt_snapshot = alt_snapshot or {}
    red_ratio = float(red_ratio or 0.0)
    avg_change = float(avg_change or 0.0)
    btc_change = float(btc_change or 0.0)
    btc_text = str(btc_mode or "")
    alt_strength = float(alt_snapshot.get("alt_strength_score", 0.0) or 0.0)
    positive_24h_ratio = float(alt_snapshot.get("positive_24h_ratio", 0.0) or 0.0)
    above_ma_ratio = float(alt_snapshot.get("above_ma_ratio", 0.0) or 0.0)

    btc_supportive = ("صاعد" in btc_text) or btc_change >= 0.0
    market_guard_ok = red_ratio < 0.58 and avg_change > -0.35 and btc_change > -0.25
    breadth_constructive = (
        alt_strength >= 0.38
        or positive_24h_ratio >= 0.42
        or above_ma_ratio >= 0.38
        or market_state in ("bull_market", "alt_season")
    )
    return btc_supportive and market_guard_ok and breadth_constructive
 except Exception:
    return False

def is_supportive_bull_flow(red_ratio, avg_change, btc_change, btc_mode: str = "", alt_snapshot: dict | None = None, market_state: str = "") -> bool:
 try:
    alt_snapshot = alt_snapshot or {}
    red_ratio = float(red_ratio or 0.0)
    avg_change = float(avg_change or 0.0)
    btc_change = float(btc_change or 0.0)
    btc_text = str(btc_mode or "")
    alt_mode_text = str(alt_snapshot.get("alt_mode", "") or "")
    btc_supportive = ("صاعد" in btc_text) or btc_change >= 0.0
    alt_supportive = ("قوي" in alt_mode_text) or market_state in ("bull_market", "alt_season")
    danger_count = 0
    if red_ratio >= 0.68:
        danger_count += 1
    if avg_change <= -0.30:
        danger_count += 1
    if btc_change <= -0.25:
        danger_count += 1
    if "🔴" in alt_mode_text or "ضعيف جدًا" in alt_mode_text:
        danger_count += 1
    return bool(btc_supportive and alt_supportive and danger_count < 2)
 except Exception:
    return False

# =========================
# DETERMINE LONG MARKET MODE
# =========================
def determine_long_market_mode(
 market_guard: dict,
 market_state: str,
 btc_mode: str,
 alt_snapshot: dict,
 current_mode: str,
 allow_state_writes: bool = True,
) -> dict:
 now_ts = int(time.time())
 current_mode = normalize_market_mode(current_mode)
 red_ratio = float(market_guard.get("red_ratio_15m", 0.0) or 0.0)
 avg_change = float(market_guard.get("avg_change_15m", 0.0) or 0.0)
 btc_change = float(market_guard.get("btc_change_15m", 0.0) or 0.0)
 alt_mode = alt_snapshot.get("alt_mode", "")
 alt_weak_cautious = market_guard.get("alt_weak_cautious", False)
 btc_zone = market_guard.get("btc_zone") or {}
 btc_zone_name = str(btc_zone.get("zone", "") or "")
 btc_zone_breakdown = bool(btc_zone.get("breakdown", False))
 btc_lower_range = btc_zone_name == "lower_range" and not btc_zone_breakdown
 last_transition_ts = 0
 last_recovery_check_ts = 0
 normal_candidate_since = 0
 safe_since = 0
 if r:
    try:
        last_transition_ts = int(r.get(MARKET_MODE_LAST_TRANSITION_KEY) or 0)
        last_recovery_check_ts = int(r.get(MARKET_MODE_LAST_RECOVERY_CHECK_KEY) or 0)
        normal_candidate_since = int(r.get(MARKET_MODE_NORMAL_CANDIDATE_KEY) or 0)
        safe_since = int(r.get(MARKET_MODE_LAST_SAFE_SEEN_KEY) or 0)
    except Exception:
        pass
 time_since_last_transition = now_ts - last_transition_ts if last_transition_ts > 0 else 999999
 crash_triggered = bool(market_guard.get("block_longs"))
 crash_reason = market_guard.get("reason", "")
 if not crash_triggered:
    if red_ratio >= 0.68 and avg_change <= -1.20:
        crash_triggered = True
        crash_reason = f"red_ratio={red_ratio:.2f} & avg_change={avg_change:.2f}"
    elif btc_change <= -0.70 and red_ratio >= 0.55:
        crash_triggered = True
        crash_reason = f"btc_change={btc_change:.2f} & red_ratio={red_ratio:.2f}"
    elif alt_weak_cautious:
        pass  # will be handled below as STRONG_LONG_ONLY
    elif alt_mode == "🔴 ضعيف" and red_ratio >= 0.60:
        if btc_zone_breakdown and red_ratio >= 0.60:
            crash_triggered = True
            crash_reason = f"alt_weak + BTC breakdown & red_ratio={red_ratio:.2f}"
        else:
            alt_weak_cautious = True
            crash_reason = f"alt_weak -> STRONG_LONG_ONLY (red_ratio={red_ratio:.2f})"
 if crash_triggered:
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
            r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
        except Exception:
            pass
    return {"mode": MODE_BLOCK_LONGS, "reason": f"كراش: {crash_reason}"}
 
 if alt_weak_cautious:
    selective_normal_ready = is_selective_market_normal_ready(
        red_ratio=red_ratio,
        avg_change=avg_change,
        btc_change=btc_change,
        btc_mode=btc_mode,
        alt_snapshot=alt_snapshot,
        market_state=market_state,
    )
    if selective_normal_ready and time_since_last_transition >= MODE_TRANSITION_MIN_INTERVAL:
        if allow_state_writes and r:
            try:
                r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
                r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
            except Exception:
                pass
        return {"mode": MODE_NORMAL_LONG, "reason": "BTC قوي والسوق انتقائي قابل للتداول → NORMAL_LONG"}
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
            r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
        except Exception:
            pass
    return {"mode": MODE_STRONG_LONG_ONLY, "reason": "alt ضعيف + BTC صاعد → وضع حذر (إشارات قوية فقط)"}

 if current_mode == MODE_BLOCK_LONGS:
    if now_ts - last_recovery_check_ts >= RECOVERY_CHECK_INTERVAL:
        if allow_state_writes and r:
            try:
                r.set(MARKET_MODE_LAST_RECOVERY_CHECK_KEY, str(now_ts))
            except Exception:
                pass
        if is_market_recovery_ready(red_ratio, avg_change, btc_change, alt_mode):
            if allow_state_writes and r:
                try:
                    r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
                    r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
                except Exception:
                    pass
            return {"mode": MODE_RECOVERY_LONG, "reason": "انتهى البلوك وشروط الريكافري اتحققت"}
    block_exit_ready = is_market_no_longer_crashing(red_ratio, avg_change, btc_change, alt_mode)
    if btc_lower_range and not btc_zone_breakdown and red_ratio < 0.68 and avg_change > -1.05 and btc_change > -0.65:
        block_exit_ready = True
    if block_exit_ready:
        if safe_since == 0:
            if allow_state_writes and r:
                try:
                    r.set(MARKET_MODE_LAST_SAFE_SEEN_KEY, str(now_ts))
                except Exception:
                    pass
            return {"mode": MODE_BLOCK_LONGS, "reason": "الكراش هدأ، بدأ عداد الخروج الآمن من BLOCK"}
        safe_duration = now_ts - safe_since
        if safe_duration >= BLOCK_EXIT_CONFIRM_DURATION:
            if allow_state_writes and r:
                try:
                    r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
                    r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
                except Exception:
                    pass
            return {"mode": MODE_STRONG_LONG_ONLY, "reason": f"السوق لم يعد كراشًا لمدة {safe_duration}s، خروج احتياطي إلى STRONG_LONG_ONLY"}
        return {"mode": MODE_BLOCK_LONGS, "reason": f"السوق أهدأ لكن ننتظر تأكيد الخروج الآمن ({safe_duration}s/{BLOCK_EXIT_CONFIRM_DURATION}s)"}
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
        except Exception:
            pass
    return {"mode": MODE_BLOCK_LONGS, "reason": "ما زلنا داخل BLOCK"}
 if current_mode == MODE_STRONG_LONG_ONLY:
    if (
        is_market_normal_ready(red_ratio, avg_change, btc_change, market_state)
        or is_selective_market_normal_ready(
            red_ratio=red_ratio,
            avg_change=avg_change,
            btc_change=btc_change,
            btc_mode=btc_mode,
            alt_snapshot=alt_snapshot,
            market_state=market_state,
        )
    ):
        if normal_candidate_since == 0:
            if allow_state_writes and r:
                try:
                    r.set(MARKET_MODE_NORMAL_CANDIDATE_KEY, str(now_ts))
                except Exception:
                    pass
            return {"mode": MODE_STRONG_LONG_ONLY, "reason": "بدأ مرشح الرجوع للوضع الطبيعي"}
        if now_ts - normal_candidate_since >= STRONG_TO_NORMAL_CONFIRM_DURATION:
            if allow_state_writes and r:
                try:
                    r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
                except Exception:
                    pass
            if time_since_last_transition < STRONG_TO_NORMAL_CONFIRM_DURATION:
                return {"mode": MODE_STRONG_LONG_ONLY, "reason": "تأكيد طبيعي لكن أقل مدة انتقال لم تمر"}
            return {"mode": MODE_NORMAL_LONG, "reason": "استقرار 5 دقائق، رجوع للوضع الطبيعي"}
        return {"mode": MODE_STRONG_LONG_ONLY, "reason": f"جاري التأكد من الاستقرار... ({now_ts - normal_candidate_since}s/{STRONG_TO_NORMAL_CONFIRM_DURATION}s)"}
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
        except Exception:
            pass
    weak_market = (
        market_state in ("mixed", "btc_leading", "risk_off")
        or (0.52 <= red_ratio < 0.68)
        or (avg_change < -0.30)
        or (btc_mode in ("🔴 هابط", "🟡 محايد") and alt_mode != "🟢 قوي")
    )
    if weak_market and is_supportive_bull_flow(red_ratio, avg_change, btc_change, btc_mode, alt_snapshot, market_state):
        weak_market = False
    if weak_market:
        if allow_state_writes and r:
            try:
                r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
            except Exception:
                pass
        return {"mode": MODE_STRONG_LONG_ONLY, "reason": "السوق ضعيف/مختلط لكن ليس كراش"}
    return {"mode": MODE_STRONG_LONG_ONLY, "reason": "الحالة مستقرة لكن لم تصل لشروط العودة الكاملة"}
 if current_mode == MODE_RECOVERY_LONG:
    if is_market_normal_ready(red_ratio, avg_change, btc_change, market_state):
        if normal_candidate_since == 0:
            if allow_state_writes and r:
                try:
                    r.set(MARKET_MODE_NORMAL_CANDIDATE_KEY, str(now_ts))
                except Exception:
                    pass
            return {"mode": MODE_RECOVERY_LONG, "reason": "بدأ مرشح الرجوع للوضع الطبيعي"}
        if now_ts - normal_candidate_since >= NORMAL_CANDIDATE_DURATION:
            if allow_state_writes and r:
                try:
                    r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
                except Exception:
                    pass
            return {"mode": MODE_NORMAL_LONG, "reason": "الريكافري استقر بما يكفي، رجوع للوضع"}
        return {"mode": MODE_RECOVERY_LONG, "reason": f"...جاري التأكد من الاستقرار ({now_ts - normal_candidate_since}s/{NORMAL_CANDIDATE_DURATION}s)"}
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
        except Exception:
            pass
    return {"mode": MODE_RECOVERY_LONG, "reason": "ما زلنا في وضع الريكافري"}

 if (
    is_market_normal_ready(red_ratio, avg_change, btc_change, market_state)
    or is_selective_market_normal_ready(
        red_ratio=red_ratio,
        avg_change=avg_change,
        btc_change=btc_change,
        btc_mode=btc_mode,
        alt_snapshot=alt_snapshot,
        market_state=market_state,
    )
 ):
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
        except Exception:
            pass
    return {"mode": MODE_NORMAL_LONG, "reason": "السوق طبيعي/انتقائي قابل للتداول"}
 weak_market = (
    market_state in ("mixed", "btc_leading", "risk_off")
    or (0.52 <= red_ratio < 0.68)
    or (avg_change < -0.30)
    or (btc_mode in ("🔴 هابط", "🟡 محايد") and alt_mode != "🟢 قوي")
 )
 if weak_market and is_supportive_bull_flow(red_ratio, avg_change, btc_change, btc_mode, alt_snapshot, market_state):
    weak_market = False
 if weak_market and time_since_last_transition >= MODE_TRANSITION_MIN_INTERVAL:
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
        except Exception:
            pass
    return {"mode": MODE_STRONG_LONG_ONLY, "reason": "السوق ضعيف/مختلط لكن ليس كراش"}
 return {"mode": MODE_NORMAL_LONG, "reason": "لا يوجد تغيير في المود"}

def _market_mode_label(mode: str) -> str:
    mode = normalize_market_mode(mode)
    return {
        MODE_NORMAL_LONG: "🟢 NORMAL LONG",
        MODE_STRONG_LONG_ONLY: "🟡 STRONG LONG ONLY",
        MODE_BLOCK_LONGS: "🔴 BLOCK LONGS",
        MODE_RECOVERY_LONG: "🟠 RECOVERY LONG",
    }.get(mode, html.escape(str(mode)))

def get_market_mode_action_text(mode: str) -> str:
    mode = normalize_market_mode(mode)
    if mode == MODE_NORMAL_LONG:
        return "• الإشارات العادية: مسموحة حسب الفلاتر\n• التنفيذ التجريبي: Whitelist + Quality Filters"
    if mode == MODE_STRONG_LONG_ONLY:
        return "• الإشارات العادية: أقوى الفرص فقط\n🎯 التنفيذ التجريبي: Whitelist أو Elite عالي الجودة فقط"
    if mode == MODE_BLOCK_LONGS:
        return "• الإشارات العادية: ممنوعة أو مشددة جدًا\n• التنفيذ التجريبي: استثناءات قوية جدًا فقط"
    if mode == MODE_RECOVERY_LONG:
        return "• الإشارات العادية: فرص Recovery محدودة\n• التنفيذ التجريبي: Recovery confirmed + Whitelist"
    return "• مراقبة السوق قبل الدخول"


def get_market_mode_execution_policy_short(mode: str) -> str:
    """Compact one-line policy for periodic reminders."""
    mode = normalize_market_mode(mode)
    if mode == MODE_NORMAL_LONG:
        return "Whitelist + Quality Filters"
    if mode == MODE_STRONG_LONG_ONLY:
        return "Strong Momentum + Whitelist"
    if mode == MODE_BLOCK_LONGS:
        return "Protected Only + Strict Risk"
    if mode == MODE_RECOVERY_LONG:
        return "Recovery Setups + Whitelist"
    return "Watch Only"


def _format_mode_duration_text(seconds: int) -> str:
    try:
        seconds = max(0, int(seconds or 0))
        minutes = seconds // 60
        if minutes < 1:
            return "0m"
        if minutes < 60:
            return f"{minutes}m"
        hours, mins = divmod(minutes, 60)
        if hours < 24:
            return f"{hours}h {mins}m"
        days, hours = divmod(hours, 24)
        return f"{days}d {hours}h"
    except Exception:
        return "0m"


def get_market_mode_duration_text(current_mode: str = "") -> str:
    try:
        now_ts = int(time.time())
        transition_ts = 0
        if r:
            transition_ts = int(float(r.get(MARKET_MODE_LAST_TRANSITION_KEY) or 0))
        if transition_ts <= 0:
            return "0m"
        return _format_mode_duration_text(now_ts - transition_ts)
    except Exception:
        return "0m"


def _btc_short_label(btc_mode: str) -> str:
    text = str(btc_mode or "")
    if "هابط" in text or "Weak" in text:
        return "🔴 BTC Weak"
    if "صاعد" in text or "Strong" in text:
        return "🟢 BTC Strong"
    if "محايد" in text or "Neutral" in text:
        return "🟡 BTC Neutral"
    return html.escape(text or "BTC N/A")


def _market_short_label(market_state_label: str = "", alt_mode: str = "", market_bias_label: str = "") -> str:
    text = " | ".join(str(x or "") for x in (market_state_label, alt_mode, market_bias_label))
    low = text.lower()
    if "risk" in low or "weak" in low or "ضعيف" in text:
        return "🔴 Weak Market"
    if "bull" in low or "strong" in low or "قوي" in text:
        return "🟢 Strong Market"
    if "mixed" in low or "متماسك" in text or "مختلط" in text or "محايد" in text:
        return "🟡 Mixed Market"
    return html.escape(str(market_state_label or alt_mode or "Mixed Market"))


def get_weak_drift_display_status(
    current_mode: str,
    btc_mode: str = "",
    market_state_label: str = "",
    alt_mode: str = "",
    market_bias_label: str = "",
) -> dict:
    """Display-only Weak Drift status for mood/reminder messages.

    The real execution gate remains get_weak_trend_drift_status(candidate).
    This helper does not change trading decisions; it only explains whether
    the current market context is likely to restrict weak execution candidates.
    """
    try:
        mode = normalize_market_mode(current_mode)
        if mode == MODE_BLOCK_LONGS:
            return {
                "active": True,
                "label": "🔴 Weak Drift: STRICT",
                "note": "التنفيذ مقيد جدًا لأن BLOCK LONGS مفعل.",
            }
        text = " | ".join(str(x or "") for x in (btc_mode, market_state_label, alt_mode, market_bias_label))
        low = text.lower()
        active = bool(
            "هابط" in text
            or "weak" in low
            or "mixed" in low
            or "مختلط" in text
            or "متماسك" in text
            or "محايد" in text
            or "ضعيف" in text
            or mode in (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG)
        )
        if active:
            return {
                "active": True,
                "label": "🔴 Weak Drift: ON",
                "note": "التنفيذ الضعيف مقيد مؤقتًا بسبب ضعف/تذبذب الزخم.",
            }
        return {
            "active": False,
            "label": "🟢 Weak Drift: OFF",
            "note": "التنفيذ يعمل طبيعيًا حسب Whitelist + Quality Filters.",
        }
    except Exception:
        return {"active": False, "label": "🟢 Weak Drift: OFF", "note": "التنفيذ يعمل طبيعيًا."}



def _safe_json_loads_for_reminder(raw):
    try:
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _market_reminder_collect_stats(window_seconds: int = 1200) -> dict:
    """Collect lightweight display-only stats for Market Reminder.

    This function is intentionally defensive: if Redis/key formats differ, it
    returns safe zeros instead of affecting scanning, alerts, or execution.
    """
    now_ts = int(time.time())
    since_ts = now_ts - int(window_seconds or 1200)
    stats = {
        "signals": 0,
        "exec_candidates": 0,
        "rejections": 0,
        "top_reject": "N/A",
        "open_winners": 0,
        "open_tp1_protected": 0,
        "open_danger": 0,
        "protected_on_block": 0,
    }
    if not r:
        return stats

    try:
        try:
            trades = load_all_trades_for_report(
                r, market_type="futures", side="long", since_ts=None, include_open=True
            )
        except Exception:
            trades = _load_long_trades_from_redis(limit=1200)

        recent_trades = []
        open_trades = []
        for t in trades or []:
            if not isinstance(t, dict):
                continue
            created = _trade_created_ts_for_exec(t)
            if created >= since_ts:
                recent_trades.append(t)
            if _is_execution_trade_open(t) and is_execution_candidate_trade(t):
                open_trades.append(t)

        stats["signals"] = len(recent_trades)
        stats["exec_candidates"] = sum(1 for t in recent_trades if is_execution_candidate_trade(t))

        for t in open_trades:
            pnl = _execution_floating_pnl_pct(t)
            if pnl is not None and pnl > 0:
                stats["open_winners"] += 1
            if bool(t.get("tp1_hit")) or bool(t.get("sl_moved_to_entry")) or bool(t.get("protected_breakeven")):
                stats["open_tp1_protected"] += 1
            if bool(t.get("protected_on_block")) or str(t.get("protected_reason", "")) == "market_mode_block_longs":
                stats["protected_on_block"] += 1
            # Danger means open and currently losing before any protection/TP1.
            if pnl is not None and pnl < 0 and not bool(t.get("tp1_hit")) and not bool(t.get("sl_moved_to_entry")):
                stats["open_danger"] += 1
    except Exception as exc:
        logger.debug(f"market reminder trade stats skipped: {exc}")

    try:
        from collections import Counter
        reason_counter = Counter()
        reject_count = 0
        patterns = (
            "rejected_candidate:*",
            "rejection:long:*",
            "rejected:long:*",
            "rejected:futures:long:*",
            "rejection:futures:long:*",
        )
        seen_keys = set()
        for pattern in patterns:
            try:
                for key in r.scan_iter(pattern):
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    data = _safe_json_loads_for_reminder(r.get(key))
                    if not data:
                        continue
                    ts = 0
                    for ts_key in ("created_ts", "ts", "timestamp", "candle_time"):
                        try:
                            ts = int(float(data.get(ts_key) or 0))
                            if ts > 0:
                                break
                        except Exception:
                            pass
                    if ts and ts < since_ts:
                        continue
                    reject_count += 1
                    reason = str(data.get("reason") or data.get("reject_reason") or data.get("category") or "unknown")
                    if reason:
                        reason_counter[reason] += 1
            except Exception:
                continue
        stats["rejections"] = reject_count
        if reason_counter:
            stats["top_reject"] = reason_counter.most_common(1)[0][0]
    except Exception as exc:
        logger.debug(f"market reminder rejection stats skipped: {exc}")

    return stats


def _market_reminder_execution_line(mode: str, drift_label: str) -> str:
    mode = normalize_market_mode(mode)
    drift_clean = str(drift_label or "Weak Drift: OFF").replace("🔴 ", "").replace("🟢 ", "")
    if mode == MODE_BLOCK_LONGS:
        return f"PAUSED | {drift_clean}"
    if mode == MODE_STRONG_LONG_ONLY:
        return f"Whitelist/Elite ACTIVE | {drift_clean}"
    if mode == MODE_RECOVERY_LONG:
        return f"Whitelist ACTIVE | Recovery Filter ON"
    return f"Whitelist ACTIVE | {drift_clean}"


def _estimate_strong_coins_for_reminder(alt_snapshot: dict, ranked_pairs_count: int) -> int:
    """Small display-only estimate for reminder heartbeat.

    It is intentionally conservative and defensive; it must never affect trading.
    """
    try:
        ranked_pairs_count = max(0, int(ranked_pairs_count or 0))
        if ranked_pairs_count <= 0:
            return 0
        score = float((alt_snapshot or {}).get("alt_strength_score") or 0.0)
        # Keep the displayed number conservative: weak/neutral markets stay low,
        # strong broad markets gradually show more strong coins.
        estimated = int(round(max(0.0, score - 0.45) * 50.0))
        return max(0, min(ranked_pairs_count, estimated))
    except Exception:
        return 0


def _market_reminder_mode_icon(mode: str) -> str:
    mode = normalize_market_mode(mode)
    if mode == MODE_NORMAL_LONG:
        return "🟢"
    if mode == MODE_STRONG_LONG_ONLY:
        return "🟡"
    if mode == MODE_RECOVERY_LONG:
        return "🔵"
    if mode == MODE_BLOCK_LONGS:
        return "🔴"
    return "🧭"


def build_compact_market_mode_reminder(
    reminder_count: int,
    current_mode: str,
    btc_mode: str = "",
    market_state_label: str = "",
    alt_mode: str = "",
    market_bias_label: str = "",
    strong_coins_count: int = 0,
    ranked_pairs_count: int = 0,
    block_protection_active: bool = False,
    protection_summary: dict = None,
    market_guard: dict = None,
) -> str:
    """Compact human Market Reminder template for all modes.

    v202: first reminder after 15m, then every 30m; text is actionable and
    avoids debug-style reason strings.
    """
    normalized_mode = normalize_market_mode(current_mode)
    mode_icon = _market_reminder_mode_icon(normalized_mode)
    duration_text = get_market_mode_duration_text(normalized_mode)
    drift = get_weak_drift_display_status(
        normalized_mode, btc_mode, market_state_label, alt_mode, market_bias_label
    )
    drift_label = str(drift.get("label", "🟢 Weak Drift: OFF"))
    stats = _market_reminder_collect_stats(window_seconds=1800)
    market_guard = market_guard or {}

    try:
        red_ratio_pct = float(market_guard.get("red_ratio_15m", 0.0) or 0.0) * 100.0
        avg15m = float(market_guard.get("avg_change_15m", 0.0) or 0.0)
        btc15m = float(market_guard.get("btc_change_15m", 0.0) or 0.0)
    except Exception:
        red_ratio_pct, avg15m, btc15m = 0.0, 0.0, 0.0

    try:
        strong_coins_count = max(0, int(strong_coins_count or 0))
    except Exception:
        strong_coins_count = 0
    try:
        ranked_pairs_count = max(0, int(ranked_pairs_count or 0))
    except Exception:
        ranked_pairs_count = 0

    if normalized_mode == MODE_BLOCK_LONGS:
        status_text = "📉 السوق مازال تحت ضغط جماعي."
        action_lines = [
            "• تجنب المطاردة",
            "• السماح فقط بالإشارات القوية جدًا",
            "• متابعة حماية الصفقات المفتوحة",
        ]
    elif normalized_mode == MODE_STRONG_LONG_ONLY:
        status_text = "⚠️ السوق انتقائي حاليًا؛ نركز على الجودة فقط."
        action_lines = [
            "• التركيز على reclaim / retest / wave_3",
            "• الإشارة العادية ممكنة لو الجودة قوية",
            "• التنفيذ التجريبي يخضع للـ Whitelist/Elite",
        ]
    elif normalized_mode == MODE_RECOVERY_LONG:
        status_text = "🟠 السوق يحاول التعافي بعد ضغط."
        action_lines = [
            "• فرص Recovery محدودة",
            "• تأكيد التعافي أهم من الكمية",
            "• تجنب أول شمعة ارتداد ضعيفة",
        ]
    else:
        status_text = "🟢 السوق يسمح بالبحث الطبيعي عن فرص لونج."
        action_lines = [
            "• الإشارات العادية مسموحة حسب الفلاتر",
            "• التنفيذ التجريبي: Whitelist + Quality Filters",
            "• Weak Drift يمنع التنفيذ الضعيف فقط",
        ]

    lines = [
        f"{mode_icon} <b>Market Reminder #{int(reminder_count)}</b>",
        f"⏱ {html.escape(duration_text)} in {html.escape(normalized_mode)}",
        "",
        status_text,
        "",
        f"📊 <b>السوق اللحظي:</b> Red {red_ratio_pct:.0f}% | Avg {avg15m:+.2f}% | BTC {btc15m:+.2f}%",
        f"🌍 <b>السياق:</b> {html.escape(_btc_short_label(btc_mode))} | {html.escape(_market_short_label(market_state_label, alt_mode, market_bias_label))}",
        f"⚡ <b>Strong Setups:</b> {strong_coins_count} / {ranked_pairs_count}",
        "",
        "🎯 <b>التصرف:</b>",
    ]
    lines.extend([html.escape(x) for x in action_lines])
    lines.extend([
        "",
        f"📡 Signals {int(stats.get('signals', 0))} | 🚀 Exec {int(stats.get('exec_candidates', 0))} | ❌ Reject {int(stats.get('rejections', 0))}",
        f"❌ <b>Top Reject:</b> {html.escape(str(stats.get('top_reject') or 'N/A'))}",
        "",
        "💼 <b>Open Candidates:</b>",
        f"🟢 {int(stats.get('open_winners', 0))} Winners | 🟡 {int(stats.get('open_tp1_protected', 0))} TP1 Protected | 🔴 {int(stats.get('open_danger', 0))} Danger",
        "",
        f"🧠 <b>Execution:</b> {html.escape(_market_reminder_execution_line(normalized_mode, drift_label))}",
    ])

    if normalized_mode == MODE_BLOCK_LONGS and (block_protection_active or protection_summary):
        summary = protection_summary or {}
        protected = int(summary.get("protected_winners") or stats.get("protected_on_block") or 0)
        lines.extend([
            "",
            "🛡 <b>Protection Check:</b>",
            f"🔒 Protected winners: {protected}",
            "⚠️ Tracking فقط — لا يوجد أمر SL مباشر إلى OKX من هذا الريمايندر.",
        ])

    return "\n".join(lines)

def maybe_send_market_mode_reminder(
    current_mode: str,
    btc_mode: str = "",
    market_info: dict = None,
    alt_snapshot: dict = None,
    ranked_pairs: list = None,
    market_guard: dict = None,
) -> bool:
    """Send the periodic Market Reminder when due.

    Kept separate so cooldown exits can check reminders before `continue`.
    Returns True only when a reminder is sent.
    """
    if not r:
        return False
    now_ts_local = int(time.time())
    market_info = market_info or {}
    alt_snapshot = alt_snapshot or {}
    ranked_pairs = ranked_pairs or []
    market_guard = market_guard or {}
    try:
        last_reminder = int(r.get(MARKET_MODE_LAST_REMINDER_KEY) or 0)
        reminder_mode = r.get(MARKET_MODE_REMINDER_MODE_KEY) or current_mode
        reminder_count = int(r.get(MARKET_MODE_REMINDER_COUNT_KEY) or 0)
        if reminder_mode != current_mode:
            reminder_count = 0
            last_reminder = 0

        # v202 cadence: first reminder after 15m, then every 30m.
        if last_reminder <= 0:
            r.set(MARKET_MODE_LAST_REMINDER_KEY, str(now_ts_local))
            r.set(MARKET_MODE_REMINDER_MODE_KEY, current_mode)
            return False
        required_interval = MARKET_REMINDER_FIRST_INTERVAL if reminder_count <= 0 else MARKET_REMINDER_INTERVAL
        if now_ts_local - last_reminder < required_interval:
            return False

        reminder_count += 1

        protection_summary = None
        block_protection_active = False
        if current_mode == MODE_BLOCK_LONGS:
            try:
                already_applied = bool(r.get(MARKET_MODE_BLOCK_PROTECTION_APPLIED_KEY))
                block_protection_active = already_applied
                if reminder_count == 1 and not already_applied:
                    protection_summary = update_open_trades(
                        r,
                        market_type="futures",
                        side="long",
                        timeframe=TIMEFRAME,
                        market_mode=current_mode,
                        protect_breakeven_on_block=True,
                        breakeven_min_profit_pct=PROTECT_ON_BLOCK_MIN_PROFIT_PCT,
                        breakeven_buffer_pct=PROTECT_ON_BLOCK_BUFFER_PCT,
                        block_pressure_level=market_guard.get("level", ""),
                        block_red_ratio=market_guard.get("red_ratio_15m", 0),
                        block_avg_change=market_guard.get("avg_change_15m", 0),
                        block_btc_change=market_guard.get("btc_change_15m", 0),
                        reason="market_mode=BLOCK_LONGS:first_reminder",
                    )
                    r.set(MARKET_MODE_BLOCK_PROTECTION_APPLIED_KEY, str(now_ts_local))
                    block_protection_active = True
            except Exception as _protect_exc:
                logger.warning(f"BLOCK first-reminder protection error: {_protect_exc}")

        strong_coins_count = _estimate_strong_coins_for_reminder(alt_snapshot, len(ranked_pairs))
        reminder_msg = build_compact_market_mode_reminder(
            reminder_count=reminder_count,
            current_mode=current_mode,
            btc_mode=btc_mode,
            market_state_label=market_info.get("market_state_label", ""),
            alt_mode=alt_snapshot.get("alt_mode", ""),
            strong_coins_count=strong_coins_count,
            ranked_pairs_count=len(ranked_pairs),
            block_protection_active=block_protection_active,
            protection_summary=protection_summary,
            market_guard=market_guard,
        )
        send_telegram_message(reminder_msg)

        r.set(MARKET_MODE_LAST_REMINDER_KEY, str(now_ts_local))
        r.set(MARKET_MODE_REMINDER_COUNT_KEY, str(reminder_count))
        r.set(MARKET_MODE_REMINDER_MODE_KEY, current_mode)
        return True
    except Exception as e:
        logger.warning(f"Market mode reminder error: {e}")
        return False

def format_block_protection_summary_message(summary: dict) -> str:
    """Format one compact Telegram message after first BLOCK_LONGS reminder protection."""
    summary = summary or {}
    protected = int(summary.get("protected_winners") or 0)
    compressed = int(summary.get("risk_compressed") or 0)
    monitoring = int(summary.get("monitoring_only") or 0)
    ignored = int(summary.get("ignored_tracking_only") or 0)
    already = int(summary.get("already_protected") or 0)
    close_to_sl = int(summary.get("skipped_close_to_sl") or 0)
    execution_seen = int(summary.get("execution_seen") or 0)

    return (
        "🛡️ <b>BLOCK Protection Applied</b>\n\n"
        "تم تنفيذ تقييم حماية صفقات التنفيذ المفتوحة بسبب استمرار BLOCK_LONGS حتى أول Reminder.\n\n"
        f"✅ <b>Protected winners:</b> {protected}\n"
        f"🟡 <b>Risk compressed:</b> {compressed}\n"
        f"⏸ <b>Monitoring only:</b> {monitoring}\n"
        f"🔒 <b>Already protected:</b> {already}\n"
        f"📍 <b>Close to SL skipped:</b> {close_to_sl}\n"
        f"🚫 <b>Ignored tracking-only:</b> {ignored}\n"
        f"📊 <b>Execution trades checked:</b> {execution_seen}\n\n"
        "⚠️ <b>Tracking فقط</b> — لم يتم إرسال أمر تعديل SL مباشر إلى OKX حاليًا."
    )

def format_market_mode_reason_for_message(reason: str = "", metrics: dict = None) -> str:
    """Turn internal mode reasons into human Telegram text."""
    metrics = metrics or {}
    reason_text = str(reason or "").strip()

    def pct_from_ratio(value):
        try:
            return f"{float(value or 0) * 100:.0f}%"
        except Exception:
            return "N/A"

    red_ratio = metrics.get("red_ratio_15m")
    avg15m = metrics.get("avg_change_15m")
    btc15m = metrics.get("btc_change_15m")
    guard = str(metrics.get("market_guard_level") or metrics.get("guard") or "normal")

    if reason_text in ("fast_intraday_block", "fast_intraday_strong", "fast_intraday_monitor") or "fast_intraday" in reason_text:
        if reason_text == "fast_intraday_block":
            title = "ضغط جماعي سريع مع تأكيد خطر لحظي"
            decision = "لذلك تم تفعيل وضع الحماية BLOCK_LONGS."
        elif reason_text == "fast_intraday_strong":
            title = "ضغط جماعي سريع بدون انهيار مؤكد"
            decision = "لذلك تم الانتقال إلى STRONG_LONG_ONLY بدل BLOCK_LONGS."
        else:
            title = "ضغط لحظي تحت المراقبة"
            decision = "تم تحديث المود حسب قراءة السوق السريعة."
        return (
            f"{title}\n"
            f"• Red Ratio: {pct_from_ratio(red_ratio)}\n"
            f"• Avg 15m: {fmt_pct(avg15m)}\n"
            f"• BTC 15m: {fmt_pct(btc15m)}\n"
            f"• Guard: {html.escape(guard)}\n\n"
            f"{decision}"
        )

    cleaned = reason_text
    # Remove confusing stale fragments from previous decisions.
    cleaned = cleaned.replace("لا يوجد تغيير في المود |", "").replace("لا يوجد تغيير في المود", "").strip(" |")
    if not cleaned:
        return "تحديث حالة السوق بناءً على قراءة المود الحالية."
    return html.escape(cleaned)

def format_mode_transition_message(old_mode: str, new_mode: str, reason: str = "", reason_metrics: dict = None, human_reason: str = "") -> str:
    old_mode = normalize_market_mode(old_mode)
    new_mode = normalize_market_mode(new_mode)
    mode_ar = _market_mode_label(new_mode)
    mode_icon = str(mode_ar).split(" ", 1)[0] if mode_ar else "🧭"
    transition = f"{_market_mode_label(old_mode)} → {mode_ar}"
    mode_desc = get_market_mode_arabic_description(new_mode)
    action = get_market_mode_action_text(new_mode)
    allowed_lines = get_market_mode_allowed_lines(new_mode)
    reason_display = human_reason or format_market_mode_reason_for_message(reason, reason_metrics or {})

    lines = [
        f"{mode_icon} <b>Market Mood - {new_mode}</b>",
        "",
        "🔁 <b>تغيير المود</b>",
        "",
        f"⚙️ <b>المود الحالي:</b> {mode_ar}",
        f"📋 <b>الوصف:</b> {html.escape(mode_desc)}",
        "",
        f"🔄 <b>الانتقال:</b> {transition}",
    ]
    if reason_display:
        lines.extend([
            "",
            "🧠 <b>السبب:</b>",
            reason_display,
        ])
    lines.extend([
        "",
        "🎯 <b>التصرف:</b>",
        html.escape(action),
        "",
        "✅ <b>المسموح:</b>",
    ])
    lines.extend([html.escape(x) for x in allowed_lines])
    if new_mode == MODE_BLOCK_LONGS:
        lines.extend([
            "",
            "🛡️ <b>حماية BLOCK:</b>",
            "سيتم تقييم صفقات التنفيذ المفتوحة عند أول Market Reminder داخل BLOCK_LONGS.",
            "⚠️ الحماية الحالية Tracking فقط وليست أمر SL مباشر على OKX.",
        ])
    lines.extend([
        "",
        "📌 <b>ملاحظة:</b> الإشارة العادية منفصلة عن التنفيذ التجريبي؛ التنفيذ يخضع لقواعد الجودة والزخم.",
    ])
    if new_mode in (MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG):
        lines.append("🧩 <b>Weak Drift:</b> يمنع التنفيذ الضعيف فقط ولا يمنع الإشارة العادية.")
    return "\n".join(lines)

def handle_market_mode_transition(mode_result: dict) -> str:
 new_mode = mode_result.get("mode", MODE_NORMAL_LONG)
 if not r:
    return new_mode
 try:
    last_mode = r.get(MARKET_MODE_LAST_KEY) or MODE_NORMAL_LONG
    if new_mode != last_mode:
        msg = format_mode_transition_message(last_mode, new_mode, reason=mode_result.get("reason", ""), reason_metrics=mode_result.get("reason_metrics", {}), human_reason=mode_result.get("human_reason", ""))
        send_telegram_message(msg)
        now_ts = int(time.time())
        if new_mode == MODE_BLOCK_LONGS and last_mode != MODE_BLOCK_LONGS:
            try:
                r.set(MARKET_MODE_BLOCK_STARTED_KEY, str(now_ts))
                r.delete(MARKET_MODE_BLOCK_PROTECTION_APPLIED_KEY)
            except Exception:
                pass
        if new_mode != MODE_BLOCK_LONGS:
            try:
                r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
                r.delete(MARKET_MODE_BLOCK_PROTECTION_APPLIED_KEY)
            except Exception:
                pass
        r.set(MARKET_MODE_LAST_KEY, new_mode)
        r.set(MARKET_MODE_LAST_TRANSITION_KEY, str(now_ts))
        try:
            # Reset reminder state on mode transition so the first reminder for
            # the new mode is scheduled from the transition time, not from an
            # old timestamp belonging to the previous mode.
            r.delete(MARKET_MODE_REMINDER_COUNT_KEY)
            r.set(MARKET_MODE_REMINDER_MODE_KEY, new_mode)
            r.set(MARKET_MODE_LAST_REMINDER_KEY, str(now_ts))
        except Exception:
            pass
        logger.info(f"MODE TRANSITION: {last_mode} → {new_mode} | reason: {mode_result.get('reason')}")
    r.set(MARKET_MODE_KEY, new_mode)
 except Exception as e:
    logger.error(f"handle_market_mode_transition error: {e}")
 return new_mode


# =========================
# v200 ARCHITECTURE LAYER HELPERS
# =========================
FAST_INTRADAY_RED_RATIO = 0.85
FAST_INTRADAY_AVG15M = -0.70
FAST_INTRADAY_BTC15M = -0.35
# v202: breadth-only stress should usually move NORMAL -> STRONG, not BLOCK.
# BLOCK needs breadth + price pressure, BTC pressure, or explicit guard danger.
FAST_INTRADAY_BLOCK_AVG15M = -0.70
FAST_INTRADAY_BLOCK_BTC15M = -0.35
FAST_INTRADAY_BLOCK_RED_RATIO = 0.85
FAST_INTRADAY_HARD_BTC15M = -0.70
FAST_INTRADAY_HARD_AVG15M = -1.00
FAST_MODE_MONITOR_INTERVAL = 75
FAST_MODE_MONITOR_SAMPLE_SIZE = 30
FAST_MODE_MONITOR_LOCK_KEY = "market_mode:long:fast_monitor_lock"
FAST_MODE_MONITOR_LOCK_TTL = 65
MARKET_REMINDER_FIRST_INTERVAL = 15 * 60
MARKET_REMINDER_INTERVAL = 30 * 60

HARD_RESISTANCE_SOURCES = {"previous_rejection", "swing_high_confirmed", "round_level_confirmed"}
SOFT_RESISTANCE_SOURCES = {"bb_upper", "recent_high", "recent_20_high", "minor_round", "dynamic_resistance", "bb_upper_hint"}


def _arch_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _arch_add_warning(candidate: dict, warning: str) -> None:
    if not candidate or not warning:
        return
    candidate.setdefault("warning_reasons", [])
    if not isinstance(candidate.get("warning_reasons"), list):
        candidate["warning_reasons"] = [str(candidate.get("warning_reasons"))]
    if warning not in candidate["warning_reasons"]:
        candidate["warning_reasons"].append(warning)
    candidate.setdefault("soft_warning_reasons", [])
    if warning not in candidate["soft_warning_reasons"]:
        candidate["soft_warning_reasons"].append(warning)
    _bump_long_soft_warning(warning)


def _arch_add_adjustment(candidate: dict, name: str, value: float, reason: str) -> None:
    if not candidate:
        return
    candidate.setdefault("adjustments_log", [])
    if not isinstance(candidate.get("adjustments_log"), list):
        candidate["adjustments_log"] = []
    candidate["adjustments_log"].append({"name": name, "value": value, "reason": reason})


def build_market_engine_context(market_guard: dict, market_state: str, btc_mode: str, alt_snapshot: dict) -> dict:
    """Separate slow trend from 15m intraday stress for v202 mode decisions.

    Breadth-only redness moves the bot to STRONG_LONG_ONLY.  BLOCK_LONGS requires
    confirmed danger: breadth + price pressure, BTC pressure, or explicit guard danger.
    """
    market_guard = market_guard or {}
    alt_snapshot = alt_snapshot or {}
    red_ratio = _arch_float(market_guard.get("red_ratio_15m"), 0.0)
    avg15m = _arch_float(market_guard.get("avg_change_15m"), 0.0)
    btc15m = _arch_float(market_guard.get("btc_change_15m"), 0.0)
    guard_level = str(market_guard.get("level") or "normal").lower()

    breadth_stress = red_ratio >= FAST_INTRADAY_RED_RATIO
    avg_stress = avg15m <= FAST_INTRADAY_AVG15M
    btc_stress = btc15m <= FAST_INTRADAY_BTC15M
    guard_danger = guard_level in ("danger", "block")
    danger = bool(breadth_stress or avg_stress or btc_stress or guard_danger)

    severe = bool(
        guard_danger
        or btc15m <= FAST_INTRADAY_HARD_BTC15M
        or avg15m <= FAST_INTRADAY_HARD_AVG15M
        or (breadth_stress and (avg_stress or btc_stress))
    )

    if severe:
        stress_label = "severe"
    elif danger:
        stress_label = "breadth_stress" if breadth_stress and not (avg_stress or btc_stress or guard_danger) else "danger"
    else:
        stress_label = "normal"

    return {
        "overall_trend": market_state or "unknown",
        "btc_mode": btc_mode or "unknown",
        "alt_mode": alt_snapshot.get("alt_mode", "unknown"),
        "intraday_stress": stress_label,
        "fast_intraday_override": bool(danger),
        "fast_intraday_severe": bool(severe),
        "red_ratio_15m": red_ratio,
        "avg_change_15m": avg15m,
        "btc_change_15m": btc15m,
        "market_guard_level": guard_level,
        "fast_intraday_reason": (
            f"red_ratio={red_ratio:.2f}, avg15m={avg15m:.2f}, btc15m={btc15m:.2f}, guard={guard_level}"
        ),
    }


def _format_fast_intraday_reason_for_logs(market_engine: dict) -> str:
    try:
        return (
            f"red_ratio={float(market_engine.get('red_ratio_15m', 0) or 0):.2f}, "
            f"avg15m={float(market_engine.get('avg_change_15m', 0) or 0):.2f}, "
            f"btc15m={float(market_engine.get('btc_change_15m', 0) or 0):.2f}, "
            f"guard={market_engine.get('market_guard_level', 'normal')}"
        )
    except Exception:
        return str(market_engine.get("fast_intraday_reason", ""))


def apply_fast_intraday_override(mode_result: dict, market_engine: dict, current_mode: str) -> dict:
    """Fast 15m stress cannot leave the bot in NORMAL_LONG, but breadth-only stress is STRONG not BLOCK."""
    result = dict(mode_result or {})
    if not market_engine.get("fast_intraday_override"):
        result.setdefault("architecture_layer", "market_engine")
        return result

    old_mode = normalize_market_mode(result.get("mode", current_mode or MODE_NORMAL_LONG))
    severe = bool(market_engine.get("fast_intraday_severe"))

    if severe:
        result["mode"] = MODE_BLOCK_LONGS
        reason_key = "fast_intraday_block"
    elif old_mode == MODE_NORMAL_LONG:
        result["mode"] = MODE_STRONG_LONG_ONLY
        reason_key = "fast_intraday_strong"
    else:
        result["mode"] = old_mode
        reason_key = "fast_intraday_monitor"

    result["reason"] = reason_key
    result["human_reason"] = format_market_mode_reason_for_message(reason_key, market_engine)
    result["reason_metrics"] = dict(market_engine or {})
    result["fast_intraday_override"] = True
    result["intraday_stress"] = market_engine.get("intraday_stress")
    logger.info(
        "MARKET ENGINE FAST OVERRIDE | "
        f"old={old_mode} -> new={result.get('mode')} | {_format_fast_intraday_reason_for_logs(market_engine)}"
    )
    return result


def _v200_collect_setup_tags(candidate: dict) -> list:
    """Unified tag source for whitelist, weak drift, resistance relaxation, badge, and diagnostics."""
    if not candidate:
        return []
    try:
        tags = set(build_execution_setup_tags(candidate))
    except Exception:
        tags = set()
    fields = [
        "primary_extra_setup", "extra_setup", "extra_setup_name", "setup_type", "setup_type_base",
        "context_setup", "wave_label", "wave_estimate", "entry_maturity", "opportunity_type",
    ]
    for field in fields:
        value = candidate.get(field)
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if item:
                    tags.add(str(item).strip())
        elif value not in (None, ""):
            tags.add(str(value).strip())
    for field in ("extra_setup_names", "extra_setups", "context_setups", "early_execution_setup_tags"):
        value = candidate.get(field)
        if isinstance(value, dict):
            value = list(value.keys()) + list(value.values())
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if item:
                    tags.add(str(item).strip())
        elif value not in (None, ""):
            tags.add(str(value).strip())
    diagnostics = candidate.get("diagnostics") or {}
    if isinstance(diagnostics, dict):
        for key in ("setup_tags", "execution_setup_tags", "wave_context", "context_setups"):
            value = diagnostics.get(key)
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    if item:
                        tags.add(str(item).strip())
            elif value not in (None, ""):
                tags.add(str(value).strip())
    if candidate.get("relative_strength_vs_btc"):
        tags.add("relative_strength_vs_btc")
    try:
        if int(candidate.get("wave_estimate") or 0) == 3:
            tags.add("wave_3")
    except Exception:
        pass
    return sorted({t for t in tags if t and t.lower() not in ("none", "unknown", "false")})


def v200_score_candidate(candidate: dict) -> dict:
    """Centralized final score snapshot. Does not duplicate existing penalties."""
    if not candidate:
        return candidate
    raw = _arch_float(candidate.get("raw_score", candidate.get("score", 0.0)), 0.0)
    effective = _arch_float(candidate.get("effective_score", candidate.get("score", raw)), raw)
    candidate["raw_score"] = round(raw, 2)
    candidate["effective_score"] = round(effective, 2)
    candidate["score"] = round(effective, 2)
    candidate.setdefault("final_threshold", FINAL_MIN_SCORE)
    candidate.setdefault("adjustments_log", [])
    candidate.setdefault("warnings", candidate.get("warning_reasons", []))
    return candidate


def v200_smart_resistance_classification(candidate: dict) -> dict:
    """Classifies resistance after candidate build for diagnostics and later ranking."""
    if not candidate:
        return candidate
    source = str(candidate.get("nearest_resistance_source") or candidate.get("resistance_source") or "unknown")
    entry = _arch_float(candidate.get("entry") or candidate.get("recommended_entry") or candidate.get("market_entry"), 0.0)
    resistance = _arch_float(candidate.get("nearest_resistance"), 0.0)
    sl = _arch_float(candidate.get("sl"), 0.0)
    dist_pct = ((resistance - entry) / entry * 100.0) if entry > 0 and resistance > entry else None
    risk = entry - sl if entry > 0 and sl > 0 else 0.0
    resistance_r = ((resistance - entry) / risk) if risk > 0 and resistance > entry else None
    tags = set(candidate.get("execution_setup_tags") or [])
    strong_setup = bool(tags.intersection(set(STRONG_ONLY_ALLOWED_SETUPS))) or bool(candidate.get("has_extra_strong_setup")) or bool(candidate.get("mtf_confirmed"))
    vol_ok = _arch_float(candidate.get("vol_ratio"), 0.0) >= 1.05
    source_soft = source in SOFT_RESISTANCE_SOURCES or source in ("unknown", "micro_high", "tiny_wick")
    source_hard = source in HARD_RESISTANCE_SOURCES or "previous_rejection" in source or "confirmed" in source
    ultra_close = bool(dist_pct is not None and dist_pct <= 0.12) or bool(resistance_r is not None and resistance_r <= 0.15)
    classification = "resistance_none"
    if resistance > entry > 0:
        if ultra_close or source_hard:
            classification = "resistance_hard_reject" if ultra_close else "resistance_structural_hard"
        elif source_soft and strong_setup and vol_ok:
            classification = "resistance_warning_only"
            _arch_add_warning(candidate, "resistance_warning_only")
            candidate["resistance_relaxed_reason"] = "soft_source_with_strong_setup_volume_or_mtf"
        else:
            classification = "resistance_penalty_only"
            _arch_add_warning(candidate, "resistance_penalty_only")
    candidate["resistance_classification"] = classification
    candidate["resistance_distance_pct"] = round(dist_pct, 4) if dist_pct is not None else None
    candidate["resistance_r"] = round(resistance_r, 4) if resistance_r is not None else None
    return candidate


def v200_finalize_candidate(candidate: dict, market_engine: dict) -> dict:
    """Final candidate normalization before ranking/messaging."""
    if not candidate:
        return candidate
    candidate["architecture_version"] = "v200"
    candidate["decision_path"] = "signal_engine>setup_tags>quality_engine>scoring_engine>smart_resistance>ranking>messaging>execution_optional"
    candidate["market_engine"] = market_engine or {}
    candidate["execution_setup_tags"] = _v200_collect_setup_tags(candidate)
    candidate = v200_score_candidate(candidate)
    candidate = v200_smart_resistance_classification(candidate)
    warnings = candidate.get("warning_reasons") or []
    if warnings:
        LONG_LAYER_COUNTER["soft_penalty_only"] = int(LONG_LAYER_COUNTER.get("soft_penalty_only", 0) or 0) + 1
        for warning in warnings:
            _bump_long_soft_warning(str(warning))
    LONG_LAYER_COUNTER["candidate_built"] = int(LONG_LAYER_COUNTER.get("candidate_built", 0) or 0) + 1
    return candidate


def v200_rank_candidates(candidates: list, current_mode: str) -> list:
    """Ranking engine: rank after hard danger rejection, not reject-first."""
    def _rank(c):
        tags = set(c.get("execution_setup_tags") or [])
        setup_bonus = 0.45 if tags.intersection(set(STRONG_ONLY_ALLOWED_SETUPS)) else 0.0
        mtf_bonus = 0.25 if c.get("mtf_confirmed") else 0.0
        volume_bonus = min(max(_arch_float(c.get("vol_ratio"), 0.0) - 1.0, 0.0), 1.2) * 0.20
        resistance_room = _arch_float(c.get("resistance_r"), 1.0) if c.get("resistance_r") is not None else 1.0
        resistance_bonus = min(max(resistance_room, 0.0), 2.0) * 0.10
        ma_dist = abs(_arch_float(c.get("dist_ma"), 0.0))
        distance_penalty = min(ma_dist / 20.0, 0.45)
        mode_bonus = 0.15 if current_mode == MODE_STRONG_LONG_ONLY and setup_bonus else 0.0
        return (
            _arch_float(c.get("effective_score", c.get("score", 0.0)), 0.0)
            + setup_bonus + mtf_bonus + volume_bonus + resistance_bonus + mode_bonus - distance_penalty,
            _arch_float(c.get("momentum_priority"), 0.0),
            _arch_float(c.get("change_24h"), 0.0),
        )
    try:
        for c in candidates or []:
            c["v200_rank_score"] = round(_rank(c)[0], 4)
        return sorted(candidates or [], key=_rank, reverse=True)
    except Exception:
        return candidates or []


def v200_momentum_zero_fallback(before_filter: list, after_filter: list, current_mode: str) -> list:
    """Prevents total silence when the momentum filter removes every viable non-block candidate."""
    if after_filter or not before_filter or current_mode == MODE_BLOCK_LONGS:
        return after_filter
    strong = []
    for c in before_filter:
        tags = set(c.get("execution_setup_tags") or [])
        score = _arch_float(c.get("effective_score", c.get("score", 0.0)), 0.0)
        vol_ratio = _arch_float(c.get("vol_ratio"), 0.0)
        if score >= FINAL_MIN_SCORE and vol_ratio >= 1.05 and (tags.intersection(set(STRONG_ONLY_ALLOWED_SETUPS)) or c.get("has_extra_strong_setup") or c.get("mtf_confirmed")):
            _arch_add_warning(c, "top_momentum_zero_fallback_allowed")
            strong.append(c)
    fallback = v200_rank_candidates(strong, current_mode)[:3]
    if fallback:
        logger.warning(
            f"V200 MOMENTUM FALLBACK | before={len(before_filter)} after=0 restored={len(fallback)} | mode={current_mode}"
        )
    return fallback


def v200_log_candidate_diagnostics(candidate: dict, layer: str = "candidate") -> None:
    try:
        logger.info(
            "V200 DIAGNOSTICS | "
            f"layer={layer} | symbol={candidate.get('symbol')} | mode={candidate.get('market_mode')} | "
            f"score={candidate.get('score')} | raw={candidate.get('raw_score')} | eff={candidate.get('effective_score')} | "
            f"tags={candidate.get('execution_setup_tags')} | res={candidate.get('resistance_classification')} | "
            f"warnings={candidate.get('warning_reasons', [])[:4]}"
        )
    except Exception:
        pass


# =========================
# TELEGRAM LOOP
# =========================
def bootstrap_telegram_offset_once():
 if is_telegram_bootstrap_done():
    return
 if not acquire_telegram_poll_lock():
    return
 try:
    updates = get_telegram_updates(offset=0)
    if updates:
        latest_offset = updates[-1]["update_id"] + 1
        save_telegram_offset(latest_offset)
        logger.info(f"Telegram bootstrap offset set to {latest_offset}")
    else:
        logger.info("Telegram bootstrap: no pending updates")
    mark_telegram_bootstrap_done()
 except Exception as e:
    logger.error(f"Telegram bootstrap error: {e}")
 finally:
    release_telegram_poll_lock()

HEAVY_TELEGRAM_COMMANDS = {
    "/open_trades",
    "/report_1h",
    "/report_today",
    "/report_month",
    "/report_30d",
    "/report_all",
    "/report_deep",
    "/report_setups",
    "/report_execution",
    "/report_scores",
    "/report_market",
    "/report_losses",
    "/report_diagnostics",
    "/report_exits",
    "/report_rejections",
    "/report_daily",
    "/report_7d",
}

def run_command_handler_async(command: str, chat_id: str, handler) -> None:
    """Run heavy Telegram commands without blocking the polling loop."""
    def _runner():
        try:
            handler(chat_id)
        except Exception as e:
            logger.error(f"Telegram async command failed | {command}: {e}", exc_info=True)
            try:
                send_telegram_reply(chat_id, f"❌ حصل خطأ أثناء تنفيذ {html.escape(command)}")
            except Exception:
                pass

    threading.Thread(target=_runner, name=f"telegram-{command.strip('/') or 'cmd'}", daemon=True).start()

def dispatch_telegram_command(command: str, chat_id: str, handler) -> None:
    if command in HEAVY_TELEGRAM_COMMANDS:
        send_telegram_reply(chat_id, "⏳ جاري تجهيز التقرير...")
        run_command_handler_async(command, chat_id, handler)
        return
    handler(chat_id)

def handle_telegram_commands():
 if not acquire_telegram_poll_lock():
    return
 try:
    offset = get_telegram_offset()
    updates = get_telegram_updates(offset=offset)
    latest_offset = offset
    for update in updates:
        try:
            latest_offset = update["update_id"] + 1
            if update.get("callback_query"):
                handle_callback_query(update["callback_query"])
                continue
            message = update.get("message") or {}
            text = (message.get("text") or "").strip()
            chat = message.get("chat") or {}
            chat_id = str(chat.get("id", ""))
            if not text or not chat_id:
                continue
            command = text.split()[0].split("@")[0]
            handler = COMMAND_HANDLERS.get(command)
            if handler:
                dispatch_telegram_command(command, chat_id, handler)
        except Exception as e:
            logger.error(f"handle_telegram_commands error: {e}")
    if latest_offset != offset:
        save_telegram_offset(latest_offset)
 finally:
    release_telegram_poll_lock()

# =======================
# SMART SL
# =======================
def build_smart_sl_long(
    df,
    entry: float,
    atr_value: float,
    signal_type: str = "standard",
    market_state: str = "mixed",
    breakout: bool = False,
    pre_breakout: bool = False,
    is_reverse: bool = False,
) -> dict:
    result = {
        "sl": calculate_stop_loss(entry, atr_value, signal_type=signal_type),
        "nearest_support": None,
        "sl_method": "atr_fallback",
        "sl_notes": ["fallback_to_standard_sl"],
    }
    try:
        if not SMART_SL_ENABLED or entry <= 0 or atr_value <= 0:
            return result
        support_candidates = []
        supp_data = find_nearest_support_long(df, entry)
        if supp_data:
            support_candidates.append({
                "price": supp_data["price"],
                "source": supp_data.get("source", "nearest_support")
            })

        signal_row = get_signal_row(df)
        if signal_row is not None:
            idx = signal_row.name
            if idx is not None and idx >= 10:
                lookback = min(SMART_SL_SUPPORT_LOOKBACK, idx)
                recent_lows = df["low"].iloc[idx - lookback:idx].astype(float)
                swing_low = float(recent_lows.min())
                if swing_low < entry:
                    support_candidates.append({
                        "price": swing_low,
                        "source": "swing_low_atr"
                    })

        ma_val = _safe_float(signal_row.get("ma"), 0.0) if signal_row is not None else 0.0
        vwap_val = _safe_float(signal_row.get("vwap"), 0.0) if signal_row is not None else 0.0
        if 0 < ma_val < entry:
            support_candidates.append({"price": ma_val, "source": "ma_support"})
        if 0 < vwap_val < entry:
            support_candidates.append({"price": vwap_val, "source": "vwap_support"})

        if not support_candidates:
            return result

        support_candidates.sort(key=lambda x: x["price"], reverse=True)
        chosen = support_candidates[0]

        raw_sl = chosen["price"] - atr_value * SMART_SL_ATR_BUFFER
        min_sl = entry * (1 - SMART_SL_MAX_PCT / 100.0)
        max_sl = entry * (1 - SMART_SL_MIN_PCT / 100.0)
        sl = max(min_sl, min(max_sl, raw_sl))
        sl = round(sl, 6)

        result["sl"] = sl
        result["nearest_support"] = chosen["price"]
        result["sl_method"] = f"{chosen['source']}_atr"
        result["sl_notes"] = [f"support={chosen['price']}", f"source={chosen['source']}", f"buffer={SMART_SL_ATR_BUFFER}"]
        if sl == max_sl:
            result["sl_method"] = "bounded_max_sl"
        elif sl == min_sl:
            result["sl_method"] = "bounded_min_sl"
        return result
    except Exception as e:
        logger.warning(f"build_smart_sl_long error: {e}")
        return result

# =======================
# FUNDING PARALLEL FETCH
# =======================
def fetch_funding_rates_parallel(symbols, max_workers=MAX_CANDLE_FETCH_WORKERS) -> dict:
    if not symbols:
        return {}
    result = {}
    def _fetch(sym):
        return sym, get_funding_rate(sym)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch, s): s for s in symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                _, rate = future.result()
                result[sym] = rate
            except Exception as e:
                logger.warning(f"Funding fetch failed for {sym}: {e}")
                result[sym] = 0.0
    return result

# =========================
# LOCAL CACHE CLEANUP
# =========================
def cleanup_local_caches():
    now = time.time()
    margin = 300
    stale_send = [sym for sym, ts in sent_cache.items() if now - ts > LOCAL_RECENT_SEND_SECONDS + margin]
    for sym in stale_send:
        del sent_cache[sym]
    horizon = 3600
    stale_candle = [sym for sym, ts in last_candle_cache_meta.items() if now - ts > horizon]
    for sym in stale_candle:
        last_candle_cache.pop(sym, None)
        last_candle_cache_meta.pop(sym, None)


def _is_trade_counted_for_execution_drawdown_guard(trade: dict) -> bool:
    """Daily DD counts only trades that entered the execution path.

    Count: accepted_preview, pending_pullback_preview, live/placed/executed.
    Ignore: candidate_only, rejected_limit, execution_paused, daily_drawdown_lock, preview_rejected.
    """
    try:
        status = str(_trade_field(trade, "execution_status", "") or "").strip().lower()
        result_status = str(_trade_field(trade, "execution_result_status", "") or "").strip().lower()
        combined = {status, result_status}
        counted = {
            "accepted_preview",
            "pending_pullback_preview",
            "live_execute",
            "live_executed",
            "live_order_placed",
            "executed",
            "order_placed",
        }
        ignored = {
            "",
            "candidate_only",
            "execution_paused",
            "rejected_limit",
            "rejected_existing_symbol",
            "rejected_invalid_order",
            "daily_drawdown_lock",
            "preview_rejected",
            "not_candidate",
        }
        if combined & counted:
            return True
        if combined <= ignored:
            return False
        return False
    except Exception:
        return False


def _execution_day_key() -> str:
    try:
        return str(int(get_local_day_start_ts()))
    except Exception:
        return time.strftime("%Y%m%d", time.localtime())


def _get_simulated_start_equity_usd() -> float:
    try:
        return float(EXECUTION_WALLET_FALLBACK_USD or 1000.0)
    except Exception:
        return 1000.0


def get_execution_daily_start_equity_usd() -> tuple:
    """Return fixed start-of-day equity for Daily DD.

    Current safe mode uses config/simulation value. Live OKX equity sync can be
    connected later without changing the Daily DD math.
    """
    day_key = _execution_day_key()
    fallback = _get_simulated_start_equity_usd()
    if not r:
        return fallback, "config"
    try:
        stored_day = str(r.get(EXECUTION_DAILY_START_TS_KEY) or "")
        stored_equity = r.get(EXECUTION_DAILY_START_EQUITY_KEY)
        if stored_day == day_key and stored_equity not in (None, ""):
            equity = float(stored_equity)
            if equity > 0:
                return equity, "redis"
        r.set(EXECUTION_DAILY_START_TS_KEY, day_key)
        r.set(EXECUTION_DAILY_START_EQUITY_KEY, str(fallback))
        return fallback, "config"
    except Exception as e:
        logger.warning(f"get_execution_daily_start_equity_usd error: {e}")
        return fallback, "config"


def _load_daily_execution_guard_trades() -> list:
    since_ts = get_local_day_start_ts()
    try:
        trades = load_all_trades_for_report(r, market_type="futures", side="long", since_ts=since_ts, include_open=True)
    except Exception:
        trades = _load_long_trades_from_redis(limit=1500)
        trades = [t for t in trades if _trade_created_ts_for_exec(t) >= since_ts]
    return [t for t in trades if _is_trade_counted_for_execution_drawdown_guard(t)]


def _execution_daily_wallet_net_usd() -> float:
    """Today's execution Wallet Impact in USD, using 40/40/20 PnL helpers."""
    try:
        trades = _load_daily_execution_guard_trades()
        net_pct = 0.0
        for t in trades:
            pnl = _execution_floating_pnl_pct(t) if _is_execution_trade_open(t) else _execution_final_pnl_pct(t)
            if pnl is not None:
                net_pct += float(pnl)
        return float(net_pct) * (float(EXECUTION_TRADE_MARGIN_USD or 35.0) / 100.0)
    except Exception as e:
        logger.warning(f"_execution_daily_wallet_net_usd error: {e}")
        return 0.0


def get_execution_daily_guard_snapshot() -> dict:
    """Current Daily DD status based on Wallet Impact / start-of-day equity."""
    try:
        equity, source = get_execution_daily_start_equity_usd()
        net_usd = _execution_daily_wallet_net_usd()
        dd_pct = (net_usd / equity * 100.0) if equity else 0.0
        limit_pct = float(EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT or 35.0)
        limit_usd = -abs(equity * limit_pct / 100.0) if equity else 0.0
        locked = bool(is_execution_paused())
        reason = ""
        if r:
            try:
                reason = str(r.get(EXECUTION_DRAWDOWN_LOCK_REASON_KEY) or "")
            except Exception:
                reason = ""
        return {
            "start_equity_usd": float(equity or 0.0),
            "equity_source": source,
            "daily_net_usd": float(net_usd or 0.0),
            "daily_dd_pct": float(dd_pct or 0.0),
            "limit_pct": limit_pct,
            "limit_usd": float(limit_usd or 0.0),
            "locked": locked,
            "reason": reason,
            "day_start_ts": get_local_day_start_ts(),
            "trade_margin_usd": float(EXECUTION_TRADE_MARGIN_USD or 35.0),
        }
    except Exception as e:
        logger.warning(f"get_execution_daily_guard_snapshot error: {e}")
        return {
            "start_equity_usd": _get_simulated_start_equity_usd(),
            "equity_source": "config",
            "daily_net_usd": 0.0,
            "daily_dd_pct": 0.0,
            "limit_pct": float(EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT or 35.0),
            "limit_usd": -abs(_get_simulated_start_equity_usd() * float(EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT or 35.0) / 100.0),
            "locked": bool(is_execution_paused()),
            "reason": "snapshot_error",
            "day_start_ts": get_local_day_start_ts(),
            "trade_margin_usd": float(EXECUTION_TRADE_MARGIN_USD or 35.0),
        }


def _execution_daily_pnl_pct() -> float:
    """Compatibility: Daily DD % based on Wallet Impact, not summed leveraged trade percentages."""
    return float(get_execution_daily_guard_snapshot().get("daily_dd_pct", 0.0) or 0.0)


def enforce_execution_daily_drawdown_guard() -> dict:
    """Stop execution when daily Wallet Impact drawdown reaches configured limit."""
    if not r:
        return {"locked": False, "pnl": 0.0, "wallet_net_usd": 0.0}
    try:
        snapshot = get_execution_daily_guard_snapshot()
        daily_dd_pct = float(snapshot.get("daily_dd_pct", 0.0) or 0.0)
        daily_net_usd = float(snapshot.get("daily_net_usd", 0.0) or 0.0)
        if is_execution_paused():
            return {"locked": True, "pnl": daily_dd_pct, "wallet_net_usd": daily_net_usd, "reason": "execution_paused"}
        if daily_dd_pct <= -abs(EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT):
            reason = f"daily_wallet_drawdown_{daily_dd_pct:.2f}%_{daily_net_usd:.2f}$"
            r.set(EXECUTION_PAUSE_KEY, "1")
            r.set(EXECUTION_DRAWDOWN_LOCK_REASON_KEY, reason)
            logger.warning(f"EXECUTION DAILY WALLET DRAWDOWN LOCK: {reason}")
            return {"locked": True, "pnl": daily_dd_pct, "wallet_net_usd": daily_net_usd, "reason": reason}
        return {"locked": False, "pnl": daily_dd_pct, "wallet_net_usd": daily_net_usd}
    except Exception as e:
        logger.warning(f"enforce_execution_daily_drawdown_guard error: {e}")
        return {"locked": False, "pnl": 0.0, "wallet_net_usd": 0.0}


def _normalize_execution_status(status: str, reason: str = "") -> str:
    status_l = str(status or "").lower().strip()
    reason_l = str(reason or "").lower().strip()
    text = f"{status_l}|{reason_l}"
    if status_l in ("accepted_preview", "pending_pullback_preview"):
        return status_l
    if "daily_drawdown" in text or "drawdown_lock" in text:
        return "daily_drawdown_lock"
    if "max_open" in text or "limit" in text or "too_many" in text or "position_limit" in text or "max_positions" in text:
        return "rejected_limit"
    if "existing" in text or "same_symbol" in text or "already_open" in text or "duplicate" in text or "already_in_execution" in text:
        return "rejected_existing_symbol"
    if "invalid" in text or "missing" in text or ("entry" in text and "sl" in text):
        return "rejected_invalid_order"
    if status_l in ("rejected_limit", "rejected_existing_symbol", "rejected_invalid_order", "daily_drawdown_lock", "preview_rejected", "candidate_only"):
        return status_l
    if status_l in ("rejected", "skipped", "unavailable"):
        return "preview_rejected"
    return status_l or "candidate_only"


def _execution_rejection_reason_ar(status: str, reason: str = "") -> str:
    status = str(status or "")
    if status == "daily_drawdown_lock":
        return "تم إيقاف التنفيذ بسبب تجاوز حد الخسارة اليومية 35%، والعودة فقط عبر /resume_trading"
    if status == "rejected_limit":
        return "تم الوصول للحد الأقصى للصفقات المفتوحة"
    if status == "rejected_existing_symbol":
        return "توجد صفقة تنفيذ مفتوحة لنفس العملة"
    if status == "rejected_invalid_order":
        return "بيانات الأمر غير مكتملة أو غير صالحة"
    if status == "candidate_only":
        return "مرشح فقط ولم يدخل مرحلة التنفيذ التجريبي"
    if status == "preview_rejected":
        return str(reason or "تم رفض التنفيذ من وحدة التنفيذ / إدارة المخاطر")
    return str(reason or "سبب غير محدد")


def build_execution_rejection_message(symbol: str, status: str, reason: str = "") -> str:
    return (
        "⚠️ <b>Execution Candidate Rejected</b>\n"
        "━━━━━━━━━━━━\n"
        f"🪙 <b>{html.escape(str(symbol or '?'))}</b>\n"
        f"📌 Status: <code>{html.escape(str(status or 'preview_rejected'))}</code>\n"
        f"❌ {html.escape(_execution_rejection_reason_ar(status, reason))}\n"
        "━━━━━━━━━━━━\n"
        "📊 تابعها من: /report_execution"
    )


def build_execution_paused_message(symbol: str) -> str:
    return (
        "⚠️ <b>Execution Candidate Rejected</b>\n"
        "━━━━━━━━━━━━\n"
        f"🪙 <b>{html.escape(str(symbol or '?'))}</b>\n"
        "📌 Status: <code>execution_paused</code>\n"
        "❌ التنفيذ متوقف يدويًا أو بسبب Daily DD\n"
        "━━━━━━━━━━━━\n"
        "📊 تابعها من: /report_execution"
    )


def _execution_message_already_sent(candidate: dict, expected_status: str = "") -> bool:
    if not r or not candidate:
        return False
    try:
        symbol = str(candidate.get("symbol") or "")
        candle_time = int(float(candidate.get("candle_time") or 0))
        if not symbol or candle_time <= 0:
            return False
        for key in (f"trade:futures:long:{symbol}:{candle_time}", f"trade_history:futures:long:{symbol}:{candle_time}"):
            raw = r.get(key)
            if not raw:
                continue
            data = json.loads(raw)
            if not isinstance(data, dict):
                continue
            diag = data.get("diagnostics", {}) or {}
            status = str(data.get("execution_status") or diag.get("execution_status") or "")
            sent = bool(data.get("execution_message_sent") or diag.get("execution_message_sent"))
            if sent and (not expected_status or status == expected_status):
                return True
        return False
    except Exception:
        return False


def update_execution_status_for_candidate(candidate: dict, status: str, reason: str = "", message_sent: bool = False) -> None:
    if not r or not candidate:
        return
    try:
        symbol = str(candidate.get("symbol") or "")
        candle_time = int(float(candidate.get("candle_time") or 0))
        if not symbol or candle_time <= 0:
            return
        updates = {
            "execution_status": status,
            "execution_result_status": status,
            "execution_reject_reason": reason or "",
            "execution_message_sent": bool(message_sent),
            "execution_updated_ts": int(time.time()),
        }
        if "execution_candidate_badged" in candidate:
            updates["execution_candidate_badged"] = bool(candidate.get("execution_candidate_badged"))
            updates["execution_candidate_badge_sent"] = bool(candidate.get("execution_candidate_badge_sent", candidate.get("execution_candidate_badged")))
        for key in (f"trade:futures:long:{symbol}:{candle_time}", f"trade_history:futures:long:{symbol}:{candle_time}"):
            raw = r.get(key)
            if not raw:
                continue
            data = json.loads(raw)
            if not isinstance(data, dict):
                continue
            data.update(updates)
            diagnostics = data.get("diagnostics", {}) or {}
            diagnostics.update(updates)
            data["diagnostics"] = diagnostics
            ttl = r.ttl(key)
            r.set(key, json.dumps(data, ensure_ascii=False))
            if ttl and ttl > 0:
                r.expire(key, ttl)
    except Exception as e:
        logger.warning(f"update_execution_status_for_candidate error: {e}")

# =========================
# MAIN LOOP
# =========================
def run_command_poller():
    bootstrap_telegram_offset_once()
    while True:
        try:
            handle_telegram_commands()
        except Exception as e:
            logger.error(f"Command poller error: {e}")
        time.sleep(COMMAND_POLL_INTERVAL)

def run_scanner_loop():
    global last_global_send_ts, _last_scan_skip_log_ts
    logger.info("🚀 run_scanner_loop entered")
    while True:
        try:
            cleanup_local_caches()
        except Exception:
            pass
        scan_locked = False
        try:
            scan_locked = acquire_scan_lock()
            if not scan_locked:
                _now_ts = time.time()
                if _now_ts - _last_scan_skip_log_ts >= 60:
                    logger.info("⏳ Another long scan is running --- skipping")
                    _last_scan_skip_log_ts = _now_ts
                time.sleep(SCAN_IDLE_SLEEP_SECONDS)
                continue
            logger.info(f"LONG RUN START | pid={os.getpid()}")

            stats_reset_ts = None
            if r:
                try:
                    raw_reset = r.get(STATS_RESET_TS_KEY)
                    if raw_reset:
                        stats_reset_ts = int(raw_reset)
                except Exception:
                    pass
            ranked_pairs = get_ranked_pairs()
            logger.info(f"SCAN_LIMIT CONFIG = {SCAN_LIMIT} | ranked_pairs_count = {len(ranked_pairs)}")
            btc_mode = get_btc_mode()
            btc_zone = get_btc_range_zone(timeframe="1H", lookback=50)
            alt_snapshot = None
            if r:
                try:
                    cached_snapshot = r.get(ALT_SNAPSHOT_CACHE_KEY)
                    if cached_snapshot:
                        alt_snapshot = json.loads(cached_snapshot)
                        logger.info("ALT SNAPSHOT ---> loaded from cache")
                except Exception as e:
                    logger.warning(f"Alt snapshot cache read error: {e}")
            if alt_snapshot is None:
                alt_snapshot = get_alt_market_snapshot(ranked_pairs)
                if r:
                    try:
                        r.set(ALT_SNAPSHOT_CACHE_KEY, json.dumps(alt_snapshot), ex=ALT_SNAPSHOT_CACHE_TTL)
                    except Exception as e:
                        logger.warning(f"Alt snapshot cache write error: {e}")

            sample_pairs = sorted(
                ranked_pairs,
                key=lambda x: x.get("_rank_volume_24h", 0),
                reverse=True
            )[:MARKET_GUARD_SAMPLE_SIZE]
            guard_candles_map = fetch_candles_parallel(sample_pairs, timeframe="15m", limit=MARKET_GUARD_CANDLE_LIMIT, max_workers=MAX_CANDLE_FETCH_WORKERS)
            if "BTC-USDT-SWAP" not in guard_candles_map:
                btc_candles_15m = get_candles("BTC-USDT-SWAP", "15m", 5)
                guard_candles_map["BTC-USDT-SWAP"] = btc_candles_15m

            market_info = get_market_state(btc_mode, alt_snapshot)
            market_state = market_info["market_state"]
            market_state_label = market_info["market_state_label"]
            market_bias_label = market_info["market_bias_label"]
            btc_dominance_proxy = market_info["btc_dominance_proxy"]
            alt_mode = alt_snapshot.get("alt_mode", "🟡 محايد")
            market_guard = get_market_guard_snapshot(
                ranked_pairs=ranked_pairs,
                btc_mode=btc_mode,
                alt_snapshot=alt_snapshot,
                candles_map_15m=guard_candles_map,
                btc_zone=btc_zone,
            )
            current_mode = r.get(MARKET_MODE_KEY) if r else MODE_NORMAL_LONG
            if not current_mode:
                current_mode = MODE_NORMAL_LONG
            market_engine_context = build_market_engine_context(
                market_guard=market_guard,
                market_state=market_state,
                btc_mode=btc_mode,
                alt_snapshot=alt_snapshot,
            )
            mode_result = determine_long_market_mode(
                market_guard=market_guard,
                market_state=market_state,
                btc_mode=btc_mode,
                alt_snapshot=alt_snapshot,
                current_mode=current_mode,
                allow_state_writes=True,
            )
            mode_result = apply_fast_intraday_override(mode_result, market_engine_context, current_mode)
            current_mode = handle_market_mode_transition(mode_result)

            try:
                update_open_trades(
                    r,
                    market_type="futures",
                    side="long",
                    timeframe=TIMEFRAME,
                    market_mode=current_mode,
                    protect_breakeven_on_block=False,
                    breakeven_min_profit_pct=PROTECT_ON_BLOCK_MIN_PROFIT_PCT,
                    breakeven_buffer_pct=PROTECT_ON_BLOCK_BUFFER_PCT,
                    reason=f"market_mode={current_mode}",
                )
            except TypeError:
                logger.warning("update_open_trades does not accept breakeven_buffer_pct, using fallback without buffer")
                try:
                    update_open_trades(
                        r,
                        market_type="futures",
                        side="long",
                        timeframe=TIMEFRAME,
                        market_mode=current_mode,
                        protect_breakeven_on_block=False,
                        breakeven_min_profit_pct=PROTECT_ON_BLOCK_MIN_PROFIT_PCT,
                        reason=f"market_mode={current_mode}",
                    )
                except Exception:
                    update_open_trades(r, market_type="futures", side="long", timeframe=TIMEFRAME)
            except Exception as e:
                logger.error(f"update_open_trades error: {e}")

            if current_mode == MODE_BLOCK_LONGS and r:
                try:
                    trade_keys = list(r.smembers("open_trades:futures:long"))
                    open_count = len(trade_keys)
                    protected_count = 0
                    for trade_key in trade_keys:
                        try:
                            raw = r.get(trade_key)
                            if raw:
                                trade = json.loads(raw)
                                if isinstance(trade, dict):
                                    if (trade.get("protected_breakeven") or
                                        trade.get("sl_moved_to_entry") or
                                        trade.get("breakeven_protection_reason")):
                                        protected_count += 1
                        except Exception:
                            continue
                    logger.info(
                        f"BLOCK_LONGS protection: {protected_count}/{open_count} open long trades currently protected."
                    )
                except Exception as e:
                    logger.warning(f"Could not count protected trades: {e}")

            winrate_summary = get_winrate_summary(r, market_type="futures", side="long")
            logger.info(format_winrate_summary(winrate_summary))

            logger.info(
                f"MARKET MODE | mode={current_mode} | "
                f"guard_level={market_guard.get('level')} | "
                f"red_ratio={market_guard.get('red_ratio_15m')} | "
                f"avg_change={market_guard.get('avg_change_15m')} | "
                f"btc_change={market_guard.get('btc_change_15m')} | "
                f"reason={mode_result.get('reason')}"
            )
            snapshot_data = {
                "created_ts": int(time.time()),
                "current_mode": current_mode,
                "mode_reason": mode_result.get("reason", ""),
                "btc_mode": btc_mode,
                "btc_zone": btc_zone,
                "alt_snapshot": alt_snapshot,
                "market_info": market_info,
                "market_guard": market_guard,
                "market_engine_context": market_engine_context,
                "ranked_pairs_count": len(ranked_pairs),
                "suggested_mode": mode_result.get("mode", current_mode),
                "suggested_reason": mode_result.get("reason", ""),
            }
            save_market_status_snapshot(snapshot_data)

            # Periodic compact market mode reminder (first after 15m, then every 30m)
            maybe_send_market_mode_reminder(
                current_mode=current_mode,
                btc_mode=btc_mode,
                market_info=market_info,
                alt_snapshot=alt_snapshot,
                ranked_pairs=ranked_pairs,
                market_guard=market_guard,
            )

            global_cooldown_active = is_global_cooldown_active()
            if global_cooldown_active and current_mode in (MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY):
                maybe_send_market_mode_reminder(
                    current_mode=current_mode,
                    btc_mode=btc_mode,
                    market_info=market_info,
                    alt_snapshot=alt_snapshot,
                    ranked_pairs=ranked_pairs,
                    market_guard=market_guard,
                )
                logger.info(
                    f"GLOBAL COOLDOWN active - market mode checked, skipping normal/strong signal sending | mode={current_mode}"
                )
                time.sleep(60)
                continue
            if current_mode == MODE_BLOCK_LONGS:
                logger.warning("MODE BLOCK LONGS - scanning candidates for strict exceptions only")

            upcoming_events = get_upcoming_high_impact_events()
            has_high_impact_news = len(upcoming_events) > 0
            news_warning_text = format_news_warning(upcoming_events)
            logger.info(
                f"LONG MARKET STATE | mode={current_mode} | btc={btc_mode} | alt={alt_mode} | "
                f"state={market_state_label} | flow={market_bias_label}"
            )

            # ---------- RECOVERY LONG ----------
            if current_mode == MODE_RECOVERY_LONG:
                scan_pairs = sorted(
                    ranked_pairs,
                    key=lambda x: x.get("_rank_volume_24h", 0),
                    reverse=True
                )[:20]
                recovery_sent = 0
                for pair_data in scan_pairs:
                    if recovery_sent >= RECOVERY_MAX_ALERTS:
                        break
                    symbol = pair_data["instId"]
                    candles = get_candles(symbol, TIMEFRAME, 100)
                    if not candles:
                        time.sleep(0.4)
                        candles = get_candles(symbol, TIMEFRAME, 100)
                    if not candles:
                        log_long_rejection(symbol=symbol, reason="data_error_no_candles", candle_time=None, market_state=market_state, current_mode=current_mode, extra={"category": "data_error", "original_reason": "no_candles", "data_error_type": "empty_response_or_too_few", "details": "Retry failed for recovery scan"})
                        logger.warning(f"DATA ERROR | {symbol} | no_candles | recovery scan")
                        continue
                    df = to_dataframe(candles)
                    if df is None or df.empty:
                        log_long_rejection(symbol=symbol, reason="dataframe_empty", candle_time=None, market_state=market_state, current_mode=current_mode)
                        continue
                    if not is_valid_candle_timing(df):
                        log_long_rejection(symbol=symbol, reason="invalid_candle_timing", candle_time=get_signal_candle_time(df), market_state=market_state, current_mode=current_mode)
                        continue
                    signal_row = get_signal_row(df)
                    if signal_row is None:
                        continue
                    candle_time = get_signal_candle_time(df)
                    now_local = time.time()
                    if symbol in last_candle_cache and last_candle_cache[symbol] == candle_time:
                        logger.info(f"{symbol} skipped (recovery): local same candle cache")
                        continue
                    if symbol in sent_cache and now_local - sent_cache[symbol] < LOCAL_RECENT_SEND_SECONDS:
                        logger.info(f"{symbol} skipped (recovery): local recent send cache")
                        continue
                    if already_sent_same_candle(symbol, candle_time, "long"):
                        logger.info(f"⏭ Recovery {symbol} skipped: duplicate candle")
                        continue
                    if is_symbol_on_cooldown(symbol, "long"):
                        logger.info(f"⏭ Recovery {symbol} skipped: cooldown")
                        continue
                    dist_ma = get_distance_from_ma_percent(df)
                    if dist_ma >= -3.0:
                        log_long_rejection(symbol=symbol, reason="recovery_dist_ma_not_deep", candle_time=candle_time, market_state=market_state, current_mode=current_mode, dist_ma=dist_ma)
                        continue
                    rsi_now = _safe_float(signal_row.get("rsi"), 50)
                    if rsi_now > 38:
                        log_long_rejection(symbol=symbol, reason="recovery_rsi_too_high", candle_time=candle_time, market_state=market_state, current_mode=current_mode, rsi_now=rsi_now)
                        continue
                    vol_ratio = get_volume_ratio(df)
                    if vol_ratio < 1.02:
                        log_long_rejection(symbol=symbol, reason="recovery_low_volume", candle_time=candle_time, market_state=market_state, current_mode=current_mode, vol_ratio=vol_ratio)
                        continue
                    price = _safe_float(signal_row["close"], 0)
                    atr_value = _safe_float(signal_row.get("atr"), 0)
                    if atr_value <= 0:
                        log_long_rejection(symbol=symbol, reason="recovery_atr_invalid", candle_time=candle_time, market_state=market_state, current_mode=current_mode)
                        continue
                    entry1 = price
                    entry2 = round(price - (atr_value * RECOVERY_ENTRY2_ATR_MULT), 6)
                    avg_entry = round((entry1 + entry2) / 2, 6)
                    sl = round(avg_entry - (atr_value * RECOVERY_SL_ATR_MULT), 6)
                    rr1, rr2 = 2.0, 3.2
                    tp1 = calc_tp_long(avg_entry, sl, rr=rr1)
                    tp2 = calc_tp_long(avg_entry, sl, rr=rr2)
                    tv_link = build_tradingview_link(symbol)
                    red_ratio = market_guard.get("red_ratio_15m", 0.0)
                    avg_change = market_guard.get("avg_change_15m", 0.0)
                    btc_change = market_guard.get("btc_change_15m", 0.0)
                    alert_id = build_alert_id(symbol, candle_time)
                    recovery_msg = build_recovery_long_message(
                        symbol=symbol,
                        entry1=entry1,
                        entry2=entry2,
                        sl=sl,
                        tp1=tp1,
                        tp2=tp2,
                        rr1=rr1,
                        rr2=rr2,
                        atr_value=atr_value,
                        red_ratio=red_ratio,
                        avg_change=avg_change,
                        btc_change=btc_change,
                        alt_mode=alt_mode,
                        tv_link=tv_link,
                    )
                    locked = reserve_signal_slot(symbol, candle_time, "long")
                    if not locked:
                        continue
                    sent_data = send_telegram_message(
                        recovery_msg,
                        reply_markup=build_track_reply_markup(alert_id),
                    )
                    if sent_data.get("ok"):
                        recovery_sent += 1
                        sent_cache[symbol] = time.time()
                        last_candle_cache[symbol] = candle_time
                        last_candle_cache_meta[symbol] = time.time()
                        message_id = str(((sent_data.get("result") or {}).get("message_id")) or "")
                        alert_snapshot = {
                            "alert_id": alert_id,
                            "symbol": symbol,
                            "mode": MODE_RECOVERY_LONG,
                            "market_mode": MODE_RECOVERY_LONG,
                            "timeframe": TIMEFRAME,
                            "entry1": entry1,
                            "entry2": entry2,
                            "average_planned_entry": avg_entry,
                            "total_size_pct": RECOVERY_TOTAL_SIZE_PCT,
                            "entry1_size_pct": RECOVERY_ENTRY1_SIZE_PCT,
                            "entry2_size_pct": RECOVERY_ENTRY2_SIZE_PCT,
                            "sl": sl,
                            "tp1": tp1,
                            "tp2": tp2,
                            "rr1": rr1,
                            "rr2": rr2,
                            "score": 0.0,
                            "candle_time": candle_time,
                            "created_ts": int(time.time()),
                            "market_state": market_state,
                            "alt_mode": alt_mode,
                            "btc_mode": btc_mode,
                            "market_entry": entry1,
                            "entry": entry1,
                            "recommended_entry": avg_entry,
                            "pullback_entry": entry2,
                            "entry_timing": "Recovery Entry",
                            "opportunity_type": "Recovery Long",
                            "is_reverse": True,
                            "setup_type": "recovery|mtf_yes|vol_mid|post_crash",
                            "rsi_now": rsi_now,
                            "dist_ma": dist_ma,
                            "vol_ratio": vol_ratio,
                            "market_red_ratio_15m": red_ratio,
                            "market_avg_change_15m": avg_change,
                            "btc_change_15m": btc_change,
                            "recovery_reason": "Post crash rebound",
                            "above_upper_bb": False,
                            "change_4h": 0.0,
                            "late_pump_risk": False,
                            "bull_continuation_risk": False,
                            "tp1_close_pct": TP1_CLOSE_PCT,
                            "tp2_close_pct": TP2_CLOSE_PCT,
                            "move_sl_to_entry_after_tp1": MOVE_SL_TO_ENTRY_AFTER_TP1,
                            "entry_mode": "market",
                            "pullback_triggered": True,
                        }
                        save_alert_snapshot(alert_snapshot, message_id=message_id)
                        recovery_candidate = {
                            "symbol": symbol,
                            "candle_time": candle_time,
                            "entry": avg_entry,
                            "sl": sl,
                            "tp1": tp1,
                            "tp2": tp2,
                            "score": 0.0,
                            "setup_type": "recovery|mtf_yes|vol_mid|post_crash",
                            "reasons": ["Recovery Long", "Post Crash", "Oversold Bounce"],
                            "warning_reasons": [],
                            "btc_mode": btc_mode,
                            "funding_label": "🟡 محايد",
                            "pre_breakout": False,
                            "breakout": False,
                            "vol_ratio": vol_ratio,
                            "candle_strength": 0.0,
                            "mtf_confirmed": False,
                            "is_new": False,
                            "btc_dominance_proxy": btc_dominance_proxy,
                            "change_24h": extract_24h_change_percent(pair_data),
                            "raw_score": 0.0,
                            "effective_score": 0.0,
                            "dynamic_threshold": 0.0,
                            "required_min_score": 0.0,
                            "final_threshold": 0.0,
                            "dist_ma": dist_ma,
                            "entry_timing": "Recovery Entry",
                            "opportunity_type": "Recovery Long",
                            "market_state": market_state,
                            "market_state_label": market_state_label,
                            "market_bias_label": market_bias_label,
                            "alt_mode": alt_mode,
                            "early_priority": "none",
                            "breakout_quality": "none",
                            "risk_level": "🔴 مرتفع",
                            "fake_signal": False,
                            "is_reverse": True,
                            "reversal_4h_confirmed": False,
                            "rank_volume_24h": float(pair_data.get("_rank_volume_24h", 0)),
                            "alert_id": alert_id,
                            "has_high_impact_news": has_high_impact_news,
                            "news_titles": [e.get("title", "") for e in upcoming_events[:3]],
                            "warning_penalty": 0.0,
                            "warning_high_count": 0,
                            "warning_medium_count": 0,
                            "warning_penalty_details": [],
                            "adjustments_log": [],
                            "pullback_entry": entry2,
                            "pullback_low": entry2,
                            "pullback_high": entry1,
                            "rr1": rr1,
                            "rr2": rr2,
                            "setup_type_base": "recovery",
                            "fib_position": "unknown",
                            "fib_position_ratio": 0.0,
                            "fib_label": "غير معروف",
                            "had_pullback": False,
                            "pullback_pct": 0.0,
                            "pullback_label": "غير معروف",
                            "wave_estimate": 0,
                            "wave_peaks": 0,
                            "wave_label": "غير معروف",
                            "entry_maturity": "unknown",
                            "maturity_penalty": 0.0,
                            "maturity_bonus": 0.0,
                            "falling_knife_risk": False,
                            "falling_knife_reasons": [],
                            "reversal_quality": "",
                            "wave_context": "post_crash",
                            "setup_context": "",
                            "reversal_structure_confirmed": False,
                            "strong_bull_pullback": False,
                            "strong_breakout_exception": False,
                            "target_method": "rr",
                            "nearest_resistance": None,
                            "nearest_support": None,
                            "resistance_warning": "",
                            "support_warning": "",
                            "target_notes": [],
                            "sl_method": "atr",
                            "sl_notes": [],
                            "tp1_close_pct": TP1_CLOSE_PCT,
                            "tp2_close_pct": TP2_CLOSE_PCT,
                            "move_sl_to_entry_after_tp1": MOVE_SL_TO_ENTRY_AFTER_TP1,
                            "has_extra_strong_setup": False,
                            "extra_setup_names": [],
                            "extra_setup_bonus": 0.0,
                            "primary_extra_setup": "",
                            "extra_setups_details": {},
                            "current_mode": current_mode,
                            "has_pullback_plan": False,
                            "entry_mode": "market",
                            "market_entry": entry1,
                            "pullback_triggered": True,
                            "recommended_entry": avg_entry,
                        }
                        register_ok = register_trade_from_candidate(recovery_candidate)
                        set_alert_registration_status(alert_id, register_ok)
                        if not register_ok:
                            logger.error(f"REGISTRATION FAILED after send: symbol={symbol}, alert_id={alert_id}, setup=recovery, mode={current_mode}, message_id={message_id}")
                        logger.info(f"✅ SENT RECOVERY LONG ---> {symbol}")
                    else:
                        release_signal_slot(symbol, candle_time, "long")
                        logger.error(f"❌ FAILED RECOVERY SEND ---> {symbol}")
                if recovery_sent > 0:
                    set_global_cooldown()
                logger.info("Sleeping 60 seconds (recovery mode)...")
                time.sleep(60)
                continue
            # ---------- NORMAL / STRONG ----------
            scan_pairs = ranked_pairs
            max_alerts = MAX_ALERTS_PER_RUN
            filtered_scan_pairs = [
                p for p in scan_pairs
                if prefilter_pair_before_candles(p, current_mode)
            ]
            logger.info(f"Pre-filter kept {len(filtered_scan_pairs)} of {len(scan_pairs)} pairs before candle fetch")
            symbols = [p["instId"] for p in filtered_scan_pairs]
            funding_map = fetch_funding_rates_parallel(symbols, max_workers=MAX_CANDLE_FETCH_WORKERS)
            candles_map = fetch_candles_parallel(
                filtered_scan_pairs,
                timeframe=TIMEFRAME,
                limit=100,
                max_workers=MAX_CANDLE_FETCH_WORKERS,
            )
            htf_candles_map = fetch_candles_parallel(
                filtered_scan_pairs,
                timeframe=HTF_TIMEFRAME,
                limit=100,
                max_workers=MAX_CANDLE_FETCH_WORKERS,
            )
            htf_4h_candles_map = fetch_candles_parallel(
                filtered_scan_pairs,
                timeframe="4H",
                limit=80,
                max_workers=MAX_CANDLE_FETCH_WORKERS,
            )
            logger.info(f"Parallel HTF candle fetch completed: {len(htf_candles_map)} symbols")
            logger.info(f"Parallel candle fetch completed: {len(candles_map)} symbols")
            btc_15m_candles = get_candles("BTC-USDT-SWAP", TIMEFRAME, 100)
            btc_15m_df = to_dataframe(btc_15m_candles)
            # BTC 24h change is needed for relative_strength_24.
            # Use the ticker snapshot from this scan instead of relying on locals().
            try:
                btc_ticker_24h = next((p for p in scan_pairs if str(p.get("instId", "")) == "BTC-USDT-SWAP"), None)
                btc_change_24h = extract_24h_change_percent(btc_ticker_24h or {})
            except Exception:
                btc_change_24h = 0.0
            tested = 0
            sent_count = 0
            sent_symbols_this_run = set()
            candidates = []
            candidates_symbols = set()
            _reset_long_run_counters()
            no_candles_symbols_this_run = set()
            no_candles_cooldown_symbols_this_run = set()
            for pair_data in filtered_scan_pairs:
                final_threshold_min = None
                tested += 1
                symbol = pair_data["instId"]
                change_24h = extract_24h_change_percent(pair_data)
                if is_candle_temporarily_blocked(symbol, TIMEFRAME):
                    no_candles_cooldown_symbols_this_run.add(symbol)
                    if len(no_candles_cooldown_symbols_this_run) <= NO_CANDLES_LOG_SAMPLE_LIMIT:
                        logger.info(f"DATA ERROR | {symbol} | no_candles cooldown | main scan")
                    log_long_rejection(symbol=symbol, reason="data_error_no_candles_cooldown", candle_time=None, market_state=market_state, current_mode=current_mode,
                        extra={
                            "category": "data_error",
                            "original_reason": "no_candles",
                            "data_error_type": "temporary_cooldown",
                            "details": "Symbol temporarily skipped after repeated candle fetch failures"
                        })
                    continue
                candles = candles_map.get(symbol, [])
                if not candles:
                    time.sleep(0.4)
                    candles = get_candles(symbol, TIMEFRAME, 100)
                if not candles:
                    no_candles_symbols_this_run.add(symbol)
                    fail_count = record_candle_fetch_failure(symbol, TIMEFRAME)
                    log_long_rejection(symbol=symbol, reason="data_error_no_candles", candle_time=None, market_state=market_state, current_mode=current_mode,
                        extra={
                            "category": "data_error",
                            "original_reason": "no_candles",
                            "data_error_type": "empty_response_or_too_few",
                            "fail_count": fail_count,
                            "details": "Retry failed for main scan"
                        })
                    if len(no_candles_symbols_this_run) <= NO_CANDLES_LOG_SAMPLE_LIMIT:
                        logger.warning(f"DATA ERROR | {symbol} | no_candles | main scan | fail_count={fail_count}")
                    continue
                record_candle_fetch_success(symbol, TIMEFRAME)
                df = to_dataframe(candles)
                if df is None or df.empty:
                    log_long_rejection(symbol=symbol, reason="dataframe_empty", candle_time=None, market_state=market_state, current_mode=current_mode)
                    continue
                if not is_valid_candle_timing(df):
                    log_long_rejection(symbol=symbol, reason="invalid_candle_timing", candle_time=get_signal_candle_time(df), market_state=market_state, current_mode=current_mode)
                    logger.info(f"{symbol} --> skipped (candle timing invalid)")
                    continue
                candle_time = get_signal_candle_time(df)
                now = time.time()
                if symbol in last_candle_cache and last_candle_cache[symbol] == candle_time:
                    logger.info(f"{symbol} skipped: local same candle cache")
                    continue
                if symbol in sent_cache and now - sent_cache[symbol] < LOCAL_RECENT_SEND_SECONDS:
                    logger.info(f"{symbol} skipped: local recent send cache")
                    continue
                if already_sent_same_candle(symbol, candle_time, "long"):
                    logger.info(f"⏭ {symbol} skipped: duplicate candle")
                    continue
                if is_symbol_on_cooldown(symbol, "long"):
                    logger.info(f"⏭ {symbol} skipped: cooldown")
                    continue
                if symbol in sent_symbols_this_run:
                    continue
                if symbol in candidates_symbols:
                    continue
                early_signal = early_bullish_signal(df)
                pre_breakout = is_pre_breakout(df)
                breakout = is_breakout(df)
                mtf_confirmed = is_higher_timeframe_confirmed_from_candles(
                    htf_candles_map.get(symbol, [])
                )
                htf_1h_context = get_htf_context_from_candles(htf_candles_map.get(symbol, []))
                htf_4h_context = get_htf_context_from_candles(htf_4h_candles_map.get(symbol, []))
                is_new = is_new_listing_by_candles(candles)
                funding = funding_map.get(symbol, 0.0)
                vol_ratio = get_volume_ratio(df)
                dist_ma = get_distance_from_ma_percent(df)
                candle_strength = get_candle_strength_ratio(df)
                gaining_strength = is_gaining_intraday_strength(df)
                signal_row = get_signal_row(df)
                atr_value = _safe_float(signal_row.get("atr"), 0) if signal_row is not None else 0.0
                rsi_now = _safe_float(signal_row.get("rsi"), 50) if signal_row is not None else 50.0
                vwap_distance = get_vwap_distance_percent(df)
                rsi_slope = get_rsi_slope(df)
                macd_hist = _safe_float(signal_row.get("macd_hist"), 0.0) if signal_row is not None else 0.0
                macd_hist_slope = get_macd_hist_slope(df)
                above_upper_bb = is_above_upper_bollinger(df)
                change_4h = get_change_4h(df)
                breakout_quality = get_breakout_quality(df, vol_ratio) if breakout else "none"
                falling_knife_data = detect_falling_knife_risk(
                    df=df,
                    dist_ma=dist_ma,
                    change_24h=change_24h,
                    vol_ratio=vol_ratio,
                )
                is_reverse = is_oversold_reversal_long(
                    df=df,
                    dist_ma=dist_ma,
                    change_24h=change_24h,
                    vol_ratio=vol_ratio,
                    funding=funding,
                )
                reversal_4h_result = {"confirmed": False, "checks": 0, "details": ""}
                if is_reverse:
                    reversal_4h_result = is_4h_oversold_confirmed(symbol)
                early_priority = "none"
                if early_signal and not is_reverse:
                    early_priority = classify_early_priority_long(
                        early_signal=early_signal,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        dist_ma=dist_ma,
                        vol_ratio=vol_ratio,
                        candle_strength=candle_strength,
                        mtf_confirmed=mtf_confirmed,
                        gaining_strength=gaining_strength,
                        market_state=market_state,
                    )
                temp_opportunity_type = classify_opportunity_type_long(
                    is_reverse=is_reverse,
                    pre_breakout=pre_breakout,
                    breakout=breakout,
                    dist_ma=dist_ma,
                    mtf_confirmed=mtf_confirmed,
                )
                # Default setup_type before final setup construction.
                # STRONG_LONG_ONLY guards run before final build_setup_type(), so keep this safe.
                setup_type = str(temp_opportunity_type or "unknown")
                extra_setups = detect_extra_strong_long_setups(
                    symbol=symbol,
                    df=df,
                    btc_df=btc_15m_df,
                    dist_ma=dist_ma,
                    rsi_now=rsi_now,
                    vol_ratio=vol_ratio,
                    mtf_confirmed=mtf_confirmed,
                    atr_value=atr_value,
                )
                has_extra_strong_setup = bool(extra_setups.get("has_extra_setup"))
                extra_setup_names = extra_setups.get("setups", [])
                extra_setup_bonus = float(extra_setups.get("score_bonus", 0.0) or 0.0)
                primary_extra_setup = extra_setups.get("primary_setup", "")
                context_setups = extra_setups.get("context_setups", [])
                # Canonical execution tags are built early so pre-score guards and
                # final execution gates read the same setup source.
                early_execution_setup_tags = build_execution_setup_tags(
                    symbol=symbol,
                    setup_type=temp_opportunity_type,
                    primary_extra_setup=primary_extra_setup,
                    extra_setup_names=extra_setup_names,
                    extra_setups=extra_setup_names,
                    extra_setups_details=extra_setups.get("details", {}),
                    context_setups=context_setups,
                    wave_context="",
                    wave_estimate=0,
                    relative_strength_vs_btc=(
                        "relative_strength_vs_btc" in [str(x) for x in (extra_setup_names or [])]
                        or str(primary_extra_setup or "") == "relative_strength_vs_btc"
                    ),
                )

                if logger.isEnabledFor(logging.DEBUG) and (primary_extra_setup or extra_setup_names):
                    for _must_tag in (primary_extra_setup, *(extra_setup_names or [])):
                        _must_norm = _normalize_execution_tag(_must_tag)
                        if _must_norm and _must_norm not in set(early_execution_setup_tags):
                            logger.debug(f"EXEC TAG MISS | {symbol} | missing={_must_norm} | early_tags={early_execution_setup_tags}")

                # SAFE DEFAULTS for softening / warning blocks.
                # These variables are used only as local flags before score_result exists.
                # Always initialize them before any guard can append/read them.
                entry_warning = False
                warning_reasons = []
                softening_applied = False
                soft_warning = False
                late_warning = False

                # Conservative pre-score strong setup flag used only to soften RSI/MACD guards
                # in bull/alt-season + MTF/volume contexts. It does not open weak setups.
                relaxed_pre_score_setup = is_relaxed_execution_setup(
                    setup_type=temp_opportunity_type,
                    extra_setup_names=extra_setup_names,
                    primary_extra_setup=primary_extra_setup,
                    execution_setup_tags=early_execution_setup_tags,
                    mtf_confirmed=mtf_confirmed,
                    vol_ratio=vol_ratio,
                    score=0.0,
                ) or bool(has_extra_strong_setup)

                _preliminary_entry_timing = classify_entry_timing_long(
                    dist_ma=dist_ma,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    vol_ratio=vol_ratio,
                    rsi_now=rsi_now,
                    candle_strength=candle_strength,
                    late_pump_risk=False,
                    entry_maturity_data={},
                )
                late_guard = get_late_pump_risk(
                    market_state=market_state,
                    opportunity_type=temp_opportunity_type,
                    dist_ma=dist_ma,
                    rsi_now=rsi_now,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    breakout_quality=breakout_quality,
                    change_4h=change_4h,
                    vwap_distance=vwap_distance,
                    rsi_slope=rsi_slope,
                    macd_hist_slope=macd_hist_slope,
                    entry_timing=_preliminary_entry_timing,
                    alt_mode=alt_mode,
                )
                if late_guard.get("should_block") and not is_reverse:
                    _late_reasons_text = "|".join(str(x) for x in (late_guard.get("reasons", []) or [])).lower()
                    _hard_late_block = any(token in _late_reasons_text for token in (
                        "danger", "hard_late", "extreme", "wave_5", "موجة 5", "موجة خامسة",
                        "متأخر جدًا", "very_late", "overextended", "crash", "dump"
                    ))
                    _normal_flow_late_warning_only = (
                        current_mode == MODE_NORMAL_LONG
                        and relaxed_pre_score_setup
                        and mtf_confirmed
                        and float(vol_ratio or 0.0) >= 1.05
                        and market_state in ("bull_market", "alt_season")
                        and "صاعد" in str(btc_mode or "")
                        and not _hard_late_block
                    )
                    if _normal_flow_late_warning_only:
                        late_guard["should_block"] = False
                        late_guard.setdefault("warnings", [])
                        if isinstance(late_guard.get("warnings"), list):
                            late_guard["warnings"].append("normal_flow_late_warning_only")
                        logger.info(
                            f"⚠️ {symbol} late_guard warning only | normal_flow=True | "
                            f"reasons={late_guard.get('reasons', [])}"
                        )
                    else:
                        log_long_rejection(
                            symbol=symbol,
                            reason="late_guard_should_block",
                            candle_time=candle_time,
                            market_state=market_state,
                            current_mode=current_mode,
                            entry_timing="",
                            opportunity_type=temp_opportunity_type,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                            extra={
                                "late_guard_reasons": late_guard.get("reasons", []),
                                "has_extra_strong_setup": has_extra_strong_setup,
                                "extra_setup_names": extra_setup_names,
                                "primary_extra_setup": primary_extra_setup,
                                "extra_setup_bonus": extra_setup_bonus,
                            },
                        )
                        logger.info(f"{symbol} --> skipped by late_guard should_block: {late_guard.get('reasons', [])}")
                        continue
                entry_maturity_data = {
                    "fib_position": "unknown",
                    "fib_position_ratio": 0.0,
                    "fib_label": "غير معروف",
                    "had_pullback": False,
                    "pullback_pct": 0.0,
                    "pullback_label": "غير معروف",
                    "wave_estimate": 0,
                    "wave_peaks": 0,
                    "wave_label": "غير معروف",
                    "entry_maturity": "unknown",
                    "maturity_penalty": 0.0,
                    "maturity_bonus": 0.0,
                    "block_signal": False,
                    "warning_reasons": [],
                }
                if analyze_entry_maturity is not None:
                    try:
                        entry_maturity_data = analyze_entry_maturity(df)
                    except Exception as e:
                        logger.warning(f"{symbol} --> entry maturity analysis failed: {e}")
                if entry_maturity_data.get("block_signal"):
                    # Final softening for strong continuation in bullish/MTF context.
                    # This prevents good continuation setups from being killed early as danger_late,
                    # while keeping true wave-5/overextended weak setups blocked.
                    _maturity_text = "|".join(str(entry_maturity_data.get(k, "") or "") for k in ("entry_maturity", "fib_label", "pullback_label", "wave_label"))
                    _maturity_text_l = _maturity_text.lower()
                    _maturity_wave = int(float(entry_maturity_data.get("wave_estimate", 0) or 0))
                    _maturity_had_pullback = bool(entry_maturity_data.get("had_pullback", False))
                    _hard_maturity_block = (
                        _maturity_wave >= 5
                        or "موجة 5" in _maturity_text
                        or "موجة خامسة" in _maturity_text
                        or "danger" in _maturity_text_l
                        or "hard_late" in _maturity_text_l
                        or "متأخر جدًا" in _maturity_text
                    ) and not _maturity_had_pullback
                    late_strong_continuation_soften = (
                        current_mode == MODE_NORMAL_LONG
                        and not is_reverse
                        and market_state in ("bull_market", "alt_season")
                        and mtf_confirmed
                        and vol_ratio >= 1.05
                        and dist_ma < 3.8
                        and (
                            breakout
                            or pre_breakout
                            or breakout_quality in ("strong", "good", "ok")
                            or relaxed_pre_score_setup
                            or has_extra_strong_setup
                        )
                        and not _hard_maturity_block
                    )

                    if late_strong_continuation_soften:
                        entry_maturity_data["block_signal"] = False
                        entry_maturity_data["maturity_penalty"] = min(
                            float(entry_maturity_data.get("maturity_penalty", 0.0) or 0.0),
                            0.25,
                        )
                        if not isinstance(entry_maturity_data.get("warning_reasons"), list):
                            entry_maturity_data["warning_reasons"] = []
                        entry_maturity_data["warning_reasons"].append(
                            "دخول متأخر لكن Bull+MTF+Volume قوي؛ تحذير بدل رفض"
                        )
                        logger.info(
                            f"{symbol} --> entry maturity block softened to warning "
                            f"(bull/MTF/strong continuation, dist_ma={dist_ma:.2f}, "
                            f"vol={vol_ratio:.2f}, setup={primary_extra_setup or setup_type})"
                        )
                    elif not is_reverse and not pre_breakout and not (
                        breakout_quality == "strong"
                        and mtf_confirmed
                        and vol_ratio >= 1.8
                    ):
                        log_long_rejection(
                            symbol=symbol,
                            reason="entry_maturity_block",
                            candle_time=candle_time,
                            market_state=market_state,
                            current_mode=current_mode,
                            entry_timing=entry_maturity_data.get("entry_maturity", ""),
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                            extra={
                                "entry_maturity": entry_maturity_data.get("entry_maturity"),
                                "fib_position": entry_maturity_data.get("fib_position"),
                                "wave_estimate": entry_maturity_data.get("wave_estimate"),
                                "has_extra_strong_setup": has_extra_strong_setup,
                                "extra_setup_names": extra_setup_names,
                                "primary_extra_setup": primary_extra_setup,
                                "extra_setup_bonus": extra_setup_bonus,
                            },
                        )
                        logger.info(f"{symbol} → skipped by entry maturity guard: {entry_maturity_data}")
                        continue
                if market_state == "bull_market" and not is_reverse:
                    # v201: VWAP extension in bull market is normally a warning/score issue.
                    # Hard reject only when there is no strong/whitelisted context to justify continuation.
                    _vwap_soft_context = bool(
                        relaxed_pre_score_setup
                        or has_extra_strong_setup
                        or mtf_confirmed
                        or primary_extra_setup
                        or extra_setup_names
                    )
                    if vwap_distance >= 2.4 and not breakout and not pre_breakout and not _vwap_soft_context:
                        log_long_rejection(
                            symbol=symbol,
                            reason="vwap_overextended_bull_market",
                            candle_time=candle_time,
                            market_state=market_state,
                            current_mode=current_mode,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                            extra={
                                "has_extra_strong_setup": has_extra_strong_setup,
                                "extra_setup_names": extra_setup_names,
                                "primary_extra_setup": primary_extra_setup,
                                "extra_setup_bonus": extra_setup_bonus,
                            },
                        )
                        continue
                    if vwap_distance >= 2.8 and breakout_quality != "strong" and not pre_breakout and not _vwap_soft_context:
                        log_long_rejection(
                            symbol=symbol,
                            reason="vwap_overextended_bull_market",
                            candle_time=candle_time,
                            market_state=market_state,
                            current_mode=current_mode,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                            extra={"breakout_quality": breakout_quality,
                                   "has_extra_strong_setup": has_extra_strong_setup,
                                   "extra_setup_names": extra_setup_names,
                                   "primary_extra_setup": primary_extra_setup,
                                   "extra_setup_bonus": extra_setup_bonus},
                        )
                        continue
                if not is_reverse:
                    # v127: keep RSI momentum/slope weakness as a hard block only for weak setups.
                    # Strong, unified setup tags in NORMAL/STRONG modes should not be silenced by
                    # rsi_momentum_weak when the broader market context or MTF is supportive.
                    _pre_score_market_supportive = (
                        market_state in ("bull_market", "alt_season")
                        or "صاعد" in str(btc_mode or "")
                        or "قوي" in str(alt_mode or "")
                        or "🟢" in str(btc_mode or "")
                        or "🟢" in str(alt_mode or "")
                        or current_mode == MODE_STRONG_LONG_ONLY
                    )
                    bull_mtf_strong_soften = (
                        current_mode in (MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY)
                        and (
                            relaxed_pre_score_setup
                            or has_extra_strong_setup
                            or breakout
                            or primary_extra_setup
                            or bool(extra_setup_names)
                        )
                        and vol_ratio >= (1.00 if current_mode == MODE_STRONG_LONG_ONLY else 1.05)
                        and (mtf_confirmed or _pre_score_market_supportive)
                    )
                    _normal_momentum_soften = (
                        current_mode == MODE_NORMAL_LONG
                        and _pre_score_market_supportive
                        and vol_ratio >= 1.10
                        and dist_ma < 4.6
                        and not late_guard.get("extreme_late_pump", False)
                    )
                    if _normal_momentum_soften and not bull_mtf_strong_soften:
                        bull_mtf_strong_soften = True

                    if (
                        rsi_now >= 67
                        and rsi_slope <= 0
                        and dist_ma >= 3.4
                        and not pre_breakout
                        and breakout_quality != "strong"
                    ):
                        if bull_mtf_strong_soften:
                            logger.info(
                                f"{symbol} --> RSI momentum weak softened to warning "
                                f"(bull/MTF/strong setup, rsi={rsi_now:.1f}, slope={rsi_slope:.2f})"
                            )
                            entry_warning = True
                            softening_applied = True
                            soft_warning = True
                            warning_reasons.append("RSI momentum weak لكن Bull+MTF+setup قوي؛ تم السماح مع تحذير")
                        else:
                            log_long_rejection(
                                symbol=symbol,
                                reason="rsi_momentum_weak",
                                candle_time=candle_time,
                                market_state=market_state,
                                current_mode=current_mode,
                                dist_ma=dist_ma,
                                rsi_now=rsi_now,
                                vol_ratio=vol_ratio,
                                vwap_distance=vwap_distance,
                                mtf_confirmed=mtf_confirmed,
                                breakout=breakout,
                                pre_breakout=pre_breakout,
                                is_reverse=is_reverse,
                                extra={"rsi_slope": rsi_slope, "breakout_quality": breakout_quality,
                                       "has_extra_strong_setup": has_extra_strong_setup,
                                       "extra_setup_names": extra_setup_names,
                                       "primary_extra_setup": primary_extra_setup,
                                       "extra_setup_bonus": extra_setup_bonus,
                                       "execution_setup_tags": early_execution_setup_tags,
                                       "pre_score_market_supportive": locals().get("_pre_score_market_supportive", False),
                                       "soften_allowed": False},
                            )
                            continue
                    if rsi_slope < -2.5 and not breakout and not pre_breakout:
                        if bull_mtf_strong_soften:
                            logger.info(
                                f"{symbol} --> RSI slope weak softened to warning "
                                f"(bull/MTF/strong setup, slope={rsi_slope:.2f})"
                            )
                            entry_warning = True
                            softening_applied = True
                            soft_warning = True
                            warning_reasons.append("RSI slope ضعيف لكن Bull+MTF+setup قوي؛ تم السماح مع تحذير")
                        else:
                            log_long_rejection(
                                symbol=symbol,
                                reason="rsi_momentum_weak",
                                candle_time=candle_time,
                                market_state=market_state,
                                current_mode=current_mode,
                                dist_ma=dist_ma,
                                rsi_now=rsi_now,
                                vol_ratio=vol_ratio,
                                vwap_distance=vwap_distance,
                                mtf_confirmed=mtf_confirmed,
                                breakout=breakout,
                                pre_breakout=pre_breakout,
                                is_reverse=is_reverse,
                                extra={"rsi_slope": rsi_slope,
                                       "has_extra_strong_setup": has_extra_strong_setup,
                                       "extra_setup_names": extra_setup_names,
                                       "primary_extra_setup": primary_extra_setup,
                                       "extra_setup_bonus": extra_setup_bonus,
                                       "execution_setup_tags": early_execution_setup_tags,
                                       "pre_score_market_supportive": locals().get("_pre_score_market_supportive", False),
                                       "soften_allowed": False},
                            )
                            continue
                if not is_reverse:
                    if macd_hist < 0 and not breakout and not pre_breakout:
                        if (
                            (
                                market_state in ("bull_market", "alt_season")
                                or "صاعد" in str(btc_mode or "")
                                or "قوي" in str(alt_mode or "")
                                or current_mode == MODE_STRONG_LONG_ONLY
                            )
                            and (
                                mtf_confirmed
                                or relaxed_pre_score_setup
                                or has_extra_strong_setup
                                or primary_extra_setup
                                or bool(extra_setup_names)
                            )
                            and vol_ratio >= 1.05
                            and dist_ma < 4.8
                        ):
                            logger.info(
                                f"{symbol} --> MACD negative softened to warning "
                                f"(bull/MTF/strong setup, macd_hist={macd_hist:.6f})"
                            )
                            entry_warning = True
                            softening_applied = True
                            soft_warning = True
                            warning_reasons.append("MACD سلبي بسيط لكن Bull+MTF+setup قوي؛ تم السماح مع تحذير")
                        else:
                            log_long_rejection(
                                symbol=symbol,
                                reason="macd_negative",
                                candle_time=candle_time,
                                market_state=market_state,
                                current_mode=current_mode,
                                dist_ma=dist_ma,
                                rsi_now=rsi_now,
                                vol_ratio=vol_ratio,
                                vwap_distance=vwap_distance,
                                mtf_confirmed=mtf_confirmed,
                                breakout=breakout,
                                pre_breakout=pre_breakout,
                                is_reverse=is_reverse,
                                extra={"macd_hist": macd_hist,
                                       "has_extra_strong_setup": has_extra_strong_setup,
                                       "extra_setup_names": extra_setup_names,
                                       "primary_extra_setup": primary_extra_setup,
                                       "extra_setup_bonus": extra_setup_bonus,
                                       "soften_allowed": False},
                            )
                            continue
                    if (
                        macd_hist_slope < 0
                        and rsi_now >= 66
                        and dist_ma >= 3.4
                        and not pre_breakout
                        and breakout_quality != "strong"
                    ):
                        # v201: falling MACD after a move is a hard reject only for real chase danger.
                        _macd_hard_danger = (
                            dist_ma >= 5.0
                            and rsi_now >= 70
                            and vol_ratio >= 1.80
                            and not mtf_confirmed
                            and not relaxed_pre_score_setup
                            and not has_extra_strong_setup
                        )
                        if _macd_hard_danger:
                            log_long_rejection(
                                symbol=symbol,
                                reason="macd_momentum_falling",
                                candle_time=candle_time,
                                market_state=market_state,
                                current_mode=current_mode,
                                dist_ma=dist_ma,
                                rsi_now=rsi_now,
                                vol_ratio=vol_ratio,
                                vwap_distance=vwap_distance,
                                mtf_confirmed=mtf_confirmed,
                                breakout=breakout,
                                pre_breakout=pre_breakout,
                                is_reverse=is_reverse,
                                extra={"macd_hist_slope": macd_hist_slope, "macd_hist": macd_hist,
                                       "has_extra_strong_setup": has_extra_strong_setup,
                                       "extra_setup_names": extra_setup_names,
                                       "primary_extra_setup": primary_extra_setup,
                                       "extra_setup_bonus": extra_setup_bonus},
                            )
                            continue
                        entry_warning = True
                        softening_applied = True
                        soft_warning = True
                        warning_reasons.append("MACD momentum falling؛ تحذير بدل رفض إلا في المطاردة الخطرة")
                trap_check = is_momentum_exhaustion_trap(
                    market_state=market_state,
                    opportunity_type=temp_opportunity_type,
                    dist_ma=dist_ma,
                    vwap_distance=vwap_distance,
                    rsi_now=rsi_now,
                    rsi_slope=rsi_slope,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                    macd_hist=macd_hist,
                    macd_hist_slope=macd_hist_slope,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    breakout_quality=breakout_quality,
                    is_reverse=is_reverse,
                )
                if trap_check["is_trap"] and not is_reverse:
                    log_long_rejection(
                        symbol=symbol,
                        reason="momentum_exhaustion_trap",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={"trap_reasons": trap_check["reasons"], "checks": trap_check["checks"],
                               "has_extra_strong_setup": has_extra_strong_setup,
                               "extra_setup_names": extra_setup_names,
                               "primary_extra_setup": primary_extra_setup,
                               "extra_setup_bonus": extra_setup_bonus},
                    )
                    continue
                entry_timing_temp = classify_entry_timing_long(
                    dist_ma=dist_ma,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    vol_ratio=vol_ratio,
                    rsi_now=rsi_now,
                    candle_strength=candle_strength,
                    late_pump_risk=late_guard.get("late_pump_risk", False),
                    entry_maturity_data=entry_maturity_data,
                )
                guard = evaluate_late_breakout_guard(
                    df=df,
                    breakout=breakout,
                    entry_timing=entry_timing_temp,
                    dist_ma=dist_ma,
                    vol_ratio=vol_ratio,
                    rsi_now=rsi_now,
                    mtf_confirmed=mtf_confirmed,
                    market_state=market_state,
                    breakout_quality=breakout_quality,
                    pre_breakout=pre_breakout,
                    is_reverse=is_reverse,
                )
                if guard["blocked"]:
                    log_long_rejection(
                        symbol=symbol,
                        reason="late_breakout_guard_blocked",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        entry_timing=entry_timing_temp,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={"guard_reason": guard["reason"], "upper_wick_ratio": guard["upper_wick_ratio"],
                               "has_extra_strong_setup": has_extra_strong_setup,
                               "extra_setup_names": extra_setup_names,
                               "primary_extra_setup": primary_extra_setup,
                               "extra_setup_bonus": extra_setup_bonus},
                    )
                    logger.info(f"{symbol} --> late breakout guard BLOCKED: {guard['reason']}")
                    continue
                if guard["retest_required"]:
                    log_long_rejection(
                        symbol=symbol,
                        reason="retest_required",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        entry_timing=entry_timing_temp,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={"guard_reason": guard["reason"], "upper_wick_ratio": guard["upper_wick_ratio"],
                               "has_extra_strong_setup": has_extra_strong_setup,
                               "extra_setup_names": extra_setup_names,
                               "primary_extra_setup": primary_extra_setup,
                               "extra_setup_bonus": extra_setup_bonus},
                    )
                    logger.info(f"{symbol} --> retest required: {guard['reason']} (skipped direct alert)")
                    continue

                strong_bull_pullback = (
                    market_state in ("bull_market", "alt_season")
                    and mtf_confirmed
                    and vol_ratio >= 1.35
                    and rsi_now <= 66
                    and dist_ma <= 3.2
                    and candle_strength >= 0.45
                    and gaining_strength
                    and (not late_guard["late_pump_risk"])
                    and (not late_guard["extreme_late_pump"])
                    and (not trap_check["is_trap"])
                    and vwap_distance <= 2.2
                    and (macd_hist >= 0 or macd_hist_slope >= 0)
                )

                strong_breakout_exception = (
                    breakout
                    and breakout_quality == "strong"
                    and mtf_confirmed
                    and vol_ratio >= 1.8
                    and rsi_now <= 66
                    and dist_ma <= 3.8
                    and vwap_distance <= 2.2
                )

                pre_score_adjustments_log = []
                wave5_eval = {}

                if not is_reverse:
                    entry_maturity_status = str(entry_maturity_data.get("entry_maturity", "unknown") or "unknown")
                    had_pullback = bool(entry_maturity_data.get("had_pullback", False))
                    fib_position = str(entry_maturity_data.get("fib_position", "unknown") or "unknown")
                    wave_estimate = int(entry_maturity_data.get("wave_estimate", 0) or 0)

                    healthy_pullback_context = (
                        entry_maturity_status == "healthy"
                        and had_pullback
                        and "صحي" in str(entry_timing_temp)
                    )

                    hard_late_entry = (
                        not healthy_pullback_context
                        and (
                            "متأخر جدًا" in str(entry_timing_temp)
                            or "موجة خامسة" in str(entry_timing_temp)
                            or "امتداد سعري" in str(entry_timing_temp)
                            or "نهاية موجة" in str(entry_timing_temp)
                            or (
                                fib_position == "overextended"
                                and wave_estimate >= 5
                                and not had_pullback
                            )
                        )
                    )

                    if hard_late_entry and (breakout or pre_breakout):
                        if not (dist_ma >= 4.8 and vol_ratio >= 2.0):
                            hard_late_entry = False
                            pre_score_adjustments_log.append({
                                "name": "late_breakout_warning_penalty",
                                "value": -0.25,
                                "reason": "late_entry_breakout_caution"
                            })
                            if not isinstance(entry_maturity_data.get("warning_reasons"), list):
                                entry_maturity_data["warning_reasons"] = []
                            entry_maturity_data["warning_reasons"].append("اختراق متأخر، يُنصح بالحذر")
                            logger.info(
                                f"{symbol} --> late breakout caution (no hard block): "
                                f"dist_ma={dist_ma:.2f}, vol_ratio={vol_ratio:.2f}"
                            )

                    elif hard_late_entry and (
                        "متأخر جدًا" in str(entry_timing_temp)
                        or "مطاردة حركة" in str(entry_timing_temp)
                    ):
                        _late_cond_count = sum([
                            dist_ma >= 4.2,
                            rsi_now >= 67,
                            vol_ratio >= 1.8,
                            candle_strength >= 0.62,
                            "ضعيف" in str(alt_mode),
                        ])
                        if _late_cond_count < 2:
                            hard_late_entry = False
                            pre_score_adjustments_log.append({
                                "name": "late_entry_conditional_penalty",
                                "value": -0.30,
                                "reason": "late_entry_conditional_override"
                            })
                            if not isinstance(entry_maturity_data.get("warning_reasons"), list):
                                entry_maturity_data["warning_reasons"] = []
                            entry_maturity_data["warning_reasons"].append("دخول متأخر تحت الحد الحرج")
                            logger.info(
                                f"{symbol} --> late_entry conditional override "
                                f"(conds={_late_cond_count}/5, no hard block): "
                                f"dist_ma={dist_ma:.2f}, rsi={rsi_now:.1f}, "
                                f"vol={vol_ratio:.2f}, cs={candle_strength:.2f}"
                            )

                    elif hard_late_entry:
                        hard_late_entry = False
                        pre_score_adjustments_log.append({
                            "name": "simple_late_entry_penalty",
                            "value": -0.30,
                            "reason": "simple_late_entry"
                        })
                        if not isinstance(entry_maturity_data.get("warning_reasons"), list):
                            entry_maturity_data["warning_reasons"] = []
                        entry_maturity_data["warning_reasons"].append("دخول متأخر نسبي")
                        logger.info(
                            f"{symbol} --> simple late entry → penalty only "
                            f"(entry_timing={entry_timing_temp})"
                        )

                    if hard_late_entry:
                        wave5_eval = evaluate_wave5_htf_override(
                            entry_timing=entry_timing_temp,
                            entry_maturity_data=entry_maturity_data,
                            dist_ma=dist_ma,
                            htf_1h_context=htf_1h_context,
                            htf_4h_context=htf_4h_context,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            breakout_quality=breakout_quality,
                            mtf_confirmed=mtf_confirmed,
                            vol_ratio=vol_ratio,
                        )
                        if wave5_eval["can_override"]:
                            pre_score_adjustments_log.append({
                                "name": "wave5_htf_override_penalty",
                                "value": -wave5_eval["penalty"],
                                "reason": wave5_eval["label"]
                            })
                            warning_msg = "15m wave late but HTF healthy" if "htf_healthy" in wave5_eval["label"] else "15m wave late but breakout confirmed"
                            if "warning_reasons" not in entry_maturity_data:
                                entry_maturity_data["warning_reasons"] = []
                            entry_maturity_data["warning_reasons"].append(warning_msg)
                            logger.info(f"{symbol} --> wave5 overridden: {wave5_eval['label']}, penalty={wave5_eval['penalty']}")
                        else:
                            reason_specific = wave5_eval["label"] or classify_hard_late_rejection_reason(
                                entry_timing=entry_timing_temp,
                                entry_maturity_status=entry_maturity_status,
                                had_pullback=had_pullback,
                                fib_position=fib_position,
                                wave_estimate=wave_estimate,
                                dist_ma=dist_ma,
                                rsi_now=rsi_now,
                                vol_ratio=vol_ratio,
                                vwap_distance=vwap_distance,
                                breakout=breakout,
                                pre_breakout=pre_breakout,
                                mtf_confirmed=mtf_confirmed,
                            )
                            log_long_rejection(
                                symbol=symbol,
                                reason=reason_specific,
                                candle_time=candle_time,
                                market_state=market_state,
                                current_mode=current_mode,
                                entry_timing=entry_timing_temp,
                                opportunity_type=temp_opportunity_type,
                                dist_ma=dist_ma,
                                rsi_now=rsi_now,
                                vol_ratio=vol_ratio,
                                vwap_distance=vwap_distance,
                                mtf_confirmed=mtf_confirmed,
                                breakout=breakout,
                                pre_breakout=pre_breakout,
                                is_reverse=is_reverse,
                                extra={
                                    "entry_maturity": entry_maturity_status,
                                    "had_pullback": had_pullback,
                                    "fib_position": fib_position,
                                    "wave_estimate": wave_estimate,
                                    "breakout_quality": breakout_quality,
                                    "htf_1h_context": htf_1h_context,
                                    "htf_4h_context": htf_4h_context,
                                    "has_extra_strong_setup": has_extra_strong_setup,
                                    "extra_setup_names": extra_setup_names,
                                    "primary_extra_setup": primary_extra_setup,
                                    "extra_setup_bonus": extra_setup_bonus,
                                },
                            )
                            logger.info(
                                f"{symbol} --> rejected by {reason_specific} "
                                f"(entry_timing={entry_timing_temp}, entry_maturity={entry_maturity_status}, "
                                f"had_pullback={had_pullback}, fib={fib_position}, wave={wave_estimate}, "
                                f"score_pre=not_calculated, dist_ma={dist_ma:.2f}, rsi={rsi_now:.1f}, "
                                f"vol={vol_ratio:.2f}, vwap={vwap_distance:.2f})"
                            )
                            continue

                breakout_warning = guard.get("warning", "")
                upper_wick_ratio = guard.get("upper_wick_ratio", 0.0)
                late_breakout_guard_reason = guard.get("reason", "none") or "none"

                # تعريف القيم الافتراضية لـ execution مبكرًا
                execution_entry = None
                execution_sl = None
                execution_tp1 = None
                execution_tp2 = None
                execution_risk = None

                _strong_signal_tags = set()
                for _tag in (extra_setup_names or []):
                    if _tag:
                        _strong_signal_tags.add(str(_tag))
                for _tag in (primary_extra_setup, setup_type, setup_type_base if 'setup_type_base' in locals() else ''):
                    if _tag:
                        _strong_signal_tags.add(str(_tag))
                _tier_a_strong_setup = bool(_strong_signal_tags & STRONG_SIGNAL_TIER_A_SETUPS)
                _tier_b_strong_setup = bool(_strong_signal_tags & STRONG_SIGNAL_TIER_B_SETUPS)
                _tier_b_quality_ok = (
                    _tier_b_strong_setup
                    and vol_ratio >= 1.10
                    and rsi_now >= 47
                    and dist_ma <= 4.2
                    and (mtf_confirmed or candle_strength >= 0.42 or gaining_strength)
                )

                # STRONG_LONG_ONLY soft admission: allow high-quality continuation /
                # healthy pullback signals to pass the signal gate without changing
                # the execution whitelist / Elite path.
                _entry_timing_text = str(entry_timing_temp or "")
                _setup_text_for_continuation = "|".join(str(x) for x in _strong_signal_tags).lower()
                _opportunity_text = str(temp_opportunity_type or "").lower()
                _continuation_context_ok = (
                    "continuation" in _setup_text_for_continuation
                    or "استمرار" in _opportunity_text
                    or "continuation" in _opportunity_text
                    or "higher_low_continuation" in _strong_signal_tags
                )
                _not_chasing_late = not (
                    "مطاردة" in _entry_timing_text
                    or "متأخر جدًا" in _entry_timing_text
                    or "late_pump" in _entry_timing_text.lower()
                )
                _strong_continuation_quality_ok = (
                    _continuation_context_ok
                    and _not_chasing_late
                    and vol_ratio >= 1.15
                    and rsi_now >= 47
                    and dist_ma <= 4.5
                    and (mtf_confirmed or candle_strength >= 0.42 or gaining_strength)
                )
                _healthy_pullback_quality_ok = (
                    entry_maturity_status == "healthy"
                    and had_pullback
                    and _not_chasing_late
                    and vol_ratio >= 1.10
                    and rsi_now >= 45
                    and dist_ma <= 4.8
                    and (mtf_confirmed or candle_strength >= 0.40 or gaining_strength)
                )
                _additional_valid_strong_setup = (
                    _tier_a_strong_setup
                    or _tier_b_quality_ok
                    or _strong_continuation_quality_ok
                    or _healthy_pullback_quality_ok
                )

                if current_mode == MODE_STRONG_LONG_ONLY:
                    if "هابط" in btc_mode and "ضعيف" in alt_mode:
                        final_threshold_min = 7.4
                    else:
                        final_threshold_min = 6.0

                    if not (
                        breakout or pre_breakout or early_priority == "strong"
                        or strong_bull_pullback or strong_breakout_exception
                        or has_extra_strong_setup
                        or _additional_valid_strong_setup
                        or (is_reverse and reversal_4h_result.get("confirmed"))
                    ):
                        log_long_rejection(
                            symbol=symbol,
                            reason="strong_only_no_valid_setup",
                            candle_time=candle_time,
                            market_state=market_state,
                            current_mode=current_mode,
                            entry_timing=entry_timing_temp,
                            opportunity_type=temp_opportunity_type,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                            secondary_reasons=[
                                "early_priority: " + early_priority,
                                "breakout_quality: " + breakout_quality,
                                "has_extra_strong_setup: " + str(has_extra_strong_setup),
                                "additional_valid_strong_setup: " + str(_additional_valid_strong_setup),
                                "strong_continuation_quality_ok: " + str(_strong_continuation_quality_ok),
                                "healthy_pullback_quality_ok: " + str(_healthy_pullback_quality_ok),
                                "strong_signal_tags: " + ",".join(sorted(_strong_signal_tags)),
                                "strong_bull_pullback: " + str(strong_bull_pullback),
                            ],
                            extra={
                                "early_priority": early_priority,
                                "strong_bull_pullback": strong_bull_pullback,
                                "breakout_quality": breakout_quality,
                                "has_extra_strong_setup": has_extra_strong_setup,
                                "extra_setup_names": extra_setup_names,
                                "primary_extra_setup": primary_extra_setup,
                                "extra_setup_bonus": extra_setup_bonus,
                            },
                        )
                        logger.info(f"{symbol} --> skipped (STRONG_LONG_ONLY: no valid strong setup)")
                        continue

                    setup_match = any(
                        s in (extra_setup_names or []) + [primary_extra_setup] + [setup_type]
                        for s in STRONG_ONLY_ALLOWED_SETUPS
                    )
                    if not setup_match:
                        logger.info(
                            f"{symbol} --> tracking only: setup not in execution whitelist; "
                            "gate will check elite"
                        )

                    if vol_ratio < STRONG_ONLY_MIN_VOL_RATIO and not has_extra_strong_setup:
                        log_long_rejection(
                            symbol=symbol,
                            reason="strong_only_low_volume",
                            candle_time=candle_time,
                            market_state=market_state,
                            current_mode=current_mode,
                            entry_timing=entry_timing_temp,
                            opportunity_type=temp_opportunity_type,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                        )
                        logger.info(f"{symbol} --> skipped (STRONG_LONG_ONLY: vol_ratio too low)")
                        continue

                    extra_setup_can_bypass_mtf = (
                        primary_extra_setup in (
                            "failed_breakdown_trap",
                            "retest_breakout_confirmed",
                            "vwap_reclaim",
                            "support_bounce_confirmed",
                        )
                        and vol_ratio >= 1.20
                        and rsi_now >= 48
                        and dist_ma <= 3.2
                    )
                    if not mtf_confirmed and not extra_setup_can_bypass_mtf and not (is_reverse and reversal_4h_result.get("confirmed")):
                        logger.info(
                            f"{symbol} --> STRONG_LONG_ONLY: MTF not confirmed; "
                            "signal remains allowed, execution gate will decide"
                        )

                signal_idx = signal_row.name
                lookback_start = max(0, signal_idx - 20)
                recent_high = _safe_float(df["high"].iloc[lookback_start:signal_idx].max(), 0)
                if atr_value > 0 and recent_high > 0:
                    pullback_low = recent_high - (atr_value * 0.15)
                    pullback_high = recent_high + (atr_value * 0.35)
                    pullback_entry = round((pullback_low + pullback_high) / 2, 6)
                else:
                    pullback_low = None
                    pullback_high = None
                    pullback_entry = None
                if not breakout and not pre_breakout:
                    pullback_low = None
                    pullback_high = None
                    pullback_entry = None

                has_pullback_plan = (
                    pullback_low is not None
                    and pullback_high is not None
                    and pullback_entry is not None
                )

                # حذف الكود القديم الخاص بـ execution المؤقت
                # سيتم حسابه لاحقاً بعد معرفة stop_loss و rr1/rr2

                if vol_ratio < 1.02 and not breakout and not pre_breakout and not early_signal and not has_extra_strong_setup:
                    log_long_rejection(
                        symbol=symbol,
                        reason="low_volume_no_breakout",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={
                            "has_extra_strong_setup": has_extra_strong_setup,
                            "extra_setup_names": extra_setup_names,
                            "primary_extra_setup": primary_extra_setup,
                            "extra_setup_bonus": extra_setup_bonus,
                        },
                    )
                    continue
                if is_late_long_entry(dist_ma=dist_ma, breakout=breakout, pre_breakout=pre_breakout) and not is_reverse:
                    log_long_rejection(
                        symbol=symbol,
                        reason="late_entry_simple",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={
                            "has_extra_strong_setup": has_extra_strong_setup,
                            "extra_setup_names": extra_setup_names,
                            "primary_extra_setup": primary_extra_setup,
                            "extra_setup_bonus": extra_setup_bonus,
                        },
                    )
                    continue
                if is_exhausted_long_move(
                    dist_ma=dist_ma,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                ) and not is_reverse:
                    log_long_rejection(
                        symbol=symbol,
                        reason="exhausted_long_move",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={"candle_strength": candle_strength,
                               "has_extra_strong_setup": has_extra_strong_setup,
                               "extra_setup_names": extra_setup_names,
                               "primary_extra_setup": primary_extra_setup,
                               "extra_setup_bonus": extra_setup_bonus},
                    )
                    continue
                try:
                    score_result = calculate_long_score(
                        df=df,
                        vol_ratio=vol_ratio,
                        mtf_confirmed=mtf_confirmed,
                        btc_mode=btc_mode,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_new=is_new,
                        funding=funding,
                        btc_dominance_proxy=btc_dominance_proxy,
                        market_state=market_state,
                        alt_mode=alt_mode,
                        market_bias_label=market_bias_label,
                    )
                except Exception as score_err:
                    logger.error(f"{symbol} --> calculate_long_score failed: {score_err}")
                    log_long_rejection(
                        symbol=symbol,
                        reason="calculate_long_score_failed",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                    )
                    continue
                raw_score = float(score_result.get("score", 0))
                effective_score = raw_score
                adjustments_log = []
                if pre_score_adjustments_log:
                    for adj in pre_score_adjustments_log:
                        effective_score += float(adj.get("value", 0.0) or 0.0)
                    adjustments_log.extend(pre_score_adjustments_log)

                score_result = append_late_pump_warnings(score_result, late_guard)
                if late_guard.get("extreme_late_pump") and not is_reverse:
                    effective_score -= EXTREME_LATE_PUMP_SCORE_PENALTY
                    adjustments_log.append({
                        "name": "extreme_late_pump_penalty",
                        "value": -EXTREME_LATE_PUMP_SCORE_PENALTY,
                        "reason": "extreme_late_pump"
                    })
                elif late_guard.get("late_pump_risk") and not is_reverse:
                    effective_score -= LATE_PUMP_SCORE_PENALTY
                    adjustments_log.append({
                        "name": "late_pump_penalty",
                        "value": -LATE_PUMP_SCORE_PENALTY,
                        "reason": "late_pump"
                    })
                if late_guard.get("bull_continuation_risk") and not is_reverse:
                    effective_score -= BULL_CONTINUATION_SCORE_PENALTY
                    adjustments_log.append({
                        "name": "bull_continuation_penalty",
                        "value": -BULL_CONTINUATION_SCORE_PENALTY,
                        "reason": "bull_continuation"
                    })
                if score_result.get("fake_signal"):
                    if breakout or pre_breakout:
                        eff = -0.20
                    elif early_signal:
                        eff = -0.15
                    else:
                        eff = -0.30
                    effective_score += eff
                    adjustments_log.append({
                        "name": "fake_signal_penalty",
                        "value": eff,
                        "reason": "fake_signal"
                    })
                if not gaining_strength and not breakout and not pre_breakout:
                    effective_score -= 0.15
                    adjustments_log.append({
                        "name": "not_gaining_strength_penalty",
                        "value": -0.15,
                        "reason": "no_intraday_strength"
                    })
                early_bonus = get_early_priority_score_bonus(early_priority)
                if early_bonus != 0:
                    effective_score += early_bonus
                    adjustments_log.append({
                        "name": "early_priority_bonus",
                        "value": early_bonus,
                        "reason": early_priority
                    })
                if breakout and not late_guard.get("late_pump_risk") and not late_guard.get("bull_continuation_risk"):
                    if vol_ratio >= 1.5:
                        effective_score += 0.30
                        adjustments_log.append({
                            "name": "breakout_volume_bonus",
                            "value": 0.30,
                            "reason": "breakout_high_vol"
                        })
                    elif vol_ratio >= 1.3:
                        effective_score += 0.15
                        adjustments_log.append({
                            "name": "breakout_volume_bonus",
                            "value": 0.15,
                            "reason": "breakout_medium_vol"
                        })
                if is_reverse:
                    effective_score += OVERSOLD_REVERSAL_SCORE_BONUS
                    adjustments_log.append({
                        "name": "oversold_reversal_bonus",
                        "value": OVERSOLD_REVERSAL_SCORE_BONUS,
                        "reason": "is_reverse"
                    })

                if has_extra_strong_setup and not is_reverse:
                    effective_score += extra_setup_bonus
                    adjustments_log.append({
                        "name": "extra_strong_setup_bonus",
                        "value": extra_setup_bonus,
                        "reason": extra_setup_names,
                    })
                    score_result.setdefault("reasons", [])
                    score_result["reasons"].append(f"Extra Setup: {primary_extra_setup}")

                if not is_reverse:
                    maturity_penalty = float(entry_maturity_data.get("maturity_penalty", 0.0) or 0.0)
                    maturity_bonus = float(entry_maturity_data.get("maturity_bonus", 0.0) or 0.0)
                    if maturity_penalty != 0:
                        effective_score -= maturity_penalty
                        adjustments_log.append({
                            "name": "entry_maturity_penalty",
                            "value": -maturity_penalty,
                            "reason": "entry_maturity"
                        })
                    if maturity_bonus != 0:
                        effective_score += maturity_bonus
                        adjustments_log.append({
                            "name": "entry_maturity_bonus",
                            "value": maturity_bonus,
                            "reason": "entry_maturity"
                        })
                # Merge warnings created by early softening guards before scoring penalties/reports.
                if warning_reasons:
                    if "warning_reasons" not in score_result or score_result["warning_reasons"] is None:
                        score_result["warning_reasons"] = []
                    for _soft_reason in warning_reasons:
                        if _soft_reason not in score_result["warning_reasons"]:
                            score_result["warning_reasons"].append(_soft_reason)

                for reason in entry_maturity_data.get("warning_reasons", []):
                    if "warning_reasons" not in score_result or score_result["warning_reasons"] is None:
                        score_result["warning_reasons"] = []
                    if reason not in score_result["warning_reasons"]:
                        score_result["warning_reasons"].append(reason)
                if trap_check["soft_trap"] and not is_reverse:
                    effective_score -= 0.30
                    adjustments_log.append({
                        "name": "soft_trap_penalty",
                        "value": -0.30,
                        "reason": "momentum_exhaustion_soft_trap"
                    })
                    if "Momentum Exhaustion Trap" not in score_result.get("warning_reasons", []):
                        if "warning_reasons" not in score_result or score_result["warning_reasons"] is None:
                            score_result["warning_reasons"] = []
                        score_result["warning_reasons"].append("Momentum Exhaustion Trap")
                if breakout_warning:
                    if "warning_reasons" not in score_result or score_result["warning_reasons"] is None:
                        score_result["warning_reasons"] = []
                    if breakout_warning not in score_result["warning_reasons"]:
                        score_result["warning_reasons"].append(breakout_warning)
                current_warnings = score_result.get("warning_reasons", [])
                warning_penalty_value, warning_penalty_details, warning_high_count, warning_medium_count = calculate_warning_penalty(current_warnings)
                if warning_penalty_value != 0.0 and not is_reverse:
                    effective_score -= warning_penalty_value
                    adjustments_log.append({
                        "name": "warning_penalty",
                        "value": -warning_penalty_value,
                        "reason": warning_penalty_details
                    })

                # BTC Range Zone context adjustment (market-context only).
                # This does not change execution whitelist, risk manager, TP/SL, or tracking logic.
                try:
                    _btc_zone = btc_zone if isinstance(btc_zone, dict) else {}
                    _btc_zone_adj = float(_btc_zone.get("score_adjustment", 0.0) or 0.0)
                    _btc_zone_label = str(_btc_zone.get("label", "") or "")
                    _btc_zone_pos = _btc_zone.get("position_pct")
                    if _btc_zone_adj != 0.0:
                        effective_score += _btc_zone_adj
                        adjustments_log.append({
                            "name": "btc_range_zone_adjustment",
                            "value": _btc_zone_adj,
                            "reason": _btc_zone.get("reason", "btc_range_zone"),
                        })
                    if _btc_zone_label:
                        _btc_zone_display = _btc_zone_label
                        if _btc_zone_pos is not None:
                            _btc_zone_display += f" | Pos {_btc_zone_pos}%"
                        if _btc_zone_adj:
                            _btc_zone_display += f" | Score {_btc_zone_adj:+.2f}"
                        if _btc_zone_adj > 0:
                            score_result.setdefault("reasons", [])
                            if _btc_zone_display not in score_result["reasons"]:
                                score_result["reasons"].append(_btc_zone_display)
                        elif _btc_zone_adj < 0:
                            if "warning_reasons" not in score_result or score_result["warning_reasons"] is None:
                                score_result["warning_reasons"] = []
                            if _btc_zone_display not in score_result["warning_reasons"]:
                                score_result["warning_reasons"].append(_btc_zone_display)
                    score_result["btc_zone"] = _btc_zone
                except Exception as _btc_zone_score_err:
                    logger.warning(f"BTC zone score adjustment error for {symbol}: {_btc_zone_score_err}")

                score_result["score"] = round(effective_score, 2)

                price = _safe_float(signal_row["close"], 0)

                # دخول السوق الأصلي
                market_entry = price
                entry_price_for_trade = price
                entry_mode = "market"
                pullback_triggered = True

                wait_pullback = False
                # Pre-compute an early resistance warning before pullback decision.
                # The final TP/SL calculation is still performed later after the final entry is chosen.
                early_resistance_warning = ""
                try:
                    if breakout:
                        _early_sl_type = "breakout"
                    elif pre_breakout:
                        _early_sl_type = "pre_breakout"
                    elif is_new:
                        _early_sl_type = "new_listing"
                    else:
                        _early_sl_type = "standard"
                    _early_rr1, _early_rr2 = get_rr_targets_long(signal_type=_early_sl_type, entry_timing=entry_timing_temp)
                    _early_smart_sl = build_smart_sl_long(
                        df=df,
                        entry=market_entry,
                        atr_value=atr_value,
                        signal_type=_early_sl_type,
                        market_state=market_state,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                    )
                    _early_stop_loss = _safe_float(_early_smart_sl.get("sl"), 0.0)
                    if _early_stop_loss > 0:
                        _early_targets = build_smart_tp1_long(
                            df=df,
                            entry=market_entry,
                            sl=_early_stop_loss,
                            rr1=_early_rr1,
                            rr2=_early_rr2,
                            atr_value=atr_value,
                            market_state=market_state,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                        )
                        early_resistance_warning = str(_early_targets.get("resistance_warning", "") or "")
                except Exception:
                    early_resistance_warning = ""
                if has_pullback_plan:
                    temp_cand_dec = {
                        "market_entry": market_entry,
                        "pullback_entry": pullback_entry,
                        "pullback_low": pullback_low,
                        "pullback_high": pullback_high,
                        "opportunity_type": temp_opportunity_type,
                        "entry_timing": entry_timing_temp,
                        "resistance_warning": early_resistance_warning,
                        "setup_type": "|".join([str(primary_extra_setup or ""), " ".join(extra_setup_names or []), " ".join(context_setups or [])]),
                        "vol_ratio": vol_ratio,
                    }
                    wait_pullback = should_wait_for_pullback_entry(temp_cand_dec)

                final_pullback_entry = None
                if wait_pullback:
                    try:
                        final_pullback_entry = (float(pullback_low) + float(pullback_high)) / 2.0
                    except Exception:
                        try:
                            final_pullback_entry = float(pullback_entry)
                        except Exception:
                            final_pullback_entry = None
                    if not final_pullback_entry or final_pullback_entry <= 0:
                        try:
                            final_pullback_entry = float(pullback_entry)
                        except Exception:
                            final_pullback_entry = None
                    entry_price_for_trade = final_pullback_entry if final_pullback_entry and final_pullback_entry > 0 else price
                    pullback_entry = entry_price_for_trade
                    entry_mode = "pullback_pending"
                    pullback_triggered = False
                    recommended = entry_price_for_trade
                else:
                    entry_price_for_trade = price
                    entry_mode = "market"
                    pullback_triggered = True
                    recommended = price

                if breakout:
                    sl_type = "breakout"
                elif pre_breakout:
                    sl_type = "pre_breakout"
                elif is_new:
                    sl_type = "new_listing"
                else:
                    sl_type = "standard"
                rr1, rr2 = get_rr_targets_long(signal_type=sl_type, entry_timing=entry_timing_temp)
                smart_sl = build_smart_sl_long(
                    df=df,
                    entry=entry_price_for_trade,
                    atr_value=atr_value,
                    signal_type=sl_type,
                    market_state=market_state,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    is_reverse=is_reverse,
                )
                stop_loss = smart_sl["sl"]
                nearest_support = smart_sl.get("nearest_support")
                sl_method = smart_sl.get("sl_method", "atr")
                sl_notes = smart_sl.get("sl_notes", [])

                smart_targets_early = build_smart_tp1_long(
                    df=df,
                    entry=entry_price_for_trade,
                    sl=stop_loss,
                    rr1=rr1,
                    rr2=rr2,
                    atr_value=atr_value,
                    market_state=market_state,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                )
                early_resistance_warning = smart_targets_early.get("resistance_warning", "")
                early_nearest_resistance = smart_targets_early.get("nearest_resistance", None)
                resistance_rejected = False
                resistance_dynamic_penalty = 0.0
                # Prevent the older near_resistance_guard_long from re-rejecting
                # a signal after the new Smart Resistance Balance block already
                # classified it as noise / warning-only.
                smart_resistance_balance_checked = False
                smart_resistance_warning_only = False

                try:
                    _risk_for_quality = float(entry_price_for_trade) - float(stop_loss)
                    _nearest_res = float(early_nearest_resistance or 0.0)
                    if _nearest_res > float(entry_price_for_trade) and _risk_for_quality > 0:
                        smart_resistance_balance_checked = True
                        _res_dist_pct = ((_nearest_res - float(entry_price_for_trade)) / float(entry_price_for_trade)) * 100.0
                        _res_r = (_nearest_res - float(entry_price_for_trade)) / _risk_for_quality
                        _tp1_structure = _nearest_res * 0.995
                        _min_tp1 = float(entry_price_for_trade) + (_risk_for_quality * 1.2)
                        # Smart Resistance Balance (final):
                        # - normal setups remain protected
                        # - relaxed/strong setups use softer hard-reject limits
                        # - ultra-tiny non-structural resistance is treated as noise/warning, not a hard reject
                        _res_source = str(smart_targets_early.get("nearest_resistance_source") or "unknown")
                        # Resistance source quality:
                        # - previous_rejection / confirmed round levels are real structural resistance.
                        # - swing_high is still structural, but softer than major rejection levels.
                        # - bb_upper / recent_20_high are dynamic hints in NORMAL_LONG, not hard walls by themselves.
                        _major_structural_sources = {"previous_rejection", "round_level_confirmed"}
                        _minor_structural_sources = {"swing_high"}
                        _dynamic_hint_sources = {"bb_upper", "recent_20_high"}
                        _weak_micro_res_sources = {"unknown", "micro_high", "tiny_wick"}
                        _res_is_major_structural = _res_source in _major_structural_sources
                        _res_is_minor_structural = _res_source in _minor_structural_sources
                        _res_is_dynamic_hint = _res_source in _dynamic_hint_sources
                        _res_is_structural = _res_is_major_structural or _res_is_minor_structural
                        _res_is_micro_noise = (_res_source in _weak_micro_res_sources) and (_res_dist_pct < 0.25 or _res_r < 0.20)

                        _res_relaxed_setup = is_relaxed_execution_setup(
                            setup_type=setup_type,
                            extra_setup_names=extra_setup_names,
                            primary_extra_setup=primary_extra_setup,
                            execution_setup_tags=_collect_execution_setup_tags({
                                "setup_type": setup_type,
                                "primary_extra_setup": primary_extra_setup,
                                "extra_setup_names": extra_setup_names,
                                "execution_setup_tags": locals().get("execution_setup_tags", []),
                                "relative_strength_vs_btc": locals().get("relative_strength_vs_btc", False),
                                "wave_estimate": locals().get("wave_estimate", None),
                            }),
                            mtf_confirmed=mtf_confirmed,
                            vol_ratio=vol_ratio,
                            score=score_result.get("score", raw_score),
                        )

                        # Market Adaptive Resistance:
                        # In bull/alt-season conditions, a minor swing_high / bb_upper should not
                        # behave like a major supply wall when the signal itself has MTF + volume
                        # + strong setup support. Keep truly close/low-R resistance protected.
                        _score_for_resistance = float(score_result.get("score", raw_score) or 0.0)
                        _strong_market_for_resistance = (
                            market_state in ("bull_market", "alt_season")
                            or current_mode in (MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY)
                        )
                        _res_is_ultra_close = (_res_dist_pct < 0.08 or _res_r < 0.10)
                        _normal_dynamic_hint_warning_only = (
                            current_mode == MODE_NORMAL_LONG
                            and _res_is_dynamic_hint
                            and not _res_is_ultra_close
                        )
                        _bull_mtf_flex = (
                            current_mode == MODE_NORMAL_LONG
                            and _strong_market_for_resistance
                            and bool(mtf_confirmed)
                            and float(vol_ratio or 0.0) >= 1.05
                            and _score_for_resistance >= 6.5
                            and (_res_relaxed_setup or breakout or pre_breakout)
                            and not _res_is_major_structural
                            and not _res_is_ultra_close
                        )

                        if _res_is_micro_noise or _normal_dynamic_hint_warning_only or _bull_mtf_flex:
                            smart_resistance_warning_only = True
                            early_resistance_warning = ""
                            _res_note = "normal_bull_flex_resistance_warning" if _bull_mtf_flex else ("normal_dynamic_resistance_hint" if _normal_dynamic_hint_warning_only else "tiny/micro resistance ignored")
                            logger.info(
                                f"⚪ {symbol} {_res_note} | "
                                f"source={_res_source} | dist={_res_dist_pct:.2f}% | R={_res_r:.2f}"
                            )
                        else:
                            if _res_relaxed_setup or _bull_mtf_flex:
                                # v202: strong/whitelist/Bull+MTF setups should not die on normal nearby resistance.
                                # Hard reject only for truly ultra-close resistance.
                                _hard_dist_limit = 0.15
                                _hard_r_limit = 0.10
                            elif _res_is_major_structural:
                                _hard_dist_limit = 0.50
                                _hard_r_limit = 0.35
                            else:
                                _hard_dist_limit = 0.35
                                _hard_r_limit = 0.25

                            _should_hard_reject_resistance = (
                                _res_is_ultra_close
                                or (
                                    (_res_dist_pct < _hard_dist_limit or _res_r < _hard_r_limit)
                                    and not _bull_mtf_flex
                                )
                            )

                            if _should_hard_reject_resistance:
                                resistance_rejected = True
                                log_long_rejection(
                                    symbol=symbol,
                                    reason="near_resistance_before_tp1",
                                    candle_time=candle_time,
                                    score=score_result.get("score"),
                                    raw_score=raw_score,
                                    market_state=market_state,
                                    current_mode=current_mode,
                                    entry_timing=entry_timing_temp,
                                    opportunity_type=temp_opportunity_type,
                                    dist_ma=dist_ma,
                                    rsi_now=rsi_now,
                                    vol_ratio=vol_ratio,
                                    vwap_distance=vwap_distance,
                                    mtf_confirmed=mtf_confirmed,
                                    breakout=breakout,
                                    pre_breakout=pre_breakout,
                                    is_reverse=is_reverse,
                                    extra={
                                        "nearest_resistance": _nearest_res,
                                        "resistance_source": _res_source,
                                        "resistance_distance_pct": _res_dist_pct,
                                        "resistance_r": _res_r,
                                        "relaxed": _res_relaxed_setup,
                                        "structural": _res_is_structural,
                                        "major_structural": _res_is_major_structural,
                                        "minor_structural": _res_is_minor_structural,
                                        "bull_flex": _bull_mtf_flex,
                                        "ultra_close": _res_is_ultra_close,
                                        "limits": f"dist<{_hard_dist_limit}/R<{_hard_r_limit}",
                                        "category": "trade_quality",
                                    },
                                )
                                logger.info(
                                    f"⛔ {symbol} rejected: near_resistance_before_tp1 | "
                                    f"source={_res_source} | dist={_res_dist_pct:.2f}% | R={_res_r:.2f} | "
                                    f"relaxed={_res_relaxed_setup} | bull_flex={_bull_mtf_flex} | ultra={_res_is_ultra_close} | "
                                    f"limits=dist<{_hard_dist_limit}/R<{_hard_r_limit}"
                                )
                                continue

                        # Do not let the old 1.2R structure rule kill relaxed/bull-market warnings.
                        # Hard reject only if the resistance is real structural and still makes TP1 unrewarding.
                        _tp1_unrewarding_structural = (
                            _tp1_structure < _min_tp1
                            and _res_is_structural
                            and not ((_res_relaxed_setup or _bull_mtf_flex) and _res_r >= 0.10 and _res_dist_pct >= 0.15)
                            and not _bull_mtf_flex
                        )

                        if _tp1_unrewarding_structural:
                            resistance_rejected = True
                            log_long_rejection(
                                symbol=symbol,
                                reason="tp1_not_rewarding_before_resistance",
                                candle_time=candle_time,
                                score=score_result.get("score"),
                                raw_score=raw_score,
                                market_state=market_state,
                                current_mode=current_mode,
                                entry_timing=entry_timing_temp,
                                opportunity_type=temp_opportunity_type,
                                dist_ma=dist_ma,
                                rsi_now=rsi_now,
                                vol_ratio=vol_ratio,
                                vwap_distance=vwap_distance,
                                mtf_confirmed=mtf_confirmed,
                                breakout=breakout,
                                pre_breakout=pre_breakout,
                                is_reverse=is_reverse,
                                extra={
                                    "nearest_resistance": _nearest_res,
                                    "resistance_source": _res_source,
                                    "tp1_structure": _tp1_structure,
                                    "min_tp1": _min_tp1,
                                    "resistance_distance_pct": _res_dist_pct,
                                    "resistance_r": _res_r,
                                    "relaxed": _res_relaxed_setup,
                                    "structural": _res_is_structural,
                                    "category": "trade_quality",
                                },
                            )
                            logger.info(
                                f"⛔ {symbol} rejected: tp1_not_rewarding_before_resistance | "
                                f"source={_res_source} | R={_res_r:.2f} | structural={_res_is_structural}"
                            )
                            continue
                        elif _tp1_structure < _min_tp1:
                            smart_resistance_warning_only = True
                            early_resistance_warning = ""
                            logger.info(
                                f"⚠️ {symbol} resistance warning only | "
                                f"source={_res_source} | dist={_res_dist_pct:.2f}% | R={_res_r:.2f} | "
                                f"relaxed={_res_relaxed_setup} | bull_flex={_bull_mtf_flex}"
                            )
                except Exception as _smart_reject_error:
                    logger.warning(f"smart resistance hard reject check error for {symbol}: {_smart_reject_error}")

                # If Smart Resistance Balance evaluated the level and did not hard-reject,
                # do not let the older dynamic guard kill the same signal again.
                # This keeps near-resistance as warning/penalty only for balanced cases.
                if smart_resistance_balance_checked and not resistance_rejected:
                    early_resistance_warning = ""
                
                explicit_warnings = score_result.get("warning_reasons") or []
                _, inferred_warnings = classify_reasons(score_result.get("reasons", []))
                warnings_count_early = len(explicit_warnings) if explicit_warnings else len(inferred_warnings)
                base_risk_early = get_base_risk_label(score_result, warnings_count_early)
                display_risk_early = adjust_risk_with_entry_timing(base_risk_early, entry_timing_temp)

                should_reject_near_resistance, res_dynamic_penalty = near_resistance_guard_long(
                    resistance_warning=early_resistance_warning,
                    nearest_resistance=early_nearest_resistance,
                    market_state=market_state,
                    btc_mode=btc_mode,
                    alt_mode=alt_mode,
                    current_mode=current_mode,
                    display_risk=display_risk_early,
                    late_guard=late_guard,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                    upper_wick_ratio=upper_wick_ratio,
                    breakout=breakout,
                    breakout_quality=breakout_quality,
                    mtf_confirmed=mtf_confirmed,
                    rsi_now=rsi_now,
                    dist_ma=dist_ma,
                    vwap_distance=vwap_distance,
                    score_after_penalties=score_result["score"],
                )
                # v201: avoid double-killing by near_resistance after Smart TP/Resistance already evaluated.
                # Keep hard reject only for weak/danger context; otherwise convert to warning + capped penalty.
                _near_resistance_soft_context = bool(
                    relaxed_pre_score_setup
                    or has_extra_strong_setup
                    or primary_extra_setup
                    or extra_setup_names
                    or (mtf_confirmed and (breakout or pre_breakout) and vol_ratio >= 1.05)
                    or (market_state in ("bull_market", "alt_season") and mtf_confirmed and vol_ratio >= 1.15)
                )
                _near_resistance_true_danger = bool(
                    late_guard.get("extreme_late_pump", False)
                    or ("مرتفع" in str(display_risk_early) and upper_wick_ratio >= 0.45)
                    or (market_state == "risk_off" and not mtf_confirmed)
                    or (dist_ma >= 5.2 and rsi_now >= 70 and vol_ratio >= 1.8)
                )
                if should_reject_near_resistance and _near_resistance_soft_context and not _near_resistance_true_danger:
                    should_reject_near_resistance = False
                    res_dynamic_penalty = min(float(res_dynamic_penalty or 0.0) + 0.15, 0.35)
                    soft_warning = True
                    entry_warning = True
                    warning_reasons.append("قرب مقاومة لكن setup/MTF داعم؛ تحذير بدل رفض")
                    logger.info(f"{symbol} --> near resistance softened to warning (v201)")
                if should_reject_near_resistance:
                    log_long_rejection(
                        symbol=symbol,
                        reason="near_resistance",
                        candle_time=candle_time,
                        score=score_result.get("score"),
                        raw_score=raw_score,
                        market_state=market_state,
                        current_mode=current_mode,
                        entry_timing=entry_timing_temp,
                        opportunity_type=temp_opportunity_type,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        secondary_reasons=[
                            "weak_market_conditions" if any(cond for cond in [
                                market_state in ("risk_off", "btc_leading", "mixed"),
                                "هابط" in btc_mode,
                                "ضعيف" in alt_mode,
                                "مرتفع" in display_risk_early,
                                current_mode == MODE_STRONG_LONG_ONLY,
                                late_guard.get("late_pump_risk", False),
                                (vol_ratio >= 1.8 and candle_strength >= 0.60),
                                upper_wick_ratio >= 0.35,
                            ]) else "strong_exception_missing"
                        ],
                        extra={
                            "nearest_resistance": early_nearest_resistance,
                            "resistance_warning": early_resistance_warning,
                            "res_dynamic_penalty": res_dynamic_penalty,
                            "upper_wick_ratio": upper_wick_ratio,
                            "has_extra_strong_setup": has_extra_strong_setup,
                            "extra_setup_names": extra_setup_names,
                            "primary_extra_setup": primary_extra_setup,
                            "extra_setup_bonus": extra_setup_bonus,
                        },
                    )
                    logger.info(f"{symbol} --> rejected by near resistance + weak market guard")
                    continue
                if res_dynamic_penalty != 0.0:
                    effective_score -= res_dynamic_penalty
                    score_result["score"] = round(effective_score, 2)
                    adjustments_log.append({
                        "name": "near_resistance_dynamic_penalty",
                        "value": -res_dynamic_penalty,
                        "reason": early_resistance_warning
                    })

                wave_context_early = infer_wave_context(
                    entry_maturity_data=entry_maturity_data,
                    is_reverse=is_reverse,
                    dist_ma=dist_ma,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                )
                setup_type_candidate = {
                    "is_reverse": is_reverse,
                    "breakout": breakout,
                    "pre_breakout": pre_breakout,
                    "mtf_confirmed": mtf_confirmed,
                    "vol_ratio": vol_ratio,
                    "market_state": market_state,
                    "wave_context": wave_context_early,
                }
                setup_type = build_setup_type(setup_type_candidate)
                setup_type_base = "|".join(str(setup_type).split("|")[:4])

                dynamic_threshold = get_dynamic_entry_threshold(
                    market_state=market_state,
                    score_result=score_result,
                    vol_ratio=vol_ratio,
                    mtf_confirmed=mtf_confirmed,
                    is_new=is_new,
                    gaining_strength=gaining_strength,
                )
                if dist_ma < -4.4 and not breakout and not pre_breakout and not is_reverse:
                    dynamic_threshold += 0.15
                if candle_strength < 0.45 and dist_ma < -4.0 and not breakout and not pre_breakout and not is_reverse:
                    dynamic_threshold += 0.10
                dynamic_threshold += get_early_priority_threshold_adjustment(early_priority)
                dynamic_threshold = round(dynamic_threshold, 2)
                if current_mode == MODE_STRONG_LONG_ONLY:
                    effective_score -= 0.08
                    score_result["score"] = round(effective_score, 2)
                    adjustments_log.append({
                        "name": "strong_mode_penalty",
                        "value": -0.08,
                        "reason": "strong_mode"
                    })
                    dynamic_threshold += 0.10
                    dynamic_threshold = round(dynamic_threshold, 2)
                    if score_result["score"] < STRONG_ONLY_MIN_SCORE:
                        log_long_rejection(
                            symbol=symbol,
                            reason="strong_only_insufficient_score",
                            candle_time=candle_time,
                            score=score_result["score"],
                            raw_score=raw_score,
                            market_state=market_state,
                            current_mode=current_mode,
                            entry_timing=entry_timing_temp,
                            opportunity_type=temp_opportunity_type,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                        )
                        logger.info(f"{symbol} --> skipped (STRONG_LONG_ONLY: score < {STRONG_ONLY_MIN_SCORE})")
                        continue

                if current_mode == MODE_STRONG_LONG_ONLY and "🔴" in entry_timing_temp:
                    _slo_is_chase_or_very_late = (
                        "مطاردة حركة" in str(entry_timing_temp)
                        or "متأخر جدًا" in str(entry_timing_temp)
                    )
                    _slo_late_block = (
                        _slo_is_chase_or_very_late
                        and (dist_ma >= 4.2 or "ضعيف" in str(alt_mode))
                    )
                    if _slo_late_block:
                        log_long_rejection(
                            symbol=symbol,
                            reason="strong_only_late_entry",
                            candle_time=candle_time,
                            market_state=market_state,
                            current_mode=current_mode,
                            entry_timing=entry_timing_temp,
                            opportunity_type=temp_opportunity_type,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                            extra={
                                "has_extra_strong_setup": has_extra_strong_setup,
                                "extra_setup_names": extra_setup_names,
                                "primary_extra_setup": primary_extra_setup,
                                "extra_setup_bonus": extra_setup_bonus,
                                "slo_chase_or_very_late": _slo_is_chase_or_very_late,
                                "slo_dist_ma": dist_ma,
                                "slo_alt_mode": alt_mode,
                            },
                        )
                        logger.info(
                            f"{symbol} --> skipped (STRONG_LONG_ONLY: late entry "
                            f"chase/very-late, dist_ma={dist_ma:.2f}, alt_mode={alt_mode})"
                        )
                        continue
                    else:
                        logger.info(
                            f"{symbol} --> STRONG_LONG_ONLY: late '🔴' but not blocked "
                            f"(chase_or_very_late={_slo_is_chase_or_very_late}, "
                            f"dist_ma={dist_ma:.2f}, alt_mode={alt_mode})"
                        )

                opportunity_type = temp_opportunity_type
                entry_timing = entry_timing_temp
                effective_required_min_score = FINAL_MIN_SCORE

                if is_reverse:
                    effective_required_min_score = OVERSOLD_REVERSAL_MIN_SCORE

                if pre_breakout:
                    effective_required_min_score = max(effective_required_min_score - PRE_BREAKOUT_EXTRA_SCORE, 5.8)

                if (
                    market_state == "bull_market"
                    and opportunity_type in ("استمرار", "continuation")
                    and not breakout
                    and not pre_breakout
                    and not is_reverse
                ):
                    bull_continuation_extra = 0.0
                    if dist_ma > BULL_CONTINUATION_MAX_DIST_MA:
                        bull_continuation_extra += 0.30
                    if rsi_now > BULL_CONTINUATION_MAX_RSI:
                        bull_continuation_extra += 0.30
                    if vwap_distance >= 2.4:
                        bull_continuation_extra += 0.30
                    if vol_ratio >= 1.8 and candle_strength >= 0.60:
                        bull_continuation_extra += 0.30

                    effective_required_min_score += bull_continuation_extra
                    effective_required_min_score = round(effective_required_min_score, 2)

                    if bull_continuation_extra != 0:
                        adjustments_log.append({
                            "name": "bull_continuation_threshold_extra",
                            "value": bull_continuation_extra,
                            "reason": "extra_required_min_score_for_bull_continuation"
                        })

                    if not strong_bull_pullback and score_result["score"] < 7.5:
                        log_long_rejection(
                            symbol=symbol,
                            reason="bull_continuation_strict_filter",
                            candle_time=candle_time,
                            score=score_result["score"],
                            raw_score=raw_score,
                            market_state=market_state,
                            current_mode=current_mode,
                            entry_timing=entry_timing,
                            opportunity_type=opportunity_type,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                            extra={
                                "required_score": 7.5,
                                "bull_continuation_extra": bull_continuation_extra,
                                "candle_strength": candle_strength,
                                "breakout_quality": breakout_quality,
                                "late_guard_reasons": late_guard.get("reasons", []),
                                "strong_bull_pullback": strong_bull_pullback,
                                "has_extra_strong_setup": has_extra_strong_setup,
                                "extra_setup_names": extra_setup_names,
                                "primary_extra_setup": primary_extra_setup,
                                "extra_setup_bonus": extra_setup_bonus,
                            },
                        )
                        logger.info(
                            f"{symbol} --> skipped "
                            f"(bull continuation strict filter | score={score_result['score']} | "
                            f"dist_ma={dist_ma:.2f} | rsi={rsi_now:.1f} | vwap={vwap_distance:.2f})"
                        )
                        continue

                    if strong_bull_pullback:
                        adjustments_log.append({
                            "name": "strong_bull_pullback_exception",
                            "value": 0.0,
                            "reason": "bypassed_bull_continuation_7_5_filter"
                        })

                final_threshold = max(dynamic_threshold, effective_required_min_score)

                if early_priority == "strong":
                    final_threshold -= 0.15
                    adjustments_log.append({
                        "name": "early_priority_final_threshold_discount",
                        "value": -0.15,
                        "reason": "early_priority_strong"
                    })

                if current_mode == MODE_STRONG_LONG_ONLY and final_threshold_min is not None:
                    final_threshold = max(final_threshold, final_threshold_min)

                final_threshold = round(final_threshold, 2)

                if (not mtf_confirmed) and "🔴" in entry_timing and not is_reverse:
                    log_long_rejection(
                        symbol=symbol,
                        reason="late_without_mtf",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        entry_timing=entry_timing,
                        opportunity_type=opportunity_type,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={
                            "has_extra_strong_setup": has_extra_strong_setup,
                            "extra_setup_names": extra_setup_names,
                            "primary_extra_setup": primary_extra_setup,
                            "extra_setup_bonus": extra_setup_bonus,
                        },
                    )
                    logger.info(f"{symbol} --> skipped (late without MTF confirmation)")
                    continue
                if mtf_confirmed and change_4h > 3 and not breakout and not pre_breakout and not is_reverse:
                    log_long_rejection(
                        symbol=symbol,
                        reason="chasing_4h_move",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        entry_timing=entry_timing,
                        opportunity_type=opportunity_type,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={"change_4h": change_4h,
                               "has_extra_strong_setup": has_extra_strong_setup,
                               "extra_setup_names": extra_setup_names,
                               "primary_extra_setup": primary_extra_setup,
                               "extra_setup_bonus": extra_setup_bonus},
                    )
                    logger.info(f"{symbol} --> skipped (chasing 4h move)")
                    continue

                if not breakout and not pre_breakout and dist_ma > 6.2 and not is_reverse:
                    log_long_rejection(
                        symbol=symbol,
                        reason="late_move_without_breakout",
                        candle_time=candle_time,
                        market_state=market_state,
                        current_mode=current_mode,
                        entry_timing=entry_timing,
                        opportunity_type=opportunity_type,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={
                            "has_extra_strong_setup": has_extra_strong_setup,
                            "extra_setup_names": extra_setup_names,
                            "primary_extra_setup": primary_extra_setup,
                            "extra_setup_bonus": extra_setup_bonus,
                        },
                    )
                    logger.info(f"{symbol} --> rejected (late move without breakout)")
                    continue
                if "🟢 مبكر" in entry_timing:
                    if not mtf_confirmed and not breakout and not pre_breakout:
                        if early_priority != "strong" or score_result["score"] < dynamic_threshold:
                            log_long_rejection(
                                symbol=symbol,
                                reason="early_without_confirmation",
                                candle_time=candle_time,
                                market_state=market_state,
                                current_mode=current_mode,
                                entry_timing=entry_timing,
                                opportunity_type=opportunity_type,
                                dist_ma=dist_ma,
                                rsi_now=rsi_now,
                                vol_ratio=vol_ratio,
                                vwap_distance=vwap_distance,
                                mtf_confirmed=mtf_confirmed,
                                breakout=breakout,
                                pre_breakout=pre_breakout,
                                is_reverse=is_reverse,
                                extra={"early_priority": early_priority, "dynamic_threshold": dynamic_threshold,
                                       "has_extra_strong_setup": has_extra_strong_setup,
                                       "extra_setup_names": extra_setup_names,
                                       "primary_extra_setup": primary_extra_setup,
                                       "extra_setup_bonus": extra_setup_bonus},
                            )
                            logger.info(f"{symbol} --> مبكر بدون تأكيد قوي، تم التخطي")
                            continue
                if is_new and not passes_new_listing_filter(
                    score=float(score_result["score"]),
                    breakout=breakout or pre_breakout,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                ):
                    log_long_rejection(
                        symbol=symbol,
                        reason="new_listing_filter",
                        candle_time=candle_time,
                        score=score_result.get("score"),
                        raw_score=raw_score,
                        market_state=market_state,
                        current_mode=current_mode,
                        entry_timing=entry_timing,
                        opportunity_type=opportunity_type,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        extra={"candle_strength": candle_strength,
                               "has_extra_strong_setup": has_extra_strong_setup,
                               "extra_setup_names": extra_setup_names,
                               "primary_extra_setup": primary_extra_setup,
                               "extra_setup_bonus": extra_setup_bonus},
                    )
                    logger.info(f"{symbol} → rejected by balanced new listing filter")
                    continue

                if score_result["score"] < final_threshold:
                    sec_reasons = []
                    if late_guard.get("late_pump_risk"):
                        sec_reasons.append("late_pump_risk")
                    if late_guard.get("bull_continuation_risk"):
                        sec_reasons.append("bull_continuation_risk")
                    if trap_check["soft_trap"]:
                        sec_reasons.append("soft_trap")
                    if warning_penalty_value > 0:
                        sec_reasons.append("warning_penalty_applied")
                    if res_dynamic_penalty > 0:
                        sec_reasons.append("near_resistance_penalty")
                    if setup_type_base in WEAK_SETUP_TYPES and not is_reverse and not pre_breakout and breakout_quality != "strong":
                        sec_reasons.append("weak_historical_setup_penalty_applied")
                    log_long_rejection(
                        symbol=symbol,
                        reason="final_threshold",
                        candle_time=candle_time,
                        score=score_result.get("score"),
                        raw_score=raw_score,
                        final_threshold=final_threshold,
                        market_state=market_state,
                        current_mode=current_mode,
                        entry_timing=entry_timing,
                        opportunity_type=opportunity_type,
                        dist_ma=dist_ma,
                        rsi_now=rsi_now,
                        vol_ratio=vol_ratio,
                        vwap_distance=vwap_distance,
                        mtf_confirmed=mtf_confirmed,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_reverse=is_reverse,
                        secondary_reasons=sec_reasons,
                        extra={
                            "dynamic_threshold": dynamic_threshold,
                            "required_min_score": effective_required_min_score,
                            "adjustments_log": adjustments_log,
                            "warning_reasons": score_result.get("warning_reasons", []),
                            "early_priority": early_priority,
                            "breakout_quality": breakout_quality,
                            "has_extra_strong_setup": has_extra_strong_setup,
                            "extra_setup_names": extra_setup_names,
                            "primary_extra_setup": primary_extra_setup,
                            "extra_setup_bonus": extra_setup_bonus,
                        },
                    )
                    logger.info(
                        f"{symbol} --> rejected by final_threshold {final_threshold:.2f} "
                        f"(score={score_result['score']:.2f}, dynamic={dynamic_threshold:.2f}, "
                        f"required={effective_required_min_score:.2f})"
                    )
                    continue

                tp1 = smart_targets_early.get("tp1", calc_tp_long(entry_price_for_trade, stop_loss, rr=rr1))
                tp2 = smart_targets_early.get("tp2", calc_tp_long(entry_price_for_trade, stop_loss, rr=rr2))
                target_method = smart_targets_early.get("target_method", "rr")
                nearest_resistance = smart_targets_early.get("nearest_resistance")
                resistance_warning = early_resistance_warning
                support_warning = smart_targets_early.get("support_warning", "")
                target_notes = smart_targets_early.get("target_notes", [])
                rr1 = smart_targets_early.get("rr1_effective", rr1)
                rr2 = smart_targets_early.get("rr2_effective", rr2)

                # حساب execution إذا كان هناك خطة بول باك مفعلة فعلاً
                if wait_pullback:
                    execution_entry = entry_price_for_trade

                    if execution_entry and execution_entry > 0:
                        support_candidates = []
                        if nearest_support is not None:
                            support_candidates.append(float(nearest_support))
                        try:
                            swing_low_val = df["low"].iloc[max(0, signal_idx - 20):signal_idx].astype(float).min()
                            support_candidates.append(swing_low_val)
                        except Exception:
                            pass
                        support_candidates.append(float(pullback_low))
                        if support_candidates:
                            execution_sl = min(support_candidates)
                        else:
                            execution_sl = float(pullback_low) * 0.997
                        execution_sl = min(execution_sl, float(pullback_low) * 0.997)

                        execution_risk = execution_entry - execution_sl
                        if execution_risk <= 0 or (execution_risk / execution_entry) < 0.002:
                            execution_sl = float(pullback_low) * 0.995
                            execution_risk = execution_entry - execution_sl
                        if execution_risk <= 0:
                            execution_sl = float(stop_loss)
                            execution_risk = execution_entry - execution_sl
                        if execution_risk <= 0:
                            execution_sl = execution_tp1 = execution_tp2 = None
                        else:
                            execution_tp1 = execution_entry + (execution_risk * float(rr1))
                            execution_tp2 = execution_entry + (execution_risk * float(rr2))
                    else:
                        execution_entry = execution_sl = execution_tp1 = execution_tp2 = None
                else:
                    execution_entry = execution_sl = execution_tp1 = execution_tp2 = None

                momentum_priority = get_momentum_priority(
                    score=float(score_result["score"]),
                    breakout=breakout,
                    vol_ratio=vol_ratio,
                    is_new=is_new,
                    pre_breakout=pre_breakout,
                    dist_ma=dist_ma,
                    gaining_strength=gaining_strength,
                    early_priority=early_priority,
                    is_reverse=is_reverse,
                )
                if late_guard.get("extreme_late_pump") and not is_reverse:
                    momentum_priority -= 0.90
                elif late_guard.get("late_pump_risk") and not is_reverse:
                    momentum_priority -= 0.60
                if late_guard.get("bull_continuation_risk") and not is_reverse:
                    momentum_priority -= 0.40
                if trap_check["soft_trap"] and not is_reverse:
                    momentum_priority -= 0.30
                momentum_priority = round(momentum_priority, 2)
                alert_id = build_alert_id(symbol, candle_time)
                wave_context = infer_wave_context(
                    entry_maturity_data=entry_maturity_data,
                    is_reverse=is_reverse,
                    dist_ma=dist_ma,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                )
                if wave5_eval.get("can_override"):
                    if "htf_healthy" in wave5_eval.get("label", ""):
                        wave_context = "wave_5_15m_htf_healthy"
                    elif "breakout" in wave5_eval.get("label", ""):
                        wave_context = "wave_5_15m_breakout_confirmed"
                    else:
                        wave_context = "wave_5_override"
                if primary_extra_setup:
                    wave_context = primary_extra_setup
                setup_type_candidate_final = {
                    "is_reverse": is_reverse,
                    "breakout": breakout,
                    "pre_breakout": pre_breakout,
                    "mtf_confirmed": mtf_confirmed,
                    "vol_ratio": vol_ratio,
                    "market_state": market_state,
                    "wave_context": wave_context,
                }
                setup_type = build_setup_type(setup_type_candidate_final)
                setup_type_base = "|".join(str(setup_type).split("|")[:4])
                if (setup_type_base in WEAK_SETUP_TYPES
                    and not is_reverse
                    and not pre_breakout
                    and breakout_quality != "strong"
                    and current_mode != MODE_RECOVERY_LONG):
                    effective_score -= 0.60
                    score_result["score"] = round(effective_score, 2)
                    adjustments_log.append({
                        "name": "weak_historical_setup_penalty",
                        "value": -0.60,
                        "reason": "weak_historical_setup"
                    })
                    if "warning_reasons" not in score_result or score_result["warning_reasons"] is None:
                        score_result["warning_reasons"] = []
                    if "Weak Historical Setup" not in score_result["warning_reasons"]:
                        score_result["warning_reasons"].append("Weak Historical Setup")
                    if score_result["score"] < final_threshold:
                        log_long_rejection(
                            symbol=symbol,
                            reason="weak_historical_setup",
                            candle_time=candle_time,
                            score=score_result.get("score"),
                            raw_score=raw_score,
                            final_threshold=final_threshold,
                            market_state=market_state,
                            current_mode=current_mode,
                            setup_type=setup_type,
                            entry_timing=entry_timing,
                            opportunity_type=opportunity_type,
                            dist_ma=dist_ma,
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            vwap_distance=vwap_distance,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            is_reverse=is_reverse,
                            extra={
                                "setup_type_base": setup_type_base,
                                "breakout_quality": breakout_quality,
                                "has_extra_strong_setup": has_extra_strong_setup,
                                "extra_setup_names": extra_setup_names,
                                "primary_extra_setup": primary_extra_setup,
                                "extra_setup_bonus": extra_setup_bonus,
                            },
                        )
                        logger.info(f"{symbol} --> skipped (weak historical setup: score below final_threshold {final_threshold})")
                        continue
                    momentum_priority -= 0.60
                    momentum_priority = round(momentum_priority, 2)

                final_explicit_warnings = score_result.get("warning_reasons") or []
                _, final_inferred_warnings = classify_reasons(score_result.get("reasons", []))
                final_warnings_count = len(final_explicit_warnings) if final_explicit_warnings else len(final_inferred_warnings)
                final_base_risk = get_base_risk_label(score_result, final_warnings_count)
                final_display_risk = adjust_risk_with_entry_timing(final_base_risk, entry_timing)

                candidate = {
                    "symbol": symbol,
                    "candle_time": candle_time,
                    "entry": entry_price_for_trade,
                    "sl": stop_loss,
                    "tp1": tp1,
                    "tp2": tp2,
                    "score": float(score_result["score"]),
                    "setup_type": setup_type,
                    "setup_type_base": setup_type_base,
                    "reasons": score_result.get("reasons", []),
                    "warning_reasons": score_result.get("warning_reasons", []),
                    "btc_mode": btc_mode,
                    "funding_label": score_result.get("funding_label", "🟡 محايد"),
                    "pre_breakout": pre_breakout,
                    "breakout": breakout,
                    "vol_ratio": vol_ratio,
                    "candle_strength": candle_strength,
                    "mtf_confirmed": mtf_confirmed,
                    "is_new": is_new,
                    "btc_dominance_proxy": btc_dominance_proxy,
                    "change_24h": change_24h,
                    "raw_score": raw_score,
                    "effective_score": float(score_result["score"]),
                    "dynamic_threshold": dynamic_threshold,
                    "required_min_score": effective_required_min_score,
                    "final_threshold": final_threshold,
                    "dist_ma": dist_ma,
                    "entry_timing": entry_timing,
                    "opportunity_type": opportunity_type,
                    "market_state": market_state,
                    "market_state_label": market_state_label,
                    "market_bias_label": market_bias_label,
                    "alt_mode": alt_mode,
                    "early_priority": early_priority,
                    "breakout_quality": breakout_quality,
                    "risk_level": final_display_risk,
                    "fake_signal": bool(score_result.get("fake_signal", False)),
                    "is_reverse": is_reverse,
                    "reversal_4h_confirmed": reversal_4h_result.get("confirmed", False),
                    "rank_volume_24h": float(pair_data.get("_rank_volume_24h", 0)),
                    "alert_id": alert_id,
                    "has_high_impact_news": has_high_impact_news,
                    "news_titles": [e.get("title", "") for e in upcoming_events[:3]],
                    "warning_penalty": warning_penalty_value,
                    "warning_high_count": warning_high_count,
                    "warning_medium_count": warning_medium_count,
                    "warning_penalty_details": warning_penalty_details,
                    "adjustments_log": adjustments_log,
                    "pullback_entry": pullback_entry,
                    "pullback_low": pullback_low,
                    "pullback_high": pullback_high,
                    "rr1": rr1,
                    "rr2": rr2,
                    "fib_position": entry_maturity_data.get("fib_position", "unknown"),
                    "fib_position_ratio": entry_maturity_data.get("fib_position_ratio", 0.0),
                    "fib_label": entry_maturity_data.get("fib_label", ""),
                    "had_pullback": entry_maturity_data.get("had_pullback", False),
                    "pullback_pct": entry_maturity_data.get("pullback_pct", 0.0),
                    "pullback_label": entry_maturity_data.get("pullback_label", ""),
                    "wave_estimate": entry_maturity_data.get("wave_estimate", 0),
                    "wave_peaks": entry_maturity_data.get("wave_peaks", 0),
                    "wave_label": entry_maturity_data.get("wave_label", ""),
                    "entry_maturity": entry_maturity_data.get("entry_maturity", "unknown"),
                    "maturity_penalty": entry_maturity_data.get("maturity_penalty", 0.0),
                    "maturity_bonus": entry_maturity_data.get("maturity_bonus", 0.0),
                    "falling_knife_risk": bool(falling_knife_data.get("falling_knife_risk", False)),
                    "falling_knife_reasons": falling_knife_data.get("reasons", []),
                    "reversal_quality": "",
                    "wave_context": wave_context,
                    "setup_context": "",
                    "reversal_structure_confirmed": False,
                    "strong_bull_pullback": strong_bull_pullback,
                    "strong_breakout_exception": strong_breakout_exception,
                    "htf_1h_context": htf_1h_context,
                    "htf_4h_context": htf_4h_context,
                    "has_extra_strong_setup": has_extra_strong_setup,
                    "extra_setup_names": extra_setup_names,
                    "extra_setup_bonus": extra_setup_bonus,
                    "primary_extra_setup": primary_extra_setup,
                    "extra_setups_details": extra_setups.get("details", {}),
                    "context_setups": context_setups,
                    "target_method": target_method,
                    "nearest_resistance": nearest_resistance,
                    "nearest_support": nearest_support,
                    "resistance_warning": resistance_warning,
                    "support_warning": support_warning,
                    "target_notes": target_notes,
                    "sl_method": sl_method,
                    "sl_notes": sl_notes,
                    "tp1_close_pct": TP1_CLOSE_PCT,
                    "tp2_close_pct": TP2_CLOSE_PCT,
                    "move_sl_to_entry_after_tp1": MOVE_SL_TO_ENTRY_AFTER_TP1,
                    "momentum_priority": momentum_priority,
                    "now": now,
                    "relative_strength_short": round(get_change_8(df) - get_change_8(btc_15m_df if 'btc_15m_df' in locals() else None), 4),
                    "relative_strength_24": round(float(change_24h or 0.0) - float(btc_change_24h or 0.0), 4),
                    "relative_strength_vs_btc": (round(get_change_8(df) - get_change_8(btc_15m_df if 'btc_15m_df' in locals() else None), 4) >= 1.5 or round(float(change_24h or 0.0) - float(btc_change_24h or 0.0), 4) >= 2.0),
                    "block_exception": False,
                    "block_longs_execution_candidate": False,
                    "early_execution_setup_tags": early_execution_setup_tags,
                    "current_mode": current_mode,
                    "market_mode": current_mode,
                    "btc_zone": btc_zone if isinstance(btc_zone, dict) else {},
                    "btc_zone_label": (btc_zone or {}).get("label", "") if isinstance(btc_zone, dict) else "",
                    "btc_zone_score_adjustment": (btc_zone or {}).get("score_adjustment", 0.0) if isinstance(btc_zone, dict) else 0.0,
                    "late_breakout_guard_reason": late_breakout_guard_reason,
                    "setup_stats": get_setup_type_stats(
                        redis_client=r,
                        market_type="futures",
                        side="long",
                        setup_type=setup_type,
                        since_ts=stats_reset_ts,
                    ),
                    "reversal_4h_result": reversal_4h_result,
                    "above_upper_bb": above_upper_bb,
                    "change_4h": change_4h,
                    "late_guard": late_guard,
                    "rsi_now": rsi_now,
                    "vwap_distance": vwap_distance,
                    "rsi_slope": rsi_slope,
                    "macd_hist": macd_hist,
                    "macd_hist_slope": macd_hist_slope,
                    "upper_wick_ratio": upper_wick_ratio,
                    "res_dynamic_penalty": res_dynamic_penalty,
                    "signal_rating": score_result.get("signal_rating", "⚡ عادي"),
                    "has_pullback_plan": has_pullback_plan,
                    "market_entry": market_entry,
                    "recommended_entry": recommended,
                    "entry_mode": entry_mode,
                    "pullback_triggered": pullback_triggered,
                    "execution_entry": execution_entry,
                    "execution_sl": execution_sl,
                    "execution_tp1": execution_tp1,
                    "execution_tp2": execution_tp2,
                }
                candidate["execution_setup_tags"] = build_execution_setup_tags(candidate)
                candidate = v200_finalize_candidate(candidate, market_engine_context)
                v200_log_candidate_diagnostics(candidate, layer="candidate_built")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "EXEC TAGS | "
                        f"symbol={symbol} | tags={candidate.get('execution_setup_tags', [])} | "
                        f"primary={primary_extra_setup} | extras={extra_setup_names} | wave={candidate.get('wave_estimate')}"
                    )
                if current_mode == MODE_BLOCK_LONGS:
                    if not is_strong_exception(candidate):
                        log_long_rejection(
                            symbol=symbol,
                            reason="market_mode_block_longs",
                            candle_time=candle_time,
                            score=candidate.get("score"),
                            raw_score=candidate.get("raw_score"),
                            final_threshold=candidate.get("final_threshold"),
                            market_state=candidate.get("market_state", ""),
                            current_mode=current_mode,
                            setup_type=candidate.get("setup_type", ""),
                            entry_timing=candidate.get("entry_timing", ""),
                            opportunity_type=candidate.get("opportunity_type", ""),
                            dist_ma=candidate.get("dist_ma"),
                            rsi_now=rsi_now,
                            vol_ratio=vol_ratio,
                            mtf_confirmed=mtf_confirmed,
                            breakout=breakout,
                            pre_breakout=pre_breakout,
                            extra={
                                "blocked_by": "market_mode",
                                "mode": "BLOCK_LONGS",
                                "relative_strength_short": candidate.get("relative_strength_short"),
                                "relative_strength_24": candidate.get("relative_strength_24"),
                            },
                        )
                        logger.info(f"⛔ BLOCK_LONGS | skipped {symbol}")
                        continue
                    candidate["block_exception"] = True
                    candidate["block_longs_execution_candidate"] = True
                    candidate["market_mode"] = current_mode
                    logger.info(f"🔥 BLOCK_LONGS EXCEPTION | allowed {symbol}")

                candidate["bucket"] = get_candidate_bucket(candidate)
                candidates.append(candidate)
                candidates_symbols.add(symbol)
            if no_candles_symbols_this_run:
                sample = ", ".join(sorted(list(no_candles_symbols_this_run))[:NO_CANDLES_LOG_SAMPLE_LIMIT])
                logger.warning(
                    f"DATA ERROR SUMMARY | no_candles unique={len(no_candles_symbols_this_run)} "
                    f"sample=[{sample}]"
                )
            if no_candles_cooldown_symbols_this_run:
                sample = ", ".join(sorted(list(no_candles_cooldown_symbols_this_run))[:NO_CANDLES_LOG_SAMPLE_LIMIT])
                logger.info(
                    f"DATA ERROR SUMMARY | no_candles cooldown unique={len(no_candles_cooldown_symbols_this_run)} "
                    f"sample=[{sample}]"
                )
            candidates_before_momentum = len(candidates)
            logger.info(f"Long candidates found before momentum filter: {candidates_before_momentum}")
            candidates_ranked_before_momentum = v200_rank_candidates(candidates, current_mode)
            candidates = apply_top_momentum_filter(candidates_ranked_before_momentum)
            candidates = v200_momentum_zero_fallback(candidates_ranked_before_momentum, candidates, current_mode)
            candidates = v200_rank_candidates(candidates, current_mode)
            candidates_after_momentum = len(candidates)
            logger.info(f"Long candidates found after momentum/ranking engine: {candidates_after_momentum}")
            top_candidates = diversify_candidates(candidates, min(max_alerts, len(candidates)))
            top_candidates_count = len(top_candidates)
            for candidate in top_candidates:
                if sent_count >= max_alerts:
                    logger.info(f"Reached max alerts: {sent_count}/{max_alerts}, stopping send")
                    break
                symbol = candidate["symbol"]
                if symbol in sent_symbols_this_run:
                    continue
                locked = reserve_signal_slot(symbol, candidate["candle_time"], "long")
                if not locked:
                    continue

                if not validate_signal_prices(candidate):
                    logger.warning(f"SKIP {symbol}: invalid_price_data — entry/sl/tp1/tp2/market_price contains zero")
                    release_signal_slot(symbol, candidate["candle_time"], "long")
                    continue

                tv_link = build_tradingview_link(symbol)
                temp_score_result = {
                    "score": candidate["score"],
                    "reasons": candidate.get("reasons", []),
                    "warning_reasons": candidate.get("warning_reasons", []),
                    "funding_label": candidate.get("funding_label", "🟡 محايد"),
                    "signal_rating": candidate.get("signal_rating", "⚡ عادي"),
                    "fake_signal": candidate.get("fake_signal", False),
                }
                message = build_message(
                    symbol=symbol,
                    price=candidate["entry"],
                    score_result=temp_score_result,
                    stop_loss=candidate["sl"],
                    tp1=candidate["tp1"],
                    tp2=candidate["tp2"],
                    rr1=candidate["rr1"],
                    rr2=candidate["rr2"],
                    btc_mode=btc_mode,
                    btc_dominance_proxy=btc_dominance_proxy,
                    tv_link=tv_link,
                    is_new=candidate["is_new"],
                    change_24h=candidate["change_24h"],
                    market_state_label=market_state_label,
                    market_bias_label=market_bias_label,
                    alt_mode=alt_mode,
                    news_warning=news_warning_text,
                    opportunity_type=candidate["opportunity_type"],
                    entry_timing=candidate["entry_timing"],
                    display_risk=candidate["risk_level"],
                    setup_stats=candidate.get("setup_stats"),
                    is_reverse=candidate["is_reverse"],
                    reversal_4h_confirmed=candidate["reversal_4h_result"]["confirmed"],
                    reversal_4h_details=candidate["reversal_4h_result"].get("details", ""),
                    breakout_quality=candidate["breakout_quality"],
                    pullback_low=candidate.get("pullback_low"),
                    pullback_high=candidate.get("pullback_high"),
                    entry_maturity_data={
                        "fib_position": candidate.get("fib_position", "unknown"),
                        "fib_position_ratio": candidate.get("fib_position_ratio", 0.0),
                        "fib_label": candidate.get("fib_label", "غير معروف"),
                        "had_pullback": candidate.get("had_pullback", False),
                        "pullback_pct": candidate.get("pullback_pct", 0.0),
                        "pullback_label": candidate.get("pullback_label", "غير معروف"),
                        "wave_estimate": candidate.get("wave_estimate", 0),
                        "wave_peaks": candidate.get("wave_peaks", 0),
                        "wave_label": candidate.get("wave_label", "غير معروف"),
                        "entry_maturity": candidate.get("entry_maturity", "unknown"),
                        "maturity_penalty": candidate.get("maturity_penalty", 0.0),
                        "maturity_bonus": candidate.get("maturity_bonus", 0.0),
                    },
                    warning_penalty=candidate["warning_penalty"] + candidate.get("res_dynamic_penalty", 0.0),
                    resistance_warning=candidate["resistance_warning"],
                    target_method=candidate["target_method"],
                    nearest_resistance=candidate["nearest_resistance"],
                    wave_context=candidate["wave_context"],
                    extra_setup_names=candidate["extra_setup_names"],
                    primary_extra_setup=candidate["primary_extra_setup"],
                    has_pullback_plan=candidate["has_pullback_plan"],
                    market_price=candidate.get("market_entry", candidate["entry"]),
                    sl_method=candidate["sl_method"],
                    context_setups=candidate.get("context_setups", []),
                    execution_entry=candidate.get("execution_entry"),
                    execution_sl=candidate.get("execution_sl"),
                    execution_tp1=candidate.get("execution_tp1"),
                    execution_tp2=candidate.get("execution_tp2"),
                )
                reply_markup = build_track_reply_markup(candidate["alert_id"])

                if candidate.get("block_exception"):
                    message = "🔥 <b>استثناء BLOCK_LONGS:</b> العملة أقوى من BTC أو Setup قوي جدًا\n\n" + message

                badge = build_execution_badge_line(candidate)
                candidate["execution_candidate_badged"] = bool(badge)
                candidate["execution_candidate_badge_sent"] = bool(badge)
                if not badge:
                    candidate["execution_status"] = "not_candidate"
                    candidate.setdefault("execution_reject_reason", "no_execution_badge")
                if badge:
                    # Execution candidates have their own premium hero header;
                    # remove the calmer normal-signal header to avoid visual duplication.
                    message = message.replace("📈 <b>LONG SIGNAL</b>\n┄┄┄┄┄┄┄┄┄┄┄┄\n\n", "", 1)
                    message = badge + "\n\n" + message

                sent_data = send_telegram_message(
                    message,
                    reply_markup=reply_markup,
                )
                if sent_data.get("ok"):
                    sent_count += 1
                    sent_symbols_this_run.add(symbol)
                    sent_cache[symbol] = time.time()
                    last_candle_cache[symbol] = candidate["candle_time"]
                    last_candle_cache_meta[symbol] = time.time()
                    last_global_send_ts = time.time()
                    message_id = str(((sent_data.get("result") or {}).get("message_id")) or "")
                    alert_snapshot = {
                        "alert_id": candidate["alert_id"],
                        "symbol": symbol,
                        "mode": current_mode,
                        "market_mode": current_mode,
                        "timeframe": TIMEFRAME,
                        "market_entry": candidate.get("market_entry", candidate["entry"]),
                        "entry": candidate["entry"],
                        "recommended_entry": candidate.get("recommended_entry", candidate["entry"]),
                        "pullback_entry": candidate.get("pullback_entry"),
                        "entry_mode": candidate.get("entry_mode", "market"),
                        "pullback_triggered": candidate.get("pullback_triggered", candidate.get("entry_mode") == "market"),
                        "sl": candidate["sl"],
                        "tp1": candidate["tp1"],
                        "tp2": candidate["tp2"],
                        "rr1": candidate["rr1"],
                        "rr2": candidate["rr2"],
                        "score": candidate["score"],
                        "candle_time": candidate["candle_time"],
                        "created_ts": int(time.time()),
                        "market_state": market_state,
                        "alt_mode": alt_mode,
                        "btc_mode": btc_mode,
                        "entry_timing": candidate["entry_timing"],
                        "opportunity_type": candidate["opportunity_type"],
                        "early_priority": candidate["early_priority"],
                        "is_reverse": candidate["is_reverse"],
                        "setup_type": candidate["setup_type"],
                        "above_upper_bb": candidate["above_upper_bb"],
                        "change_4h": candidate["change_4h"],
                        "late_pump_risk": candidate["late_guard"].get("late_pump_risk", False),
                        "bull_continuation_risk": candidate["late_guard"].get("bull_continuation_risk", False),
                        "rsi_now": candidate["rsi_now"],
                        "dist_ma": candidate["dist_ma"],
                        "vol_ratio": candidate["vol_ratio"],
                        "pullback_low": candidate["pullback_low"],
                        "pullback_high": candidate["pullback_high"],
                        "has_pullback_plan": candidate["has_pullback_plan"],
                        "market_guard_active": bool(market_guard.get("active", False)),
                        "market_guard_level": market_guard.get("level", "normal"),
                        "market_red_ratio_15m": market_guard.get("red_ratio_15m", 0.0),
                        "market_avg_change_15m": market_guard.get("avg_change_15m", 0.0),
                        "btc_change_15m": market_guard.get("btc_change_15m", 0.0),
                        "vwap_distance": candidate["vwap_distance"],
                        "rsi_slope": candidate["rsi_slope"],
                        "macd_hist": candidate["macd_hist"],
                        "macd_hist_slope": candidate["macd_hist_slope"],
                        "upper_wick_ratio": candidate["upper_wick_ratio"],
                        "retest_required": False,
                        "late_breakout_guard_reason": candidate["late_breakout_guard_reason"],
                        "fib_position": candidate.get("fib_position", "unknown"),
                        "fib_position_ratio": candidate.get("fib_position_ratio", 0.0),
                        "fib_label": candidate.get("fib_label", "غير معروف"),
                        "had_pullback": candidate.get("had_pullback", False),
                        "pullback_pct": candidate.get("pullback_pct", 0.0),
                        "pullback_label": candidate.get("pullback_label", "غير معروف"),
                        "wave_estimate": candidate.get("wave_estimate", 0),
                        "wave_peaks": candidate.get("wave_peaks", 0),
                        "wave_label": candidate.get("wave_label", "غير معروف"),
                        "entry_maturity": candidate.get("entry_maturity", "unknown"),
                        "maturity_penalty": candidate.get("maturity_penalty", 0.0),
                        "maturity_bonus": candidate.get("maturity_bonus", 0.0),
                        "final_threshold": candidate["final_threshold"],
                        "adjustments_log": candidate["adjustments_log"],
                        "warning_penalty": candidate["warning_penalty"],
                        "warning_penalty_details": candidate["warning_penalty_details"],
                        "falling_knife_risk": candidate["falling_knife_risk"],
                        "falling_knife_reasons": candidate["falling_knife_reasons"],
                        "target_method": candidate["target_method"],
                        "nearest_resistance": candidate["nearest_resistance"],
                        "nearest_support": candidate["nearest_support"],
                        "resistance_warning": candidate["resistance_warning"],
                        "support_warning": candidate["support_warning"],
                        "target_notes": candidate["target_notes"],
                        "sl_method": candidate["sl_method"],
                        "sl_notes": candidate["sl_notes"],
                        "wave_context": candidate["wave_context"],
                        "setup_context": candidate["setup_context"],
                        "reversal_quality": candidate["reversal_quality"],
                        "reversal_structure_confirmed": candidate["reversal_structure_confirmed"],
                        "strong_bull_pullback": candidate["strong_bull_pullback"],
                        "strong_breakout_exception": candidate["strong_breakout_exception"],
                        "htf_1h_context": candidate["htf_1h_context"],
                        "htf_4h_context": candidate["htf_4h_context"],
                        "has_extra_strong_setup": candidate["has_extra_strong_setup"],
                        "extra_setup_names": candidate["extra_setup_names"],
                        "extra_setup_bonus": candidate["extra_setup_bonus"],
                        "primary_extra_setup": candidate["primary_extra_setup"],
                        "extra_setups_details": candidate.get("extra_setups_details", {}),
                        "context_setups": candidate.get("context_setups", []),
                        "tp1_close_pct": TP1_CLOSE_PCT,
                        "tp2_close_pct": TP2_CLOSE_PCT,
                        "move_sl_to_entry_after_tp1": MOVE_SL_TO_ENTRY_AFTER_TP1,
                        "execution_entry": candidate.get("execution_entry"),
                        "execution_sl": candidate.get("execution_sl"),
                        "execution_tp1": candidate.get("execution_tp1"),
                        "execution_tp2": candidate.get("execution_tp2"),
                        "block_exception": candidate.get("block_exception", False),
                        "block_longs_execution_candidate": candidate.get("block_longs_execution_candidate", False),
                        "current_mode": current_mode,
                        "market_mode": current_mode,
                        "relative_strength_short": candidate.get("relative_strength_short"),
                        "relative_strength_24": candidate.get("relative_strength_24"),
                        "relative_strength_vs_btc": candidate.get("relative_strength_vs_btc"),
                        "execution_setup_tags": candidate.get("execution_setup_tags", []),
                        "execution_candidate_badged": bool(candidate.get("execution_candidate_badged", False)),
                        "execution_candidate_badge_sent": bool(candidate.get("execution_candidate_badge_sent", candidate.get("execution_candidate_badged", False))),
                    }
                    save_alert_snapshot(alert_snapshot, message_id=message_id)
                    if candidate.get("execution_candidate_badged"):
                        candidate.setdefault("execution_status", "candidate_only")
                    else:
                        candidate["execution_status"] = "not_candidate"
                        candidate.setdefault("execution_reject_reason", "no_execution_badge")
                    candidate_alert_id = candidate["alert_id"]
                    register_ok = register_trade_from_candidate(candidate)
                    set_alert_registration_status(candidate_alert_id, register_ok)
                    if not register_ok:
                        logger.error(
                            f"REGISTRATION FAILED after send: symbol={symbol}, "
                            f"alert_id={candidate_alert_id}, setup={candidate['setup_type']}, "
                            f"mode={current_mode}, entry={candidate.get('entry')}, "
                            f"sl={candidate.get('sl')}, tp1={candidate.get('tp1')}, "
                            f"tp2={candidate.get('tp2')}"
                        )

                    try:
                        gate_decision = _decide_long_execution_candidate(candidate, mutate=True)
                        if not gate_decision.get("allowed"):
                            gate_reason = gate_decision.get("reason", "not_execution_candidate")
                            update_execution_status_for_candidate(candidate, "not_candidate", gate_reason, message_sent=False)
                            logger.info(
                                f"EXEC SKIP: {symbol} is not an execution candidate | "
                                f"gate_path={gate_decision.get('path')} | reason={gate_reason}"
                            )
                        elif is_execution_paused():
                            exec_status = "execution_paused"
                            exec_reason = "execution_paused_manual_or_daily_dd"
                            already_sent = _execution_message_already_sent(candidate, exec_status)
                            update_execution_status_for_candidate(candidate, exec_status, exec_reason, message_sent=True)
                            if not already_sent:
                                send_telegram_message(build_execution_paused_message(symbol))
                            logger.info(f"EXEC PAUSED: {symbol} | message_sent={not already_sent}")
                        else:
                            dd_guard = enforce_execution_daily_drawdown_guard()
                            if dd_guard.get("locked"):
                                exec_status = "daily_drawdown_lock"
                                exec_reason = dd_guard.get("reason", "daily_drawdown_lock")
                                update_execution_status_for_candidate(candidate, exec_status, exec_reason, message_sent=False)
                                send_telegram_message(build_execution_rejection_message(symbol, exec_status, exec_reason))
                                logger.info(f"EXEC RESULT: {symbol} | status={exec_status} | reason={exec_reason} | has_message=True")
                            elif EXECUTION_AVAILABLE:
                                candidate = _apply_market_execution_fallback(candidate)
                                _ensure_execution_setup_tags(candidate)
                                if not _candidate_has_complete_execution_plan(candidate):
                                    exec_status = "rejected_invalid_order"
                                    exec_reason = "missing_or_invalid_entry_sl_tp"
                                    update_execution_status_for_candidate(candidate, exec_status, exec_reason, message_sent=False)
                                    send_telegram_message(build_execution_rejection_message(symbol, exec_status, exec_reason))
                                    logger.info(f"EXEC RESULT: {symbol} | status={exec_status} | reason={exec_reason} | has_message=True")
                                else:
                                    exec_result = process_trade_candidate(r, symbol, candidate)
                                    raw_status = exec_result.get("status")
                                    raw_reason = exec_result.get("reason", "")
                                    exec_status = _normalize_execution_status(raw_status, raw_reason)
                                    execution_message = exec_result.get("execution_message")
                                    has_message = bool(execution_message)
                                    if exec_status in ("accepted_preview", "pending_pullback_preview"):
                                        if execution_message:
                                            send_telegram_message(execution_message)
                                        update_execution_status_for_candidate(candidate, exec_status, raw_reason, message_sent=has_message)
                                    else:
                                        rejection_message = build_execution_rejection_message(symbol, exec_status, raw_reason)
                                        send_telegram_message(rejection_message)
                                        has_message = True
                                        update_execution_status_for_candidate(candidate, exec_status, raw_reason, message_sent=True)
                                    logger.info(
                                        f"EXEC RESULT: {symbol} | status={exec_status} | reason={raw_reason} | has_message={has_message}"
                                    )
                            else:
                                update_execution_status_for_candidate(candidate, "preview_rejected", "execution_module_not_available", message_sent=False)
                    except Exception as _exec_e:
                        logger.error(f"Execution preview error for {symbol}: {_exec_e}")
                    logger.info(f"✅ SENT LONG ---> {symbol}")
                else:
                    release_signal_slot(symbol, candidate["candle_time"], "long")
                    logger.error(f"❌ FAILED SEND ---> {symbol}")
            if sent_count > 0:
                set_global_cooldown()
                logger.info(f"Global long cooldown set for {GLOBAL_COOLDOWN_SECONDS}s after {sent_count} alert(s)")
            exec_candidates_this_run = 0
            try:
                exec_candidates_this_run = sum(1 for c in top_candidates if bool(c.get("execution_candidate_badged")))
            except Exception:
                exec_candidates_this_run = 0
            logger.info(f"Sent long alerts this run: {sent_count}")
            logger.info(
                "LONG RUN SUMMARY v202 | "
                f"scanned={tested} | "
                f"rejected_hard={sum(LONG_REJECTION_REASON_COUNTER.values())} | "
                f"soft_penalty_only={LONG_LAYER_COUNTER.get('soft_penalty_only', 0)} | "
                f"candidates_before_momentum={locals().get('candidates_before_momentum', 0)} | "
                f"candidates_after_ranking={locals().get('candidates_after_momentum', 0)} | "
                f"top_candidates={locals().get('top_candidates_count', 0)} | sent={sent_count} | "
                f"exec_candidates={exec_candidates_this_run} | "
                f"top_reject={_format_long_run_rejection_top(5)} | "
                f"top_soft_warning={_format_long_run_soft_warning_top(5)}"
            )
            logger.info(f"Tested {tested} pairs")
            logger.info(f"Scan complete. Sleeping {SCAN_LOOP_SLEEP_SECONDS}s before next run...")
            time.sleep(SCAN_LOOP_SLEEP_SECONDS)
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            time.sleep(10)
        finally:
            if scan_locked:
                release_scan_lock()

def acquire_fast_mode_monitor_lock() -> bool:
    if not r:
        return True
    try:
        return bool(r.set(FAST_MODE_MONITOR_LOCK_KEY, "1", ex=FAST_MODE_MONITOR_LOCK_TTL, nx=True))
    except Exception:
        return False


def run_fast_market_mode_monitor():
    """Lightweight mode checker independent from the full 200-pair scan.

    It prevents the old delay problem: market mode can change every ~75 seconds
    using BTC 15m + top 30 alts 15m, even if the full scan is still busy.
    """
    logger.info(f"⚡ Fast market mode monitor entered | every {FAST_MODE_MONITOR_INTERVAL}s")
    while True:
        try:
            if not acquire_fast_mode_monitor_lock():
                time.sleep(FAST_MODE_MONITOR_INTERVAL)
                continue

            ranked_pairs = get_ranked_pairs()
            if not ranked_pairs:
                time.sleep(FAST_MODE_MONITOR_INTERVAL)
                continue

            sample_pairs = sorted(
                ranked_pairs,
                key=lambda x: x.get("_rank_volume_24h", 0),
                reverse=True,
            )[:FAST_MODE_MONITOR_SAMPLE_SIZE]
            guard_candles_map = fetch_candles_parallel(
                sample_pairs,
                timeframe="15m",
                limit=MARKET_GUARD_CANDLE_LIMIT,
                max_workers=min(MAX_CANDLE_FETCH_WORKERS, 8),
            )
            if "BTC-USDT-SWAP" not in guard_candles_map:
                guard_candles_map["BTC-USDT-SWAP"] = get_candles("BTC-USDT-SWAP", "15m", 5)

            btc_mode = get_btc_mode()
            btc_zone = get_btc_range_zone(timeframe="1H", lookback=50)

            alt_snapshot = None
            if r:
                try:
                    cached_snapshot = r.get(ALT_SNAPSHOT_CACHE_KEY)
                    if cached_snapshot:
                        alt_snapshot = json.loads(cached_snapshot)
                except Exception:
                    alt_snapshot = None
            if alt_snapshot is None:
                alt_snapshot = get_alt_market_snapshot(ranked_pairs)
                if r:
                    try:
                        r.set(ALT_SNAPSHOT_CACHE_KEY, json.dumps(alt_snapshot), ex=ALT_SNAPSHOT_CACHE_TTL)
                    except Exception:
                        pass

            market_info = get_market_state(btc_mode, alt_snapshot)
            market_guard = get_market_guard_snapshot(
                ranked_pairs=ranked_pairs,
                btc_mode=btc_mode,
                alt_snapshot=alt_snapshot,
                candles_map_15m=guard_candles_map,
                btc_zone=btc_zone,
            )
            current_mode = r.get(MARKET_MODE_KEY) if r else MODE_NORMAL_LONG
            if not current_mode:
                current_mode = MODE_NORMAL_LONG

            market_engine_context = build_market_engine_context(
                market_guard=market_guard,
                market_state=market_info.get("market_state", "mixed"),
                btc_mode=btc_mode,
                alt_snapshot=alt_snapshot,
            )
            mode_result = determine_long_market_mode(
                market_guard=market_guard,
                market_state=market_info.get("market_state", "mixed"),
                btc_mode=btc_mode,
                alt_snapshot=alt_snapshot,
                current_mode=current_mode,
                allow_state_writes=True,
            )
            mode_result = apply_fast_intraday_override(mode_result, market_engine_context, current_mode)
            new_mode = handle_market_mode_transition(mode_result)

            try:
                snapshot_data = {
                    "created_ts": int(time.time()),
                    "source": "fast_market_mode_monitor",
                    "current_mode": new_mode,
                    "mode_reason": mode_result.get("reason", ""),
                    "human_reason": mode_result.get("human_reason", ""),
                    "btc_mode": btc_mode,
                    "btc_zone": btc_zone,
                    "alt_snapshot": alt_snapshot,
                    "market_info": market_info,
                    "market_guard": market_guard,
                    "market_engine_context": market_engine_context,
                    "ranked_pairs_count": len(ranked_pairs),
                    "suggested_mode": mode_result.get("mode", new_mode),
                    "suggested_reason": mode_result.get("reason", ""),
                }
                save_market_status_snapshot(snapshot_data)
            except Exception:
                pass

            logger.info(
                "FAST MODE CHECK | "
                f"mode={new_mode} | red={market_guard.get('red_ratio_15m')} | "
                f"avg={market_guard.get('avg_change_15m')} | btc={market_guard.get('btc_change_15m')} | "
                f"stress={market_engine_context.get('intraday_stress')} | reason={mode_result.get('reason')}"
            )
        except Exception as e:
            logger.warning(f"Fast market mode monitor error: {e}")
        time.sleep(FAST_MODE_MONITOR_INTERVAL)


def run():
    logger.info(
        f"LONG BOT STARTED | pid={os.getpid()} | replica={os.getenv('RAILWAY_REPLICA_ID', 'unknown')}"
    )
    clear_webhook()
    clear_stale_scan_locks_on_startup()
    command_thread = threading.Thread(target=run_command_poller, daemon=True)
    command_thread.start()
    fast_mode_thread = threading.Thread(target=run_fast_market_mode_monitor, daemon=True)
    fast_mode_thread.start()
    run_scanner_loop()

if __name__ == "__main__":
    run()
