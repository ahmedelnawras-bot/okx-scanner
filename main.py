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
) 
from analysis.rejection_tracking import (
    log_rejected_candidate,
    build_rejections_report_message,
)

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
COMMAND_POLL_INTERVAL = 3
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
SCAN_LOCK_TTL = 600

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

# Track leverage display
TRACK_LEVERAGE = 15.0

# Partial Take Profit Management
TP1_CLOSE_PCT = 50
TP2_CLOSE_PCT = 50
MOVE_SL_TO_ENTRY_AFTER_TP1 = True

# Market Guard / Modes
MARKET_MODE_KEY = "market_mode:long:current"
MARKET_MODE_LAST_KEY = "market_mode:long:last_mode"
MARKET_MODE_LAST_TRANSITION_KEY = "market_mode:long:last_transition_ts"
MARKET_MODE_LAST_RECOVERY_CHECK_KEY = "market_mode:long:last_recovery_check_ts"
MARKET_MODE_NORMAL_CANDIDATE_KEY = "market_mode:long:normal_candidate_since"
MARKET_MODE_BLOCK_STARTED_KEY = "market_mode:long:block_started_ts"
MARKET_MODE_LAST_SAFE_SEEN_KEY = "market_mode:long:last_safe_seen_ts"

MODE_NORMAL_LONG = "NORMAL_LONG"
MODE_STRONG_LONG_ONLY = "STRONG_LONG_ONLY"
MODE_BLOCK_LONGS = "BLOCK_LONGS"
MODE_RECOVERY_LONG = "RECOVERY_LONG"

MODE_TRANSITION_MIN_INTERVAL = 240
RECOVERY_CHECK_INTERVAL = 120
NORMAL_CANDIDATE_DURATION = 180
BLOCK_EXIT_CONFIRM_DURATION = 240
STRONG_TO_NORMAL_CONFIRM_DURATION = 180

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
last_global_send_ts = 0.0


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

def acquire_scan_lock() -> bool:
 if not r:
    return True
 try:
    locked = r.set(SCAN_LOCK_KEY, "1", ex=SCAN_LOCK_TTL, nx=True)
    return bool(locked)
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
# NEW HELPER: consolidated rejection logging
# =========================
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
):
    try:
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
            extra=extra,
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
    # wave_5_no_pullback
    if wave_estimate >= 5 and not had_pullback:
        return "wave_5_no_pullback"
    # overextended_late_entry
    if fib_position == "overextended" or "امتداد سعري" in str(entry_timing):
        return "overextended_late_entry"
    # post_dump_weak_rebound
    if dist_ma is not None and dist_ma < -2.0 and rsi_now is not None and rsi_now < 45 and not breakout and not pre_breakout and not mtf_confirmed:
        return "post_dump_weak_rebound"
    # weak_recovery_below_ma
    if dist_ma is not None and dist_ma < 0 and not mtf_confirmed and not breakout and not pre_breakout:
        return "weak_recovery_below_ma"
    # no_structure_break
    if not breakout and not pre_breakout:
        return "no_structure_break"
    # low_volume_bounce
    if vol_ratio is not None and vol_ratio < 1.0:
        return "low_volume_bounce"
    return "hard_late_entry"

# =========================
# ECONOMIC CALENDAR
# =========================
def get_upcoming_high_impact_events(window_hours: int = NEWS_WINDOW_HOURS) -> list:
 try:
    now = int(time.time())
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
 except Exception as e:
    logger.warning(f"Economic calendar error: {e}")
    return []


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

def send_telegram_reply(chat_id: str, message: str) -> bool:
 if not BOT_TOKEN or not chat_id:
    logger.error("❌ Telegram reply config missing")
    return False
 payload = {
    "chat_id": chat_id,
    "text": message,
    "parse_mode": "HTML",
    "disable_web_page_preview": True,
 }
 data = telegram_api_call("sendMessage", payload)
 return bool(data.get("ok"))

def get_telegram_updates(offset: int = 0):
 if not BOT_TOKEN:
    return []
 url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
 params = {"timeout": 1, "offset": offset}
 try:
    response = requests.get(url, params=params, timeout=5)
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


def build_help_message() -> str:
 return """<b>📋 OKX Scanner Bot - LONG</b>

<b>⚡ أوامر سريعة:</b>
/mood - حالة السوق والمود الحالي
/status - نفس أمر /mood
/report_1h - تقرير آخر ساعة
/report_today - تقرير اليوم
/report_7d - تقرير آخر 7 أيام
/report_30d - تقرير آخر 30 يوم
/report_all - كل الصفقات

<b>📊 تحليل الأداء:</b>
/report_deep - تحليل متقدم شامل
/report_exits - جودة الخروج TP1/TP2/SL
/report_rejections - تحليل أسباب رفض الفرص
/report_setups - أفضل وأسوأ أنواع الإشارات
/report_scores - تحليل السكور
/report_market - الأداء حسب حالة السوق
/report_losses - تحليل أسباب الخسارة
/report_diagnostics - تقرير تشخيصي كامل

<b>🛠 إدارة:</b>
/reset_stats - تصفير إحصائيات اللونج
/stats_since_reset - الأداء منذ آخر تصفير
/how_it_work - شرح طريقة عمل البوت

<b>ℹ️ ملاحظة:</b>
البوت يستخدم Entry Maturity لتقليل الدخول المتأخر في نهاية الموجة."""

def build_how_it_work_message() -> str:
 return """📘 <b>كيف يعمل بوت اللونج؟</b>

🧠 <b>فكرة البوت:</b>
البوت يبحث عن فرص <b>Long Futures</b> على OKX - بفترة متوازنة حتى لا يخنق الإشارات الجيدة.

🔍 <b>منطق العمل:</b>
1. اختيار العملات الأعلى سيولة وحجم تداول
2. تحليل فريم 15m
3. قياس قوة الزخم الصاعد
4. تقييم:
• الفوليوم
• RSI
• موقع السعر من المتوسط
• Breakout / Pre-Breakout
• تأكيد فريم 1H
• حالة السوق العامة
• Entry Maturity (موجات وبولباك)
5. Smart Early Priority للإشارات المبكرة
6. إعطاء Score من 10
7. إرسال فقط الفرص المقبولة نهائيًا

📈 <b>Track:</b>
يعرض الحقًا أداء الصفقة بعد إرسالها"""

def reset_stats(chat_id: str):
 if not r:
    send_telegram_reply(chat_id, "❌ Redis غير متصل")
    return
 if ADMIN_CHAT_IDS and str(chat_id) not in ADMIN_CHAT_IDS:
    send_telegram_reply(chat_id, f"⛔ غير مسموح\nchat_id={chat_id}")
    logger.warning(f"reset_stats blocked for non-admin chat_id={chat_id}")
    return
 try:
    keys = r.keys("trade:futures:long:*")
    deleted = 0
    for key in keys:
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


# ----------- Helper functions for exits report -----------
def _load_long_trades_from_redis(limit: int = 700) -> list:
    trades = []
    if not r:
        return trades
    try:
        keys = r.keys("trade:futures:long:*")
        for key in keys[:limit]:
            try:
                raw = r.get(key)
                if not raw:
                    continue
                data = json.loads(raw)
                if isinstance(data, dict):
                    data["_redis_key"] = key
                    trades.append(data)
            except Exception:
                continue

        trades.sort(
            key=lambda x: int(float(x.get("created_ts") or x.get("candle_time") or 0)),
            reverse=True
        )
        return trades
    except Exception as e:
        logger.error(f"_load_long_trades_from_redis error: {e}")
        return trades


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
        if result == "tp1_win":
            return "tp1_only"
        if result == "loss":
            return "loss"
        if result == "expired":
            return "expired"
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
    # try stored fields
    for field in ["realized_leveraged_pnl_pct", "leveraged_pnl_pct", "pnl_pct", "result_pct", "realized_pnl_pct", "raw_pnl_pct"]:
        val = trade.get(field)
        if val is not None:
            try:
                f = float(val)
                if f != f:
                    continue
                return f
            except Exception:
                continue
    # estimate from entry/sl/tp1/tp2
    entry = _safe_float(trade.get("entry"), None)
    if entry is None or entry <= 0:
        return 0.0
    result = str(trade.get("result", "") or "").lower()
    if result == "loss":
        sl = _safe_float(trade.get("sl"), 0)
        if sl > 0:
            return round(((sl - entry) / entry) * 100 * TRACK_LEVERAGE, 4)
        return 0.0
    if result == "tp2_win":
        tp2 = _safe_float(trade.get("tp2"), 0)
        if tp2 > 0:
            return round(((tp2 - entry) / entry) * 100 * TRACK_LEVERAGE, 4)
        return 0.0
    if result == "tp1_win":
        tp1 = _safe_float(trade.get("tp1"), 0)
        if tp1 > 0:
            return round(((tp1 - entry) / entry) * 100 * TRACK_LEVERAGE, 4)
        return 0.0
    if result == "expired":
        last_price = _safe_float(trade.get("last_price"), 0)
        if last_price > 0:
            return round(((last_price - entry) / entry) * 100 * TRACK_LEVERAGE, 4)
        return 0.0
    if bool(trade.get("protected_breakeven_exit", False)):
        return 0.0
    # partial or other: use current approximate? skip for now
    return 0.0


def _is_trade_win_bucket(bucket: str) -> bool:
    return bucket in ("tp2", "tp1_only", "partial", "breakeven_protected", "tp1_hit_open_or_unknown")


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
        tp1_count = sum(1 for _, b in items if b in ("tp2", "tp1_only", "partial", "tp1_hit_open_or_unknown"))
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
    # sort by total desc, take top max_items
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
        tp1_effective = len(tp2_wins) + len(tp1_only) + len(partial) + len(tp1_hit_other)
        tp2_effective = len(tp2_wins)
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

        lines = [
            "📊 <b>تقرير جودة الخروج الشامل - LONG</b>",
            "",
            f"• إجمالي الصفقات: {total}",
            f"• مفتوح: {len(open_trades)}",
            f"• مغلق: {closed_count}",
            f"• 🎯 TP2: {tp2_effective}",
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
 """حفظ Snapshot لحالة السوق في Redis مع TTL."""
 if not r or not snapshot:
    return
 try:
    r.set(MARKET_STATUS_SNAPSHOT_KEY, json.dumps(snapshot, ensure_ascii=False), ex=MARKET_STATUS_SNAPSHOT_TTL)
 except Exception as e:
    logger.warning(f"Failed to save market status snapshot: {e}")

def load_market_status_snapshot(max_age_seconds: int = 240):
 """تحميل Snapshot من Redis مع التحقق من العمر."""
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
# MARKET STATUS MESSAGE
# =========================
def build_market_status_message() -> str:
 """
 يعرض حالة السوق الحالية والمود الحالي للبوت LONG.
 يحاول استخدام Snapshot محفوظ أولاً، وإلا يقوم بعمل Live calculation.
 """
 try:
    snapshot = load_market_status_snapshot(max_age_seconds=300)
    if snapshot:
        created_ts = int(snapshot.get("created_ts", 0))
        age_seconds = max(0, int(time.time()) - created_ts)
        age_str = f"منذ {age_seconds}s" if age_seconds < 60 else f"منذ {age_seconds // 60}m"
        current_mode = normalize_market_mode(snapshot.get("current_mode", MODE_NORMAL_LONG))
        mode_reason = snapshot.get("mode_reason", "")
        btc_mode = snapshot.get("btc_mode", "🟡 محايد")
        alt_snapshot = snapshot.get("alt_snapshot", {})
        alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك")
        market_info = snapshot.get("market_info", {})
        market_state_label = market_info.get("market_state_label", "Mixed")
        market_bias_label = market_info.get("market_bias_label", "السوق مختلط")
        market_guard = snapshot.get("market_guard", {})
        red_ratio = float(market_guard.get("red_ratio_15m", 0.0) or 0.0)
        avg_change = float(market_guard.get("avg_change_15m", 0.0) or 0.0)
        btc_change = float(market_guard.get("btc_change_15m", 0.0) or 0.0)
        valid_count = int(market_guard.get("valid_count", 0) or 0)
        guard_level = str(market_guard.get("level", "normal"))
        suggested_mode = normalize_market_mode(snapshot.get("current_mode", MODE_NORMAL_LONG))
        suggested_reason = mode_reason
        data_source = f"📦 Snapshot ({age_str})"
    else:
        current_mode = normalize_market_mode(r.get(MARKET_MODE_KEY) if r else MODE_NORMAL_LONG)
        if not current_mode:
            current_mode = MODE_NORMAL_LONG
        btc_mode = get_btc_mode()
        ranked_pairs = get_ranked_pairs()
        alt_snapshot = get_alt_market_snapshot(ranked_pairs)
        alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك")
        market_info = get_market_state(btc_mode, alt_snapshot)
        market_state_label = market_info.get("market_state_label", "Mixed")
        market_bias_label = market_info.get("market_bias_label", "السوق مختلط")
        market_guard = get_market_guard_snapshot(ranked_pairs, btc_mode, alt_snapshot)
        red_ratio = float(market_guard.get("red_ratio_15m", 0.0) or 0.0)
        avg_change = float(market_guard.get("avg_change_15m", 0.0) or 0.0)
        btc_change = float(market_guard.get("btc_change_15m", 0.0) or 0.0)
        valid_count = int(market_guard.get("valid_count", 0) or 0)
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
        age_seconds = 0
        data_source = "⚠️ Live (Snapshot غير متاح)"
        age_str = ""

    mode_ar = {
        MODE_NORMAL_LONG: "🟢 NORMAL LONG",
        MODE_STRONG_LONG_ONLY: "🟡 STRONG LONG ONLY",
        MODE_BLOCK_LONGS: "🔴 BLOCK LONGS",
        MODE_RECOVERY_LONG: "🟠 RECOVERY LONG",
    }.get(current_mode, html.escape(str(current_mode)))

    suggested_mode_ar = {
        MODE_NORMAL_LONG: "🟢 NORMAL LONG",
        MODE_STRONG_LONG_ONLY: "🟡 STRONG LONG ONLY",
        MODE_BLOCK_LONGS: "🔴 BLOCK LONGS",
        MODE_RECOVERY_LONG: "🟠 RECOVERY LONG",
    }.get(suggested_mode, html.escape(str(suggested_mode)))

    if current_mode == MODE_NORMAL_LONG:
        action = "✅ مسموح باللونج العادي مع الالتزام بالفلاتر."
    elif current_mode == MODE_STRONG_LONG_ONLY:
        action = "⚠️ لا تدخل إلا الفرص القوية جدًا: Breakout / Pre-Breakout / MTF وفوليوم واضح."
    elif current_mode == MODE_BLOCK_LONGS:
        action = "🛑 الأفضل وقف اللونج الجديد مؤقتًا وانتظار خروج السوق من الضغط."
    elif current_mode == MODE_RECOVERY_LONG:
        action = "🛟 فرص Recovery فقط وبحجم صغير، ولا تتعامل معها كإشارة لونج عادية."
    else:
        action = "ℹ️ مراقبة السوق قبل الدخول"

    lines = [
        "🧭 <b>Market Mood - LONG</b>",
        f"⚙️ <b>المود الحالي:</b> {mode_ar}",
        f"🔮 <b>المود المحسوب الآن:</b> {suggested_mode_ar}",
        f"🧠 <b>سبب الحساب:</b> {html.escape(str(suggested_reason))}",
        f"📡 <b>مصدر البيانات:</b> {data_source}",
    ]
    if age_seconds > 0:
        lines.append(f"⏱ <b>عمر البيانات:</b> {age_str}")
    lines.extend([
        "",
        "🌍 <b>السوق:</b>",
        f"• BTC: {html.escape(str(btc_mode))}",
        f"• Alt Mode: {html.escape(str(alt_mode))}",
        f"• State: {html.escape(str(market_state_label))}",
        f"• Flow: {html.escape(str(market_bias_label))}",
        "",
        "🛡 <b>Market Guard 15m:</b>",
        f"• Level: {html.escape(guard_level)}",
        f"• Sample: {valid_count}",
        f"• Red Ratio: {red_ratio * 100:.1f}%",
        f"• Avg 15m: {avg_change:+.2f}%",
        f"• BTC 15m: {btc_change:+.2f}%",
        "",
        f"🎯 <b>التصرف:</b> {html.escape(action)}",
    ])
    if suggested_mode != current_mode:
        lines.append("")
        lines.append("ℹ️ المود المحسوب مختلف عن المخزن، وسيتحدث مع دورة الفحص القادمة")
    return "\n".join(lines)
 except Exception as e:
    logger.error(f"build_market_status_message error: {e}")
    return f"❌ حصل خطأ أثناء بناء حالة السوق\n{html.escape(str(e))}"

# =========================
# COMMAND HANDLERS
# =========================
COMMAND_HANDLERS = {
 "/help": lambda chat_id: send_telegram_reply(chat_id, build_help_message()),
 "/mood": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/status": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/market": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/market_status": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/market_mode": lambda chat_id: send_telegram_reply(chat_id, build_market_status_message()),
 "/how_it_work": lambda chat_id: send_telegram_reply(chat_id, build_how_it_work_message()),
 "/report_1h": lambda chat_id: send_telegram_reply(chat_id, build_report_message("1h")),
 "/report_today": lambda chat_id: send_telegram_reply(chat_id, build_report_message("today")),
 "/report_month": lambda chat_id: send_telegram_reply(chat_id, build_report_message("month")),
 "/report_30d": lambda chat_id: send_telegram_reply(chat_id, build_report_message("month")),
 "/report_all": lambda chat_id: send_telegram_reply(chat_id, build_report_message("all")),
 "/report_deep": lambda chat_id: send_telegram_reply(chat_id, build_deep_report_message()),
 "/report_setups": lambda chat_id: send_telegram_reply(chat_id, build_setups_report(r, market_type="futures", side="long", period="all")),
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
            return 0.0, 0.0, 0.0, 0.0

        ts_col = df["ts"].astype(float)
        since_ms = since_ts * 1000 if since_ts < 10_000_000_000 else since_ts
        work = df[ts_col > since_ms].copy()
        if work.empty:
            work = df.tail(20).copy()

        lows = work["low"].astype(float)
        highs = work["high"].astype(float)
        if lows.empty or highs.empty or entry <= 0:
            return 0.0, 0.0, 0.0, 0.0

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
        return 0.0, 0.0, 0.0, 0.0

def get_alert_status(alert: dict) -> str:
 try:
    symbol = alert["symbol"]
    mode = alert.get("mode") or alert.get("market_mode", "")
    avg_planned = _safe_float(alert.get("average_planned_entry"), 0.0)
    entry = _safe_float(alert.get("entry"), 0)
    recommended_entry = _safe_float(alert.get("recommended_entry"), 0.0)
    pullback_entry = _safe_float(alert.get("pullback_entry"), 0.0)
    if mode == MODE_RECOVERY_LONG and avg_planned > 0:
        effective_entry = avg_planned
    else:
        effective_entry = recommended_entry if recommended_entry > 0 else pullback_entry if pullback_entry > 0 else entry
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
 if "TP2" in status:
    return "🎯 TP2 Hit"
 if "TP1" in status:
    return "✅ TP1 Hit"
 if "SL" in status:
    return "❌ SL Hit"
 if current_move > 0.30:
    return "🟢 ربح"
 if current_move < -0.30:
    return "🔴 خسارة"
 if -0.30 <= current_move <= 0.30:
    return "🟡 تعادل"
 return "⚪ غير محدد"

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
    if not raw:
        return None
    data = json.loads(raw)
    return data if isinstance(data, dict) else None
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
    if status == "partial":
        return "✅ TP1 Hit / الصفقة جزئية"
    if result == "tp1_win":
        return "✅ TP1 ثم رجوع Entry"
    if result == "tp2_win":
        return "🎯 TP2 Hit"
    if result == "loss":
        return "❌ SL Hit"
    if result == "expired":
        return "⏳ Expired"
    if status == "open":
        return "⏳ Open"
    if tp1_hit:
        return "✅ TP1 Hit"
    return ""
 except Exception:
    return ""

def build_track_message(alert: dict) -> str:
 try:
    symbol = clean_symbol_for_message(alert.get("symbol", "Unknown"))
    mode = alert.get("mode") or alert.get("market_mode", "")
    avg_planned = _safe_float(alert.get("average_planned_entry"), 0.0)
    entry = _safe_float(alert.get("entry"), 0.0)
    recommended_entry = _safe_float(alert.get("recommended_entry"), 0.0)
    pullback_entry = _safe_float(alert.get("pullback_entry"), 0.0)
    sl = _safe_float(alert.get("sl"), 0.0)
    tp1 = _safe_float(alert.get("tp1"), 0.0)
    tp2 = _safe_float(alert.get("tp2"), 0.0)
    candle_time = int(_safe_float(alert.get("candle_time"), 0))
    created_ts = int(_safe_float(alert.get("created_ts"), candle_time))
    current_price = get_last_price(alert.get("symbol", ""))
    if mode == MODE_RECOVERY_LONG and avg_planned > 0:
        effective_entry = avg_planned
    else:
        effective_entry = recommended_entry if recommended_entry > 0 else pullback_entry if pullback_entry > 0 else entry
    favorable_price, favorable_pct, adverse_price, adverse_pct = get_max_move_since_alert(
        symbol=alert.get("symbol", ""),
        since_ts=candle_time,
        entry=effective_entry,
        side="long",
    )
    status = get_alert_status(alert)
    duration_seconds = max(0, int(time.time()) - created_ts)
    duration_h = duration_seconds // 3600
    duration_m = (duration_seconds % 3600) // 60
    current_move = 0.0
    if effective_entry > 0 and current_price > 0:
        current_move = round(((current_price - effective_entry) / effective_entry) * 100, 2)
    state_badge = get_track_state_badge(status, current_move)
    tv_link = build_track_tradingview_link(alert.get("symbol", ""))
    leveraged_current = round(current_move * TRACK_LEVERAGE, 2)
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
    official_trade = load_registered_trade_for_alert(alert)
    official_status = format_official_trade_status(official_trade)
    msg = (
        f"📌 <b>Alert Track</b>\n\n"
        f"🪙 {html.escape(symbol)}\n"
        f"📈 Long\n"
        f"⏱ {html.escape(str(alert.get('timeframe', TIMEFRAME)))}\n"
        f"{recovery_extra}\n"
        f"💰 Signal Entry: {entry:.6f}\n"
    )
    if recommended_entry > 0 and recommended_entry != entry:
        msg += f"📌 Recommended Entry: {recommended_entry:.6f}\n"
    if effective_entry != entry and mode != MODE_RECOVERY_LONG:
        msg += f"⚡ Effective Entry: {effective_entry:.6f}\n"
    msg += (
        f"🛑 SL: {sl:.6f}\n"
        f"🎯 TP1: {tp1:.6f} | إغلاق 50%\n"
        f"🏁 TP2: {tp2:.6f} | إغلاق 50%\n"
        f"🛡 بعد TP1: نقل SL إلى Entry\n\n"
        f"{state_badge}\n"
        f"📊 {html.escape(status)}\n"
    )
    if official_status:
        msg += f"🏛 {html.escape(official_status)}\n"
    msg += (
        f"💵 السعر الحالي: {current_price:.6f}\n"
        f"🔢 الرافعة: {TRACK_LEVERAGE:.0f}x\n"
        f"📈 التغير الحالي: {current_move:.2f}% | بعد الرافعة: {leveraged_current:.2f}%\n"
        f"🚀 أقصى صعود: {favorable_price:.6f} | +{favorable_pct:.2f}% | بعد الرافعة: +{leveraged_favorable:.2f}%\n"
        f"📉 أقصى هبوط ضدك: {adverse_price:.6f} | +{adverse_pct:.2f}% | بعد الرافعة: +{leveraged_adverse:.2f}%\n"
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
) -> dict:
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
    extreme_late_pump = (
        dist_ma >= 5.2
        and rsi_now >= 70
        and vol_ratio >= 2.0
        and candle_strength >= 0.65
    )
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
    }
 except Exception:
    return {
        "late_pump_risk": False,
        "extreme_late_pump": False,
        "bull_continuation_risk": False,
        "should_block": False,
        "reasons": [],
        "checks": 0,
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
    elif alt_strength_score >= 0.50:
        alt_mode = "🟡 متماسك"
    else:
        alt_mode = "🔴 ضعيف"
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
        "alt_mode": "تماسك",
    }

def get_market_state(btc_mode: str, alt_snapshot: dict):
 alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك")
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
 if "صاعد" in btc_mode and ("قوي" in alt_mode or "متماسك" in alt_mode):
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
    min_sl = round(float(price) * 0.990, 6)
    max_sl = round(float(price) * 0.960, 6)
    return max(max_sl, min(min_sl, sl))
 except Exception:
    return round(float(price) * 0.975, 6)

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
            },
        )
    except Exception:
        pass

 strong_candidates = []

 # If few candidates, relax thresholds
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
        else:
            _log_top_momentum_rejection(c, reason="top_momentum_min_score")

    strong_candidates.sort(
        key=lambda x: (x["momentum_priority"], x["score"], x["change_24h"], x["rank_volume_24h"]),
        reverse=True
    )
    logger.info(f"Top momentum small-candidate mode: kept {len(strong_candidates)} of {len(candidates)}")
    return strong_candidates

 # Normal mode
 min_score = get_effective_min_score(False, False)
 for c in candidates:
    if c["score"] >= min_score:
        if not (c.get("breakout") or c.get("pre_breakout") or c.get("early_priority") == "strong"
                or c.get("strong_bull_pullback") or c.get("strong_breakout_exception") or c.get("is_reverse")):
            if c["score"] < 6.8:
                _log_top_momentum_rejection(c, reason="top_momentum_plain_continuation_score")
                continue
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


def _collect_resistance_candidates_long(df, entry: float) -> list:
    candidates = []
    try:
        if df is None or df.empty or entry <= 0:
            return candidates

        signal_row = get_signal_row(df)
        if signal_row is None:
            return candidates

        idx = signal_row.name
        if idx is None:
            return candidates

        start = max(0, idx - SMART_TP1_LOOKBACK_SWING)
        work = df.iloc[start:idx].copy()          # exclude signal candle
        if work.empty:
            return candidates

        highs = work["high"].astype(float)
        closes = work["close"].astype(float)

        # recent swing highs above entry
        for h in highs.tail(30).tolist():
            h = _safe_float(h, 0.0)
            if h > entry:
                candidates.append({
                    "price": h,
                    "source": "swing_high"
                })

        # recent close highs above entry
        for c in closes.tail(30).tolist():
            c = _safe_float(c, 0.0)
            if c > entry:
                candidates.append({
                    "price": c,
                    "source": "recent_close_high"
                })

        # Bollinger upper
        bb_upper = _safe_float(signal_row.get("bb_upper"), 0.0)
        if bb_upper > entry:
            candidates.append({
                "price": bb_upper,
                "source": "bb_upper"
            })

        # previous 20 high
        if len(work) >= 20:
            prev_high = _safe_float(work["high"].tail(20).max(), 0.0)
            if prev_high > entry:
                candidates.append({
                    "price": prev_high,
                    "source": "prev_20_high"
                })

        # simple psychological / round levels
        if SMART_TP1_ROUND_LEVELS_ENABLED:
            round_levels = []
            if entry >= 100:
                step = 5.0
            elif entry >= 10:
                step = 0.5
            elif entry >= 1:
                step = 0.05
            elif entry >= 0.1:
                step = 0.005
            elif entry >= 0.01:
                step = 0.0005
            else:
                step = 0.00005

            try:
                base = int(entry / step) * step
                for i in range(1, 8):
                    level = base + step * i
                    if level > entry:
                        round_levels.append(level)
            except Exception:
                round_levels = []

            for level in round_levels:
                candidates.append({
                    "price": level,
                    "source": "round_level"
                })

        # remove duplicates and invalid
        cleaned = []
        seen = set()
        for item in candidates:
            price = _safe_float(item.get("price"), 0.0)
            if price <= entry:
                continue
            key = round(price, 10)
            if key in seen:
                continue
            seen.add(key)
            cleaned.append({
                "price": price,
                "source": item.get("source", "unknown")
            })

        cleaned.sort(key=lambda x: x["price"])
        return cleaned

    except Exception as e:
        logger.warning(f"_collect_resistance_candidates_long error: {e}")
        return candidates


def find_nearest_resistance_long(df, entry: float):
    try:
        candidates = _collect_resistance_candidates_long(df, entry)
        if not candidates:
            return None
        return candidates[0]
    except Exception:
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

        # لو المقاومة قبل الحد الأدنى 1.2R، نخلي TP1 على 1.2R لكن نطلع تحذير
        if resistance_rr < SMART_TP1_MIN_RR:
            result["tp1"] = _round_price_dynamic(min_tp1)
            result["tp2"] = _round_price_dynamic(calc_tp_long(entry, sl, rr=rr2))
            result["target_method"] = "rr_min_due_close_resistance"
            result["resistance_warning"] = "مقاومة قريبة جدًا قبل TP1"
            result["target_notes"].append(
                f"nearest_resistance_before_min_rr rr={resistance_rr:.2f}"
            )
            result["rr1_effective"] = SMART_TP1_MIN_RR
            return result

        # لو المقاومة بين 1.2R و RR الأصلي، TP1 يكون قبل المقاومة ببوفر
        if SMART_TP1_MIN_RR <= buffered_rr < rr1:
            smart_tp1 = max(min_tp1, resistance_before_buffer)
            result["tp1"] = _round_price_dynamic(smart_tp1)
            result["tp2"] = _round_price_dynamic(calc_tp_long(entry, sl, rr=rr2))
            result["target_method"] = "structure_before_resistance"
            result["target_notes"].append(
                f"tp1_before_resistance source={nearest_resistance_data.get('source', 'unknown')} rr={buffered_rr:.2f}"
            )
            result["rr1_effective"] = round((smart_tp1 - entry) / risk, 2)
            return result

        # لو المقاومة بعيدة، استخدم RR الطبيعي
        result["tp1"] = _round_price_dynamic(rr_tp1)

        # TP2: لو السوق قوي والكسر موجود، نخلي TP2 أعلى قليلًا لو المقاومة بعيدة
        tp2_rr = rr2
        if market_state in ("bull_market", "alt_season") and (breakout or pre_breakout):
            tp2_rr = max(rr2, rr1 + 1.2)

        result["tp2"] = _round_price_dynamic(calc_tp_long(entry, sl, rr=tp2_rr))
        result["target_method"] = "rr_with_structure_check"
        result["target_notes"].append(
            f"resistance_ok source={nearest_resistance_data.get('source', 'unknown')} rr={resistance_rr:.2f}"
        )
        result["rr1_effective"] = rr1
        result["rr2_effective"] = tp2_rr
        return result

    except Exception as e:
        logger.warning(f"build_smart_tp1_long error: {e}")
        result["target_notes"].append("smart_tp1_error_fallback_rr")
        return result

# ==========================================
# NEAR RESISTANCE GUARD (NEW)
# ==========================================
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
    """
    Returns (should_reject: bool, dynamic_penalty: float)
    """
    if not resistance_warning or resistance_warning != "مقاومة قريبة جدًا قبل TP1":
        return False, 0.0

    # Determine penalty based on market state
    if market_state in ("risk_off",):
        base_penalty = 0.75
    elif market_state == "btc_leading":
        base_penalty = 0.60
    elif market_state == "mixed":
        base_penalty = 0.45
    else:  # bull_market, alt_season
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
        # Exception for very strong breakout
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
            return True, 0.0  # reject completely

    # not weak enough to reject, but still penalize harder
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
    pullback_text = (
        f"📥 <b>منطقة دخول البول باك:</b> "
        f"من {fmt_num(pullback_low, 6)} إلى {fmt_num(pullback_high, 6)}\n"
    )
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

 return f"""{header_block}🚀 <b>لونج فيوتشر | {safe_symbol}</b>
💰 <b>السعر:</b> {fmt_num(price, 6)} | ⏱ <b>الفريم:</b> {safe_15m}
⭐ <b>السكور:</b> {rtl_fix(f"{float(score_result['score']):.1f} / 10")}
🏷 <b>التصنيف:</b> {safe_rating}
{pullback_text}
🎯 <b>TP1:</b> {fmt_num(tp1, 6)} ({fmt_pct(tp1_pct)} | {rtl_fix(f"{rr1}R")} | إغلاق 50%)
🏁 <b>TP2:</b> {fmt_num(tp2, 6)} ({fmt_pct(tp2_pct)} | {rtl_fix(f"{rr2}R")} | إغلاق 50%)
🛡 <b>بعد TP1:</b> نقل SL إلى Entry
🛑 <b>SL:</b> {fmt_num(stop_loss, 6)} ({rtl_fix(f"-{abs(float(sl_pct)):.2f}%")})
🧠 <b>نوع الفرصة:</b> {safe_opportunity_type}{reverse_block}{reversal_4h_block}{breakout_quality_block}
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
• TP1: {fmt_num(tp1, 6)} ({fmt_pct(tp1_pct)} | {rtl_fix(f"{rr1}R")} | إغلاق 50%)
• TP2: {fmt_num(tp2, 6)} ({fmt_pct(tp2_pct)} | {rtl_fix(f"{rr2}R")} | إغلاق 50%)
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

def get_market_guard_snapshot(ranked_pairs, btc_mode: str, alt_snapshot: dict) -> dict:
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
    }
 try:
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
        candles = get_candles(
            symbol, MARKET_GUARD_TIMEFRAME, MARKET_GUARD_CANDLE_LIMIT
        )
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
        }
    red_ratio = round(red_count / valid, 4)
    avg_change = round(sum(changes) / len(changes), 4) if changes else 0.0
    btc_candles = get_candles("BTC-USDT-SWAP", MARKET_GUARD_TIMEFRAME, 5)
    btc_df = to_dataframe(btc_candles)
    btc_change = 0.0
    if btc_df is not None and not btc_df.empty:
        btc_change = get_last_candle_change_pct(btc_df)
    block = False
    reason = ""
    alt_mode_str = str(alt_snapshot.get("alt_mode", ""))
    if red_ratio >= MARKET_GUARD_RED_RATIO_BLOCK and avg_change <= MARKET_GUARD_AVG_CHANGE_15M_BLOCK:
        block = True
        reason = f"red_ratio={red_ratio:.2f} & avg_change={avg_change:.2f}"
    elif btc_change <= MARKET_GUARD_BTC_CHANGE_15M_BLOCK and red_ratio >= 0.55:
        block = True
        reason = f"btc_change={btc_change:.2f} & red_ratio={red_ratio:.2f}"
    elif MARKET_GUARD_ALT_WEAK_BLOCK and "ضعيف" in alt_mode_str and red_ratio >= 0.60:
        block = True
        reason = f"alt_weak & red_ratio={red_ratio:.2f}"
    return {
        "active": True,
        "block_longs": bool(block),
        "level": "danger" if block else "normal",
        "valid_count": valid,
        "red_ratio_15m": red_ratio,
        "avg_change_15m": avg_change,
        "btc_change_15m": btc_change,
        "reason": reason,
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

    # في Bull Market / Alt Season نسمح برجوع أسرع للوضع الطبيعي
    # لأن وجود بعض الشموع الحمراء طبيعي طالما متوسط السوق و BTC ليسوا في ضغط واضح
    if market_state in ("bull_market", "alt_season"):
        return (
            red_ratio < 0.58
            and avg_change > -0.35
            and btc_change > -0.20
        )

    # في Mixed نبقى أكثر تحفظًا
    if market_state == "mixed":
        return (
            red_ratio < 0.52
            and avg_change > -0.25
            and btc_change > -0.15
        )

    # لا نرجع NORMAL من btc_leading أو risk_off
    return False
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
    elif alt_mode == "🔴 ضعيف" and red_ratio >= 0.60:
        crash_triggered = True
        crash_reason = f"alt_weak & red_ratio={red_ratio:.2f}"
 if crash_triggered:
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
            r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
        except Exception:
            pass
    return {"mode": MODE_BLOCK_LONGS, "reason": f"كراش: {crash_reason}"}
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
    if is_market_no_longer_crashing(red_ratio, avg_change, btc_change, alt_mode):
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
    if is_market_normal_ready(red_ratio, avg_change, btc_change, market_state):
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
            if time_since_last_transition < MODE_TRANSITION_MIN_INTERVAL:
                return {"mode": MODE_STRONG_LONG_ONLY, "reason": "تأكيد طبيعي لكن أقل مدة انتقال لم تمر"}
            return {"mode": MODE_NORMAL_LONG, "reason": "استقرار 3 دقائق، رجوع للوضع الطبيعي"}
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
 if is_market_normal_ready(red_ratio, avg_change, btc_change, market_state):
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
        except Exception:
            pass
    return {"mode": MODE_NORMAL_LONG, "reason": "السوق طبيعي"}
 weak_market = (
    market_state in ("mixed", "btc_leading", "risk_off")
    or (0.52 <= red_ratio < 0.68)
    or (avg_change < -0.30)
    or (btc_mode in ("🔴 هابط", "🟡 محايد") and alt_mode != "🟢 قوي")
 )
 if weak_market and time_since_last_transition >= MODE_TRANSITION_MIN_INTERVAL:
    if allow_state_writes and r:
        try:
            r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
        except Exception:
            pass
    return {"mode": MODE_STRONG_LONG_ONLY, "reason": "السوق ضعيف/مختلط لكن ليس كراش"}
 return {"mode": MODE_NORMAL_LONG, "reason": "لا يوجد تغيير في المود"}

def format_mode_transition_message(old_mode: str, new_mode: str, reason: str = "") -> str:
 transition = f"{html.escape(old_mode)} → {html.escape(new_mode)}"
 lines = []
 if new_mode == MODE_NORMAL_LONG:
    lines = [
        "🟢 <b>Mode Changed: NORMAL LONG</b>",
        "• السوق رجع طبيعي نسبيًا",
        "• البوت رجع يرسل إشارات Long بالحجم والمنطق العادي",
        f"• الانتقال: {transition}",
        "• الفحص: Market Guard / 15m"
    ]
 elif new_mode == MODE_STRONG_LONG_ONLY:
    lines = [
        "🟡 <b>Mode Changed: STRONG LONG ONLY</b>",
        "• السوق غير مثالي، لكن ليس كراش",
        "• البوت سيسمح فقط بإشارات Long القوية جدًا",
        f"• الانتقال: {transition}",
        "• الشروط أصبحت أقوى مؤقتًا"
    ]
 elif new_mode == MODE_BLOCK_LONGS:
    lines = [
        "🔴 <b>Mode Changed: BLOCK LONGS</b>",
        "• السوق يهبط بعنف أو فيه ضغط جماعي واضح",
        "• تم إيقاف إشارات Long الجديدة مؤقتًا",
        f"• الانتقال: {transition}",
        "• الأوامر والتقارير و Track شغالة عادي"
    ]
 elif new_mode == MODE_RECOVERY_LONG:
    lines = [
        "🟠 <b>Mode Changed: RECOVERY LONG</b>",
        "• السوق بدأ يهدأ بعد ضغط/كراش",
        "• البوت سيسمح فقط بفرص Recovery Long بشروط خاصة",
        f"• الانتقال: {transition}",
        f"• حجم الصفقة: {RECOVERY_TOTAL_SIZE_PCT}% من الحجم الطبيعي",
        f"• Entry 1 = {RECOVERY_ENTRY1_SIZE_PCT}%",
        f"• Entry 2 = {RECOVERY_ENTRY2_SIZE_PCT}%"
    ]
 else:
    lines = [f"Mode: {html.escape(new_mode)}"]
 if reason:
    lines.append("")
    lines.append(f"🧠 <b>السبب:</b> {html.escape(str(reason))}")
 return "\n".join(lines)

def handle_market_mode_transition(mode_result: dict) -> str:
 new_mode = mode_result.get("mode", MODE_NORMAL_LONG)
 if not r:
    return new_mode
 try:
    last_mode = r.get(MARKET_MODE_LAST_KEY) or MODE_NORMAL_LONG
    if new_mode != last_mode:
        msg = format_mode_transition_message(last_mode, new_mode, reason=mode_result.get("reason", ""))
        send_telegram_message(msg)
        now_ts = int(time.time())
        if new_mode == MODE_BLOCK_LONGS and last_mode != MODE_BLOCK_LONGS:
            try:
                r.set(MARKET_MODE_BLOCK_STARTED_KEY, str(now_ts))
            except Exception:
                pass
        if new_mode != MODE_BLOCK_LONGS:
            try:
                r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
            except Exception:
                pass
        r.set(MARKET_MODE_LAST_KEY, new_mode)
        r.set(MARKET_MODE_LAST_TRANSITION_KEY, str(now_ts))
        logger.info(f"MODE TRANSITION: {last_mode} → {new_mode} | reason: {mode_result.get('reason')}")
    r.set(MARKET_MODE_KEY, new_mode)
 except Exception as e:
    logger.error(f"handle_market_mode_transition error: {e}")
 return new_mode

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
                handler(chat_id)
        except Exception as e:
            logger.error(f"handle_telegram_commands error: {e}")
    if latest_offset != offset:
        save_telegram_offset(latest_offset)
 finally:
    release_telegram_poll_lock()

# =======================
# SAFE REGISTER TRADE WRAPPER
# =======================
def safe_register_trade(**kwargs):
    try:
        return register_trade(**kwargs)
    except Exception as e:
        logger.error(f"register_trade failed: {e}")
        return None

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
 global last_global_send_ts
 while True:
    scan_locked = False
    try:
        scan_locked = acquire_scan_lock()
        if not scan_locked:
            logger.info("Another long scan is running --- skipping")
            time.sleep(30)
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
        market_info = get_market_state(btc_mode, alt_snapshot)
        market_state = market_info["market_state"]
        market_state_label = market_info["market_state_label"]
        market_bias_label = market_info["market_bias_label"]
        btc_dominance_proxy = market_info["btc_dominance_proxy"]
        alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك")
        market_guard = get_market_guard_snapshot(
            ranked_pairs=ranked_pairs,
            btc_mode=btc_mode,
            alt_snapshot=alt_snapshot,
        )
        current_mode = r.get(MARKET_MODE_KEY) if r else MODE_NORMAL_LONG
        if not current_mode:
            current_mode = MODE_NORMAL_LONG
        mode_result = determine_long_market_mode(
            market_guard=market_guard,
            market_state=market_state,
            btc_mode=btc_mode,
            alt_snapshot=alt_snapshot,
            current_mode=current_mode,
            allow_state_writes=True,
        )
        current_mode = handle_market_mode_transition(mode_result)

        try:
            update_open_trades(
                r,
                market_type="futures",
                side="long",
                timeframe=TIMEFRAME,
                market_mode=current_mode,
                protect_breakeven_on_block=True,
                breakeven_min_profit_pct=0.15,
                reason=f"market_mode={current_mode}",
            )
        except TypeError:
            logger.warning("update_open_trades does not support breakeven kwargs yet, falling back to old call")
            update_open_trades(r, market_type="futures", side="long", timeframe=TIMEFRAME)
        except Exception as e:
            logger.error(f"update_open_trades error: {e}")

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
            "alt_snapshot": alt_snapshot,
            "market_info": market_info,
            "market_guard": market_guard,
            "ranked_pairs_count": len(ranked_pairs),
        }
        save_market_status_snapshot(snapshot_data)
        global_cooldown_active = is_global_cooldown_active()
        if global_cooldown_active and current_mode in (MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY):
            logger.info(
                f"GLOBAL COOLDOWN active - market mode checked, skipping normal/strong signal sending | mode={current_mode}"
            )
            time.sleep(60)
            continue
        if current_mode == MODE_BLOCK_LONGS:
            logger.warning("MODE BLOCK LONGS - blocking new long signals before candidate scan")
            time.sleep(60)
            continue
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
                    log_long_rejection(symbol=symbol, reason="no_candles", candle_time=None, market_state=market_state, current_mode=current_mode)
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
                # حماية تكرار محلية - لا تسجل رفض
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
                    }
                    save_alert_snapshot(alert_snapshot, message_id=message_id)
                    safe_register_trade(
                        redis_client=r,
                        symbol=symbol,
                        market_type="futures",
                        side="long",
                        candle_time=candle_time,
                        entry=avg_entry,
                        sl=sl,
                        tp1=tp1,
                        tp2=tp2,
                        score=0.0,
                        timeframe=TIMEFRAME,
                        btc_mode=btc_mode,
                        funding_label="🟡 محايد",
                        reasons=["Recovery Long", "Post Crash", "Oversold Bounce"],
                        pre_breakout=False,
                        breakout=False,
                        vol_ratio=vol_ratio,
                        candle_strength=0.0,
                        mtf_confirmed=False,
                        is_new=False,
                        btc_dominance_proxy=btc_dominance_proxy,
                        change_24h=extract_24h_change_percent(pair_data),
                        setup_type="recovery|mtf_yes|vol_mid|post_crash",
                        raw_score=0.0,
                        effective_score=0.0,
                        dynamic_threshold=0.0,
                        required_min_score=0.0,
                        dist_ma=dist_ma,
                        entry_timing="Recovery Entry",
                        opportunity_type="Recovery Long",
                        market_state=market_state,
                        market_state_label=market_state_label,
                        market_bias_label=market_bias_label,
                        alt_mode=alt_mode,
                        early_priority="none",
                        breakout_quality="none",
                        fake_signal=False,
                        is_reverse_signal=True,
                        reversal_4h_confirmed=False,
                        rank_volume_24h=float(pair_data.get("_rank_volume_24h", 0)),
                        alert_id=alert_id,
                        has_high_impact_news=has_high_impact_news,
                        news_titles=[e.get("title", "") for e in upcoming_events[:3]],
                        warning_reasons=[],
                        pullback_low=entry2,
                        pullback_high=entry1,
                        pullback_entry=entry2,
                        rr1=rr1,
                        rr2=rr2,
                        tp1_close_pct=TP1_CLOSE_PCT,
                        tp2_close_pct=TP2_CLOSE_PCT,
                        move_sl_to_entry_after_tp1=MOVE_SL_TO_ENTRY_AFTER_TP1,
                        fib_position="unknown",
                        fib_position_ratio=0.0,
                        fib_label="غير معروف",
                        had_pullback=False,
                        pullback_pct=0.0,
                        pullback_label="غير معروف",
                        wave_estimate=0,
                        wave_peaks=0,
                        wave_label="غير معروف",
                        entry_maturity="unknown",
                        maturity_penalty=0.0,
                        maturity_bonus=0.0,
                    )
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
        logger.info(f"Parallel HTF candle fetch completed: {len(htf_candles_map)} symbols")
        logger.info(f"Parallel candle fetch completed: {len(candles_map)} symbols")
        tested = 0
        sent_count = 0
        sent_symbols_this_run = set()
        candidates = []
        candidates_symbols = set()
        for pair_data in filtered_scan_pairs:
            # Reset final_threshold_min per symbol
            final_threshold_min = None
            tested += 1
            symbol = pair_data["instId"]
            change_24h = extract_24h_change_percent(pair_data)
            candles = candles_map.get(symbol, [])
            if not candles:
                log_long_rejection(symbol=symbol, reason="no_candles", candle_time=None, market_state=market_state, current_mode=current_mode)
                continue
            df = to_dataframe(candles)
            if df is None or df.empty:
                log_long_rejection(symbol=symbol, reason="dataframe_empty", candle_time=None, market_state=market_state, current_mode=current_mode)
                continue
            if not is_valid_candle_timing(df):
                log_long_rejection(symbol=symbol, reason="invalid_candle_timing", candle_time=get_signal_candle_time(df), market_state=market_state, current_mode=current_mode)
                logger.info(f"{symbol} --> skipped (candle timing invalid)")
                continue
            candle_time = get_signal_candle_time(df)
            # حماية تكرار محلية
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
            is_new = is_new_listing_by_candles(candles)
            funding = get_funding_rate(symbol)
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
            )
            # تفعيل حظر الـ late_guard المباشر
            if late_guard.get("should_block") and not is_reverse:
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
                    extra={"late_guard_reasons": late_guard.get("reasons", [])},
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
                if not is_reverse and not pre_breakout and not (
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
                        },
                    )
                    logger.info(f"{symbol} → skipped by entry maturity guard: {entry_maturity_data}")
                    continue
            if market_state == "bull_market" and not is_reverse:
                if vwap_distance >= 2.4 and not breakout and not pre_breakout:
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
                    )
                    continue
                if vwap_distance >= 2.8 and breakout_quality != "strong" and not pre_breakout:
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
                        extra={"breakout_quality": breakout_quality},
                    )
                    continue
            if not is_reverse:
                if (
                    rsi_now >= 67
                    and rsi_slope <= 0
                    and dist_ma >= 3.4
                    and not pre_breakout
                    and breakout_quality != "strong"
                ):
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
                        extra={"rsi_slope": rsi_slope, "breakout_quality": breakout_quality},
                    )
                    continue
                if rsi_slope < -2.5 and not breakout and not pre_breakout:
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
                        extra={"rsi_slope": rsi_slope},
                    )
                    continue
            if not is_reverse:
                if macd_hist < 0 and not breakout and not pre_breakout:
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
                        extra={"macd_hist": macd_hist},
                    )
                    continue
                if (
                    macd_hist_slope < 0
                    and rsi_now >= 66
                    and dist_ma >= 3.4
                    and not pre_breakout
                    and breakout_quality != "strong"
                ):
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
                        extra={"macd_hist_slope": macd_hist_slope, "macd_hist": macd_hist},
                    )
                    continue
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
                    extra={"trap_reasons": trap_check["reasons"], "checks": trap_check["checks"]},
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
                    extra={"guard_reason": guard["reason"], "upper_wick_ratio": guard["upper_wick_ratio"]},
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
                    extra={"guard_reason": guard["reason"], "upper_wick_ratio": guard["upper_wick_ratio"]},
                )
                logger.info(f"{symbol} --> retest required: {guard['reason']} (skipped direct alert)")
                continue

            # compute strong_bull_pullback once for later reuse
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

            # Pre-score adjustments log collector
            pre_score_adjustments_log = []

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

                if hard_late_entry:
                    if strong_bull_pullback or strong_breakout_exception:
                        pre_score_adjustments_log.append({
                            "name": "hard_late_exception",
                            "value": 0.0,
                            "reason": "strong_bull_pullback_or_strong_breakout"
                        })
                        logger.info(f"{symbol} --> hard late entry overridden by strong exception")
                    else:
                        reason_specific = classify_hard_late_rejection_reason(
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

            # STRONG_LONG_ONLY filter
            if current_mode == MODE_STRONG_LONG_ONLY:
                if "هابط" in btc_mode and "ضعيف" in alt_mode:
                    # Raise final threshold later
                    final_threshold_min = 7.4
                else:
                    final_threshold_min = 6.0

                if not (breakout or pre_breakout or early_priority == "strong" or strong_bull_pullback or (is_reverse and reversal_4h_result.get("confirmed"))):
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
                        extra={
                            "early_priority": early_priority,
                            "strong_bull_pullback": strong_bull_pullback,
                            "breakout_quality": breakout_quality,
                        },
                    )
                    logger.info(f"{symbol} --> skipped (STRONG_LONG_ONLY: no valid strong setup)")
                    continue
                if strong_bull_pullback and not (breakout or pre_breakout or early_priority == "strong"):
                    logger.info(f"{symbol} --> allowed by STRONG_LONG_ONLY strong_bull_pullback exception")
                if not mtf_confirmed and not (is_reverse and reversal_4h_result.get("confirmed")):
                    log_long_rejection(
                        symbol=symbol,
                        reason="strong_only_mtf_not_confirmed",
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
                    logger.info(f"{symbol} --> skipped (STRONG_LONG_ONLY: mtf not confirmed)")
                    continue
                if vol_ratio < 1.25 and not strong_bull_pullback:
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
                        extra={"strong_bull_pullback": strong_bull_pullback},
                    )
                    logger.info(f"{symbol} --> skipped (STRONG_LONG_ONLY: vol_ratio too low)")
                    continue
                if breakout and breakout_quality == "weak":
                    log_long_rejection(
                        symbol=symbol,
                        reason="strong_only_weak_breakout",
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
                        extra={"breakout_quality": breakout_quality},
                    )
                    logger.info(f"{symbol} --> skipped (STRONG_LONG_ONLY: weak breakout)")
                    continue

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
            if vol_ratio < 1.02 and not breakout and not pre_breakout and not early_signal:
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
                    extra={"candle_strength": candle_strength},
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
            # Merge pre-score adjustments
            if pre_score_adjustments_log:
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

            score_result["score"] = round(effective_score, 2)

            # Apply near resistance guard - now after score calculation for correct penalty logic
            resistance_rejected = False
            resistance_dynamic_penalty = 0.0
            # Build smart targets early to get resistance_warning
            price = _safe_float(signal_row["close"], 0)
            if breakout:
                sl_type = "breakout"
            elif pre_breakout:
                sl_type = "pre_breakout"
            elif is_new:
                sl_type = "new_listing"
            else:
                sl_type = "standard"
            stop_loss = calculate_stop_loss(price, atr_value, signal_type=sl_type)
            rr1, rr2 = get_rr_targets_long(signal_type=sl_type, entry_timing=entry_timing_temp)
            smart_targets_early = build_smart_tp1_long(
                df=df,
                entry=price,
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
            # Apply the new guard
            should_reject_near_resistance, res_dynamic_penalty = near_resistance_guard_long(
                resistance_warning=early_resistance_warning,
                nearest_resistance=early_nearest_resistance,
                market_state=market_state,
                btc_mode=btc_mode,
                alt_mode=alt_mode,
                current_mode=current_mode,
                display_risk=get_base_risk_label(score_result, 0),  # rough initial
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
                    extra={
                        "nearest_resistance": early_nearest_resistance,
                        "resistance_warning": early_resistance_warning,
                        "res_dynamic_penalty": res_dynamic_penalty,
                        "upper_wick_ratio": upper_wick_ratio,
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
                effective_score -= 0.35
                score_result["score"] = round(effective_score, 2)
                adjustments_log.append({
                    "name": "strong_mode_penalty",
                    "value": -0.35,
                    "reason": "strong_mode"
                })
                dynamic_threshold += 0.25
                dynamic_threshold = round(dynamic_threshold, 2)
            if current_mode == MODE_STRONG_LONG_ONLY and "🔴" in entry_timing_temp:
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
                )
                logger.info(f"{symbol} --> skipped (STRONG_LONG_ONLY: late entry)")
                continue

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

            # Apply STRONG_LONG_ONLY minimum if set
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
                    extra={"change_4h": change_4h},
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
                            extra={"early_priority": early_priority, "dynamic_threshold": dynamic_threshold},
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
                    extra={"candle_strength": candle_strength},
                )
                logger.info(f"{symbol} → rejected by balanced new listing filter")
                continue

            if score_result["score"] < final_threshold:
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
                    extra={
                        "dynamic_threshold": dynamic_threshold,
                        "required_min_score": effective_required_min_score,
                        "adjustments_log": adjustments_log,
                        "warning_reasons": score_result.get("warning_reasons", []),
                        "early_priority": early_priority,
                        "breakout_quality": breakout_quality,
                    },
                )
                logger.info(
                    f"{symbol} --> rejected by final_threshold {final_threshold:.2f} "
                    f"(score={score_result['score']:.2f}, dynamic={dynamic_threshold:.2f}, "
                    f"required={effective_required_min_score:.2f})"
                )
                continue

            # No duplicate penalty; use smart targets already computed
            tp1 = smart_targets_early.get("tp1", calc_tp_long(price, stop_loss, rr=rr1))
            tp2 = smart_targets_early.get("tp2", calc_tp_long(price, stop_loss, rr=rr2))
            target_method = smart_targets_early.get("target_method", "rr")
            nearest_resistance = smart_targets_early.get("nearest_resistance")
            nearest_support = smart_targets_early.get("nearest_support")
            resistance_warning = early_resistance_warning
            support_warning = smart_targets_early.get("support_warning", "")
            target_notes = smart_targets_early.get("target_notes", [])
            rr1 = smart_targets_early.get("rr1_effective", rr1)
            rr2 = smart_targets_early.get("rr2_effective", rr2)

            tv_link = build_tradingview_link(symbol)
            explicit_warnings = score_result.get("warning_reasons") or []
            _, inferred_warnings = classify_reasons(score_result.get("reasons", []))
            warnings_count = len(explicit_warnings) if explicit_warnings else len(inferred_warnings)
            base_risk = get_base_risk_label(score_result, warnings_count)
            display_risk = adjust_risk_with_entry_timing(base_risk, entry_timing)
            if "🔴" in entry_timing and "🔴" in display_risk and not breakout and vol_ratio < 1.2:
                log_long_rejection(
                    symbol=symbol,
                    reason="late_high_risk_low_volume",
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
                    extra={"display_risk": display_risk},
                )
                logger.info(f"{symbol} → rejected (late + high risk + no breakout + low vol)")
                continue
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
            setup_type_candidate = {
                "is_reverse": is_reverse,
                "breakout": breakout,
                "pre_breakout": pre_breakout,
                "mtf_confirmed": mtf_confirmed,
                "vol_ratio": vol_ratio,
                "market_state": market_state,
                "wave_context": wave_context,
            }
            setup_type = build_setup_type(setup_type_candidate)
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
                        },
                    )
                    logger.info(f"{symbol} --> skipped (weak historical setup: score below final_threshold {final_threshold})")
                    continue
                momentum_priority -= 0.60
                momentum_priority = round(momentum_priority, 2)
            setup_stats = get_setup_type_stats(
                redis_client=r,
                market_type="futures",
                side="long",
                setup_type=setup_type,
                since_ts=stats_reset_ts,
            )
            candidate = {
                "symbol": symbol,
                "score": float(score_result["score"]),
                "momentum_priority": momentum_priority,
                "breakout": breakout,
                "pre_breakout": pre_breakout,
                "vol_ratio": vol_ratio,
                "candle_strength": candle_strength,
                "is_new": is_new,
                "rank_volume_24h": float(pair_data.get("_rank_volume_24h", 0)),
                "early_priority": early_priority,
                "is_reverse": is_reverse,
                "setup_type": setup_type,
                "setup_type_base": setup_type_base,
                "raw_score": raw_score,
                "dynamic_threshold": dynamic_threshold,
                "required_min_score": effective_required_min_score,
                "final_threshold": final_threshold,
                "adjustments_log": adjustments_log,
                "warning_penalty": warning_penalty_value,
                "warning_penalty_details": warning_penalty_details,
                "warning_high_count": warning_high_count,
                "warning_medium_count": warning_medium_count,
                "dist_ma": dist_ma,
                "entry_timing": entry_timing,
                "opportunity_type": opportunity_type,
                "market_state": market_state,
                "current_mode": current_mode,
                "market_state_label": market_state_label,
                "market_bias_label": market_bias_label,
                "breakout_quality": breakout_quality,
                "fake_signal": bool(score_result.get("fake_signal", False)),
                "reversal_4h_confirmed": reversal_4h_result.get("confirmed", False),
                "has_high_impact_news": has_high_impact_news,
                "warning_reasons": score_result.get("warning_reasons", []),
                "news_titles": [e.get("title", "") for e in upcoming_events[:3]],
                "pullback_low": pullback_low,
                "pullback_high": pullback_high,
                "pullback_entry": pullback_entry,
                "rr1": rr1,
                "rr2": rr2,
                "above_upper_bb": above_upper_bb,
                "change_4h": change_4h,
                "late_pump_risk": late_guard.get("late_pump_risk", False),
                "extreme_late_pump": late_guard.get("extreme_late_pump", False),
                "bull_continuation_risk": late_guard.get("bull_continuation_risk", False),
                "late_guard_reasons": late_guard.get("reasons", []),
                "rsi_now": rsi_now,
                "market_guard_active": bool(market_guard.get("active", False)),
                "market_guard_level": market_guard.get("level", "normal"),
                "market_red_ratio_15m": market_guard.get("red_ratio_15m", 0.0),
                "market_avg_change_15m": market_guard.get("avg_change_15m", 0.0),
                "btc_change_15m": market_guard.get("btc_change_15m", 0.0),
                "vwap_distance": vwap_distance,
                "rsi_slope": rsi_slope,
                "macd_hist": macd_hist,
                "macd_hist_slope": macd_hist_slope,
                "upper_wick_ratio": upper_wick_ratio,
                "retest_required": False,
                "late_breakout_guard_reason": late_breakout_guard_reason,
                "fib_position": entry_maturity_data.get("fib_position", "unknown"),
                "fib_position_ratio": entry_maturity_data.get("fib_position_ratio", 0.0),
                "fib_label": entry_maturity_data.get("fib_label", "غير معروف"),
                "had_pullback": entry_maturity_data.get("had_pullback", False),
                "pullback_pct": entry_maturity_data.get("pullback_pct", 0.0),
                "pullback_label": entry_maturity_data.get("pullback_label", "غير معروف"),
                "wave_estimate": entry_maturity_data.get("wave_estimate", 0),
                "wave_peaks": entry_maturity_data.get("wave_peaks", 0),
                "wave_label": entry_maturity_data.get("wave_label", "غير معروف"),
                "entry_maturity": entry_maturity_data.get("entry_maturity", "unknown"),
                "maturity_penalty": entry_maturity_data.get("maturity_penalty", 0.0),
                "maturity_bonus": entry_maturity_data.get("maturity_bonus", 0.0),
                "mtf_confirmed": mtf_confirmed,
                "falling_knife_risk": bool(falling_knife_data.get("falling_knife_risk", False)),
                "falling_knife_reasons": falling_knife_data.get("reasons", []),
                "target_method": target_method,
                "nearest_resistance": nearest_resistance,
                "nearest_support": nearest_support,
                "resistance_warning": resistance_warning,
                "support_warning": support_warning,
                "target_notes": target_notes,
                "sl_method": "atr",
                "sl_notes": f"sl_type={sl_type}",
                "wave_context": wave_context,
                "setup_context": setup_type,
                "reversal_quality": "",
                "reversal_structure_confirmed": False,
                "strong_bull_pullback": strong_bull_pullback,
                "strong_breakout_exception": strong_breakout_exception,
                "tp1_close_pct": TP1_CLOSE_PCT,
                "tp2_close_pct": TP2_CLOSE_PCT,
                "move_sl_to_entry_after_tp1": MOVE_SL_TO_ENTRY_AFTER_TP1,
                "message": build_message(
                    symbol=symbol,
                    price=price,
                    score_result=score_result,
                    stop_loss=stop_loss,
                    tp1=tp1,
                    tp2=tp2,
                    rr1=rr1,
                    rr2=rr2,
                    btc_mode=btc_mode,
                    btc_dominance_proxy=btc_dominance_proxy,
                    tv_link=tv_link,
                    is_new=is_new,
                    change_24h=change_24h,
                    market_state_label=market_state_label,
                    market_bias_label=market_bias_label,
                    alt_mode=alt_mode,
                    news_warning=news_warning_text,
                    opportunity_type=opportunity_type,
                    entry_timing=entry_timing,
                    display_risk=display_risk,
                    setup_stats=setup_stats,
                    is_reverse=is_reverse,
                    reversal_4h_confirmed=reversal_4h_result.get("confirmed", False),
                    reversal_4h_details=reversal_4h_result.get("details", ""),
                    breakout_quality=breakout_quality,
                    pullback_low=pullback_low,
                    pullback_high=pullback_high,
                    entry_maturity_data=entry_maturity_data,
                    warning_penalty=warning_penalty_value + res_dynamic_penalty,
                    resistance_warning=resistance_warning,
                    target_method=target_method,
                    nearest_resistance=nearest_resistance,
                    wave_context=wave_context,
                ),
                "reply_markup": build_track_reply_markup(alert_id),
                "alert_id": alert_id,
                "alert_snapshot": {
                    "alert_id": alert_id,
                    "symbol": symbol,
                    "mode": current_mode,
                    "market_mode": current_mode,
                    "timeframe": TIMEFRAME,
                    "market_entry": price,
                    "entry": price,
                    "recommended_entry": pullback_entry if pullback_entry else price,
                    "pullback_entry": pullback_entry,
                    "sl": stop_loss,
                    "tp1": tp1,
                    "tp2": tp2,
                    "rr1": rr1,
                    "rr2": rr2,
                    "score": float(score_result["score"]),
                    "candle_time": candle_time,
                    "created_ts": int(time.time()),
                    "market_state": market_state,
                    "alt_mode": alt_mode,
                    "btc_mode": btc_mode,
                    "entry_timing": entry_timing,
                    "opportunity_type": opportunity_type,
                    "early_priority": early_priority,
                    "is_reverse": is_reverse,
                    "setup_type": setup_type,
                    "above_upper_bb": above_upper_bb,
                    "change_4h": change_4h,
                    "late_pump_risk": late_guard.get("late_pump_risk", False),
                    "bull_continuation_risk": late_guard.get("bull_continuation_risk", False),
                    "rsi_now": rsi_now,
                    "dist_ma": dist_ma,
                    "vol_ratio": vol_ratio,
                    "pullback_low": pullback_low,
                    "pullback_high": pullback_high,
                    "pullback_entry": pullback_entry,
                    "market_guard_active": bool(market_guard.get("active", False)),
                    "market_guard_level": market_guard.get("level", "normal"),
                    "market_red_ratio_15m": market_guard.get("red_ratio_15m", 0.0),
                    "market_avg_change_15m": market_guard.get("avg_change_15m", 0.0),
                    "btc_change_15m": market_guard.get("btc_change_15m", 0.0),
                    "vwap_distance": vwap_distance,
                    "rsi_slope": rsi_slope,
                    "macd_hist": macd_hist,
                    "macd_hist_slope": macd_hist_slope,
                    "upper_wick_ratio": upper_wick_ratio,
                    "retest_required": False,
                    "late_breakout_guard_reason": late_breakout_guard_reason,
                    "fib_position": entry_maturity_data.get("fib_position", "unknown"),
                    "fib_position_ratio": entry_maturity_data.get("fib_position_ratio", 0.0),
                    "fib_label": entry_maturity_data.get("fib_label", "غير معروف"),
                    "had_pullback": entry_maturity_data.get("had_pullback", False),
                    "pullback_pct": entry_maturity_data.get("pullback_pct", 0.0),
                    "pullback_label": entry_maturity_data.get("pullback_label", "غير معروف"),
                    "wave_estimate": entry_maturity_data.get("wave_estimate", 0),
                    "wave_peaks": entry_maturity_data.get("wave_peaks", 0),
                    "wave_label": entry_maturity_data.get("wave_label", "غير معروف"),
                    "entry_maturity": entry_maturity_data.get("entry_maturity", "unknown"),
                    "maturity_penalty": entry_maturity_data.get("maturity_penalty", 0.0),
                    "maturity_bonus": entry_maturity_data.get("maturity_bonus", 0.0),
                    "final_threshold": final_threshold,
                    "adjustments_log": adjustments_log,
                    "warning_penalty": warning_penalty_value,
                    "warning_penalty_details": warning_penalty_details,
                    "falling_knife_risk": bool(falling_knife_data.get("falling_knife_risk", False)),
                    "falling_knife_reasons": falling_knife_data.get("reasons", []),
                    "target_method": target_method,
                    "nearest_resistance": nearest_resistance,
                    "nearest_support": nearest_support,
                    "resistance_warning": resistance_warning,
                    "support_warning": support_warning,
                    "target_notes": target_notes,
                    "sl_method": "atr",
                    "sl_notes": f"sl_type={sl_type}",
                    "wave_context": wave_context,
                    "setup_context": setup_type,
                    "reversal_quality": "",
                    "reversal_structure_confirmed": False,
                    "strong_bull_pullback": strong_bull_pullback,
                    "strong_breakout_exception": strong_breakout_exception,
                    "tp1_close_pct": TP1_CLOSE_PCT,
                    "tp2_close_pct": TP2_CLOSE_PCT,
                    "move_sl_to_entry_after_tp1": MOVE_SL_TO_ENTRY_AFTER_TP1,
                },
                "candle_time": candle_time,
                "now": now,
                "entry": price,
                "sl": stop_loss,
                "tp1": tp1,
                "tp2": tp2,
                "funding_label": score_result.get("funding_label", "🟡 محايد"),
                "reasons": score_result.get("reasons", []),
                "mtf_confirmed": mtf_confirmed,
                "btc_dominance_proxy": btc_dominance_proxy,
                "change_24h": change_24h,
                "market_state": market_state,
                "alt_mode": alt_mode,
            }
            candidate["bucket"] = get_candidate_bucket(candidate)
            candidates.append(candidate)
            candidates_symbols.add(symbol)
        logger.info(f"Long candidates found before momentum filter: {len(candidates)}")
        candidates = apply_top_momentum_filter(candidates)
        logger.info(f"Long candidates found after momentum filter: {len(candidates)}")
        top_candidates = diversify_candidates(candidates, min(max_alerts, len(candidates)))
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
            sent_data = send_telegram_message(
                candidate["message"],
                reply_markup=candidate.get("reply_markup"),
            )
            if sent_data.get("ok"):
                sent_count += 1
                sent_symbols_this_run.add(symbol)
                sent_cache[symbol] = time.time()
                last_candle_cache[symbol] = candidate["candle_time"]
                last_global_send_ts = time.time()
                message_id = str(((sent_data.get("result") or {}).get("message_id")) or "")
                save_alert_snapshot(candidate.get("alert_snapshot", {}), message_id=message_id)
                trade_reasons = list(candidate["reasons"] or [])
                if candidate.get("is_reverse"):
                    if "OVERSOLD_REVERSAL" not in trade_reasons:
                        trade_reasons.append("OVERSOLD_REVERSAL")
                safe_register_trade(
                    redis_client=r,
                    symbol=symbol,
                    market_type="futures",
                    side="long",
                    candle_time=candidate["candle_time"],
                    entry=candidate["entry"],
                    sl=candidate["sl"],
                    tp1=candidate["tp1"],
                    tp2=candidate["tp2"],
                    score=candidate["score"],
                    timeframe=TIMEFRAME,
                    btc_mode=btc_mode,
                    funding_label=candidate.get("funding_label", "🟡 محايد"),
                    reasons=trade_reasons,
                    pre_breakout=candidate.get("pre_breakout", False),
                    breakout=candidate["breakout"],
                    vol_ratio=candidate["vol_ratio"],
                    candle_strength=candidate["candle_strength"],
                    mtf_confirmed=candidate.get("mtf_confirmed", False),
                    is_new=candidate["is_new"],
                    btc_dominance_proxy=candidate.get("btc_dominance_proxy", "🟡 محايد"),
                    change_24h=candidate["change_24h"],
                    setup_type=candidate["setup_type"],
                    setup_type_base=candidate.get("setup_type_base", ""),
                    raw_score=candidate.get("raw_score", candidate["score"]),
                    effective_score=candidate["score"],
                    dynamic_threshold=candidate["dynamic_threshold"],
                    required_min_score=candidate["required_min_score"],
                    final_threshold=candidate.get("final_threshold", 0.0),
                    adjustments_log=candidate.get("adjustments_log", []),
                    warning_penalty=candidate.get("warning_penalty", 0.0),
                    warning_penalty_details=candidate.get("warning_penalty_details", []),
                    warning_high_count=candidate.get("warning_high_count", 0),
                    warning_medium_count=candidate.get("warning_medium_count", 0),
                    dist_ma=candidate["dist_ma"],
                    entry_timing=candidate.get("entry_timing", ""),
                    opportunity_type=candidate.get("opportunity_type", ""),
                    market_state=candidate.get("market_state", "mixed"),
                    market_state_label=candidate.get("market_state_label", ""),
                    market_bias_label=candidate.get("market_bias_label", ""),
                    alt_mode=candidate.get("alt_mode", ""),
                    early_priority=candidate.get("early_priority", "none"),
                    breakout_quality=candidate.get("breakout_quality", "none"),
                    fake_signal=candidate.get("fake_signal", False),
                    is_reverse_signal=candidate.get("is_reverse", False),
                    reversal_4h_confirmed=candidate.get("reversal_4h_confirmed", False),
                    rank_volume_24h=candidate.get("rank_volume_24h", 0.0),
                    alert_id=candidate.get("alert_id", ""),
                    has_high_impact_news=candidate.get("has_high_impact_news", False),
                    news_titles=candidate.get("news_titles", []),
                    warning_reasons=candidate.get("warning_reasons", []),
                    pullback_low=candidate.get("pullback_low"),
                    pullback_high=candidate.get("pullback_high"),
                    pullback_entry=candidate.get("pullback_entry"),
                    rr1=candidate.get("rr1", 2.0),
                    rr2=candidate.get("rr2", 3.2),
                    tp1_close_pct=candidate.get("tp1_close_pct", TP1_CLOSE_PCT),
                    tp2_close_pct=candidate.get("tp2_close_pct", TP2_CLOSE_PCT),
                    move_sl_to_entry_after_tp1=candidate.get("move_sl_to_entry_after_tp1", MOVE_SL_TO_ENTRY_AFTER_TP1),
                    fib_position=candidate.get("fib_position", "unknown"),
                    fib_position_ratio=candidate.get("fib_position_ratio", 0.0),
                    fib_label=candidate.get("fib_label", "غير معروف"),
                    had_pullback=candidate.get("had_pullback", False),
                    pullback_pct=candidate.get("pullback_pct", 0.0),
                    pullback_label=candidate.get("pullback_label", "غير معروف"),
                    wave_estimate=candidate.get("wave_estimate", 0),
                    wave_peaks=candidate.get("wave_peaks", 0),
                    wave_label=candidate.get("wave_label", "غير معروف"),
                    entry_maturity=candidate.get("entry_maturity", "unknown"),
                    maturity_penalty=candidate.get("maturity_penalty", 0.0),
                    maturity_bonus=candidate.get("maturity_bonus", 0.0),
                    falling_knife_risk=candidate.get("falling_knife_risk", False),
                    falling_knife_reasons=candidate.get("falling_knife_reasons", []),
                    target_method=candidate.get("target_method", "rr"),
                    nearest_resistance=candidate.get("nearest_resistance"),
                    nearest_support=candidate.get("nearest_support"),
                    resistance_warning=candidate.get("resistance_warning", ""),
                    support_warning=candidate.get("support_warning", ""),
                    target_notes=candidate.get("target_notes", []),
                    sl_method=candidate.get("sl_method", "atr"),
                    sl_notes=candidate.get("sl_notes", ""),
                    wave_context=candidate.get("wave_context", ""),
                    setup_context=candidate.get("setup_context", ""),
                    reversal_quality=candidate.get("reversal_quality", ""),
                    reversal_structure_confirmed=candidate.get("reversal_structure_confirmed", False),
                    strong_bull_pullback=candidate.get("strong_bull_pullback", False),
                    strong_breakout_exception=candidate.get("strong_breakout_exception", False),
                )
                logger.info(f"✅ SENT LONG ---> {symbol}")
            else:
                release_signal_slot(symbol, candidate["candle_time"], "long")
                logger.error(f"❌ FAILED SEND ---> {symbol}")
        if sent_count > 0:
            set_global_cooldown()
            logger.info(f"Global long cooldown set for {GLOBAL_COOLDOWN_SECONDS}s after {sent_count} alert(s)")
        logger.info(f"Sent long alerts this run: {sent_count}")
        logger.info(f"Tested {tested} pairs")
        logger.info("Sleeping 60 seconds...")
        time.sleep(60)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        time.sleep(10)
    finally:
        if scan_locked:
            release_scan_lock()

def run():
 logger.info(
    f"LONG BOT STARTED | pid={os.getpid()} | replica={os.getenv('RAILWAY_REPLICA_ID', 'unknown')}"
 )
 clear_webhook()
 command_thread = threading.Thread(target=run_command_poller, daemon=True)
 command_thread.start()
 run_scanner_loop()

if __name__ == "__main__":
 run()
