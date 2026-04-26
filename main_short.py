import os
import sys
import time
import html
import json
import logging
import threading
import requests
import pandas as pd
import numpy as np
import redis
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ================================================
# CRITICAL IMPORTS – must fail loudly if missing
# ================================================
try:
    from analysis.scoring_short import calculate_short_score, is_breakdown
except ImportError:
    raise ImportError("analysis.scoring_short is required. Please fix the module.")

try:
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
except ImportError:
    raise ImportError("tracking.performance is required. Please fix the module.")

# Optional diagnostic reports (fallback accepted)
try:
    from analysis.performance_diagnostics import (
        build_setups_report,
        build_scores_report,
        build_market_report,
        build_losses_report,
        build_full_diagnostics_report,
    )
except ImportError:
    build_setups_report = build_scores_report = build_market_report = build_losses_report = build_full_diagnostics_report = None

try:
    from analysis.backtest import build_deep_report
except ImportError:
    def build_deep_report(*args, **kwargs):
        return "Build deep report unavailable"


# ================================================
# CONFIG
# ================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
REDIS_URL = os.getenv("REDIS_URL")

OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"
OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
OKX_FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate"
OKX_TICKER_SINGLE_URL = "https://www.okx.com/api/v5/market/ticker"

SCAN_LIMIT = 150
TIMEFRAME = "15m"
HTF_TIMEFRAME = "1H"

FINAL_MIN_SCORE = 6.5
PRE_BREAKDOWN_EXTRA_SCORE = 0.2
MAX_ALERTS_PER_RUN = 3

COOLDOWN_SECONDS = 3600
LOCAL_RECENT_SEND_SECONDS = 2700
GLOBAL_COOLDOWN_SECONDS = 300
COMMAND_POLL_INTERVAL = 3

MIN_24H_QUOTE_VOLUME = 1_000_000
NEW_LISTING_MAX_CANDLES = 50

TOP_MOMENTUM_PERCENT = 0.22
TOP_MOMENTUM_MIN_SCORE = 6.8
TOP_MOMENTUM_NEW_MIN_SCORE = 5.8

NEW_LISTING_MIN_VOL_RATIO = 1.6
NEW_LISTING_MIN_CANDLE_STRENGTH = 0.40
NEW_LISTING_MAX_PER_RUN = 1

PRE_BREAKDOWN_LOOKBACK = 20
PRE_BREAKDOWN_PROXIMITY_MAX = 1.035
PRE_BREAKDOWN_VOLUME_SIGNIFICANCE = 1.15
PRE_BREAKDOWN_RECENT_VOL_BARS = 3
PRE_BREAKDOWN_BASELINE_VOL_BARS = 12

OVEREXTENDED_REVERSAL_ENABLED = True
OVEREXTENDED_REVERSAL_HTF = "4H"
OVEREXTENDED_REVERSAL_CONFIRM_TF = "1H"
OVEREXTENDED_REVERSAL_TRIGGER_TF = "15m"
OVEREXTENDED_REVERSAL_MIN_DIST_MA_4H = 7.0
OVEREXTENDED_REVERSAL_MIN_RSI_4H = 70.0
OVEREXTENDED_REVERSAL_MIN_24H_CHANGE = 12.0
OVEREXTENDED_REVERSAL_MIN_VOL_RATIO_15M = 1.03
OVEREXTENDED_REVERSAL_SCORE_BONUS = 0.35
OVEREXTENDED_REVERSAL_MIN_SCORE = 6.2

# Market mode constants
MARKET_MODE_KEY = "market_mode:short:current"
MARKET_MODE_LAST_KEY = "market_mode:short:last_mode"
MARKET_MODE_LAST_TRANSITION_KEY = "market_mode:short:last_transition_ts"
MARKET_MODE_NORMAL_CANDIDATE_KEY = "market_mode:short:normal_candidate_since"
MARKET_MODE_LAST_SAFE_SEEN_KEY = "market_mode:short:last_safe_seen_ts"
MARKET_MODE_BLOCK_STARTED_KEY = "market_mode:short:block_started_ts"

MODE_NORMAL_SHORT = "NORMAL_SHORT"
MODE_STRONG_SHORT_ONLY = "STRONG_SHORT_ONLY"
MODE_BLOCK_SHORTS = "BLOCK_SHORTS"

MODE_TRANSITION_MIN_INTERVAL = 240   # seconds
NORMAL_CANDIDATE_DURATION = 240
BLOCK_EXIT_CONFIRM_DURATION = 240
STRONG_TO_NORMAL_CONFIRM_DURATION = 240

# Bull Guard – thresholds adjusted to be less sensitive
BULL_GUARD_ENABLED = True
BULL_GUARD_SAMPLE_SIZE = 30
BULL_GUARD_TIMEFRAME = "15m"
BULL_GUARD_CANDLE_LIMIT = 30
BULL_GUARD_MIN_VALID = 12
BULL_GUARD_GREEN_RATIO_BLOCK = 0.72
BULL_GUARD_AVG_CHANGE_15M_BLOCK = 0.85
BULL_GUARD_BTC_CHANGE_15M_BLOCK = 0.85
BULL_GUARD_ALT_STRONG_BLOCK = True

WEAK_SETUP_TYPES = {
    "continuation|mtf_yes|vol_mid|bull_market",
    "continuation|mtf_yes|vol_high|bull_market",
    "pre_breakdown|mtf_no|vol_low|mixed",
}

ALT_MARKET_SAMPLE_SIZE = 12
ALT_MARKET_MIN_VALID = 6
ALT_MARKET_TIMEFRAME = "1H"
ALT_MARKET_CANDLE_LIMIT = 60

SCAN_LOCK_KEY = "scan:short:running"
SCAN_LOCK_TTL = 300
TELEGRAM_OFFSET_KEY = "telegram:offset:short"
TELEGRAM_BOOTSTRAP_DONE_KEY = "telegram:bootstrap_done:short"
TELEGRAM_POLL_LOCK_KEY = "telegram:poll:lock:short"
TELEGRAM_POLL_LOCK_TTL = 10
NEWS_WINDOW_HOURS = 2
ECONOMIC_CALENDAR_URL = "https://www.tradingview.com/economic-calendar/"
STATS_RESET_TS_KEY = "stats:last_reset_ts:short"
EXTRA_ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
ADMIN_CHAT_IDS = set()
if CHAT_ID:
    ADMIN_CHAT_IDS.add(str(CHAT_ID))
if EXTRA_ADMIN_CHAT_ID:
    ADMIN_CHAT_IDS.add(str(EXTRA_ADMIN_CHAT_ID))

CANDLE_CACHE_TTL_15M = 25
CANDLE_CACHE_TTL_1H = 90
CANDLE_CACHE_TTL_4H = 180
CANDLE_CACHE_TTL_DEFAULT = 20
ALT_SNAPSHOT_CACHE_KEY = "cache:alt_snapshot_short"
ALT_SNAPSHOT_CACHE_TTL = 600
ALERT_KEY_PREFIX = "alert:short"
ALERT_BY_MESSAGE_KEY_PREFIX = "alertmsg:short"
ALERT_TTL_SECONDS = 14 * 24 * 3600
TRACK_LEVERAGE = 15.0

# ================================================
# LOGGING
# ================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("okx-short-scanner")

# ================================================
# REDIS
# ================================================
r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.error(f"Redis connection error: {e}")
        r = None
else:
    logger.warning("REDIS_URL not found")

sent_cache = {}
last_candle_cache = {}
last_global_send_ts = 0.0

def safe_json_dumps(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False)


# ================================================
# UTILITY HELPERS (unchanged from original)
# ================================================
def clean_symbol_for_message(symbol: str) -> str:
    return symbol.replace("-SWAP", "")

def get_same_candle_key(symbol: str, candle_time: int, signal_type: str = "short") -> str:
    return f"sent:{signal_type}:{symbol}:{candle_time}"

def get_symbol_cooldown_key(symbol: str, signal_type: str = "short") -> str:
    clean = clean_symbol_for_message(symbol)
    return f"cooldown:{signal_type}:{clean}"

def get_alert_key(alert_id: str) -> str:
    return f"{ALERT_KEY_PREFIX}:{alert_id}"

def get_alert_by_message_key(message_id: str) -> str:
    return f"{ALERT_BY_MESSAGE_KEY_PREFIX}:{message_id}"

def already_sent_same_candle(symbol: str, candle_time: int, signal_type: str = "short") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_same_candle_key(symbol, candle_time, signal_type)))
    except Exception as e:
        logger.error(f"Redis same candle exists error: {e}")
        return False

def is_symbol_on_cooldown(symbol: str, signal_type: str = "short") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_symbol_cooldown_key(symbol, signal_type)))
    except Exception as e:
        logger.error(f"Redis symbol cooldown exists error: {e}")
        return False

def reserve_signal_slot(symbol: str, candle_time: int, signal_type: str = "short") -> bool:
    if not r:
        return True
    same_candle_key = get_same_candle_key(symbol, candle_time, signal_type)
    cooldown_key = get_symbol_cooldown_key(symbol, signal_type)
    try:
        same_candle_ok = r.set(same_candle_key, "1", ex=7200, nx=True)
        if not same_candle_ok:
            return False
        cooldown_ok = r.set(cooldown_key, "1", ex=COOLDOWN_SECONDS, nx=True)
        if not cooldown_ok:
            try:
                r.delete(same_candle_key)
            except Exception:
                pass
            return False
        return True
    except Exception as e:
        logger.error(f"Redis reserve error: {e}")
        return False

def release_signal_slot(symbol: str, candle_time: int, signal_type: str = "short") -> None:
    if not r:
        return
    try:
        r.delete(get_same_candle_key(symbol, candle_time, signal_type))
        r.delete(get_symbol_cooldown_key(symbol, signal_type))
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
        return bool(r.exists("global_cooldown:short"))
    except Exception:
        return False

def set_global_cooldown() -> None:
    if not r:
        return
    try:
        r.set("global_cooldown:short", "1", ex=GLOBAL_COOLDOWN_SECONDS)
    except Exception:
        pass


# ================================================
# ECONOMIC CALENDAR
# ================================================
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
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.tradingview.com/"}
        res = requests.get(url, params=params, headers=headers, timeout=10)
        if res.status_code != 200:
            logger.warning(f"Economic calendar HTTP {res.status_code}")
            return []
        data = res.json()
        events = data if isinstance(data, list) else data.get("result", [])
        high_impact = []
        for event in events:
            importance = str(event.get("importance", "")).lower()
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
        logger.info(f"Economic calendar: {len(high_impact)} high-impact events")
        return high_impact
    except Exception as e:
        logger.warning(f"Economic calendar error: {e}")
        return []

def format_news_warning(events: list) -> str:
    calendar_link = html.escape(ECONOMIC_CALENDAR_URL, quote=True)
    if not events:
        return f'📰 <b>News:</b> No High-Impact news | <a href="{calendar_link}">Calendar</a>'
    parts = []
    for event in events[:2]:
        title = html.escape(event.get("title", "Unknown Event"))
        link = html.escape(event.get("link", ECONOMIC_CALENDAR_URL), quote=True)
        parts.append(f'<a href="{link}">{title}</a>')
    events_text = " | ".join(parts)
    return f'📰 <b>News:</b> {events_text} | <a href="{calendar_link}">Calendar</a>'


# ================================================
# TELEGRAM OFFSET
# ================================================
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
    except Exception:
        return False

def release_telegram_poll_lock() -> None:
    if not r:
        return
    try:
        r.delete(TELEGRAM_POLL_LOCK_KEY)
    except Exception:
        pass


# ================================================
# TELEGRAM API
# ================================================
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
    except Exception as e:
        logger.error(f"Telegram {method} Exception: {e}")
        return {"ok": False, "error": str(e)}

def answer_callback_query(callback_query_id: str, text: str = "") -> None:
    if not callback_query_id:
        return
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    telegram_api_call("answerCallbackQuery", payload)

def send_telegram_message(message: str, reply_markup=None) -> dict:
    if not BOT_TOKEN or not CHAT_ID:
        logger.error("Telegram config missing")
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
            logger.error(f"getUpdates HTTP Error: {response.text}")
            return []
        data = response.json()
        if not data.get("ok"):
            logger.error(f"getUpdates API Error: {data}")
            return []
        return data.get("result", [])
    except Exception as e:
        logger.error(f"getUpdates Exception: {e}")
        return []


TELEGRAM_COMMANDS = {
    "/help": "Show all commands",
    "/how_it_work": "Explain bot logic",
    "/report_1h": "Last 1 hour",
    "/report_today": "Today's report",
    "/report_month": "Last 30 days",
    "/report_all": "All trades",
    "/report_deep": "Advanced analytics",
    "/report_setups": "Best/Worst setup types",
    "/report_scores": "Performance by score",
    "/report_market": "Performance by market condition",
    "/report_losses": "Loss analysis",
    "/report_diagnostics": "Full diagnostic report",
    "/reset_stats": "Reset stats",
    "/stats_since_reset": "Stats since last reset",
}


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
        "1h": "Short Report - Last 1H",
        "today": "Short Report - Today",
        "month": "Short Report - Last 30 days",
        "all": "Short Report - All Time",
    }
    try:
        if period == "today":
            summary = get_trade_summary(
                redis_client=r,
                market_type="futures",
                side="short",
                since_ts=get_local_day_start_ts(),
            )
            return format_period_summary(title_map["today"], summary)
        if period == "month":
            summary = get_period_summary(
                redis_client=r,
                period="30d",
                market_type="futures",
                side="short",
            )
            return format_period_summary(title_map["month"], summary)
        summary = get_period_summary(
            redis_client=r,
            period=period,
            market_type="futures",
            side="short",
        )
        return format_period_summary(title_map.get(period, "Short Report"), summary)
    except Exception as e:
        logger.error(f"build_report_message error on period={period}: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


def build_deep_report_message() -> str:
    try:
        return build_deep_report(r, market_type="futures", side="short")
    except Exception as e:
        logger.error(f"build_deep_report error: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


def build_help_message() -> str:
    lines = [
        "🤖 <b>OKX Scanner Bot - SHORT</b>",
        "",
        "📊 <b>التقارير:</b>",
    ]
    for cmd, desc in TELEGRAM_COMMANDS.items():
        if cmd.startswith("/report"):
            lines.append(f"{cmd} - {desc}")
    lines.extend([
        "",
        "⚙️ <b>أوامر إضافية:</b>",
        "/reset_stats - إعادة تصفير الإحصائيات",
        "/stats_since_reset - الإحصائيات من آخر تصفير",
        "",
        "🔥 <b>نصيحة:</b>",
        "استخدم /report_today لمتابعة أداء الشورت",
    ])
    return "\n".join(lines)


def build_how_it_work_message() -> str:
    return """📘 <b>كيف يعمل بوت الشورت؟</b>

🤖 <b>فكرة البوت:</b>
البوت يبحث عن فرص <b>Short Futures</b> على OKX،
بفلترة متوازنة حتى لا يخنق الإشارات الجيدة.

🔍 <b>منطق العمل:</b>
1. اختيار العملات الأعلى سيولة وحجم تداول
2. تحليل فريم 15m
3. قياس قوة الزخم الهابط
4. تقييم:
• الفوليوم
• RSI
• موقع السعر من المتوسط
• Breakdown / Pre-Breakdown
• تأكيد 1H
• حالة السوق العامة
5. Smart Early Priority للإشارات المبكرة
6. فلتر Short Exhaustion Trap لمنع الدخول المتأخر
7. إعطاء Score من 10
8. إرسال فقط الفرص المقبولة نهائيًا

📌 <b>زر Track:</b>
يعرض لاحقًا:
• الحالة الرسمية
• السعر الحالي
• أقصى هبوط لصالح الصفقة
• أقصى صعود ضد الصفقة
• الرافعة التقديرية ومدة الصفقة

✅ <b>أفضل استخدام:</b>
راجع الشارت بسرعة وخد القرار بعد التأكد من السياق العام."""


# ================================================
# RESET / STATS
# ================================================
def reset_stats(chat_id: str):
    if ADMIN_CHAT_IDS and str(chat_id) not in ADMIN_CHAT_IDS:
        send_telegram_reply(chat_id, f"⛔ غير مسموح\nchat_id={chat_id}")
        return
    if not r:
        send_telegram_reply(chat_id, "❌ Redis غير متصل")
        return
    try:
        deleted = 0
        for key in r.scan_iter("trade:futures:short:*"):
            r.delete(key)
            deleted += 1
        extra_keys = ["open_trades:futures:short", "stats:futures:short"]
        for key in extra_keys:
            try:
                r.delete(key)
            except Exception:
                pass
        reset_ts = int(time.time())
        r.set(STATS_RESET_TS_KEY, str(reset_ts))
        send_telegram_reply(
            chat_id,
            f"🧹 تم تصفير إحصائيات الشورت الحالية بنجاح\n"
            f"📊 مفاتيح الصفقات المحذوفة: {deleted}\n"
            f"🕒 وقت التصفير: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(reset_ts))}"
        )
    except Exception as e:
        send_telegram_reply(chat_id, f"❌ حصل خطأ: {html.escape(str(e))}")


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
            side="short",
            since_ts=reset_ts,
        )
        body = format_period_summary("Since Reset", summary)
        send_telegram_reply(
            chat_id,
            f"📊 <b>Short Stats Since Reset</b>\n"
            f"🕒 منذ: {html.escape(reset_time_text)}\n\n{body}"
        )
    except Exception as e:
        send_telegram_reply(chat_id, f"❌ حصل خطأ: {html.escape(str(e))}")


COMMAND_HANDLERS = {
    "/help": lambda chat_id: send_telegram_reply(chat_id, build_help_message()),
    "/how_it_work": lambda chat_id: send_telegram_reply(chat_id, build_how_it_work_message()),
    "/report_1h": lambda chat_id: send_telegram_reply(chat_id, build_report_message("1h")),
    "/report_today": lambda chat_id: send_telegram_reply(chat_id, build_report_message("today")),
    "/report_month": lambda chat_id: send_telegram_reply(chat_id, build_report_message("month")),
    "/report_all": lambda chat_id: send_telegram_reply(chat_id, build_report_message("all")),
    "/report_deep": lambda chat_id: send_telegram_reply(chat_id, build_deep_report_message()),
    "/report_setups": lambda chat_id: send_telegram_reply(
        chat_id,
        build_setups_report(r, market_type="futures", side="short", period="all")
        if build_setups_report else "خاصية غير متوفرة"
    ),
    "/report_scores": lambda chat_id: send_telegram_reply(
        chat_id,
        build_scores_report(r, market_type="futures", side="short", period="all")
        if build_scores_report else "خاصية غير متوفرة"
    ),
    "/report_market": lambda chat_id: send_telegram_reply(
        chat_id,
        build_market_report(r, market_type="futures", side="short", period="all")
        if build_market_report else "خاصية غير متوفرة"
    ),
    "/report_losses": lambda chat_id: send_telegram_reply(
        chat_id,
        build_losses_report(r, market_type="futures", side="short", period="all")
        if build_losses_report else "خاصية غير متوفرة"
    ),
    "/report_diagnostics": lambda chat_id: send_telegram_reply(
        chat_id,
        build_full_diagnostics_report(r, market_type="futures", side="short", period="all")
        if build_full_diagnostics_report else "خاصية غير متوفرة"
    ),
    "/reset_stats": lambda chat_id: reset_stats(chat_id),
    "/stats_since_reset": lambda chat_id: stats_since_reset(chat_id),
}


# ================================================
# SMART PRICE FORMATTERS
# ================================================
def fmt_price(value: float) -> str:
    try:
        value = float(value)
        if value == 0:
            return "0"
        if value < 0.000001:
            return f"{value:.10f}".rstrip("0").rstrip(".")
        if value < 0.001:
            return f"{value:.8f}".rstrip("0").rstrip(".")
        if value < 1:
            return f"{value:.6f}".rstrip("0").rstrip(".")
        return f"{value:.4f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


def round_price(value: float) -> float:
    try:
        value = float(value)
        if value <= 0:
            return 0.0
        if value >= 100:
            return round(value, 4)
        if value >= 1:
            return round(value, 6)
        if value >= 0.01:
            return round(value, 8)
        if value >= 0.0001:
            return round(value, 10)
        return round(value, 12)
    except Exception:
        return 0.0


# ================================================
# SAFE SIGNAL ROW
# ================================================
def get_signal_row(df):
    if df is None or df.empty:
        return None
    try:
        if len(df) == 1:
            return df.iloc[-1]
        if "confirm" in df.columns:
            confirm_value = df.iloc[-1].get("confirm", 0)
            try:
                is_confirmed = int(float(confirm_value)) == 1
            except Exception:
                is_confirmed = False
            return df.iloc[-1] if is_confirmed else df.iloc[-2]
        return df.iloc[-2]
    except Exception:
        try:
            return df.iloc[-2] if len(df) >= 2 else df.iloc[-1]
        except Exception:
            return None


# ================================================
# ALERT TRACKING (Enhanced)
# ================================================
def build_alert_id(symbol: str, candle_time: int) -> str:
    return f"{clean_symbol_for_message(symbol)}:{int(candle_time)}"


def save_alert_snapshot(alert_data: dict, message_id=None) -> None:
    if not r or not alert_data:
        return
    try:
        alert_id = alert_data.get("alert_id")
        if not alert_id:
            return
        payload = dict(alert_data)
        if message_id is not None:
            payload["message_id"] = str(message_id)
        r.set(get_alert_key(alert_id), safe_json_dumps(payload), ex=ALERT_TTL_SECONDS)
        if message_id:
            r.set(get_alert_by_message_key(str(message_id)), alert_id, ex=ALERT_TTL_SECONDS)
    except Exception as e:
        logger.error(f"save_alert_snapshot error: {e}")


def load_alert_snapshot_by_id(alert_id: str):
    if not r or not alert_id:
        return None
    try:
        raw = r.get(get_alert_key(alert_id))
        if not raw:
            return None
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.error(f"load_alert_snapshot_by_id error: {e}")
        return None


def load_alert_snapshot_by_message_id(message_id: str):
    if not r or not message_id:
        return None
    try:
        alert_id = r.get(get_alert_by_message_key(str(message_id)))
        if not alert_id:
            return None
        return load_alert_snapshot_by_id(alert_id)
    except Exception as e:
        logger.error(f"load_alert_snapshot_by_message_id error: {e}")
        return None


def load_registered_trade_for_alert(alert: dict):
    if not r or not alert:
        return None
    try:
        symbol = alert.get("symbol", "")
        candle_time = int(float(alert.get("candle_time", 0)))
        trade_key = f"trade:futures:short:{symbol}:{candle_time}"
        raw = r.get(trade_key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"load_registered_trade_for_alert error: {e}")
        return None


def should_ignore_track_callback(chat_id: str, message_id: str, alert_id: str) -> bool:
    if not r:
        return False
    try:
        lock_key = f"track:callback:lock:short:{chat_id}:{message_id}:{alert_id}"
        locked = r.set(lock_key, "1", ex=8, nx=True)
        return not bool(locked)
    except Exception as e:
        logger.error(f"should_ignore_track_callback error: {e}")
        return False


def get_last_price(symbol: str) -> float:
    try:
        res = requests.get(OKX_TICKER_SINGLE_URL, params={"instId": symbol}, timeout=10).json()
        data = res.get("data", [])
        if not data:
            return 0.0
        return float(data[0].get("last", 0))
    except Exception as e:
        logger.error(f"get_last_price error on {symbol}: {e}")
        return 0.0


def get_max_move_since_alert(symbol: str, since_ts: int, entry: float, side: str = "short"):
    try:
        candles = get_candles(symbol, TIMEFRAME, 100)
        df = to_dataframe(candles)
        if df is None or df.empty:
            return 0.0, 0.0
        work = df[df["ts"] >= (since_ts * 1000 if since_ts < 10_000_000_000 else since_ts)].copy()
        if work.empty:
            work = df.tail(20).copy()
        lows = work["low"].astype(float)
        highs = work["high"].astype(float)
        if lows.empty or highs.empty or entry <= 0:
            return 0.0, 0.0
        if side == "short":
            favorable_pct = round(((entry - float(lows.min())) / entry) * 100, 2)
            adverse_pct = round(((float(highs.max()) - entry) / entry) * 100, 2)
            return favorable_pct, adverse_pct
        else:
            favorable_pct = round(((float(highs.max()) - entry) / entry) * 100, 2)
            adverse_pct = round(((entry - float(lows.min())) / entry) * 100, 2)
            return favorable_pct, adverse_pct
    except Exception as e:
        logger.error(f"get_max_move_since_alert error: {e}")
        return 0.0, 0.0


def format_official_trade_status(trade: dict) -> str:
    if not trade:
        return "⚪ غير مسجل"
    status = str(trade.get("status", "open")).lower()
    result = str(trade.get("result", "")).lower()
    tp1_hit = bool(trade.get("tp1_hit", False))
    tp2_hit = bool(trade.get("tp2_hit", False))
    if result in ("tp2_win", "full_win", "closed_win") or tp2_hit:
        return "🎯 TP2 Hit | ربح كامل"
    if result in ("tp1_win", "partial_win") or status == "partial" or tp1_hit:
        return "✅ TP1 Hit | ربح جزئي"
    if result in ("safe", "closed_safe") or status in ("safe", "closed_safe"):
        return "🛡️ Safe Exit | خروج آمن"
    if result in ("loss", "sl_hit") or status in ("loss", "closed_loss"):
        return "❌ SL Hit | خسارة"
    if result == "expired" or status == "expired":
        return "⏳ Expired | انتهت المدة"
    if status == "open":
        return "⌛ Open"
    return "⌛ Open"


def build_track_tradingview_link(symbol: str) -> str:
    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
    return f"https://www.tradingview.com/chart/?symbol=OKX:{base}USDT.P"


def build_track_message(alert: dict) -> str:
    try:
        symbol_raw = alert.get("symbol", "")
        symbol = clean_symbol_for_message(symbol_raw)
        official_trade = load_registered_trade_for_alert(alert)

        signal_entry = float(alert.get("entry", 0) or 0)
        effective_entry = signal_entry
        sl = float(alert.get("sl", 0) or 0)
        tp1 = float(alert.get("tp1", 0) or 0)
        tp2 = float(alert.get("tp2", 0) or 0)

        if official_trade and isinstance(official_trade, dict):
            effective_entry = float(official_trade.get("entry", signal_entry) or signal_entry)
            sl = float(official_trade.get("sl", sl) or sl)
            tp1 = float(official_trade.get("tp1", tp1) or tp1)
            tp2 = float(official_trade.get("tp2", tp2) or tp2)

        timeframe = str(alert.get("timeframe", "15m"))
        candle_time = int(float(alert.get("candle_time", 0) or 0))
        created_ts = int(float(alert.get("created_ts", candle_time) or candle_time))
        current_price = get_last_price(symbol_raw)

        favorable_pct, adverse_pct = get_max_move_since_alert(
            symbol=symbol_raw,
            since_ts=candle_time,
            entry=effective_entry,
            side="short",
        )
        duration_seconds = max(0, int(time.time()) - created_ts)
        duration_h = duration_seconds // 3600
        duration_m = (duration_seconds % 3600) // 60

        current_move = 0.0
        if effective_entry > 0 and current_price > 0:
            current_move = round(((effective_entry - current_price) / effective_entry) * 100, 2)

        leveraged_move = round(current_move * TRACK_LEVERAGE, 2)
        leveraged_fav = round(favorable_pct * TRACK_LEVERAGE, 2)
        leveraged_adv = round(adverse_pct * TRACK_LEVERAGE, 2)

        official_status = format_official_trade_status(official_trade)

        if current_price > 0 and effective_entry > 0:
            if current_price <= tp2:
                live_status = "🟢 رابحة الآن"
            elif current_price <= tp1:
                live_status = "🟢 رابحة الآن"
            elif current_price >= sl:
                live_status = "🔴 خاسرة الآن"
            elif current_price < effective_entry:
                live_status = "🟢 رابحة الآن"
            elif current_price > effective_entry:
                live_status = "🔴 عكس الاتجاه الآن"
            else:
                live_status = "🟡 محايدة"
        else:
            live_status = "🟡 قيد المتابعة"

        tv_link = build_track_tradingview_link(symbol_raw)

        msg = (
            f"📌 <b>Alert Track</b>\n\n"
            f"العملة: {html.escape(symbol)}\n"
            f"النوع: Short\n"
            f"الفريم: {html.escape(timeframe)}\n\n"
            f"Signal Entry: {fmt_price(signal_entry)}\n"
            f"📩 Effective Entry: {fmt_price(effective_entry)}\n"
            f"SL: {fmt_price(sl)}\n"
            f"TP1: {fmt_price(tp1)}\n"
            f"TP2: {fmt_price(tp2)}\n\n"
            f"الحالة: {html.escape(live_status)}\n"
            f"⌛ نسخة التتبع: Open\n"
            f"⌛ الحالة الرسمية: {html.escape(official_status)}\n"
            f"السعر الحالي: {fmt_price(current_price)}\n"
            f"الرافعة التقديرية: {TRACK_LEVERAGE:.0f}x\n"
            f"الحركة الحالية: {current_move:+.2f}% | بعد الرافعة: {leveraged_move:+.2f}%\n"
            f"أقصى هبوط لصالح الصفقة: {favorable_pct:+.2f}% | بعد الرافعة: {leveraged_fav:+.2f}%\n"
            f"أقصى صعود ضدك: {adverse_pct:+.2f}% | بعد الرافعة: {leveraged_adv:+.2f}%\n"
            f"المدة: {duration_h}h {duration_m}m\n\n"
            f'🔗 <a href="{html.escape(tv_link, quote=True)}">فتح الشارت على TradingView - {html.escape(timeframe)}</a>'
        )
        return msg
    except Exception as e:
        logger.error(f"build_track_message error: {e}")
        return "❌ حصل خطأ أثناء متابعة الإشارة"


def build_track_reply_markup(alert_id: str) -> dict:
    return {
        "inline_keyboard": [
            [{"text": "📌 Track", "callback_data": f"track_short:{alert_id}"}]
        ]
    }


def handle_callback_query(callback_query: dict):
    try:
        callback_id = callback_query.get("id", "")
        data = callback_query.get("data", "") or ""
        message = callback_query.get("message") or {}
        message_id = str(message.get("message_id", ""))
        chat_id = str((message.get("chat") or {}).get("id", "") or "")
        if not data.startswith("track_short:"):
            answer_callback_query(callback_id, "زر غير مدعوم")
            return
        alert_id = data.split(":", 1)[1].strip()
        if not r:
            answer_callback_query(callback_id, "Redis غير متصل")
            return
        if should_ignore_track_callback(chat_id, message_id, alert_id):
            answer_callback_query(callback_id, "تم استلام الطلب بالفعل")
            return
        alert = load_alert_snapshot_by_id(alert_id)
        if not alert and message_id:
            alert = load_alert_snapshot_by_message_id(message_id)
        if not alert:
            answer_callback_query(callback_id, "بيانات الإشارة غير متاحة أو انتهت صلاحيتها")
            return
        answer_callback_query(callback_id, "...جار جلب نتيجة الإشارة")
        if chat_id:
            send_telegram_reply(chat_id, build_track_message(alert))
    except Exception as e:
        logger.error(f"handle_callback_query error: {e}")
        try:
            answer_callback_query(callback_query.get("id", ""), "حصل خطأ")
        except Exception:
            pass


# ================================================
# SAFE WRAPPERS
# ================================================
def safe_get_setup_type_stats(redis_client, market_type, side, setup_type, since_ts=None):
    try:
        kwargs = dict(redis_client=redis_client, market_type=market_type, side=side,
                      setup_type=setup_type, since_ts=since_ts)
        return get_setup_type_stats(**kwargs)
    except TypeError:
        try:
            kwargs.pop("since_ts", None)
            return get_setup_type_stats(**kwargs)
        except TypeError:
            try:
                return get_setup_type_stats(redis_client, market_type, side, setup_type)
            except Exception:
                return {"closed": 0, "winrate": 0.0}
    except Exception:
        return {"closed": 0, "winrate": 0.0}


def safe_register_trade(**kwargs):
    try:
        return register_trade(**kwargs)
    except TypeError as e:
        logger.warning(f"register_trade full kwargs TypeError, trying minimal payload: {e}")
        minimal = {
            "redis_client": kwargs.get("redis_client"),
            "symbol": kwargs.get("symbol"),
            "market_type": kwargs.get("market_type"),
            "side": kwargs.get("side"),
            "candle_time": kwargs.get("candle_time"),
            "entry": kwargs.get("entry"),
            "sl": kwargs.get("sl"),
            "tp1": kwargs.get("tp1"),
            "tp2": kwargs.get("tp2"),
            "score": kwargs.get("score"),
            "timeframe": kwargs.get("timeframe"),
            "btc_mode": kwargs.get("btc_mode"),
            "funding_label": kwargs.get("funding_label"),
            "reasons": kwargs.get("reasons"),
            "pre_breakout": kwargs.get("pre_breakout"),
            "breakout": kwargs.get("breakout"),
            "vol_ratio": kwargs.get("vol_ratio"),
            "candle_strength": kwargs.get("candle_strength"),
            "mtf_confirmed": kwargs.get("mtf_confirmed"),
            "is_new": kwargs.get("is_new"),
            "btc_dominance_proxy": kwargs.get("btc_dominance_proxy"),
            "change_24h": kwargs.get("change_24h"),
        }
        minimal = {k: v for k, v in minimal.items() if v is not None}
        try:
            return register_trade(**minimal)
        except Exception as e2:
            logger.error(f"register_trade minimal payload failed: {e2}")
            return None
    except Exception as e:
        logger.error(f"register_trade failed: {e}")
        return None


# ================================================
# SETUP TYPE SYSTEM
# ================================================
def get_setup_family(candidate: dict) -> str:
    if candidate.get("is_reverse"):
        return "reverse"
    if candidate.get("breakdown"):
        return "breakdown"
    if candidate.get("pre_breakdown"):
        return "pre_breakdown"
    if candidate.get("pullback_short"):
        return "pullback_short"
    return "continuation"

def get_setup_volume_band(vol_ratio: float) -> str:
    if vol_ratio >= 1.80:
        return "vol_high"
    elif vol_ratio >= 1.25:
        return "vol_mid"
    return "vol_low"

def get_setup_market_regime(market_state: str) -> str:
    allowed = {"bull_market", "alt_season", "mixed", "btc_leading", "risk_off"}
    return market_state if market_state in allowed else "mixed"

def build_setup_type(candidate: dict) -> str:
    family = get_setup_family(candidate)
    mtf = "mtf_yes" if candidate.get("mtf_confirmed") else "mtf_no"
    vol_band = get_setup_volume_band(candidate.get("vol_ratio", 1.0))
    regime = get_setup_market_regime(candidate.get("market_state", "mixed"))
    return f"{family}|{mtf}|{vol_band}|{regime}"

def get_hybrid_label_from_stats(setup_stats: dict) -> str:
    if not setup_stats:
        setup_stats = {"closed": 0, "winrate": 0.0}
    closed = setup_stats.get("closed", 0)
    winrate = setup_stats.get("winrate", 0.0)
    if closed < 8:
        return f"⚪ No Data ({closed} trades)"
    if winrate >= 70 and closed >= 15:
        return f"🔥 ELITE ({winrate:.0f}% | {closed} trades)"
    if winrate >= 55 and closed >= 8:
        return f"🟢 GOOD ({winrate:.0f}% | {closed} trades)"
    return f"⚠️ WEAK ({winrate:.0f}% | {closed} trades)"


# ================================================
# BREAKDOWN QUALITY
# ================================================
def get_breakdown_quality(df, vol_ratio: float) -> str:
    try:
        if df is None or df.empty or len(df) < 5:
            return "none"
        signal_row = get_signal_row(df)
        if signal_row is None:
            return "none"
        idx = signal_row.name
        if idx is None or idx < 3:
            return "none"
        close = float(signal_row["close"])
        open_ = float(signal_row["open"])
        high = float(signal_row["high"])
        low = float(signal_row["low"])
        candle_range = high - low
        if candle_range <= 0:
            return "none"
        body = abs(close - open_)
        lower_wick = min(close, open_) - low
        upper_wick = high - max(close, open_)
        close_position = (close - low) / candle_range
        recent_low = float(df["low"].iloc[max(0, idx - 20):idx].min())
        bearish_close = close < open_
        broke_below = close < recent_low
        if not bearish_close or not broke_below:
            return "none"
        score = 0
        if close_position <= 0.35:
            score += 2
        elif close_position <= 0.50:
            score += 1
        if upper_wick <= body * 0.6:
            score += 1
        if vol_ratio >= 1.3:
            score += 1
        if score >= 4:
            return "strong"
        if score >= 2:
            return "ok"
        return "weak"
    except Exception:
        return "none"


# ================================================
# TECHNICAL INDICATORS (ENHANCED)
# ================================================
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
    return ma, ma + std * std_mult, ma - std * std_mult

def compute_adx(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().replace(0, pd.NA)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)
    di_sum = (plus_di + minus_di).replace(0, pd.NA)
    dx = 100 * ((plus_di - minus_di).abs() / di_sum)
    adx = dx.rolling(period).mean()
    return adx.fillna(0), plus_di.fillna(0), minus_di.fillna(0)

def compute_stoch_rsi(df, period=14, smooth_k=3, smooth_d=3):
    rsi = df["rsi"]
    min_rsi = rsi.rolling(period).min()
    max_rsi = rsi.rolling(period).max()
    denom = (max_rsi - min_rsi).replace(0, pd.NA)
    stoch_rsi = 100 * (rsi - min_rsi) / denom
    stoch_rsi = stoch_rsi.fillna(50)
    stoch_k = stoch_rsi.rolling(smooth_k).mean()
    stoch_d = stoch_k.rolling(smooth_d).mean()
    return stoch_k.fillna(50), stoch_d.fillna(50)

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

    # VWAP rolling 20
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].fillna(0).replace([float("inf"), -float("inf")], 0)
    tp_vol = typical_price * vol
    sum_tp_vol = tp_vol.rolling(20, min_periods=1).sum()
    sum_vol = vol.rolling(20, min_periods=1).sum()
    vwap = (sum_tp_vol / sum_vol.replace(0, pd.NA)).fillna(0)
    df["vwap"] = vwap.astype(float)

    # MACD
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - macd_signal
    df["macd"] = macd_line.astype(float)
    df["macd_signal"] = macd_signal.astype(float)
    df["macd_hist"] = macd_hist.astype(float)

    # Bollinger Bands, Stoch RSI, ADX
    df["bb_mid"], df["bb_upper"], df["bb_lower"] = compute_bollinger_bands(df["close"])
    df["stoch_rsi_k"], df["stoch_rsi_d"] = compute_stoch_rsi(df)
    df["adx"], df["plus_di"], df["minus_di"] = compute_adx(df)

    # Fill only VWAP/MACD/BB/Stoch/ADX zeros, do NOT fill MA/RSI/ATR
    df["vwap"] = df["vwap"].fillna(0)
    df["macd"] = df["macd"].fillna(0)
    df["macd_signal"] = df["macd_signal"].fillna(0)
    df["macd_hist"] = df["macd_hist"].fillna(0)
    df["bb_mid"] = df["bb_mid"].fillna(0)
    df["bb_upper"] = df["bb_upper"].fillna(0)
    df["bb_lower"] = df["bb_lower"].fillna(0)
    df["stoch_rsi_k"] = df["stoch_rsi_k"].fillna(50)
    df["stoch_rsi_d"] = df["stoch_rsi_d"].fillna(50)
    df["adx"] = df["adx"].fillna(0)
    df["plus_di"] = df["plus_di"].fillna(0)
    df["minus_di"] = df["minus_di"].fillna(0)

    return df


def get_signal_candle_time(df):
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return int(time.time() // (15 * 60))
        ts = int(signal_row["ts"])
        if ts > 10_000_000_000:
            return ts // 1000
        return ts
    except Exception:
        return int(time.time() // (15 * 60))

def get_vwap_distance_percent(df) -> float:
    try:
        row = get_signal_row(df)
        if row is None:
            return 0.0
        close = float(row.get("close", 0))
        vwap = float(row.get("vwap", 0))
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
        rsi_now = float(row.get("rsi", 50))
        rsi_prev = float(df.iloc[idx - bars].get("rsi", rsi_now))
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
        now = float(row.get("macd_hist", 0))
        prev = float(df.iloc[idx - bars].get("macd_hist", now))
        return round(now - prev, 6)
    except Exception:
        return 0.0

def get_distance_from_ma_percent(df) -> float:
    """Short distance: positive means price below MA (favourable)."""
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return 0.0
        ma = float(signal_row.get("ma", 0))
        close = float(signal_row["close"])
        if ma <= 0:
            return 0.0
        return round(((ma - close) / ma) * 100, 4)
    except Exception:
        return 0.0

def get_distance_above_ma_percent(df) -> float:
    """Positive means price above MA (used for overbought reversal)."""
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return 0.0
        ma = float(signal_row.get("ma", 0))
        close = float(signal_row["close"])
        if ma <= 0:
            return 0.0
        return round(((close - ma) / ma) * 100, 4)
    except Exception:
        return 0.0

def get_volume_ratio(df) -> float:
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return 1.0
        idx = signal_row.name
        if idx is None or idx < 1:
            return 1.0
        start_idx = max(0, idx - 20)
        avg_volume = float(df.iloc[start_idx:idx]["volume"].mean())
        last_volume = float(signal_row["volume"])
        if avg_volume <= 0:
            return 1.0
        return round(last_volume / avg_volume, 4)
    except Exception:
        return 1.0

def get_candle_strength_ratio(df) -> float:
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return 0.0
        high = float(signal_row["high"])
        low = float(signal_row["low"])
        open_ = float(signal_row["open"])
        close = float(signal_row["close"])
        full = high - low
        if full <= 0:
            return 0.0
        body = abs(close - open_)
        return round(body / full, 4)
    except Exception:
        return 0.0

def get_candle_anatomy(df) -> Dict[str, float]:
    """Returns candle structure ratios."""
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return {"body_ratio": 0, "upper_wick_ratio": 0, "lower_wick_ratio": 0, "close_position": 0}
        high = float(signal_row["high"])
        low = float(signal_row["low"])
        open_ = float(signal_row["open"])
        close = float(signal_row["close"])
        full = high - low
        if full <= 0:
            return {"body_ratio": 0, "upper_wick_ratio": 0, "lower_wick_ratio": 0, "close_position": 0}
        body = abs(close - open_)
        upper_wick = high - max(open_, close)
        lower_wick = min(open_, close) - low
        close_position = (close - low) / full
        return {
            "body_ratio": round(body / full, 4),
            "upper_wick_ratio": round(upper_wick / full, 4),
            "lower_wick_ratio": round(lower_wick / full, 4),
            "close_position": round(close_position, 4),
        }
    except Exception:
        return {"body_ratio": 0, "upper_wick_ratio": 0, "lower_wick_ratio": 0, "close_position": 0}

def get_bb_position(df) -> str:
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return "unknown"
        close = float(signal_row["close"])
        bb_upper = float(signal_row.get("bb_upper", 0))
        bb_lower = float(signal_row.get("bb_lower", 0))
        bb_mid = float(signal_row.get("bb_mid", 0))
        if close <= bb_lower:
            return "below_lower"
        elif close <= bb_mid:
            return "below_mid"
        elif close >= bb_upper:
            return "above_upper"
        elif bb_mid > 0 and bb_upper > bb_mid:
            return "inside"
        return "unknown"
    except Exception:
        return "unknown"

def is_higher_timeframe_confirmed(symbol: str) -> bool:
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
        ma_value = signal_row.get("ma", None)
        below_ma = ma_value is not None and float(signal_row["close"]) < float(ma_value)
        low_rsi = float(signal_row.get("rsi", 50)) <= 50
        last_3 = df.iloc[idx - 3:idx]
        red_candles = sum(1 for _, row in last_3.iterrows() if float(row["close"]) < float(row["open"]))
        structure_weak = red_candles >= 2
        return bool((below_ma and low_rsi) or (below_ma and structure_weak) or (low_rsi and structure_weak))
    except Exception:
        return False


# ================================================
# OKX DATA (unchanged)
# ================================================
def get_candle_cache_key(symbol: str, timeframe: str, limit: int) -> str:
    return f"candles:short:{symbol}:{timeframe}:{limit}"

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
                    return data
        except Exception:
            pass
    try:
        params = {"instId": symbol, "bar": timeframe, "limit": limit}
        res = requests.get(OKX_CANDLES_URL, params=params, timeout=20).json()
        data = res.get("data", [])
        if data and r:
            try:
                r.set(cache_key, json.dumps(data), ex=cache_ttl)
            except Exception:
                pass
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

def get_ranked_pairs():
    try:
        res = requests.get(OKX_TICKERS_URL, params={"instType": "SWAP"}, timeout=20).json()
        data = res.get("data", [])
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
        filtered.sort(
            key=lambda x: (x.get("_rank_change_24h", 0), x.get("_rank_volume_24h", 0)),
            reverse=True,
        )
        top = filtered[:SCAN_LIMIT]
        logger.info(f"Ranked pairs: {len(top)} after filtering")
        return top
    except Exception as e:
        logger.error(f"get_ranked_pairs error: {e}")
        return []

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
        rsi_value = float(signal_row.get("rsi", 50))
        if ma_value is not None:
            if float(signal_row["close"]) > float(ma_value) and rsi_value >= 55:
                return "🟢 صاعد"
            if float(signal_row["close"]) < float(ma_value) and rsi_value <= 45:
                return "🔴 هابط"
        return "🟡 محايد"
    except Exception as e:
        logger.error(f"BTC mode error: {e}")
        return "🟡 محايد"

def get_market_state(btc_mode: str, alt_snapshot: dict):
    alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك")
    if "🔴 هابط" in btc_mode and "🔴 ضعيف" in alt_mode:
        return {
            "market_state": "risk_off",
            "market_state_label": "🟢 Short-Friendly",
            "market_bias_label": "🟢 السوق ضعيف والسيولة هابطة",
            "btc_short_bias": "🟢 داعم للشورت",
        }
    if "🟢 صاعد" in btc_mode and "🔴 ضعيف" in alt_mode:
        return {
            "market_state": "btc_leading",
            "market_state_label": "🟡 Mixed Pressure",
            "market_bias_label": "🟡 الألت ضعيف لكن BTC ما زال صاعد",
            "btc_short_bias": "🟡 انتقائي",
        }
    if "🟢 صاعد" in btc_mode and "🟢 قوي" in alt_mode:
        return {
            "market_state": "bull_market",
            "market_state_label": "🔴 Bull Market",
            "market_bias_label": "🔴 السوق صاعد والشورت أخطر",
            "btc_short_bias": "🔴 ضد الشورت",
        }
    if ("🟡 محايد" in btc_mode or "🔴 هابط" in btc_mode) and "🟢 قوي" in alt_mode:
        return {
            "market_state": "alt_season",
            "market_state_label": "🟡 Alt Strength",
            "market_bias_label": "🟡 بعض الألت قوي رغم ضعف BTC",
            "btc_short_bias": "🟡 انتقائي",
        }
    return {
        "market_state": "mixed",
        "market_state_label": "🟡 Mixed",
        "market_bias_label": "🟡 السوق مختلط والسيولة غير محسومة",
        "btc_short_bias": "🟡 محايد",
    }

def get_alt_market_snapshot(ranked_pairs, sample_size=ALT_MARKET_SAMPLE_SIZE):
    if not ranked_pairs:
        return {
            "sample_size": 0,
            "valid_count": 0,
            "above_ma_ratio": 0.0,
            "rsi_support_ratio": 0.0,
            "positive_24h_ratio": 0.0,
            "alt_strength_score": 0.0,
            "alt_mode": "🟡 متماسك",
        }
    sampled = []
    for item in ranked_pairs:
        symbol = item.get("instId", "")
        if symbol == "BTC-USDT-SWAP":
            continue
        sampled.append(item)
        if len(sampled) >= sample_size:
            break
    valid = 0
    above_ma = 0
    rsi_ok = 0
    pos_24h = 0
    for item in sampled:
        symbol = item.get("instId", "")
        change_24h = extract_24h_change_percent(item)
        candles = get_candles(symbol, ALT_MARKET_TIMEFRAME, ALT_MARKET_CANDLE_LIMIT)
        df = to_dataframe(candles)
        if df is None or df.empty or len(df) < 25:
            continue
        try:
            signal_row = get_signal_row(df)
            if signal_row is None:
                continue
            close = float(signal_row["close"])
            ma = float(signal_row.get("ma", 0))
            rsi = float(signal_row.get("rsi", 50))
            valid += 1
            if ma > 0 and close > ma:
                above_ma += 1
            if rsi >= 52:
                rsi_ok += 1
            if change_24h > 0:
                pos_24h += 1
        except Exception:
            continue
    if valid == 0:
        return {
            "sample_size": len(sampled),
            "valid_count": 0,
            "above_ma_ratio": 0.0,
            "rsi_support_ratio": 0.0,
            "positive_24h_ratio": 0.0,
            "alt_strength_score": 0.0,
            "alt_mode": "🟡 متماسك",
        }
    above_ma_ratio = round(above_ma / valid, 4)
    rsi_support_ratio = round(rsi_ok / valid, 4)
    positive_24h_ratio = round(pos_24h / valid, 4)
    alt_strength_score = round(
        (above_ma_ratio * 0.45) + (rsi_support_ratio * 0.35) + (positive_24h_ratio * 0.20), 4
    )
    if valid < ALT_MARKET_MIN_VALID:
        alt_mode = "🟡 متماسك"
    elif alt_strength_score >= 0.68 and above_ma_ratio >= 0.58 and rsi_support_ratio >= 0.50:
        alt_mode = "🟢 قوي"
    elif alt_strength_score >= 0.50:
        alt_mode = "🟡 متماسك"
    else:
        alt_mode = "🔴 ضعيف"
    return {
        "sample_size": len(sampled),
        "valid_count": valid,
        "above_ma_ratio": above_ma_ratio,
        "rsi_support_ratio": rsi_support_ratio,
        "positive_24h_ratio": positive_24h_ratio,
        "alt_strength_score": alt_strength_score,
        "alt_mode": alt_mode,
    }


def is_valid_candle_timing(df) -> bool:
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        now = int(time.time())
        candle_seconds = 15 * 60
        last_completed_ts = (now // candle_seconds) * candle_seconds
        ts = int(signal_row["ts"])
        if ts > 10_000_000_000:
            ts = ts // 1000
        candle_age = last_completed_ts - ts
        return 0 <= candle_age <= (candle_seconds * 2)
    except Exception:
        return False

def is_pre_breakdown(df, lookback=PRE_BREAKDOWN_LOOKBACK) -> bool:
    try:
        min_len = max(lookback + 6, PRE_BREAKDOWN_BASELINE_VOL_BARS + PRE_BREAKDOWN_RECENT_VOL_BARS + 2)
        if df is None or df.empty or len(df) < min_len:
            return False
        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name
        if idx is None or idx < max(lookback, PRE_BREAKDOWN_BASELINE_VOL_BARS + PRE_BREAKDOWN_RECENT_VOL_BARS):
            return False
        close = float(signal_row["close"])
        ma_value = float(signal_row.get("ma", close))
        recent_low = float(df["low"].iloc[idx - lookback:idx].min())
        if recent_low <= 0 or close <= 0:
            return False
        proximity = close / recent_low
        if not (1.0 < proximity <= PRE_BREAKDOWN_PROXIMITY_MAX):
            return False
        recent_vols = df["volume"].iloc[idx - PRE_BREAKDOWN_RECENT_VOL_BARS:idx].astype(float).tolist()
        vol_increasing = (
            len(recent_vols) == PRE_BREAKDOWN_RECENT_VOL_BARS
            and recent_vols[1] >= recent_vols[0]
            and recent_vols[2] >= recent_vols[1]
        )
        baseline_start = idx - (PRE_BREAKDOWN_BASELINE_VOL_BARS + PRE_BREAKDOWN_RECENT_VOL_BARS)
        baseline_end = idx - PRE_BREAKDOWN_RECENT_VOL_BARS
        baseline_vols = df["volume"].iloc[baseline_start:baseline_end].astype(float)
        if baseline_vols.empty:
            return False
        recent_avg_vol = sum(recent_vols) / len(recent_vols)
        baseline_avg_vol = float(baseline_vols.mean())
        if baseline_avg_vol <= 0:
            return False
        volume_significant = recent_avg_vol >= baseline_avg_vol * PRE_BREAKDOWN_VOLUME_SIGNIFICANCE
        recent_atr = float(signal_row.get("atr", 0))
        prev_atr = float(df["atr"].iloc[idx - 5:idx].mean())
        compressed = prev_atr > 0 and recent_atr > 0 and recent_atr < prev_atr * 0.95
        below_ma = close < ma_value
        return vol_increasing and volume_significant and compressed and below_ma
    except Exception:
        return False

def is_new_listing_by_candles(candles) -> bool:
    try:
        return len(candles) < NEW_LISTING_MAX_CANDLES
    except Exception:
        return False

def early_bearish_signal(df):
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
        open_ = float(last["open"])
        close = float(last["close"])
        high = float(last["high"])
        low = float(last["low"])
        rsi_now = float(last.get("rsi", 50))
        rsi_prev = float(prev.get("rsi", 50))
        candle_range = high - low
        body = abs(close - open_)
        body_ratio = (body / candle_range) if candle_range > 0 else 0.0
        avg_vol = float(df.iloc[max(0, idx - 10):idx]["volume"].mean())
        vol_ok = avg_vol > 0 and float(last["volume"]) >= avg_vol * 1.08
        bearish_close = close < open_
        weak_close_position = candle_range > 0 and ((close - low) / candle_range) <= 0.45
        rsi_weakening = rsi_now < 52 and rsi_now <= rsi_prev
        real_body = body_ratio >= 0.32
        checks = sum([bearish_close, weak_close_position, rsi_weakening, vol_ok, real_body])
        return checks >= 3
    except Exception:
        return False

def is_losing_intraday_strength(df) -> bool:
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
        lower_close = float(last["close"]) <= float(prev["close"])
        weaker_rsi = float(last.get("rsi", 50)) <= float(prev.get("rsi", 50))
        not_near_high = float(last["close"]) < (float(last["high"]) * 0.998)
        checks = sum([lower_close, weaker_rsi, not_near_high])
        return checks >= 2
    except Exception:
        return False

def is_overextended_on_4h(symbol: str, change_24h: float) -> bool:
    try:
        if not OVEREXTENDED_REVERSAL_ENABLED:
            return False
        candles = get_candles(symbol, OVEREXTENDED_REVERSAL_HTF, 120)
        df = to_dataframe(candles)
        if df is None or df.empty or len(df) < 30:
            return False
        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name
        if idx is None or idx < 3:
            return False
        dist_ma_4h = get_distance_above_ma_percent(df)
        rsi_4h = float(signal_row.get("rsi", 50))
        last_3 = df.iloc[idx - 3:idx + 1]
        green_count = sum(1 for _, row in last_3.iterrows() if float(row["close"]) >= float(row["open"]))
        near_high = float(signal_row["high"]) > 0 and (float(signal_row["close"]) >= (float(signal_row["high"]) * 0.985))
        checks = 0
        if dist_ma_4h >= OVEREXTENDED_REVERSAL_MIN_DIST_MA_4H:
            checks += 1
        if rsi_4h >= OVEREXTENDED_REVERSAL_MIN_RSI_4H:
            checks += 1
        if change_24h >= OVEREXTENDED_REVERSAL_MIN_24H_CHANGE:
            checks += 1
        if green_count >= 3:
            checks += 1
        if near_high:
            checks += 1
        return checks >= 4
    except Exception:
        return False

def is_1h_reversal_weakening(symbol: str) -> bool:
    try:
        candles = get_candles(symbol, OVEREXTENDED_REVERSAL_CONFIRM_TF, 120)
        df = to_dataframe(candles)
        if df is None or df.empty or len(df) < 20:
            return False
        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name
        if idx is None or idx < 2:
            return False
        last = df.iloc[idx]
        prev = df.iloc[idx - 1]
        close_now = float(last["close"])
        open_now = float(last["open"])
        high_now = float(last["high"])
        low_now = float(last["low"])
        close_prev = float(prev["close"])
        rsi_now = float(last.get("rsi", 50))
        rsi_prev = float(prev.get("rsi", 50))
        ma_now = float(last.get("ma", 0))
        candle_range = high_now - low_now
        weak_close = candle_range > 0 and ((close_now - low_now) / candle_range) <= 0.45
        bearish_close = close_now < open_now
        lower_close = close_now <= close_prev
        rsi_turn = rsi_now <= rsi_prev
        not_near_high = high_now > 0 and close_now < (high_now * 0.995)
        lost_ma = ma_now > 0 and close_now < ma_now
        checks = sum([bearish_close, lower_close, rsi_turn, weak_close, not_near_high, lost_ma])
        return checks >= 3
    except Exception:
        return False

def is_15m_reverse_trigger(df, early_signal: bool, breakdown: bool, pre_breakdown: bool, vol_ratio: float) -> bool:
    try:
        if df is None or df.empty or len(df) < 10:
            return False
        if breakdown or pre_breakdown or early_signal:
            return True
        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name
        if idx is None or idx < 1:
            return False
        last = df.iloc[idx]
        prev = df.iloc[idx - 1]
        open_ = float(last["open"])
        close = float(last["close"])
        high = float(last["high"])
        low = float(last["low"])
        prev_close = float(prev["close"])
        rsi_now = float(last.get("rsi", 50))
        rsi_prev = float(prev.get("rsi", 50))
        candle_range = high - low
        weak_close = candle_range > 0 and ((close - low) / candle_range) <= 0.45
        bearish_close = close < open_
        lower_close = close <= prev_close
        rsi_turn = rsi_now <= rsi_prev
        checks = 0
        if bearish_close:
            checks += 1
        if weak_close:
            checks += 1
        if lower_close:
            checks += 1
        if rsi_turn:
            checks += 1
        if vol_ratio >= OVEREXTENDED_REVERSAL_MIN_VOL_RATIO_15M:
            checks += 1
        return checks >= 3
    except Exception:
        return False

def is_overextended_reversal_short(
    symbol: str, df, change_24h: float, vol_ratio: float, early_signal: bool, breakdown: bool, pre_breakdown: bool
) -> bool:
    try:
        if not OVEREXTENDED_REVERSAL_ENABLED:
            return False
        overextended_4h = is_overextended_on_4h(symbol, change_24h)
        weakening_1h = is_1h_reversal_weakening(symbol)
        trigger_15m = is_15m_reverse_trigger(df, early_signal, breakdown, pre_breakdown, vol_ratio)
        return bool(overextended_4h and weakening_1h and trigger_15m)
    except Exception:
        return False


# ================================================
# PULLBACK SHORT LOGIC (FIXED)
# ================================================
def is_pullback_short(df, vol_ratio, mtf_confirmed):
    """
    Detect a pullback short: price under MA, pullback near MA/VWAP,
    bearish candle, RSI rejection under 55, ADX >= 18, minus_di > plus_di,
    volume ratio >= 1.08.
    Returns True if at least 5 conditions satisfied.
    """
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name
        if idx is None or idx < 2:
            return False
        ma = float(signal_row.get("ma", 0))
        close = float(signal_row["close"])
        vwap = float(signal_row.get("vwap", 0))
        rsi = float(signal_row.get("rsi", 50))
        adx = float(signal_row.get("adx", 0))
        plus_di = float(signal_row.get("plus_di", 0))
        minus_di = float(signal_row.get("minus_di", 0))

        conditions = []
        # 1. price below MA
        conditions.append(close < ma and ma > 0)
        # 2. price near MA or VWAP (within 2% above or 1% below)
        near_ma = ma > 0 and abs(close - ma) / ma <= 0.02
        near_vwap = vwap > 0 and abs(close - vwap) / vwap <= 0.02
        conditions.append(near_ma or near_vwap)
        # 3. bearish candle
        conditions.append(close < float(signal_row["open"]))
        # 4. RSI < 55
        conditions.append(rsi < 55)
        # 5. ADX >= 18
        conditions.append(adx >= 18)
        # 6. minus_di > plus_di
        conditions.append(minus_di > plus_di)
        # 7. volume ratio >= 1.08
        conditions.append(vol_ratio >= 1.08)
        # 8. mtf_confirmed as bonus (not mandatory)
        if mtf_confirmed:
            conditions.append(True)

        satisfied = sum(conditions)
        return satisfied >= 5
    except Exception:
        return False


# ================================================
# LATE SHORT AFTER DROP FILTER (ADJUSTED)
# ================================================
def is_late_short_after_drop(
    dist_ma: float,
    rsi_now: float,
    rsi_slope: float,
    vol_ratio: float,
    candle_strength: float,
    lower_wick_ratio: float,
    bb_position: str,
    is_reverse: bool,
    breakdown_quality: str,
    pre_breakdown: bool,
) -> Dict[str, Any]:
    """Reject short if it looks like entering after a large drop, 
    with stricter threshold for strong breakdowns."""
    if is_reverse or pre_breakdown:
        return {"reject": False, "reasons": [], "checks": 0}
    checks = 0
    reasons = []
    if dist_ma >= 3.5:
        checks += 1
        reasons.append("far_below_ma")
    if rsi_now <= 35:
        checks += 1
        reasons.append("rsi_oversold")
    if rsi_slope >= 0:
        checks += 1
        reasons.append("rsi_recovering")
    if vol_ratio >= 1.6:
        checks += 1
        reasons.append("high_volume_spike")
    if candle_strength >= 0.58:
        checks += 1
        reasons.append("big_red_candle")
    if lower_wick_ratio >= 0.32:
        checks += 1
        reasons.append("long_lower_wick")
    if bb_position == "below_lower":
        checks += 1
        reasons.append("below_lower_band")
    reject_level = 6 if breakdown_quality == "strong" else 5
    reject = checks >= reject_level
    return {"reject": reject, "reasons": reasons, "checks": checks}


# ================================================
# SCORING HELPERS (ENHANCED)
# ================================================
def classify_early_priority_short(
    early_signal: bool, breakdown: bool, pre_breakdown: bool, dist_ma: float,
    vol_ratio: float, candle_strength: float, mtf_confirmed: bool,
    losing_strength: bool, market_state: str,
) -> str:
    try:
        if not early_signal or breakdown or pre_breakdown:
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
        if losing_strength:
            score += 1
        if market_state in ("risk_off", "btc_leading", "mixed"):
            score += 1
        elif market_state in ("bull_market", "alt_season"):
            score -= 1
        if score >= 7:
            return "strong"
        if score >= 4:
            return "medium"
        return "weak"
    except Exception:
        return "none"

def get_early_priority_score_bonus(priority: str) -> float:
    if priority == "strong":
        return 0.25
    if priority == "medium":
        return 0.10
    if priority == "weak":
        return -0.10
    return 0.0

def get_early_priority_threshold_adjustment(priority: str) -> float:
    if priority == "strong":
        return -0.25
    if priority == "medium":
        return -0.10
    if priority == "weak":
        return 0.10
    return 0.0

def get_early_priority_min_score_adjustment(priority: str) -> float:
    if priority == "strong":
        return -0.20
    if priority == "medium":
        return -0.10
    return 0.0

def get_early_priority_momentum_bonus(priority: str) -> float:
    if priority == "strong":
        return 0.35
    if priority == "medium":
        return 0.15
    if priority == "weak":
        return -0.10
    return 0.0

def calculate_stop_loss_short(df, entry, signal_type="standard"):
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return round_price(entry)
        idx = signal_row.name
        atr_value = float(signal_row.get("atr", 0))
        recent_high = float(df["high"].iloc[max(0, idx - 4):idx + 1].max())

        if signal_type == "breakdown":
            atr_mult = 1.35
        elif signal_type == "pre_breakdown":
            atr_mult = 1.75
        elif signal_type == "new_listing":
            atr_mult = 2.10
        elif signal_type == "reverse":
            atr_mult = 1.90
        elif signal_type == "pullback_short":
            atr_mult = 1.45
        else:
            atr_mult = 1.55

        atr_stop = float(entry) + (atr_value * atr_mult)
        structure_buffer = atr_value * 0.20
        structure_stop = recent_high + structure_buffer
        stop_loss = max(atr_stop, structure_stop)

        min_pct = {
            "breakdown": 1.1,
            "standard": 1.3,
            "pre_breakdown": 1.6,
            "new_listing": 2.0,
            "reverse": 1.6,
            "pullback_short": 1.2,
        }.get(signal_type, 1.3)

        current_pct = ((stop_loss - float(entry)) / float(entry)) * 100
        if current_pct < min_pct:
            stop_loss = float(entry) * (1 + (min_pct / 100))

        return round_price(stop_loss)
    except Exception:
        return round_price(entry)


def calc_tp_short(entry: float, sl: float, rr: float) -> float:
    risk = float(sl) - float(entry)
    return round_price(float(entry) - (risk * rr))


def get_rr_targets(signal_type="standard", entry_timing=""):
    if signal_type == "breakdown":
        return 1.4, 2.3
    if signal_type == "pre_breakdown":
        return 1.7, 2.8
    if signal_type == "new_listing":
        return 1.9, 3.2
    if signal_type == "reverse":
        return 1.5, 2.5
    if signal_type == "pullback_short":
        return 1.5, 2.5
    if "🔴 متأخر" in entry_timing:
        return 1.7, 2.8
    return 1.4, 2.4

def build_tradingview_link(symbol):
    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
    return f"https://www.tradingview.com/chart/?symbol=OKX:{base}USDT.P"

def classify_opportunity_type_short(
    breakdown: bool, pre_breakdown: bool, dist_ma: float, mtf_confirmed: bool,
    is_reverse: bool = False, pullback_short: bool = False,
) -> str:
    try:
        if is_reverse:
            return "Overextended Reversal"
        if pullback_short:
            return "Pullback Short"
        if pre_breakdown and not breakdown:
            return "Pre-Breakdown"
        if breakdown:
            return "Breakdown"
        if dist_ma <= 1.4 and mtf_confirmed:
            return "Pullback هبوطي"
        return "استمرار هبوطي"
    except Exception:
        return "استمرار هبوطي"

def classify_entry_timing_short(
    dist_ma: float, breakdown: bool, pre_breakdown: bool, vol_ratio: float, is_reverse: bool = False,
) -> str:
    try:
        if is_reverse:
            return "♻️ 15m Trigger بعد 4H/1H تأكيد"
        if dist_ma > 5.0:
            return "🔴 متأخر (قرب النهاية)"
        if (pre_breakdown or breakdown) and dist_ma <= 3.0 and vol_ratio >= 1.15:
            return "🟢 مبكر (بداية الحركة)"
        if breakdown and 3.0 < dist_ma <= 4.4 and vol_ratio >= 1.25:
            return "🟡 متوسط (نص الحركة)"
        if 3.0 < dist_ma <= 5.0 and vol_ratio >= 1.10:
            return "🟡 متوسط (نص الحركة)"
        return "🔴 متأخر (قرب النهاية)"
    except Exception:
        return "🟡 متوسط (نص الحركة)"

def get_entry_timing_penalty(entry_timing: str) -> float:
    try:
        if "♻️" in entry_timing:
            return 0.0
        if "🔴 متأخر" in entry_timing:
            return 0.25
        if "🟡 متوسط" in entry_timing:
            return 0.10
        return 0.0
    except Exception:
        return 0.0

def get_base_risk_label_short(score_result: dict, warnings_count: int) -> str:
    risk_level = score_result.get("risk_level")
    if risk_level:
        return risk_level
    if warnings_count == 0:
        return "🟢 منخفضة"
    if warnings_count == 1:
        return "🟡 متوسطة"
    return "🔴 عالية"

def adjust_risk_with_entry_timing_short(base_risk: str, entry_timing: str) -> str:
    try:
        if "♻️" in entry_timing:
            return base_risk
        if "🔴 متأخر" in entry_timing:
            return "🔴 عالية"
        if "🟡 متوسط" in entry_timing and base_risk == "🟢 منخفضة":
            return "🟡 متوسطة"
        return base_risk
    except Exception:
        return base_risk

def build_market_summary_short(btc_mode: str, alt_mode: str) -> str:
    safe_alt = alt_mode if alt_mode else "🟡 متماسك"
    safe_btc = btc_mode if btc_mode else "🟡 محايد"
    return f"{safe_alt} | BTC: {safe_btc}"

def get_reverse_banner_short(is_reverse: bool) -> str:
    if is_reverse:
        return "♻️ <b>OVEREXTENDED REVERSAL</b>"
    return ""

def get_reverse_style_note_short(is_reverse: bool) -> str:
    if is_reverse:
        return "⚠️ <b>تنبيه خاص:</b> 4H Overextended | 1H Weakening | 15m Trigger"
    return ""

def get_effective_min_score_with_reverse(base_min_score: float, is_reverse: bool) -> float:
    if is_reverse:
        return round(min(base_min_score, OVEREXTENDED_REVERSAL_MIN_SCORE), 2)
    return round(base_min_score, 2)

def calculate_sl_percent(entry, sl):
    try:
        return round(((float(sl) - float(entry)) / float(entry)) * 100, 2)
    except Exception:
        return 0.0

def get_dynamic_entry_threshold(
    market_state: str,
    score_result: dict,
    vol_ratio: float,
    mtf_confirmed: bool,
    is_new: bool,
    losing_strength: bool,
) -> float:
    if market_state == "risk_off":
        threshold = 6.0
    elif market_state == "btc_leading":
        threshold = 6.3
    elif market_state == "mixed":
        threshold = 6.2
    elif market_state == "bull_market":
        threshold = 6.7
    elif market_state == "alt_season":
        threshold = 6.6
    else:
        threshold = 6.3

    if mtf_confirmed:
        threshold -= 0.10

    if market_state in ("risk_off", "mixed"):
        if vol_ratio >= 1.8:
            threshold -= 0.15
        elif vol_ratio >= 1.35:
            threshold -= 0.08
    else:
        if vol_ratio >= 1.8:
            threshold += 0.05

    if is_new:
        threshold += 0.10
    if not losing_strength:
        threshold += 0.10
    if score_result.get("fake_signal"):
        threshold += 0.15

    threshold = max(5.8, min(6.9, threshold))
    return round(threshold, 2)


# ================================================
# REASON HANDLING (arabic labels)
# ================================================
def normalize_reason(reason: str) -> str:
    mapping = {
        "RSI ضعيف": "RSI ضعيف",
        "RSI هابط": "RSI هابط",
        "RSI هابط بقوة": "RSI هابط بقوة",
        "RSI منخفض": "RSI منخفض (تشبع بيعي)",
        "فوليوم بيعي": "فوليوم بيعي",
        "فوليوم قوي": "فوليوم قوي",
        "فوليوم انفجاري": "فوليوم انفجاري",
        "تحت MA": "تحت المتوسط",
        "شمعة بيعية جيدة": "شمعة بيعية جيدة",
        "شمعة بيعية قوية": "شمعة بيعية قوية",
        "كسر دعم": "كسر دعم",
        "كسر دعم مبكر": "كسر دعم مبكر",
        "كسر دعم قوي مؤكد": "كسر دعم قوي مؤكد",
        "زخم هابط مبكر": "زخم هابط مبكر",
        "زخم هابط مبكر 🎯": "زخم هابط مبكر 🎯",
        "تأكيد فريم الساعة": "تأكيد فريم الساعة",
        "BTC ضاغط": "BTC ضاغط",
        "BTC غير داعم للشورت": "BTC غير داعم للشورت",
        "هيمنة ضد الألت": "هيمنة ضد الألت (ضغط على العملات)",
        "تمويل إيجابي": "تمويل إيجابي (داعم للشورت)",
        "تمويل سلبي": "تمويل سلبي (خطر شورت سكويز)",
        "عملة جديدة": "عملة جديدة",
        "بعيد عن MA (هبوط متأخر)": "بعيد عن المتوسط (هبوط متأخر)",
        "ممتد هبوط": "ممتد هبوط",
        "فوق المتوسط": "فوق المتوسط",
        "رفض سعري سفلي": "رفض سعري سفلي",
        "أخبار اقتصادية مهمة قريبة": "أخبار اقتصادية مهمة قريبة",
        "♻️ 4H Overextended + 1H Weakening + 15m Trigger": "♻️ 4H Overextended + 1H Weakening + 15m Trigger",
        "Short Exhaustion Trap": "خطر الدخول في آخر الهبوط",
        "far_below_vwap": "بعيد تحت VWAP",
        "rsi_recovering": "RSI بدأ يتعافى ضد الشورت",
        "macd_hist_recovering": "زخم MACD الهبوطي يتراجع",
        "macd_hist_positive_against_short": "MACD إيجابي ضد الشورت",
        "Weak Historical Setup": "نوع إشارة ضعيف تاريخيًا",
        "Bull Run Guard": "السوق صاعد بقوة ضد الشورت",
        "far_below_ma": "بعيد تحت المتوسط",
        "rsi_oversold": "RSI منخفض / تشبع بيعي",
        "volume_spike": "فوليوم انفجاري بعد الهبوط",
        "big_red_candle": "شمعة هبوط كبيرة قد تكون متأخرة",
        "late_short_after_drop": "دخول متأخر بعد الهبوط",
        "pullback_short": "Pullback Short",
        "below_lower_band": "تحت الحد السفلي لبولينجر",
        "oversold_rsi": "تشبع بيعي RSI",
        "long_lower_wick": "فتيل سفلي طويل",
        "weak_adx_breakdown": "ADX ضعيف رغم الكسر",
        "minus_di_weak": "minus_di ضعيف",
        "high_volume_spike": "فوليوم عالي بعد الهبوط",
        "ADX strong": "ADX قوي داعم للشورت",
    }
    return mapping.get(reason, reason)


def sort_reasons(reasons):
    priority = {
        "♻️ 4H Overextended + 1H Weakening + 15m Trigger": 0,
        "تحت المتوسط": 1,
        "زخم هابط مبكر": 2,
        "زخم هابط مبكر 🎯": 3,
        "كسر دعم": 4,
        "كسر دعم مبكر": 5,
        "كسر دعم قوي مؤكد": 6,
        "فوليوم بيعي": 7,
        "فوليوم قوي": 8,
        "فوليوم انفجاري": 9,
        "شمعة بيعية جيدة": 10,
        "شمعة بيعية قوية": 11,
        "RSI ضعيف": 12,
        "RSI هابط": 13,
        "RSI هابط بقوة": 14,
        "تأكيد فريم الساعة": 15,
        "BTC ضاغط": 16,
        "هيمنة ضد الألت (ضغط على العملات)": 17,
        "تمويل إيجابي (داعم للشورت)": 18,
        "عملة جديدة": 19,
        "RSI منخفض (تشبع بيعي)": 101,
        "فوق المتوسط": 102,
        "بعيد عن المتوسط (هبوط متأخر)": 103,
        "ممتد هبوط": 104,
        "BTC غير داعم للشورت": 105,
        "تمويل سلبي (خطر شورت سكويز)": 106,
        "رفض سعري سفلي": 107,
        "أخبار اقتصادية مهمة قريبة": 108,
        "خطر الدخول في آخر الهبوط": 109,
        "بعيد تحت VWAP": 110,
        "RSI بدأ يتعافى ضد الشورت": 111,
        "زخم MACD الهبوطي يتراجع": 112,
        "MACD إيجابي ضد الشورت": 113,
        "نوع إشارة ضعيف تاريخيًا": 114,
        "السوق صاعد بقوة ضد الشورت": 115,
        "دخول متأخر بعد الهبوط": 116,
        "Pullback Short": 20,
        "تحت الحد السفلي لبولينجر": 117,
        "تشبع بيعي RSI": 118,
        "فتيل سفلي طويل": 119,
        "ADX ضعيف رغم الكسر": 120,
        "minus_di ضعيف": 121,
        "فوليوم عالي بعد الهبوط": 122,
        "ADX قوي داعم للشورت": 21,
    }
    return sorted(reasons, key=lambda x: priority.get(x, 999))


def classify_reasons(reasons):
    warning_keywords = [
        "RSI منخفض",
        "بعيد عن المتوسط",
        "ممتد",
        "فوق المتوسط",
        "BTC غير داعم",
        "تمويل سلبي",
        "رفض سعري",
        "أخبار اقتصادية",
        "خطر الدخول في آخر الهبوط",
        "بعيد تحت VWAP",
        "RSI بدأ يتعافى ضد الشورت",
        "زخم MACD الهبوطي يتراجع",
        "MACD إيجابي ضد الشورت",
        "نوع إشارة ضعيف تاريخيًا",
        "السوق صاعد بقوة ضد الشورت",
        "دخول متأخر بعد الهبوط",
        "تحت الحد السفلي لبولينجر",
        "تشبع بيعي RSI",
        "فتيل سفلي طويل",
        "ADX ضعيف رغم الكسر",
        "minus_di ضعيف",
        "فوليوم عالي بعد الهبوط",
    ]
    normalized = [normalize_reason(r) for r in reasons]
    bearish = []
    warnings = []
    for rr in normalized:
        if any(k in rr for k in warning_keywords):
            warnings.append(rr)
        else:
            bearish.append(rr)
    bearish = list(dict.fromkeys(bearish))
    warnings = list(dict.fromkeys(warnings))
    if "كسر دعم مبكر" in bearish and "كسر دعم" in bearish:
        bearish.remove("كسر دعم")
    if "كسر دعم قوي مؤكد" in bearish and "كسر دعم" in bearish:
        bearish.remove("كسر دعم")
    bearish = sort_reasons(bearish)
    warnings = sort_reasons(warnings)
    return bearish, warnings


def format_bearish_reasons(bearish):
    highlight_keywords = [
        "4H Overextended",
        "كسر دعم",
        "زخم هابط",
        "فوليوم",
        "شمعة",
        "RSI",
        "Pullback Short",
        "ADX قوي داعم للشورت"
    ]
    highlighted = []
    used = set()
    for kw in highlight_keywords:
        for rr in bearish:
            if kw in rr and rr not in used:
                highlighted.append(rr)
                used.add(rr)
                break
        if len(highlighted) >= 2:
            break
    formatted = []
    for rr in bearish:
        safe = html.escape(rr)
        line = f"• {safe}"
        if rr in highlighted:
            line = f"• <b>{safe}</b>"
        formatted.append(line)
    return "\n".join(formatted)


# ================================================
# SHORT EXHAUSTION TRAP
# ================================================
def is_short_exhaustion_trap(
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
    breakdown: bool,
    pre_breakdown: bool,
    breakdown_quality: str,
    is_reverse: bool,
) -> Dict[str, Any]:
    if is_reverse:
        return {"is_trap": False, "soft_trap": False, "reasons": [], "checks": 0}
    checks = 0
    reasons = []
    if dist_ma >= 3.8:
        checks += 1
        reasons.append("far_below_ma")
    if vwap_distance <= -2.0:
        checks += 1
        reasons.append("far_below_vwap")
    if rsi_now <= 36:
        checks += 1
        reasons.append("rsi_oversold")
    if rsi_slope >= 0:
        checks += 1
        reasons.append("rsi_recovering")
    if vol_ratio >= 1.8:
        checks += 1
        reasons.append("volume_spike")
    if candle_strength >= 0.60:
        checks += 1
        reasons.append("big_red_candle")
    if macd_hist_slope > 0:
        checks += 1
        reasons.append("macd_hist_recovering")
    if macd_hist > 0:
        checks += 1
        reasons.append("macd_hist_positive_against_short")
    is_trap = checks >= 5 and not pre_breakdown and breakdown_quality != "strong"
    soft_trap = not is_trap and checks >= 4
    return {"is_trap": is_trap, "soft_trap": soft_trap, "reasons": reasons, "checks": checks}


# ================================================
# BULL RUN GUARD (ADJUSTED)
# ================================================
def get_last_candle_change_pct_short(df) -> float:
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return 0.0
        open_ = float(signal_row.get("open", 0))
        close = float(signal_row.get("close", 0))
        if open_ <= 0:
            return 0.0
        return round(((close - open_) / open_) * 100, 4)
    except Exception:
        return 0.0


def get_bull_guard_snapshot(ranked_pairs, btc_mode: str, alt_snapshot: dict) -> dict:
    if not BULL_GUARD_ENABLED:
        return {"active": False, "block_shorts": False, "level": "normal",
                "valid_count": 0, "green_ratio_15m": 0.0, "avg_change_15m": 0.0,
                "btc_change_15m": 0.0, "reason": "disabled"}
    if not ranked_pairs:
        return {"active": False, "block_shorts": False, "level": "normal",
                "valid_count": 0, "green_ratio_15m": 0.0, "avg_change_15m": 0.0,
                "btc_change_15m": 0.0, "reason": "no ranked pairs"}
    try:
        sample = sorted(
            ranked_pairs,
            key=lambda x: x.get("_rank_volume_24h", 0),
            reverse=True
        )[:BULL_GUARD_SAMPLE_SIZE]
        changes = []
        green_count = 0
        valid = 0
        for item in sample:
            symbol = item.get("instId", "")
            if not symbol:
                continue
            candles = get_candles(symbol, BULL_GUARD_TIMEFRAME, BULL_GUARD_CANDLE_LIMIT)
            df = to_dataframe(candles)
            if df is None or df.empty:
                continue
            change = get_last_candle_change_pct_short(df)
            changes.append(change)
            valid += 1
            if change > 0:
                green_count += 1
        if valid < BULL_GUARD_MIN_VALID:
            return {"active": False, "block_shorts": False, "level": "normal",
                    "valid_count": valid, "green_ratio_15m": 0.0, "avg_change_15m": 0.0,
                    "btc_change_15m": 0.0, "reason": f"valid pairs too low ({valid})"}
        green_ratio = round(green_count / valid, 4)
        avg_change = round(sum(changes) / valid, 4)
        btc_change = 0.0
        try:
            btc_candles = get_candles("BTC-USDT-SWAP", BULL_GUARD_TIMEFRAME, BULL_GUARD_CANDLE_LIMIT)
            btc_df = to_dataframe(btc_candles)
            if btc_df is not None and not btc_df.empty:
                btc_change = get_last_candle_change_pct_short(btc_df)
        except Exception:
            pass
        alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك") if alt_snapshot else "🟡 متماسك"
        block = False
        reason_parts = []

        cond1 = green_ratio >= BULL_GUARD_GREEN_RATIO_BLOCK and avg_change >= BULL_GUARD_AVG_CHANGE_15M_BLOCK
        cond2 = btc_change >= BULL_GUARD_BTC_CHANGE_15M_BLOCK and green_ratio >= 0.62 and avg_change >= 0.35
        cond3 = (BULL_GUARD_ALT_STRONG_BLOCK and alt_mode == "🟢 قوي"
                 and green_ratio >= 0.68 and avg_change >= 0.45)

        if cond1:
            block = True
            reason_parts.append(f"cond1: green_ratio={green_ratio:.2f} & avg_change={avg_change:.2f}")
        elif cond2:
            block = True
            reason_parts.append(f"cond2: btc_change={btc_change:.2f} & green_ratio={green_ratio:.2f} & avg_change={avg_change:.2f}")
        elif cond3:
            block = True
            reason_parts.append(f"cond3: alt_mode={alt_mode} & green_ratio={green_ratio:.2f} & avg_change={avg_change:.2f}")
        else:
            reason_parts.append("no block condition met")

        reason = " | ".join(reason_parts)
        return {
            "active": True,
            "block_shorts": block,
            "level": "danger" if block else "normal",
            "valid_count": valid,
            "green_ratio_15m": green_ratio,
            "avg_change_15m": avg_change,
            "btc_change_15m": btc_change,
            "reason": reason,
        }
    except Exception as e:
        logger.error(f"Bull guard snapshot error: {e}")
        return {"active": False, "block_shorts": False, "level": "normal",
                "valid_count": 0, "green_ratio_15m": 0.0, "avg_change_15m": 0.0,
                "btc_change_15m": 0.0, "reason": f"error: {e}"}


# ================================================
# MARKET MODE TRANSITIONS (with stricter NORMAL->STRONG)
# ================================================
def normalize_short_market_mode(mode: str) -> str:
    allowed = {MODE_NORMAL_SHORT, MODE_STRONG_SHORT_ONLY, MODE_BLOCK_SHORTS}
    if mode in allowed:
        return mode
    return MODE_NORMAL_SHORT


def determine_short_market_mode(bull_guard: dict, current_mode: str, market_state: str, btc_mode: str, alt_snapshot: dict) -> dict:
    now_ts = int(time.time())
    current_mode = normalize_short_market_mode(current_mode)
    green_ratio = float(bull_guard.get("green_ratio_15m", 0) or 0)
    avg_change = float(bull_guard.get("avg_change_15m", 0) or 0)
    btc_change = float(bull_guard.get("btc_change_15m", 0) or 0)
    alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك") if alt_snapshot else "🟡 متماسك"
    last_transition_ts = 0
    normal_candidate_since = 0
    safe_since = 0
    if r:
        try:
            last_transition_ts = int(r.get(MARKET_MODE_LAST_TRANSITION_KEY) or 0)
            normal_candidate_since = int(r.get(MARKET_MODE_NORMAL_CANDIDATE_KEY) or 0)
            safe_since = int(r.get(MARKET_MODE_LAST_SAFE_SEEN_KEY) or 0)
        except Exception:
            pass
    time_since_last_transition = now_ts - last_transition_ts if last_transition_ts > 0 else 999999
    bull_block = bool(bull_guard.get("block_shorts"))
    if bull_block:
        if r:
            try:
                r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
                r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
            except Exception:
                pass
        return {"mode": MODE_BLOCK_SHORTS, "reason": bull_guard.get("reason", "bull guard block")}

    if current_mode == MODE_BLOCK_SHORTS:
        no_longer_danger = green_ratio < 0.62 and avg_change < 0.75 and btc_change < 0.45
        if no_longer_danger:
            if safe_since == 0:
                if r:
                    try:
                        r.set(MARKET_MODE_LAST_SAFE_SEEN_KEY, str(now_ts))
                    except Exception:
                        pass
                return {"mode": MODE_BLOCK_SHORTS, "reason": "safe timer started"}
            safe_duration = now_ts - safe_since
            if safe_duration >= BLOCK_EXIT_CONFIRM_DURATION:
                if r:
                    try:
                        r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
                    except Exception:
                        pass
                return {"mode": MODE_STRONG_SHORT_ONLY, "reason": "exiting block after safe interval"}
            return {"mode": MODE_BLOCK_SHORTS, "reason": f"confirming safe exit {safe_duration}/{BLOCK_EXIT_CONFIRM_DURATION}s"}
        else:
            if r:
                try:
                    r.delete(MARKET_MODE_LAST_SAFE_SEEN_KEY)
                except Exception:
                    pass
            return {"mode": MODE_BLOCK_SHORTS, "reason": "still in bull run"}

    if current_mode == MODE_STRONG_SHORT_ONLY:
        normal_ready = green_ratio < 0.54 and avg_change < 0.45 and btc_change < 0.25
        if normal_ready:
            if normal_candidate_since == 0:
                if r:
                    try:
                        r.set(MARKET_MODE_NORMAL_CANDIDATE_KEY, str(now_ts))
                    except Exception:
                        pass
                return {"mode": MODE_STRONG_SHORT_ONLY, "reason": "normal candidate started"}
            if now_ts - normal_candidate_since >= STRONG_TO_NORMAL_CONFIRM_DURATION:
                if time_since_last_transition < MODE_TRANSITION_MIN_INTERVAL:
                    return {"mode": MODE_STRONG_SHORT_ONLY, "reason": "min transition interval not met"}
                if r:
                    try:
                        r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
                    except Exception:
                        pass
                return {"mode": MODE_NORMAL_SHORT, "reason": "returning to normal"}
            return {"mode": MODE_STRONG_SHORT_ONLY, "reason": "confirming normal stability"}
        else:
            if r:
                try:
                    r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
                except Exception:
                    pass
            return {"mode": MODE_STRONG_SHORT_ONLY, "reason": "conditions not met for normal"}

    weak_market = (
        market_state in ("mixed", "btc_leading")
        or (0.58 <= green_ratio < 0.72)
        or (avg_change > 0.50)
        or (btc_mode in ("🟢 صاعد", "🟡 محايد") and alt_mode == "🟢 قوي")
    )
    if weak_market and time_since_last_transition >= MODE_TRANSITION_MIN_INTERVAL:
        if r:
            try:
                r.delete(MARKET_MODE_NORMAL_CANDIDATE_KEY)
            except Exception:
                pass
        return {"mode": MODE_STRONG_SHORT_ONLY, "reason": "weak/choppy market, tightening"}
    return {"mode": MODE_NORMAL_SHORT, "reason": "stable/normal"}


def handle_market_mode_transition(mode_result: dict) -> str:
    new_mode = normalize_short_market_mode(mode_result.get("mode", MODE_NORMAL_SHORT))
    reason = mode_result.get("reason", "")
    last_mode = r.get(MARKET_MODE_LAST_KEY) if r else MODE_NORMAL_SHORT
    last_mode = normalize_short_market_mode(last_mode)
    now_ts = int(time.time())

    if last_mode == new_mode:
        if r:
            r.set(MARKET_MODE_KEY, new_mode)
        return new_mode

    last_ts_str = r.get(MARKET_MODE_LAST_TRANSITION_KEY) if r else None
    if last_ts_str:
        last_ts = int(last_ts_str)
        if now_ts - last_ts < MODE_TRANSITION_MIN_INTERVAL and new_mode != MODE_BLOCK_SHORTS:
            logger.info(f"Suppressing mode change {last_mode} -> {new_mode} (interval too short)")
            if r:
                r.set(MARKET_MODE_KEY, last_mode)
            return last_mode

    if r:
        r.set(MARKET_MODE_KEY, new_mode)
        r.set(MARKET_MODE_LAST_KEY, new_mode)
        r.set(MARKET_MODE_LAST_TRANSITION_KEY, str(now_ts))

    send_telegram_message(format_mode_transition_message(last_mode, new_mode, reason))
    logger.info(f"Market mode changed: {last_mode} → {new_mode}, reason={reason}")
    return new_mode


def format_mode_transition_message(old_mode: str, new_mode: str, reason: str = "") -> str:
    if new_mode == MODE_NORMAL_SHORT:
        lines = [
            "🟢 <b>Mode Changed: NORMAL SHORT</b>",
            "السوق رجع طبيعي نسبيًا، البوت رجع يسمح بإشارات الشورت العادية.",
        ]
    elif new_mode == MODE_STRONG_SHORT_ONLY:
        lines = [
            "🟡 <b>Mode Changed: STRONG SHORT ONLY</b>",
            "السوق غير مثالي للشورت، سيتم السماح فقط بالإشارات القوية (Breakdown, Pre-Breakdown, Early Strong, Reversal).",
        ]
    elif new_mode == MODE_BLOCK_SHORTS:
        lines = [
            "🚨 <b>Mode Changed: BLOCK SHORTS</b>",
            "تم رصد Bull Run / صعود جماعي، تم إيقاف إشارات الشورت الجديدة مؤقتًا.",
            "الأوامر والتقارير و Track شغالة عادي.",
        ]
    else:
        lines = [f"Mode: {new_mode}"]
    if reason:
        lines.append(f"السبب: {reason}")
    return "\n".join(lines)


# ================================================
# BUILD MESSAGE (FINAL)
# ================================================
def build_message_new(
    symbol, price, score_result, stop_loss, tp1, tp2, rr1, rr2, btc_mode, btc_short_bias, tv_link, is_new,
    change_24h=0.0,
    market_state_label=None,
    market_bias_label=None,
    alt_mode=None,
    news_warning="",
    opportunity_type="استمرار هبوطي",
    entry_timing="🟡 متوسط",
    display_risk="🟡 متوسطة",
    setup_stats=None,
    is_reverse=False,
    breakdown_quality="none",
    bull_guard_active=False,
    bull_guard_level="",
    pullback_short=False,
    bb_position="inside",
    lower_wick_ratio=0.0,
    stoch_rsi_k=50.0,
    rsi_now=50.0,
):
    symbol_clean = clean_symbol_for_message(symbol)
    bearish, inferred_warnings = classify_reasons(score_result.get("reasons", []))
    explicit_warnings = [normalize_reason(w) for w in (score_result.get("warning_reasons") or [])]
    warnings = explicit_warnings if explicit_warnings else inferred_warnings
    warnings = list(dict.fromkeys(warnings))
    warnings = sort_reasons(warnings)
    if is_reverse:
        reverse_reason = "♻️ 4H Overextended + 1H Weakening + 15m Trigger"
        if reverse_reason not in bearish:
            bearish = [reverse_reason] + bearish
    if pullback_short and "Pullback Short" not in bearish:
        bearish.insert(0, "Pullback Short")
    bearish_text = format_bearish_reasons(bearish) if bearish else "• زخم هابط"
    warnings_text = "\n".join(f"• {html.escape(w)}" for w in warnings) if warnings else ""
    sl_pct = calculate_sl_percent(price, stop_loss)
    tp1_pct = round(((price - tp1) / price) * 100, 2) if price else 0.0
    tp2_pct = round(((price - tp2) / price) * 100, 2) if price else 0.0
    new_tag = "\n🆕 <b>عملة جديدة</b>" if is_new else ""
    hybrid_label = get_hybrid_label_from_stats(setup_stats or {})
    reverse_banner = get_reverse_banner_short(is_reverse)
    reverse_note = get_reverse_style_note_short(is_reverse)
    bq_map = {"strong": "🟢 كسر قوي", "ok": "🟡 كسر مقبول", "weak": "🔴 كسر ضعيف"}
    bq_label = bq_map.get(breakdown_quality, "غير مؤكد")
    safe_market = html.escape(build_market_summary_short(btc_mode=btc_mode, alt_mode=alt_mode or "🟡 متماسك"))
    safe_tv_link = html.escape(tv_link, quote=True)
    warnings_block = f"\n\n⚠️ <b>ملاحظات:</b>\n{warnings_text}" if warnings_text else ""
    news_block = f"\n\n{news_warning}" if news_warning else ""
    funding_text = score_result.get("funding_label", "🟡 محايد")
    signal_rating = score_result.get("signal_rating", "⚡ عادي")
    header_block = hybrid_label + "\n\n" if hybrid_label else ""
    if reverse_banner:
        header_block += reverse_banner + "\n\n"
    # Extra short note
    extra_note = ""
    if bb_position == "below_lower":
        extra_note += "\n⚠️ السعر تحت Bollinger Lower"
    if lower_wick_ratio >= 0.32:
        extra_note += "\n⚠️ فتيل سفلي طويل"
    if rsi_now <= 35 and stoch_rsi_k <= 15:
        extra_note += "\n⚠️ RSI / Stoch تشبع بيعي"
    return f"""{header_block}🔴 <b>شورت فيوتشر | {html.escape(symbol_clean)}</b>

💰 السعر: {fmt_price(price)} | ⏱ الفريم: 15m
⭐ السكور: {score_result.get("score", 0):.1f} / 10
🏷 التصنيف: {html.escape(signal_rating)}

🎯 TP1: {fmt_price(tp1)} (-{tp1_pct:.2f}% | {rr1}R)
🏁 TP2: {fmt_price(tp2)} (-{tp2_pct:.2f}% | {rr2}R)
🛑 SL: {fmt_price(stop_loss)} (+{sl_pct:.2f}%)

🧠 نوع الفرصة: {html.escape(opportunity_type)}{reverse_note}
🧩 جودة الكسر: {bq_label}

🌍 السوق: {safe_market}
💸 التمويل: {html.escape(funding_text)}{new_tag}{extra_note}

📊 <b>أسباب الدخول:</b>
{bearish_text}{warnings_block}{news_block}

📍 الدخول: {html.escape(entry_timing)}
⚖️ المخاطرة: {html.escape(display_risk)}

🔗 <a href="{safe_tv_link}">Open Chart (15m / 1H)</a>"""


# ================================================
# CANDIDATE SELECTION FUNCTIONS
# ================================================
def get_effective_min_score(is_new: bool, is_reverse: bool = False) -> float:
    if is_reverse:
        return OVEREXTENDED_REVERSAL_MIN_SCORE
    return TOP_MOMENTUM_NEW_MIN_SCORE if is_new else TOP_MOMENTUM_MIN_SCORE

def get_momentum_priority(
    score: float,
    breakdown: bool,
    vol_ratio: float,
    is_new: bool,
    pre_breakdown: bool = False,
    dist_ma: float = 0.0,
    losing_strength: bool = False,
    early_priority: str = "none",
    is_reverse: bool = False,
    soft_trap: bool = False,
    pullback_short: bool = False,
) -> float:
    priority = float(score)
    if breakdown:
        priority += 0.9
    elif pre_breakdown:
        priority += 0.6
    elif pullback_short:
        priority += 0.5
    if vol_ratio >= 1.8:
        priority += 0.8
    elif vol_ratio >= 1.35:
        priority += 0.4
    if is_new and vol_ratio >= NEW_LISTING_MIN_VOL_RATIO:
        priority += 0.4
    if not is_reverse:
        if dist_ma > 5.2:
            priority -= 0.7
        elif dist_ma > 4.2:
            priority -= 0.25
    else:
        priority += 0.45
    if losing_strength:
        priority += 0.20
    priority += get_early_priority_momentum_bonus(early_priority)
    if soft_trap:
        priority -= 0.30
    return round(priority, 2)

def get_candidate_bucket(candidate: dict) -> str:
    if candidate.get("is_reverse"):
        return "reverse"
    if candidate["is_new"] and candidate["breakdown"]:
        return "new_breakdown"
    if candidate.get("pre_breakdown") and not candidate["breakdown"]:
        return "pre_breakdown"
    if candidate["breakdown"]:
        return "breakdown"
    if candidate.get("early_priority") == "strong":
        return "early_strong"
    if candidate.get("pullback_short"):
        return "pullback_short"
    if candidate.get("vol_ratio") >= 1.8:
        return "volume"
    return "standard"

def apply_top_momentum_filter(candidates):
    if not candidates:
        return []
    strong_candidates = []
    for c in candidates:
        min_score = get_effective_min_score(
            is_new=c.get("is_new", False),
            is_reverse=c.get("is_reverse", False),
        )
        if c["score"] >= min_score:
            strong_candidates.append(c)
    if not strong_candidates:
        logger.info("Top momentum filter: no candidates above threshold")
        return []
    strong_candidates.sort(
        key=lambda x: (x["momentum_priority"], x["score"], x["change_24h"], x["rank_volume_24h"]),
        reverse=True,
    )
    top_n = max(4, int(len(strong_candidates) * TOP_MOMENTUM_PERCENT))
    filtered = strong_candidates[:top_n]
    final_candidates = []
    new_count = 0
    for c in filtered:
        if c["is_new"]:
            if new_count >= NEW_LISTING_MAX_PER_RUN:
                continue
            new_count += 1
        final_candidates.append(c)
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
            key=lambda x: (x["momentum_priority"], x["score"], x["change_24h"], x["rank_volume_24h"]),
            reverse=True,
        )
    diversified = []
    used_patterns = set()
    for bucket_name in [
        "reverse", "new_breakdown", "pre_breakdown", "breakdown",
        "early_strong", "pullback_short", "volume", "standard"
    ]:
        if bucket_name not in buckets or not buckets[bucket_name]:
            continue
        candidate = buckets[bucket_name][0]
        pattern = (
            candidate["breakdown"],
            candidate.get("pre_breakdown", False),
            round(candidate["vol_ratio"], 1),
            candidate["is_new"],
            candidate.get("early_priority", "none"),
            candidate.get("is_reverse", False),
            candidate.get("pullback_short", False),
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
            key=lambda x: (x["momentum_priority"], x["score"], x["change_24h"], x["rank_volume_24h"]),
            reverse=True,
        )
        for candidate in remaining:
            if len(diversified) >= max_alerts:
                break
            pattern = (
                candidate["breakdown"],
                candidate.get("pre_breakdown", False),
                round(candidate["vol_ratio"], 1),
                candidate["is_new"],
                candidate.get("early_priority", "none"),
                candidate.get("is_reverse", False),
                candidate.get("pullback_short", False),
            )
            if pattern in used_patterns or candidate in diversified:
                continue
            diversified.append(candidate)
            used_patterns.add(pattern)
    return diversified[:max_alerts]


def passes_new_listing_filter(score: float, breakdown: bool, vol_ratio: float, candle_strength: float) -> bool:
    checks = 0
    if breakdown:
        checks += 1
    if vol_ratio >= NEW_LISTING_MIN_VOL_RATIO:
        checks += 1
    if candle_strength >= NEW_LISTING_MIN_CANDLE_STRENGTH:
        checks += 1
    if score >= TOP_MOMENTUM_NEW_MIN_SCORE:
        checks += 1
    return checks >= 3


# ================================================
# MAIN SCANNER LOOP (ENHANCED)
# ================================================
def run_scanner_loop():
    global last_global_send_ts
    while True:
        scan_locked = False
        try:
            scan_locked = acquire_scan_lock()
            if not scan_locked:
                logger.info("Another short scan is running — skipping")
                time.sleep(30)
                continue
            logger.info("SHORT RUN START")

            # 1. update open trades
            update_open_trades(r, market_type="futures", side="short", timeframe=TIMEFRAME)

            # 2. winrate summary (logging only)
            winrate_summary = get_winrate_summary(r, market_type="futures", side="short")
            logger.info(format_winrate_summary(winrate_summary))

            # 3. stats reset ts
            stats_reset_ts = None
            if r:
                try:
                    raw_reset = r.get(STATS_RESET_TS_KEY)
                    if raw_reset:
                        stats_reset_ts = int(raw_reset)
                except Exception:
                    pass

            # 4. ranked pairs
            ranked_pairs = get_ranked_pairs()

            # 5. btc mode
            btc_mode = get_btc_mode()

            # 6. alt snapshot
            alt_snapshot = None
            if r:
                try:
                    cached = r.get(ALT_SNAPSHOT_CACHE_KEY)
                    if cached:
                        alt_snapshot = json.loads(cached)
                except Exception:
                    pass
            if alt_snapshot is None:
                alt_snapshot = get_alt_market_snapshot(ranked_pairs)
                if r:
                    try:
                        r.set(ALT_SNAPSHOT_CACHE_KEY, safe_json_dumps(alt_snapshot), ex=ALT_SNAPSHOT_CACHE_TTL)
                    except Exception:
                        pass

            # 7. market info
            market_info = get_market_state(btc_mode, alt_snapshot)
            market_state = market_info["market_state"]
            market_state_label = market_info["market_state_label"]
            market_bias_label = market_info["market_bias_label"]
            btc_short_bias = market_info["btc_short_bias"]
            alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك")

            # 8. bull guard
            bull_guard = get_bull_guard_snapshot(ranked_pairs, btc_mode, alt_snapshot)

            # 9. current mode
            current_mode = r.get(MARKET_MODE_KEY) if r else MODE_NORMAL_SHORT
            if not current_mode:
                current_mode = MODE_NORMAL_SHORT

            # 10. determine mode
            mode_result = determine_short_market_mode(bull_guard, current_mode, market_state, btc_mode, alt_snapshot)

            # 11. handle transition
            current_mode = handle_market_mode_transition(mode_result)

            logger.info(
                f"MARKET MODE: {current_mode} | block={bull_guard.get('block_shorts')} | "
                f"level={bull_guard.get('level')} | green_ratio={bull_guard.get('green_ratio_15m')} | "
                f"avg_change={bull_guard.get('avg_change_15m')} | btc_change={bull_guard.get('btc_change_15m')} | "
                f"valid={bull_guard.get('valid_count')} | bull_reason={bull_guard.get('reason')} | "
                f"mode_reason={mode_result.get('reason')}"
            )

            # 12. BLOCK SHORTS
            if current_mode == MODE_BLOCK_SHORTS:
                logger.warning("BLOCK SHORTS active – no signals, sleeping 60s")
                time.sleep(60)
                continue

            # 13. global cooldown check AFTER market mode
            if is_global_cooldown_active() and current_mode in (MODE_NORMAL_SHORT, MODE_STRONG_SHORT_ONLY):
                logger.info("Global cooldown active – skipping signal sending")
                time.sleep(60)
                continue

            upcoming_events = get_upcoming_high_impact_events()
            has_high_impact_news = len(upcoming_events) > 0
            news_warning_text = format_news_warning(upcoming_events)

            tested = 0
            candidates = []

            for pair_data in ranked_pairs:
                tested += 1
                symbol = pair_data["instId"]
                change_24h = extract_24h_change_percent(pair_data)
                candles = get_candles(symbol, TIMEFRAME, 100)
                df = to_dataframe(candles)
                if df is None or df.empty:
                    continue
                if not is_valid_candle_timing(df):
                    continue

                signal_row = get_signal_row(df)
                if signal_row is None:
                    continue

                early_signal = early_bearish_signal(df)
                pre_breakdown = is_pre_breakdown(df)
                breakdown = is_breakdown(df)
                mtf_confirmed = is_higher_timeframe_confirmed(symbol)
                is_new = is_new_listing_by_candles(candles)
                funding = get_funding_rate(symbol)
                vol_ratio = get_volume_ratio(df)
                dist_ma = get_distance_from_ma_percent(df)
                candle_strength = get_candle_strength_ratio(df)
                losing_strength = is_losing_intraday_strength(df)
                vwap_distance = get_vwap_distance_percent(df)
                rsi_now = float(signal_row.get("rsi", 50))
                rsi_slope = get_rsi_slope(df)
                macd_hist = float(signal_row.get("macd_hist", 0))
                macd_hist_slope = get_macd_hist_slope(df)
                breakdown_quality = get_breakdown_quality(df, vol_ratio)

                # New indicators
                candle_anatomy = get_candle_anatomy(df)
                body_ratio = candle_anatomy["body_ratio"]
                upper_wick_ratio = candle_anatomy["upper_wick_ratio"]
                lower_wick_ratio = candle_anatomy["lower_wick_ratio"]
                close_position = candle_anatomy["close_position"]
                bb_position = get_bb_position(df)
                adx = float(signal_row.get("adx", 0))
                plus_di = float(signal_row.get("plus_di", 0))
                minus_di = float(signal_row.get("minus_di", 0))
                stoch_rsi_k = float(signal_row.get("stoch_rsi_k", 50))
                stoch_rsi_d = float(signal_row.get("stoch_rsi_d", 50))

                is_reverse = is_overextended_reversal_short(
                    symbol=symbol, df=df, change_24h=change_24h, vol_ratio=vol_ratio,
                    early_signal=early_signal, breakdown=breakdown, pre_breakdown=pre_breakdown,
                )

                pullback_short = is_pullback_short(df, vol_ratio, mtf_confirmed)

                opportunity_type = classify_opportunity_type_short(
                    breakdown=breakdown, pre_breakdown=pre_breakdown,
                    dist_ma=dist_ma, mtf_confirmed=mtf_confirmed,
                    is_reverse=is_reverse, pullback_short=pullback_short,
                )

                early_priority = classify_early_priority_short(
                    early_signal=early_signal,
                    breakdown=breakdown,
                    pre_breakdown=pre_breakdown,
                    dist_ma=dist_ma,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                    mtf_confirmed=mtf_confirmed,
                    losing_strength=losing_strength,
                    market_state=market_state,
                )

                # NORMAL_SHORT gate: only allow strong setups
                if current_mode == MODE_NORMAL_SHORT:
                    if not (breakdown or pre_breakdown or pullback_short or early_priority == "strong" or is_reverse):
                        logger.info(f"{symbol} → normal mode filtered: no strong setup")
                        continue

                # STRONG_SHORT_ONLY gate (updated with pullback exception)
                if current_mode == MODE_STRONG_SHORT_ONLY:
                    if not (is_reverse or breakdown_quality == "strong" or pre_breakdown or pullback_short):
                        continue
                    if adx < 20 and not is_reverse:
                        logger.info(f"{symbol} → strong mode: weak ADX")
                        continue
                    if minus_di <= plus_di and not is_reverse:
                        logger.info(f"{symbol} → strong mode: minus_di <= plus_di")
                        continue
                    if breakdown_quality == "weak" and not is_reverse:
                        continue
                    if not mtf_confirmed and not is_reverse:
                        # Only allow pullback_short with very strong conditions if MTF not confirmed
                        if not (pullback_short and adx >= 24 and minus_di > plus_di and vol_ratio >= 1.35):
                            continue
                    if vol_ratio < 1.25 and not pullback_short:
                        continue

                # Late short after drop filter
                late_filter = is_late_short_after_drop(
                    dist_ma=dist_ma, rsi_now=rsi_now, rsi_slope=rsi_slope,
                    vol_ratio=vol_ratio, candle_strength=candle_strength,
                    lower_wick_ratio=lower_wick_ratio, bb_position=bb_position,
                    is_reverse=is_reverse, breakdown_quality=breakdown_quality,
                    pre_breakdown=pre_breakdown,
                )
                if late_filter["reject"]:
                    logger.info(f"{symbol} → late short after drop rejected")
                    continue

                # Exhaustion trap (remains)
                trap = is_short_exhaustion_trap(
                    market_state=market_state, opportunity_type=opportunity_type,
                    dist_ma=dist_ma, vwap_distance=vwap_distance, rsi_now=rsi_now,
                    rsi_slope=rsi_slope, vol_ratio=vol_ratio, candle_strength=candle_strength,
                    macd_hist=macd_hist, macd_hist_slope=macd_hist_slope,
                    breakdown=breakdown, pre_breakdown=pre_breakdown,
                    breakdown_quality=breakdown_quality, is_reverse=is_reverse,
                )
                if trap["is_trap"]:
                    logger.info(f"{symbol} → short exhaustion trap rejected")
                    continue

                # VWAP/MACD/RSI slope extra filters (remains)
                if not is_reverse and vwap_distance <= -3.0 and not pre_breakdown:
                    if breakdown_quality != "strong":
                        continue
                if not is_reverse:
                    if rsi_now <= 34 and rsi_slope >= 0 and dist_ma >= 3.4:
                        if breakdown_quality != "strong":
                            continue
                    if macd_hist_slope > 0 and rsi_now <= 38 and dist_ma >= 3.4:
                        if breakdown_quality != "strong" and not pre_breakdown:
                            continue
                    if macd_hist > 0 and not breakdown and not pre_breakdown:
                        continue

                # volume floor
                if vol_ratio < 1.08 and not breakdown and not pre_breakdown and not early_signal and not is_reverse:
                    continue
                if dist_ma > 5.0 and not breakdown and not pre_breakdown and not is_reverse:
                    continue

                try:
                    score_result = calculate_short_score(
                        df=df, vol_ratio=vol_ratio, mtf_confirmed=mtf_confirmed,
                        btc_mode=btc_mode, breakdown=breakdown, pre_breakdown=pre_breakdown,
                        is_new=is_new, funding=funding, btc_short_bias_proxy=btc_short_bias,
                        market_state=market_state, alt_mode=alt_mode,
                        market_bias_label=market_bias_label,
                    )
                except Exception:
                    continue

                raw_score = float(score_result.get("score", 0))
                effective_score = raw_score

                # New score adjustments
                if adx >= 22 and minus_di > plus_di:
                    effective_score += 0.30
                    score_result.setdefault("reasons", []).append("ADX strong")
                if adx < 18 and breakdown:
                    effective_score -= 0.40
                    score_result.setdefault("warning_reasons", []).append("weak_adx_breakdown")
                if stoch_rsi_k <= 15 and rsi_now <= 35 and dist_ma >= 3.2:
                    effective_score -= 0.50
                    score_result.setdefault("warning_reasons", []).append("oversold_rsi")
                if lower_wick_ratio >= 0.32:
                    effective_score -= 0.50
                    score_result.setdefault("warning_reasons", []).append("long_lower_wick")
                if pullback_short:
                    effective_score += 0.35
                    score_result.setdefault("reasons", []).append("pullback_short")
                if close_position is not None and close_position <= 0.35:
                    effective_score -= 0.15
                if bb_position == "below_lower" and rsi_now <= 35:
                    effective_score -= 0.40
                    score_result.setdefault("warning_reasons", []).append("below_lower_band")
                if bb_position == "below_mid" and adx >= 20:
                    effective_score += 0.20

                if score_result.get("fake_signal"):
                    effective_score -= 0.30
                if trap["soft_trap"]:
                    effective_score -= 0.30
                    score_result.setdefault("warning_reasons", []).append("Short Exhaustion Trap")
                if has_high_impact_news:
                    score_result.setdefault("warning_reasons", []).append("أخبار اقتصادية مهمة قريبة")
                effective_score += get_early_priority_score_bonus(early_priority)
                if is_reverse:
                    effective_score += OVEREXTENDED_REVERSAL_SCORE_BONUS
                score_result["score"] = round(effective_score, 2)

                dynamic_threshold = get_dynamic_entry_threshold(
                    market_state=market_state,
                    score_result=score_result,
                    vol_ratio=vol_ratio,
                    mtf_confirmed=mtf_confirmed,
                    is_new=is_new,
                    losing_strength=losing_strength,
                )
                dynamic_threshold += get_early_priority_threshold_adjustment(early_priority)
                if is_reverse:
                    dynamic_threshold = min(dynamic_threshold, OVEREXTENDED_REVERSAL_MIN_SCORE + 0.20)
                dynamic_threshold = round(dynamic_threshold, 2)

                if score_result["score"] < dynamic_threshold and early_priority != "strong" and not is_reverse:
                    logger.info(f"{symbol} → rejected by dynamic threshold {score_result['score']:.2f} < {dynamic_threshold}")
                    continue

                pre_breakdown_only = pre_breakdown and not breakdown and not early_signal
                required_min_score = FINAL_MIN_SCORE + PRE_BREAKDOWN_EXTRA_SCORE if pre_breakdown_only else FINAL_MIN_SCORE
                entry_timing = classify_entry_timing_short(
                    dist_ma=dist_ma, breakdown=breakdown, pre_breakdown=pre_breakdown,
                    vol_ratio=vol_ratio, is_reverse=is_reverse,
                )
                timing_penalty = get_entry_timing_penalty(entry_timing)
                effective_required_min_score = required_min_score + timing_penalty
                effective_required_min_score += get_early_priority_min_score_adjustment(early_priority)
                effective_required_min_score = get_effective_min_score_with_reverse(
                    effective_required_min_score,
                    is_reverse=is_reverse,
                )
                if score_result["score"] < effective_required_min_score:
                    logger.info(f"{symbol} → rejected by effective min score {score_result['score']} < {effective_required_min_score}")
                    continue

                if is_new and not passes_new_listing_filter(
                    score=float(score_result["score"]),
                    breakdown=breakdown,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                ):
                    logger.info(f"{symbol} → rejected by new listing filter")
                    continue

                setup_type = build_setup_type({
                    "is_reverse": is_reverse,
                    "breakdown": breakdown,
                    "pre_breakdown": pre_breakdown,
                    "pullback_short": pullback_short,
                    "mtf_confirmed": mtf_confirmed,
                    "vol_ratio": vol_ratio,
                    "market_state": market_state,
                })
                setup_stats = safe_get_setup_type_stats(
                    redis_client=r,
                    market_type="futures",
                    side="short",
                    setup_type=setup_type,
                    since_ts=stats_reset_ts,
                )

                if setup_type in WEAK_SETUP_TYPES and not is_reverse and breakdown_quality != "strong":
                    effective_score -= 0.60
                    score_result["score"] = round(effective_score, 2)
                    score_result.setdefault("warning_reasons", []).append("Weak Historical Setup")
                    if effective_score < 7.2:
                        logger.info(f"{symbol} → rejected by weak historical setup score")
                        continue

                signal_row_final = get_signal_row(df)
                if signal_row_final is None:
                    continue
                price = float(signal_row_final["close"])
                sl_type = "reverse" if is_reverse else (
                    "breakdown" if breakdown else (
                        "pre_breakdown" if pre_breakdown else (
                            "new_listing" if is_new else (
                                "pullback_short" if pullback_short else "standard"
                            )
                        )
                    )
                )
                stop_loss = calculate_stop_loss_short(df, price, signal_type=sl_type)
                rr1, rr2 = get_rr_targets(signal_type=sl_type)
                tp1 = calc_tp_short(price, stop_loss, rr=rr1)
                tp2 = calc_tp_short(price, stop_loss, rr=rr2)

                # Validate levels before building candidate
                if not (stop_loss > price and tp1 < price and tp2 < tp1):
                    logger.warning(
                        f"{symbol} → invalid short levels entry={price} sl={stop_loss} tp1={tp1} tp2={tp2}"
                    )
                    continue

                tv_link = build_tradingview_link(symbol)

                display_risk = adjust_risk_with_entry_timing_short(
                    get_base_risk_label_short(score_result, len(score_result.get("warning_reasons", []))),
                    entry_timing,
                )

                alert_id = build_alert_id(symbol, get_signal_candle_time(df))
                momentum_priority = get_momentum_priority(
                    score=float(score_result["score"]),
                    breakdown=breakdown,
                    vol_ratio=vol_ratio,
                    is_new=is_new,
                    pre_breakdown=pre_breakdown,
                    dist_ma=dist_ma,
                    losing_strength=losing_strength,
                    early_priority=early_priority,
                    is_reverse=is_reverse,
                    soft_trap=trap["soft_trap"],
                    pullback_short=pullback_short,
                )

                # Build candidate data, do not build message yet
                candidate = {
                    "symbol": symbol,
                    "score": float(score_result["score"]),
                    "momentum_priority": momentum_priority,
                    "breakdown": breakdown,
                    "pre_breakdown": pre_breakdown,
                    "vol_ratio": vol_ratio,
                    "candle_strength": candle_strength,
                    "is_new": is_new,
                    "rank_volume_24h": float(pair_data.get("_rank_volume_24h", 0)),
                    "early_priority": early_priority,
                    "is_reverse": is_reverse,
                    "pullback_short": pullback_short,
                    "setup_type": setup_type,
                    "raw_score": raw_score,
                    "dynamic_threshold": dynamic_threshold,
                    "required_min_score": effective_required_min_score,
                    "dist_ma": dist_ma,
                    "entry_timing": entry_timing,
                    "opportunity_type": opportunity_type,
                    "market_state": market_state,
                    "market_state_label": market_state_label,
                    "market_bias_label": market_bias_label,
                    "alt_mode": alt_mode,
                    "breakdown_quality": breakdown_quality,
                    "fake_signal": bool(score_result.get("fake_signal", False)),
                    "reversal_4h_confirmed": bool(is_reverse),
                    "has_high_impact_news": has_high_impact_news,
                    "news_titles": [e.get("title", "") for e in upcoming_events[:2]],
                    "warning_reasons": score_result.get("warning_reasons", []),
                    "rr1": rr1,
                    "rr2": rr2,
                    "price": price,
                    "stop_loss": stop_loss,
                    "tp1": tp1,
                    "tp2": tp2,
                    "tv_link": tv_link,
                    "display_risk": display_risk,
                    "setup_stats": setup_stats,
                    "bb_position": bb_position,
                    "lower_wick_ratio": lower_wick_ratio,
                    "upper_wick_ratio": upper_wick_ratio,
                    "close_position": close_position,
                    "body_ratio": body_ratio,
                    "stoch_rsi_k": stoch_rsi_k,
                    "stoch_rsi_d": stoch_rsi_d,
                    "adx": adx,
                    "plus_di": plus_di,
                    "minus_di": minus_di,
                    "rsi_now": rsi_now,
                    "funding_label": score_result.get("funding_label", "🟡 محايد"),
                    "reasons": score_result.get("reasons", []),
                    "score_result": score_result,
                    "mtf_confirmed": mtf_confirmed,
                    "btc_short_bias": btc_short_bias,
                    "change_24h": change_24h,
                    "alert_id": alert_id,
                    "candle_time": get_signal_candle_time(df),
                    "entry": price,
                    "sl": stop_loss,
                    "signal_rating": score_result.get("signal_rating", "⚡ عادي"),
                    "late_short_trap": late_filter.get("reject", False),
                    "late_short_reasons": late_filter.get("reasons", []),
                    "alert_snapshot": {
                        "alert_id": alert_id,
                        "symbol": symbol,
                        "mode": current_mode,
                        "market_mode": current_mode,
                        "timeframe": TIMEFRAME,
                        "entry": price,
                        "sl": stop_loss,
                        "tp1": tp1,
                        "tp2": tp2,
                        "rr1": rr1,
                        "rr2": rr2,
                        "score": float(score_result["score"]),
                        "candle_time": get_signal_candle_time(df),
                        "created_ts": int(time.time()),
                        "market_state": market_state,
                        "alt_mode": alt_mode,
                        "btc_mode": btc_mode,
                        "entry_timing": entry_timing,
                        "opportunity_type": opportunity_type,
                        "early_priority": early_priority,
                        "is_reverse": is_reverse,
                        "pullback_short": pullback_short,
                        "setup_type": setup_type,
                        "breakdown_quality": breakdown_quality,
                        "rsi_now": rsi_now,
                        "dist_ma": dist_ma,
                        "vol_ratio": vol_ratio,
                        "bull_guard_active": bool(bull_guard.get("block_shorts")),
                        "bull_guard_level": bull_guard.get("level", ""),
                        "market_green_ratio_15m": bull_guard.get("green_ratio_15m", 0),
                        "market_avg_change_15m": bull_guard.get("avg_change_15m", 0),
                        "btc_change_15m": bull_guard.get("btc_change_15m", 0),
                        "vwap_distance": vwap_distance,
                        "rsi_slope": rsi_slope,
                        "macd_hist": macd_hist,
                        "macd_hist_slope": macd_hist_slope,
                        "adx": adx,
                        "plus_di": plus_di,
                        "minus_di": minus_di,
                        "stoch_rsi_k": stoch_rsi_k,
                        "stoch_rsi_d": stoch_rsi_d,
                        "bb_position": bb_position,
                        "lower_wick_ratio": lower_wick_ratio,
                        "upper_wick_ratio": upper_wick_ratio,
                        "close_position": close_position,
                        "body_ratio": body_ratio,
                    },
                }
                candidate["bucket"] = get_candidate_bucket(candidate)
                candidates.append(candidate)

            logger.info(f"Candidates before momentum filter: {len(candidates)}")
            candidates = apply_top_momentum_filter(candidates)
            top_candidates = diversify_candidates(candidates, MAX_ALERTS_PER_RUN)
            logger.info(f"Sending {len(top_candidates)} alerts after diversification")

            sent_count = 0
            for candidate in top_candidates:
                symbol = candidate["symbol"]
                if already_sent_same_candle(symbol, candidate["candle_time"], "short"):
                    continue
                if is_symbol_on_cooldown(symbol, "short"):
                    continue
                locked = reserve_signal_slot(symbol, candidate["candle_time"], "short")
                if not locked:
                    continue

                # Build message now
                message = build_message_new(
                    symbol=symbol,
                    price=candidate["price"],
                    score_result=candidate["score_result"],
                    stop_loss=candidate["stop_loss"],
                    tp1=candidate["tp1"],
                    tp2=candidate["tp2"],
                    rr1=candidate["rr1"],
                    rr2=candidate["rr2"],
                    btc_mode=btc_mode,
                    btc_short_bias=candidate["btc_short_bias"],
                    tv_link=candidate["tv_link"],
                    is_new=candidate["is_new"],
                    change_24h=candidate["change_24h"],
                    market_state_label=candidate["market_state_label"],
                    market_bias_label=candidate["market_bias_label"],
                    alt_mode=candidate["alt_mode"],
                    news_warning=news_warning_text,
                    opportunity_type=candidate["opportunity_type"],
                    entry_timing=candidate["entry_timing"],
                    display_risk=candidate["display_risk"],
                    setup_stats=candidate["setup_stats"],
                    is_reverse=candidate["is_reverse"],
                    breakdown_quality=candidate["breakdown_quality"],
                    bull_guard_active=False,
                    bull_guard_level="",
                    pullback_short=candidate["pullback_short"],
                    bb_position=candidate["bb_position"],
                    lower_wick_ratio=candidate["lower_wick_ratio"],
                    stoch_rsi_k=candidate["stoch_rsi_k"],
                    rsi_now=candidate["rsi_now"],
                )

                sent_data = send_telegram_message(
                    message,
                    reply_markup=build_track_reply_markup(candidate["alert_id"]),
                )
                if sent_data.get("ok"):
                    sent_count += 1
                    sent_cache[symbol] = time.time()
                    last_candle_cache[symbol] = candidate["candle_time"]
                    last_global_send_ts = time.time()
                    message_id = str(((sent_data.get("result") or {}).get("message_id")) or "")
                    save_alert_snapshot(candidate.get("alert_snapshot", {}), message_id=message_id)

                    trade_reasons = list(candidate["reasons"] or [])
                    if candidate.get("is_reverse"):
                        tag = "OVEREXTENDED_REVERSAL_4H_1H_15M"
                        if tag not in trade_reasons:
                            trade_reasons.append(tag)
                    if candidate.get("pullback_short"):
                        if "PULLBACK_SHORT" not in trade_reasons:
                            trade_reasons.append("PULLBACK_SHORT")

                    safe_register_trade(
                        redis_client=r,
                        symbol=symbol,
                        market_type="futures",
                        side="short",
                        candle_time=candidate["candle_time"],
                        entry=candidate["entry"],
                        sl=candidate["sl"],
                        tp1=candidate["tp1"],
                        tp2=candidate["tp2"],
                        score=candidate["score"],
                        timeframe=TIMEFRAME,
                        btc_mode=btc_mode,
                        funding_label=candidate["funding_label"],
                        reasons=trade_reasons,
                        pre_breakout=candidate["pre_breakdown"],
                        breakout=candidate["breakdown"],
                        vol_ratio=candidate["vol_ratio"],
                        candle_strength=candidate["candle_strength"],
                        mtf_confirmed=candidate["mtf_confirmed"],
                        is_new=candidate["is_new"],
                        btc_dominance_proxy=candidate["btc_short_bias"],
                        change_24h=candidate["change_24h"],
                        setup_type=candidate.get("setup_type", "unknown"),
                        raw_score=candidate.get("raw_score", candidate["score"]),
                        effective_score=candidate["score"],
                        dynamic_threshold=candidate.get("dynamic_threshold", 0.0),
                        required_min_score=candidate.get("required_min_score", 0.0),
                        dist_ma=candidate.get("dist_ma", 0.0),
                        entry_timing=candidate.get("entry_timing", ""),
                        opportunity_type=candidate.get("opportunity_type", ""),
                        market_state=candidate.get("market_state", ""),
                        market_state_label=candidate.get("market_state_label", ""),
                        market_bias_label=candidate.get("market_bias_label", ""),
                        alt_mode=candidate.get("alt_mode", ""),
                        early_priority=candidate.get("early_priority", ""),
                        breakout_quality=candidate.get("breakdown_quality", ""),
                        fake_signal=candidate.get("fake_signal", False),
                        is_reverse_signal=candidate.get("is_reverse", False),
                        reversal_4h_confirmed=bool(candidate.get("is_reverse", False)),
                        rank_volume_24h=candidate.get("rank_volume_24h", 0.0),
                        alert_id=candidate.get("alert_id", ""),
                        has_high_impact_news=candidate.get("has_high_impact_news", False),
                        news_titles=candidate.get("news_titles", []),
                        warning_reasons=candidate.get("warning_reasons", []),
                        rr1=candidate.get("rr1", 1.4),
                        rr2=candidate.get("rr2", 2.4),
                        adx=candidate.get("adx", 0),
                        plus_di=candidate.get("plus_di", 0),
                        minus_di=candidate.get("minus_di", 0),
                        stoch_rsi_k=candidate.get("stoch_rsi_k", 50),
                        stoch_rsi_d=candidate.get("stoch_rsi_d", 50),
                        bb_position=candidate.get("bb_position", "inside"),
                        lower_wick_ratio=candidate.get("lower_wick_ratio", 0),
                        upper_wick_ratio=candidate.get("upper_wick_ratio", 0),
                        close_position=candidate.get("close_position", 0),
                        body_ratio=candidate.get("body_ratio", 0),
                        pullback_short=candidate.get("pullback_short", False),
                        late_short_trap=candidate.get("late_short_trap", False),
                        late_short_reasons=candidate.get("late_short_reasons", []),
                    )
                    logger.info(f"SENT SHORT → {symbol} score={candidate['score']:.1f}")
                else:
                    release_signal_slot(symbol, candidate["candle_time"], "short")

            if sent_count > 0:
                set_global_cooldown()
            logger.info(f"Sent {sent_count} short alerts, tested {tested} pairs, sleeping 60s")
            time.sleep(60)

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            traceback.print_exc()
            time.sleep(10)
        finally:
            if scan_locked:
                release_scan_lock()


# ================================================
# TELEGRAM POLLER
# ================================================
def bootstrap_telegram_offset_once():
    if is_telegram_bootstrap_done():
        return
    if not acquire_telegram_poll_lock():
        return
    try:
        updates = get_telegram_updates(offset=0)
        if updates:
            save_telegram_offset(updates[-1]["update_id"] + 1)
        mark_telegram_bootstrap_done()
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
                chat_id = str((message.get("chat") or {}).get("id", ""))
                if not text or not chat_id:
                    continue
                command = text.split()[0].split("@")[0]
                handler = COMMAND_HANDLERS.get(command)
                if handler:
                    handler(chat_id)
            except Exception as e:
                logger.error(f"handle_telegram_commands item error: {e}")
        if latest_offset != offset:
            save_telegram_offset(latest_offset)
    finally:
        release_telegram_poll_lock()


def run_command_poller():
    bootstrap_telegram_offset_once()
    while True:
        try:
            handle_telegram_commands()
        except Exception as e:
            logger.error(f"Command poller error: {e}")
        time.sleep(COMMAND_POLL_INTERVAL)


def run():
    logger.info(f"SHORT BOT STARTED | pid={os.getpid()}")
    clear_webhook()
    threading.Thread(target=run_command_poller, daemon=True).start()
    run_scanner_loop()


if __name__ == "__main__":
    run()
