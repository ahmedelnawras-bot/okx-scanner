import os
import sys
import time
import html
import logging
import requests
import pandas as pd
import redis

from analysis.scoring import calculate_long_score, is_breakout
from tracking.performance import (
    register_trade,
    update_open_trades,
    get_winrate_summary,
    format_winrate_summary,
    calc_tp1,
    calc_tp2,
)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("okx-scanner")

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
REDIS_URL = os.getenv("REDIS_URL")

OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"
OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
OKX_FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate"

SCAN_LIMIT = 200
TIMEFRAME = "15m"
HTF_TIMEFRAME = "1H"

FINAL_MIN_SCORE = 6.3
MAX_ALERTS_PER_RUN = 3

COOLDOWN_SECONDS = 3600
LOCAL_RECENT_SEND_SECONDS = 2700   # 45 دقيقة
GLOBAL_COOLDOWN_SECONDS = 120

MIN_24H_QUOTE_VOLUME = 1_000_000
NEW_LISTING_MAX_CANDLES = 50

TOP_MOMENTUM_PERCENT = 0.20
TOP_MOMENTUM_MIN_SCORE = 7.0
TOP_MOMENTUM_NEW_MIN_SCORE = 6.0

NEW_LISTING_MIN_VOL_RATIO = 1.8
NEW_LISTING_MIN_CANDLE_STRENGTH = 0.45
NEW_LISTING_MAX_PER_RUN = 1

SCAN_LOCK_KEY = "scan:running"
SCAN_LOCK_TTL = 300

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


def release_signal_slot(symbol: str, candle_time: int, signal_type: str = "long") -> None:
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


# =========================
# TELEGRAM
# =========================
def send_telegram_message(message: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        logger.error("❌ Telegram config missing")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=15)

        if response.status_code != 200:
            logger.error(f"❌ Telegram HTTP Error: {response.text}")
            return False

        data = response.json()
        if not data.get("ok"):
            logger.error(f"❌ Telegram API Error: {data}")
            return False

        return True

    except Exception as e:
        logger.error(f"❌ Telegram Exception: {e}")
        return False


# =========================
# OKX DATA
# =========================
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


def get_ranked_pairs():
    try:
        res = requests.get(
            OKX_TICKERS_URL,
            params={"instType": "SWAP"},
            timeout=20,
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

            item["_rank_volume_24h"] = vol_24h
            filtered.append(item)

        filtered.sort(key=lambda x: x.get("_rank_volume_24h", 0), reverse=True)
        top = filtered[:SCAN_LIMIT]

        logger.info(f"After liquidity filter: {len(filtered)}")
        logger.info(f"Using top ranked pairs: {len(top)}")

        return top

    except Exception as e:
        logger.error(f"get_ranked_pairs error: {e}")
        return []


def compute_rsi(series, period=14):
    """
    Wilder RSI (الأدق والأشهر)
    """
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

    return df


def get_candles(symbol, timeframe="15m", limit=100):
    try:
        params = {"instId": symbol, "bar": timeframe, "limit": limit}
        res = requests.get(OKX_CANDLES_URL, params=params, timeout=20).json()
        return res.get("data", [])
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


def get_signal_row(df):
    try:
        if "confirm" not in df.columns or len(df) < 2:
            return df.iloc[-2]

        last = df.iloc[-1]
        if str(int(float(last["confirm"]))) == "1":
            return last

        return df.iloc[-2]
    except Exception:
        return df.iloc[-2]


def get_signal_candle_time(df):
    try:
        signal_row = get_signal_row(df)
        ts = int(signal_row["ts"])
        if ts > 10_000_000_000:
            return ts // 1000
        return ts
    except Exception:
        return int(time.time() // (15 * 60))


# =========================
# STRATEGY HELPERS
# =========================
def early_bullish_signal(df):
    try:
        if df is None or df.empty or len(df) < 25:
            return False

        signal_row = get_signal_row(df)
        signal_idx = signal_row.name

        if signal_idx is None or signal_idx < 1:
            return False

        last = df.iloc[signal_idx]
        prev = df.iloc[signal_idx - 1]

        score = 0
        if float(last["close"]) > float(last["open"]):
            score += 1
        if "rsi" in df.columns and float(last["rsi"]) > 50:
            score += 1
        if float(last["volume"]) > float(prev["volume"]):
            score += 1

        return score >= 2

    except Exception:
        return False


def is_higher_timeframe_confirmed(symbol):
    try:
        candles = get_candles(symbol, HTF_TIMEFRAME, 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return False

        signal_row = get_signal_row(df)
        ma_value = signal_row.get("ma", None)

        score = 0
        if ma_value is not None and float(signal_row["close"]) > float(ma_value):
            score += 1
        if float(signal_row.get("rsi", 0)) > 50:
            score += 1

        return score >= 2

    except Exception as e:
        logger.error(f"MTF error on {symbol}: {e}")
        return False


def get_btc_mode():
    try:
        candles = get_candles("BTC-USDT-SWAP", "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return "🟡 محايد"

        signal_row = get_signal_row(df)
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


def calculate_stop_loss(price, atr_value):
    try:
        return round(float(price) - (float(atr_value) * 1.2), 6)
    except Exception:
        return round(float(price), 6)


def calculate_sl_percent(entry, sl):
    try:
        return round(((entry - sl) / entry) * 100, 2)
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


def get_volume_ratio(df) -> float:
    """
    مقارنة حجم شمعة الإشارة بمتوسط آخر 20 شمعة قبلها
    بدل الشمعة السابقة فقط
    """
    try:
        signal_row = get_signal_row(df)
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


def is_valid_candle_timing(df) -> bool:
    try:
        now = int(time.time())
        candle_seconds = 15 * 60

        # مرجع ثابت: بداية آخر شمعة حالية
        last_completed_ts = (now // candle_seconds) * candle_seconds

        signal_row = get_signal_row(df)
        ts = int(signal_row["ts"])

        if ts > 10_000_000_000:
            ts = ts // 1000

        candle_age = last_completed_ts - ts

        # مقبول لو شمعة الإشارة ليست أقدم من آخر شمعتين مكتملتين
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


def get_effective_min_score(is_new: bool) -> float:
    return TOP_MOMENTUM_NEW_MIN_SCORE if is_new else TOP_MOMENTUM_MIN_SCORE


def get_momentum_priority(score: float, breakout: bool, vol_ratio: float, is_new: bool) -> float:
    priority = float(score)

    if breakout:
        priority += 1.0

    if vol_ratio >= 2.0:
        priority += 1.0
    elif vol_ratio >= 1.5:
        priority += 0.5

    if is_new and vol_ratio >= NEW_LISTING_MIN_VOL_RATIO:
        priority += 0.5

    return round(priority, 2)


def get_candidate_bucket(candidate: dict) -> str:
    if candidate["is_new"] and candidate["breakout"]:
        return "new_breakout"
    if candidate["breakout"]:
        return "breakout"
    if candidate["vol_ratio"] >= 2.0:
        return "volume"
    return "standard"


def apply_top_momentum_filter(candidates):
    if not candidates:
        return []

    strong_candidates = []
    for c in candidates:
        min_score = get_effective_min_score(c["is_new"])
        if c["score"] >= min_score:
            strong_candidates.append(c)

    if not strong_candidates:
        logger.info("Top momentum filter: no candidates above threshold")
        return []

    strong_candidates.sort(
        key=lambda x: (x["momentum_priority"], x["score"], x["rank_volume_24h"]),
        reverse=True,
    )

    top_n = max(3, int(len(strong_candidates) * TOP_MOMENTUM_PERCENT))
    filtered = strong_candidates[:top_n]

    final_candidates = []
    new_count = 0

    for c in filtered:
        if c["is_new"]:
            if new_count >= NEW_LISTING_MAX_PER_RUN:
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
            key=lambda x: (x["momentum_priority"], x["score"], x["rank_volume_24h"]),
            reverse=True,
        )

    diversified = []
    used_patterns = set()

    for bucket_name in ["new_breakout", "breakout", "volume", "standard"]:
        if bucket_name not in buckets or not buckets[bucket_name]:
            continue

        candidate = buckets[bucket_name][0]
        pattern = (
            candidate["breakout"],
            round(candidate["vol_ratio"], 1),
            candidate["is_new"],
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
            key=lambda x: (x["momentum_priority"], x["score"], x["rank_volume_24h"]),
            reverse=True,
        )

        for candidate in remaining:
            if len(diversified) >= max_alerts:
                break

            pattern = (
                candidate["breakout"],
                round(candidate["vol_ratio"], 1),
                candidate["is_new"],
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
            f"{c['symbol']}[{get_candidate_bucket(c)}|{c['momentum_priority']}]"
            for c in diversified
        )
    )

    return diversified[:max_alerts]


def build_message(symbol, price, score_result, stop_loss, btc_mode, tv_link, is_new):
    symbol_clean = clean_symbol_for_message(symbol)
    details = " + ".join(score_result["reasons"]) if score_result["reasons"] else "زخم مبكر"
    funding_text = score_result.get("funding_label", "🟡 محايد")
    signal_rating = score_result.get("signal_rating", "⚡ عادي")
    sl_pct = calculate_sl_percent(price, stop_loss)

    tp1 = calc_tp1(price, stop_loss)
    tp2 = calc_tp2(price, stop_loss)

    tp1_pct = round(((tp1 - price) / price) * 100, 2) if price else 0.0
    tp2_pct = round(((tp2 - price) / price) * 100, 2) if price else 0.0

    new_tag = "\n🆕 <b>عملة جديدة</b>" if is_new else ""

    safe_symbol = html.escape(symbol_clean)
    safe_btc = html.escape(btc_mode)
    safe_details = html.escape(details)
    safe_funding = html.escape(funding_text)
    safe_rating = html.escape(signal_rating)
    safe_tv_link = html.escape(tv_link, quote=True)

    return f"""🚀 <b>لونج فيوتشر | {safe_symbol}</b>

💰 {price:.6f} | ⏱ 15m
⭐ {score_result["score"]:.1f} / 10 | 🛑 SL: {stop_loss:.6f} (-{sl_pct}%)

🎯 TP1: {tp1:.6f} (+{tp1_pct}%)
🏁 TP2: {tp2:.6f} (+{tp2_pct}%)

🏷 <b>التصنيف:</b> {safe_rating}

🪙 BTC: {safe_btc}
💸 التمويل: {safe_funding}{new_tag}

📊 {safe_details}

🔗 <a href="{safe_tv_link}">Open Chart</a>
"""


def run():
    global last_global_send_ts

    while True:
        scan_locked = False

        try:
            global_elapsed = time.time() - last_global_send_ts
            if last_global_send_ts > 0 and global_elapsed < GLOBAL_COOLDOWN_SECONDS:
                remaining = int(GLOBAL_COOLDOWN_SECONDS - global_elapsed)
                logger.info(f"GLOBAL COOLDOWN active ({remaining}s) — skipping scan")
                time.sleep(min(remaining, 60))
                continue

            scan_locked = acquire_scan_lock()
            if not scan_locked:
                logger.info("Another scan is running — skipping")
                time.sleep(30)
                continue

            logger.info(f"RUN START | pid={os.getpid()} | ts={int(time.time())}")

            update_open_trades(r, market_type="futures", side="long", timeframe=TIMEFRAME)
            winrate_summary = get_winrate_summary(r, market_type="futures", side="long")
            logger.info(format_winrate_summary(winrate_summary))

            ranked_pairs = get_ranked_pairs()
            btc_mode = get_btc_mode()

            tested = 0
            sent_count = 0
            sent_symbols_this_run = set()
            candidates = []
            candidates_symbols = set()

            for pair_data in ranked_pairs:
                tested += 1
                symbol = pair_data["instId"]

                candles = get_candles(symbol, TIMEFRAME, 100)
                df = to_dataframe(candles)

                if df is None or df.empty:
                    continue

                if not is_valid_candle_timing(df):
                    logger.info(f"{symbol} → skipped (candle timing invalid)")
                    continue

                early_signal = early_bullish_signal(df)
                if not early_signal:
                    logger.info(f"{symbol} → early_signal: False")
                    continue

                breakout = is_breakout(df)
                mtf_confirmed = is_higher_timeframe_confirmed(symbol)
                is_new = is_new_listing_by_candles(candles)
                funding = get_funding_rate(symbol)

                score_result = calculate_long_score(
                    df=df,
                    mtf_confirmed=mtf_confirmed,
                    btc_mode=btc_mode,
                    breakout=breakout,
                    is_new=is_new,
                    funding=funding,
                )

                logger.info(
                    f"{symbol} → early_signal: True | "
                    f"score: {score_result['score']} | "
                    f"score_signal: {score_result['signal']} | "
                    f"fake: {score_result['fake_signal']} | "
                    f"mtf: {mtf_confirmed} | "
                    f"new: {is_new}"
                )

                if score_result["fake_signal"]:
                    logger.info(f"{symbol} → rejected by fake signal")
                    continue

                if score_result["score"] < FINAL_MIN_SCORE:
                    logger.info(f"{symbol} → rejected by final min score ({score_result['score']})")
                    continue

                candle_time = get_signal_candle_time(df)
                now = time.time()

                if symbol in last_candle_cache and last_candle_cache[symbol] == candle_time:
                    logger.info(f"{symbol} → skipped (same candle in memory)")
                    continue

                if symbol in sent_cache and now - sent_cache[symbol] < LOCAL_RECENT_SEND_SECONDS:
                    logger.info(f"{symbol} → skipped (local cooldown active)")
                    continue

                if symbol in sent_symbols_this_run:
                    logger.info(f"{symbol} → skipped (already sent this run)")
                    continue

                if symbol in candidates_symbols:
                    logger.info(f"{symbol} → skipped (already queued this run)")
                    continue

                if already_sent_same_candle(symbol, candle_time, "long"):
                    logger.info(f"{symbol} → skipped (same candle in Redis)")
                    continue

                if is_symbol_on_cooldown(symbol, "long"):
                    logger.info(f"{symbol} → skipped (cooldown active)")
                    continue

                vol_ratio = get_volume_ratio(df)
                candle_strength = get_candle_strength_ratio(df)

                if is_new:
                    if not passes_new_listing_filter(
                        score=float(score_result["score"]),
                        breakout=breakout,
                        vol_ratio=vol_ratio,
                        candle_strength=candle_strength,
                    ):
                        logger.info(f"{symbol} → rejected by balanced new listing filter")
                        continue

                signal_row = get_signal_row(df)
                price = float(signal_row["close"])
                atr_value = float(signal_row["atr"])
                stop_loss = calculate_stop_loss(price, atr_value)
                tv_link = build_tradingview_link(symbol)

                momentum_priority = get_momentum_priority(
                    score=float(score_result["score"]),
                    breakout=breakout,
                    vol_ratio=vol_ratio,
                    is_new=is_new,
                )

                candidate = {
                    "symbol": symbol,
                    "score": float(score_result["score"]),
                    "momentum_priority": momentum_priority,
                    "breakout": breakout,
                    "vol_ratio": vol_ratio,
                    "candle_strength": candle_strength,
                    "is_new": is_new,
                    "rank_volume_24h": float(pair_data.get("_rank_volume_24h", 0)),
                    "message": build_message(
                        symbol=symbol,
                        price=price,
                        score_result=score_result,
                        stop_loss=stop_loss,
                        btc_mode=btc_mode,
                        tv_link=tv_link,
                        is_new=is_new,
                    ),
                    "candle_time": candle_time,
                    "now": now,
                    "entry": price,
                    "sl": stop_loss,
                    "funding_label": score_result.get("funding_label", "🟡 محايد"),
                }
                candidate["bucket"] = get_candidate_bucket(candidate)

                candidates.append(candidate)
                candidates_symbols.add(symbol)

            logger.info(f"Candidates found before momentum filter: {len(candidates)}")

            candidates = apply_top_momentum_filter(candidates)
            logger.info(f"Candidates found after momentum filter: {len(candidates)}")

            top_candidates = diversify_candidates(candidates, MAX_ALERTS_PER_RUN)

            for candidate in top_candidates:
                symbol = candidate["symbol"]

                if symbol in sent_symbols_this_run:
                    logger.info(f"{symbol} → skipped (already sent final stage)")
                    continue

                logger.info(
                    f"FINAL CHECK | {symbol} | candle={candidate['candle_time']} | "
                    f"same_candle={already_sent_same_candle(symbol, candidate['candle_time'], 'long')} | "
                    f"cooldown={is_symbol_on_cooldown(symbol, 'long')}"
                )

                if already_sent_same_candle(symbol, candidate["candle_time"], "long"):
                    logger.info(f"{symbol} → skipped (already sent this candle Redis FINAL)")
                    continue

                if is_symbol_on_cooldown(symbol, "long"):
                    logger.info(f"{symbol} → skipped (cooldown active final)")
                    continue

                locked = reserve_signal_slot(
                    symbol=symbol,
                    candle_time=candidate["candle_time"],
                    signal_type="long",
                )

                if not locked:
                    logger.info(f"{symbol} → skipped (reserve failed / duplicate)")
                    continue

                sent_ok = send_telegram_message(candidate["message"])

                if sent_ok:
                    sent_symbols_this_run.add(symbol)
                    sent_count += 1
                    sent_cache[symbol] = time.time()
                    last_candle_cache[symbol] = candidate["candle_time"]
                    last_global_send_ts = time.time()

                    register_trade(
                        redis_client=r,
                        symbol=symbol,
                        market_type="futures",
                        side="long",
                        candle_time=candidate["candle_time"],
                        entry=candidate["entry"],
                        sl=candidate["sl"],
                        score=candidate["score"],
                        timeframe=TIMEFRAME,
                        btc_mode=btc_mode,
                        funding_label=candidate["funding_label"],
                    )

                    logger.info(
                        f"SENT → {symbol} | score: {candidate['score']} | "
                        f"momentum: {candidate['momentum_priority']} | "
                        f"bucket: {candidate['bucket']} | new={candidate['is_new']}"
                    )
                else:
                    release_signal_slot(
                        symbol=symbol,
                        candle_time=candidate["candle_time"],
                        signal_type="long",
                    )
                    logger.error(f"FAILED SEND → {symbol}")

            logger.info(f"Sent alerts this run: {sent_count}")
            logger.info(f"Tested {tested} pairs")
            logger.info("Sleeping 60 seconds...")

            time.sleep(60)

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            time.sleep(10)

        finally:
            if scan_locked:
                release_scan_lock()


if __name__ == "__main__":
    run()
