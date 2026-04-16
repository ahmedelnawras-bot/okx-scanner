import sys
import os
import time
import redis
import html

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal
from analysis.scoring import calculate_long_score

# =========================
# SETTINGS
# =========================
COOLDOWN_SECONDS = 3600          # ساعة لنفس الزوج/النوع
MAX_ALERTS_PER_RUN = 2           # تقليل السبام
SCAN_LIMIT = 200                 # Top 200 حقيقي
MIN_24H_QUOTE_VOLUME = 1_000_000 # فلتر سيولة مبدئي
NEW_LISTING_MAX_CANDLES = 50     # أقل من كده = عملة جديدة تقريبًا

REDIS_URL = os.environ.get("REDIS_URL")

r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print("✅ Redis connected")
    except Exception as e:
        print(f"❌ Redis connection error: {e}")
        r = None
else:
    print("⚠️ REDIS_URL not found")


# =========================
# REDIS KEYS
# =========================
def clean_symbol_for_message(symbol: str) -> str:
    return symbol.replace("-SWAP", "")


def get_same_candle_key(symbol: str, candle_time: int, signal_type: str = "long") -> str:
    return f"sent:{signal_type}:{symbol}:{candle_time}"


def get_cooldown_key(symbol: str, signal_type: str = "long") -> str:
    clean = clean_symbol_for_message(symbol)
    return f"cooldown:{signal_type}:{clean}"


def get_pair_lock_key(symbol: str, signal_type: str = "long") -> str:
    clean = clean_symbol_for_message(symbol)
    return f"pairlock:{signal_type}:{clean}"


def already_sent_same_candle(symbol: str, candle_time: int, signal_type: str = "long") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_same_candle_key(symbol, candle_time, signal_type)))
    except Exception as e:
        print(f"Redis exists error (same candle): {e}")
        return False


def in_cooldown(symbol: str, signal_type: str = "long") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_cooldown_key(symbol, signal_type)))
    except Exception as e:
        print(f"Redis exists error (cooldown): {e}")
        return False


def pair_locked(symbol: str, signal_type: str = "long") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_pair_lock_key(symbol, signal_type)))
    except Exception as e:
        print(f"Redis exists error (pair lock): {e}")
        return False


def mark_sent(symbol: str, candle_time: int, signal_type: str = "long") -> None:
    if not r:
        return
    try:
        r.set(get_same_candle_key(symbol, candle_time, signal_type), "1", ex=7200)
        r.set(get_cooldown_key(symbol, signal_type), "1", ex=COOLDOWN_SECONDS)
        r.set(get_pair_lock_key(symbol, signal_type), "1", ex=COOLDOWN_SECONDS)
        print(f"✅ Redis saved for {symbol} | candle={candle_time}")
    except Exception as e:
        print(f"Redis save error: {e}")


# =========================
# MARKET FILTERING
# =========================
def is_excluded_symbol(symbol: str) -> bool:
    """
    استبعاد stablecoins وأشياء مزعجة شائعة.
    """
    excluded_prefixes = (
        "USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP", "USD0"
    )
    if symbol.startswith(excluded_prefixes):
        return True

    # فلتر بدائي للأسماء المبالغ فيها
    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "")
    if len(base) > 20:
        return True

    return False


def extract_24h_quote_volume(ticker: dict) -> float:
    """
    يحاول يقرأ أفضل حقل متاح لسيولة 24h بقيمة quote/base مناسبة.
    عدّل ترتيب الحقول لو okx_client عندك مختلف.
    """
    candidate_fields = [
        "volCcy24h",    # الأفضل غالبًا
        "turnover24h",
        "quoteVolume",
        "vol24h",
    ]

    for field in candidate_fields:
        value = ticker.get(field)
        if value is None:
            continue
        try:
            return float(value)
        except Exception:
            continue

    return 0.0


def get_ranked_pairs():
    """
    Top 200 حقيقي:
    1) USDT-SWAP فقط
    2) استبعاد stablecoins والعملات الغريبة
    3) فلتر سيولة
    4) ترتيب تنازلي حسب 24h volume
    """
    futures = get_tickers("SWAP")
    print(f"Fetched {len(futures)} futures pairs")

    filtered = []
    for p in futures:
        symbol = p.get("instId", "")

        if "USDT" not in symbol:
            continue

        if not symbol.endswith("-SWAP"):
            continue

        if is_excluded_symbol(symbol):
            continue

        vol_24h = extract_24h_quote_volume(p)
        if vol_24h < MIN_24H_QUOTE_VOLUME:
            continue

        p["_rank_volume_24h"] = vol_24h
        filtered.append(p)

    filtered.sort(key=lambda x: x.get("_rank_volume_24h", 0), reverse=True)

    top_pairs = filtered[:SCAN_LIMIT]

    print(f"After liquidity filter: {len(filtered)}")
    print(f"Using top ranked pairs: {len(top_pairs)}")
    return top_pairs


# =========================
# INDICATORS / HELPERS
# =========================
def is_volume_spike(df, multiplier=1.2):
    if df is None or df.empty or len(df) < 20:
        return False

    last_volume = df["volume"].iloc[-1]
    avg_volume_20 = df["volume"].rolling(20).mean().iloc[-1]

    if avg_volume_20 == 0:
        return False

    return last_volume >= (avg_volume_20 * multiplier)


def get_last_candle_time(df):
    try:
        ts = df["ts"].iloc[-1]
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts // 1000
        return ts
    except Exception as e:
        print(f"⚠️ candle time error: {e}")
        return 0


def get_btc_mode():
    try:
        candles = get_candles("BTC-USDT-SWAP", "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return "🟡 محايد"

        df = add_ma(df)
        df = add_rsi(df)

        last = df.iloc[-1]
        ma_value = last.get("ma", None)
        rsi_value = float(last.get("rsi", 50))

        if ma_value is not None:
            if last["close"] > ma_value and rsi_value >= 55:
                return "🟢 صاعد (داعم)"
            elif last["close"] < ma_value and rsi_value <= 45:
                return "🔴 هابط (ضاغط)"
            return "🟡 محايد"

        return "🟡 محايد"

    except Exception as e:
        print(f"BTC mode error: {e}")
        return "🟡 محايد"


def is_higher_timeframe_confirmed(symbol):
    """
    MTF مرن:
    - فوق MA20 = نقطة
    - RSI > 50 = نقطة
    يكفي نقطة واحدة
    """
    try:
        candles = get_candles(symbol, "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return False

        df = add_ma(df)
        df = add_rsi(df)

        last = df.iloc[-1]
        ma_value = last.get("ma", None)

        score = 0
        if ma_value is not None and last["close"] > ma_value:
            score += 1
        if float(last.get("rsi", 0)) > 50:
            score += 1

        return score >= 1

    except Exception as e:
        print(f"MTF error on {symbol}: {e}")
        return False


def is_breakout(df, lookback=20):
    try:
        if df is None or df.empty or len(df) < lookback + 2:
            return False

        recent_high = df["high"].rolling(lookback).max().iloc[-2]
        last_close = df["close"].iloc[-1]
        return bool(last_close > recent_high)
    except Exception as e:
        print(f"Breakout error: {e}")
        return False


def calculate_stop_loss(price, atr_value):
    try:
        return round(float(price) - (float(atr_value) * 1.2), 6)
    except Exception:
        return round(float(price), 6)


def is_new_listing_by_candles(candles) -> bool:
    """
    لو التاريخ قليل، نعتبرها عملة جديدة.
    """
    try:
        return len(candles) < NEW_LISTING_MAX_CANDLES
    except Exception:
        return False


def build_tradingview_link(symbol):
    """
    مثال:
    MINA-USDT-SWAP -> OKX:MINAUSDT.P
    """
    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
    tv_symbol = f"OKX:{base}USDT.P"
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"


def build_message(symbol, price, score, stop_loss, btc_mode, volume_spike, mtf_confirmed, breakout, tv_link, is_new):
    msg_symbol = clean_symbol_for_message(symbol)

    reasons = ["زخم مبكر"]
    flags = []

    if volume_spike:
        reasons.append("فوليوم قوي")
        flags.append("Vol ↑")

    if breakout:
        reasons.append("اختراق")
        flags.append("Break ✔")

    flags.append("RSI ↑")

    if mtf_confirmed:
        reasons.append("تأكيد 1H")
        flags.append("MTF ✔")

    reason_line = " + ".join(reasons)
    flags_line = " | ".join(flags) if flags else "Setup"

    safe_symbol = html.escape(msg_symbol)
    safe_btc = html.escape(btc_mode)
    safe_reason = html.escape(reason_line)
    safe_flags = html.escape(flags_line)
    safe_tv_link = html.escape(tv_link, quote=True)

    new_tag = "\n🆕 <b>عملة جديدة</b>\n" if is_new else "\n"

    return f"""🚀 <b>لونج فيوتشر | {safe_symbol}</b>

💰 {round(price, 6)} | ⏱ 15m
⭐ {round(score, 1)} / 10 | 🛑 {stop_loss}

🪙 BTC: {safe_btc}{new_tag}
📊 {safe_reason}

🔥 {safe_flags}

🔗 <a href="{safe_tv_link}">فتح الشارت</a>
"""


# =========================
# MAIN RUN
# =========================
def run():
    print("🚀 Bot Started...")

    btc_mode = get_btc_mode()
    print(f"BTC mode: {btc_mode}")

    ranked_pairs = get_ranked_pairs()

    tested = 0
    collected_keys = set()
    candidates = []

    for pair_data in ranked_pairs:
        tested += 1
        symbol = pair_data["instId"]

        try:
            candles = get_candles(symbol, "15m", 100)
            df = to_dataframe(candles)

            if df is None or df.empty:
                print(f"{symbol} → empty dataframe")
                continue

            df = add_ma(df)
            df = add_rsi(df)
            df = add_atr(df)

            signal = early_bullish_signal(df)
            volume_spike = is_volume_spike(df, multiplier=1.2)
            mtf_confirmed = is_higher_timeframe_confirmed(symbol)
            breakout = is_breakout(df, lookback=20)
            is_new = is_new_listing_by_candles(candles)

            if signal:
                score = calculate_long_score(df)

                if volume_spike:
                    score += 0.3
                else:
                    score -= 1.0

                if mtf_confirmed:
                    score += 0.5
                else:
                    score -= 0.5

                if breakout:
                    score += 0.5

                if "🔴" in btc_mode:
                    score -= 1.0
                elif "🟢" in btc_mode:
                    score += 0.3

                # bonus صغير للعملات الجديدة فقط لو الشروط أصلًا جيدة
                if is_new and score >= 7.5:
                    score += 0.2

                score = max(0, min(10, score))
            else:
                score = 0

            print(
                f"{symbol} → signal: {signal} | "
                f"score: {score} | "
                f"volume_spike: {volume_spike} | "
                f"mtf: {mtf_confirmed} | "
                f"breakout: {breakout} | "
                f"new: {is_new}"
            )

            if not signal:
                continue

            if score < 7.5:
                continue

            if score < 8 and not volume_spike:
                continue

            candle_time = get_last_candle_time(df)
            if candle_time == 0:
                print(f"{symbol} → skipped (invalid candle time)")
                continue

            same_candle_key = get_same_candle_key(symbol, candle_time, "long")

            if same_candle_key in collected_keys:
                print(f"{symbol} → skipped (already collected in this run)")
                continue

            if already_sent_same_candle(symbol, candle_time, "long"):
                print(f"{symbol} → skipped (same candle in Redis)")
                continue

            if in_cooldown(symbol, "long"):
                print(f"{symbol} → skipped (cooldown in Redis)")
                continue

            if pair_locked(symbol, "long"):
                print(f"{symbol} → skipped (pair lock)")
                continue

            price = float(df["close"].iloc[-1])
            atr_value = float(df["atr"].iloc[-1])
            stop_loss = calculate_stop_loss(price, atr_value)
            tv_link = build_tradingview_link(symbol)

            message = build_message(
                symbol=symbol,
                price=price,
                score=score,
                stop_loss=stop_loss,
                btc_mode=btc_mode,
                volume_spike=volume_spike,
                mtf_confirmed=mtf_confirmed,
                breakout=breakout,
                tv_link=tv_link,
                is_new=is_new,
            )

            candidates.append({
                "symbol": symbol,
                "score": float(score),
                "volume_spike": bool(volume_spike),
                "rank_volume_24h": float(pair_data.get("_rank_volume_24h", 0)),
                "message": message,
                "candle_time": candle_time,
            })

            collected_keys.add(same_candle_key)

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    # الترتيب النهائي:
    # 1) score
    # 2) volume spike
    # 3) liquidity rank
    candidates.sort(
        key=lambda x: (x["score"], x["volume_spike"], x["rank_volume_24h"]),
        reverse=True
    )

    top_candidates = candidates[:MAX_ALERTS_PER_RUN]

    sent_count = 0

    for candidate in top_candidates:
        sent_ok = send_telegram_message(candidate["message"])

        if sent_ok:
            mark_sent(
                symbol=candidate["symbol"],
                candle_time=candidate["candle_time"],
                signal_type="long",
            )
            sent_count += 1
            print(
                f'SENT → {candidate["symbol"]} | '
                f'score: {candidate["score"]} | '
                f'volume_spike: {candidate["volume_spike"]}'
            )
        else:
            print(f'FAILED SEND → {candidate["symbol"]}')

    print(f"Candidates found: {len(candidates)}")
    print(f"Sent alerts this run: {sent_count}")
    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    while True:
        try:
            run()
        except Exception as e:
            print(f"Fatal error: {e}")

        print("Sleeping 60 seconds...")
        time.sleep(60)
