import sys
import os
import time
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal
from analysis.scoring import calculate_long_score

COOLDOWN_SECONDS = 900  # 15 دقيقة
STATE_FILE = "alert_state.json"


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {
                    "last_sent_at": data.get("last_sent_at", {}),
                    "last_fingerprint": data.get("last_fingerprint", {}),
                }
        except Exception:
            pass

    return {
        "last_sent_at": {},
        "last_fingerprint": {},
    }


def save_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        print(f"Error saving state file: {e}")


def is_volume_spike(df, multiplier=1.2):
    if df is None or df.empty or len(df) < 20:
        return False

    last_volume = df["volume"].iloc[-1]
    avg_volume_20 = df["volume"].rolling(20).mean().iloc[-1]

    if avg_volume_20 == 0:
        return False

    return last_volume >= (avg_volume_20 * multiplier)


def should_send_alert(state, signal_key, fingerprint, now, cooldown_seconds):
    # 1) منع تكرار نفس الإشارة على نفس الشمعة
    if state["last_fingerprint"].get(signal_key) == fingerprint:
        return False, "same candle"

    # 2) منع إعادة التنبيه قبل انتهاء الـ cooldown
    last_time = float(state["last_sent_at"].get(signal_key, 0))
    if now - last_time < cooldown_seconds:
        return False, "cooldown"

    return True, "ok"


def mark_alert_sent(state, signal_key, fingerprint, now):
    state["last_sent_at"][signal_key] = now
    state["last_fingerprint"][signal_key] = fingerprint
    save_state(state)


state = load_state()


def run():
    global state

    print("🚀 Bot Started...")

    futures = get_tickers("SWAP")
    print(f"Fetched {len(futures)} futures pairs")

    usdt_pairs = [
        p for p in futures
        if "USDT" in p["instId"]
        and not p["instId"].startswith((
            "USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP"
        ))
    ]

    print(f"USDT pairs: {len(usdt_pairs)}")

    tested = 0
    sent_in_this_run = set()

    for pair_data in usdt_pairs[:200]:
        tested += 1
        symbol = pair_data["instId"]

        try:
            candles = get_candles(symbol, "15m", 100)
            df = to_dataframe(candles)

            if df.empty:
                print(f"{symbol} → empty dataframe")
                continue

            df = add_ma(df)
            df = add_rsi(df)
            df = add_atr(df)

            signal = early_bullish_signal(df)
            volume_spike = is_volume_spike(df, multiplier=1.2)

            if signal:
                score = calculate_long_score(df)

                # لو مفيش volume spike نقلل السكور بدل ما نرفض الإشارة نهائيًا
                if not volume_spike:
                    score -= 1.5

                if score < 0:
                    score = 0
            else:
                score = 0

            print(f"{symbol} → signal: {signal} | score: {score} | volume_spike: {volume_spike}")

            # فلترة أولية
            if not signal:
                continue

            if score < 7.5:
                continue

            # لو السكور أقل من 8 لازم يكون فيه volume spike
            if score < 8 and not volume_spike:
                continue

            price = df["close"].iloc[-1]
            now = time.time()

            candle_time = str(df.index[-1])
            signal_key = f"{symbol}_long"
            fingerprint = f"{signal_key}_{candle_time}"

            # حماية إضافية داخل نفس الرن
            if fingerprint in sent_in_this_run:
                print(f"{symbol} → skipped (already sent in this run)")
                continue

            allowed, reason = should_send_alert(
                state=state,
                signal_key=signal_key,
                fingerprint=fingerprint,
                now=now,
                cooldown_seconds=COOLDOWN_SECONDS,
            )

            if not allowed:
                print(f"{symbol} → skipped ({reason})")
                continue

            volume_line = "💥 Volume Spike" if volume_spike else "📊 Volume عادي"

            message = f"""🚀 لونج فيوتشر

{symbol}

💰 {price}
⏱ 15m

⭐ {score} / 10
🪙 BTC: --

📊 إشارة لونج أولية
{volume_line}
🔥 Long detected
"""

            send_telegram_message(message)

            mark_alert_sent(
                state=state,
                signal_key=signal_key,
                fingerprint=fingerprint,
                now=now,
            )

            sent_in_this_run.add(fingerprint)

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    run()
