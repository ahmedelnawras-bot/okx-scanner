# analysis/entry_maturity.py

"""
Entry Maturity Analysis

الغرض:
تحليل نضج نقطة الدخول ومنع الدخول المتأخر جدًا في نهاية الموجة.

الملف مستقل:
- لا يستخدم Redis
- لا يستخدم requests
- لا يستخدم أي مكتبات خارج pandas
- آمن مع البيانات الناقصة
"""

import pandas as pd


def safe_float(value, default=0.0):
    """
    تحويل آمن إلى float.
    يحمي من None و NaN والقيم غير الرقمية.
    """
    try:
        if value is None:
            return default
        if value != value:  # NaN check
            return default
        return float(value)
    except Exception:
        return default


def get_signal_row(df):
    """
    اختيار صف الإشارة بنفس منطق main.py تقريبًا.

    - لو df None أو empty يرجع None
    - لو confirm موجود:
      - لو آخر صف confirm == 1 استخدم آخر صف
      - غير ذلك استخدم قبل الأخير
    - لو confirm غير موجود استخدم قبل الأخير
    - fallback آمن
    """
    try:
        if df is None or df.empty:
            return None

        if len(df) == 1:
            return df.iloc[-1]

        if "confirm" in df.columns:
            last = df.iloc[-1]
            confirm_value = safe_float(last.get("confirm"), 0.0)

            if int(confirm_value) == 1:
                return last

            return df.iloc[-2]

        return df.iloc[-2]

    except Exception:
        try:
            if df is not None and not df.empty:
                if len(df) >= 2:
                    return df.iloc[-2]
                return df.iloc[-1]
        except Exception:
            pass

        return None


def default_entry_maturity_result() -> dict:
    """
    النتيجة الافتراضية الآمنة.
    """
    return {
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


def get_fib_position(df, lookback=50) -> dict:
    """
    تحسب موقع السعر من آخر range.

    high = أعلى high آخر lookback
    low = أقل low آخر lookback
    close = close صف الإشارة
    position = (close - low) / (high - low)
    """
    try:
        result = {
            "fib_position": "unknown",
            "fib_position_ratio": 0.0,
            "fib_label": "غير معروف",
        }

        if df is None or df.empty:
            return result

        required_cols = {"high", "low", "close"}
        if not required_cols.issubset(set(df.columns)):
            return result

        signal_row = get_signal_row(df)
        if signal_row is None:
            return result

        work = df.tail(int(lookback)).copy()
        if work.empty:
            return result

        high = safe_float(work["high"].max(), 0.0)
        low = safe_float(work["low"].min(), 0.0)
        close = safe_float(signal_row.get("close"), 0.0)

        range_ = high - low

        if high <= 0 or low <= 0 or close <= 0 or range_ <= 0:
            return result

        position = (close - low) / range_
        position = max(0.0, min(1.5, position))

        if 0.30 <= position <= 0.65:
            fib_position = "golden_zone"
            fib_label = "🟢 Golden Zone"
        elif position > 0.85:
            fib_position = "overextended"
            fib_label = "🔴 ممتد جدًا"
        elif position < 0.25:
            fib_position = "too_early"
            fib_label = "⚠️ مبكر جدًا"
        else:
            fib_position = "acceptable"
            fib_label = "🟡 مقبول"

        return {
            "fib_position": fib_position,
            "fib_position_ratio": round(float(position), 4),
            "fib_label": fib_label,
        }

    except Exception:
        return {
            "fib_position": "unknown",
            "fib_position_ratio": 0.0,
            "fib_label": "غير معروف",
        }


def had_recent_pullback(df, bars=14) -> dict:
    """
    يكتشف هل السعر عمل Pullback صحي قبل الدخول.

    منطق آمن:
    - استخدم آخر bars من close
    - peak = max أول 70% من النافذة
    - trough = min آخر 50% قبل شمعة الإشارة
    - current = close صف الإشارة
    - pullback_pct = (peak - trough) / peak * 100
    - recovered = current > trough
    - has_pullback = 1.0 <= pullback_pct <= 8.0 and recovered
    """
    try:
        result = {
            "had_pullback": False,
            "pullback_pct": 0.0,
            "pullback_label": "غير معروف",
        }

        if df is None or df.empty or "close" not in df.columns:
            return result

        signal_row = get_signal_row(df)
        if signal_row is None:
            return result

        work = df.tail(int(bars)).copy()
        if work.empty or len(work) < 6:
            return result

        closes = pd.to_numeric(work["close"], errors="coerce").dropna()
        if len(closes) < 6:
            return result

        window_len = len(closes)

        first_part_end = max(2, int(window_len * 0.70))
        second_part_start = max(0, int(window_len * 0.50))
        second_part_end = max(second_part_start + 1, window_len - 1)

        first_part = closes.iloc[:first_part_end]
        pullback_zone = closes.iloc[second_part_start:second_part_end]

        if first_part.empty or pullback_zone.empty:
            return result

        peak = safe_float(first_part.max(), 0.0)
        trough = safe_float(pullback_zone.min(), 0.0)
        current = safe_float(signal_row.get("close"), safe_float(closes.iloc[-1], 0.0))

        if peak <= 0 or trough <= 0 or current <= 0:
            return result

        pullback_pct = ((peak - trough) / peak) * 100.0
        pullback_pct = max(0.0, pullback_pct)

        recovered = current > trough
        has_pullback = 1.0 <= pullback_pct <= 8.0 and recovered

        if has_pullback:
            label = "✅ Pullback صحي"
        elif pullback_pct < 1.0:
            label = "⚠️ بدون Pullback واضح"
        elif pullback_pct > 8.0:
            label = "⚠️ Pullback عميق/ضعف"
        else:
            label = "🟡 Pullback محدود"

        return {
            "had_pullback": bool(has_pullback),
            "pullback_pct": round(float(pullback_pct), 2),
            "pullback_label": label,
        }

    except Exception:
        return {
            "had_pullback": False,
            "pullback_pct": 0.0,
            "pullback_label": "غير معروف",
        }


def estimate_wave_position(df, lookback=30) -> dict:
    """
    تقدير بسيط للموجة بعدد القمم المحلية.

    peak لو:
    high[i] > high[i-1] and high[i] > high[i+1]

    تحسين:
    - تجاهل القمم الضعيفة جدًا
    - القمة لازم تكون أعلى من median(highs)
    - ولازم تكون أعلى من متوسط الجيران بنسبة بسيطة
    """
    try:
        result = {
            "wave_estimate": 0,
            "wave_peaks": 0,
            "wave_label": "غير معروف",
        }

        if df is None or df.empty or "high" not in df.columns:
            return result

        work = df.tail(int(lookback)).copy()
        if work.empty or len(work) < 6:
            return result

        highs = pd.to_numeric(work["high"], errors="coerce").dropna()
        if len(highs) < 6:
            return result

        median_high = safe_float(highs.median(), 0.0)
        peaks = 0

        for i in range(1, len(highs) - 1):
            prev_high = safe_float(highs.iloc[i - 1], 0.0)
            current_high = safe_float(highs.iloc[i], 0.0)
            next_high = safe_float(highs.iloc[i + 1], 0.0)

            if prev_high <= 0 or current_high <= 0 or next_high <= 0:
                continue

            neighbors_avg = (prev_high + next_high) / 2.0

            is_local_peak = current_high > prev_high and current_high > next_high
            is_above_median = current_high >= median_high
            is_meaningful_peak = current_high >= neighbors_avg * 1.001

            if is_local_peak and is_above_median and is_meaningful_peak:
                peaks += 1

        if peaks <= 1:
            wave_estimate = 1
            wave_label = "🟢 بداية حركة"
        elif peaks == 2:
            wave_estimate = 3
            wave_label = "🟢 موجة 3 محتملة"
        else:
            wave_estimate = 5
            wave_label = "🔴 موجة 5 / قرب نهاية الحركة"

        return {
            "wave_estimate": int(wave_estimate),
            "wave_peaks": int(peaks),
            "wave_label": wave_label,
        }

    except Exception:
        return {
            "wave_estimate": 0,
            "wave_peaks": 0,
            "wave_label": "غير معروف",
        }


def analyze_entry_maturity(df) -> dict:
    """
    الدالة الرئيسية التي سيستوردها main.py.

    تجمع:
    - Fibonacci Position
    - Pullback Confirmation
    - Wave Estimate

    وتخرج قرار موحد:
    early | healthy | late | danger_late | unknown
    """
    try:
        base = default_entry_maturity_result()

        fib = get_fib_position(df)
        pullback = had_recent_pullback(df)
        wave = estimate_wave_position(df)

        result = {
            **base,
            **fib,
            **pullback,
            **wave,
        }

        fib_position = result.get("fib_position", "unknown")
        had_pb = bool(result.get("had_pullback", False))
        wave_estimate = int(result.get("wave_estimate", 0) or 0)

        warning_reasons = []

        # A) Overextended + Wave 5 = خطر واضح
        if fib_position == "overextended" and wave_estimate == 5:
            result["entry_maturity"] = "danger_late"
            result["maturity_penalty"] = 0.75
            result["maturity_bonus"] = 0.0
            result["block_signal"] = True
            warning_reasons.append("Entry Maturity: موجة متأخرة + امتداد فيبوناتشي")

        # B) Overextended فقط
        elif fib_position == "overextended":
            result["entry_maturity"] = "late"
            result["maturity_penalty"] = 0.35
            result["maturity_bonus"] = 0.0
            result["block_signal"] = False
            warning_reasons.append("Entry Maturity: السعر قريب من نهاية الموجة")

        # C) موجة خامسة بدون Pullback واضح
        elif wave_estimate == 5 and not had_pb:
            result["entry_maturity"] = "late"
            result["maturity_penalty"] = 0.30
            result["maturity_bonus"] = 0.0
            result["block_signal"] = False
            warning_reasons.append("Entry Maturity: موجة خامسة بدون Pullback واضح")

        # D) أفضل حالة
        elif fib_position == "golden_zone" and had_pb and wave_estimate in (1, 3):
            result["entry_maturity"] = "healthy"
            result["maturity_penalty"] = 0.0
            result["maturity_bonus"] = 0.15
            result["block_signal"] = False

        # E) مبكر جدًا
        elif fib_position == "too_early":
            result["entry_maturity"] = "early"
            result["maturity_penalty"] = 0.0
            result["maturity_bonus"] = 0.05
            result["block_signal"] = False
            warning_reasons.append("Entry Maturity: مبكر جدًا، يحتاج تأكيد")

        # F) fallback
        else:
            result["entry_maturity"] = "unknown"
            result["maturity_penalty"] = 0.0
            result["maturity_bonus"] = 0.0
            result["block_signal"] = False

        result["warning_reasons"] = warning_reasons

        return result

    except Exception:
        return default_entry_maturity_result()


if __name__ == "__main__":
    sample = pd.DataFrame(
        {
            "open": [100, 101, 102, 103, 104, 103, 102, 103, 104, 105, 106, 105, 106, 107],
            "high": [101, 102, 103, 104, 105, 104, 103, 104, 105, 106, 107, 106, 107, 108],
            "low": [99, 100, 101, 102, 103, 102, 101, 102, 103, 104, 105, 104, 105, 106],
            "close": [101, 102, 103, 104, 103, 102, 103, 104, 105, 106, 105, 106, 107, 108],
            "confirm": [1] * 14,
        }
    )

    print(analyze_entry_maturity(sample))
