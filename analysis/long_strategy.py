def early_bullish_signal(df):
    """
    استراتيجية لونج خفيفة ومناسبة لفحص عدد كبير من الأزواج (200+).
    تلتقط بداية الحركة الصاعدة، والـ score يكمل الفلترة.
    """

    try:
        if df is None or df.empty or len(df) < 20:
            return False

        last = df.iloc[-1]
        prev = df.iloc[-2]

        score = 0

        # 1️⃣ السعر فوق الموفنج
        if last["close"] > last["ma"]:
            score += 1

        # 2️⃣ RSI فوق 50 (بداية صعود)
        if last["rsi"] > 50:
            score += 1

        # 3️⃣ ATR بيزيد (حركة)
        if last["atr"] >= prev["atr"]:
            score += 1

        # 4️⃣ شمعة خضراء
        if last["close"] > last["open"]:
            score += 1

        # 5️⃣ حجم أعلى من السابق
        if last["volume"] > prev["volume"]:
            score += 1

        # 6️⃣ مومنتوم بسيط
        if last["close"] > prev["close"]:
            score += 1

        return score >= 4

    except Exception as e:
        print(f"Strategy error: {e}")
        return False
