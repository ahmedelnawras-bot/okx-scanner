import pandas as pd


# =========================
# 🔹 تحويل البيانات لـ DataFrame
# =========================
def to_dataframe(candles):
    df = pd.DataFrame(candles, columns=[
        "ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"
    ])

    df = df.astype(float)
    df = df.sort_values("ts")

    return df


# =========================
# 🔹 Moving Average
# =========================
def add_ma(df, period=20):
    df[f"ma{period}"] = df["c"].rolling(period).mean()
    return df


# =========================
# 🔹 RSI
# =========================
def add_rsi(df, period=14):
    delta = df["c"].diff()

    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()

    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    return df


# =========================
# 🔹 ATR (مهم للستوب)
# =========================
def add_atr(df, period=14):
    high_low = df["h"] - df["l"]
    high_close = (df["h"] - df["c"].shift()).abs()
    low_close = (df["l"] - df["c"].shift()).abs()

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr"] = tr.rolling(period).mean()

    return df
