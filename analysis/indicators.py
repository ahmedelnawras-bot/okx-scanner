import pandas as pd


def to_dataframe(candles):
    df = pd.DataFrame(candles)

    df = df.rename(columns={
        "ts": "time",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "vol": "volume"
    })

    df["close"] = df["close"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["volume"] = df["volume"].astype(float)

    return df


def add_ma(df, period=20):
    df[f"ma_{period}"] = df["close"].rolling(window=period).mean()
    return df


def add_rsi(df, period=14):
    delta = df["close"].diff()

    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    return df


def add_atr(df, period=14):
    df["tr"] = df["high"] - df["low"]
    df["atr"] = df["tr"].rolling(window=period).mean()
    return df
