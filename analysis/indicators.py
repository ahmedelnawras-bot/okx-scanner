import pandas as pd


OHLCV_COLUMNS = [
    "ts", "open", "high", "low", "close",
    "volume", "volCcy", "volCcyQuote", "confirm"
]


def _ensure_numeric(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    """Safely convert numeric market columns without breaking on bad API values."""
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    columns = columns or ["open", "high", "low", "close", "volume"]
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def to_dataframe(candles):
    """
    Convert OKX candle rows to a normalized DataFrame.

    OKX can occasionally return rows with missing/extra values or string numbers.
    This helper keeps the public behavior compatible while making conversion safer.
    """
    if not candles:
        return pd.DataFrame()

    normalized_rows = []
    for row in candles:
        if row is None:
            continue
        row = list(row)
        if len(row) < len(OHLCV_COLUMNS):
            row = row + [None] * (len(OHLCV_COLUMNS) - len(row))
        elif len(row) > len(OHLCV_COLUMNS):
            row = row[:len(OHLCV_COLUMNS)]
        normalized_rows.append(row)

    if not normalized_rows:
        return pd.DataFrame()

    df = pd.DataFrame(normalized_rows, columns=OHLCV_COLUMNS)
    df = _ensure_numeric(df)

    # Drop unusable OHLC rows, but keep partial indicator NaNs for rolling windows.
    required = ["open", "high", "low", "close"]
    existing_required = [c for c in required if c in df.columns]
    if existing_required:
        df = df.dropna(subset=existing_required)

    if "volume" in df.columns:
        df["volume"] = df["volume"].fillna(0.0)

    # OKX market candles are usually newest-first. Use chronological order for rolling indicators.
    if "ts" in df.columns and not df.empty:
        try:
            df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
            df = df.sort_values("ts").reset_index(drop=True)
        except Exception:
            df = df.reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    return df


def add_ma(df, period=20):
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    df = df.copy()
    df = _ensure_numeric(df, ["close"])
    period = max(int(period or 20), 1)
    df[f"ma{period}"] = df["close"].rolling(window=period, min_periods=period).mean()
    if period == 20 and "ma20" not in df.columns:
        df["ma20"] = df[f"ma{period}"]
    return df


def add_rsi(df, period=14):
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    df = df.copy()
    df = _ensure_numeric(df, ["close"])
    period = max(int(period or 14), 1)

    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    df["rsi"] = 100 - (100 / (1 + rs))
    df["rsi"] = df["rsi"].fillna(50.0)
    return df


def add_atr(df, period=14):
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    df = df.copy()
    df = _ensure_numeric(df, ["high", "low", "close"])
    period = max(int(period or 14), 1)

    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()

    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr"] = true_range.rolling(window=period, min_periods=period).mean()
    return df
