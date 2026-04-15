import pandas as pd

def prepare_dataframe(candles):
    df = pd.DataFrame(candles, columns=[
        "ts", "o", "h", "l", "c", "v",
        "v_quote", "confirm"
    ])

    df = df.astype({
        "o": float,
        "h": float,
        "l": float,
        "c": float,
        "v": float
    })

    df = df.iloc[::-1]
    df.reset_index(drop=True, inplace=True)

    return df


def add_ma(df, period=20):
    df[f"ma{period}"] = df["c"].rolling(period).mean()
    return df


def add_rsi(df, period=14):
    delta = df["c"].diff()

    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()

    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    return df


def add_volume_avg(df, period=20):
    df["avg_vol"] = df["v"].rolling(period).mean()
    return df
