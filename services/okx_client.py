import requests

BASE_URL = "https://www.okx.com/api/v5/market"


# =========================
# 🔹 Get Tickers (كل العملات)
# =========================
def get_tickers(inst_type="SWAP"):
    url = f"{BASE_URL}/tickers?instType={inst_type}"
    res = requests.get(url)
    data = res.json()

    if data["code"] != "0":
        return []

    return data["data"]


# =========================
# 🔹 Get Candles (الشموع)
# =========================
def get_candles(inst_id, timeframe="15m", limit=100):
    url = f"{BASE_URL}/candles"

    params = {
        "instId": inst_id,
        "bar": timeframe,
        "limit": limit
    }

    res = requests.get(url, params=params)
    data = res.json()

    if data["code"] != "0":
        return []

    return data["data"]
