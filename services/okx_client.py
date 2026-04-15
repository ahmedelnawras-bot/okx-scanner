import requests

BASE_URL = "https://www.okx.com/api/v5/market"

def get_tickers(inst_type="SWAP"):
    url = f"{BASE_URL}/tickers?instType={inst_type}"
    res = requests.get(url)
    data = res.json()

    if data["code"] != "0":
        return []

    return data["data"]
