import time
import hmac
import base64
import hashlib
import json
import requests

from execution.config import (
    OKX_API_KEY,
    OKX_API_SECRET,
    OKX_PASSPHRASE,
    OKX_SIMULATED,
)

OKX_BASE_URL = "https://www.okx.com"


class OKXTradeClient:
    def __init__(self):
        self.api_key = OKX_API_KEY
        self.api_secret = OKX_API_SECRET
        self.passphrase = OKX_PASSPHRASE
        self.simulated = OKX_SIMULATED

    def _timestamp(self):
        return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

    def _sign(self, timestamp, method, request_path, body=""):
        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self.api_secret.encode(),
            message.encode(),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode()

    def _headers(self, timestamp, method, request_path, body=""):
        return {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, request_path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
            "flag": self.simulated,
        }

    def _request(self, method, path, params=None, body=None):
        if not self.api_key:
            return {"ok": False, "error": "Missing API"}

        query = ""
        if params:
            query = "?" + "&".join(f"{k}={v}" for k, v in params.items())

        request_path = path + query
        url = OKX_BASE_URL + request_path

        body_str = json.dumps(body) if body else ""
        ts = self._timestamp()
        headers = self._headers(ts, method, request_path, body_str)

        try:
            res = requests.request(method, url, headers=headers, data=body_str)
            return res.json()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def get_balance(self):
        return self._request("GET", "/api/v5/account/balance")

    def get_positions(self):
        return self._request("GET", "/api/v5/account/positions")


if __name__ == "__main__":
    client = OKXTradeClient()
    print(client.get_balance())
    print(client.get_positions())
