import time
import hmac
import base64
import hashlib
import json
import requests
from urllib.parse import urlencode

from execution.config import (
    OKX_API_KEY,
    OKX_API_SECRET,
    OKX_PASSPHRASE,
    OKX_SIMULATED,
    OKX_BASE_URL,
    REQUEST_TIMEOUT,
)


class OKXTradeClient:
    def __init__(self):
        self.api_key = OKX_API_KEY
        self.api_secret = OKX_API_SECRET
        self.passphrase = OKX_PASSPHRASE
        self.simulated = OKX_SIMULATED
        self.base_url = OKX_BASE_URL.rstrip("/")

    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret and self.passphrase)

    def _timestamp(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _headers(self, timestamp: str, method: str, request_path: str, body: str = "") -> dict:
        return {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, request_path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
            "flag": self.simulated,
        }

    def _request(self, method: str, path: str, params: dict = None, body: dict = None) -> dict:
        if not self.has_credentials():
            return {
                "ok": False,
                "code": "missing_credentials",
                "msg": "OKX API credentials are missing",
                "data": [],
            }

        query = ""
        if params:
            query = "?" + urlencode(params)

        request_path = path + query
        url = self.base_url + request_path
        body_str = json.dumps(body, separators=(",", ":")) if body else ""

        timestamp = self._timestamp()
        headers = self._headers(timestamp, method, request_path, body_str)

        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                data=body_str if body else None,
                timeout=REQUEST_TIMEOUT,
            )

            try:
                payload = response.json()
            except Exception:
                return {
                    "ok": False,
                    "code": "invalid_json",
                    "msg": response.text[:300],
                    "status_code": response.status_code,
                    "data": [],
                }

            return {
                "ok": payload.get("code") == "0",
                "code": payload.get("code"),
                "msg": payload.get("msg", ""),
                "status_code": response.status_code,
                "data": payload.get("data", []),
                "raw": payload,
            }

        except Exception as e:
            return {
                "ok": False,
                "code": "request_error",
                "msg": str(e),
                "data": [],
            }

    def test_connection(self) -> dict:
        return self.get_balance()

    def get_balance(self) -> dict:
        return self._request("GET", "/api/v5/account/balance")

    def get_positions(self, inst_type: str = "SWAP") -> dict:
        return self._request(
            "GET",
            "/api/v5/account/positions",
            params={"instType": inst_type},
        )

    def get_open_positions_count(self, inst_type: str = "SWAP") -> int:
        res = self.get_positions(inst_type=inst_type)
        if not res.get("ok"):
            return 0
        positions = res.get("data", []) or []
        count = 0
        for p in positions:
            try:
                if abs(float(p.get("pos", 0))) > 0:
                    count += 1
            except Exception:
                continue
        return count

    def place_algo_stop_loss(
        self,
        inst_id: str,
        side: str,
        stop_price: float,
        size: str = "",
        td_mode: str = "cross",
        pos_side: str = "long",
        reduce_only: bool = True,
    ) -> dict:
        """Place a reduce-only conditional SL algo order on OKX.

        This is intentionally narrow and defensive. It is used by BLOCK_LONGS
        protection after tracking decides that a protected SL/trailing floor
        should be pushed to the platform.
        """
        try:
            inst_id = str(inst_id or "").strip()
            if not inst_id:
                return {"ok": False, "code": "missing_inst_id", "msg": "instId is required", "data": []}
            stop_price = float(stop_price or 0.0)
            if stop_price <= 0:
                return {"ok": False, "code": "invalid_stop_price", "msg": "stop price must be positive", "data": []}

            close_side = str(side or "sell").strip().lower()
            if close_side not in ("buy", "sell"):
                close_side = "sell"

            body = {
                "instId": inst_id,
                "tdMode": str(td_mode or "cross"),
                "side": close_side,
                "ordType": "conditional",
                "slTriggerPx": str(stop_price),
                "slOrdPx": "-1",
                "reduceOnly": "true" if reduce_only else "false",
            }
            if size:
                body["sz"] = str(size)
            if pos_side:
                body["posSide"] = str(pos_side)
            return self._request("POST", "/api/v5/trade/order-algo", body=body)
        except Exception as e:
            return {"ok": False, "code": "place_algo_stop_loss_error", "msg": str(e), "data": []}

    def protect_long_position_sl(
        self,
        inst_id: str,
        stop_price: float,
        size: str = "",
        td_mode: str = "cross",
        pos_side: str = "long",
    ) -> dict:
        """Convenience wrapper for long-position protection.

        For a long, protection exits with side=sell. In simulated OKX mode this
        still sends to OKX demo if credentials/flag are configured; callers can
        choose not to call this at all when they want tracking-only behavior.
        """
        return self.place_algo_stop_loss(
            inst_id=inst_id,
            side="sell",
            stop_price=stop_price,
            size=size,
            td_mode=td_mode,
            pos_side=pos_side,
            reduce_only=True,
        )

    def protect_short_position_sl(
        self,
        inst_id: str,
        stop_price: float,
        size: str = "",
        td_mode: str = "cross",
        pos_side: str = "short",
    ) -> dict:
        return self.place_algo_stop_loss(
            inst_id=inst_id,
            side="buy",
            stop_price=stop_price,
            size=size,
            td_mode=td_mode,
            pos_side=pos_side,
            reduce_only=True,
        )
