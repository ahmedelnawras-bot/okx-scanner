"""Minimal OKX REST client for paper/simulated trading.

Safety rules:
- Paper/simulated orders only by default.
- Live orders are blocked unless allow_live_trading=True and simulated=False.
- Compatible with main.py v119 constructor arguments.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


@dataclass
class OKXCredentials:
    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""
    simulated: bool = True


class OKXTradeClient:
    def __init__(
        self,
        credentials: OKXCredentials | None = None,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        simulated: bool = True,
        allow_live_trading: bool = False,
        base_url: str = "https://www.okx.com",
        timeout: int = 15,
    ):
        if credentials is None:
            credentials = OKXCredentials(
                api_key=api_key,
                api_secret=api_secret,
                passphrase=passphrase,
                simulated=simulated,
            )

        self.credentials = credentials
        self.allow_live_trading = bool(allow_live_trading)
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    @property
    def configured(self) -> bool:
        return bool(
            self.credentials.api_key
            and self.credentials.api_secret
            and self.credentials.passphrase
        )

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        message = f"{timestamp}{method.upper()}{request_path}{body}"
        mac = hmac.new(
            self.credentials.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _headers(self, timestamp: str, method: str, request_path: str, body: str = "") -> dict[str, str]:
        headers = {
            "OK-ACCESS-KEY": self.credentials.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, request_path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.credentials.passphrase,
            "Content-Type": "application/json",
        }

        if self.credentials.simulated:
            headers["x-simulated-trading"] = "1"

        return headers

    def get_balance(self) -> dict[str, Any]:
        if not self.configured:
            return {"ok": False, "error": "okx_not_configured"}

        path = "/api/v5/account/balance"
        ts = self._timestamp()

        try:
            response = requests.get(
                f"{self.base_url}{path}",
                headers=self._headers(ts, "GET", path),
                timeout=self.timeout,
            )
            return response.json()
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def _normalize_order_response(self, response: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        ok = str(response.get("code", "")) == "0"
        first = (response.get("data") or [{}])[0] if isinstance(response.get("data"), list) else {}
        reason = (
            first.get("sMsg")
            or response.get("msg")
            or first.get("ordId")
            or ("accepted" if ok else "okx_order_rejected")
        )
        return {
            "ok": ok,
            "simulated": self.credentials.simulated,
            "reason": reason,
            "order_id": first.get("ordId"),
            "payload": payload,
            "response": response,
        }

    def place_market_order(
        self,
        inst_id: str,
        side: str,
        sz: str,
        td_mode: str = "cross",
        pos_side: str = "long",
    ) -> dict[str, Any]:
        if not self.configured:
            return {"ok": False, "simulated": self.credentials.simulated, "reason": "okx_not_configured"}

        if not self.credentials.simulated and not self.allow_live_trading:
            return {
                "ok": False,
                "simulated": False,
                "reason": "live_trading_blocked_ALLOW_LIVE_TRADING_0",
            }

        path = "/api/v5/trade/order"
        payload = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "market",
            "sz": str(sz),
        }

        if pos_side:
            payload["posSide"] = pos_side

        body = json.dumps(payload, separators=(",", ":"))
        ts = self._timestamp()

        try:
            response = requests.post(
                f"{self.base_url}{path}",
                data=body,
                headers=self._headers(ts, "POST", path, body),
                timeout=self.timeout,
            )
            try:
                data = response.json()
            except Exception:
                data = {"code": str(response.status_code), "msg": response.text}
            return self._normalize_order_response(data, payload)
        except Exception as exc:
            return {"ok": False, "simulated": self.credentials.simulated, "reason": str(exc)}

    def place_market_long(
        self,
        inst_id: str,
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        td_mode: str = "cross",
    ) -> dict[str, Any]:
        """Compatibility wrapper used by main.py for long execution candidates.

        Keeps main.py stable and converts margin/leverage into a conservative
        approximate order size. OKX may still validate contract sizing rules.
        """
        try:
            notional = max(float(margin_usdt), 0.0) * max(int(leverage), 1)
            price = max(float(entry_price), 0.0)
            if price <= 0 or notional <= 0:
                return {"ok": False, "simulated": self.credentials.simulated, "reason": "invalid_entry_or_margin"}
            sz = f"{notional / price:.6f}"
        except Exception as exc:
            return {"ok": False, "simulated": self.credentials.simulated, "reason": f"size_build_failed: {exc}"}

        return self.place_market_order(
            inst_id=inst_id,
            side="buy",
            sz=sz,
            td_mode=td_mode,
            pos_side="long",
        )
