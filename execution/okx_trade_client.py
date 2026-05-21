
"""OKX REST client with managed-entry helpers for live/paper trading.

Goals of this version:
- Keep full backward compatibility with the old `place_market_long(...)`.
- Add support for attached stop loss on entry.
- Add explicit helpers for TP1 / TP2 partial exits.
- Add trailing-runner helpers for the last 20%.
- Add stop-loss amendment helpers for block protection / post-TP updates.

Notes:
- This file only provides exchange primitives.
- Strategy state transitions still belong in main/lifecycle/tracking.
- Live trading remains blocked unless allow_live_trading=True and simulated=False.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import uuid
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

    def _trade_guard_error(self) -> dict[str, Any] | None:
        if not self.configured:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "okx_not_configured",
            }

        if not self.credentials.simulated and not self.allow_live_trading:
            return {
                "ok": False,
                "simulated": False,
                "reason": "live_trading_blocked_ALLOW_LIVE_TRADING_0",
            }

        return None

    def _client_id(self, prefix: str) -> str:
        raw = uuid.uuid4().hex[:18]
        safe_prefix = "".join(ch for ch in str(prefix or "bot") if ch.isalnum())[:10] or "bot"
        return f"{safe_prefix}{raw}"[:32]

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ts = self._timestamp()
        body = json.dumps(payload, separators=(",", ":")) if payload is not None else ""
        url = f"{self.base_url}{path}"

        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                params=params,
                data=body if payload is not None else None,
                headers=self._headers(ts, method.upper(), path, body),
                timeout=self.timeout,
            )
            try:
                data = response.json()
            except Exception:
                data = {"code": str(response.status_code), "msg": response.text}
            return data
        except Exception as exc:
            return {"code": "-1", "msg": str(exc), "data": []}

    def _normalize_trade_response(
        self,
        response: dict[str, Any],
        payload: dict[str, Any],
        *,
        id_key: str,
        response_kind: str,
    ) -> dict[str, Any]:
        ok = str(response.get("code", "")) == "0"
        rows = response.get("data") or []
        first = rows[0] if isinstance(rows, list) and rows else {}
        reason = (
            first.get("sMsg")
            or response.get("msg")
            or first.get(id_key)
            or ("accepted" if ok else f"okx_{response_kind}_rejected")
        )
        result = {
            "ok": ok,
            "simulated": self.credentials.simulated,
            "reason": reason,
            "payload": payload,
            "response": response,
        }
        if id_key == "ordId":
            result["order_id"] = first.get("ordId")
            if first.get("clOrdId"):
                result["client_order_id"] = first.get("clOrdId")
        elif id_key == "algoId":
            result["algo_id"] = first.get("algoId")
            if first.get("algoClOrdId"):
                result["algo_client_order_id"] = first.get("algoClOrdId")
        return result

    def _build_position_size(self, entry_price: float, margin_usdt: float, leverage: int) -> str:
        notional = max(float(margin_usdt), 0.0) * max(int(leverage), 1)
        price = max(float(entry_price), 0.0)
        if price <= 0 or notional <= 0:
            raise ValueError("invalid_entry_or_margin")
        return f"{notional / price:.6f}"

    def _build_attached_stop_loss(
        self,
        sl_trigger_px: float,
        *,
        sl_ord_px: str = "-1",
        attach_algo_cl_ord_id: str | None = None,
    ) -> list[dict[str, str]]:
        trigger = float(sl_trigger_px)
        if trigger <= 0:
            raise ValueError("invalid_sl_trigger_px")

        item: dict[str, str] = {
            "slTriggerPx": f"{trigger:.10f}".rstrip("0").rstrip("."),
            "slOrdPx": str(sl_ord_px),
        }
        if attach_algo_cl_ord_id:
            item["attachAlgoClOrdId"] = str(attach_algo_cl_ord_id)[:32]
        return [item]

    def get_balance(self) -> dict[str, Any]:
        if not self.configured:
            return {"ok": False, "error": "okx_not_configured"}

        path = "/api/v5/account/balance"
        response = self._request("GET", path)
        return response

    def place_market_order(
        self,
        inst_id: str,
        side: str,
        sz: str,
        td_mode: str = "cross",
        pos_side: str = "long",
        *,
        reduce_only: bool | None = None,
        cl_ord_id: str | None = None,
        attach_algo_ords: list[dict[str, Any]] | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        guard = self._trade_guard_error()
        if guard:
            return guard

        path = "/api/v5/trade/order"
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "market",
            "sz": str(sz),
        }

        if pos_side:
            payload["posSide"] = pos_side
        if reduce_only is not None:
            payload["reduceOnly"] = str(bool(reduce_only)).lower()
        if cl_ord_id:
            payload["clOrdId"] = str(cl_ord_id)[:32]
        if tag:
            payload["tag"] = str(tag)[:16]
        if attach_algo_ords:
            payload["attachAlgoOrds"] = attach_algo_ords

        response = self._request("POST", path, payload=payload)
        return self._normalize_trade_response(response, payload, id_key="ordId", response_kind="order")

    def place_limit_order(
        self,
        inst_id: str,
        side: str,
        sz: str,
        px: float,
        td_mode: str = "cross",
        pos_side: str = "long",
        *,
        reduce_only: bool | None = None,
        cl_ord_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        guard = self._trade_guard_error()
        if guard:
            return guard

        price = float(px)
        if price <= 0:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "invalid_limit_price",
            }

        path = "/api/v5/trade/order"
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "limit",
            "px": f"{price:.10f}".rstrip("0").rstrip("."),
            "sz": str(sz),
        }

        if pos_side:
            payload["posSide"] = pos_side
        if reduce_only is not None:
            payload["reduceOnly"] = str(bool(reduce_only)).lower()
        if cl_ord_id:
            payload["clOrdId"] = str(cl_ord_id)[:32]
        if tag:
            payload["tag"] = str(tag)[:16]

        response = self._request("POST", path, payload=payload)
        return self._normalize_trade_response(response, payload, id_key="ordId", response_kind="order")

    def place_market_long(
        self,
        inst_id: str,
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        td_mode: str = "cross",
        *,
        sl_trigger_px: float | None = None,
        sl_ord_px: str = "-1",
        attach_algo_cl_ord_id: str | None = None,
        cl_ord_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        """Backward-compatible wrapper used by main.py.

        Optional new arguments:
        - sl_trigger_px: attach stop loss to entry order.
        - sl_ord_px: stop order price. '-1' keeps market-style stop execution.
        """
        try:
            sz = self._build_position_size(entry_price, margin_usdt, leverage)
        except Exception as exc:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": f"size_build_failed: {exc}",
            }

        attach_algo_ords = None
        if sl_trigger_px is not None:
            try:
                attach_algo_ords = self._build_attached_stop_loss(
                    sl_trigger_px,
                    sl_ord_px=sl_ord_px,
                    attach_algo_cl_ord_id=attach_algo_cl_ord_id,
                )
            except Exception as exc:
                return {
                    "ok": False,
                    "simulated": self.credentials.simulated,
                    "reason": f"invalid_attached_sl: {exc}",
                }

        result = self.place_market_order(
            inst_id=inst_id,
            side="buy",
            sz=sz,
            td_mode=td_mode,
            pos_side="long",
            cl_ord_id=cl_ord_id,
            attach_algo_ords=attach_algo_ords,
            tag=tag,
        )
        result["approx_size"] = sz
        result["entry_price_hint"] = float(entry_price)
        result["requested_leverage"] = int(leverage)
        result["requested_margin_usdt"] = float(margin_usdt)
        return result

    def place_market_long_with_attached_sl(
        self,
        inst_id: str,
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        sl_trigger_px: float,
        td_mode: str = "cross",
        *,
        sl_ord_px: str = "-1",
        attach_algo_cl_ord_id: str | None = None,
        cl_ord_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        return self.place_market_long(
            inst_id=inst_id,
            entry_price=entry_price,
            margin_usdt=margin_usdt,
            leverage=leverage,
            td_mode=td_mode,
            sl_trigger_px=sl_trigger_px,
            sl_ord_px=sl_ord_px,
            attach_algo_cl_ord_id=attach_algo_cl_ord_id,
            cl_ord_id=cl_ord_id,
            tag=tag,
        )

    def place_reduce_only_tp_limit(
        self,
        inst_id: str,
        tp_price: float,
        sz: str,
        td_mode: str = "cross",
        pos_side: str = "long",
        *,
        side: str = "sell",
        cl_ord_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        return self.place_limit_order(
            inst_id=inst_id,
            side=side,
            sz=sz,
            px=tp_price,
            td_mode=td_mode,
            pos_side=pos_side,
            reduce_only=True,
            cl_ord_id=cl_ord_id,
            tag=tag,
        )

    def place_reduce_only_tp_split(
        self,
        inst_id: str,
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        *,
        tp1_price: float,
        tp2_price: float,
        tp1_pct: float = 40.0,
        tp2_pct: float = 40.0,
        td_mode: str = "cross",
        pos_side: str = "long",
        tag: str | None = None,
    ) -> dict[str, Any]:
        try:
            full_sz = float(self._build_position_size(entry_price, margin_usdt, leverage))
        except Exception as exc:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": f"size_build_failed: {exc}",
            }

        tp1_sz = f"{max(full_sz * max(float(tp1_pct), 0.0) / 100.0, 0.0):.6f}"
        tp2_sz = f"{max(full_sz * max(float(tp2_pct), 0.0) / 100.0, 0.0):.6f}"

        tp1_result = self.place_reduce_only_tp_limit(
            inst_id=inst_id,
            tp_price=tp1_price,
            sz=tp1_sz,
            td_mode=td_mode,
            pos_side=pos_side,
            cl_ord_id=self._client_id("tp1"),
            tag=tag,
        )
        tp2_result = self.place_reduce_only_tp_limit(
            inst_id=inst_id,
            tp_price=tp2_price,
            sz=tp2_sz,
            td_mode=td_mode,
            pos_side=pos_side,
            cl_ord_id=self._client_id("tp2"),
            tag=tag,
        )

        return {
            "ok": bool(tp1_result.get("ok")) and bool(tp2_result.get("ok")),
            "simulated": self.credentials.simulated,
            "reason": "tp_split_placed" if tp1_result.get("ok") and tp2_result.get("ok") else "tp_split_partial_or_failed",
            "tp1": tp1_result,
            "tp2": tp2_result,
            "full_size": f"{full_sz:.6f}",
            "tp1_size": tp1_sz,
            "tp2_size": tp2_sz,
            "runner_size": f"{max(full_sz - float(tp1_sz) - float(tp2_sz), 0.0):.6f}",
        }

    def place_reduce_only_market_close(
        self,
        inst_id: str,
        sz: str,
        td_mode: str = "cross",
        pos_side: str = "long",
        *,
        side: str = "sell",
        cl_ord_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        return self.place_market_order(
            inst_id=inst_id,
            side=side,
            sz=sz,
            td_mode=td_mode,
            pos_side=pos_side,
            reduce_only=True,
            cl_ord_id=cl_ord_id,
            tag=tag,
        )

    def place_trailing_runner_stop(
        self,
        inst_id: str,
        side: str,
        sz: str,
        *,
        callback_ratio: float,
        active_px: float | None = None,
        td_mode: str = "cross",
        pos_side: str = "long",
        algo_cl_ord_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        guard = self._trade_guard_error()
        if guard:
            return guard

        ratio = float(callback_ratio)
        if ratio <= 0:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "invalid_callback_ratio",
            }

        path = "/api/v5/trade/order-algo"
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "move_order_stop",
            "sz": str(sz),
        }

        if pos_side:
            payload["posSide"] = pos_side
        payload["callbackRatio"] = f"{ratio:.4f}".rstrip("0").rstrip(".")
        if active_px is not None:
            active = float(active_px)
            if active > 0:
                payload["activePx"] = f"{active:.10f}".rstrip("0").rstrip(".")
        if algo_cl_ord_id:
            payload["algoClOrdId"] = str(algo_cl_ord_id)[:32]
        if tag:
            payload["tag"] = str(tag)[:16]

        response = self._request("POST", path, payload=payload)
        return self._normalize_trade_response(response, payload, id_key="algoId", response_kind="algo_order")

    def amend_attached_stop_loss(
        self,
        inst_id: str,
        ord_id: str,
        new_sl_trigger_px: float,
        *,
        new_sl_ord_px: str = "-1",
        attach_algo_id: str | None = None,
        attach_algo_cl_ord_id: str | None = None,
        req_id: str | None = None,
    ) -> dict[str, Any]:
        guard = self._trade_guard_error()
        if guard:
            return guard

        trigger = float(new_sl_trigger_px)
        if trigger <= 0:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "invalid_new_sl_trigger_px",
            }

        attach_entry: dict[str, Any] = {
            "newSlTriggerPx": f"{trigger:.10f}".rstrip("0").rstrip("."),
            "newSlOrdPx": str(new_sl_ord_px),
        }
        if attach_algo_id:
            attach_entry["attachAlgoId"] = str(attach_algo_id)
        if attach_algo_cl_ord_id:
            attach_entry["attachAlgoClOrdId"] = str(attach_algo_cl_ord_id)[:32]

        path = "/api/v5/trade/amend-order"
        payload: dict[str, Any] = {
            "instId": inst_id,
            "ordId": str(ord_id),
            "attachAlgoOrds": [attach_entry],
        }
        if req_id:
            payload["reqId"] = str(req_id)[:32]

        response = self._request("POST", path, payload=payload)
        return self._normalize_trade_response(response, payload, id_key="ordId", response_kind="amend_order")

    def cancel_order(
        self,
        inst_id: str,
        *,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> dict[str, Any]:
        guard = self._trade_guard_error()
        if guard:
            return guard

        if not ord_id and not cl_ord_id:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "missing_cancel_identifier",
            }

        path = "/api/v5/trade/cancel-order"
        payload: dict[str, Any] = {"instId": inst_id}
        if ord_id:
            payload["ordId"] = str(ord_id)
        if cl_ord_id:
            payload["clOrdId"] = str(cl_ord_id)[:32]

        response = self._request("POST", path, payload=payload)
        return self._normalize_trade_response(response, payload, id_key="ordId", response_kind="cancel_order")

    def cancel_algo_order(
        self,
        inst_id: str,
        algo_id: str,
    ) -> dict[str, Any]:
        guard = self._trade_guard_error()
        if guard:
            return guard

        path = "/api/v5/trade/cancel-algos"
        payload = [{"instId": inst_id, "algoId": str(algo_id)}]
        response = self._request("POST", path, payload=payload)
        return self._normalize_trade_response(response, {"orders": payload}, id_key="algoId", response_kind="cancel_algo")

    def build_managed_trade_plan(
        self,
        inst_id: str,
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        sl_trigger_px: float,
        tp1_price: float,
        tp2_price: float,
        *,
        tp1_pct: float = 40.0,
        tp2_pct: float = 40.0,
        runner_pct: float = 20.0,
    ) -> dict[str, Any]:
        try:
            full_sz = float(self._build_position_size(entry_price, margin_usdt, leverage))
        except Exception as exc:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": f"size_build_failed: {exc}",
            }

        tp1_sz = max(full_sz * max(float(tp1_pct), 0.0) / 100.0, 0.0)
        tp2_sz = max(full_sz * max(float(tp2_pct), 0.0) / 100.0, 0.0)
        runner_sz = max(full_sz * max(float(runner_pct), 0.0) / 100.0, 0.0)

        return {
            "ok": True,
            "simulated": self.credentials.simulated,
            "inst_id": inst_id,
            "entry_price": float(entry_price),
            "full_size": f"{full_sz:.6f}",
            "attached_stop_loss": {
                "slTriggerPx": f"{float(sl_trigger_px):.10f}".rstrip("0").rstrip("."),
                "slOrdPx": "-1",
            },
            "tp1": {
                "price": float(tp1_price),
                "close_pct": float(tp1_pct),
                "size": f"{tp1_sz:.6f}",
            },
            "tp2": {
                "price": float(tp2_price),
                "close_pct": float(tp2_pct),
                "size": f"{tp2_sz:.6f}",
            },
            "runner": {
                "close_pct": float(runner_pct),
                "size": f"{runner_sz:.6f}",
                "requires_trailing_after_tp2": True,
            },
        }
