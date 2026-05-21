"""OKX REST client with managed-entry helpers for live/paper trading.

Enhanced in this revision:
- Keep full backward compatibility with the old `place_market_long(...)`.
- Add support for attached stop loss on entry.
- Add explicit helpers for TP1 / TP2 partial exits.
- Add trailing-runner helpers for the last 20%.
- Add stop-loss amendment helpers for block protection / post-TP updates.
- Add order/algo status + fill confirmation helpers for live sync.
- Add attached-SL sync helpers so internal protected SL can be pushed to OKX.

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
import time
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

    def _read_guard_error(self) -> dict[str, Any] | None:
        if not self.configured:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "okx_not_configured",
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
        payload: dict[str, Any] | list[dict[str, Any]] | None = None,
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

    def _normalize_query_response(
        self,
        response: dict[str, Any],
        *,
        query_kind: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ok = str(response.get("code", "")) == "0"
        rows = response.get("data") or []
        return {
            "ok": ok,
            "simulated": self.credentials.simulated,
            "reason": response.get("msg") or ("ok" if ok else f"okx_{query_kind}_failed"),
            "params": params or {},
            "response": response,
            "rows": rows if isinstance(rows, list) else [],
            "row": rows[0] if isinstance(rows, list) and rows else {},
        }

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

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

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

    # ---------------------------------------------------------------------
    # Read/query helpers for fill confirmation and exchange sync.
    # Based on OKX trade query endpoints.
    # ---------------------------------------------------------------------
    def get_order_details(
        self,
        inst_id: str,
        *,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> dict[str, Any]:
        guard = self._read_guard_error()
        if guard:
            return guard
        if not ord_id and not cl_ord_id:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "missing_order_identifier",
            }

        path = "/api/v5/trade/order"
        params: dict[str, Any] = {"instId": inst_id}
        if ord_id:
            params["ordId"] = str(ord_id)
        if cl_ord_id:
            params["clOrdId"] = str(cl_ord_id)[:32]
        response = self._request("GET", path, params=params)
        return self._normalize_query_response(response, query_kind="order_details", params=params)

    def list_pending_orders(
        self,
        *,
        inst_type: str = "SWAP",
        inst_id: str | None = None,
        ord_type: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        guard = self._read_guard_error()
        if guard:
            return guard

        path = "/api/v5/trade/orders-pending"
        params: dict[str, Any] = {"instType": inst_type}
        if inst_id:
            params["instId"] = inst_id
        if ord_type:
            params["ordType"] = ord_type
        if limit:
            params["limit"] = int(limit)
        response = self._request("GET", path, params=params)
        return self._normalize_query_response(response, query_kind="orders_pending", params=params)

    def get_fills(
        self,
        *,
        inst_id: str | None = None,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        guard = self._read_guard_error()
        if guard:
            return guard

        path = "/api/v5/trade/fills"
        params: dict[str, Any] = {}
        if inst_id:
            params["instId"] = inst_id
        if ord_id:
            params["ordId"] = str(ord_id)
        if cl_ord_id:
            params["clOrdId"] = str(cl_ord_id)[:32]
        if limit:
            params["limit"] = int(limit)
        response = self._request("GET", path, params=params)
        return self._normalize_query_response(response, query_kind="fills", params=params)

    def get_order_fill_summary(
        self,
        inst_id: str,
        *,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> dict[str, Any]:
        details = self.get_order_details(inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
        if not details.get("ok"):
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": details.get("reason") or "order_details_failed",
                "details": details,
            }

        row = details.get("row") or {}
        order_state = str(row.get("state") or row.get("ordState") or "").lower()
        requested_sz = self._safe_float(row.get("sz"))
        filled_sz = self._safe_float(row.get("accFillSz") or row.get("fillSz"))
        avg_fill_price = self._safe_float(row.get("avgPx"))
        fill_ratio = (filled_sz / requested_sz) if requested_sz > 0 else 0.0
        attach_algo_ords = row.get("attachAlgoOrds") or []

        fills = self.get_fills(inst_id=inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id, limit=100)
        fills_rows = fills.get("rows") or [] if isinstance(fills, dict) else []

        return {
            "ok": True,
            "simulated": self.credentials.simulated,
            "reason": "ok",
            "inst_id": inst_id,
            "order_id": row.get("ordId") or ord_id,
            "client_order_id": row.get("clOrdId") or cl_ord_id,
            "state": order_state,
            "requested_size": requested_sz,
            "filled_size": filled_sz,
            "remaining_size": max(requested_sz - filled_sz, 0.0),
            "fill_ratio": fill_ratio,
            "avg_fill_price": avg_fill_price,
            "is_live": order_state in {"live", "partially_filled"},
            "is_filled": order_state == "filled" or (requested_sz > 0 and filled_sz >= requested_sz),
            "is_terminal": order_state in {"filled", "canceled", "mmp_canceled"},
            "is_canceled": order_state in {"canceled", "mmp_canceled"},
            "attach_algo_ords": attach_algo_ords if isinstance(attach_algo_ords, list) else [],
            "details": details,
            "fills": fills_rows,
        }

    def wait_for_order_fill(
        self,
        inst_id: str,
        *,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
        timeout_seconds: float = 15.0,
        poll_interval: float = 1.0,
    ) -> dict[str, Any]:
        deadline = time.time() + max(float(timeout_seconds), 0.5)
        last_summary: dict[str, Any] = {
            "ok": False,
            "reason": "wait_not_started",
            "simulated": self.credentials.simulated,
        }

        while time.time() <= deadline:
            last_summary = self.get_order_fill_summary(inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
            if not last_summary.get("ok"):
                return last_summary
            if last_summary.get("is_filled") or last_summary.get("is_terminal"):
                return last_summary
            time.sleep(max(float(poll_interval), 0.1))

        last_summary = dict(last_summary)
        last_summary["timed_out"] = True
        last_summary["reason"] = "fill_wait_timeout"
        return last_summary

    def list_algo_orders(
        self,
        *,
        ord_type: str = "conditional",
        inst_id: str | None = None,
        algo_id: str | None = None,
        algo_cl_ord_id: str | None = None,
        history: bool = False,
        limit: int | None = None,
    ) -> dict[str, Any]:
        guard = self._read_guard_error()
        if guard:
            return guard

        path = "/api/v5/trade/orders-algo-history" if history else "/api/v5/trade/orders-algo-pending"
        params: dict[str, Any] = {"ordType": ord_type}
        if inst_id:
            params["instId"] = inst_id
        if algo_id:
            params["algoId"] = str(algo_id)
        if algo_cl_ord_id:
            params["algoClOrdId"] = str(algo_cl_ord_id)[:32]
        if limit:
            params["limit"] = int(limit)
        response = self._request("GET", path, params=params)
        return self._normalize_query_response(response, query_kind="algo_orders", params=params)

    def get_algo_order_details(
        self,
        *,
        algo_id: str | None = None,
        algo_cl_ord_id: str | None = None,
        inst_id: str | None = None,
        ord_type: str = "conditional",
    ) -> dict[str, Any]:
        if not algo_id and not algo_cl_ord_id:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "missing_algo_identifier",
            }

        pending = self.list_algo_orders(
            ord_type=ord_type,
            inst_id=inst_id,
            algo_id=algo_id,
            algo_cl_ord_id=algo_cl_ord_id,
            history=False,
            limit=100,
        )
        if pending.get("ok") and pending.get("rows"):
            return {
                "ok": True,
                "simulated": self.credentials.simulated,
                "reason": "ok",
                "source": "pending",
                "rows": pending.get("rows") or [],
                "row": (pending.get("rows") or [None])[0],
                "response": pending.get("response"),
            }

        history = self.list_algo_orders(
            ord_type=ord_type,
            inst_id=inst_id,
            algo_id=algo_id,
            algo_cl_ord_id=algo_cl_ord_id,
            history=True,
            limit=100,
        )
        if history.get("ok") and history.get("rows"):
            return {
                "ok": True,
                "simulated": self.credentials.simulated,
                "reason": "ok",
                "source": "history",
                "rows": history.get("rows") or [],
                "row": (history.get("rows") or [None])[0],
                "response": history.get("response"),
            }

        return {
            "ok": False,
            "simulated": self.credentials.simulated,
            "reason": history.get("reason") or pending.get("reason") or "algo_not_found",
            "pending": pending,
            "history": history,
        }

    def get_attached_stop_from_order(
        self,
        inst_id: str,
        *,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> dict[str, Any]:
        summary = self.get_order_fill_summary(inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
        if not summary.get("ok"):
            return summary

        attach_items = summary.get("attach_algo_ords") or []
        stop_item = None
        if isinstance(attach_items, list):
            for item in attach_items:
                if not isinstance(item, dict):
                    continue
                if item.get("slTriggerPx") or item.get("newSlTriggerPx") or item.get("attachAlgoId"):
                    stop_item = item
                    break

        current_trigger = self._safe_float(
            (stop_item or {}).get("slTriggerPx") or (stop_item or {}).get("newSlTriggerPx")
        )

        return {
            "ok": True,
            "simulated": self.credentials.simulated,
            "reason": "ok",
            "current_sl_trigger_px": current_trigger,
            "attach_algo": stop_item or {},
            "summary": summary,
        }

    def sync_attached_stop_loss(
        self,
        inst_id: str,
        ord_id: str,
        desired_sl_trigger_px: float,
        *,
        current_sl_trigger_px: float | None = None,
        attach_algo_id: str | None = None,
        attach_algo_cl_ord_id: str | None = None,
        min_improvement_px: float = 0.0,
        req_id: str | None = None,
    ) -> dict[str, Any]:
        desired = float(desired_sl_trigger_px)
        if desired <= 0:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "invalid_desired_sl_trigger_px",
            }

        observed_current = current_sl_trigger_px
        observed_attach_algo_id = attach_algo_id
        observed_attach_algo_cl_ord_id = attach_algo_cl_ord_id

        if observed_current is None or observed_current <= 0:
            current_info = self.get_attached_stop_from_order(inst_id, ord_id=ord_id)
            if not current_info.get("ok"):
                return {
                    "ok": False,
                    "simulated": self.credentials.simulated,
                    "reason": current_info.get("reason") or "failed_to_read_current_sl",
                    "current_info": current_info,
                }
            observed_current = self._safe_float(current_info.get("current_sl_trigger_px"))
            attach_item = current_info.get("attach_algo") or {}
            if not observed_attach_algo_id:
                observed_attach_algo_id = attach_item.get("attachAlgoId") or attach_item.get("algoId")
            if not observed_attach_algo_cl_ord_id:
                observed_attach_algo_cl_ord_id = attach_item.get("attachAlgoClOrdId")

        improvement_needed = float(min_improvement_px or 0.0)
        if observed_current > 0 and desired <= (observed_current + improvement_needed):
            return {
                "ok": True,
                "simulated": self.credentials.simulated,
                "reason": "sl_sync_skipped_no_upgrade",
                "action": "skipped",
                "current_sl_trigger_px": observed_current,
                "desired_sl_trigger_px": desired,
                "attach_algo_id": observed_attach_algo_id,
                "attach_algo_cl_ord_id": observed_attach_algo_cl_ord_id,
            }

        amend = self.amend_attached_stop_loss(
            inst_id=inst_id,
            ord_id=ord_id,
            new_sl_trigger_px=desired,
            attach_algo_id=observed_attach_algo_id,
            attach_algo_cl_ord_id=observed_attach_algo_cl_ord_id,
            req_id=req_id,
        )
        return {
            "ok": bool(amend.get("ok")),
            "simulated": self.credentials.simulated,
            "reason": amend.get("reason") or ("sl_sync_amended" if amend.get("ok") else "sl_sync_failed"),
            "action": "amended" if amend.get("ok") else "failed",
            "current_sl_trigger_px": observed_current,
            "desired_sl_trigger_px": desired,
            "attach_algo_id": observed_attach_algo_id,
            "attach_algo_cl_ord_id": observed_attach_algo_cl_ord_id,
            "amend": amend,
        }
