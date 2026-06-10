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
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any
from urllib.parse import urlencode

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
        self._instrument_specs_cache: dict[str, dict[str, Any]] = {}

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
        method_u = method.upper()
        body = json.dumps(payload, separators=(",", ":")) if payload is not None else ""

        # OKX signs the exact request path. For signed GET requests with
        # query parameters, the query string must be included in the
        # prehash path, otherwise OKX returns: code=50113 / "Invalid Sign".
        query_string = urlencode(params or {}) if params else ""
        request_path = f"{path}?{query_string}" if query_string else path
        url = f"{self.base_url}{request_path}"

        try:
            response = requests.request(
                method=method_u,
                url=url,
                data=body if payload is not None else None,
                headers=self._headers(ts, method_u, request_path, body),
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


    def _public_get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            try:
                data = response.json()
            except Exception:
                data = {"code": str(response.status_code), "msg": response.text, "data": []}
            return data
        except Exception as exc:
            return {"code": "-1", "msg": str(exc), "data": []}

    @staticmethod
    def _to_decimal(value: Any, default: str = "0") -> Decimal:
        try:
            if isinstance(value, Decimal):
                return value
            text = str(value if value is not None else default).strip()
            if not text:
                text = default
            return Decimal(text)
        except (InvalidOperation, ValueError, TypeError):
            return Decimal(default)

    @staticmethod
    def _decimal_to_str(value: Decimal, default: str = "0") -> str:
        text = format(value.normalize(), "f") if value != 0 else "0"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or default

    @classmethod
    def _floor_to_step(cls, value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    @classmethod
    def _ceil_to_step(cls, value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_UP) * step

    def get_instrument_specs(
        self,
        inst_id: str,
        *,
        inst_type: str = "SWAP",
        use_cache: bool = True,
    ) -> dict[str, Any]:
        cache_key = f"{inst_type}:{inst_id}"
        if use_cache and cache_key in self._instrument_specs_cache:
            cached = dict(self._instrument_specs_cache[cache_key])
            cached["cached"] = True
            return cached

        params = {"instType": inst_type, "instId": inst_id}
        response = self._public_get("/api/v5/public/instruments", params=params)
        ok = str(response.get("code", "")) == "0"
        rows = response.get("data") or []
        row = rows[0] if isinstance(rows, list) and rows else {}

        result = {
            "ok": bool(ok and row),
            "reason": response.get("msg") or ("ok" if ok and row else "instrument_not_found"),
            "inst_id": inst_id,
            "inst_type": inst_type,
            "response": response,
            "row": row if isinstance(row, dict) else {},
            "lot_sz": str((row or {}).get("lotSz") or ""),
            "min_sz": str((row or {}).get("minSz") or ""),
            "max_mkt_sz": str((row or {}).get("maxMktSz") or ""),
            "ct_val": str((row or {}).get("ctVal") or ""),
            "ct_type": str((row or {}).get("ctType") or ""),
            "state": str((row or {}).get("state") or ""),
            "cached": False,
        }
        if result["ok"]:
            self._instrument_specs_cache[cache_key] = dict(result)
        return result

    def _build_position_size_meta(
        self,
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        *,
        inst_id: str | None = None,
    ) -> dict[str, Any]:
        notional = max(float(margin_usdt), 0.0) * max(int(leverage), 1)
        price = max(float(entry_price), 0.0)
        if price <= 0 or notional <= 0:
            raise ValueError("invalid_entry_or_margin")

        raw_base_size = Decimal(str(notional / price))
        result: dict[str, Any] = {
            "inst_id": inst_id or "",
            "notional_usdt": float(notional),
            "entry_price": float(price),
            "raw_base_size": float(raw_base_size),
            "sizing_source": "base_qty_fallback",
            "instrument_specs": None,
        }

        if not inst_id:
            size = self._floor_to_step(raw_base_size, Decimal("0.000001"))
            result.update({
                "size_decimal": size,
                "size_str": self._decimal_to_str(size),
                "lot_sz": "",
                "min_sz": "",
                "ct_val": "",
                "ct_type": "",
            })
            return result

        specs = self.get_instrument_specs(inst_id)
        result["instrument_specs"] = specs
        if not specs.get("ok"):
            size = self._floor_to_step(raw_base_size, Decimal("0.000001"))
            result.update({
                "size_decimal": size,
                "size_str": self._decimal_to_str(size),
                "lot_sz": "",
                "min_sz": "",
                "ct_val": "",
                "ct_type": "",
                "sizing_source": "base_qty_no_specs",
                "size_warning": specs.get("reason") or "instrument_specs_unavailable",
            })
            return result

        lot_sz = self._to_decimal(specs.get("lot_sz") or "0", "0")
        min_sz = self._to_decimal(specs.get("min_sz") or "0", "0")
        ct_val = self._to_decimal(specs.get("ct_val") or "0", "0")
        ct_type = str(specs.get("ct_type") or "").strip().lower()

        qty = raw_base_size
        sizing_source = "base_qty"

        if ct_val > 0:
            if ct_type == "inverse":
                qty = Decimal(str(notional)) / ct_val
                sizing_source = "contracts_inverse"
            else:
                denom = Decimal(str(price)) * ct_val
                if denom <= 0:
                    raise ValueError("invalid_contract_value")
                qty = Decimal(str(notional)) / denom
                sizing_source = "contracts_linear"

        step = lot_sz if lot_sz > 0 else Decimal("0.000001")
        normalized_qty = self._floor_to_step(qty, step)

        if normalized_qty <= 0:
            raise ValueError("normalized_size_zero")

        if min_sz > 0 and normalized_qty < min_sz:
            min_normalized = self._ceil_to_step(min_sz, step)
            if qty < min_normalized:
                raise ValueError(
                    f"size_below_min_sz raw={self._decimal_to_str(qty)} min={self._decimal_to_str(min_normalized)}"
                )
            normalized_qty = min_normalized

        result.update({
            "size_decimal": normalized_qty,
            "size_str": self._decimal_to_str(normalized_qty),
            "raw_contract_size": float(qty),
            "lot_sz": self._decimal_to_str(lot_sz) if lot_sz > 0 else "",
            "min_sz": self._decimal_to_str(min_sz) if min_sz > 0 else "",
            "ct_val": self._decimal_to_str(ct_val) if ct_val > 0 else "",
            "ct_type": ct_type,
            "sizing_source": sizing_source,
        })
        return result

    def _build_split_size_meta(
        self,
        full_size_str: str,
        *,
        inst_id: str,
        tp1_pct: float,
        tp2_pct: float,
        runner_pct: float,
    ) -> dict[str, Any]:
        full_size = self._to_decimal(full_size_str, "0")
        if full_size <= 0:
            raise ValueError("invalid_full_size")

        specs = self.get_instrument_specs(inst_id)
        lot_sz = self._to_decimal((specs.get("lot_sz") if isinstance(specs, dict) else "") or "0", "0")
        step = lot_sz if lot_sz > 0 else Decimal("0.000001")

        tp1 = self._floor_to_step(full_size * self._to_decimal(tp1_pct, "0") / Decimal("100"), step)
        tp2 = self._floor_to_step(full_size * self._to_decimal(tp2_pct, "0") / Decimal("100"), step)

        if tp1 + tp2 > full_size:
            overflow = (tp1 + tp2) - full_size
            tp2 = max(tp2 - overflow, Decimal("0"))
            tp2 = self._floor_to_step(tp2, step)

        runner = full_size - tp1 - tp2
        runner = self._floor_to_step(runner, step) if step > 0 else runner

        warnings: list[str] = []
        if tp1 <= 0:
            warnings.append("tp1_size_zero_after_lot_rounding")
        if tp2 <= 0:
            warnings.append("tp2_size_zero_after_lot_rounding")
        if runner < 0:
            warnings.append("runner_negative_after_rounding")
            runner = Decimal("0")

        return {
            "ok": True,
            "instrument_specs": specs,
            "full_size_decimal": full_size,
            "full_size": self._decimal_to_str(full_size),
            "tp1_size_decimal": tp1,
            "tp1_size": self._decimal_to_str(tp1),
            "tp2_size_decimal": tp2,
            "tp2_size": self._decimal_to_str(tp2),
            "runner_size_decimal": runner,
            "runner_size": self._decimal_to_str(runner),
            "lot_sz": self._decimal_to_str(lot_sz) if lot_sz > 0 else "",
            "warnings": warnings,
        }

    def _build_position_size(
        self,
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        *,
        inst_id: str | None = None,
    ) -> str:
        meta = self._build_position_size_meta(
            entry_price,
            margin_usdt,
            leverage,
            inst_id=inst_id,
        )
        return str(meta.get("size_str") or "0")

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

    def get_positions(
        self,
        *,
        inst_type: str = "SWAP",
        inst_id: str | None = None,
        pos_id: str | None = None,
    ) -> dict[str, Any]:
        """Read current OKX account positions.

        This is a read-only helper used by main.py for live execution guards,
        position recovery, slot protection and execution reports. It supports
        both the normalized shape used by this client ({ok, rows, row}) and the
        raw OKX response nested under `response`, so existing callers remain
        backward-compatible.
        """
        guard = self._read_guard_error()
        if guard:
            return guard

        path = "/api/v5/account/positions"
        params: dict[str, Any] = {}
        if inst_type:
            params["instType"] = str(inst_type).upper()
        if inst_id:
            params["instId"] = str(inst_id)
        if pos_id:
            params["posId"] = str(pos_id)

        response = self._request("GET", path, params=params)
        result = self._normalize_query_response(response, query_kind="positions", params=params)
        result["positions"] = result.get("rows") or []
        return result

    def list_positions(
        self,
        *,
        inst_type: str = "SWAP",
        inst_id: str | None = None,
        pos_id: str | None = None,
    ) -> dict[str, Any]:
        """Backward-compatible alias for get_positions()."""
        return self.get_positions(inst_type=inst_type, inst_id=inst_id, pos_id=pos_id)

    def place_market_order(
        self,
        inst_id: str,
        side: str,
        sz: str,
        td_mode: str = "isolated",
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
        td_mode: str = "isolated",
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
        td_mode: str = "isolated",
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
            size_meta = self._build_position_size_meta(
                entry_price,
                margin_usdt,
                leverage,
                inst_id=inst_id,
            )
            sz = str(size_meta.get("size_str") or "0")
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
        result["size_meta"] = size_meta
        result["instrument_specs"] = size_meta.get("instrument_specs")
        result["lot_sz"] = size_meta.get("lot_sz")
        result["min_sz"] = size_meta.get("min_sz")
        result["ct_val"] = size_meta.get("ct_val")
        result["ct_type"] = size_meta.get("ct_type")
        result["sizing_source"] = size_meta.get("sizing_source")
        return result

    def place_market_long_with_attached_sl(
        self,
        inst_id: str,
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        sl_trigger_px: float,
        td_mode: str = "isolated",
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
        td_mode: str = "isolated",
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
        td_mode: str = "isolated",
        pos_side: str = "long",
        tag: str | None = None,
    ) -> dict[str, Any]:
        try:
            size_meta = self._build_position_size_meta(
                entry_price,
                margin_usdt,
                leverage,
                inst_id=inst_id,
            )
            split_meta = self._build_split_size_meta(
                str(size_meta.get("size_str") or "0"),
                inst_id=inst_id,
                tp1_pct=tp1_pct,
                tp2_pct=tp2_pct,
                runner_pct=max(0.0, 100.0 - float(tp1_pct) - float(tp2_pct)),
            )
        except Exception as exc:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": f"size_build_failed: {exc}",
            }

        tp1_sz = str(split_meta.get("tp1_size") or "0")
        tp2_sz = str(split_meta.get("tp2_size") or "0")
        full_sz = float(size_meta.get("size_str") or 0.0)

        if self._safe_float(tp1_sz) <= 0 or self._safe_float(tp2_sz) <= 0:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "tp_split_size_zero_after_lot_rounding",
                "size_meta": size_meta,
                "split_meta": split_meta,
            }

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
            "full_size": str(size_meta.get("size_str") or f"{full_sz:.6f}"),
            "tp1_size": tp1_sz,
            "tp2_size": tp2_sz,
            "runner_size": str(split_meta.get("runner_size") or "0"),
            "size_meta": size_meta,
            "split_meta": split_meta,
            "warnings": list(split_meta.get("warnings") or []),
        }

    def place_reduce_only_market_close(
        self,
        inst_id: str,
        sz: str,
        td_mode: str = "isolated",
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
        td_mode: str = "isolated",
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

    def get_max_leverage(
        self,
        inst_id: str,
        mgn_mode: str = "isolated",
    ) -> dict[str, Any]:
        """Diagnostic leverage info; do not treat current leverage as max."""
        guard = self._read_guard_error()
        if guard:
            return guard

        path = "/api/v5/account/leverage-info"
        params: dict[str, Any] = {"instId": inst_id, "mgnMode": mgn_mode}
        response = self._request("GET", path, params=params)
        ok = str(response.get("code", "")) == "0"
        rows = response.get("data") or []
        row = rows[0] if isinstance(rows, list) and rows else {}

        current_lever = int(self._safe_float((row or {}).get("lever"), 0)) if ok and row else 0

        specs = self.get_instrument_specs(inst_id)
        spec_lever = 0
        if specs.get("ok"):
            spec_lever = int(self._safe_float((specs.get("row") or {}).get("lever") or 0, 0))

        return {
            "ok": bool(ok or specs.get("ok")),
            "simulated": self.credentials.simulated,
            "current_leverage": current_lever,
            "max_leverage": spec_lever if spec_lever > 0 else 0,
            "source": "leverage_info_diagnostic",
            "row": row if isinstance(row, dict) else {},
            "instrument_specs": specs,
            "response": response,
            "reason": response.get("msg") or ("ok" if (ok or specs.get("ok")) else "max_leverage_unavailable"),
        }

    def set_leverage(
        self,
        inst_id: str,
        lever: int,
        mgn_mode: str = "isolated",
        *,
        pos_side: str | None = "long",
    ) -> dict[str, Any]:
        """Set leverage to the nearest lower value accepted by OKX."""
        guard = self._trade_guard_error()
        if guard:
            return guard

        requested_lever = max(1, int(lever))
        path = "/api/v5/account/set-leverage"

        def _do_set(l: int) -> dict[str, Any]:
            payload: dict[str, Any] = {
                "instId": inst_id,
                "lever": str(int(l)),
                "mgnMode": str(mgn_mode or "isolated"),
            }
            if pos_side:
                payload["posSide"] = pos_side
            response = self._request("POST", path, payload=payload)
            ok = str(response.get("code", "")) == "0"
            rows = response.get("data") or []
            row = rows[0] if isinstance(rows, list) and rows else {}
            return {
                "ok": ok,
                "simulated": self.credentials.simulated,
                "lever_set": int(l),
                "reason": (
                    row.get("sMsg") or response.get("msg") or
                    ("leverage_set" if ok else "set_leverage_failed")
                ),
                "payload": payload,
                "response": response,
            }

        result = _do_set(requested_lever)
        if result.get("ok"):
            result["original_request"] = requested_lever
            result["capped_to_max"] = requested_lever
            return result

        max_info = self.get_max_leverage(inst_id, mgn_mode=mgn_mode)
        last_result = result
        for candidate in range(requested_lever - 1, 0, -1):
            print(
                f"⚠️ set_leverage retry | {inst_id} | requested={requested_lever}x | trying={candidate}x | mgnMode={mgn_mode}",
                flush=True,
            )
            retry = _do_set(candidate)
            last_result = retry
            if retry.get("ok"):
                retry["original_request"] = requested_lever
                retry["capped_to_max"] = candidate
                retry["max_leverage_info"] = max_info
                print(
                    f"✅ set_leverage accepted | {inst_id} | requested={requested_lever}x → actual={candidate}x | mgnMode={mgn_mode}",
                    flush=True,
                )
                return retry

        result["original_request"] = requested_lever
        result["max_leverage_info"] = max_info
        result["last_retry_result"] = last_result
        return result

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
            size_meta = self._build_position_size_meta(
                entry_price,
                margin_usdt,
                leverage,
                inst_id=inst_id,
            )
            split_meta = self._build_split_size_meta(
                str(size_meta.get("size_str") or "0"),
                inst_id=inst_id,
                tp1_pct=tp1_pct,
                tp2_pct=tp2_pct,
                runner_pct=runner_pct,
            )
        except Exception as exc:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": f"size_build_failed: {exc}",
            }

        return {
            "ok": True,
            "simulated": self.credentials.simulated,
            "inst_id": inst_id,
            "entry_price": float(entry_price),
            "full_size": str(size_meta.get("size_str") or "0"),
            "attached_stop_loss": {
                "slTriggerPx": f"{float(sl_trigger_px):.10f}".rstrip("0").rstrip("."),
                "slOrdPx": "-1",
            },
            "tp1": {
                "price": float(tp1_price),
                "close_pct": float(tp1_pct),
                "size": str(split_meta.get("tp1_size") or "0"),
            },
            "tp2": {
                "price": float(tp2_price),
                "close_pct": float(tp2_pct),
                "size": str(split_meta.get("tp2_size") or "0"),
            },
            "runner": {
                "close_pct": float(runner_pct),
                "size": str(split_meta.get("runner_size") or "0"),
                "requires_trailing_after_tp2": True,
            },
            "size_meta": size_meta,
            "split_meta": split_meta,
            "instrument_specs": size_meta.get("instrument_specs"),
            "warnings": list(split_meta.get("warnings") or []),
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


    def get_position_protection_orders(
        self,
        inst_id: str,
        *,
        entry_price: float = 0.0,
        pos_side: str = "long",
        inst_type: str = "SWAP",
        limit: int = 100,
    ) -> dict[str, Any]:
        """Read visible exchange-side TP/SL protection for an existing position.

        Read-only helper for recovered/manual OKX positions:
        - detects reduce-only sell limit orders above entry as TP candidates;
        - detects conditional/algo stop-loss triggers below entry as SL candidates;
        - does not create, amend, cancel, or manage any order.
        """
        guard = self._read_guard_error()
        if guard:
            return guard

        inst_id = str(inst_id or "").strip()
        if not inst_id:
            return {"ok": False, "simulated": self.credentials.simulated, "reason": "missing_inst_id"}

        entry = self._safe_float(entry_price, 0.0)
        wanted_side = str(pos_side or "long").strip().lower()

        def _rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
            if not isinstance(payload, dict):
                return []
            rows = payload.get("rows")
            if rows is None:
                rows = payload.get("data")
            if rows is None and isinstance(payload.get("response"), dict):
                rows = payload.get("response", {}).get("data")
            return [row for row in (rows or []) if isinstance(row, dict)] if isinstance(rows, list) else []

        def _is_ok(payload: dict[str, Any] | None) -> bool:
            if not isinstance(payload, dict):
                return False
            if "ok" in payload:
                return bool(payload.get("ok"))
            return str(payload.get("code", "")) == "0"

        def _num(row: dict[str, Any], *keys: str) -> float:
            for key in keys:
                value = self._safe_float(row.get(key), 0.0)
                if value > 0:
                    return value
            return 0.0

        pending = self.list_pending_orders(inst_type=inst_type, inst_id=inst_id, limit=limit)
        regular_rows = _rows(pending)

        algo_payloads: dict[str, dict[str, Any]] = {}
        algo_rows: list[dict[str, Any]] = []
        for ord_type in ("conditional", "oco", "trigger", "move_order_stop"):
            payload = self.list_algo_orders(ord_type=ord_type, inst_id=inst_id, history=False, limit=limit)
            algo_payloads[ord_type] = payload
            if _is_ok(payload):
                algo_rows.extend(_rows(payload))

        tp_candidates: list[dict[str, Any]] = []
        sl_candidates: list[dict[str, Any]] = []
        other_candidates: list[dict[str, Any]] = []

        for row in regular_rows:
            if str(row.get("instId") or "").strip().upper() != inst_id.upper():
                continue
            state = str(row.get("state") or row.get("ordState") or "").strip().lower()
            if state in {"filled", "canceled", "cancelled", "mmp_canceled"}:
                continue
            side = str(row.get("side") or "").strip().lower()
            reduce_only = str(row.get("reduceOnly") or row.get("reduce_only") or "").strip().lower() in {"true", "1", "yes"}
            px = _num(row, "px", "price")
            if wanted_side == "long" and side == "sell" and px > 0:
                kind = "tp" if (entry <= 0 or px > entry) else "sell_reduce_order_below_or_at_entry"
                item = {
                    "kind": kind,
                    "source": "pending_order",
                    "price": px,
                    "size": str(row.get("sz") or row.get("size") or ""),
                    "order_id": row.get("ordId"),
                    "client_order_id": row.get("clOrdId"),
                    "reduce_only": reduce_only,
                    "row": row,
                }
                if kind == "tp" and (reduce_only or str(row.get("ordType") or "").lower() == "limit"):
                    tp_candidates.append(item)
                else:
                    other_candidates.append(item)

        for row in algo_rows:
            if str(row.get("instId") or "").strip().upper() != inst_id.upper():
                continue
            state = str(row.get("state") or row.get("ordState") or row.get("algoStatus") or "").strip().lower()
            if state in {"filled", "canceled", "cancelled", "mmp_canceled"}:
                continue
            side = str(row.get("side") or "").strip().lower()
            # OKX TP/SL algo rows may expose tpTriggerPx/slTriggerPx or generic triggerPx.
            tp_px = _num(row, "tpTriggerPx", "tpOrdPx")
            sl_px = _num(row, "slTriggerPx", "triggerPx", "triggerPxUsd")
            sz = str(row.get("sz") or row.get("size") or "")
            if wanted_side == "long" and side in {"", "sell"}:
                if tp_px > 0 and (entry <= 0 or tp_px > entry):
                    tp_candidates.append({
                        "kind": "tp",
                        "source": "algo_order",
                        "price": tp_px,
                        "size": sz,
                        "algo_id": row.get("algoId"),
                        "algo_client_order_id": row.get("algoClOrdId"),
                        "ord_type": row.get("ordType"),
                        "row": row,
                    })
                if sl_px > 0 and (entry <= 0 or sl_px < entry):
                    sl_candidates.append({
                        "kind": "sl",
                        "source": "algo_order",
                        "price": sl_px,
                        "size": sz,
                        "algo_id": row.get("algoId"),
                        "algo_client_order_id": row.get("algoClOrdId"),
                        "ord_type": row.get("ordType"),
                        "row": row,
                    })
                elif sl_px > 0:
                    other_candidates.append({
                        "kind": "algo_trigger_not_classified",
                        "source": "algo_order",
                        "price": sl_px,
                        "size": sz,
                        "algo_id": row.get("algoId"),
                        "algo_client_order_id": row.get("algoClOrdId"),
                        "ord_type": row.get("ordType"),
                        "row": row,
                    })

        # Deduplicate by id/source/price and sort in long-trade meaning.
        def _dedup(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            seen: set[tuple[Any, ...]] = set()
            for item in items:
                key = (
                    item.get("source"),
                    item.get("order_id") or item.get("algo_id") or item.get("client_order_id") or item.get("algo_client_order_id"),
                    round(self._safe_float(item.get("price"), 0.0), 12),
                    str(item.get("size") or ""),
                )
                if key in seen:
                    continue
                seen.add(key)
                out.append(item)
            return out

        tp_candidates = sorted(_dedup(tp_candidates), key=lambda x: self._safe_float(x.get("price"), 0.0))
        sl_candidates = sorted(_dedup(sl_candidates), key=lambda x: self._safe_float(x.get("price"), 0.0), reverse=True)
        nearest_sl = sl_candidates[0] if sl_candidates else None

        return {
            "ok": bool(_is_ok(pending) or any(_is_ok(p) for p in algo_payloads.values())),
            "simulated": self.credentials.simulated,
            "reason": "ok" if (tp_candidates or sl_candidates) else "no_exchange_tp_sl_detected",
            "inst_id": inst_id,
            "entry_price": entry,
            "tp_orders": tp_candidates,
            "sl_orders": sl_candidates,
            "other_orders": other_candidates,
            "tp1": tp_candidates[0] if len(tp_candidates) >= 1 else None,
            "tp2": tp_candidates[1] if len(tp_candidates) >= 2 else None,
            "sl": nearest_sl,
            "has_tp": bool(tp_candidates),
            "has_sl": bool(sl_candidates),
            "has_any_protection": bool(tp_candidates or sl_candidates),
            "pending_orders": pending,
            "algo_orders": algo_payloads,
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


    @staticmethod
    def _stop_loss_recreate_needed(reason: Any, payload: dict[str, Any] | None = None) -> bool:
        """Return True when an attached-SL amend failed because the old order/algo is no longer amendable."""
        parts = [str(reason or "")]
        if isinstance(payload, dict):
            parts.append(str(payload.get("reason") or ""))
            response = payload.get("response") if isinstance(payload.get("response"), dict) else {}
            parts.append(str(response.get("msg") or ""))
            for row in response.get("data") or []:
                if isinstance(row, dict):
                    parts.append(str(row.get("sMsg") or ""))
                    parts.append(str(row.get("sCode") or ""))
            amend = payload.get("amend") if isinstance(payload.get("amend"), dict) else {}
            if amend:
                parts.append(str(amend.get("reason") or ""))
                amend_response = amend.get("response") if isinstance(amend.get("response"), dict) else {}
                parts.append(str(amend_response.get("msg") or ""))
                for row in amend_response.get("data") or []:
                    if isinstance(row, dict):
                        parts.append(str(row.get("sMsg") or ""))
                        parts.append(str(row.get("sCode") or ""))
        blob = " | ".join(parts).lower()
        needles = (
            "filled",
            "canceled",
            "cancelled",
            "not found",
            "not exist",
            "does not exist",
            "already been filled or canceled",
            "order does not exist",
            "algo_not_found",
            "failed_to_read_current_sl",
            "missing_order_identifier",
            "51603",
            "51001",
            "51008",
            "51149",
        )
        return any(token in blob for token in needles)

    def _live_position_size_for_stop_loss(
        self,
        inst_id: str,
        *,
        pos_side: str = "long",
        inst_type: str = "SWAP",
    ) -> dict[str, Any]:
        """Read current OKX position size and normalize it to instrument lot size for reduce-only stops."""
        positions = self.get_positions(inst_type=inst_type, inst_id=inst_id)
        if not positions.get("ok"):
            return {
                "ok": False,
                "reason": positions.get("reason") or "positions_fetch_failed",
                "positions": positions,
            }

        wanted_side = str(pos_side or "long").strip().lower()
        selected = None
        for row in positions.get("rows") or []:
            if not isinstance(row, dict):
                continue
            if str(row.get("instId") or "").strip().upper() != str(inst_id or "").strip().upper():
                continue
            row_side = str(row.get("posSide") or "").strip().lower()
            if wanted_side and row_side and row_side != wanted_side:
                continue
            raw_pos = self._to_decimal(row.get("availPos") or row.get("pos") or "0", "0")
            if abs(raw_pos) > 0:
                selected = dict(row)
                break

        if not selected:
            return {"ok": False, "reason": "live_position_not_found", "positions": positions}

        raw_size = abs(self._to_decimal(selected.get("availPos") or selected.get("pos") or "0", "0"))
        specs = self.get_instrument_specs(inst_id)
        lot_sz = self._to_decimal((specs.get("lot_sz") if isinstance(specs, dict) else "") or "0", "0")
        step = lot_sz if lot_sz > 0 else Decimal("0.000001")
        normalized = self._floor_to_step(raw_size, step)
        if normalized <= 0:
            return {
                "ok": False,
                "reason": "position_size_zero_after_lot_rounding",
                "row": selected,
                "instrument_specs": specs,
            }
        return {
            "ok": True,
            "reason": "ok",
            "inst_id": inst_id,
            "size": self._decimal_to_str(normalized),
            "raw_size": self._decimal_to_str(raw_size),
            "row": selected,
            "instrument_specs": specs,
        }

    def place_reduce_only_stop_loss(
        self,
        inst_id: str,
        sl_trigger_px: float,
        *,
        sz: str | None = None,
        td_mode: str = "isolated",
        pos_side: str = "long",
        side: str = "sell",
        sl_ord_px: str = "-1",
        algo_cl_ord_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        """Create an independent reduce-only conditional SL for an existing long position."""
        guard = self._trade_guard_error()
        if guard:
            return guard

        trigger = float(sl_trigger_px)
        if trigger <= 0:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "invalid_sl_trigger_px",
            }

        size_source = {"ok": True, "size": str(sz or "").strip(), "reason": "provided_size"}
        if not size_source["size"] or self._safe_float(size_source["size"], 0.0) <= 0:
            size_source = self._live_position_size_for_stop_loss(inst_id, pos_side=pos_side)
            if not size_source.get("ok"):
                return {
                    "ok": False,
                    "simulated": self.credentials.simulated,
                    "reason": size_source.get("reason") or "position_size_unavailable",
                    "size_source": size_source,
                }

        path = "/api/v5/trade/order-algo"
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": str(td_mode or "isolated"),
            "side": str(side or "sell"),
            "ordType": "conditional",
            "sz": str(size_source.get("size") or sz),
            "slTriggerPx": f"{trigger:.10f}".rstrip("0").rstrip("."),
            "slOrdPx": str(sl_ord_px),
            "reduceOnly": "true",
        }
        if pos_side:
            payload["posSide"] = str(pos_side)
        if algo_cl_ord_id:
            payload["algoClOrdId"] = str(algo_cl_ord_id)[:32]
        if tag:
            payload["tag"] = str(tag)[:16]

        response = self._request("POST", path, payload=payload)
        result = self._normalize_trade_response(response, payload, id_key="algoId", response_kind="stop_loss_algo")
        result["action"] = "created" if result.get("ok") else "create_failed"
        result["desired_sl_trigger_px"] = trigger
        result["size_source"] = size_source
        return result

    def sync_or_recreate_stop_loss(
        self,
        inst_id: str,
        desired_sl_trigger_px: float,
        *,
        ord_id: str | None = None,
        current_sl_trigger_px: float | None = None,
        attach_algo_id: str | None = None,
        attach_algo_cl_ord_id: str | None = None,
        existing_stop_algo_id: str | None = None,
        existing_stop_algo_cl_ord_id: str | None = None,
        td_mode: str = "isolated",
        pos_side: str = "long",
        min_improvement_px: float = 0.0,
        req_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        """Amend attached SL when possible; recreate a new reduce-only conditional SL when amend is impossible."""
        desired = float(desired_sl_trigger_px)
        if desired <= 0:
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": "invalid_desired_sl_trigger_px",
            }

        improvement_needed = float(min_improvement_px or 0.0)
        if current_sl_trigger_px and current_sl_trigger_px > 0 and desired <= (float(current_sl_trigger_px) + improvement_needed):
            return {
                "ok": True,
                "simulated": self.credentials.simulated,
                "reason": "sl_sync_skipped_no_upgrade",
                "action": "skipped",
                "current_sl_trigger_px": float(current_sl_trigger_px),
                "desired_sl_trigger_px": desired,
                "attach_algo_id": attach_algo_id,
                "attach_algo_cl_ord_id": attach_algo_cl_ord_id,
                "algo_id": existing_stop_algo_id,
                "algo_client_order_id": existing_stop_algo_cl_ord_id,
            }

        amend_result = None
        if ord_id:
            amend_result = self.sync_attached_stop_loss(
                inst_id=inst_id,
                ord_id=str(ord_id),
                desired_sl_trigger_px=desired,
                current_sl_trigger_px=current_sl_trigger_px,
                attach_algo_id=attach_algo_id,
                attach_algo_cl_ord_id=attach_algo_cl_ord_id,
                min_improvement_px=min_improvement_px,
                req_id=req_id,
            )
            if amend_result.get("ok"):
                return amend_result

        recreate_reason = (amend_result or {}).get("reason") or ("missing_entry_order_id" if not ord_id else "amend_failed")
        if amend_result is not None and not self._stop_loss_recreate_needed(recreate_reason, amend_result):
            return {
                "ok": False,
                "simulated": self.credentials.simulated,
                "reason": recreate_reason,
                "action": "failed",
                "current_sl_trigger_px": current_sl_trigger_px,
                "desired_sl_trigger_px": desired,
                "amend": amend_result,
                "recreate_attempted": False,
            }

        cancel_result = None
        cancel_id = existing_stop_algo_id or attach_algo_id
        if cancel_id:
            cancel_result = self.cancel_algo_order(inst_id, str(cancel_id))

        created = self.place_reduce_only_stop_loss(
            inst_id=inst_id,
            sl_trigger_px=desired,
            td_mode=td_mode,
            pos_side=pos_side,
            algo_cl_ord_id=existing_stop_algo_cl_ord_id or attach_algo_cl_ord_id or self._client_id("slr"),
            tag=tag,
        )

        return {
            "ok": bool(created.get("ok")),
            "simulated": self.credentials.simulated,
            "reason": "sl_recreated_after_amend_failed" if created.get("ok") else (created.get("reason") or "sl_recreate_failed"),
            "action": "recreated" if created.get("ok") else "recreate_failed",
            "current_sl_trigger_px": current_sl_trigger_px,
            "desired_sl_trigger_px": desired,
            "attach_algo_id": attach_algo_id,
            "attach_algo_cl_ord_id": attach_algo_cl_ord_id,
            "algo_id": created.get("algo_id"),
            "algo_client_order_id": created.get("algo_client_order_id"),
            "amend": amend_result,
            "cancel_previous": cancel_result,
            "create": created,
            "recreate_attempted": True,
            "fallback_reason": recreate_reason,
        }
