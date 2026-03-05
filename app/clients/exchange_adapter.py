from __future__ import annotations

import hashlib
import hmac
import json
import time
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from threading import Lock, local
from typing import Any, Literal
from urllib.parse import urlencode

import requests

from app.config import settings


def _clean_symbol(value: str) -> str:
    return str(value or "").upper().strip()


class BinanceFuturesPriceAdapter:
    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = str(base_url or settings.binance_futures_base_url).rstrip("/")
        self._thread_local = local()

    def _session(self) -> requests.Session:
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update({"User-Agent": "carry-optimizer-live/1.0"})
            self._thread_local.session = sess
        return sess

    def get_latest_prices(self, symbols: list[str]) -> dict[str, float]:
        out: dict[str, float] = {}
        sess = self._session()
        for sym in sorted({_clean_symbol(x) for x in symbols if _clean_symbol(x)}):
            resp = sess.get(
                f"{self._base_url}/fapi/v1/ticker/price",
                params={"symbol": sym},
                timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()
            out[sym] = float(payload.get("price"))
        return out


class BybitV5Client:
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str | None = None,
        recv_window_ms: int | None = None,
    ) -> None:
        self._api_key = str(api_key or "").strip()
        self._api_secret = str(api_secret or "").strip()
        self._base_url = str(base_url or settings.bybit_base_url).rstrip("/")
        self._recv_window = int(recv_window_ms or settings.bybit_recv_window_ms)
        self._thread_local = local()
        self._time_offset_lock = Lock()
        self._time_offset_ms = 0
        self._instrument_cache: dict[str, dict[str, Decimal]] = {}

    def _session(self) -> requests.Session:
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update({"User-Agent": "carry-optimizer-live/1.0"})
            self._thread_local.session = sess
        return sess

    def _get_server_time_ms(self) -> int:
        sess = self._session()
        resp = sess.get(f"{self._base_url}/v5/market/time", timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise ValueError("Bybit time response is not JSON object")
        ret_code = int(payload.get("retCode", 0))
        if ret_code != 0:
            ret_msg = str(payload.get("retMsg", "Unknown error"))
            raise ValueError(f"Bybit time API error retCode={ret_code}, retMsg={ret_msg}")

        result = payload.get("result", {})
        if isinstance(result, dict):
            raw_nano = result.get("timeNano")
            if raw_nano is not None:
                try:
                    return int(str(raw_nano).strip()) // 1_000_000
                except Exception:
                    pass
            raw_second = result.get("timeSecond")
            if raw_second is not None:
                try:
                    return int(str(raw_second).strip()) * 1000
                except Exception:
                    pass

        raw_time = payload.get("time")
        if raw_time is not None:
            try:
                value = int(str(raw_time).strip())
                if value < 10_000_000_000:
                    value *= 1000
                return value
            except Exception:
                pass
        raise ValueError("Bybit time API missing server timestamp")

    def _sync_server_time_offset(self) -> int:
        server_ms = self._get_server_time_ms()
        local_ms = int(time.time() * 1000)
        offset = int(server_ms - local_ms)
        with self._time_offset_lock:
            self._time_offset_ms = offset
        return offset

    def _current_timestamp_ms(self) -> int:
        with self._time_offset_lock:
            offset = int(self._time_offset_ms)
        return int(time.time() * 1000) + offset

    @staticmethod
    def _to_decimal(raw: Any, *, default: str = "0") -> Decimal:
        txt = str(raw if raw is not None else default).strip() or default
        try:
            return Decimal(txt)
        except (InvalidOperation, ValueError):
            return Decimal(default)

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return units * step

    @staticmethod
    def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        units = (value / step).to_integral_value(rounding=ROUND_UP)
        return units * step

    @staticmethod
    def _fmt_decimal(value: Decimal) -> str:
        txt = format(value.normalize(), "f")
        if "." in txt:
            txt = txt.rstrip("0").rstrip(".")
        return txt or "0"

    def _signed_headers(self, *, timestamp_ms: int, payload: str) -> dict[str, str]:
        if not self._api_key or not self._api_secret:
            raise ValueError("Bybit API key/secret are required for live mode")
        sign_origin = f"{timestamp_ms}{self._api_key}{self._recv_window}{payload}"
        sign = hmac.new(
            self._api_secret.encode("utf-8"),
            sign_origin.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-BAPI-API-KEY": self._api_key,
            "X-BAPI-SIGN": sign,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": str(timestamp_ms),
            "X-BAPI-RECV-WINDOW": str(self._recv_window),
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        auth: bool,
        timeout: int = 15,
    ) -> dict[str, Any]:
        method_u = str(method or "GET").upper().strip()
        url = f"{self._base_url}{path}"
        params_clean = dict(params or {})
        body_clean = dict(json_body or {})
        sess = self._session()
        max_attempts = 2 if auth else 1

        for attempt in range(max_attempts):
            headers: dict[str, str] = {}
            body_text = ""
            if auth:
                ts = self._current_timestamp_ms()
                if method_u == "GET":
                    sign_payload = urlencode(sorted((str(k), str(v)) for k, v in params_clean.items()), doseq=True)
                else:
                    body_text = json.dumps(body_clean, ensure_ascii=False, separators=(",", ":"))
                    sign_payload = body_text
                headers = self._signed_headers(timestamp_ms=ts, payload=sign_payload)
                if method_u != "GET":
                    headers["Content-Type"] = "application/json"

            if method_u == "GET":
                resp = sess.get(url, params=params_clean, headers=headers, timeout=timeout)
            elif method_u == "POST":
                if auth:
                    resp = sess.post(url, data=body_text, headers=headers, timeout=timeout)
                else:
                    resp = sess.post(url, json=body_clean, headers=headers, timeout=timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method_u}")

            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Bybit response is not JSON object: {path}")
            ret_code = int(payload.get("retCode", 0))
            if ret_code == 0:
                return payload

            ret_msg = str(payload.get("retMsg", "Unknown error"))
            if auth and ret_code == 10002 and attempt + 1 < max_attempts:
                self._sync_server_time_offset()
                continue
            if ret_code == 10002:
                raise ValueError(
                    "Bybit API error retCode=10002 (timestamp/recv_window). "
                    f"retMsg={ret_msg}, path={path}. "
                    f"Please sync system clock or increase BYBIT_RECV_WINDOW_MS (current={self._recv_window})."
                )
            raise ValueError(f"Bybit API error retCode={ret_code}, retMsg={ret_msg}, path={path}")
        raise ValueError(f"Bybit request retry exhausted: path={path}")

    def get_latest_prices(self, symbols: list[str]) -> dict[str, float]:
        out: dict[str, float] = {}
        for sym in sorted({_clean_symbol(x) for x in symbols if _clean_symbol(x)}):
            payload = self._request(
                "GET",
                "/v5/market/tickers",
                params={"category": "linear", "symbol": sym},
                auth=False,
            )
            rows = payload.get("result", {}).get("list", [])
            if not isinstance(rows, list) or not rows:
                raise ValueError(f"Bybit ticker not found: {sym}")
            out[sym] = float(rows[0].get("lastPrice"))
        return out

    def get_wallet_total_equity(self) -> float:
        last_exc: Exception | None = None
        for account_type in ("UNIFIED", "CONTRACT"):
            try:
                payload = self._request(
                    "GET",
                    "/v5/account/wallet-balance",
                    params={"accountType": account_type},
                    auth=True,
                )
                rows = payload.get("result", {}).get("list", [])
                if not isinstance(rows, list) or not rows:
                    continue
                total_equity = float(rows[0].get("totalEquity", 0.0))
                if total_equity > 0:
                    return total_equity
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc is not None:
            raise last_exc
        raise ValueError("Unable to read account total equity from Bybit")

    def get_wallet_available_balance(self) -> float:
        last_exc: Exception | None = None
        for account_type in ("UNIFIED", "CONTRACT"):
            try:
                payload = self._request(
                    "GET",
                    "/v5/account/wallet-balance",
                    params={"accountType": account_type},
                    auth=True,
                )
                rows = payload.get("result", {}).get("list", [])
                if not isinstance(rows, list) or not rows:
                    continue
                first = rows[0] if isinstance(rows[0], dict) else {}
                candidates = [
                    first.get("totalAvailableBalance"),
                    first.get("availableToWithdraw"),
                ]
                margin_balance = self._to_decimal(first.get("totalMarginBalance", "0"), default="0")
                initial_margin = self._to_decimal(first.get("totalInitialMargin", "0"), default="0")
                candidates.append(margin_balance - initial_margin)
                for raw in candidates:
                    val = self._to_decimal(raw, default="0")
                    if val > 0:
                        return float(val)
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc is not None:
            raise last_exc
        raise ValueError("Unable to read available balance from Bybit")

    def _get_instrument_info(self, symbol: str) -> dict[str, Decimal]:
        sym = _clean_symbol(symbol)
        cached = self._instrument_cache.get(sym)
        if cached is not None:
            return dict(cached)

        payload = self._request(
            "GET",
            "/v5/market/instruments-info",
            params={"category": "linear", "symbol": sym},
            auth=False,
        )
        rows = payload.get("result", {}).get("list", [])
        if not isinstance(rows, list) or not rows:
            raise ValueError(f"Bybit instrument info not found: {sym}")
        first = rows[0] if isinstance(rows[0], dict) else {}
        lot = first.get("lotSizeFilter", {}) if isinstance(first.get("lotSizeFilter", {}), dict) else {}
        lev = first.get("leverageFilter", {}) if isinstance(first.get("leverageFilter", {}), dict) else {}
        qty_step = self._to_decimal(lot.get("qtyStep", "0.001"), default="0.001")
        min_order_qty = self._to_decimal(lot.get("minOrderQty", "0"), default="0")
        max_mkt_order_qty = self._to_decimal(
            lot.get("maxMktOrderQty", lot.get("maxOrderQty", "0")),
            default="0",
        )
        min_leverage = self._to_decimal(lev.get("minLeverage", "1"), default="1")
        max_leverage = self._to_decimal(lev.get("maxLeverage", "100"), default="100")
        leverage_step = self._to_decimal(lev.get("leverageStep", "0.1"), default="0.1")
        info = {
            "qty_step": qty_step,
            "min_order_qty": min_order_qty,
            "max_mkt_order_qty": max_mkt_order_qty,
            "min_leverage": min_leverage,
            "max_leverage": max_leverage,
            "leverage_step": leverage_step,
        }
        self._instrument_cache[sym] = dict(info)
        return info

    def _normalize_leverage_decimal(
        self,
        symbol: str,
        desired_leverage: Decimal,
    ) -> tuple[Decimal, dict[str, Decimal]]:
        info = self._get_instrument_info(symbol)
        min_lev = info["min_leverage"] if info["min_leverage"] > 0 else Decimal("1")
        max_lev = info["max_leverage"] if info["max_leverage"] > 0 else Decimal("100")
        step = info["leverage_step"] if info["leverage_step"] > 0 else Decimal("0.1")

        target = desired_leverage if desired_leverage > 0 else min_lev
        if target < min_lev:
            target = min_lev
        if step > 0:
            units = ((target - min_lev) / step).to_integral_value(rounding=ROUND_UP)
            normalized = min_lev + units * step
        else:
            normalized = target
        if normalized > max_lev:
            normalized = max_lev
        if normalized < min_lev:
            normalized = min_lev
        return normalized, info

    def set_symbol_leverage(self, *, symbol: str, leverage: Decimal) -> dict[str, Any]:
        sym = _clean_symbol(symbol)
        lev_text = self._fmt_decimal(leverage)
        try:
            payload = self._request(
                "POST",
                "/v5/position/set-leverage",
                json_body={
                    "category": "linear",
                    "symbol": sym,
                    "buyLeverage": lev_text,
                    "sellLeverage": lev_text,
                },
                auth=True,
            )
            result = payload.get("result", {})
            return {"symbol": sym, "leverage": lev_text, "result": result if isinstance(result, dict) else {}}
        except Exception as exc:
            msg = str(exc)
            if "retCode=110043" in msg or "not modified" in msg.lower():
                return {"symbol": sym, "leverage": lev_text, "result": {"unchanged": True}}
            raise

    def precheck_open_margin(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        entries: list[dict[str, Any]] = []
        total_notional = Decimal("0")
        total_margin = Decimal("0")
        total_fee = Decimal("0")
        adjustments: list[dict[str, Any]] = []
        fee_rate = Decimal("0.0006")
        safety_buffer_pct = Decimal("5")

        for row in rows:
            sym = _clean_symbol(row.get("asset", ""))
            if not sym:
                continue
            notional = self._to_decimal(row.get("notional", "0"), default="0")
            if notional <= 0:
                continue
            desired_lev = self._to_decimal(row.get("leverage", "1"), default="1")
            effective_lev, _ = self._normalize_leverage_decimal(sym, desired_lev)
            required_margin = notional / effective_lev if effective_lev > 0 else notional
            open_fee = notional * fee_rate
            total_notional += notional
            total_margin += required_margin
            total_fee += open_fee
            if effective_lev != desired_lev:
                adjustments.append(
                    {
                        "symbol": sym,
                        "requested_leverage": float(desired_lev),
                        "applied_leverage": float(effective_lev),
                    }
                )
            entries.append(
                {
                    "symbol": sym,
                    "notional": float(notional),
                    "requested_leverage": float(desired_lev),
                    "applied_leverage": float(effective_lev),
                    "required_margin": float(required_margin),
                    "estimated_open_fee": float(open_fee),
                }
            )

        required_before_buffer = total_margin + total_fee
        required_after_buffer = required_before_buffer * (Decimal("1") + safety_buffer_pct / Decimal("100"))
        available = Decimal(str(self.get_wallet_available_balance()))
        shortfall = required_after_buffer - available if available < required_after_buffer else Decimal("0")

        return {
            "ok": bool(available >= required_after_buffer),
            "available_balance": float(available),
            "required_margin_estimate": float(total_margin),
            "estimated_open_fee": float(total_fee),
            "required_before_buffer": float(required_before_buffer),
            "buffer_pct": float(safety_buffer_pct),
            "required_after_buffer": float(required_after_buffer),
            "shortfall": float(shortfall),
            "total_notional": float(total_notional),
            "rows": entries,
            "leverage_adjustments": adjustments,
        }

    def _normalize_qty_decimal(
        self,
        symbol: str,
        desired_qty: Decimal,
        *,
        enforce_min_qty: bool,
    ) -> tuple[Decimal, dict[str, Decimal]]:
        info = self._get_instrument_info(symbol)
        step = info["qty_step"]
        qty = self._floor_to_step(max(desired_qty, Decimal("0")), step)
        if qty <= 0:
            raise ValueError(f"Order qty became zero after step rounding: {symbol}")
        if enforce_min_qty and qty < info["min_order_qty"]:
            raise ValueError(
                f"Order qty too small for {symbol}: qty={self._fmt_decimal(qty)}, "
                f"min={self._fmt_decimal(info['min_order_qty'])}"
            )
        return qty, info

    @staticmethod
    def _split_qty(total_qty: Decimal, max_qty: Decimal, step: Decimal) -> list[Decimal]:
        if total_qty <= 0:
            return []
        if max_qty <= 0:
            return [total_qty]
        out: list[Decimal] = []
        remaining = total_qty
        while remaining > 0:
            chunk = min(remaining, max_qty)
            if step > 0:
                chunk = (chunk / step).to_integral_value(rounding=ROUND_DOWN) * step
            if chunk <= 0:
                break
            out.append(chunk)
            remaining -= chunk
        return out

    def place_market_order(
        self,
        *,
        symbol: str,
        side: Literal["Buy", "Sell"],
        qty: str,
        reduce_only: bool,
        position_idx: int | None = 0,
        order_link_id: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "category": "linear",
            "symbol": _clean_symbol(symbol),
            "side": str(side),
            "orderType": "Market",
            "qty": str(qty),
            "reduceOnly": bool(reduce_only),
            "timeInForce": "IOC",
        }
        if position_idx is not None:
            body["positionIdx"] = int(position_idx)
        if order_link_id:
            body["orderLinkId"] = str(order_link_id)[:36]
        payload = self._request(
            "POST",
            "/v5/order/create",
            json_body=body,
            auth=True,
        )
        result = payload.get("result", {}) if isinstance(payload.get("result", {}), dict) else {}
        return {
            "order_id": str(result.get("orderId", "")),
            "order_link_id": str(result.get("orderLinkId", "")),
        }

    def open_positions_from_plan(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        symbols = sorted({_clean_symbol(row.get("asset", "")) for row in rows if _clean_symbol(row.get("asset", ""))})
        if not symbols:
            raise ValueError("No symbols found in plan rows")
        prices = self.get_latest_prices(symbols)
        symbol_leverage_map: dict[str, Decimal] = {}
        leverage_adjustments: list[dict[str, Any]] = []
        for row in rows:
            sym = _clean_symbol(row.get("asset", ""))
            if not sym:
                continue
            desired_lev = self._to_decimal(row.get("leverage", "1"), default="1")
            applied_lev, _ = self._normalize_leverage_decimal(sym, desired_lev)
            prev = symbol_leverage_map.get(sym)
            if prev is None or applied_lev > prev:
                symbol_leverage_map[sym] = applied_lev
            if applied_lev != desired_lev:
                leverage_adjustments.append(
                    {
                        "symbol": sym,
                        "requested_leverage": float(desired_lev),
                        "applied_leverage": float(applied_lev),
                    }
                )
        leverage_results: list[dict[str, Any]] = []
        for sym in sorted(symbol_leverage_map):
            leverage_results.append(self.set_symbol_leverage(symbol=sym, leverage=symbol_leverage_map[sym]))
        orders: list[dict[str, Any]] = []
        for idx, row in enumerate(rows, start=1):
            sym = _clean_symbol(row.get("asset", ""))
            if not sym:
                continue
            px = float(prices.get(sym, 0.0))
            if px <= 0:
                raise ValueError(f"Invalid latest price for {sym}")
            notional = float(row.get("notional", 0.0))
            if notional <= 0:
                continue
            desired_qty = Decimal(str(notional)) / Decimal(str(px))
            qty_dec, info = self._normalize_qty_decimal(sym, desired_qty, enforce_min_qty=True)
            side = "Buy" if str(row.get("direction", "long")).lower().strip() == "long" else "Sell"
            chunks = self._split_qty(qty_dec, info["max_mkt_order_qty"], info["qty_step"])
            if not chunks:
                raise ValueError(f"Unable to split quantity for {sym}")
            for chunk_idx, chunk_qty in enumerate(chunks, start=1):
                qty_text = self._fmt_decimal(chunk_qty)
                order = self.place_market_order(
                    symbol=sym,
                    side=side,
                    qty=qty_text,
                    reduce_only=False,
                    position_idx=0,
                    order_link_id=f"liveopen-{int(time.time() * 1000)}-{idx}-{chunk_idx}",
                )
                orders.append(
                    {
                        "symbol": sym,
                        "side": side,
                        "qty": qty_text,
                        "reduce_only": False,
                        **order,
                    }
                )
        return {
            "entry_prices": prices,
            "orders": orders,
            "leverage_set": leverage_results,
            "leverage_adjustments": leverage_adjustments,
        }

    def get_positions(
        self,
        *,
        settle_coin: str = "USDT",
        symbols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        symbols_clean = sorted({_clean_symbol(x) for x in (symbols or []) if _clean_symbol(x)})
        if symbols_clean:
            for sym in symbols_clean:
                payload = self._request(
                    "GET",
                    "/v5/position/list",
                    params={"category": "linear", "symbol": sym},
                    auth=True,
                )
                rows = payload.get("result", {}).get("list", [])
                if isinstance(rows, list):
                    out.extend([dict(x) for x in rows if isinstance(x, dict)])
            return out

        payload = self._request(
            "GET",
            "/v5/position/list",
            params={"category": "linear", "settleCoin": str(settle_coin or "USDT").upper()},
            auth=True,
        )
        rows = payload.get("result", {}).get("list", [])
        if isinstance(rows, list):
            out.extend([dict(x) for x in rows if isinstance(x, dict)])
        return out

    def get_portfolio_unrealized_pnl(self, *, symbols: list[str]) -> dict[str, Any]:
        target_symbols = sorted({_clean_symbol(x) for x in (symbols or []) if _clean_symbol(x)})
        if not target_symbols:
            return {"unrealized_pnl": 0.0, "positions": []}

        positions = self.get_positions(settle_coin="USDT", symbols=target_symbols)
        target_set = set(target_symbols)
        rows: list[dict[str, Any]] = []
        total_unrealized = Decimal("0")

        for pos in positions:
            sym = _clean_symbol(pos.get("symbol", ""))
            if not sym or sym not in target_set:
                continue
            size = self._to_decimal(pos.get("size", "0"), default="0")
            if size <= 0:
                continue
            side_raw = str(pos.get("side", "")).lower().strip()
            if side_raw not in {"buy", "sell"}:
                continue
            unrealized = self._to_decimal(pos.get("unrealisedPnl", "0"), default="0")
            total_unrealized += unrealized
            rows.append(
                {
                    "symbol": sym,
                    "side": side_raw,
                    "size": float(size),
                    "avg_price": float(self._to_decimal(pos.get("avgPrice", "0"), default="0")),
                    "mark_price": float(self._to_decimal(pos.get("markPrice", "0"), default="0")),
                    "unrealized_pnl": float(unrealized),
                }
            )

        return {"unrealized_pnl": float(total_unrealized), "positions": rows}

    def close_all_positions(self, *, symbols: list[str] | None = None) -> dict[str, Any]:
        target_symbols = sorted({_clean_symbol(x) for x in (symbols or []) if _clean_symbol(x)})
        target_set = set(target_symbols)
        positions = self.get_positions(settle_coin="USDT", symbols=target_symbols or None)
        orders: list[dict[str, Any]] = []
        ignored_errors: list[dict[str, str]] = []
        for idx, pos in enumerate(positions, start=1):
            sym = _clean_symbol(pos.get("symbol", ""))
            if not sym:
                continue
            if target_set and sym not in target_set:
                continue
            size_dec = self._to_decimal(pos.get("size", "0"), default="0")
            if size_dec <= 0:
                continue
            side_raw = str(pos.get("side", "")).lower().strip()
            close_side: Literal["Buy", "Sell"] = "Sell" if side_raw == "buy" else "Buy"
            qty_dec, info = self._normalize_qty_decimal(sym, size_dec, enforce_min_qty=False)
            chunks = self._split_qty(qty_dec, info["max_mkt_order_qty"], info["qty_step"])
            if not chunks:
                continue
            try:
                pos_idx = int(pos.get("positionIdx", 0))
            except Exception:
                pos_idx = 0
            for chunk_idx, chunk_qty in enumerate(chunks, start=1):
                qty_text = self._fmt_decimal(chunk_qty)
                try:
                    order = self.place_market_order(
                        symbol=sym,
                        side=close_side,
                        qty=qty_text,
                        reduce_only=True,
                        position_idx=pos_idx,
                        order_link_id=f"liveclose-{int(time.time() * 1000)}-{idx}-{chunk_idx}",
                    )
                    orders.append(
                        {
                            "symbol": sym,
                            "side": close_side,
                            "qty": qty_text,
                            "reduce_only": True,
                            **order,
                        }
                    )
                except Exception as exc:
                    msg = str(exc)
                    if "retCode=110017" in msg or "position is zero" in msg.lower():
                        ignored_errors.append({"symbol": sym, "error": msg})
                        break
                    raise
        return {"closed_count": len(orders), "orders": orders, "ignored_errors": ignored_errors}

    def cancel_all_orders(self, *, settle_coin: str = "USDT") -> dict[str, Any]:
        payload = self._request(
            "POST",
            "/v5/order/cancel-all",
            json_body={"category": "linear", "settleCoin": str(settle_coin or "USDT").upper()},
            auth=True,
        )
        result = payload.get("result", {})
        return {"result": result if isinstance(result, dict) else {}}


class ExchangeAdapterRegistry:
    def __init__(
        self,
        *,
        bybit_base_url: str | None = None,
        binance_futures_base_url: str | None = None,
        bybit_recv_window_ms: int | None = None,
    ) -> None:
        self._bybit_base_url = str(bybit_base_url or settings.bybit_base_url).rstrip("/")
        self._binance_base_url = str(binance_futures_base_url or settings.binance_futures_base_url).rstrip("/")
        self._bybit_recv_window_ms = int(bybit_recv_window_ms or settings.bybit_recv_window_ms)
        self._bybit_public = BybitV5Client(
            api_key="",
            api_secret="",
            base_url=self._bybit_base_url,
            recv_window_ms=self._bybit_recv_window_ms,
        )
        self._binance_public = BinanceFuturesPriceAdapter(base_url=self._binance_base_url)

    def get_latest_prices(self, *, exchange: Literal["bybit", "binance"], symbols: list[str]) -> dict[str, float]:
        ex = str(exchange or "").lower().strip()
        if ex == "bybit":
            return self._bybit_public.get_latest_prices(symbols)
        if ex == "binance":
            return self._binance_public.get_latest_prices(symbols)
        raise ValueError(f"Unsupported exchange: {exchange}")

    def create_live_executor(
        self,
        *,
        exchange: Literal["bybit", "binance"],
        api_key: str,
        api_secret: str,
    ) -> BybitV5Client:
        ex = str(exchange or "").lower().strip()
        if ex == "bybit":
            return BybitV5Client(
                api_key=api_key,
                api_secret=api_secret,
                base_url=self._bybit_base_url,
                recv_window_ms=self._bybit_recv_window_ms,
            )
        if ex == "binance":
            raise ValueError("Live execution for Binance is not implemented yet.")
        raise ValueError(f"Unsupported exchange: {exchange}")
