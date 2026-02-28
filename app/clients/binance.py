from __future__ import annotations

import hashlib
import hmac
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, local
from urllib.parse import urlencode

import pandas as pd
import requests

from app.config import settings


class BinanceClient:
    FUTURES_BASE = "https://fapi.binance.com"
    _cache_lock = Lock()
    _perp_kline_cache: dict[tuple[str, int, int], pd.Series] = {}
    _funding_cache: dict[tuple[str, int, int], pd.Series] = {}
    _universe_cache: dict[int, tuple[float, list[str]]] = {}
    _exchange_info_cache: dict[str, tuple[float, dict[str, int]]] = {}
    _thread_local = local()

    def __init__(self, api_key: str | None = None, api_secret: str | None = None) -> None:
        self.api_key = (api_key or settings.binance_api_key or "").strip()
        self.api_secret = (api_secret or settings.binance_api_secret or "").strip()
        self._disk_cache_enabled = bool(settings.market_data_disk_cache_enabled)
        self._cache_dir = Path(settings.market_data_cache_dir).expanduser()
        if self._disk_cache_enabled:
            (self._cache_dir / "kline").mkdir(parents=True, exist_ok=True)
            (self._cache_dir / "funding").mkdir(parents=True, exist_ok=True)

    def _session(self) -> requests.Session:
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update({"User-Agent": "carry-optimizer/1.0"})
            self._thread_local.session = sess
        return sess

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        out = "".join(ch for ch in str(symbol or "").upper() if ch.isalnum() or ch in {"_", "-"})
        return out or "UNKNOWN"

    def _cache_file(self, bucket: str, symbol: str, start_ms: int, end_ms: int) -> Path:
        filename = f"{self._safe_symbol(symbol)}_{start_ms}_{end_ms}.pkl"
        return self._cache_dir / bucket / filename

    def _load_series_from_disk(self, bucket: str, symbol: str, start_ms: int, end_ms: int) -> pd.Series | None:
        if not self._disk_cache_enabled:
            return None
        path = self._cache_file(bucket, symbol, start_ms, end_ms)
        if not path.exists():
            return None
        try:
            series = pd.read_pickle(path)
            if not isinstance(series, pd.Series):
                return None
            series = series.sort_index()
            series = series[~series.index.duplicated(keep="last")]
            return series.astype(float, copy=False)
        except Exception:
            return None

    def _save_series_to_disk(
        self,
        bucket: str,
        symbol: str,
        start_ms: int,
        end_ms: int,
        series: pd.Series,
    ) -> None:
        if not self._disk_cache_enabled:
            return
        path = self._cache_file(bucket, symbol, start_ms, end_ms)
        tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        try:
            series.to_pickle(tmp_path)
            os.replace(tmp_path, path)
        except Exception:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    def get_perp_hourly_close(self, symbol: str, start_dt: datetime, end_dt: datetime) -> pd.Series:
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        cache_key = (symbol, start_ms, end_ms)
        with self._cache_lock:
            cached = self._perp_kline_cache.get(cache_key)
        if cached is not None:
            return cached.copy()
        disk_cached = self._load_series_from_disk("kline", symbol, start_ms, end_ms)
        if disk_cached is not None:
            with self._cache_lock:
                self._perp_kline_cache[cache_key] = disk_cached.copy()
            return disk_cached.copy()

        cursor = start_ms
        rows: list[tuple[pd.Timestamp, float]] = []
        sess = self._session()

        while cursor < end_ms:
            params = {
                "symbol": symbol,
                "interval": "1h",
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            }
            resp = sess.get(f"{self.FUTURES_BASE}/fapi/v1/klines", params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if not payload:
                break

            for entry in payload:
                open_time = pd.to_datetime(entry[0], unit="ms", utc=True)
                close_price = float(entry[4])
                rows.append((open_time, close_price))

            last_open = payload[-1][0]
            cursor = int(last_open) + 3600_000
            if len(payload) < 1000:
                break
            time.sleep(0.05)

        if not rows:
            raise ValueError(f"No perp kline data returned for {symbol}")

        series = pd.Series({ts: px for ts, px in rows}, dtype=float).sort_index()
        series = series[~series.index.duplicated(keep="last")]
        self._save_series_to_disk("kline", symbol, start_ms, end_ms, series)
        with self._cache_lock:
            self._perp_kline_cache[cache_key] = series.copy()
        return series

    def get_perp_funding_rates(self, symbol: str, start_dt: datetime, end_dt: datetime) -> pd.Series:
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        cache_key = (symbol, start_ms, end_ms)
        with self._cache_lock:
            cached = self._funding_cache.get(cache_key)
        if cached is not None:
            return cached.copy()
        disk_cached = self._load_series_from_disk("funding", symbol, start_ms, end_ms)
        if disk_cached is not None:
            with self._cache_lock:
                self._funding_cache[cache_key] = disk_cached.copy()
            return disk_cached.copy()

        cursor = start_ms
        rows: list[tuple[pd.Timestamp, float]] = []
        sess = self._session()

        while cursor < end_ms:
            params = {
                "symbol": symbol,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            }
            resp = sess.get(f"{self.FUTURES_BASE}/fapi/v1/fundingRate", params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if not payload:
                break

            for entry in payload:
                ts = pd.to_datetime(int(entry["fundingTime"]), unit="ms", utc=True).floor("h")
                rows.append((ts, float(entry["fundingRate"])))

            last_ts = int(payload[-1]["fundingTime"])
            cursor = last_ts + 1
            if len(payload) < 1000:
                break
            time.sleep(0.05)

        if not rows:
            empty = pd.Series(dtype=float)
            self._save_series_to_disk("funding", symbol, start_ms, end_ms, empty)
            with self._cache_lock:
                self._funding_cache[cache_key] = empty.copy()
            return empty

        series = pd.Series({ts: rate for ts, rate in rows}, dtype=float).sort_index()
        series = series[~series.index.duplicated(keep="last")]
        self._save_series_to_disk("funding", symbol, start_ms, end_ms, series)
        with self._cache_lock:
            self._funding_cache[cache_key] = series.copy()
        return series

    def get_perp_taker_fee(self, symbol: str) -> float:
        api_key = self.api_key
        api_secret = self.api_secret
        if not api_key or not api_secret:
            return settings.default_binance_perp_taker_fee

        ts = int(time.time() * 1000)
        params = {"symbol": symbol, "timestamp": ts}
        query = urlencode(params)
        signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        params["signature"] = signature
        headers = {"X-MBX-APIKEY": api_key}

        try:
            resp = self._session().get(
                f"{self.FUTURES_BASE}/fapi/v1/commissionRate",
                params=params,
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
            payload = resp.json()
            return float(payload.get("takerCommissionRate", settings.default_binance_perp_taker_fee))
        except Exception:
            return settings.default_binance_perp_taker_fee

    def _get_exchange_info_onboard_map(self) -> dict[str, int]:
        ttl = 600.0
        now = time.time()
        cache_key = "perp_usdt"
        with self._cache_lock:
            cached = self._exchange_info_cache.get(cache_key)
        if cached and now - cached[0] < ttl:
            return dict(cached[1])

        resp = self._session().get(f"{self.FUTURES_BASE}/fapi/v1/exchangeInfo", timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        onboard_map: dict[str, int] = {}
        for item in payload.get("symbols", []):
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("status") != "TRADING":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
            sym = item.get("symbol")
            if sym:
                onboard_raw = item.get("onboardDate")
                try:
                    onboard_map[sym] = int(onboard_raw or 0)
                except Exception:
                    onboard_map[sym] = 0

        with self._cache_lock:
            self._exchange_info_cache[cache_key] = (now, dict(onboard_map))
        return onboard_map

    def _get_exchange_info_symbols(self) -> set[str]:
        return set(self._get_exchange_info_onboard_map().keys())

    def filter_symbols_by_start_date(
        self,
        symbols: list[str],
        start_dt: datetime,
    ) -> tuple[list[str], list[str]]:
        start_ms = int(start_dt.timestamp() * 1000)
        onboard_map = self._get_exchange_info_onboard_map()
        allowed: list[str] = []
        rejected: list[str] = []
        for raw in symbols:
            sym = str(raw or "").upper().strip()
            if not sym:
                continue
            onboard_ms = int(onboard_map.get(sym, 0))
            if onboard_ms <= start_ms:
                allowed.append(sym)
            else:
                rejected.append(sym)
        return allowed, rejected

    def get_top_volume_perps(self, limit: int = 100) -> list[str]:
        limit = max(int(limit), 1)
        ttl = 300.0
        now = time.time()
        with self._cache_lock:
            cached = self._universe_cache.get(limit)
        if cached and now - cached[0] < ttl:
            return list(cached[1])

        allowed = self._get_exchange_info_symbols()
        resp = self._session().get(f"{self.FUTURES_BASE}/fapi/v1/ticker/24hr", timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        ranked: list[tuple[str, float]] = []
        for item in payload:
            sym = item.get("symbol")
            if sym not in allowed:
                continue
            try:
                quote_vol = float(item.get("quoteVolume", 0.0))
            except Exception:
                quote_vol = 0.0
            ranked.append((sym, quote_vol))

        ranked.sort(key=lambda x: x[1], reverse=True)
        top = [sym for sym, _ in ranked[:limit]]

        with self._cache_lock:
            self._universe_cache[limit] = (now, list(top))
        return top

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(timezone.utc)
