from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Iterable

import pandas as pd

from app.clients.binance import BinanceClient


@dataclass
class MarketData:
    prices: pd.DataFrame
    funding: pd.DataFrame
    fee_rate: float


@dataclass
class UniverseMarketData:
    price_series: dict[str, pd.Series]
    funding_series: dict[str, pd.Series]
    fee_by_symbol: dict[str, float]


class MarketDataLoader:
    START_COVERAGE_MAX_DELAY = pd.Timedelta(hours=1)

    def __init__(self, binance_client: BinanceClient) -> None:
        self.binance = binance_client

    @staticmethod
    def _normalize_symbols(symbols: Iterable[str]) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for raw in symbols:
            sym = str(raw or "").upper().strip()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            ordered.append(sym)
        return ordered

    def load_universe(self, symbols: list[str], start_date: date, end_date: date) -> UniverseMarketData:
        ordered_symbols = self._normalize_symbols(symbols)
        if not ordered_symbols:
            raise ValueError("symbols list is empty")

        start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
        end_dt = datetime.combine(end_date, time.max, tzinfo=timezone.utc)
        start_ts = pd.Timestamp(start_dt)

        price_series: dict[str, pd.Series] = {}
        funding_series: dict[str, pd.Series] = {}
        fee_by_symbol: dict[str, float] = {}

        # Preload once per symbol to avoid repeated network calls during candidate evaluation.
        for symbol in ordered_symbols:
            try:
                series = self.binance.get_perp_hourly_close(symbol, start_dt, end_dt)
            except Exception:
                continue
            if series.empty:
                continue
            first_ts = pd.Timestamp(series.index[0])
            if first_ts.tzinfo is None:
                first_ts = first_ts.tz_localize("UTC")
            else:
                first_ts = first_ts.tz_convert("UTC")
            # Enforce symbol availability at requested start date.
            if first_ts > start_ts + self.START_COVERAGE_MAX_DELAY:
                continue
            price_series[symbol] = series

        if not price_series:
            raise ValueError(
                "No market data available at the selected start date. "
                "Please use symbols listed before the start date or move start_date later."
            )

        for symbol in price_series.keys():
            try:
                funding_series[symbol] = self.binance.get_perp_funding_rates(symbol, start_dt, end_dt)
            except Exception:
                funding_series[symbol] = pd.Series(dtype=float)
            fee_by_symbol[symbol] = self.binance.get_perp_taker_fee(symbol)

        return UniverseMarketData(
            price_series=price_series,
            funding_series=funding_series,
            fee_by_symbol=fee_by_symbol,
        )

    def slice_market_data(self, universe_data: UniverseMarketData, symbols: list[str]) -> MarketData:
        ordered_symbols = self._normalize_symbols(symbols)
        if not ordered_symbols:
            raise ValueError("symbols list is empty")

        missing = [sym for sym in ordered_symbols if sym not in universe_data.price_series]
        if missing:
            raise ValueError(f"Missing preloaded market data for symbols: {', '.join(missing)}")

        prices = pd.concat(
            {sym: universe_data.price_series[sym] for sym in ordered_symbols},
            axis=1,
            join="inner",
        ).sort_index()
        prices = prices[~prices.index.duplicated(keep="last")]
        if len(prices) < 3:
            raise ValueError("Not enough market data to run backtest")

        funding = pd.DataFrame(index=prices.index, columns=ordered_symbols, dtype=float).fillna(0.0)
        for symbol in ordered_symbols:
            series = universe_data.funding_series.get(symbol)
            if series is not None and not series.empty:
                funding[symbol] = series.reindex(funding.index, fill_value=0.0).astype(float)

        fee_rates = [float(universe_data.fee_by_symbol.get(symbol, 0.0)) for symbol in ordered_symbols]
        fee_rate = float(sum(fee_rates) / len(fee_rates)) if fee_rates else 0.0
        return MarketData(prices=prices, funding=funding, fee_rate=fee_rate)

    def load(self, symbols: list[str], start_date: date, end_date: date) -> MarketData:
        universe_data = self.load_universe(symbols, start_date, end_date)
        return self.slice_market_data(universe_data, symbols)
