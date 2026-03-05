from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

import numpy as np

from app.services.data_loader import MarketData
from app.services.portfolio import PortfolioSpec

ANNUAL_FACTOR = 365.0 * 24.0
MAX_CURVE_POINTS = 320


@dataclass(frozen=True)
class StrategyParams:
    rehedge_hours: int
    rebalance_threshold_pct: float
    long_leverage: float
    short_leverage: float


class PortfolioBacktester:
    def run(
        self,
        market_data: MarketData,
        portfolio: PortfolioSpec,
        params: StrategyParams,
        initial_capital_usdt: float,
        tp_pct: float | None = None,
        sl_pct: float | None = None,
    ) -> dict[str, Any]:
        prices = market_data.prices.copy()
        funding = market_data.funding.copy()
        assets = list(prices.columns)

        if len(prices) < 3:
            raise ValueError("Not enough market data to run backtest")

        price_arr = prices.to_numpy(dtype=float, copy=False)
        funding_arr = funding.reindex(index=prices.index, columns=assets, fill_value=0.0).to_numpy(dtype=float, copy=False)
        timestamps = prices.index

        weight_map = {leg.asset: float(leg.weight) for leg in portfolio.legs}
        direction_map = {leg.asset: int(leg.direction) for leg in portfolio.legs}
        leg_leverage_map = {
            leg.asset: float(leg.leverage)
            for leg in portfolio.legs
            if leg.leverage is not None and float(leg.leverage) > 0
        }

        weights = np.array([float(weight_map.get(asset, 0.0)) for asset in assets], dtype=float)
        directions = np.array([float(1.0 if direction_map.get(asset, 1) > 0 else -1.0) for asset in assets], dtype=float)

        long_leverage = max(float(params.long_leverage), 1.0)
        short_leverage = max(float(params.short_leverage), 1.0)
        default_leverage_vec = np.where(directions > 0, long_leverage, short_leverage)
        leverage_override = np.array(
            [
                float(leg_leverage_map.get(asset, 0.0))
                for asset in assets
            ],
            dtype=float,
        )
        leverage_vec = np.where(leverage_override > 0.0, leverage_override, default_leverage_vec)

        nav = float(initial_capital_usdt)
        trading_fees = 0.0
        funding_income = 0.0
        rehedge_count = 0

        first_prices = price_arr[0]
        target_notional = nav * weights * leverage_vec * directions
        open_mask = (np.abs(target_notional) > 0.0) & (first_prices > 0.0)
        qty = np.zeros(len(assets), dtype=float)
        if np.any(open_mask):
            qty[open_mask] = target_notional[open_mask] / first_prices[open_mask]
            init_fee = float(np.abs(target_notional[open_mask]).sum()) * market_data.fee_rate
            trading_fees += init_fee
            nav -= init_fee

        last_rehedge_ts = timestamps[0]
        last_idx = len(timestamps) - 1
        stride = self._curve_stride(len(timestamps), MAX_CURVE_POINTS)
        equity_curve: list[list[float | int]] = [[int(timestamps[0].timestamp() * 1000), float(nav)]]

        start_nav = float(nav)
        prev_nav = float(nav)
        running_max = float(nav)
        max_drawdown = 0.0
        returns_sum = 0.0
        returns_sq_sum = 0.0
        returns_count = 1
        periods = 0

        threshold_bps = float(params.rebalance_threshold_pct) * 100.0
        tp_pct_val = float(tp_pct) if tp_pct is not None and np.isfinite(tp_pct) and float(tp_pct) > 0.0 else None
        sl_pct_val = float(sl_pct) if sl_pct is not None and np.isfinite(sl_pct) and float(sl_pct) > 0.0 else None
        base_nav = max(float(initial_capital_usdt), 1e-9)
        tp_target_nav = base_nav * (1.0 + tp_pct_val / 100.0) if tp_pct_val is not None else None
        sl_target_nav = base_nav * (1.0 - sl_pct_val / 100.0) if sl_pct_val is not None else None
        stop_triggered = False
        exit_reason = "end"
        exit_timestamp_ms = int(timestamps[last_idx].timestamp() * 1000)

        for i in range(1, len(timestamps)):
            prev_row = price_arr[i - 1]
            cur_row = price_arr[i]
            ts = timestamps[i]

            pnl = float(np.dot(qty, (cur_row - prev_row)))
            notional_prev = qty * prev_row
            funding_pnl = float(-np.dot(notional_prev, funding_arr[i]))

            nav += pnl + funding_pnl
            funding_income += funding_pnl

            delta_hours = (ts - last_rehedge_ts).total_seconds() / 3600.0
            if delta_hours >= params.rehedge_hours:
                target_notional = nav * weights * leverage_vec * directions
                current_notional = qty * cur_row

                positive_mask = np.abs(target_notional) > 0.0
                deviation_bps = np.zeros_like(target_notional)
                deviation_bps[positive_mask] = (
                    np.abs(current_notional[positive_mask] - target_notional[positive_mask])
                    / np.abs(target_notional[positive_mask])
                    * 10_000.0
                )

                rebalance_mask = positive_mask & (deviation_bps >= threshold_bps) & (cur_row > 0.0)
                if np.any(rebalance_mask):
                    diff = np.zeros_like(target_notional)
                    diff[rebalance_mask] = target_notional[rebalance_mask] - current_notional[rebalance_mask]
                    trade_notional = float(np.abs(diff[rebalance_mask]).sum())
                    fee = trade_notional * market_data.fee_rate
                    trading_fees += fee
                    nav -= fee
                    qty[rebalance_mask] += diff[rebalance_mask] / cur_row[rebalance_mask]

                rehedge_count += 1
                last_rehedge_ts = ts

            trigger_reason: str | None = None
            if tp_target_nav is not None and nav >= tp_target_nav:
                trigger_reason = "tp"
            elif sl_target_nav is not None and nav <= sl_target_nav:
                trigger_reason = "sl"

            if trigger_reason is not None:
                close_mask = (np.abs(qty) > 0.0) & (cur_row > 0.0)
                if np.any(close_mask):
                    close_notional = float(np.abs(qty[close_mask] * cur_row[close_mask]).sum())
                    close_fee = close_notional * market_data.fee_rate
                    trading_fees += close_fee
                    nav -= close_fee
                    qty[close_mask] = 0.0
                stop_triggered = True
                exit_reason = trigger_reason
                exit_timestamp_ms = int(ts.timestamp() * 1000)

            ret = 0.0
            if prev_nav != 0.0:
                ret = nav / prev_nav - 1.0
                if not np.isfinite(ret):
                    ret = 0.0
            returns_sum += ret
            returns_sq_sum += ret * ret
            returns_count += 1
            periods += 1
            prev_nav = nav

            if nav > running_max:
                running_max = nav
            if running_max > 0.0:
                drawdown = (nav - running_max) / running_max
                if drawdown < max_drawdown:
                    max_drawdown = float(drawdown)

            if stop_triggered or (i % stride == 0) or (i == last_idx):
                equity_curve.append([int(ts.timestamp() * 1000), float(nav)])
            if stop_triggered:
                break

        return self._finalize_metrics_fast(
            params={
                "rehedge_hours": params.rehedge_hours,
                "rebalance_threshold_pct": params.rebalance_threshold_pct,
                "long_leverage": long_leverage,
                "short_leverage": short_leverage,
                "leverage_mode": "per_leg" if leg_leverage_map else "uniform_by_side",
            },
            funding_income=funding_income,
            trading_fees=trading_fees,
            rehedge_count=rehedge_count,
            equity_curve=equity_curve,
            start_nav=start_nav,
            end_nav=float(nav),
            periods=periods,
            returns_sum=returns_sum,
            returns_sq_sum=returns_sq_sum,
            returns_count=returns_count,
            max_drawdown=max_drawdown,
            tp_pct=tp_pct_val,
            sl_pct=sl_pct_val,
            stop_triggered=stop_triggered,
            exit_reason=exit_reason,
            exit_timestamp_ms=exit_timestamp_ms,
        )

    @staticmethod
    def _curve_stride(point_count: int, max_points: int) -> int:
        if point_count <= max_points:
            return 1
        return max(int(math.ceil((point_count - 1) / max(max_points - 1, 1))), 1)

    @staticmethod
    def _finalize_metrics_fast(
        *,
        params: dict[str, float | int],
        funding_income: float,
        trading_fees: float,
        rehedge_count: int,
        equity_curve: list[list[float | int]],
        start_nav: float,
        end_nav: float,
        periods: int,
        returns_sum: float,
        returns_sq_sum: float,
        returns_count: int,
        max_drawdown: float,
        tp_pct: float | None,
        sl_pct: float | None,
        stop_triggered: bool,
        exit_reason: str,
        exit_timestamp_ms: int,
    ) -> dict[str, Any]:
        start_nav_safe = max(float(start_nav), 1e-9)
        total_return = float(end_nav / start_nav_safe - 1.0)

        if end_nav <= 0:
            annualized_return = -1.0
        else:
            annualized_return = float((1.0 + total_return) ** (ANNUAL_FACTOR / max(int(periods), 1)) - 1.0)

        n = max(int(returns_count), 1)
        mean_ret = returns_sum / n
        var_ret = max(returns_sq_sum / n - mean_ret * mean_ret, 0.0)
        std_ret = float(np.sqrt(var_ret))

        annualized_vol = float(std_ret * np.sqrt(ANNUAL_FACTOR))
        sharpe = float((mean_ret / std_ret) * np.sqrt(ANNUAL_FACTOR)) if std_ret > 0 else 0.0

        return {
            "params": params,
            "annualized_return": annualized_return,
            "total_return": total_return,
            "annualized_volatility": annualized_vol,
            "sharpe": sharpe,
            "max_drawdown": float(max_drawdown),
            "funding_income": float(funding_income),
            "trading_fees": float(trading_fees),
            "rehedge_count": rehedge_count,
            "equity_curve": equity_curve,
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "stop_triggered": bool(stop_triggered),
            "exit_reason": "end" if not stop_triggered else str(exit_reason),
            "exit_timestamp_ms": int(exit_timestamp_ms),
            "exit_nav": float(end_nav),
        }
