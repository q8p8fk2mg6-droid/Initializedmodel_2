from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable


@dataclass(frozen=True)
class PortfolioLeg:
    asset: str
    weight: float
    direction: int  # 1 for long, -1 for short
    leverage: float | None = None


@dataclass(frozen=True)
class PortfolioSpec:
    legs: tuple[PortfolioLeg, ...]

    def assets(self) -> list[str]:
        return [leg.asset for leg in self.legs]

    def weights(self) -> list[float]:
        return [leg.weight for leg in self.legs]

    def directions(self) -> list[int]:
        return [leg.direction for leg in self.legs]

    def as_dict_list(self) -> list[dict[str, str | float]]:
        out = []
        for leg in self.legs:
            item: dict[str, str | float] = {
                "asset": leg.asset,
                "weight": leg.weight,
                "direction": "long" if leg.direction > 0 else "short",
            }
            if leg.leverage is not None:
                item["leverage"] = float(leg.leverage)
            out.append(item)
        return out


def normalize_portfolio(legs: Iterable[PortfolioLeg]) -> PortfolioSpec:
    normalized: list[PortfolioLeg] = []
    total = 0.0
    for leg in legs:
        asset = str(leg.asset).upper().strip()
        weight = float(leg.weight)
        direction = 1 if int(leg.direction) >= 0 else -1
        leverage_raw = getattr(leg, "leverage", None)
        leverage: float | None = None
        if leverage_raw is not None:
            try:
                lev_val = float(leverage_raw)
                if math.isfinite(lev_val) and lev_val > 0:
                    leverage = max(lev_val, 1.0)
            except Exception:
                leverage = None
        if not asset or weight <= 0:
            continue
        normalized.append(PortfolioLeg(asset=asset, weight=weight, direction=direction, leverage=leverage))
        total += weight
    if total <= 0:
        raise ValueError("Portfolio weights must be positive")

    scaled = [
        PortfolioLeg(
            asset=leg.asset,
            weight=leg.weight / total,
            direction=leg.direction,
            leverage=leg.leverage,
        )
        for leg in normalized
    ]
    # Canonical order for deduplication and stable output.
    scaled.sort(key=lambda x: x.asset)
    return PortfolioSpec(legs=tuple(scaled))


def generate_weight_splits(asset_count: int, step_pct: float) -> list[list[float]]:
    if asset_count <= 0:
        raise ValueError("asset_count must be > 0")
    step = float(step_pct)
    if step <= 0:
        raise ValueError("step_pct must be > 0")
    units_total = int(round(100.0 / step))
    if abs(units_total * step - 100.0) > 1e-6:
        raise ValueError("step_pct must divide 100 evenly")
    if units_total < asset_count:
        return []

    # Generate only strictly-positive weight splits to keep search space manageable.
    splits: list[list[int]] = []

    def _dfs(remaining_units: int, slots: int, prefix: list[int]) -> None:
        if slots == 1:
            if remaining_units >= 1:
                splits.append(prefix + [remaining_units])
            return
        min_units = 1
        max_units = remaining_units - (slots - 1)
        for i in range(min_units, max_units + 1):
            _dfs(remaining_units - i, slots - 1, prefix + [i])

    _dfs(units_total, asset_count, [])
    return [[v * step / 100.0 for v in combo] for combo in splits]


def portfolio_to_vector(portfolio: PortfolioSpec, universe: list[str]) -> list[float]:
    index = {sym: i for i, sym in enumerate(universe)}
    vec = [0.0 for _ in universe]
    for leg in portfolio.legs:
        idx = index.get(leg.asset)
        if idx is None:
            continue
        vec[idx] = float(leg.weight) * (1.0 if leg.direction > 0 else -1.0)
    return vec


def portfolio_key(portfolio: PortfolioSpec) -> tuple:
    return tuple(
        (
            leg.asset,
            round(leg.weight, 6),
            leg.direction,
            None if leg.leverage is None else round(float(leg.leverage), 6),
        )
        for leg in portfolio.legs
    )
