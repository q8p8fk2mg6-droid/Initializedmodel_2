from __future__ import annotations

from dataclasses import dataclass

from app.services.portfolio import PortfolioSpec


@dataclass(frozen=True)
class PositionPlanRow:
    asset: str
    direction: str
    weight: float
    margin: float
    notional: float
    leverage: float


@dataclass(frozen=True)
class PositionPlan:
    total_capital_usdt: float
    total_margin_used: float
    total_long_notional: float
    total_short_notional: float
    rows: list[PositionPlanRow]


def build_position_plan(
    *,
    total_capital_usdt: float,
    portfolio: PortfolioSpec,
    long_leverage: float,
    short_leverage: float,
) -> PositionPlan:
    total = max(float(total_capital_usdt), 0.0)
    long_lev = max(float(long_leverage), 1.0)
    short_lev = max(float(short_leverage), 1.0)

    rows: list[PositionPlanRow] = []
    total_long_notional = 0.0
    total_short_notional = 0.0

    for leg in portfolio.legs:
        margin = total * float(leg.weight)
        is_long = leg.direction > 0
        if leg.leverage is not None:
            lev = max(float(leg.leverage), 1.0)
        else:
            lev = long_lev if is_long else short_lev
        notional = margin * lev
        if is_long:
            total_long_notional += notional
        else:
            total_short_notional += notional
        rows.append(
            PositionPlanRow(
                asset=leg.asset,
                direction="long" if is_long else "short",
                weight=float(leg.weight),
                margin=margin,
                notional=notional,
                leverage=lev,
            )
        )

    return PositionPlan(
        total_capital_usdt=total,
        total_margin_used=total,
        total_long_notional=total_long_notional,
        total_short_notional=total_short_notional,
        rows=rows,
    )
