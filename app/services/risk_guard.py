from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class RiskDecision:
    triggered: bool
    reason: Literal["tp", "sl", "none"]


class RiskGuard:
    @staticmethod
    def evaluate(*, pnl_pct: float, tp_pct: float, sl_pct: float) -> RiskDecision:
        pnl = float(pnl_pct)
        tp = max(float(tp_pct), 0.0)
        sl = max(float(sl_pct), 0.0)
        if tp > 0.0 and pnl >= tp:
            return RiskDecision(triggered=True, reason="tp")
        if sl > 0.0 and pnl <= -sl:
            return RiskDecision(triggered=True, reason="sl")
        return RiskDecision(triggered=False, reason="none")

