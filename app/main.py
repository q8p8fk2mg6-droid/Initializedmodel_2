from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any
import time as time_module
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.clients.binance import BinanceClient
from app.clients.exchange_adapter import ExchangeAdapterRegistry
from app.config import settings
from app.schemas import (
    BacktestRequest,
    BacktestRerankRequest,
    BacktestResponse,
    BacktestTimelinessRequest,
    BacktestTimelinessResponse,
    CalculatorPlanRequest,
    CalculatorPlanResponse,
    CalculatorPlanRow,
    CustomBacktestRequest,
    TimelinessWindowResult,
    TimelinessHistoryRunRecord,
    TimelinessHistoryRunsResponse,
    TimelinessLookbackStrategiesResponse,
    RefillCustomBacktestRequest,
    HistoryRunRecord,
    HistoryRunsResponse,
    LiveRobotCreateRequest,
    LiveRobotDeleteResponse,
    LiveRobotEventsResponse,
    LiveRobotListResponse,
    LiveRobotRecord,
    LiveRobotStartRequest,
    StrategyTransferExportRequest,
    StrategyTransferExportResponse,
    StrategyTransferImportRequest,
    StrategyTransferImportResponse,
    StrategyTransferPayload,
)
from app.services.backtester import PortfolioBacktester, StrategyParams as EngineStrategyParams
from app.services.data_loader import MarketDataLoader
from app.services.history_store import BacktestHistoryStore
from app.services.optimizer import PortfolioOptimizer
from app.services.portfolio import PortfolioLeg, normalize_portfolio
from app.services.position_sizer import build_position_plan
from app.services.timeliness_history_store import TimelinessHistoryStore
from app.services.live_robot_store import LiveRobotStore
from app.services.live_robot_engine import LiveRobotEngine
from app.services.mobile_notifier import MobileNotifier, MobileNotifierConfig
from app.services.strategy_transfer_store import StrategyTransferStore
from app.storage import runtime_store

ROOT_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = ROOT_DIR / "static"

app = FastAPI(title="Binance Perp Carry Optimizer")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

backtester = PortfolioBacktester()
optimizer = PortfolioOptimizer(backtester)
history_store = BacktestHistoryStore(
    file_path=settings.history_store_path,
    max_runs=settings.history_store_max_runs,
)
timeliness_history_store = TimelinessHistoryStore(
    file_path=settings.timeliness_history_store_path,
    max_runs=settings.timeliness_history_store_max_runs,
)
live_robot_store = LiveRobotStore(
    file_path=settings.live_robot_store_path,
    max_robots=settings.live_robot_store_max_robots,
    max_events=settings.live_robot_store_max_events,
)
strategy_transfer_store = StrategyTransferStore(
    file_path=settings.strategy_transfer_store_path,
    max_items=settings.strategy_transfer_store_max_items,
    default_ttl_minutes=settings.strategy_transfer_default_ttl_minutes,
    max_ttl_minutes=settings.strategy_transfer_max_ttl_minutes,
)
exchange_adapter_registry = ExchangeAdapterRegistry(
    bybit_base_url=settings.bybit_base_url,
    binance_futures_base_url=settings.binance_futures_base_url,
    bybit_recv_window_ms=settings.bybit_recv_window_ms,
)
mobile_notifier = MobileNotifier(
    MobileNotifierConfig(
        enabled=settings.mobile_notify_enabled,
        provider=settings.mobile_notify_provider,
        timeout_seconds=settings.mobile_notify_timeout_seconds,
        heartbeat_minutes=settings.mobile_notify_heartbeat_minutes,
        ntfy_base_url=settings.mobile_notify_ntfy_base_url,
        ntfy_topic=settings.mobile_notify_ntfy_topic,
        ntfy_token=settings.mobile_notify_ntfy_token,
        telegram_bot_token=settings.mobile_notify_telegram_bot_token,
        telegram_chat_id=settings.mobile_notify_telegram_chat_id,
        webhook_url=settings.mobile_notify_webhook_url,
        webhook_bearer_token=settings.mobile_notify_webhook_bearer_token,
    )
)
live_robot_engine = LiveRobotEngine(
    store=live_robot_store,
    exchange_registry=exchange_adapter_registry,
    notifier=mobile_notifier,
)


class TimelinessRuntimeTracker:
    def __init__(self) -> None:
        self._lock = Lock()
        self._state: dict[str, Any] = {
            "running": False,
            "stage": "idle",
            "started_at_ts": 0.0,
            "ended_at_ts": 0.0,
            "last_error": "",
            "message": "",
            "decision_date": "",
            "forward_days": 0,
            "total_steps": 0,
            "completed_steps": 0,
            "progress_pct": 0.0,
            "lookback_total": 0,
            "lookback_index": 0,
            "lookback_days": 0,
            "anchor_total": 0,
            "anchor_index": 0,
            "best_lookback_days": 0,
        }

    def start(
        self,
        *,
        decision_date: date,
        forward_days: int,
        lookback_total: int,
        anchor_total: int,
        total_steps: int,
    ) -> None:
        now = time_module.time()
        with self._lock:
            self._state = {
                "running": True,
                "stage": "preparing",
                "started_at_ts": now,
                "ended_at_ts": 0.0,
                "last_error": "",
                "message": "Preparing timeliness analysis...",
                "decision_date": decision_date.isoformat(),
                "forward_days": int(forward_days),
                "total_steps": max(int(total_steps), 1),
                "completed_steps": 0,
                "progress_pct": 0.0,
                "lookback_total": int(lookback_total),
                "lookback_index": 0,
                "lookback_days": 0,
                "anchor_total": int(anchor_total),
                "anchor_index": 0,
                "best_lookback_days": 0,
            }

    def update(self, **kwargs: Any) -> None:
        with self._lock:
            for key, value in kwargs.items():
                self._state[key] = value

            total_steps = max(int(self._state.get("total_steps", 1)), 1)
            completed_steps = int(self._state.get("completed_steps", 0))
            if completed_steps < 0:
                completed_steps = 0
            if completed_steps > total_steps:
                completed_steps = total_steps
            self._state["completed_steps"] = completed_steps
            self._state["progress_pct"] = float(completed_steps / total_steps * 100.0)

    def step_done(self, *, message: str | None = None) -> None:
        with self._lock:
            completed_steps = int(self._state.get("completed_steps", 0)) + 1
            total_steps = max(int(self._state.get("total_steps", 1)), 1)
            if completed_steps > total_steps:
                completed_steps = total_steps
            self._state["completed_steps"] = completed_steps
            self._state["progress_pct"] = float(completed_steps / total_steps * 100.0)
            if message is not None:
                self._state["message"] = str(message)

    def finish(self, *, error: str = "", message: str = "") -> None:
        now = time_module.time()
        with self._lock:
            total_steps = max(int(self._state.get("total_steps", 1)), 1)
            completed_steps = int(self._state.get("completed_steps", 0))
            if not error:
                completed_steps = total_steps
            self._state["running"] = False
            self._state["stage"] = "idle" if not error else "failed"
            self._state["ended_at_ts"] = now
            self._state["last_error"] = str(error or "")
            self._state["completed_steps"] = completed_steps
            self._state["progress_pct"] = float(completed_steps / total_steps * 100.0)
            if message:
                self._state["message"] = message

    def get(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)


timeliness_runtime = TimelinessRuntimeTracker()


def _downsample_curve(curve: list[list[float | int]], max_points: int = 300) -> list[list[float | int]]:
    if len(curve) <= max_points:
        return curve
    step = max(len(curve) // max_points, 1)
    sampled = curve[::step]
    if sampled[-1] != curve[-1]:
        sampled.append(curve[-1])
    return sampled


def _range_to_ms(start_date: date, end_date: date) -> tuple[int, int]:
    start_ms = int(datetime.combine(start_date, time.min, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime.combine(end_date, time.max, tzinfo=timezone.utc).timestamp() * 1000)
    return start_ms, end_ms


def _align_curve_to_range(
    curve: list[list[float | int]],
    start_date: date,
    end_date: date,
) -> list[list[float | int]]:
    if not curve:
        return []
    start_ms, end_ms = _range_to_ms(start_date, end_date)

    normalized: list[list[float | int]] = []
    for point in curve:
        if len(point) < 2:
            continue
        ts = int(point[0])
        nav = float(point[1])
        if ts < start_ms or ts > end_ms:
            continue
        normalized.append([ts, nav])

    if not normalized:
        nav = float(curve[0][1])
        return [[start_ms, nav], [end_ms, nav]]

    if normalized[0][0] > start_ms:
        normalized.insert(0, [start_ms, float(normalized[0][1])])
    elif normalized[0][0] < start_ms:
        normalized[0][0] = start_ms

    if normalized[-1][0] < end_ms:
        normalized.append([end_ms, float(normalized[-1][1])])
    elif normalized[-1][0] > end_ms:
        normalized[-1][0] = end_ms

    if end_ms > start_ms and len(normalized) == 1:
        normalized.append([end_ms, float(normalized[0][1])])

    return normalized


def _leg_value(leg: Any, key: str, default: Any) -> Any:
    if isinstance(leg, dict):
        return leg.get(key, default)
    return getattr(leg, key, default)


def _normalize_portfolio_input(legs: list[Any]) -> list[dict[str, Any]]:
    normalized = normalize_portfolio(
        [
            PortfolioLeg(
                asset=str(_leg_value(leg, "asset", "")).upper(),
                weight=float(_leg_value(leg, "weight", 0.0)),
                direction=1 if str(_leg_value(leg, "direction", "long")).lower() == "long" else -1,
                leverage=(
                    float(_leg_value(leg, "leverage", 0.0))
                    if _leg_value(leg, "leverage", None) is not None
                    else None
                ),
            )
            for leg in legs
        ]
    )
    return normalized.as_dict_list()


def _strategy_to_history_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": str(item.get("strategy_id", "")),
        "rank": int(item.get("rank", 0)),
        "params": dict(item.get("params", {})),
        "annualized_return": float(item.get("annualized_return", 0.0)),
        "total_return": float(item.get("total_return", 0.0)),
        "sharpe": float(item.get("sharpe", 0.0)),
        "max_drawdown": float(item.get("max_drawdown", 0.0)),
        "funding_income": float(item.get("funding_income", 0.0)),
        "trading_fees": float(item.get("trading_fees", 0.0)),
        "rehedge_count": int(item.get("rehedge_count", 0)),
        "portfolio": [dict(leg) for leg in item.get("portfolio", [])],
        "equity_curve": [list(point) for point in item.get("equity_curve", [])],
    }


def _get_strategy_for_transfer(strategy_id: str, source: str) -> tuple[dict[str, Any] | None, str]:
    sid = str(strategy_id or "").strip()
    src = str(source or "auto").strip().lower()
    if not sid:
        return None, "unknown"

    if src in {"auto", "runtime"}:
        strategy = runtime_store.get_strategy(sid)
        if strategy is not None:
            return dict(strategy), "runtime"
        if src == "runtime":
            return None, "runtime"

    if src in {"auto", "history"}:
        strategy = history_store.find_strategy(sid)
        if strategy is not None:
            return dict(strategy), "history"
        return None, "history"

    return None, "unknown"


def _build_strategy_transfer_payload(strategy: dict[str, Any], source: str) -> dict[str, Any]:
    rank_value: int | None
    try:
        rank_value = int(strategy.get("rank")) if strategy.get("rank") is not None else None
    except Exception:
        rank_value = None

    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    raw_portfolio = strategy.get("portfolio", [])
    portfolio_items: list[dict[str, Any]] = []
    if isinstance(raw_portfolio, list):
        for leg in raw_portfolio:
            if not isinstance(leg, dict):
                continue
            try:
                weight = float(leg.get("weight", 0.0))
            except Exception:
                weight = 0.0
            direction = str(leg.get("direction", "long")).strip().lower()
            if direction not in {"long", "short"}:
                direction = "long"
            lev_raw = leg.get("leverage", None)
            leverage: float | None
            if lev_raw is None:
                leverage = None
            else:
                try:
                    leverage = float(lev_raw)
                except Exception:
                    leverage = None
            portfolio_items.append(
                {
                    "asset": str(leg.get("asset", "")).upper().strip(),
                    "weight": weight,
                    "direction": direction,
                    "leverage": leverage,
                }
            )

    return {
        "strategy_id": str(strategy.get("strategy_id", "")).strip(),
        "source": str(source or "unknown"),
        "rank": rank_value,
        "annualized_return": _safe_float(strategy.get("annualized_return")),
        "total_return": _safe_float(strategy.get("total_return")),
        "sharpe": _safe_float(strategy.get("sharpe")),
        "max_drawdown": _safe_float(strategy.get("max_drawdown")),
        "params": dict(strategy.get("params", {})) if isinstance(strategy.get("params", {}), dict) else {},
        "portfolio": portfolio_items,
    }


def _build_backtest_request_from_timeliness(
    req: BacktestTimelinessRequest,
    start_date: date,
    end_date: date,
) -> BacktestRequest:
    return BacktestRequest(
        start_date=start_date,
        end_date=end_date,
        binance_api_key=req.binance_api_key,
        binance_api_secret=req.binance_api_secret,
        initial_capital_usdt=req.initial_capital_usdt,
        top_k=req.top_k,
        max_evals=req.max_evals,
        parallel_workers=req.parallel_workers,
        execution_mode=req.execution_mode,
        ranking_mode=req.ranking_mode,
        min_apy_pct=req.min_apy_pct,
        min_sharpe=req.min_sharpe,
        max_mdd_pct=req.max_mdd_pct,
        universe_limit=req.universe_limit,
        portfolio_size_min=req.portfolio_size_min,
        portfolio_size_max=req.portfolio_size_max,
        weight_step_pct=req.weight_step_pct,
        require_both_directions=req.require_both_directions,
        candidate_pool_size=req.candidate_pool_size,
        random_seed=req.random_seed,
        rehedge_hours_min=req.rehedge_hours_min,
        rehedge_hours_max=req.rehedge_hours_max,
        rehedge_hours_step=req.rehedge_hours_step,
        rebalance_threshold_pct_min=req.rebalance_threshold_pct_min,
        rebalance_threshold_pct_max=req.rebalance_threshold_pct_max,
        rebalance_threshold_pct_step=req.rebalance_threshold_pct_step,
        long_leverage_min=req.long_leverage_min,
        long_leverage_max=req.long_leverage_max,
        long_leverage_step=req.long_leverage_step,
        short_leverage_min=req.short_leverage_min,
        short_leverage_max=req.short_leverage_max,
        short_leverage_step=req.short_leverage_step,
    )


def _rank_strategy_key(mode: str):
    if mode == "mdd_asc_return_desc":
        return lambda item: (abs(float(item.get("max_drawdown", 0.0))), -float(item.get("annualized_return", 0.0)))
    if mode == "sharpe_desc_return_desc":
        return lambda item: (
            -float(item.get("sharpe", 0.0)),
            -float(item.get("annualized_return", 0.0)),
            abs(float(item.get("max_drawdown", 0.0))),
        )
    return lambda item: (-float(item.get("annualized_return", 0.0)), abs(float(item.get("max_drawdown", 0.0))))


def _pick_strategy_for_forward_test(optimize_result: dict[str, Any], ranking_mode: str) -> dict[str, Any] | None:
    top = optimize_result.get("top_strategies", [])
    if isinstance(top, list) and top:
        first = top[0]
        return dict(first) if isinstance(first, dict) else None

    all_items = optimize_result.get("all_strategies", [])
    if not isinstance(all_items, list) or not all_items:
        return None
    ordered = [dict(item) for item in all_items if isinstance(item, dict)]
    if not ordered:
        return None
    ordered.sort(key=_rank_strategy_key(ranking_mode))
    ordered[0]["rank"] = 1
    return ordered[0]


def _run_forward_test(
    *,
    strategy: dict[str, Any],
    client: BinanceClient,
    loader: MarketDataLoader,
    start_date: date,
    end_date: date,
    initial_capital_usdt: float,
) -> dict[str, Any]:
    portfolio = normalize_portfolio(
        [
            PortfolioLeg(
                asset=str(leg.get("asset", "")).upper(),
                weight=float(leg.get("weight", 0.0)),
                direction=1 if str(leg.get("direction", "long")).lower() == "long" else -1,
                leverage=float(leg.get("leverage")) if leg.get("leverage") is not None else None,
            )
            for leg in strategy.get("portfolio", [])
        ]
    )
    if len(portfolio.legs) == 0:
        raise ValueError("Selected strategy has empty portfolio.")

    start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    _, excluded_symbols = client.filter_symbols_by_start_date(portfolio.assets(), start_dt)
    if excluded_symbols:
        preview = ", ".join(excluded_symbols[:10])
        more = "" if len(excluded_symbols) <= 10 else f" ... (+{len(excluded_symbols) - 10})"
        raise ValueError(
            "Selected strategy contains symbols that were not listed at start_date "
            f"{start_date.isoformat()}: {preview}{more}"
        )

    params_raw = strategy.get("params", {}) or {}
    params = EngineStrategyParams(
        rehedge_hours=int(params_raw.get("rehedge_hours", 24)),
        rebalance_threshold_pct=float(params_raw.get("rebalance_threshold_pct", 1.0)),
        long_leverage=float(params_raw.get("long_leverage", 1.0)),
        short_leverage=float(params_raw.get("short_leverage", 1.0)),
    )
    market_data = loader.load(portfolio.assets(), start_date, end_date)
    result = backtester.run(
        market_data=market_data,
        portfolio=portfolio,
        params=params,
        initial_capital_usdt=initial_capital_usdt,
    )
    return {
        "annualized_return": float(result.get("annualized_return", 0.0)),
        "total_return": float(result.get("total_return", 0.0)),
        "sharpe": float(result.get("sharpe", 0.0)),
        "max_drawdown": float(result.get("max_drawdown", 0.0)),
    }


def _score_timeliness_window(metrics: list[dict[str, float]]) -> dict[str, float]:
    n = max(len(metrics), 1)
    avg_annualized_return = float(sum(float(item.get("annualized_return", 0.0)) for item in metrics) / n)
    avg_total_return = float(sum(float(item.get("total_return", 0.0)) for item in metrics) / n)
    avg_sharpe = float(sum(float(item.get("sharpe", 0.0)) for item in metrics) / n)
    avg_max_drawdown = float(sum(float(item.get("max_drawdown", 0.0)) for item in metrics) / n)
    win_rate = float(sum(1 for item in metrics if float(item.get("total_return", 0.0)) > 0.0) / n)
    score = float(avg_sharpe + avg_annualized_return - abs(avg_max_drawdown) * 1.2 + win_rate * 0.3)
    return {
        "score": score,
        "avg_annualized_return": avg_annualized_return,
        "avg_total_return": avg_total_return,
        "avg_sharpe": avg_sharpe,
        "avg_max_drawdown": avg_max_drawdown,
        "win_rate": win_rate,
    }


def _quantile_values(values: list[float], quantiles: list[float]) -> list[float]:
    if not values:
        return []
    ordered = sorted(float(v) for v in values)
    n = len(ordered)
    out: list[float] = []
    for q in quantiles:
        qq = min(max(float(q), 0.0), 1.0)
        idx = int(round((n - 1) * qq))
        idx = min(max(idx, 0), n - 1)
        out.append(float(ordered[idx]))
    return out


def _build_threshold_candidates(
    *,
    train_annualized_returns: list[float],
    train_sharpes: list[float],
    train_abs_mdds: list[float],
    default_min_apy: float,
    default_min_sharpe: float,
    default_max_mdd_abs: float,
) -> tuple[list[float], list[float], list[float]]:
    apy_vals = [float(default_min_apy)] + _quantile_values(train_annualized_returns, [0.15, 0.35, 0.5, 0.65, 0.85])
    sharpe_vals = [float(default_min_sharpe)] + _quantile_values(train_sharpes, [0.15, 0.35, 0.5, 0.65, 0.85])
    mdd_vals = [float(default_max_mdd_abs)] + _quantile_values(train_abs_mdds, [0.2, 0.4, 0.6, 0.8, 0.95])

    apy_candidates = sorted({round(float(x), 6) for x in apy_vals})
    sharpe_candidates = sorted({round(float(x), 6) for x in sharpe_vals})
    mdd_candidates = sorted({max(round(float(x), 6), 0.0) for x in mdd_vals})
    return apy_candidates, sharpe_candidates, mdd_candidates


def _select_forward_metrics_by_thresholds(
    *,
    samples: list[dict[str, Any]],
    ranking_mode: str,
    min_annualized_return: float,
    min_sharpe: float,
    max_drawdown_abs: float,
) -> tuple[list[dict[str, float]], int]:
    buckets: dict[int, list[dict[str, Any]]] = {}
    for sample in samples:
        try:
            train_apy = float(sample.get("annualized_return", 0.0))
            train_sharpe = float(sample.get("sharpe", 0.0))
            train_mdd = abs(float(sample.get("max_drawdown", 0.0)))
            anchor_idx = int(sample.get("anchor_index", 0))
        except Exception:
            continue
        if anchor_idx <= 0:
            continue
        if train_apy < float(min_annualized_return):
            continue
        if train_sharpe < float(min_sharpe):
            continue
        if train_mdd > float(max_drawdown_abs):
            continue
        buckets.setdefault(anchor_idx, []).append(sample)

    selected_metrics: list[dict[str, float]] = []
    for anchor_idx, items in buckets.items():
        _ = anchor_idx
        valid = [dict(item) for item in items if isinstance(item, dict)]
        if not valid:
            continue
        valid.sort(key=_rank_strategy_key(ranking_mode))
        best = valid[0]
        selected_metrics.append(
            {
                "annualized_return": float(best.get("forward_annualized_return", 0.0)),
                "total_return": float(best.get("forward_total_return", 0.0)),
                "sharpe": float(best.get("forward_sharpe", 0.0)),
                "max_drawdown": float(best.get("forward_max_drawdown", 0.0)),
            }
        )
    return selected_metrics, len(selected_metrics)


def _learn_thresholds_from_anchor_samples(
    *,
    samples: list[dict[str, Any]],
    ranking_mode: str,
    anchors_requested: int,
    default_min_apy: float,
    default_min_sharpe: float,
    default_max_mdd_abs: float,
) -> dict[str, Any] | None:
    if not samples:
        return None

    train_annualized_returns = [float(item.get("annualized_return", 0.0)) for item in samples]
    train_sharpes = [float(item.get("sharpe", 0.0)) for item in samples]
    train_abs_mdds = [abs(float(item.get("max_drawdown", 0.0))) for item in samples]

    apy_candidates, sharpe_candidates, mdd_candidates = _build_threshold_candidates(
        train_annualized_returns=train_annualized_returns,
        train_sharpes=train_sharpes,
        train_abs_mdds=train_abs_mdds,
        default_min_apy=default_min_apy,
        default_min_sharpe=default_min_sharpe,
        default_max_mdd_abs=default_max_mdd_abs,
    )

    best: dict[str, Any] | None = None
    for min_apy in apy_candidates:
        for min_sharpe in sharpe_candidates:
            for max_mdd_abs in mdd_candidates:
                selected_metrics, selected_count = _select_forward_metrics_by_thresholds(
                    samples=samples,
                    ranking_mode=ranking_mode,
                    min_annualized_return=min_apy,
                    min_sharpe=min_sharpe,
                    max_drawdown_abs=max_mdd_abs,
                )
                if not selected_metrics:
                    continue
                scored = _score_timeliness_window(selected_metrics)
                coverage = float(selected_count / max(int(anchors_requested), 1))
                rank_key = (
                    coverage,
                    float(scored["score"]),
                    float(scored["avg_sharpe"]),
                    float(scored["avg_annualized_return"]),
                    -abs(float(scored["avg_max_drawdown"])),
                )
                candidate = {
                    "min_apy": float(min_apy),
                    "min_sharpe": float(min_sharpe),
                    "max_mdd_abs": float(max_mdd_abs),
                    "selected_count": int(selected_count),
                    "coverage": coverage,
                    "scored": scored,
                    "rank_key": rank_key,
                }
                if best is None or tuple(candidate["rank_key"]) > tuple(best["rank_key"]):
                    best = candidate

    if best is not None:
        return best

    # Fallback: try request thresholds, then fully relaxed thresholds.
    for fallback in (
        (float(default_min_apy), float(default_min_sharpe), float(default_max_mdd_abs)),
        (-1e9, -1e9, 1e9),
    ):
        selected_metrics, selected_count = _select_forward_metrics_by_thresholds(
            samples=samples,
            ranking_mode=ranking_mode,
            min_annualized_return=fallback[0],
            min_sharpe=fallback[1],
            max_drawdown_abs=fallback[2],
        )
        if not selected_metrics:
            continue
        scored = _score_timeliness_window(selected_metrics)
        coverage = float(selected_count / max(int(anchors_requested), 1))
        return {
            "min_apy": float(fallback[0]),
            "min_sharpe": float(fallback[1]),
            "max_mdd_abs": float(fallback[2]),
            "selected_count": int(selected_count),
            "coverage": coverage,
            "scored": scored,
            "rank_key": (
                coverage,
                float(scored["score"]),
                float(scored["avg_sharpe"]),
                float(scored["avg_annualized_return"]),
                -abs(float(scored["avg_max_drawdown"])),
            ),
        }
    return None


def _build_top_by_mode_with_thresholds(
    *,
    all_strategies: list[dict[str, Any]],
    top_k: int,
    min_annualized_return: float,
    min_sharpe: float,
    max_drawdown_abs: float,
) -> dict[str, list[dict[str, Any]]]:
    ranking_modes = ["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"]
    return {
        mode: optimizer.rank_strategies(
            all_strategies,
            mode,
            top_k,
            min_annualized_return=min_annualized_return,
            min_sharpe=min_sharpe,
            max_drawdown_abs=max_drawdown_abs,
        )
        for mode in ranking_modes
    }


def _optimize_timeliness_train_window(
    req: BacktestTimelinessRequest,
    *,
    client: BinanceClient,
    loader: MarketDataLoader,
    universe_raw: list[str],
    train_start: date,
    train_end: date,
    optimize_service: PortfolioOptimizer | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    train_req = _build_backtest_request_from_timeliness(req, train_start, train_end)
    train_start_dt = datetime.combine(train_start, time.min, tzinfo=timezone.utc)
    train_universe, excluded_symbols = client.filter_symbols_by_start_date(universe_raw, train_start_dt)
    if len(train_universe) < train_req.portfolio_size_min:
        preview = ", ".join(excluded_symbols[:10])
        more = "" if len(excluded_symbols) <= 10 else f" ... (+{len(excluded_symbols) - 10})"
        raise ValueError(
            "Not enough symbols existed at train start for portfolio search. "
            f"train_start={train_start.isoformat()}, available={len(train_universe)}, "
            f"required_min={train_req.portfolio_size_min}. "
            f"Excluded newer symbols: {preview}{more}"
        )

    optimizer_to_use = optimize_service if optimize_service is not None else PortfolioOptimizer(backtester)
    optimize_result = optimizer_to_use.optimize(train_req, loader, train_universe)
    all_strategies = optimize_result["all_strategies"]
    top_strategies = optimize_result["top_strategies"]
    top_by_mode = optimize_result.get("top_strategies_by_mode", {})
    return all_strategies, top_strategies, top_by_mode


def _align_optimizer_curves_to_range(
    *,
    all_strategies: list[dict[str, Any]],
    top_strategies: list[dict[str, Any]],
    top_by_mode: dict[str, list[dict[str, Any]]],
    start_date: date,
    end_date: date,
) -> None:
    for item in all_strategies:
        curve = item.get("equity_curve", [])
        aligned = _align_curve_to_range(curve, start_date, end_date) if curve else []
        item["equity_curve"] = _downsample_curve(aligned) if aligned else []
    for item in top_strategies:
        curve = item.get("equity_curve", [])
        aligned = _align_curve_to_range(curve, start_date, end_date) if curve else []
        item["equity_curve"] = _downsample_curve(aligned) if aligned else []
    for mode, mode_list in list(top_by_mode.items()):
        if not isinstance(mode, str) or not isinstance(mode_list, list):
            continue
        for item in mode_list:
            curve = item.get("equity_curve", [])
            aligned = _align_curve_to_range(curve, start_date, end_date) if curve else []
            item["equity_curve"] = _downsample_curve(aligned) if aligned else []


def _load_timeliness_request_snapshot(record: dict[str, Any]) -> BacktestTimelinessRequest:
    raw = record.get("request")
    if not isinstance(raw, dict):
        raise ValueError("Timeliness history request snapshot is missing.")
    try:
        return BacktestTimelinessRequest.model_validate(raw)
    except Exception as exc:
        raise ValueError("Timeliness history request snapshot is invalid.") from exc


def _build_timeliness_anchor_windows(
    *,
    decision_date: date,
    forward_days: int,
    lookback_days: int,
    anchor_index: int,
) -> tuple[date, date, date, date]:
    if int(anchor_index) < 1:
        raise ValueError("anchor_index must be >= 1")
    if int(forward_days) < 1:
        raise ValueError("forward_days must be >= 1")
    if int(lookback_days) < 1:
        raise ValueError("lookback_days must be >= 1")

    decision_minus_one = decision_date - timedelta(days=1)
    offset = int(anchor_index)
    test_start = decision_date - timedelta(days=int(forward_days) * offset)
    test_end = min(
        test_start + timedelta(days=int(forward_days) - 1),
        decision_minus_one,
    )
    if test_end < test_start:
        raise ValueError("Invalid anchor forward range.")

    train_end = test_start - timedelta(days=1)
    train_start = train_end - timedelta(days=int(lookback_days) - 1)
    if train_end <= train_start:
        raise ValueError("Invalid anchor training range.")
    return train_start, train_end, test_start, test_end


@app.get("/")
def home() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "index.html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/mobile")
def mobile_home() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "mobile.html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/system/runtime")
def get_runtime_status() -> dict[str, Any]:
    stats = optimizer.get_runtime_stats()
    stats["timeliness"] = timeliness_runtime.get()
    running_robot_id = live_robot_store.find_running_robot_id()
    stats["live_robot"] = {
        "running_robot_id": running_robot_id,
        "robot_count": len(live_robot_store.list_robots()),
    }
    return stats


@app.get("/api/universe")
def get_universe(limit: int = 100) -> dict:
    client = BinanceClient()
    try:
        symbols = client.get_top_volume_perps(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"limit": limit, "symbols": symbols}


@app.post("/api/strategy-transfer/export", response_model=StrategyTransferExportResponse)
def export_strategy_transfer(req: StrategyTransferExportRequest, request: Request) -> StrategyTransferExportResponse:
    strategy, source_used = _get_strategy_for_transfer(req.strategy_id, req.source)
    if strategy is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Strategy not found: {req.strategy_id}. "
                "Please run/open strategy ranking first, or switch source(runtime/history)."
            ),
        )

    payload = StrategyTransferPayload.model_validate(
        _build_strategy_transfer_payload(strategy, source_used)
    ).model_dump(mode="json")
    record = strategy_transfer_store.create_transfer(
        payload=payload,
        source={
            "source": source_used,
            "strategy_id": str(req.strategy_id).strip(),
            "request_source": str(req.source or "auto").strip().lower(),
        },
        expires_minutes=req.expires_minutes,
    )
    base_url = str(request.base_url).rstrip("/")
    transfer_code = str(record.get("transfer_code", "")).strip().upper()
    import_url = f"{base_url}/mobile?code={transfer_code}"
    return StrategyTransferExportResponse(
        transfer_code=transfer_code,
        created_at=str(record.get("created_at", "")),
        expires_at=str(record.get("expires_at", "")),
        import_url=import_url,
        strategy=StrategyTransferPayload.model_validate(record.get("payload", {})),
    )


@app.post("/api/strategy-transfer/import", response_model=StrategyTransferImportResponse)
def import_strategy_transfer(req: StrategyTransferImportRequest) -> StrategyTransferImportResponse:
    record = strategy_transfer_store.get_transfer(req.transfer_code, consume=bool(req.consume))
    if record is None:
        raise HTTPException(
            status_code=404,
            detail="Transfer code is invalid/expired or already consumed.",
        )
    consumed_at = str(record.get("consumed_at", "")).strip() or None
    return StrategyTransferImportResponse(
        transfer_code=str(record.get("transfer_code", "")).strip().upper(),
        expires_at=str(record.get("expires_at", "")),
        consumed_at=consumed_at,
        strategy=StrategyTransferPayload.model_validate(record.get("payload", {})),
    )


@app.post("/api/live/robots", response_model=LiveRobotRecord)
def create_live_robot(req: LiveRobotCreateRequest) -> LiveRobotRecord:
    req_exchange = str(req.exchange).lower().strip()
    req_mode = str(req.execution_mode).lower().strip()
    if req_mode == "live" and req_exchange == "binance":
        raise HTTPException(
            status_code=400,
            detail="Live execution for Binance is not implemented yet. Please use Bybit or dry-run.",
        )

    has_runtime_creds = bool(str(req.api_key or "").strip()) and bool(str(req.api_secret or "").strip())
    has_env_creds = False
    if req_exchange == "bybit":
        has_env_creds = bool(str(settings.bybit_api_key or "").strip()) and bool(str(settings.bybit_api_secret or "").strip())
    elif req_exchange == "binance":
        has_env_creds = bool(str(settings.binance_api_key or "").strip()) and bool(str(settings.binance_api_secret or "").strip())
    credentials_mode = "runtime" if has_runtime_creds else ("env" if has_env_creds else "none")

    if req_mode == "live" and credentials_mode == "none":
        raise HTTPException(
            status_code=400,
            detail="Live mode requires API credentials. Provide api_key/api_secret or set env credentials.",
        )

    config = {
        "name": req.name.strip(),
        "exchange": req.exchange,
        "exchange_account": req.exchange_account.strip() if isinstance(req.exchange_account, str) else None,
        "tp_pct": float(req.tp_pct),
        "sl_pct": float(req.sl_pct),
        "poll_interval_seconds": int(req.poll_interval_seconds),
        "execution_mode": req.execution_mode,
        "credentials_mode": credentials_mode,
        "total_capital_usdt": float(req.total_capital_usdt),
        "rows": [row.model_dump() for row in req.rows],
        "source_strategy_id": req.source_strategy_id,
    }
    record = live_robot_store.create_robot(config=config)
    rid = str(record.get("robot_id", "")).strip()
    live_robot_engine.register_credentials(
        rid,
        exchange=req.exchange,
        api_key=req.api_key,
        api_secret=req.api_secret,
    )
    live_robot_store.append_event(
        rid,
        level="info",
        event_type="created",
        message="Robot config created.",
        data={
            "exchange": req.exchange,
            "mode": req.execution_mode,
            "credentials_mode": credentials_mode,
            "row_count": len(req.rows),
        },
    )
    latest = live_robot_store.get_robot(rid, include_events=True) or record
    return LiveRobotRecord(**latest)


@app.get("/api/live/robots", response_model=LiveRobotListResponse)
def list_live_robots() -> LiveRobotListResponse:
    robots = [LiveRobotRecord(**item) for item in live_robot_store.list_robots()]
    return LiveRobotListResponse(robots=robots)


@app.get("/api/live/robots/{robot_id}", response_model=LiveRobotRecord)
def get_live_robot(robot_id: str) -> LiveRobotRecord:
    record = live_robot_store.get_robot(robot_id, include_events=True)
    if record is None:
        raise HTTPException(status_code=404, detail="Live robot not found")
    return LiveRobotRecord(**record)


@app.post("/api/live/robots/{robot_id}/start", response_model=LiveRobotRecord)
def start_live_robot(robot_id: str, req: LiveRobotStartRequest | None = None) -> LiveRobotRecord:
    record_before_start = live_robot_store.get_robot(robot_id, include_events=False)
    if record_before_start is not None and req is not None:
        key = str(req.api_key or "").strip()
        secret = str(req.api_secret or "").strip()
        if key and secret:
            cfg = record_before_start.get("config", {}) if isinstance(record_before_start, dict) else {}
            exchange = str(cfg.get("exchange", "bybit")).lower().strip() or "bybit"
            live_robot_engine.register_credentials(
                robot_id,
                exchange=exchange,
                api_key=key,
                api_secret=secret,
            )
    try:
        record = live_robot_engine.start(robot_id)
        return LiveRobotRecord(**record)
    except ValueError as exc:
        detail = str(exc)
        if live_robot_store.get_robot(robot_id, include_events=False) is not None:
            live_robot_store.update_state(
                robot_id,
                {
                    "status": "error",
                    "running": False,
                    "trigger_reason": "start_failed",
                    "last_error": detail,
                },
            )
            live_robot_store.append_event(
                robot_id,
                level="error",
                event_type="start_failed",
                message=detail,
                data={},
            )
            latest = live_robot_store.get_robot(robot_id, include_events=False)
            if latest is not None:
                mobile_notifier.notify_robot_event(
                    latest,
                    event_type="start_failed",
                    level="error",
                    message=detail,
                    data={},
                )
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@app.post("/api/live/robots/{robot_id}/stop", response_model=LiveRobotRecord)
def stop_live_robot(robot_id: str) -> LiveRobotRecord:
    try:
        record = live_robot_engine.stop(robot_id, reason="manual_stop")
        return LiveRobotRecord(**record)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@app.post("/api/live/robots/{robot_id}/close-all", response_model=LiveRobotRecord)
def close_all_live_robot(robot_id: str) -> LiveRobotRecord:
    try:
        record = live_robot_engine.close_all(robot_id)
        return LiveRobotRecord(**record)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@app.post("/api/live/robots/{robot_id}/status-check", response_model=LiveRobotRecord)
def check_live_robot_status(robot_id: str, req: LiveRobotStartRequest | None = None) -> LiveRobotRecord:
    record_before_check = live_robot_store.get_robot(robot_id, include_events=False)
    if record_before_check is not None and req is not None:
        key = str(req.api_key or "").strip()
        secret = str(req.api_secret or "").strip()
        if key and secret:
            cfg = record_before_check.get("config", {}) if isinstance(record_before_check, dict) else {}
            exchange = str(cfg.get("exchange", "bybit")).lower().strip() or "bybit"
            live_robot_engine.register_credentials(
                robot_id,
                exchange=exchange,
                api_key=key,
                api_secret=secret,
            )
    try:
        record = live_robot_engine.check_status(robot_id)
        return LiveRobotRecord(**record)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@app.delete("/api/live/robots/{robot_id}", response_model=LiveRobotDeleteResponse)
def delete_live_robot(robot_id: str) -> LiveRobotDeleteResponse:
    try:
        removed = live_robot_engine.delete(robot_id)
        rid = str(removed.get("robot_id", "")).strip() or str(robot_id).strip()
        return LiveRobotDeleteResponse(deleted=True, robot_id=rid)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@app.get("/api/live/robots/{robot_id}/events", response_model=LiveRobotEventsResponse)
def get_live_robot_events(robot_id: str, limit: int = 200) -> LiveRobotEventsResponse:
    events = live_robot_store.get_events(robot_id, limit=limit)
    if events is None:
        raise HTTPException(status_code=404, detail="Live robot not found")
    return LiveRobotEventsResponse(robot_id=robot_id, events=events)


@app.get("/api/history/top", response_model=HistoryRunsResponse)
def get_history_top(limit: int = 20) -> HistoryRunsResponse:
    n = min(max(int(limit), 1), 200)
    runs = history_store.list_runs(limit=n)
    return HistoryRunsResponse(runs=runs)


@app.get("/api/history/top/{run_id}", response_model=HistoryRunRecord)
def get_history_run(run_id: str) -> HistoryRunRecord:
    record = history_store.get_run(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="History run not found")
    return HistoryRunRecord(**record)


@app.post("/api/history/top/clear")
def clear_history_top() -> dict:
    history_store.clear()
    return {"status": "ok"}


@app.get("/api/history/timeliness", response_model=TimelinessHistoryRunsResponse)
def get_history_timeliness(limit: int = 20) -> TimelinessHistoryRunsResponse:
    n = min(max(int(limit), 1), 200)
    runs = timeliness_history_store.list_runs(limit=n)
    records = [TimelinessHistoryRunRecord(**item) for item in runs]
    return TimelinessHistoryRunsResponse(runs=records)


@app.get("/api/history/timeliness/{run_id}", response_model=TimelinessHistoryRunRecord)
def get_history_timeliness_run(run_id: str) -> TimelinessHistoryRunRecord:
    record = timeliness_history_store.get_run(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Timeliness history run not found")
    return TimelinessHistoryRunRecord(**record)


@app.post("/api/history/timeliness/clear")
def clear_history_timeliness() -> dict:
    timeliness_history_store.clear()
    return {"status": "ok"}


@app.post(
    "/api/history/timeliness/{run_id}/lookback/{lookback_days}",
    response_model=TimelinessLookbackStrategiesResponse,
)
def load_history_timeliness_lookback(
    run_id: str,
    lookback_days: int,
    anchor_index: int = 1,
) -> TimelinessLookbackStrategiesResponse:
    record = timeliness_history_store.get_run(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Timeliness history run not found")

    lookback_days_int = int(lookback_days)
    if lookback_days_int < 7 or lookback_days_int > 2000:
        raise HTTPException(status_code=400, detail="lookback_days out of range [7, 2000]")

    available_lookbacks = {
        int(item.get("lookback_days", 0))
        for item in record.get("lookback_results", [])
        if isinstance(item, dict) and int(item.get("lookback_days", 0)) > 0
    }
    if available_lookbacks and lookback_days_int not in available_lookbacks:
        raise HTTPException(
            status_code=404,
            detail=f"lookback_days={lookback_days_int} not found in this timeliness run",
        )
    anchor_index_int = int(anchor_index)
    if anchor_index_int < 1 or anchor_index_int > 100:
        raise HTTPException(status_code=400, detail="anchor_index out of range [1, 100]")

    lookback_entry: dict[str, Any] | None = None
    for item in record.get("lookback_results", []):
        if not isinstance(item, dict):
            continue
        if int(item.get("lookback_days", 0)) == lookback_days_int:
            lookback_entry = item
            break

    try:
        req = _load_timeliness_request_snapshot(record)
        anchor_total = max(int(req.anchor_count), 1)
        if anchor_index_int > anchor_total:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"anchor_index={anchor_index_int} exceeds anchor_count={anchor_total} "
                    "for this timeliness run"
                ),
            )
        train_start, train_end, test_start, test_end = _build_timeliness_anchor_windows(
            decision_date=req.decision_date,
            forward_days=int(req.forward_days),
            lookback_days=lookback_days_int,
            anchor_index=anchor_index_int,
        )

        client = BinanceClient(api_key=req.binance_api_key, api_secret=req.binance_api_secret)
        loader = MarketDataLoader(client)
        universe_raw = client.get_top_volume_perps(limit=req.universe_limit)
        if not universe_raw:
            raise ValueError("Universe is empty.")

        all_strategies, strategies, top_by_mode = _optimize_timeliness_train_window(
            req,
            client=client,
            loader=loader,
            universe_raw=universe_raw,
            train_start=train_start,
            train_end=train_end,
            optimize_service=optimizer,
        )
        _align_optimizer_curves_to_range(
            all_strategies=all_strategies,
            top_strategies=strategies,
            top_by_mode=top_by_mode,
            start_date=train_start,
            end_date=train_end,
        )

        applied_min_apy_pct: float | None = None
        applied_min_sharpe: float | None = None
        applied_max_mdd_pct: float | None = None
        if lookback_entry is not None:
            raw_min_apy_pct = lookback_entry.get("learned_min_apy_pct")
            raw_min_sharpe = lookback_entry.get("learned_min_sharpe")
            raw_max_mdd_pct = lookback_entry.get("learned_max_mdd_pct")
            if (
                raw_min_apy_pct is not None
                and raw_min_sharpe is not None
                and raw_max_mdd_pct is not None
            ):
                try:
                    applied_min_apy_pct = float(raw_min_apy_pct)
                    applied_min_sharpe = float(raw_min_sharpe)
                    applied_max_mdd_pct = float(raw_max_mdd_pct)
                except Exception:
                    applied_min_apy_pct = None
                    applied_min_sharpe = None
                    applied_max_mdd_pct = None

        if (
            applied_min_apy_pct is not None
            and applied_min_sharpe is not None
            and applied_max_mdd_pct is not None
        ):
            top_by_mode = _build_top_by_mode_with_thresholds(
                all_strategies=all_strategies,
                top_k=req.top_k,
                min_annualized_return=float(applied_min_apy_pct) / 100.0,
                min_sharpe=float(applied_min_sharpe),
                max_drawdown_abs=float(applied_max_mdd_pct) / 100.0,
            )
            learned_strategies = top_by_mode.get(req.ranking_mode, [])
            if learned_strategies:
                strategies = learned_strategies
            else:
                applied_min_apy_pct = None
                applied_min_sharpe = None
                applied_max_mdd_pct = None
                top_by_mode = _build_top_by_mode_with_thresholds(
                    all_strategies=all_strategies,
                    top_k=req.top_k,
                    min_annualized_return=float(req.min_apy_pct) / 100.0,
                    min_sharpe=float(req.min_sharpe),
                    max_drawdown_abs=float(req.max_mdd_pct) / 100.0,
                )
                strategies = top_by_mode.get(req.ranking_mode, []) or strategies

        runtime_store.set_backtest(
            all_strategies=all_strategies,
            meta={
                "ranking_mode": req.ranking_mode,
                "execution_mode": req.execution_mode,
                "start_date": train_start.isoformat(),
                "end_date": train_end.isoformat(),
                "universe_limit": req.universe_limit,
                "top_k": req.top_k,
                "decision_date": req.decision_date.isoformat(),
                "best_lookback_days": lookback_days_int,
                "forward_days": req.forward_days,
                "mode": "timeliness_history",
                "timeliness_run_id": run_id,
                "anchor_index": anchor_index_int,
                "test_start_date": test_start.isoformat(),
                "test_end_date": test_end.isoformat(),
                "applied_min_apy_pct": applied_min_apy_pct,
                "applied_min_sharpe": applied_min_sharpe,
                "applied_max_mdd_pct": applied_max_mdd_pct,
            },
        )

        return TimelinessLookbackStrategiesResponse(
            run_id=run_id,
            lookback_days=lookback_days_int,
            anchor_index=anchor_index_int,
            ranking_mode=req.ranking_mode,
            train_start_date=train_start,
            train_end_date=train_end,
            test_start_date=test_start,
            test_end_date=test_end,
            applied_min_apy_pct=applied_min_apy_pct,
            applied_min_sharpe=applied_min_sharpe,
            applied_max_mdd_pct=applied_max_mdd_pct,
            strategies=strategies,
        )
    except HTTPException:
        raise
    except Exception as exc:
        optimizer.mark_runtime_failed(str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/backtest/run", response_model=BacktestResponse)
def run_backtest(req: BacktestRequest) -> BacktestResponse:
    try:
        client = BinanceClient(api_key=req.binance_api_key, api_secret=req.binance_api_secret)
        universe_raw = client.get_top_volume_perps(limit=req.universe_limit)
        start_dt = datetime.combine(req.start_date, time.min, tzinfo=timezone.utc)
        universe, excluded_symbols = client.filter_symbols_by_start_date(universe_raw, start_dt)
        if len(universe) < req.portfolio_size_min:
            preview = ", ".join(excluded_symbols[:10])
            more = "" if len(excluded_symbols) <= 10 else f" ... (+{len(excluded_symbols) - 10})"
            raise ValueError(
                "Not enough symbols existed at start_date for portfolio search. "
                f"start_date={req.start_date.isoformat()}, available={len(universe)}, "
                f"required_min={req.portfolio_size_min}. "
                f"Excluded newer symbols: {preview}{more}"
            )
        loader = MarketDataLoader(client)
        optimize_result = optimizer.optimize(req, loader, universe)
        all_strategies = optimize_result["all_strategies"]
        strategies = optimize_result["top_strategies"]
        top_by_mode = optimize_result.get("top_strategies_by_mode", {})
    except Exception as exc:
        optimizer.mark_runtime_failed(str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    for item in all_strategies:
        curve = item.get("equity_curve", [])
        aligned = _align_curve_to_range(curve, req.start_date, req.end_date) if curve else []
        item["equity_curve"] = _downsample_curve(aligned) if aligned else []
    for item in strategies:
        curve = item.get("equity_curve", [])
        aligned = _align_curve_to_range(curve, req.start_date, req.end_date) if curve else []
        item["equity_curve"] = _downsample_curve(aligned) if aligned else []
    for mode, mode_list in list(top_by_mode.items()):
        if not isinstance(mode, str) or not isinstance(mode_list, list):
            continue
        for item in mode_list:
            curve = item.get("equity_curve", [])
            aligned = _align_curve_to_range(curve, req.start_date, req.end_date) if curve else []
            item["equity_curve"] = _downsample_curve(aligned) if aligned else []

    history_top_by_mode: dict[str, list[dict[str, Any]]] = {}
    for mode, mode_list in list(top_by_mode.items()):
        if not isinstance(mode, str) or not isinstance(mode_list, list):
            continue
        history_top_by_mode[mode] = [_strategy_to_history_item(item) for item in mode_list]
    history_top_default = history_top_by_mode.get(
        "sharpe_desc_return_desc",
        history_top_by_mode.get(req.ranking_mode, [_strategy_to_history_item(item) for item in strategies]),
    )

    runtime_store.set_backtest(
        all_strategies=all_strategies,
        meta={
            "ranking_mode": req.ranking_mode,
            "execution_mode": req.execution_mode,
            "start_date": req.start_date.isoformat(),
            "end_date": req.end_date.isoformat(),
            "universe_limit": req.universe_limit,
            "top_k": req.top_k,
        },
    )
    history_store.add_run(
        run_meta={
            "start_date": req.start_date.isoformat(),
            "end_date": req.end_date.isoformat(),
            "initial_capital_usdt": req.initial_capital_usdt,
            "ranking_mode": req.ranking_mode,
            "top_k": req.top_k,
            "max_evals": req.max_evals,
            "parallel_workers": req.parallel_workers,
            "execution_mode": req.execution_mode,
            "universe_limit": req.universe_limit,
            "portfolio_size_min": req.portfolio_size_min,
            "portfolio_size_max": req.portfolio_size_max,
            "weight_step_pct": req.weight_step_pct,
            "candidate_pool_size": req.candidate_pool_size,
            "min_apy_pct": req.min_apy_pct,
            "min_sharpe": req.min_sharpe,
            "max_mdd_pct": req.max_mdd_pct,
        },
        top_strategies=history_top_default,
        top_strategies_by_mode=history_top_by_mode,
    )

    return BacktestResponse(
        ranking_mode=req.ranking_mode,
        strategies=strategies,
    )


@app.post("/api/backtest/timeliness", response_model=BacktestTimelinessResponse)
def run_backtest_timeliness(req: BacktestTimelinessRequest) -> BacktestTimelinessResponse:
    total_steps = max(len(req.lookback_windows_days) * int(req.anchor_count) + 1, 1)
    timeliness_runtime.start(
        decision_date=req.decision_date,
        forward_days=req.forward_days,
        lookback_total=len(req.lookback_windows_days),
        anchor_total=req.anchor_count,
        total_steps=total_steps,
    )
    try:
        client = BinanceClient(api_key=req.binance_api_key, api_secret=req.binance_api_secret)
        loader = MarketDataLoader(client)
        universe_raw = client.get_top_volume_perps(limit=req.universe_limit)
        if not universe_raw:
            raise ValueError("Universe is empty.")

        timeliness_runtime.update(
            stage="evaluating_lookbacks",
            message="Evaluating lookback windows...",
        )

        lookback_results: list[TimelinessWindowResult] = []
        best_lookback_days: int | None = None
        best_rank_key: tuple[float, float, float, float, float] | None = None
        learned_thresholds_by_lookback: dict[int, dict[str, float]] = {}
        anchor_candidate_limit = 4
        default_min_apy = float(req.min_apy_pct) / 100.0
        default_min_sharpe = float(req.min_sharpe)
        default_max_mdd_abs = float(req.max_mdd_pct) / 100.0
        decision_minus_one = req.decision_date - timedelta(days=1)

        for lookback_idx, lookback_days in enumerate(req.lookback_windows_days, start=1):
            timeliness_runtime.update(
                stage="evaluating_lookback",
                lookback_index=lookback_idx,
                lookback_days=int(lookback_days),
                anchor_index=0,
                message=f"Evaluating lookback L={int(lookback_days)} days.",
            )
            anchor_samples: list[dict[str, Any]] = []
            notes: list[str] = []
            for anchor_idx, offset in enumerate(range(req.anchor_count, 0, -1), start=1):
                timeliness_runtime.update(
                    stage="evaluating_anchor",
                    lookback_index=lookback_idx,
                    lookback_days=int(lookback_days),
                    anchor_index=anchor_idx,
                    message=(
                        f"L={int(lookback_days)} days, "
                        f"anchor {anchor_idx}/{int(req.anchor_count)}."
                    ),
                )
                test_start = req.decision_date - timedelta(days=req.forward_days * offset)
                test_end = min(
                    test_start + timedelta(days=req.forward_days - 1),
                    decision_minus_one,
                )
                if test_end < test_start:
                    timeliness_runtime.step_done(
                        message=(
                            f"L={int(lookback_days)} days, "
                            f"anchor {anchor_idx}/{int(req.anchor_count)} skipped."
                        )
                    )
                    continue
                train_end = test_start - timedelta(days=1)
                train_start = train_end - timedelta(days=lookback_days - 1)
                if train_end <= train_start:
                    notes.append("train range invalid")
                    timeliness_runtime.step_done(
                        message=(
                            f"L={int(lookback_days)} days, "
                            f"anchor {anchor_idx}/{int(req.anchor_count)} invalid range."
                        )
                    )
                    continue

                try:
                    train_all_strategies, _, _ = _optimize_timeliness_train_window(
                        req,
                        client=client,
                        loader=loader,
                        universe_raw=universe_raw,
                        train_start=train_start,
                        train_end=train_end,
                    )
                    ordered_candidates = [dict(item) for item in train_all_strategies if isinstance(item, dict)]
                    ordered_candidates.sort(key=_rank_strategy_key(req.ranking_mode))
                    if not ordered_candidates:
                        notes.append(f"anchor {anchor_idx}: no strategy from training run")
                        continue

                    tested = ordered_candidates[: min(anchor_candidate_limit, len(ordered_candidates))]
                    valid_for_anchor = 0
                    for strategy_idx, selected in enumerate(tested, start=1):
                        try:
                            metric = _run_forward_test(
                                strategy=selected,
                                client=client,
                                loader=loader,
                                start_date=test_start,
                                end_date=test_end,
                                initial_capital_usdt=req.initial_capital_usdt,
                            )
                            anchor_samples.append(
                                {
                                    "anchor_index": int(anchor_idx),
                                    "annualized_return": float(selected.get("annualized_return", 0.0)),
                                    "total_return": float(selected.get("total_return", 0.0)),
                                    "sharpe": float(selected.get("sharpe", 0.0)),
                                    "max_drawdown": float(selected.get("max_drawdown", 0.0)),
                                    "forward_annualized_return": float(metric.get("annualized_return", 0.0)),
                                    "forward_total_return": float(metric.get("total_return", 0.0)),
                                    "forward_sharpe": float(metric.get("sharpe", 0.0)),
                                    "forward_max_drawdown": float(metric.get("max_drawdown", 0.0)),
                                }
                            )
                            valid_for_anchor += 1
                        except Exception as forward_exc:
                            if strategy_idx == 1:
                                notes.append(f"anchor {anchor_idx} top strategy failed: {str(forward_exc)}")
                    if valid_for_anchor == 0:
                        notes.append(f"anchor {anchor_idx} has no forward-valid candidate")
                except Exception as anchor_exc:
                    notes.append(f"anchor failed: {str(anchor_exc)}")
                finally:
                    timeliness_runtime.step_done(
                        message=(
                            f"L={int(lookback_days)} days, "
                            f"anchor {anchor_idx}/{int(req.anchor_count)} completed."
                        )
                    )

            if anchor_samples:
                learned = _learn_thresholds_from_anchor_samples(
                    samples=anchor_samples,
                    ranking_mode=req.ranking_mode,
                    anchors_requested=req.anchor_count,
                    default_min_apy=default_min_apy,
                    default_min_sharpe=default_min_sharpe,
                    default_max_mdd_abs=default_max_mdd_abs,
                )
                if learned is None:
                    note_text = "No valid threshold combination from anchor samples."
                    if notes:
                        head = "; ".join(notes[:3])
                        tail = "" if len(notes) <= 3 else f"; ... (+{len(notes) - 3} more)"
                        note_text = f"{note_text} {head}{tail}"
                    lookback_results.append(
                        TimelinessWindowResult(
                            lookback_days=lookback_days,
                            anchors_requested=req.anchor_count,
                            anchors_completed=0,
                            notes=note_text,
                        )
                    )
                    timeliness_runtime.update(
                        stage="scoring_lookback",
                        lookback_index=lookback_idx,
                        lookback_days=int(lookback_days),
                        anchor_index=int(req.anchor_count),
                        message=f"L={int(lookback_days)} days has no valid threshold-selection result.",
                    )
                    continue

                scored = dict(learned["scored"])
                learned_min_apy_pct = float(learned["min_apy"]) * 100.0
                learned_min_sharpe = float(learned["min_sharpe"])
                learned_max_mdd_pct = float(learned["max_mdd_abs"]) * 100.0
                selected_count = int(learned["selected_count"])
                coverage = float(learned["coverage"])

                note_parts = [
                    (
                        "learned thresholds: "
                        f"APY>={learned_min_apy_pct:.2f}%, "
                        f"Sharpe>={learned_min_sharpe:.3f}, "
                        f"MDD<={learned_max_mdd_pct:.2f}% "
                        f"(coverage={coverage * 100.0:.1f}%)"
                    )
                ]
                if notes:
                    head = "; ".join(notes[:3])
                    tail = "" if len(notes) <= 3 else f"; ... (+{len(notes) - 3} more)"
                    note_parts.append(f"partial skips: {head}{tail}")
                note_text = " | ".join(note_parts)
                lookback_results.append(
                    TimelinessWindowResult(
                        lookback_days=lookback_days,
                        score=scored["score"],
                        anchors_requested=req.anchor_count,
                        anchors_completed=selected_count,
                        avg_annualized_return=scored["avg_annualized_return"],
                        avg_total_return=scored["avg_total_return"],
                        avg_sharpe=scored["avg_sharpe"],
                        avg_max_drawdown=scored["avg_max_drawdown"],
                        win_rate=scored["win_rate"],
                        learned_min_apy_pct=learned_min_apy_pct,
                        learned_min_sharpe=learned_min_sharpe,
                        learned_max_mdd_pct=learned_max_mdd_pct,
                        notes=note_text,
                    )
                )
                timeliness_runtime.update(
                    stage="scoring_lookback",
                    lookback_index=lookback_idx,
                    lookback_days=int(lookback_days),
                    anchor_index=int(req.anchor_count),
                    message=(
                        f"L={int(lookback_days)} days scored "
                        f"({selected_count}/{int(req.anchor_count)} anchors)."
                    ),
                )
                learned_thresholds_by_lookback[int(lookback_days)] = {
                    "min_apy": float(learned["min_apy"]),
                    "min_sharpe": float(learned["min_sharpe"]),
                    "max_mdd_abs": float(learned["max_mdd_abs"]),
                }
                rank_key = tuple(learned["rank_key"])
                if best_rank_key is None or rank_key > best_rank_key:
                    best_rank_key = rank_key
                    best_lookback_days = lookback_days
            else:
                note_text = "No valid anchor evaluation."
                if notes:
                    head = "; ".join(notes[:3])
                    tail = "" if len(notes) <= 3 else f"; ... (+{len(notes) - 3} more)"
                    note_text = f"{note_text} {head}{tail}"
                lookback_results.append(
                    TimelinessWindowResult(
                        lookback_days=lookback_days,
                        anchors_requested=req.anchor_count,
                        anchors_completed=0,
                        notes=note_text,
                    )
                )
                timeliness_runtime.update(
                    stage="scoring_lookback",
                    lookback_index=lookback_idx,
                    lookback_days=int(lookback_days),
                    anchor_index=int(req.anchor_count),
                    message=f"L={int(lookback_days)} days has no valid anchor result.",
                )

        if best_lookback_days is None:
            raise ValueError(
                "No valid lookback window produced out-of-sample metrics. "
                "Try reducing filters or narrowing complexity."
            )

        timeliness_runtime.update(
            stage="final_training",
            best_lookback_days=int(best_lookback_days),
            lookback_days=int(best_lookback_days),
            anchor_index=int(req.anchor_count),
            message=f"Final training with best L={int(best_lookback_days)} days.",
        )
        final_train_end = decision_minus_one
        final_train_start = final_train_end - timedelta(days=best_lookback_days - 1)
        best_learned_threshold = learned_thresholds_by_lookback.get(int(best_lookback_days))
        all_strategies, base_strategies, base_top_by_mode = _optimize_timeliness_train_window(
            req,
            client=client,
            loader=loader,
            universe_raw=universe_raw,
            train_start=final_train_start,
            train_end=final_train_end,
        )
        timeliness_runtime.step_done(message="Final training completed.")
    except Exception as exc:
        timeliness_runtime.finish(error=str(exc), message=f"Timeliness analysis failed: {str(exc)}")
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        _align_optimizer_curves_to_range(
            all_strategies=all_strategies,
            top_strategies=base_strategies,
            top_by_mode=base_top_by_mode,
            start_date=final_train_start,
            end_date=final_train_end,
        )

        applied_min_apy_pct: float | None = None
        applied_min_sharpe: float | None = None
        applied_max_mdd_pct: float | None = None
        strategies = base_strategies
        top_by_mode = base_top_by_mode

        if best_learned_threshold is not None:
            applied_min_apy_pct = float(best_learned_threshold["min_apy"]) * 100.0
            applied_min_sharpe = float(best_learned_threshold["min_sharpe"])
            applied_max_mdd_pct = float(best_learned_threshold["max_mdd_abs"]) * 100.0
            learned_top_by_mode = _build_top_by_mode_with_thresholds(
                all_strategies=all_strategies,
                top_k=req.top_k,
                min_annualized_return=float(best_learned_threshold["min_apy"]),
                min_sharpe=float(best_learned_threshold["min_sharpe"]),
                max_drawdown_abs=float(best_learned_threshold["max_mdd_abs"]),
            )
            learned_strategies = learned_top_by_mode.get(req.ranking_mode, [])
            if learned_strategies:
                top_by_mode = learned_top_by_mode
                strategies = learned_strategies
            else:
                applied_min_apy_pct = None
                applied_min_sharpe = None
                applied_max_mdd_pct = None

        history_top_by_mode: dict[str, list[dict[str, Any]]] = {}
        for mode, mode_list in list(top_by_mode.items()):
            if not isinstance(mode, str) or not isinstance(mode_list, list):
                continue
            history_top_by_mode[mode] = [_strategy_to_history_item(item) for item in mode_list]
        history_top_default = history_top_by_mode.get(
            "sharpe_desc_return_desc",
            history_top_by_mode.get(req.ranking_mode, [_strategy_to_history_item(item) for item in strategies]),
        )

        runtime_store.set_backtest(
            all_strategies=all_strategies,
            meta={
                "ranking_mode": req.ranking_mode,
                "execution_mode": req.execution_mode,
                "start_date": final_train_start.isoformat(),
                "end_date": final_train_end.isoformat(),
                "universe_limit": req.universe_limit,
                "top_k": req.top_k,
                "decision_date": req.decision_date.isoformat(),
                "best_lookback_days": best_lookback_days,
                "forward_days": req.forward_days,
                "mode": "timeliness",
                "applied_min_apy_pct": applied_min_apy_pct,
                "applied_min_sharpe": applied_min_sharpe,
                "applied_max_mdd_pct": applied_max_mdd_pct,
            },
        )
        history_store.add_run(
            run_meta={
                "mode": "timeliness",
                "decision_date": req.decision_date.isoformat(),
                "forward_days": req.forward_days,
                "best_lookback_days": best_lookback_days,
                "start_date": final_train_start.isoformat(),
                "end_date": final_train_end.isoformat(),
                "initial_capital_usdt": req.initial_capital_usdt,
                "ranking_mode": req.ranking_mode,
                "top_k": req.top_k,
                "max_evals": req.max_evals,
                "parallel_workers": req.parallel_workers,
                "execution_mode": req.execution_mode,
                "universe_limit": req.universe_limit,
                "portfolio_size_min": req.portfolio_size_min,
                "portfolio_size_max": req.portfolio_size_max,
                "weight_step_pct": req.weight_step_pct,
                "candidate_pool_size": req.candidate_pool_size,
                "min_apy_pct": req.min_apy_pct,
                "min_sharpe": req.min_sharpe,
                "max_mdd_pct": req.max_mdd_pct,
                "applied_min_apy_pct": applied_min_apy_pct,
                "applied_min_sharpe": applied_min_sharpe,
                "applied_max_mdd_pct": applied_max_mdd_pct,
            },
            top_strategies=history_top_default,
            top_strategies_by_mode=history_top_by_mode,
        )

        request_snapshot = req.model_dump(mode="json")
        request_snapshot["binance_api_key"] = None
        request_snapshot["binance_api_secret"] = None

        timeliness_history_record = timeliness_history_store.add_run(
            run_meta={
                "decision_date": req.decision_date.isoformat(),
                "deploy_end_date": (req.decision_date + timedelta(days=req.forward_days - 1)).isoformat(),
                "forward_days": req.forward_days,
                "anchor_count": req.anchor_count,
                "lookback_windows_days": [int(x) for x in req.lookback_windows_days],
                "best_lookback_days": best_lookback_days,
                "start_date": final_train_start.isoformat(),
                "end_date": final_train_end.isoformat(),
                "ranking_mode": req.ranking_mode,
                "top_k": req.top_k,
                "max_evals": req.max_evals,
                "parallel_workers": req.parallel_workers,
                "execution_mode": req.execution_mode,
                "universe_limit": req.universe_limit,
                "portfolio_size_min": req.portfolio_size_min,
                "portfolio_size_max": req.portfolio_size_max,
                "weight_step_pct": req.weight_step_pct,
                "candidate_pool_size": req.candidate_pool_size,
                "initial_capital_usdt": req.initial_capital_usdt,
                "min_apy_pct": req.min_apy_pct,
                "min_sharpe": req.min_sharpe,
                "max_mdd_pct": req.max_mdd_pct,
                "applied_min_apy_pct": applied_min_apy_pct,
                "applied_min_sharpe": applied_min_sharpe,
                "applied_max_mdd_pct": applied_max_mdd_pct,
            },
            lookback_results=[item.model_dump(mode="json") for item in lookback_results],
            request_snapshot=request_snapshot,
        )
        timeliness_run_id = str(timeliness_history_record.get("run_id", "")).strip() or None

        resp = BacktestTimelinessResponse(
            ranking_mode=req.ranking_mode,
            decision_date=req.decision_date,
            deploy_end_date=req.decision_date + timedelta(days=req.forward_days - 1),
            best_lookback_days=best_lookback_days,
            applied_min_apy_pct=applied_min_apy_pct,
            applied_min_sharpe=applied_min_sharpe,
            applied_max_mdd_pct=applied_max_mdd_pct,
            timeliness_run_id=timeliness_run_id,
            lookback_results=lookback_results,
            strategies=strategies,
        )
        timeliness_runtime.finish(error="", message="Timeliness analysis completed.")
        return resp
    except Exception as exc:
        timeliness_runtime.finish(error=str(exc), message=f"Timeliness analysis failed: {str(exc)}")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/backtest/custom", response_model=BacktestResponse)
def run_custom_backtest(req: CustomBacktestRequest) -> BacktestResponse:
    try:
        portfolio = normalize_portfolio(
            [
                PortfolioLeg(
                    asset=str(leg.asset).upper(),
                    weight=float(leg.weight),
                    direction=1 if leg.direction == "long" else -1,
                    leverage=float(leg.leverage) if leg.leverage is not None else None,
                )
                for leg in req.portfolio
            ]
        )

        client = BinanceClient(api_key=req.binance_api_key, api_secret=req.binance_api_secret)
        start_dt = datetime.combine(req.start_date, time.min, tzinfo=timezone.utc)
        _, excluded_symbols = client.filter_symbols_by_start_date(portfolio.assets(), start_dt)
        if excluded_symbols:
            preview = ", ".join(excluded_symbols[:10])
            more = "" if len(excluded_symbols) <= 10 else f" ... (+{len(excluded_symbols) - 10})"
            raise ValueError(
                "Custom portfolio contains symbols that were not listed at start_date "
                f"{req.start_date.isoformat()}: {preview}{more}"
            )
        loader = MarketDataLoader(client)
        market_data = loader.load(portfolio.assets(), req.start_date, req.end_date)

        params = EngineStrategyParams(
            rehedge_hours=int(req.rehedge_hours),
            rebalance_threshold_pct=float(req.rebalance_threshold_pct),
            long_leverage=float(req.long_leverage),
            short_leverage=float(req.short_leverage),
        )
        result = backtester.run(
            market_data=market_data,
            portfolio=portfolio,
            params=params,
            initial_capital_usdt=req.initial_capital_usdt,
            tp_pct=req.tp_pct,
            sl_pct=req.sl_pct,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result["strategy_id"] = str(uuid.uuid4())
    result["rank"] = 1
    aligned_curve = _align_curve_to_range(result.get("equity_curve", []), req.start_date, req.end_date)
    result["equity_curve"] = _downsample_curve(aligned_curve) if aligned_curve else []
    result["portfolio"] = portfolio.as_dict_list()

    runtime_store.set_backtest(
        all_strategies=[result],
        meta={
            "ranking_mode": req.ranking_mode,
            "start_date": req.start_date.isoformat(),
            "end_date": req.end_date.isoformat(),
            "top_k": 1,
            "tp_pct": req.tp_pct,
            "sl_pct": req.sl_pct,
        },
    )

    return BacktestResponse(
        ranking_mode=req.ranking_mode,
        strategies=[result],
    )


@app.post("/api/backtest/custom/refill", response_model=BacktestResponse)
def run_refill_custom_backtest(req: RefillCustomBacktestRequest) -> BacktestResponse:
    source = runtime_store.get_strategy(req.strategy_id)
    if source is None:
        source = history_store.find_strategy(req.strategy_id)
    if source is None:
        raise HTTPException(
            status_code=404,
            detail="Strategy not found in current runtime/history. Please rerun backtest and select from ranking.",
        )

    try:
        source_portfolio = source.get("portfolio", [])
        portfolio = normalize_portfolio(
            [
                PortfolioLeg(
                    asset=str(leg.get("asset", "")).upper(),
                    weight=float(leg.get("weight", 0.0)),
                    direction=(
                        (1 if str(leg.get("direction", "long")).lower() == "long" else -1)
                        * (-1 if req.reverse_directions else 1)
                    ),
                    leverage=float(leg.get("leverage")) if leg.get("leverage") is not None else None,
                )
                for leg in source_portfolio
            ]
        )
        if len(portfolio.legs) == 0:
            raise ValueError("Selected strategy has empty portfolio.")

        params_raw = source.get("params", {}) or {}
        params = EngineStrategyParams(
            rehedge_hours=int(params_raw.get("rehedge_hours", 24)),
            rebalance_threshold_pct=float(params_raw.get("rebalance_threshold_pct", 1.0)),
            long_leverage=float(params_raw.get("long_leverage", 1.0)),
            short_leverage=float(params_raw.get("short_leverage", 1.0)),
        )

        client = BinanceClient(api_key=req.binance_api_key, api_secret=req.binance_api_secret)
        start_dt = datetime.combine(req.start_date, time.min, tzinfo=timezone.utc)
        _, excluded_symbols = client.filter_symbols_by_start_date(portfolio.assets(), start_dt)
        if excluded_symbols:
            preview = ", ".join(excluded_symbols[:10])
            more = "" if len(excluded_symbols) <= 10 else f" ... (+{len(excluded_symbols) - 10})"
            raise ValueError(
                "Selected strategy contains symbols that were not listed at start_date "
                f"{req.start_date.isoformat()}: {preview}{more}"
            )

        loader = MarketDataLoader(client)
        market_data = loader.load(portfolio.assets(), req.start_date, req.end_date)
        result = backtester.run(
            market_data=market_data,
            portfolio=portfolio,
            params=params,
            initial_capital_usdt=req.initial_capital_usdt,
            tp_pct=req.tp_pct,
            sl_pct=req.sl_pct,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result["strategy_id"] = str(uuid.uuid4())
    result["rank"] = 1
    aligned_curve = _align_curve_to_range(result.get("equity_curve", []), req.start_date, req.end_date)
    result["equity_curve"] = _downsample_curve(aligned_curve) if aligned_curve else []
    result["portfolio"] = portfolio.as_dict_list()

    runtime_store.set_backtest(
        all_strategies=[result],
        meta={
            "ranking_mode": req.ranking_mode,
            "start_date": req.start_date.isoformat(),
            "end_date": req.end_date.isoformat(),
            "top_k": 1,
            "tp_pct": req.tp_pct,
            "sl_pct": req.sl_pct,
        },
    )

    return BacktestResponse(
        ranking_mode=req.ranking_mode,
        strategies=[result],
    )


@app.post("/api/backtest/rerank", response_model=BacktestResponse)
def rerank_backtest(req: BacktestRerankRequest) -> BacktestResponse:
    ctx = runtime_store.get_backtest_context()
    if ctx is None:
        raise HTTPException(status_code=404, detail="No backtest context found. Run backtest first.")

    all_strategies = ctx["all_strategies"]
    strategies = optimizer.rank_strategies(
        all_strategies,
        req.ranking_mode,
        req.top_k,
        min_annualized_return=float(req.min_apy_pct) / 100.0,
        min_sharpe=float(req.min_sharpe),
        max_drawdown_abs=float(req.max_mdd_pct) / 100.0,
    )
    start_date = None
    end_date = None
    meta_start = ctx["meta"].get("start_date")
    meta_end = ctx["meta"].get("end_date")
    try:
        if isinstance(meta_start, str):
            start_date = date.fromisoformat(meta_start)
        if isinstance(meta_end, str):
            end_date = date.fromisoformat(meta_end)
    except Exception:
        start_date = None
        end_date = None

    for item in strategies:
        curve = item.get("equity_curve", [])
        if curve and start_date is not None and end_date is not None:
            aligned = _align_curve_to_range(curve, start_date, end_date)
            item["equity_curve"] = _downsample_curve(aligned) if aligned else []
        else:
            item["equity_curve"] = _downsample_curve(curve) if curve else []

    return BacktestResponse(
        ranking_mode=req.ranking_mode,
        strategies=strategies,
    )


@app.post("/api/calculator/plan", response_model=CalculatorPlanResponse)
def calculator_plan(req: CalculatorPlanRequest) -> CalculatorPlanResponse:
    strategy = None
    if req.strategy_id:
        strategy = runtime_store.get_strategy(req.strategy_id)
        if strategy is None:
            raise HTTPException(status_code=404, detail="Strategy not found")

    if strategy is not None:
        portfolio_input = strategy.get("portfolio", [])
        long_lev = float(req.long_leverage) if req.long_leverage is not None else float(
            strategy.get("params", {}).get("long_leverage", 1.0)
        )
        short_lev = float(req.short_leverage) if req.short_leverage is not None else float(
            strategy.get("params", {}).get("short_leverage", 1.0)
        )
    else:
        portfolio_input = req.portfolio or []
        long_lev = float(req.long_leverage or 1.0)
        short_lev = float(req.short_leverage or 1.0)

    try:
        normalized_portfolio = _normalize_portfolio_input(portfolio_input)
        portfolio = normalize_portfolio(
            [
                PortfolioLeg(
                    asset=str(leg.get("asset", "")).upper(),
                    weight=float(leg.get("weight", 0.0)),
                    direction=1 if str(leg.get("direction", "long")).lower() == "long" else -1,
                    leverage=float(leg.get("leverage")) if leg.get("leverage") is not None else None,
                )
                for leg in normalized_portfolio
            ]
        )

        plan = build_position_plan(
            total_capital_usdt=req.total_capital_usdt,
            portfolio=portfolio,
            long_leverage=long_lev,
            short_leverage=short_lev,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    rows = [
        CalculatorPlanRow(
            asset=row.asset,
            direction=row.direction,
            weight_pct=row.weight * 100.0,
            margin=row.margin,
            notional=row.notional,
            leverage=row.leverage,
        )
        for row in plan.rows
    ]

    return CalculatorPlanResponse(
        total_capital_usdt=plan.total_capital_usdt,
        total_margin_used=plan.total_margin_used,
        total_long_notional=plan.total_long_notional,
        total_short_notional=plan.total_short_notional,
        rows=rows,
    )
