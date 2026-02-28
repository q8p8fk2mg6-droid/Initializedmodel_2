from __future__ import annotations

from collections import OrderedDict
import math
import os
import random
from threading import Lock
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from joblib import Parallel, delayed
import numpy as np
from sklearn.ensemble import RandomForestRegressor

from app.schemas import BacktestRequest
from app.services.backtester import PortfolioBacktester, StrategyParams
from app.services.data_loader import MarketData, MarketDataLoader, UniverseMarketData
from app.services.portfolio import (
    PortfolioLeg,
    PortfolioSpec,
    generate_weight_splits,
    normalize_portfolio,
    portfolio_key,
    portfolio_to_vector,
)


@dataclass(frozen=True)
class CandidateParams:
    rehedge_hours: int
    rebalance_threshold_pct: float
    long_leverage: float
    short_leverage: float
    portfolio: PortfolioSpec

    def feature_vector(self, universe: list[str]) -> list[float]:
        base = [
            float(self.rehedge_hours),
            float(self.rebalance_threshold_pct),
            float(self.long_leverage),
            float(self.short_leverage),
        ]
        base.extend(portfolio_to_vector(self.portfolio, universe))
        index = {sym: i for i, sym in enumerate(universe)}
        leverage_vec = [0.0 for _ in universe]
        for leg in self.portfolio.legs:
            idx = index.get(leg.asset)
            if idx is None:
                continue
            default_lev = self.long_leverage if leg.direction > 0 else self.short_leverage
            leg_lev = float(leg.leverage) if leg.leverage is not None else float(default_lev)
            leverage_vec[idx] = leg_lev * (1.0 if leg.direction > 0 else -1.0)
        base.extend(leverage_vec)
        return base


@dataclass(frozen=True)
class SearchComplexityEstimate:
    units_total: int
    weight_split_counts: dict[int, int]
    total_weight_splits: int
    parameter_grid_size: int
    target_candidate_pool: int
    eval_budget: int


class PortfolioOptimizer:
    MAX_WEIGHT_SPLITS_PER_SIZE = 500_000
    MAX_WEIGHT_SPLITS_TOTAL = 1_000_000
    MAX_TARGET_CANDIDATE_POOL = 60_000
    MAX_EVAL_BUDGET = 20_000
    MAX_CANDIDATE_TO_EVAL_RATIO = 6
    MIN_MARKET_CACHE_ENTRIES = 16
    MAX_MARKET_CACHE_ENTRIES = 128

    def __init__(self, backtester: PortfolioBacktester) -> None:
        self.backtester = backtester
        self._stats_lock = Lock()
        self._runtime_stats: dict[str, Any] = {
            "running": False,
            "stage": "idle",
            "started_at_ts": 0.0,
            "ended_at_ts": 0.0,
            "last_error": "",
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_size": 0,
            "cache_limit": 0,
            "evaluated_count": 0,
            "eval_budget": 0,
            "parallel_workers": 0,
            "candidate_pool_size": 0,
            "memory_bytes": 0,
            "peak_memory_bytes": 0,
            "access_count": 0,
        }

    @staticmethod
    def _get_process_memory_bytes() -> int:
        try:
            if os.name == "nt":
                import ctypes

                class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                    _fields_ = [
                        ("cb", ctypes.c_ulong),
                        ("PageFaultCount", ctypes.c_ulong),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t),
                    ]

                counters = PROCESS_MEMORY_COUNTERS()
                counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
                ok = ctypes.windll.psapi.GetProcessMemoryInfo(
                    ctypes.windll.kernel32.GetCurrentProcess(),
                    ctypes.byref(counters),
                    counters.cb,
                )
                if ok:
                    return int(counters.WorkingSetSize)
                return 0

            import resource

            rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            if rss <= 0:
                return 0
            # Linux ru_maxrss is KB; macOS is bytes.
            return rss * 1024 if rss < 10_000_000 else rss
        except Exception:
            return 0

    def _start_runtime_stats(self) -> None:
        now = time.time()
        mem = self._get_process_memory_bytes()
        with self._stats_lock:
            self._runtime_stats = {
                "running": True,
                "stage": "preparing",
                "started_at_ts": now,
                "ended_at_ts": 0.0,
                "last_error": "",
                "cache_hits": 0,
                "cache_misses": 0,
                "cache_size": 0,
                "cache_limit": 0,
                "evaluated_count": 0,
                "eval_budget": 0,
                "parallel_workers": 0,
                "candidate_pool_size": 0,
                "memory_bytes": mem,
                "peak_memory_bytes": mem,
                "access_count": 0,
            }

    def _update_runtime_stats(self, **kwargs: Any) -> None:
        with self._stats_lock:
            for key, value in kwargs.items():
                self._runtime_stats[key] = value

    def _record_cache_access(self, *, hit: bool, cache_size: int, cache_limit: int) -> None:
        with self._stats_lock:
            if hit:
                self._runtime_stats["cache_hits"] = int(self._runtime_stats.get("cache_hits", 0)) + 1
            else:
                self._runtime_stats["cache_misses"] = int(self._runtime_stats.get("cache_misses", 0)) + 1
            access_count = int(self._runtime_stats.get("access_count", 0)) + 1
            self._runtime_stats["access_count"] = access_count
            self._runtime_stats["cache_size"] = int(cache_size)
            self._runtime_stats["cache_limit"] = int(cache_limit)
            # Sample RSS every 16 cache accesses to reduce overhead.
            if access_count % 16 == 0:
                mem = self._get_process_memory_bytes()
                self._runtime_stats["memory_bytes"] = mem
                self._runtime_stats["peak_memory_bytes"] = max(
                    int(self._runtime_stats.get("peak_memory_bytes", 0)),
                    mem,
                )

    def _finish_runtime_stats(self, error: str = "") -> None:
        now = time.time()
        mem = self._get_process_memory_bytes()
        with self._stats_lock:
            self._runtime_stats["running"] = False
            self._runtime_stats["stage"] = "idle"
            self._runtime_stats["ended_at_ts"] = now
            self._runtime_stats["last_error"] = str(error or "")
            self._runtime_stats["memory_bytes"] = mem
            self._runtime_stats["peak_memory_bytes"] = max(
                int(self._runtime_stats.get("peak_memory_bytes", 0)),
                mem,
            )

    def get_runtime_stats(self) -> dict[str, Any]:
        with self._stats_lock:
            stats = dict(self._runtime_stats)
        hits = int(stats.get("cache_hits", 0))
        misses = int(stats.get("cache_misses", 0))
        total = hits + misses
        hit_rate = (hits / total) if total > 0 else 0.0
        mem_bytes = int(stats.get("memory_bytes", 0))
        peak_mem_bytes = int(stats.get("peak_memory_bytes", 0))
        stats["cache_hit_rate"] = hit_rate
        stats["memory_mb"] = mem_bytes / (1024 * 1024)
        stats["peak_memory_mb"] = peak_mem_bytes / (1024 * 1024)
        return stats

    def mark_runtime_failed(self, error: str) -> None:
        self._finish_runtime_stats(error=error)

    def optimize(
        self,
        req: BacktestRequest,
        data_loader: MarketDataLoader,
        universe: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        self._start_runtime_stats()
        rehedge_vals = self._linear_values(
            req.rehedge_hours_min,
            req.rehedge_hours_max,
            req.rehedge_hours_step,
            cast_int=True,
        )
        threshold_vals = self._linear_values(
            req.rebalance_threshold_pct_min,
            req.rebalance_threshold_pct_max,
            req.rebalance_threshold_pct_step,
            cast_int=False,
            quant="0.1",
        )
        long_leverage_vals = self._linear_values(
            req.long_leverage_min,
            req.long_leverage_max,
            req.long_leverage_step,
            cast_int=False,
            quant="0.1",
        )
        short_leverage_vals = self._linear_values(
            req.short_leverage_min,
            req.short_leverage_max,
            req.short_leverage_step,
            cast_int=False,
            quant="0.1",
        )
        complexity = self._estimate_complexity(
            req,
            rehedge_vals,
            threshold_vals,
            long_leverage_vals,
            short_leverage_vals,
        )
        self._raise_if_complexity_too_high(req, complexity)
        requested_target_pool = self._resolve_target_pool_size(req.candidate_pool_size, req.max_evals)
        target_pool, eval_budget_limit = self._resolve_safe_search_limits(
            requested_target_pool=requested_target_pool,
            requested_max_evals=req.max_evals,
        )

        rng = random.Random(req.random_seed)
        weight_splits = self._build_weight_splits(
            req.portfolio_size_min,
            req.portfolio_size_max,
            req.weight_step_pct,
        )
        if req.portfolio_size_max > len(universe):
            raise ValueError(
                "Parameter invalid: Top contracts count is smaller than max portfolio size "
                f"({len(universe)} < {req.portfolio_size_max})."
            )

        candidate_pool = self._build_candidate_pool(
            req,
            target_pool,
            universe,
            weight_splits,
            rehedge_vals,
            threshold_vals,
            long_leverage_vals,
            short_leverage_vals,
            rng,
        )
        self._update_runtime_stats(
            stage="sampling",
            candidate_pool_size=len(candidate_pool),
            eval_budget=int(eval_budget_limit),
        )

        if not candidate_pool:
            raise ValueError("No candidate pool available for optimization")

        preloaded_market_data = data_loader.load_universe(
            self._collect_candidate_symbols(candidate_pool),
            req.start_date,
            req.end_date,
        )
        available_symbols = set(preloaded_market_data.price_series.keys())
        candidate_pool = [
            c for c in candidate_pool if all(sym in available_symbols for sym in c.portfolio.assets())
        ]
        if not candidate_pool:
            raise ValueError("No candidate has complete market data in selected date range")

        eval_budget = min(eval_budget_limit, len(candidate_pool))
        parallel_workers = self._resolve_parallel_workers(req.parallel_workers, eval_budget)
        market_cache_entries = self._resolve_market_cache_entries(req, parallel_workers, eval_budget)
        self._update_runtime_stats(
            stage="evaluating",
            eval_budget=int(eval_budget),
            parallel_workers=int(parallel_workers),
            cache_limit=int(market_cache_entries),
        )
        keep_all_curves = (req.execution_mode == "performance")
        market_data_cache: OrderedDict[tuple[str, ...], MarketData] = OrderedDict()
        cache_lock = Lock()

        warmup_n = min(max(24, eval_budget // 4), eval_budget, len(candidate_pool))
        evaluated = self._evaluate_batch(
            candidate_pool[:warmup_n],
            req,
            data_loader,
            preloaded_market_data,
            market_data_cache,
            cache_lock,
            market_cache_entries,
            parallel_workers=parallel_workers,
            keep_curve=keep_all_curves,
        )
        evaluated = [item for item in evaluated if item is not None]
        self._update_runtime_stats(evaluated_count=len(evaluated))

        remaining_budget = eval_budget - len(evaluated)
        if remaining_budget > 0:
            remaining = candidate_pool[warmup_n:]
            if evaluated and remaining:
                chosen = self._ml_pick(evaluated, remaining, universe, remaining_budget)
            else:
                chosen = remaining[:remaining_budget]
            evaluated.extend(
                [
                    item
                    for item in self._evaluate_batch(
                        chosen,
                        req,
                        data_loader,
                        preloaded_market_data,
                        market_data_cache,
                        cache_lock,
                        market_cache_entries,
                        parallel_workers,
                        keep_curve=keep_all_curves,
                    )
                    if item is not None
                ]
            )
            self._update_runtime_stats(evaluated_count=len(evaluated))

        if not evaluated:
            raise ValueError("No valid strategies evaluated")

        for item in evaluated:
            item["strategy_id"] = str(uuid.uuid4())

        min_annualized_return = float(req.min_apy_pct) / 100.0
        min_sharpe = float(req.min_sharpe)
        max_drawdown_abs = float(req.max_mdd_pct) / 100.0
        ranking_modes = ["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"]
        self._update_runtime_stats(stage="ranking")
        top_by_mode: dict[str, list[dict[str, Any]]] = {
            mode: self.rank_strategies(
                evaluated,
                mode,
                req.top_k,
                min_annualized_return=min_annualized_return,
                min_sharpe=min_sharpe,
                max_drawdown_abs=max_drawdown_abs,
            )
            for mode in ranking_modes
        }
        top = top_by_mode.get(req.ranking_mode, [])
        if not keep_all_curves:
            union_map: dict[str, dict[str, Any]] = {}
            for mode in ranking_modes:
                for item in top_by_mode.get(mode, []):
                    sid = str(item.get("strategy_id", ""))
                    if sid and sid not in union_map:
                        union_map[sid] = item
            self._hydrate_top_curves(
                list(union_map.values()),
                req,
                data_loader,
                preloaded_market_data,
                market_data_cache,
                cache_lock,
                market_cache_entries,
            )
            top_curve_map = {
                sid: [list(point) for point in item.get("equity_curve", [])]
                for sid, item in union_map.items()
            }
            for item in evaluated:
                sid = str(item.get("strategy_id", ""))
                item["equity_curve"] = top_curve_map.get(sid, [])
        self._finish_runtime_stats(error="")
        return {
            "all_strategies": evaluated,
            "top_strategies": top,
            "top_strategies_by_mode": top_by_mode,
        }

    def _build_weight_splits(
        self,
        min_assets: int,
        max_assets: int,
        step_pct: float,
    ) -> dict[int, list[list[float]]]:
        splits: dict[int, list[list[float]]] = {}
        for n in range(min_assets, max_assets + 1):
            combos = generate_weight_splits(n, step_pct)
            if not combos:
                raise ValueError(f"No positive weight splits for {n} assets")
            splits[n] = combos
        return splits

    def _sample_portfolio(
        self,
        universe: list[str],
        weight_splits: dict[int, list[list[float]]],
        min_assets: int,
        max_assets: int,
        require_both: bool,
        long_leverage_vals: list[float],
        short_leverage_vals: list[float],
        rng: random.Random,
    ) -> PortfolioSpec:
        attempt = 0
        while True:
            attempt += 1
            if attempt > 200:
                raise ValueError("Failed to sample a valid portfolio")
            asset_count = rng.randint(min_assets, max_assets)
            assets = rng.sample(universe, asset_count)
            weights = rng.choice(weight_splits[asset_count])
            directions = [1 if rng.random() >= 0.5 else -1 for _ in range(asset_count)]
            if require_both:
                if all(d > 0 for d in directions) or all(d < 0 for d in directions):
                    continue
            legs: list[PortfolioLeg] = []
            for a, w, d in zip(assets, weights, directions):
                lev = float(rng.choice(long_leverage_vals if d > 0 else short_leverage_vals))
                legs.append(PortfolioLeg(asset=a, weight=w, direction=d, leverage=lev))
            return normalize_portfolio(legs)

    def _build_candidate_pool(
        self,
        req: BacktestRequest,
        target_pool: int,
        universe: list[str],
        weight_splits: dict[int, list[list[float]]],
        rehedge_vals: list[int],
        threshold_vals: list[float],
        long_leverage_vals: list[float],
        short_leverage_vals: list[float],
        rng: random.Random,
    ) -> list[CandidateParams]:
        pool: list[CandidateParams] = []
        seen: set[tuple] = set()
        max_attempts = target_pool * 20
        attempts = 0

        while len(pool) < target_pool and attempts < max_attempts:
            attempts += 1
            portfolio = self._sample_portfolio(
                universe,
                weight_splits,
                req.portfolio_size_min,
                req.portfolio_size_max,
                req.require_both_directions,
                long_leverage_vals,
                short_leverage_vals,
                rng,
            )
            long_lev_values = [float(leg.leverage or 1.0) for leg in portfolio.legs if leg.direction > 0]
            short_lev_values = [float(leg.leverage or 1.0) for leg in portfolio.legs if leg.direction < 0]
            candidate = CandidateParams(
                rehedge_hours=int(rng.choice(rehedge_vals)),
                rebalance_threshold_pct=float(rng.choice(threshold_vals)),
                long_leverage=(
                    float(sum(long_lev_values) / len(long_lev_values))
                    if long_lev_values
                    else float(long_leverage_vals[0])
                ),
                short_leverage=(
                    float(sum(short_lev_values) / len(short_lev_values))
                    if short_lev_values
                    else float(short_leverage_vals[0])
                ),
                portfolio=portfolio,
            )
            key = (
                candidate.rehedge_hours,
                round(candidate.rebalance_threshold_pct, 6),
                round(candidate.long_leverage, 6),
                round(candidate.short_leverage, 6),
                portfolio_key(portfolio),
            )
            if key in seen:
                continue
            seen.add(key)
            pool.append(candidate)

        return pool

    @staticmethod
    def _collect_candidate_symbols(candidates: list[CandidateParams]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for candidate in candidates:
            for sym in candidate.portfolio.assets():
                if sym in seen:
                    continue
                seen.add(sym)
                ordered.append(sym)
        return ordered

    def _evaluate_candidate(
        self,
        candidate: CandidateParams,
        req: BacktestRequest,
        data_loader: MarketDataLoader,
        preloaded_market_data: UniverseMarketData,
        market_data_cache: OrderedDict[tuple[str, ...], MarketData],
        cache_lock: Lock,
        market_cache_entries: int,
        keep_curve: bool = False,
    ) -> dict[str, Any] | None:
        try:
            market_data = self._get_market_data(
                data_loader,
                preloaded_market_data,
                candidate.portfolio.assets(),
                market_data_cache,
                cache_lock,
                market_cache_entries,
            )

            params = StrategyParams(
                rehedge_hours=candidate.rehedge_hours,
                rebalance_threshold_pct=candidate.rebalance_threshold_pct,
                long_leverage=candidate.long_leverage,
                short_leverage=candidate.short_leverage,
            )
            result = self.backtester.run(
                market_data=market_data,
                portfolio=candidate.portfolio,
                params=params,
                initial_capital_usdt=req.initial_capital_usdt,
            )
            result["portfolio"] = candidate.portfolio.as_dict_list()
            if not keep_curve:
                # Keep full metrics for ranking, but drop curve to reduce memory pressure.
                result["equity_curve"] = []
            return result
        except Exception:
            return None

    def _get_market_data(
        self,
        data_loader: MarketDataLoader,
        preloaded_market_data: UniverseMarketData,
        symbols: list[str],
        market_data_cache: OrderedDict[tuple[str, ...], MarketData],
        cache_lock: Lock,
        market_cache_entries: int,
    ) -> MarketData:
        market_key = tuple(symbols)
        with cache_lock:
            market_data = market_data_cache.get(market_key)
            if market_data is not None:
                market_data_cache.move_to_end(market_key)
                self._record_cache_access(
                    hit=True,
                    cache_size=len(market_data_cache),
                    cache_limit=market_cache_entries,
                )
                return market_data

        built = data_loader.slice_market_data(preloaded_market_data, symbols)

        with cache_lock:
            market_data = market_data_cache.get(market_key)
            if market_data is not None:
                market_data_cache.move_to_end(market_key)
                self._record_cache_access(
                    hit=True,
                    cache_size=len(market_data_cache),
                    cache_limit=market_cache_entries,
                )
                return market_data

            market_data_cache[market_key] = built
            market_data_cache.move_to_end(market_key)
            while len(market_data_cache) > market_cache_entries:
                market_data_cache.popitem(last=False)
            self._record_cache_access(
                hit=False,
                cache_size=len(market_data_cache),
                cache_limit=market_cache_entries,
            )
            return built

    def _hydrate_top_curves(
        self,
        strategies: list[dict[str, Any]],
        req: BacktestRequest,
        data_loader: MarketDataLoader,
        preloaded_market_data: UniverseMarketData,
        market_data_cache: OrderedDict[tuple[str, ...], MarketData],
        cache_lock: Lock,
        market_cache_entries: int,
    ) -> None:
        if not strategies:
            return

        candidates: list[CandidateParams] = []
        candidate_idx: list[int] = []
        for idx, item in enumerate(strategies):
            try:
                portfolio = normalize_portfolio(
                    [
                        PortfolioLeg(
                            asset=str(leg.get("asset", "")).upper(),
                            weight=float(leg.get("weight", 0.0)),
                            direction=1 if str(leg.get("direction", "long")).lower() == "long" else -1,
                            leverage=float(leg.get("leverage"))
                            if leg.get("leverage") is not None
                            else None,
                        )
                        for leg in item.get("portfolio", [])
                    ]
                )
                params_raw = item.get("params", {})
                candidates.append(
                    CandidateParams(
                        rehedge_hours=int(params_raw.get("rehedge_hours", 24)),
                        rebalance_threshold_pct=float(params_raw.get("rebalance_threshold_pct", 1.0)),
                        long_leverage=float(params_raw.get("long_leverage", 1.0)),
                        short_leverage=float(params_raw.get("short_leverage", 1.0)),
                        portfolio=portfolio,
                    )
                )
                candidate_idx.append(idx)
            except Exception:
                item["equity_curve"] = []

        if not candidates:
            return

        self._update_runtime_stats(stage="hydrating_curves")
        curve_workers = self._resolve_parallel_workers(req.parallel_workers, len(candidates))
        hydrated = self._evaluate_batch(
            candidates,
            req,
            data_loader,
            preloaded_market_data,
            market_data_cache,
            cache_lock,
            market_cache_entries,
            parallel_workers=curve_workers,
            keep_curve=True,
        )
        for idx, result in zip(candidate_idx, hydrated):
            if result is None:
                strategies[idx]["equity_curve"] = []
            else:
                strategies[idx]["equity_curve"] = [list(point) for point in result.get("equity_curve", [])]

    def _evaluate_batch(
        self,
        candidates: list[CandidateParams],
        req: BacktestRequest,
        data_loader: MarketDataLoader,
        preloaded_market_data: UniverseMarketData,
        market_data_cache: OrderedDict[tuple[str, ...], MarketData],
        cache_lock: Lock,
        market_cache_entries: int,
        parallel_workers: int,
        keep_curve: bool = False,
    ) -> list[dict[str, Any] | None]:
        if not candidates:
            return []
        if parallel_workers <= 1 or len(candidates) <= 1:
            return [
                self._evaluate_candidate(
                    c,
                    req,
                    data_loader,
                    preloaded_market_data,
                    market_data_cache,
                    cache_lock,
                    market_cache_entries,
                    keep_curve=keep_curve,
                )
                for c in candidates
            ]

        try:
            return Parallel(
                n_jobs=parallel_workers,
                backend="threading",
                pre_dispatch="2*n_jobs",
            )(
                delayed(self._evaluate_candidate)(
                    c,
                    req,
                    data_loader,
                    preloaded_market_data,
                    market_data_cache,
                    cache_lock,
                    market_cache_entries,
                    keep_curve,
                )
                for c in candidates
            )
        except Exception:
            return [
                self._evaluate_candidate(
                    c,
                    req,
                    data_loader,
                    preloaded_market_data,
                    market_data_cache,
                    cache_lock,
                    market_cache_entries,
                    keep_curve=keep_curve,
                )
                for c in candidates
            ]

    def _ml_pick(
        self,
        evaluated: list[dict[str, Any]],
        remaining: list[CandidateParams],
        universe: list[str],
        budget: int,
    ) -> list[CandidateParams]:
        if not remaining:
            return []
        if not evaluated:
            return remaining[:budget]

        x_train = np.array(
            [
                self._features_from_result(item, universe)
                for item in evaluated
            ],
            dtype=float,
        )
        y_train = np.array(
            [self._objective(item["annualized_return"], item["max_drawdown"]) for item in evaluated],
            dtype=float,
        )

        model = RandomForestRegressor(
            n_estimators=300,
            max_depth=8,
            random_state=42,
            min_samples_leaf=2,
            n_jobs=-1,
        )
        model.fit(x_train, y_train)

        x_candidates = np.array([c.feature_vector(universe) for c in remaining], dtype=float)
        preds = model.predict(x_candidates)
        order = np.argsort(preds)[::-1]
        picked = [remaining[int(i)] for i in order[:budget]]
        return picked

    @staticmethod
    def _resolve_target_pool_size(candidate_pool_size: int, max_evals: int) -> int:
        if candidate_pool_size > 0:
            return int(candidate_pool_size)
        eval_budget = max(int(max_evals), 1)
        return min(max(eval_budget * 30, 300), 8000)

    @classmethod
    def _resolve_safe_search_limits(
        cls,
        requested_target_pool: int,
        requested_max_evals: int,
    ) -> tuple[int, int]:
        target_requested = max(int(requested_target_pool), 1)
        eval_requested = max(min(int(requested_max_evals), target_requested), 1)

        eval_budget = min(eval_requested, cls.MAX_EVAL_BUDGET)
        target_cap = min(
            cls.MAX_TARGET_CANDIDATE_POOL,
            max(eval_budget * cls.MAX_CANDIDATE_TO_EVAL_RATIO, eval_budget),
        )
        target_pool = max(eval_budget, min(target_requested, target_cap))
        return target_pool, eval_budget

    def _estimate_complexity(
        self,
        req: BacktestRequest,
        rehedge_vals: list[int],
        threshold_vals: list[float],
        long_leverage_vals: list[float],
        short_leverage_vals: list[float],
    ) -> SearchComplexityEstimate:
        step_pct = float(req.weight_step_pct)
        units_total = int(round(100.0 / step_pct))
        if units_total <= 0:
            raise ValueError("Invalid weight step.")

        weight_split_counts: dict[int, int] = {}
        for n in range(req.portfolio_size_min, req.portfolio_size_max + 1):
            if n > units_total:
                raise ValueError(
                    "Parameter invalid: with weight_step_pct="
                    f"{step_pct:g}%, at most {units_total} assets can have positive weights, "
                    f"but portfolio_size_max is {req.portfolio_size_max}."
                )
            weight_split_counts[n] = math.comb(units_total - 1, n - 1)

        total_weight_splits = sum(weight_split_counts.values())
        parameter_grid_size = (
            len(rehedge_vals)
            * len(threshold_vals)
            * len(long_leverage_vals)
            * len(short_leverage_vals)
        )
        target_candidate_pool = self._resolve_target_pool_size(req.candidate_pool_size, req.max_evals)
        eval_budget = min(int(req.max_evals), target_candidate_pool)
        return SearchComplexityEstimate(
            units_total=units_total,
            weight_split_counts=weight_split_counts,
            total_weight_splits=total_weight_splits,
            parameter_grid_size=parameter_grid_size,
            target_candidate_pool=target_candidate_pool,
            eval_budget=eval_budget,
        )

    def _raise_if_complexity_too_high(
        self,
        req: BacktestRequest,
        estimate: SearchComplexityEstimate,
    ) -> None:
        max_n, max_count = max(estimate.weight_split_counts.items(), key=lambda kv: kv[1])
        if max_count > self.MAX_WEIGHT_SPLITS_PER_SIZE:
            raise ValueError(
                "Search complexity too high: weight_step_pct="
                f"{float(req.weight_step_pct):g}% with portfolio size {max_n} generates about "
                f"{max_count:,} positive weight splits (limit {self.MAX_WEIGHT_SPLITS_PER_SIZE:,}). "
                "Please increase weight_step_pct (suggest 5 or 10) or reduce portfolio_size_max (suggest <=5)."
            )
        if estimate.total_weight_splits > self.MAX_WEIGHT_SPLITS_TOTAL:
            raise ValueError(
                "Search complexity too high: total positive weight splits across "
                f"{req.portfolio_size_min}~{req.portfolio_size_max} assets is about "
                f"{estimate.total_weight_splits:,} (limit {self.MAX_WEIGHT_SPLITS_TOTAL:,}). "
                "Please increase weight_step_pct or narrow portfolio size range."
            )

    def _features_from_result(self, item: dict[str, Any], universe: list[str]) -> list[float]:
        params = item.get("params", {})
        portfolio = item.get("portfolio", [])
        legs = [
            PortfolioLeg(
                asset=str(leg.get("asset", "")).upper(),
                weight=float(leg.get("weight", 0.0)),
                direction=1 if str(leg.get("direction", "long")).lower() == "long" else -1,
                leverage=float(leg.get("leverage"))
                if leg.get("leverage") is not None
                else None,
            )
            for leg in portfolio
        ]
        spec = normalize_portfolio(legs)
        base = [
            float(params.get("rehedge_hours", 0)),
            float(params.get("rebalance_threshold_pct", 0.0)),
            float(params.get("long_leverage", 1.0)),
            float(params.get("short_leverage", 1.0)),
        ]
        base.extend(portfolio_to_vector(spec, universe))
        index = {sym: i for i, sym in enumerate(universe)}
        leverage_vec = [0.0 for _ in universe]
        for leg in spec.legs:
            idx = index.get(leg.asset)
            if idx is None:
                continue
            default_lev = float(params.get("long_leverage", 1.0)) if leg.direction > 0 else float(
                params.get("short_leverage", 1.0)
            )
            leg_lev = float(leg.leverage) if leg.leverage is not None else default_lev
            leverage_vec[idx] = leg_lev * (1.0 if leg.direction > 0 else -1.0)
        base.extend(leverage_vec)
        return base

    @staticmethod
    def _resolve_parallel_workers(requested: int, eval_budget: int) -> int:
        if eval_budget <= 1:
            return 1
        cpu = os.cpu_count() or 1
        if requested <= 0:
            workers = max(cpu - 1, 1)
        else:
            workers = min(int(requested), cpu)
        return max(1, min(workers, eval_budget))

    @classmethod
    def _resolve_market_cache_entries(
        cls,
        req: BacktestRequest,
        parallel_workers: int,
        eval_budget: int,
    ) -> int:
        # Keep cache bounded to avoid OOM on high-eval/high-parallel runs.
        if req.execution_mode == "memory":
            target = max(cls.MIN_MARKET_CACHE_ENTRIES, parallel_workers * 2)
        else:
            target = max(32, parallel_workers * 4)
        if eval_budget >= 10_000:
            target = min(target, 96)
        if eval_budget >= 20_000:
            target = min(target, 64)
        return max(cls.MIN_MARKET_CACHE_ENTRIES, min(target, cls.MAX_MARKET_CACHE_ENTRIES))

    @staticmethod
    def _objective(annualized_return: float, max_drawdown: float) -> float:
        return annualized_return + max_drawdown * 0.2

    @staticmethod
    def _rank_key(mode: str):
        if mode == "mdd_asc_return_desc":
            return lambda item: (abs(float(item["max_drawdown"])), -float(item["annualized_return"]))
        if mode == "sharpe_desc_return_desc":
            return lambda item: (
                -float(item["sharpe"]),
                -float(item["annualized_return"]),
                abs(float(item["max_drawdown"])),
            )
        return lambda item: (-float(item["annualized_return"]), abs(float(item["max_drawdown"])))

    def rank_strategies(
        self,
        strategies: list[dict[str, Any]],
        ranking_mode: str,
        top_k: int,
        min_annualized_return: float = 0.0,
        min_sharpe: float = 1.5,
        max_drawdown_abs: float = 0.8,
    ) -> list[dict[str, Any]]:
        ordered = [
            dict(item)
            for item in strategies
            if float(item.get("annualized_return", -999.0)) >= float(min_annualized_return)
            and float(item.get("sharpe", -999.0)) >= float(min_sharpe)
            and abs(float(item.get("max_drawdown", -1.0))) <= float(max_drawdown_abs)
        ]
        ordered.sort(key=self._rank_key(ranking_mode))
        top = ordered[:top_k]
        for rank, item in enumerate(top, start=1):
            item["rank"] = rank
        return top

    @staticmethod
    def _linear_values(
        start: float,
        end: float,
        step: float,
        cast_int: bool,
        quant: str | None = None,
    ) -> list[float | int]:
        start_d = Decimal(str(start))
        end_d = Decimal(str(end))
        step_d = Decimal(str(step))
        if step_d <= 0:
            raise ValueError("step must be > 0")
        if end_d < start_d:
            raise ValueError("end must be >= start")

        count = int(((end_d - start_d) / step_d).to_integral_value(rounding=ROUND_HALF_UP)) + 1
        values: list[float | int] = []
        q = Decimal(quant) if quant else None
        for i in range(count):
            cur = start_d + step_d * i
            if cur > end_d:
                cur = end_d
            if q is not None:
                cur = cur.quantize(q)
            if cast_int:
                values.append(int(cur))
            else:
                values.append(float(cur))

        if values:
            last = values[-1]
            if cast_int:
                if int(last) != int(end_d):
                    values.append(int(end_d))
            else:
                end_f = float(end_d if q is None else end_d.quantize(q))
                if abs(float(last) - end_f) > 1e-12:
                    values.append(end_f)

        deduped = []
        seen = set()
        for v in values:
            key = f"{v}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(v)
        return deduped
