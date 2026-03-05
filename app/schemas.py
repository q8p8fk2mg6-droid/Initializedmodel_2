from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class PortfolioLegModel(BaseModel):
    asset: str
    weight: float
    direction: Literal["long", "short"] = "long"
    leverage: float | None = Field(default=None, ge=1.0, le=20.0)


class BacktestRequest(BaseModel):
    start_date: date
    end_date: date
    binance_api_key: str | None = None
    binance_api_secret: str | None = None
    initial_capital_usdt: float = Field(default=1000.0, gt=0)
    top_k: int = Field(default=50, ge=1, le=50)
    max_evals: int = Field(default=10000, ge=10)
    parallel_workers: int = Field(default=32, ge=0, le=128)
    execution_mode: Literal["performance", "memory"] = "performance"
    ranking_mode: Literal["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"] = "sharpe_desc_return_desc"
    min_apy_pct: float = Field(default=0.0, ge=-100.0, le=10000.0)
    min_sharpe: float = Field(default=1.5, ge=-100.0, le=100.0)
    max_mdd_pct: float = Field(default=30.0, ge=0.0, le=100.0)

    universe_limit: int = Field(default=100, ge=10, le=200)
    portfolio_size_min: int = Field(default=3, ge=1, le=10)
    portfolio_size_max: int = Field(default=4, ge=1, le=10)
    weight_step_pct: float = Field(default=5.0, gt=0.0, le=50.0)
    require_both_directions: bool = True
    candidate_pool_size: int = Field(default=0, ge=0)
    random_seed: int | None = None

    rehedge_hours_min: int = Field(default=1, ge=0)
    rehedge_hours_max: int = Field(default=720, ge=0)
    rehedge_hours_step: int = Field(default=1, ge=1)
    rebalance_threshold_pct_min: float = Field(default=0.0, ge=0.0, le=100.0)
    rebalance_threshold_pct_max: float = Field(default=5.0, ge=0.0, le=100.0)
    rebalance_threshold_pct_step: float = Field(default=1.0, gt=0.0, le=100.0)
    long_leverage_min: float = Field(default=1.0, ge=1.0, le=20.0)
    long_leverage_max: float = Field(default=3.0, ge=1.0, le=20.0)
    long_leverage_step: float = Field(default=0.5, gt=0.0, le=20.0)
    short_leverage_min: float = Field(default=1.0, ge=1.0, le=20.0)
    short_leverage_max: float = Field(default=3.0, ge=1.0, le=20.0)
    short_leverage_step: float = Field(default=0.5, gt=0.0, le=20.0)

    @model_validator(mode="after")
    def validate_request(self) -> "BacktestRequest":
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be later than start_date")
        if self.portfolio_size_min > self.portfolio_size_max:
            raise ValueError("portfolio_size_min must be <= portfolio_size_max")
        if self.rehedge_hours_min > self.rehedge_hours_max:
            raise ValueError("rehedge_hours_min must be <= rehedge_hours_max")
        if self.rebalance_threshold_pct_min > self.rebalance_threshold_pct_max:
            raise ValueError("rebalance_threshold_pct_min must be <= rebalance_threshold_pct_max")
        if self.long_leverage_min > self.long_leverage_max:
            raise ValueError("long_leverage_min must be <= long_leverage_max")
        if self.short_leverage_min > self.short_leverage_max:
            raise ValueError("short_leverage_min must be <= short_leverage_max")
        step = float(self.weight_step_pct)
        units_total = 100.0 / step
        if abs(units_total - round(units_total)) > 1e-6:
            raise ValueError("weight_step_pct must divide 100 evenly")
        return self


class StrategyParams(BaseModel):
    rehedge_hours: int
    rebalance_threshold_pct: float
    long_leverage: float
    short_leverage: float
    leverage_mode: str | None = None


class BacktestCandidate(BaseModel):
    strategy_id: str
    rank: int
    params: StrategyParams
    annualized_return: float
    total_return: float
    annualized_volatility: float
    sharpe: float
    max_drawdown: float
    funding_income: float
    trading_fees: float
    rehedge_count: int
    equity_curve: list[list[float | int]]
    portfolio: list[PortfolioLegModel]
    tp_pct: float | None = None
    sl_pct: float | None = None
    stop_triggered: bool = False
    exit_reason: Literal["tp", "sl", "end"] | None = None
    exit_timestamp_ms: int | None = None
    exit_nav: float | None = None


class BacktestResponse(BaseModel):
    ranking_mode: Literal["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"]
    strategies: list[BacktestCandidate]


class BacktestTimelinessRequest(BaseModel):
    decision_date: date
    forward_days: int = Field(default=30, ge=7, le=120)
    lookback_windows_days: list[int] = Field(default_factory=lambda: [30, 60, 90, 180, 360], min_length=1, max_length=12)
    anchor_count: int = Field(default=6, ge=1, le=24)

    binance_api_key: str | None = None
    binance_api_secret: str | None = None
    initial_capital_usdt: float = Field(default=1000.0, gt=0)
    top_k: int = Field(default=50, ge=1, le=50)
    max_evals: int = Field(default=10000, ge=10)
    parallel_workers: int = Field(default=32, ge=0, le=128)
    execution_mode: Literal["performance", "memory"] = "performance"
    ranking_mode: Literal["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"] = "sharpe_desc_return_desc"
    min_apy_pct: float = Field(default=0.0, ge=-100.0, le=10000.0)
    min_sharpe: float = Field(default=1.5, ge=-100.0, le=100.0)
    max_mdd_pct: float = Field(default=30.0, ge=0.0, le=100.0)

    universe_limit: int = Field(default=100, ge=10, le=200)
    portfolio_size_min: int = Field(default=3, ge=1, le=10)
    portfolio_size_max: int = Field(default=4, ge=1, le=10)
    weight_step_pct: float = Field(default=5.0, gt=0.0, le=50.0)
    require_both_directions: bool = True
    candidate_pool_size: int = Field(default=0, ge=0)
    random_seed: int | None = None

    rehedge_hours_min: int = Field(default=1, ge=0)
    rehedge_hours_max: int = Field(default=720, ge=0)
    rehedge_hours_step: int = Field(default=1, ge=1)
    rebalance_threshold_pct_min: float = Field(default=0.0, ge=0.0, le=100.0)
    rebalance_threshold_pct_max: float = Field(default=5.0, ge=0.0, le=100.0)
    rebalance_threshold_pct_step: float = Field(default=1.0, gt=0.0, le=100.0)
    long_leverage_min: float = Field(default=1.0, ge=1.0, le=20.0)
    long_leverage_max: float = Field(default=3.0, ge=1.0, le=20.0)
    long_leverage_step: float = Field(default=0.5, gt=0.0, le=20.0)
    short_leverage_min: float = Field(default=1.0, ge=1.0, le=20.0)
    short_leverage_max: float = Field(default=3.0, ge=1.0, le=20.0)
    short_leverage_step: float = Field(default=0.5, gt=0.0, le=20.0)

    @field_validator("lookback_windows_days", mode="before")
    @classmethod
    def validate_lookback_windows_days(cls, value: Any) -> list[int]:
        raw_list: list[Any]
        if isinstance(value, str):
            raw_list = [x.strip() for x in value.split(",")]
        elif isinstance(value, (list, tuple, set)):
            raw_list = list(value)
        else:
            raise ValueError("lookback_windows_days must be a list of integers")

        seen: set[int] = set()
        out: list[int] = []
        for raw in raw_list:
            try:
                days = int(raw)
            except Exception as exc:
                raise ValueError(f"Invalid lookback day value: {raw}") from exc
            if days < 7 or days > 2000:
                raise ValueError(f"lookback day out of range [7, 2000]: {days}")
            if days in seen:
                continue
            seen.add(days)
            out.append(days)
        if not out:
            raise ValueError("lookback_windows_days must contain at least one valid value")
        return out

    @model_validator(mode="after")
    def validate_request(self) -> "BacktestTimelinessRequest":
        if self.portfolio_size_min > self.portfolio_size_max:
            raise ValueError("portfolio_size_min must be <= portfolio_size_max")
        if self.rehedge_hours_min > self.rehedge_hours_max:
            raise ValueError("rehedge_hours_min must be <= rehedge_hours_max")
        if self.rebalance_threshold_pct_min > self.rebalance_threshold_pct_max:
            raise ValueError("rebalance_threshold_pct_min must be <= rebalance_threshold_pct_max")
        if self.long_leverage_min > self.long_leverage_max:
            raise ValueError("long_leverage_min must be <= long_leverage_max")
        if self.short_leverage_min > self.short_leverage_max:
            raise ValueError("short_leverage_min must be <= short_leverage_max")
        step = float(self.weight_step_pct)
        units_total = 100.0 / step
        if abs(units_total - round(units_total)) > 1e-6:
            raise ValueError("weight_step_pct must divide 100 evenly")
        return self


class TimelinessWindowResult(BaseModel):
    lookback_days: int
    score: float | None = None
    anchors_requested: int
    anchors_completed: int
    avg_annualized_return: float | None = None
    avg_total_return: float | None = None
    avg_sharpe: float | None = None
    avg_max_drawdown: float | None = None
    win_rate: float | None = None
    learned_min_apy_pct: float | None = None
    learned_min_sharpe: float | None = None
    learned_max_mdd_pct: float | None = None
    notes: str | None = None


class BacktestTimelinessResponse(BaseModel):
    ranking_mode: Literal["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"]
    decision_date: date
    deploy_end_date: date
    best_lookback_days: int | None = None
    applied_min_apy_pct: float | None = None
    applied_min_sharpe: float | None = None
    applied_max_mdd_pct: float | None = None
    timeliness_run_id: str | None = None
    lookback_results: list[TimelinessWindowResult]
    strategies: list[BacktestCandidate]


class TimelinessHistoryRunRecord(BaseModel):
    run_id: str
    created_at: str
    meta: dict[str, Any]
    lookback_results: list[TimelinessWindowResult]


class TimelinessHistoryRunsResponse(BaseModel):
    runs: list[TimelinessHistoryRunRecord]


class TimelinessLookbackStrategiesResponse(BaseModel):
    run_id: str
    lookback_days: int
    anchor_index: int
    ranking_mode: Literal["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"]
    train_start_date: date
    train_end_date: date
    test_start_date: date
    test_end_date: date
    applied_min_apy_pct: float | None = None
    applied_min_sharpe: float | None = None
    applied_max_mdd_pct: float | None = None
    strategies: list[BacktestCandidate]


class BacktestRerankRequest(BaseModel):
    ranking_mode: Literal["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"] = "sharpe_desc_return_desc"
    min_apy_pct: float = Field(default=0.0, ge=-100.0, le=10000.0)
    min_sharpe: float = Field(default=1.5, ge=-100.0, le=100.0)
    max_mdd_pct: float = Field(default=30.0, ge=0.0, le=100.0)
    top_k: int = Field(default=50, ge=1, le=50)


class CustomBacktestRequest(BaseModel):
    start_date: date
    end_date: date
    portfolio: list[PortfolioLegModel]
    binance_api_key: str | None = None
    binance_api_secret: str | None = None
    initial_capital_usdt: float = Field(default=1000.0, gt=0)
    rehedge_hours: int = Field(default=24, ge=0)
    rebalance_threshold_pct: float = Field(default=1.0, ge=0.0, le=100.0)
    long_leverage: float = Field(default=1.0, ge=1.0, le=20.0)
    short_leverage: float = Field(default=1.0, ge=1.0, le=20.0)
    tp_pct: float | None = Field(default=None, gt=0.0, le=1000.0)
    sl_pct: float | None = Field(default=None, gt=0.0, le=100.0)
    ranking_mode: Literal["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"] = "sharpe_desc_return_desc"

    @model_validator(mode="after")
    def validate_dates(self) -> "CustomBacktestRequest":
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be later than start_date")
        return self


class RefillCustomBacktestRequest(BaseModel):
    strategy_id: str
    start_date: date
    end_date: date
    initial_capital_usdt: float = Field(default=1000.0, gt=0)
    reverse_directions: bool = False
    tp_pct: float | None = Field(default=None, gt=0.0, le=1000.0)
    sl_pct: float | None = Field(default=None, gt=0.0, le=100.0)
    ranking_mode: Literal["return_desc", "mdd_asc_return_desc", "sharpe_desc_return_desc"] = "sharpe_desc_return_desc"
    binance_api_key: str | None = None
    binance_api_secret: str | None = None

    @model_validator(mode="after")
    def validate_dates(self) -> "RefillCustomBacktestRequest":
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be later than start_date")
        return self


class CalculatorPlanRequest(BaseModel):
    strategy_id: str | None = None
    portfolio: list[PortfolioLegModel] | None = None
    total_capital_usdt: float = Field(default=100000.0, gt=0)
    long_leverage: float | None = Field(default=None, ge=1.0, le=20.0)
    short_leverage: float | None = Field(default=None, ge=1.0, le=20.0)

    @model_validator(mode="after")
    def validate_payload(self) -> "CalculatorPlanRequest":
        if self.strategy_id is None and not self.portfolio:
            raise ValueError("strategy_id or portfolio must be provided")
        return self


class CalculatorPlanRow(BaseModel):
    asset: str
    direction: Literal["long", "short"]
    weight_pct: float
    margin: float
    notional: float
    leverage: float


class CalculatorPlanResponse(BaseModel):
    total_capital_usdt: float
    total_margin_used: float
    total_long_notional: float
    total_short_notional: float
    rows: list[CalculatorPlanRow]


class HistoryTopStrategy(BaseModel):
    strategy_id: str
    rank: int
    params: StrategyParams
    annualized_return: float
    total_return: float
    sharpe: float
    max_drawdown: float
    funding_income: float
    trading_fees: float
    rehedge_count: int
    portfolio: list[PortfolioLegModel]
    equity_curve: list[list[float | int]] = Field(default_factory=list)


class HistoryRunRecord(BaseModel):
    run_id: str
    created_at: str
    meta: dict[str, Any]
    top_strategies: list[HistoryTopStrategy]
    top_strategies_by_mode: dict[str, list[HistoryTopStrategy]] = Field(default_factory=dict)


class HistoryRunsResponse(BaseModel):
    runs: list[HistoryRunRecord]


class LiveRobotPlanRow(BaseModel):
    asset: str
    direction: Literal["long", "short"]
    weight_pct: float = Field(gt=0.0, le=100.0)
    margin: float = Field(gt=0.0)
    notional: float = Field(gt=0.0)
    leverage: float = Field(ge=1.0, le=100.0)

    @field_validator("asset")
    @classmethod
    def validate_asset(cls, value: str) -> str:
        sym = str(value or "").upper().strip()
        if not sym:
            raise ValueError("asset is required")
        return sym


class LiveRobotCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    exchange: Literal["bybit", "binance"] = "bybit"
    exchange_account: str | None = Field(default=None, max_length=120)
    tp_pct: float = Field(gt=0.0, le=1000.0)
    sl_pct: float = Field(gt=0.0, le=100.0)
    poll_interval_seconds: int = Field(default=10, ge=1, le=3600)
    execution_mode: Literal["dry-run", "live"] = "dry-run"
    total_capital_usdt: float = Field(gt=0.0)
    rows: list[LiveRobotPlanRow] = Field(min_length=1, max_length=30)
    source_strategy_id: str | None = None
    api_key: str | None = None
    api_secret: str | None = None

    @model_validator(mode="after")
    def validate_credentials_pair(self) -> "LiveRobotCreateRequest":
        has_key = bool(str(self.api_key or "").strip())
        has_secret = bool(str(self.api_secret or "").strip())
        if has_key != has_secret:
            raise ValueError("api_key and api_secret must be provided together")
        return self


class LiveRobotStartRequest(BaseModel):
    api_key: str | None = None
    api_secret: str | None = None

    @model_validator(mode="after")
    def validate_credentials_pair(self) -> "LiveRobotStartRequest":
        has_key = bool(str(self.api_key or "").strip())
        has_secret = bool(str(self.api_secret or "").strip())
        if has_key != has_secret:
            raise ValueError("api_key and api_secret must be provided together")
        return self


class LiveRobotEvent(BaseModel):
    event_id: str
    timestamp: str
    level: str
    type: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class LiveRobotState(BaseModel):
    status: str
    running: bool
    started_at: str | None = None
    stopped_at: str | None = None
    base_equity: float | None = None
    current_equity: float | None = None
    pnl_pct: float | None = None
    trigger_reason: str | None = None
    last_error: str | None = None
    last_heartbeat: str | None = None
    entry_prices: dict[str, float] = Field(default_factory=dict)


class LiveRobotConfig(BaseModel):
    name: str
    exchange: Literal["bybit", "binance"]
    exchange_account: str | None = None
    tp_pct: float
    sl_pct: float
    poll_interval_seconds: int
    execution_mode: Literal["dry-run", "live"]
    credentials_mode: Literal["runtime", "env", "none"] = "none"
    total_capital_usdt: float
    rows: list[LiveRobotPlanRow]
    source_strategy_id: str | None = None


class LiveRobotRecord(BaseModel):
    robot_id: str
    created_at: str
    updated_at: str
    config: LiveRobotConfig
    state: LiveRobotState
    events: list[LiveRobotEvent] = Field(default_factory=list)


class LiveRobotListResponse(BaseModel):
    robots: list[LiveRobotRecord]


class LiveRobotEventsResponse(BaseModel):
    robot_id: str
    events: list[LiveRobotEvent]


class LiveRobotDeleteResponse(BaseModel):
    deleted: bool
    robot_id: str


class StrategyTransferPayload(BaseModel):
    strategy_id: str
    source: Literal["runtime", "history", "unknown"] = "unknown"
    rank: int | None = None
    annualized_return: float | None = None
    total_return: float | None = None
    sharpe: float | None = None
    max_drawdown: float | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    portfolio: list[PortfolioLegModel] = Field(default_factory=list)


class StrategyTransferExportRequest(BaseModel):
    strategy_id: str
    source: Literal["auto", "runtime", "history"] = "auto"
    expires_minutes: int | None = Field(default=None, ge=1, le=10080)


class StrategyTransferExportResponse(BaseModel):
    transfer_code: str
    created_at: str
    expires_at: str
    import_url: str
    strategy: StrategyTransferPayload


class StrategyTransferImportRequest(BaseModel):
    transfer_code: str = Field(min_length=4, max_length=32)
    consume: bool = True

    @field_validator("transfer_code")
    @classmethod
    def normalize_transfer_code(cls, value: str) -> str:
        return str(value or "").strip().upper()


class StrategyTransferImportResponse(BaseModel):
    transfer_code: str
    expires_at: str
    consumed_at: str | None = None
    strategy: StrategyTransferPayload
