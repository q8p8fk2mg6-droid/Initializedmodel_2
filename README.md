# Binance Perp Carry Optimizer

基于 `Delta_nuetral_carry_trade` 的回测与机器学习思路，扩展到 Binance 成交量 Top100 永续合约组合寻优。

## 功能
- Top100 合约中随机采样组合（3~4 个币种）
- 多/空方向、再平衡频率、杠杆范围的机器学习寻优
- 资金曲线、收益率排名、仓位计算器
- 使用 Binance 历史资金费率

## 启动
```powershell
cd D:\projects\Initializedmodel_1
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
python -m uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```
浏览器打开：`http://127.0.0.1:8001`

## 注意
- 组合越多、回测时间越长，耗时越久。
- 使用并行会提高 API 调用量，建议先用小 `max_evals` 测试。

## Stage-2 数据缓存
- 新增本地磁盘缓存：`kline` 与 `funding` 会按 `symbol + 时间区间` 缓存到本地，重复回测可直接复用。
- 默认开启，配置项：
  - `MARKET_DATA_DISK_CACHE_ENABLED=true|false`
  - `MARKET_DATA_CACHE_DIR=.cache/market_data`
- 首次运行会慢一些（需要落盘），同区间后续运行会明显提速。

## Stage-3 History Top Store
- Every completed ML backtest run now stores its Top strategies to local history.
- History API:
  - `GET /api/history/top?limit=20`
  - `POST /api/history/top/clear`
- New env settings:
  - `HISTORY_STORE_PATH=.cache/history/top_runs.json`
  - `HISTORY_STORE_MAX_RUNS=500`

## Stage-4 Live Robot (MVP)
- New frontend section: `7) 实盘机器人（MVP Dry-Run）`.
- Workflow:
  - Run position calculator first.
  - Click `从仓位计算器导入快照`.
  - Fill robot config (exchange / TP% / SL% / poll seconds / mode).
  - Create robot and control it with start/stop/close-all.
- TP/SL is portfolio-level (overall equity), not per-contract.
- Legacy note: first MVP version was `dry-run` only.
- Live robot APIs:
  - `POST /api/live/robots`
  - `GET /api/live/robots`
  - `GET /api/live/robots/{robot_id}`
  - `POST /api/live/robots/{robot_id}/start`
  - `POST /api/live/robots/{robot_id}/stop`
  - `POST /api/live/robots/{robot_id}/close-all`
  - `POST /api/live/robots/{robot_id}/status-check`
  - `DELETE /api/live/robots/{robot_id}`
  - `GET /api/live/robots/{robot_id}/events`
- New env settings:
  - `LIVE_ROBOT_STORE_PATH=.cache/live/robots.json`
  - `LIVE_ROBOT_STORE_MAX_ROBOTS=200`
  - `LIVE_ROBOT_STORE_MAX_EVENTS=1000`

## Stage-4.1 Live Update (Bybit)
- Live mode is now available for Bybit linear contracts.
- Portfolio-level TP/SL (live) is evaluated by strategy contracts PnL only:
  - `pnl_pct = unrealized_pnl_of_robot_symbols / base_capital_of_robot * 100%`
  - Spot holdings and unrelated account assets are excluded from TP/SL calculation.
  - `pnl_pct >= TP%` => close all
  - `pnl_pct <= -SL%` => close all
- Order handling in live mode:
  - Open positions by market orders from calculator snapshot.
  - Start includes margin precheck; if estimated required margin exceeds available balance, start is rejected before sending any open orders.
  - Leverage is normalized by exchange step using ceiling (example: requested `1.5`, exchange step `0.2` => applied `1.6`).
  - Close-all uses reduce-only market orders.
  - If open flow fails, engine attempts rollback by close-all.
- Credential priority:
  - Runtime request (`api_key` + `api_secret`) first.
  - Then environment variables.
- Status after interruption/restart:
  - Robot configs/events persist in store and can be viewed after reopening frontend.
  - Backend process restart does not auto-resume monitor thread.
  - Use `status-check` to probe current worker/position state.
- New env settings for live:
  - `BYBIT_API_KEY=`
  - `BYBIT_API_SECRET=`
  - `BYBIT_BASE_URL=https://api.bybit.com`
  - `BYBIT_RECV_WINDOW_MS=10000`
  - `BINANCE_FUTURES_BASE_URL=https://fapi.binance.com`

## Stage-4.2 Mobile Control + Strategy Transfer
- New mobile page:
  - `GET /mobile`
- Desktop ranking table now supports `Export to Mobile` per strategy.
- Strategy transfer APIs:
  - `POST /api/strategy-transfer/export`
  - `POST /api/strategy-transfer/import`
- Transfer store env settings:
  - `STRATEGY_TRANSFER_STORE_PATH=.cache/live/strategy_transfer.json`
  - `STRATEGY_TRANSFER_STORE_MAX_ITEMS=1000`
  - `STRATEGY_TRANSFER_DEFAULT_TTL_MINUTES=60`
  - `STRATEGY_TRANSFER_MAX_TTL_MINUTES=1440`
