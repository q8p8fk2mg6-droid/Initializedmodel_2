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
