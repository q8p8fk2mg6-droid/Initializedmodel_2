from __future__ import annotations

from threading import Lock


class RuntimeStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._latest: dict[str, dict] = {}
        self._latest_all: list[dict] = []
        self._latest_meta: dict = {}

    def set_backtest(self, all_strategies: list[dict], meta: dict) -> None:
        with self._lock:
            self._latest = {item["strategy_id"]: item for item in all_strategies}
            self._latest_all = [dict(item) for item in all_strategies]
            self._latest_meta = dict(meta)

    def get_strategy(self, strategy_id: str) -> dict | None:
        with self._lock:
            return self._latest.get(strategy_id)

    def get_backtest_context(self) -> dict | None:
        with self._lock:
            if not self._latest_all:
                return None
            return {
                "all_strategies": [dict(item) for item in self._latest_all],
                "meta": dict(self._latest_meta),
            }


runtime_store = RuntimeStore()
