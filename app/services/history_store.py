from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from threading import Lock
from typing import Any
import uuid


class BacktestHistoryStore:
    _KNOWN_RANKING_MODES = (
        "sharpe_desc_return_desc",
        "return_desc",
        "mdd_asc_return_desc",
    )

    def __init__(self, file_path: str, max_runs: int = 500) -> None:
        self._lock = Lock()
        self._path = Path(file_path)
        self._max_runs = max(int(max_runs), 1)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._runs: list[dict[str, Any]] = []
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            self._runs = []
            return
        try:
            raw = self._path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            if isinstance(payload, list):
                self._runs = [item for item in payload if isinstance(item, dict)]
            else:
                self._runs = []
        except Exception:
            self._runs = []

    def _flush(self) -> None:
        tmp = self._path.with_name(f"{self._path.name}.tmp")
        tmp.write_text(
            json.dumps(self._runs, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self._path)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _copy_record(record: dict[str, Any], include_curves: bool) -> dict[str, Any]:
        def copy_list(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for item in raw:
                strategy = dict(item)
                if not include_curves:
                    strategy.pop("equity_curve", None)
                out.append(strategy)
            return out

        top_by_mode: dict[str, list[dict[str, Any]]] = {}
        raw_by_mode = record.get("top_strategies_by_mode")
        if isinstance(raw_by_mode, dict):
            for mode, items in raw_by_mode.items():
                if not isinstance(mode, str) or not isinstance(items, list):
                    continue
                top_by_mode[mode] = copy_list([x for x in items if isinstance(x, dict)])

        copied_strategies = copy_list([x for x in record.get("top_strategies", []) if isinstance(x, dict)])
        if not copied_strategies:
            copied_strategies = top_by_mode.get("sharpe_desc_return_desc", [])
        if not top_by_mode and copied_strategies:
            top_by_mode = {
                mode: copy_list(copied_strategies)
                for mode in BacktestHistoryStore._KNOWN_RANKING_MODES
            }
        return {
            "run_id": str(record.get("run_id", "")),
            "created_at": str(record.get("created_at", "")),
            "meta": dict(record.get("meta", {})),
            "top_strategies": copied_strategies,
            "top_strategies_by_mode": top_by_mode,
        }

    def add_run(
        self,
        run_meta: dict[str, Any],
        top_strategies: list[dict[str, Any]],
        top_strategies_by_mode: dict[str, list[dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        normalized_by_mode: dict[str, list[dict[str, Any]]] = {}
        if isinstance(top_strategies_by_mode, dict):
            for mode in self._KNOWN_RANKING_MODES:
                items = top_strategies_by_mode.get(mode)
                if not isinstance(items, list):
                    continue
                normalized_by_mode[mode] = [dict(item) for item in items if isinstance(item, dict)]
        record = {
            "run_id": str(uuid.uuid4()),
            "created_at": self._now_iso(),
            "meta": dict(run_meta),
            "top_strategies": [dict(item) for item in top_strategies],
            "top_strategies_by_mode": normalized_by_mode,
        }
        with self._lock:
            self._runs.insert(0, record)
            if len(self._runs) > self._max_runs:
                self._runs = self._runs[: self._max_runs]
            self._flush()
        return self._copy_record(record, include_curves=True)

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        n = max(int(limit), 1)
        with self._lock:
            return [self._copy_record(item, include_curves=False) for item in self._runs[:n]]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        key = str(run_id or "").strip()
        if not key:
            return None
        with self._lock:
            for item in self._runs:
                if str(item.get("run_id", "")) == key:
                    return self._copy_record(item, include_curves=True)
        return None

    def find_strategy(self, strategy_id: str) -> dict[str, Any] | None:
        key = str(strategy_id or "").strip()
        if not key:
            return None
        with self._lock:
            for run in self._runs:
                top = run.get("top_strategies", [])
                if isinstance(top, list):
                    for item in top:
                        if not isinstance(item, dict):
                            continue
                        if str(item.get("strategy_id", "")) == key:
                            return dict(item)

                by_mode = run.get("top_strategies_by_mode", {})
                if not isinstance(by_mode, dict):
                    continue
                for items in by_mode.values():
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        if str(item.get("strategy_id", "")) == key:
                            return dict(item)
        return None

    def clear(self) -> None:
        with self._lock:
            self._runs = []
            self._flush()
