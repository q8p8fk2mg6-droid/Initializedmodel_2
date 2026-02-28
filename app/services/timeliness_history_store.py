from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from threading import Lock
from typing import Any
import uuid


class TimelinessHistoryStore:
    def __init__(self, file_path: str, max_runs: int = 300) -> None:
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
    def _copy_record(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "run_id": str(record.get("run_id", "")),
            "created_at": str(record.get("created_at", "")),
            "meta": dict(record.get("meta", {})),
            "lookback_results": [
                dict(item) for item in record.get("lookback_results", []) if isinstance(item, dict)
            ],
            "request": dict(record.get("request", {})),
        }

    def add_run(
        self,
        run_meta: dict[str, Any],
        lookback_results: list[dict[str, Any]],
        request_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        record = {
            "run_id": str(uuid.uuid4()),
            "created_at": self._now_iso(),
            "meta": dict(run_meta),
            "lookback_results": [dict(item) for item in lookback_results if isinstance(item, dict)],
            "request": dict(request_snapshot),
        }
        with self._lock:
            self._runs.insert(0, record)
            if len(self._runs) > self._max_runs:
                self._runs = self._runs[: self._max_runs]
            self._flush()
        return self._copy_record(record)

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        n = max(int(limit), 1)
        with self._lock:
            return [self._copy_record(item) for item in self._runs[:n]]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        key = str(run_id or "").strip()
        if not key:
            return None
        with self._lock:
            for item in self._runs:
                if str(item.get("run_id", "")) == key:
                    return self._copy_record(item)
        return None

    def clear(self) -> None:
        with self._lock:
            self._runs = []
            self._flush()

