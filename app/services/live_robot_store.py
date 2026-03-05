from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from threading import Lock
from typing import Any
import uuid


class LiveRobotStore:
    def __init__(self, file_path: str, max_robots: int = 200, max_events: int = 1000) -> None:
        self._lock = Lock()
        self._path = Path(file_path)
        self._max_robots = max(int(max_robots), 1)
        self._max_events = max(int(max_events), 10)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._robots: list[dict[str, Any]] = []
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            self._robots = []
            return
        try:
            raw = self._path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            if isinstance(payload, list):
                self._robots = [item for item in payload if isinstance(item, dict)]
            else:
                self._robots = []
        except Exception:
            self._robots = []

    def _flush(self) -> None:
        tmp = self._path.with_name(f"{self._path.name}.tmp")
        tmp.write_text(
            json.dumps(self._robots, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self._path)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _copy_robot(item: dict[str, Any], include_events: bool = True) -> dict[str, Any]:
        out = {
            "robot_id": str(item.get("robot_id", "")),
            "created_at": str(item.get("created_at", "")),
            "updated_at": str(item.get("updated_at", "")),
            "config": dict(item.get("config", {})),
            "state": dict(item.get("state", {})),
        }
        if include_events:
            out["events"] = [dict(ev) for ev in item.get("events", []) if isinstance(ev, dict)]
        return out

    @staticmethod
    def _find_robot_mutable(robots: list[dict[str, Any]], robot_id: str) -> dict[str, Any] | None:
        key = str(robot_id or "").strip()
        if not key:
            return None
        for item in robots:
            if str(item.get("robot_id", "")) == key:
                return item
        return None

    def create_robot(self, *, config: dict[str, Any]) -> dict[str, Any]:
        now = self._now_iso()
        robot = {
            "robot_id": str(uuid.uuid4()),
            "created_at": now,
            "updated_at": now,
            "config": dict(config),
            "state": {
                "status": "created",
                "running": False,
                "started_at": None,
                "stopped_at": None,
                "base_equity": None,
                "current_equity": None,
                "pnl_pct": None,
                "trigger_reason": None,
                "last_error": None,
                "last_heartbeat": None,
                "entry_prices": {},
            },
            "events": [],
        }
        with self._lock:
            self._robots.insert(0, robot)
            if len(self._robots) > self._max_robots:
                self._robots = self._robots[: self._max_robots]
            self._flush()
            return self._copy_robot(robot, include_events=True)

    def list_robots(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._copy_robot(item, include_events=False) for item in self._robots]

    def get_robot(self, robot_id: str, *, include_events: bool = True) -> dict[str, Any] | None:
        with self._lock:
            item = self._find_robot_mutable(self._robots, robot_id)
            if item is None:
                return None
            return self._copy_robot(item, include_events=include_events)

    def update_state(self, robot_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            item = self._find_robot_mutable(self._robots, robot_id)
            if item is None:
                return None
            state = item.setdefault("state", {})
            state.update(dict(patch))
            item["updated_at"] = self._now_iso()
            self._flush()
            return self._copy_robot(item, include_events=True)

    def update_config(self, robot_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            item = self._find_robot_mutable(self._robots, robot_id)
            if item is None:
                return None
            config = item.setdefault("config", {})
            config.update(dict(patch))
            item["updated_at"] = self._now_iso()
            self._flush()
            return self._copy_robot(item, include_events=True)

    def append_event(
        self,
        robot_id: str,
        *,
        level: str,
        event_type: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        ev = {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._now_iso(),
            "level": str(level or "info"),
            "type": str(event_type or "event"),
            "message": str(message or ""),
            "data": dict(data or {}),
        }
        with self._lock:
            item = self._find_robot_mutable(self._robots, robot_id)
            if item is None:
                return None
            events = item.setdefault("events", [])
            events.insert(0, ev)
            if len(events) > self._max_events:
                del events[self._max_events :]
            item["updated_at"] = self._now_iso()
            self._flush()
            return dict(ev)

    def get_events(self, robot_id: str, *, limit: int = 200) -> list[dict[str, Any]] | None:
        n = max(int(limit), 1)
        with self._lock:
            item = self._find_robot_mutable(self._robots, robot_id)
            if item is None:
                return None
            events = [dict(ev) for ev in item.get("events", []) if isinstance(ev, dict)]
            return events[:n]

    def find_running_robot_id(self) -> str | None:
        with self._lock:
            for item in self._robots:
                state = item.get("state", {})
                if bool(state.get("running")):
                    return str(item.get("robot_id", ""))
        return None

    def delete_robot(self, robot_id: str) -> dict[str, Any] | None:
        key = str(robot_id or "").strip()
        if not key:
            return None
        with self._lock:
            idx = -1
            for i, item in enumerate(self._robots):
                if str(item.get("robot_id", "")) == key:
                    idx = i
                    break
            if idx < 0:
                return None
            removed = self._robots.pop(idx)
            self._flush()
            return self._copy_robot(removed, include_events=True)
