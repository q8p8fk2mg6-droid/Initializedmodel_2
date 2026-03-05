from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any
import time

import requests


@dataclass(frozen=True)
class MobileNotifierConfig:
    enabled: bool = False
    provider: str = "none"
    timeout_seconds: float = 8.0
    heartbeat_minutes: int = 15

    ntfy_base_url: str = "https://ntfy.sh"
    ntfy_topic: str = ""
    ntfy_token: str = ""

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    webhook_url: str = ""
    webhook_bearer_token: str = ""


class MobileNotifier:
    def __init__(self, config: MobileNotifierConfig) -> None:
        provider = str(config.provider or "none").strip().lower()
        self._provider = provider
        self._enabled = bool(config.enabled) and provider in {"ntfy", "telegram", "webhook"}
        self._timeout_seconds = max(float(config.timeout_seconds), 1.0)
        self._heartbeat_seconds = max(int(config.heartbeat_minutes), 0) * 60

        self._ntfy_base_url = str(config.ntfy_base_url or "https://ntfy.sh").strip().rstrip("/")
        self._ntfy_topic = str(config.ntfy_topic or "").strip().strip("/")
        self._ntfy_token = str(config.ntfy_token or "").strip()

        self._telegram_bot_token = str(config.telegram_bot_token or "").strip()
        self._telegram_chat_id = str(config.telegram_chat_id or "").strip()

        self._webhook_url = str(config.webhook_url or "").strip()
        self._webhook_bearer_token = str(config.webhook_bearer_token or "").strip()

        self._lock = Lock()
        self._last_sent: dict[str, float] = {}

    @property
    def enabled(self) -> bool:
        if not self._enabled:
            return False
        if self._provider == "ntfy":
            return bool(self._ntfy_topic)
        if self._provider == "telegram":
            return bool(self._telegram_bot_token and self._telegram_chat_id)
        if self._provider == "webhook":
            return bool(self._webhook_url)
        return False

    @property
    def heartbeat_seconds(self) -> int:
        return int(self._heartbeat_seconds)

    def _allow_send(self, key: str, min_interval_seconds: float) -> bool:
        interval = max(float(min_interval_seconds), 0.0)
        if interval <= 0.0:
            return True
        now = time.time()
        with self._lock:
            last = float(self._last_sent.get(key, 0.0))
            if now - last < interval:
                return False
            self._last_sent[key] = now
        return True

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    def _build_robot_summary(self, robot: dict[str, Any]) -> dict[str, Any]:
        config = dict(robot.get("config", {})) if isinstance(robot, dict) else {}
        state = dict(robot.get("state", {})) if isinstance(robot, dict) else {}
        robot_id = str(robot.get("robot_id", "")).strip()
        robot_name = str(config.get("name", "")).strip() or robot_id
        return {
            "robot_id": robot_id,
            "robot_name": robot_name,
            "exchange": str(config.get("exchange", "")).strip(),
            "execution_mode": str(config.get("execution_mode", "")).strip(),
            "status": str(state.get("status", "")).strip(),
            "running": bool(state.get("running")),
            "tp_pct": self._safe_float(config.get("tp_pct")),
            "sl_pct": self._safe_float(config.get("sl_pct")),
            "base_equity": self._safe_float(state.get("base_equity")),
            "current_equity": self._safe_float(state.get("current_equity")),
            "pnl_pct": self._safe_float(state.get("pnl_pct")),
            "trigger_reason": str(state.get("trigger_reason", "")).strip(),
            "last_error": str(state.get("last_error", "")).strip(),
        }

    @staticmethod
    def _priority_from_level(level: str) -> int:
        lv = str(level or "info").strip().lower()
        if lv == "error":
            return 5
        if lv == "warn":
            return 4
        return 3

    def notify_robot_event(
        self,
        robot: dict[str, Any],
        *,
        event_type: str,
        level: str,
        message: str,
        data: dict[str, Any] | None = None,
        min_interval_seconds: float = 0.0,
        dedupe_key: str | None = None,
    ) -> None:
        if not self.enabled:
            return

        summary = self._build_robot_summary(robot)
        robot_id = str(summary.get("robot_id", "")).strip()
        if not robot_id:
            return

        key = str(dedupe_key or f"{robot_id}:{event_type}:{level}")
        if not self._allow_send(key, min_interval_seconds=min_interval_seconds):
            return

        pnl_pct = summary.get("pnl_pct")
        current_equity = summary.get("current_equity")
        base_equity = summary.get("base_equity")
        tp_pct = summary.get("tp_pct")
        sl_pct = summary.get("sl_pct")

        title = f"[{str(level).upper()}] {summary.get('robot_name')} ({event_type})"
        lines = [
            str(message or "").strip(),
            f"robot_id: {robot_id}",
            f"status: {summary.get('status')} | running={str(summary.get('running')).lower()}",
            f"exchange: {summary.get('exchange')} | mode: {summary.get('execution_mode')}",
        ]
        if base_equity is not None:
            lines.append(f"base_equity: {base_equity:.4f} USDT")
        if current_equity is not None:
            lines.append(f"current_equity: {current_equity:.4f} USDT")
        if pnl_pct is not None:
            lines.append(f"pnl_pct: {pnl_pct:.4f}%")
        if tp_pct is not None or sl_pct is not None:
            tp_text = "n/a" if tp_pct is None else f"{tp_pct:.4f}%"
            sl_text = "n/a" if sl_pct is None else f"{sl_pct:.4f}%"
            lines.append(f"tp/sl: +{tp_text} / -{sl_text}")
        if summary.get("trigger_reason"):
            lines.append(f"trigger_reason: {summary.get('trigger_reason')}")
        if summary.get("last_error"):
            lines.append(f"last_error: {summary.get('last_error')}")

        payload = {
            "event_type": str(event_type or "").strip(),
            "level": str(level or "info").strip().lower(),
            "message": str(message or "").strip(),
            "robot": summary,
            "data": dict(data or {}),
        }
        body = "\n".join([line for line in lines if line])
        priority = self._priority_from_level(level)
        try:
            self._dispatch(title=title, body=body, priority=priority, payload=payload)
        except Exception:
            # Notifier failures must not break trading logic.
            return

    def maybe_notify_heartbeat(
        self,
        robot: dict[str, Any],
        *,
        message: str = "Robot heartbeat",
    ) -> None:
        if self._heartbeat_seconds <= 0:
            return
        summary = self._build_robot_summary(robot)
        robot_id = str(summary.get("robot_id", "")).strip()
        if not robot_id:
            return
        self.notify_robot_event(
            robot,
            event_type="heartbeat",
            level="info",
            message=message,
            data={},
            min_interval_seconds=float(self._heartbeat_seconds),
            dedupe_key=f"heartbeat:{robot_id}",
        )

    def _dispatch(self, *, title: str, body: str, priority: int, payload: dict[str, Any]) -> None:
        if self._provider == "ntfy":
            self._dispatch_ntfy(title=title, body=body, priority=priority)
            return
        if self._provider == "telegram":
            self._dispatch_telegram(title=title, body=body)
            return
        if self._provider == "webhook":
            self._dispatch_webhook(title=title, body=body, payload=payload)
            return
        raise ValueError(f"Unsupported notifier provider: {self._provider}")

    def _dispatch_ntfy(self, *, title: str, body: str, priority: int) -> None:
        if not self._ntfy_topic:
            return
        url = f"{self._ntfy_base_url}/{self._ntfy_topic}"
        headers = {
            "Title": title[:120],
            "Priority": str(max(min(int(priority), 5), 1)),
            "Content-Type": "text/plain; charset=utf-8",
        }
        if self._ntfy_token:
            headers["Authorization"] = f"Bearer {self._ntfy_token}"
        response = requests.post(
            url,
            data=body.encode("utf-8"),
            headers=headers,
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()

    def _dispatch_telegram(self, *, title: str, body: str) -> None:
        if not self._telegram_bot_token or not self._telegram_chat_id:
            return
        url = f"https://api.telegram.org/bot{self._telegram_bot_token}/sendMessage"
        text = f"{title}\n{body}"
        response = requests.post(
            url,
            json={
                "chat_id": self._telegram_chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()

    def _dispatch_webhook(self, *, title: str, body: str, payload: dict[str, Any]) -> None:
        if not self._webhook_url:
            return
        headers = {"Content-Type": "application/json"}
        if self._webhook_bearer_token:
            headers["Authorization"] = f"Bearer {self._webhook_bearer_token}"
        response = requests.post(
            self._webhook_url,
            json={
                "title": title,
                "message": body,
                "payload": payload,
            },
            headers=headers,
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
