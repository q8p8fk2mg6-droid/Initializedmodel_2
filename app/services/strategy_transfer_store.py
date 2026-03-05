from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from threading import Lock
from typing import Any
import secrets


class StrategyTransferStore:
    _CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

    def __init__(
        self,
        *,
        file_path: str,
        max_items: int = 1000,
        default_ttl_minutes: int = 60,
        max_ttl_minutes: int = 1440,
    ) -> None:
        self._lock = Lock()
        self._path = Path(file_path)
        self._max_items = max(int(max_items), 1)
        self._default_ttl_minutes = max(int(default_ttl_minutes), 1)
        self._max_ttl_minutes = max(int(max_ttl_minutes), 1)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._records: list[dict[str, Any]] = []
        self._load()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _now_iso() -> str:
        return StrategyTransferStore._now().isoformat()

    @staticmethod
    def _parse_iso(raw: str | None) -> datetime | None:
        txt = str(raw or "").strip()
        if not txt:
            return None
        try:
            parsed = datetime.fromisoformat(txt)
        except Exception:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _load(self) -> None:
        if not self._path.exists():
            self._records = []
            return
        try:
            raw = self._path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            if isinstance(payload, list):
                self._records = [item for item in payload if isinstance(item, dict)]
            else:
                self._records = []
        except Exception:
            self._records = []

    def _flush(self) -> None:
        tmp = self._path.with_name(f"{self._path.name}.tmp")
        tmp.write_text(
            json.dumps(self._records, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self._path)

    @staticmethod
    def _copy_record(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "transfer_code": str(item.get("transfer_code", "")),
            "created_at": str(item.get("created_at", "")),
            "expires_at": str(item.get("expires_at", "")),
            "consumed_at": str(item.get("consumed_at", "")) or None,
            "source": dict(item.get("source", {})),
            "payload": dict(item.get("payload", {})),
        }

    def _purge_expired_locked(self) -> None:
        now = self._now()
        kept: list[dict[str, Any]] = []
        for item in self._records:
            expires_at = self._parse_iso(item.get("expires_at"))
            if expires_at is None:
                continue
            if expires_at <= now:
                continue
            kept.append(item)
        self._records = kept[: self._max_items]

    @classmethod
    def _new_code(cls, length: int = 8) -> str:
        n = max(int(length), 4)
        return "".join(secrets.choice(cls._CODE_ALPHABET) for _ in range(n))

    def create_transfer(
        self,
        *,
        payload: dict[str, Any],
        source: dict[str, Any] | None = None,
        expires_minutes: int | None = None,
    ) -> dict[str, Any]:
        req_ttl = self._default_ttl_minutes if expires_minutes is None else int(expires_minutes)
        ttl_minutes = min(max(req_ttl, 1), self._max_ttl_minutes)
        created_at = self._now()
        expires_at = created_at + timedelta(minutes=ttl_minutes)

        with self._lock:
            self._purge_expired_locked()
            used_codes = {str(item.get("transfer_code", "")).upper() for item in self._records}
            code = self._new_code(8)
            for _ in range(20):
                if code not in used_codes:
                    break
                code = self._new_code(8)

            record = {
                "transfer_code": code,
                "created_at": created_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "consumed_at": None,
                "source": dict(source or {}),
                "payload": dict(payload),
            }
            self._records.insert(0, record)
            if len(self._records) > self._max_items:
                self._records = self._records[: self._max_items]
            self._flush()
            return self._copy_record(record)

    def get_transfer(self, transfer_code: str, *, consume: bool = True) -> dict[str, Any] | None:
        code = str(transfer_code or "").strip().upper()
        if not code:
            return None

        with self._lock:
            self._purge_expired_locked()
            for item in self._records:
                if str(item.get("transfer_code", "")).upper() != code:
                    continue
                consumed_at = self._parse_iso(item.get("consumed_at"))
                if consume and consumed_at is not None:
                    return None
                out = self._copy_record(item)
                if consume:
                    item["consumed_at"] = self._now_iso()
                    out["consumed_at"] = str(item.get("consumed_at", ""))
                    self._flush()
                else:
                    self._flush()
                return out
            self._flush()
            return None
