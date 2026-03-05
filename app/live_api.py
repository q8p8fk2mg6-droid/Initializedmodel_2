from __future__ import annotations

import hmac
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.clients.exchange_adapter import ExchangeAdapterRegistry
from app.config import settings
from app.schemas import (
    LiveRobotCreateRequest,
    LiveRobotDeleteResponse,
    LiveRobotEventsResponse,
    LiveRobotListResponse,
    LiveRobotRecord,
    LiveRobotStartRequest,
)
from app.services.live_robot_engine import LiveRobotEngine
from app.services.live_robot_store import LiveRobotStore
from app.services.mobile_notifier import MobileNotifier, MobileNotifierConfig


def _build_allowed_origins() -> list[str]:
    values = [str(item).strip() for item in settings.live_api_allowed_origins if str(item).strip()]
    if not values:
        values = ["http://127.0.0.1", "http://localhost"]
    if "*" in values:
        raise RuntimeError("LIVE_API_ALLOWED_ORIGINS must not include '*'.")
    return values


def _extract_bearer_token(authorization: str | None) -> str:
    raw = str(authorization or "").strip()
    if not raw:
        return ""
    parts = raw.split(" ", 1)
    if len(parts) != 2:
        return ""
    if str(parts[0]).strip().lower() != "bearer":
        return ""
    return str(parts[1]).strip()


def require_token_auth(
    authorization: str | None = Header(default=None),
    x_live_token: str | None = Header(default=None, alias="X-Live-Token"),
    x_api_token: str | None = Header(default=None, alias="X-API-Token"),
) -> None:
    if not settings.live_api_require_auth:
        return

    expected = str(settings.live_api_auth_token or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="LIVE_API_AUTH_TOKEN is empty.")

    provided = ""
    for candidate in (
        _extract_bearer_token(authorization),
        str(x_live_token or "").strip(),
        str(x_api_token or "").strip(),
    ):
        if candidate:
            provided = candidate
            break

    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )


if settings.live_api_require_auth and not str(settings.live_api_auth_token or "").strip():
    raise RuntimeError("LIVE_API_REQUIRE_AUTH=true but LIVE_API_AUTH_TOKEN is empty.")


app = FastAPI(title="Carry Optimizer Live API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_build_allowed_origins(),
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Live-Token", "X-API-Token"],
)

live_robot_store = LiveRobotStore(
    file_path=settings.live_robot_store_path,
    max_robots=settings.live_robot_store_max_robots,
    max_events=settings.live_robot_store_max_events,
)
exchange_adapter_registry = ExchangeAdapterRegistry(
    bybit_base_url=settings.bybit_base_url,
    binance_futures_base_url=settings.binance_futures_base_url,
    bybit_recv_window_ms=settings.bybit_recv_window_ms,
)
mobile_notifier = MobileNotifier(
    MobileNotifierConfig(
        enabled=settings.mobile_notify_enabled,
        provider=settings.mobile_notify_provider,
        timeout_seconds=settings.mobile_notify_timeout_seconds,
        heartbeat_minutes=settings.mobile_notify_heartbeat_minutes,
        ntfy_base_url=settings.mobile_notify_ntfy_base_url,
        ntfy_topic=settings.mobile_notify_ntfy_topic,
        ntfy_token=settings.mobile_notify_ntfy_token,
        telegram_bot_token=settings.mobile_notify_telegram_bot_token,
        telegram_chat_id=settings.mobile_notify_telegram_chat_id,
        webhook_url=settings.mobile_notify_webhook_url,
        webhook_bearer_token=settings.mobile_notify_webhook_bearer_token,
    )
)
live_robot_engine = LiveRobotEngine(
    store=live_robot_store,
    exchange_registry=exchange_adapter_registry,
    notifier=mobile_notifier,
)

router = APIRouter(prefix="/api/live", dependencies=[Depends(require_token_auth)])


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


@router.get("/health")
def health() -> dict[str, Any]:
    running_robot_id = live_robot_store.find_running_robot_id()
    return {
        "ok": True,
        "running_robot_id": running_robot_id,
        "robot_count": len(live_robot_store.list_robots()),
        "notify_enabled": bool(mobile_notifier.enabled),
    }


@router.post("/robots", response_model=LiveRobotRecord)
def create_live_robot(req: LiveRobotCreateRequest) -> LiveRobotRecord:
    req_exchange = str(req.exchange).lower().strip()
    req_mode = str(req.execution_mode).lower().strip()
    if req_mode == "live" and req_exchange == "binance":
        raise HTTPException(
            status_code=400,
            detail="Live execution for Binance is not implemented yet. Please use Bybit or dry-run.",
        )

    has_runtime_creds = bool(str(req.api_key or "").strip()) and bool(str(req.api_secret or "").strip())
    has_env_creds = False
    if req_exchange == "bybit":
        has_env_creds = bool(str(settings.bybit_api_key or "").strip()) and bool(str(settings.bybit_api_secret or "").strip())
    elif req_exchange == "binance":
        has_env_creds = bool(str(settings.binance_api_key or "").strip()) and bool(str(settings.binance_api_secret or "").strip())
    credentials_mode = "runtime" if has_runtime_creds else ("env" if has_env_creds else "none")

    if req_mode == "live" and credentials_mode == "none":
        raise HTTPException(
            status_code=400,
            detail="Live mode requires API credentials. Provide api_key/api_secret or set env credentials.",
        )

    config = {
        "name": req.name.strip(),
        "exchange": req.exchange,
        "exchange_account": req.exchange_account.strip() if isinstance(req.exchange_account, str) else None,
        "tp_pct": float(req.tp_pct),
        "sl_pct": float(req.sl_pct),
        "poll_interval_seconds": int(req.poll_interval_seconds),
        "execution_mode": req.execution_mode,
        "credentials_mode": credentials_mode,
        "total_capital_usdt": float(req.total_capital_usdt),
        "rows": [row.model_dump() for row in req.rows],
        "source_strategy_id": req.source_strategy_id,
    }
    record = live_robot_store.create_robot(config=config)
    rid = str(record.get("robot_id", "")).strip()
    live_robot_engine.register_credentials(
        rid,
        exchange=req.exchange,
        api_key=req.api_key,
        api_secret=req.api_secret,
    )
    live_robot_store.append_event(
        rid,
        level="info",
        event_type="created",
        message="Robot config created.",
        data={
            "exchange": req.exchange,
            "mode": req.execution_mode,
            "credentials_mode": credentials_mode,
            "row_count": len(req.rows),
        },
    )
    latest = live_robot_store.get_robot(rid, include_events=True) or record
    return LiveRobotRecord(**latest)


@router.get("/robots", response_model=LiveRobotListResponse)
def list_live_robots() -> LiveRobotListResponse:
    robots = [LiveRobotRecord(**item) for item in live_robot_store.list_robots()]
    return LiveRobotListResponse(robots=robots)


@router.get("/robots/{robot_id}", response_model=LiveRobotRecord)
def get_live_robot(robot_id: str) -> LiveRobotRecord:
    record = live_robot_store.get_robot(robot_id, include_events=True)
    if record is None:
        raise HTTPException(status_code=404, detail="Live robot not found")
    return LiveRobotRecord(**record)


@router.post("/robots/{robot_id}/start", response_model=LiveRobotRecord)
def start_live_robot(robot_id: str, req: LiveRobotStartRequest | None = None) -> LiveRobotRecord:
    record_before_start = live_robot_store.get_robot(robot_id, include_events=False)
    if record_before_start is not None and req is not None:
        key = str(req.api_key or "").strip()
        secret = str(req.api_secret or "").strip()
        if key and secret:
            cfg = record_before_start.get("config", {}) if isinstance(record_before_start, dict) else {}
            exchange = str(cfg.get("exchange", "bybit")).lower().strip() or "bybit"
            live_robot_engine.register_credentials(
                robot_id,
                exchange=exchange,
                api_key=key,
                api_secret=secret,
            )
    try:
        record = live_robot_engine.start(robot_id)
        return LiveRobotRecord(**record)
    except ValueError as exc:
        detail = str(exc)
        if live_robot_store.get_robot(robot_id, include_events=False) is not None:
            live_robot_store.update_state(
                robot_id,
                {
                    "status": "error",
                    "running": False,
                    "trigger_reason": "start_failed",
                    "last_error": detail,
                },
            )
            live_robot_store.append_event(
                robot_id,
                level="error",
                event_type="start_failed",
                message=detail,
                data={},
            )
            latest = live_robot_store.get_robot(robot_id, include_events=False)
            if latest is not None:
                mobile_notifier.notify_robot_event(
                    latest,
                    event_type="start_failed",
                    level="error",
                    message=detail,
                    data={},
                )
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@router.post("/robots/{robot_id}/stop", response_model=LiveRobotRecord)
def stop_live_robot(robot_id: str) -> LiveRobotRecord:
    try:
        record = live_robot_engine.stop(robot_id, reason="manual_stop")
        return LiveRobotRecord(**record)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@router.post("/robots/{robot_id}/close-all", response_model=LiveRobotRecord)
def close_all_live_robot(robot_id: str) -> LiveRobotRecord:
    try:
        record = live_robot_engine.close_all(robot_id)
        return LiveRobotRecord(**record)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@router.post("/robots/{robot_id}/status-check", response_model=LiveRobotRecord)
def check_live_robot_status(robot_id: str, req: LiveRobotStartRequest | None = None) -> LiveRobotRecord:
    record_before_check = live_robot_store.get_robot(robot_id, include_events=False)
    if record_before_check is not None and req is not None:
        key = str(req.api_key or "").strip()
        secret = str(req.api_secret or "").strip()
        if key and secret:
            cfg = record_before_check.get("config", {}) if isinstance(record_before_check, dict) else {}
            exchange = str(cfg.get("exchange", "bybit")).lower().strip() or "bybit"
            live_robot_engine.register_credentials(
                robot_id,
                exchange=exchange,
                api_key=key,
                api_secret=secret,
            )
    try:
        record = live_robot_engine.check_status(robot_id)
        return LiveRobotRecord(**record)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@router.delete("/robots/{robot_id}", response_model=LiveRobotDeleteResponse)
def delete_live_robot(robot_id: str) -> LiveRobotDeleteResponse:
    try:
        removed = live_robot_engine.delete(robot_id)
        rid = str(removed.get("robot_id", "")).strip() or str(robot_id).strip()
        return LiveRobotDeleteResponse(deleted=True, robot_id=rid)
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail) from exc


@router.get("/robots/{robot_id}/events", response_model=LiveRobotEventsResponse)
def get_live_robot_events(robot_id: str, limit: int = 200) -> LiveRobotEventsResponse:
    events = live_robot_store.get_events(robot_id, limit=limit)
    if events is None:
        raise HTTPException(status_code=404, detail="Live robot not found")
    return LiveRobotEventsResponse(robot_id=robot_id, events=events)


app.include_router(router)
