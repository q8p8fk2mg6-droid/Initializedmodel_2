from __future__ import annotations

from datetime import datetime, timezone
from threading import Event, Lock, Thread
from typing import Any

from app.clients.exchange_adapter import BybitV5Client, ExchangeAdapterRegistry
from app.config import settings
from app.services.live_robot_store import LiveRobotStore
from app.services.mobile_notifier import MobileNotifier
from app.services.risk_guard import RiskGuard


class LiveRobotEngine:
    def __init__(
        self,
        *,
        store: LiveRobotStore,
        exchange_registry: ExchangeAdapterRegistry,
        notifier: MobileNotifier | None = None,
    ) -> None:
        self._store = store
        self._exchange_registry = exchange_registry
        self._notifier = notifier
        self._lock = Lock()
        self._workers: dict[str, dict[str, Any]] = {}
        self._runtime_credentials: dict[str, dict[str, str]] = {}

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _symbols_from_rows(rows: list[dict[str, Any]]) -> list[str]:
        symbols: list[str] = []
        for row in rows:
            sym = str(row.get("asset", "")).upper().strip()
            if sym:
                symbols.append(sym)
        return sorted(set(symbols))

    @staticmethod
    def _compute_equity_from_snapshot(
        *,
        rows: list[dict[str, Any]],
        entry_prices: dict[str, float],
        current_prices: dict[str, float],
        base_equity: float,
    ) -> tuple[float, float]:
        pnl = 0.0
        for row in rows:
            sym = str(row.get("asset", "")).upper().strip()
            direction = str(row.get("direction", "long")).lower().strip()
            side = 1.0 if direction == "long" else -1.0
            notional = float(row.get("notional", 0.0))
            entry = float(entry_prices.get(sym, 0.0))
            current = float(current_prices.get(sym, 0.0))
            if entry <= 0.0 or current <= 0.0 or notional <= 0.0:
                continue
            qty = side * notional / entry
            pnl += qty * (current - entry)
        current_equity = float(base_equity + pnl)
        pnl_pct = float((current_equity - base_equity) / base_equity * 100.0) if base_equity > 0 else 0.0
        return current_equity, pnl_pct

    def register_credentials(
        self,
        robot_id: str,
        *,
        exchange: str,
        api_key: str | None,
        api_secret: str | None,
    ) -> None:
        rid = str(robot_id or "").strip()
        if not rid:
            return
        key = str(api_key or "").strip()
        secret = str(api_secret or "").strip()
        with self._lock:
            if key and secret:
                self._runtime_credentials[rid] = {
                    "exchange": str(exchange or "").lower().strip(),
                    "api_key": key,
                    "api_secret": secret,
                }
            else:
                self._runtime_credentials.pop(rid, None)

    def _resolve_credentials(
        self,
        *,
        robot_id: str,
        exchange: str,
        expected_mode: str | None = None,
    ) -> tuple[str, str, str]:
        ex = str(exchange or "").lower().strip()
        rid = str(robot_id or "").strip()

        with self._lock:
            runtime = dict(self._runtime_credentials.get(rid, {}))
        if runtime and runtime.get("exchange") == ex:
            key = str(runtime.get("api_key", "")).strip()
            secret = str(runtime.get("api_secret", "")).strip()
            if key and secret:
                return key, secret, "runtime"

        if ex == "bybit":
            key = str(settings.bybit_api_key or "").strip()
            secret = str(settings.bybit_api_secret or "").strip()
        elif ex == "binance":
            key = str(settings.binance_api_key or "").strip()
            secret = str(settings.binance_api_secret or "").strip()
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")

        if key and secret:
            return key, secret, "env"
        mode_text = str(expected_mode or "").strip().lower()
        if mode_text == "runtime":
            raise ValueError(
                f"Missing runtime API credentials for {ex}. "
                "This usually happens after backend restart/reload. "
                "Please recreate the robot with API key/secret, or set env credentials."
            )
        raise ValueError(
            f"Missing API credentials for {ex}. Provide api_key/api_secret when creating robot, "
            f"or set environment variables."
        )

    def _build_live_executor(self, *, robot_id: str, exchange: str) -> tuple[BybitV5Client, str]:
        robot = self._store.get_robot(robot_id, include_events=False) or {}
        config = robot.get("config", {}) if isinstance(robot, dict) else {}
        expected_mode = str(config.get("credentials_mode", "")).strip().lower()
        api_key, api_secret, source = self._resolve_credentials(
            robot_id=robot_id,
            exchange=exchange,
            expected_mode=expected_mode,
        )
        executor = self._exchange_registry.create_live_executor(
            exchange=exchange,
            api_key=api_key,
            api_secret=api_secret,
        )
        return executor, source

    def _worker_alive(self, robot_id: str) -> bool:
        rid = str(robot_id or "").strip()
        if not rid:
            return False
        with self._lock:
            worker = self._workers.get(rid)
            if not worker:
                return False
            thread = worker.get("thread")
            return bool(thread and thread.is_alive())

    def _notify(
        self,
        robot_id: str,
        *,
        event_type: str,
        level: str,
        message: str,
        data: dict[str, Any] | None = None,
        min_interval_seconds: float = 0.0,
        dedupe_key: str | None = None,
    ) -> None:
        notifier = self._notifier
        if notifier is None:
            return
        robot = self._store.get_robot(robot_id, include_events=False)
        if robot is None:
            return
        notifier.notify_robot_event(
            robot,
            event_type=event_type,
            level=level,
            message=message,
            data=data,
            min_interval_seconds=min_interval_seconds,
            dedupe_key=dedupe_key,
        )

    def _notify_heartbeat(self, robot_id: str, *, message: str = "Robot heartbeat") -> None:
        notifier = self._notifier
        if notifier is None:
            return
        robot = self._store.get_robot(robot_id, include_events=False)
        if robot is None:
            return
        notifier.maybe_notify_heartbeat(robot, message=message)

    def start(self, robot_id: str) -> dict[str, Any]:
        rid = str(robot_id or "").strip()
        if not rid:
            raise ValueError("robot_id is required")
        robot = self._store.get_robot(rid, include_events=True)
        if robot is None:
            raise ValueError("Robot not found")

        state = robot.get("state", {}) if isinstance(robot, dict) else {}
        if bool(state.get("running")):
            with self._lock:
                existing = self._workers.get(rid)
                if existing and existing.get("thread") and existing["thread"].is_alive():
                    latest = self._store.get_robot(rid, include_events=True)
                    if latest is None:
                        raise ValueError("Robot not found")
                    return latest
                stop_event = Event()
                worker = Thread(target=self._worker_loop, args=(rid, stop_event), daemon=True)
                self._workers[rid] = {"stop_event": stop_event, "thread": worker}
                worker.start()
            self._store.append_event(
                rid,
                level="info",
                event_type="already_running",
                message="Start ignored: robot is already running; monitor loop resumed without reopening positions.",
                data={},
            )
            self._notify(
                rid,
                event_type="already_running",
                level="info",
                message="Robot start ignored because it is already running; monitor loop resumed.",
                data={},
                min_interval_seconds=60.0,
                dedupe_key=f"already_running:{rid}",
            )
            latest = self._store.get_robot(rid, include_events=True)
            if latest is None:
                raise ValueError("Robot not found")
            return latest

        running_robot = self._store.find_running_robot_id()
        if running_robot and running_robot != rid:
            raise ValueError(f"Only one robot can run at a time in MVP mode. Running robot: {running_robot}")

        config = robot.get("config", {})
        mode = str(config.get("execution_mode", "dry-run")).strip().lower()
        rows = [dict(x) for x in config.get("rows", []) if isinstance(x, dict)]
        if not rows:
            raise ValueError("Robot has empty position rows")

        exchange = str(config.get("exchange", "bybit")).lower().strip()
        symbols = self._symbols_from_rows(rows)
        if not symbols:
            raise ValueError("Robot symbols are empty")

        resume_orphan_live = (
            mode == "live"
            and str(state.get("status", "")).strip().lower() == "orphan_open_positions"
        )

        if resume_orphan_live:
            now_iso = self._now_iso()
            self._store.update_state(
                rid,
                {
                    "status": "running",
                    "running": True,
                    "stopped_at": None,
                    "trigger_reason": None,
                    "last_error": None,
                    "last_heartbeat": now_iso,
                },
            )
            self._store.append_event(
                rid,
                level="info",
                event_type="resumed_from_orphan",
                message="Monitor resumed from orphan_open_positions without reopening positions.",
                data={"exchange": exchange, "mode": "live"},
            )
            self._notify(
                rid,
                event_type="resumed_from_orphan",
                level="warn",
                message="Robot resumed from orphan_open_positions.",
                data={"exchange": exchange, "mode": "live"},
            )
        elif mode == "dry-run":
            prices = self._exchange_registry.get_latest_prices(exchange=exchange, symbols=symbols)
            missing = [sym for sym in symbols if float(prices.get(sym, 0.0)) <= 0.0]
            if missing:
                raise ValueError(f"Missing price for symbols: {', '.join(missing)}")
            base_equity = float(config.get("total_capital_usdt", 0.0))
            if base_equity <= 0.0:
                raise ValueError("total_capital_usdt must be > 0")
            current_equity, pnl_pct = self._compute_equity_from_snapshot(
                rows=rows,
                entry_prices=prices,
                current_prices=prices,
                base_equity=base_equity,
            )
            now_iso = self._now_iso()
            self._store.update_state(
                rid,
                {
                    "status": "running",
                    "running": True,
                    "started_at": now_iso,
                    "stopped_at": None,
                    "base_equity": base_equity,
                    "current_equity": current_equity,
                    "pnl_pct": pnl_pct,
                    "trigger_reason": None,
                    "last_error": None,
                    "last_heartbeat": now_iso,
                    "entry_prices": prices,
                },
            )
            self._store.append_event(
                rid,
                level="info",
                event_type="started",
                message=f"Robot started in dry-run mode on {exchange}.",
                data={"exchange": exchange, "symbols": symbols, "mode": "dry-run"},
            )
            self._notify(
                rid,
                event_type="started",
                level="info",
                message=f"Robot started in dry-run mode on {exchange}.",
                data={"exchange": exchange, "symbols": symbols, "mode": "dry-run"},
            )
        elif mode == "live":
            executor, cred_source = self._build_live_executor(robot_id=rid, exchange=exchange)
            margin_precheck = executor.precheck_open_margin(rows)
            if not bool(margin_precheck.get("ok")):
                required = float(margin_precheck.get("required_after_buffer", 0.0))
                available = float(margin_precheck.get("available_balance", 0.0))
                shortfall = float(margin_precheck.get("shortfall", 0.0))
                adjustments = margin_precheck.get("leverage_adjustments", [])
                adjust_text = ""
                if isinstance(adjustments, list) and adjustments:
                    preview = ", ".join(
                        f"{str(x.get('symbol', '-'))}:{float(x.get('requested_leverage', 0.0)):.4f}->{float(x.get('applied_leverage', 0.0)):.4f}"
                        for x in adjustments[:6]
                    )
                    if len(adjustments) > 6:
                        preview += ", ..."
                    adjust_text = f" leverage_adjustments=[{preview}]"
                raise ValueError(
                    "Live start precheck failed: estimated required margin exceeds available balance. "
                    f"required_with_buffer={required:.4f}, available={available:.4f}, shortfall={shortfall:.4f}.{adjust_text}"
                )
            try:
                open_result = executor.open_positions_from_plan(rows)
                base_equity = float(config.get("total_capital_usdt", 0.0))
                if base_equity <= 0.0:
                    raise ValueError("total_capital_usdt must be > 0 for live mode TP/SL baseline")
            except Exception as open_exc:
                rollback_extra = ""
                try:
                    rollback = executor.close_all_positions(symbols=symbols)
                    rollback_extra = f"; rollback closed {int(rollback.get('closed_count', 0))} orders"
                except Exception as rollback_exc:
                    rollback_extra = f"; rollback failed: {str(rollback_exc)}"
                raise ValueError(f"Live start failed while opening positions: {str(open_exc)}{rollback_extra}") from open_exc

            entry_prices = {
                str(k).upper().strip(): float(v)
                for k, v in dict(open_result.get("entry_prices", {})).items()
                if str(k).strip()
            }
            now_iso = self._now_iso()
            self._store.update_state(
                rid,
                {
                    "status": "running",
                    "running": True,
                    "started_at": now_iso,
                    "stopped_at": None,
                    "base_equity": base_equity,
                    "current_equity": base_equity,
                    "pnl_pct": 0.0,
                    "trigger_reason": None,
                    "last_error": None,
                    "last_heartbeat": now_iso,
                    "entry_prices": entry_prices,
                },
            )
            self._store.append_event(
                rid,
                level="info",
                event_type="started",
                message=(
                    f"Robot started in LIVE mode on {exchange}. "
                    f"Opened {len(open_result.get('orders', []))} market orders."
                ),
                data={
                    "exchange": exchange,
                    "mode": "live",
                    "credential_source": cred_source,
                    "order_count": int(len(open_result.get("orders", []))),
                    "margin_precheck": margin_precheck,
                    "leverage_adjustments": open_result.get("leverage_adjustments", []),
                    "tp_sl_basis": "portfolio_unrealized_pnl_over_robot_capital",
                },
            )
            self._notify(
                rid,
                event_type="started",
                level="info",
                message=(
                    f"Robot started in live mode on {exchange}. "
                    f"Opened {len(open_result.get('orders', []))} market orders."
                ),
                data={
                    "exchange": exchange,
                    "mode": "live",
                    "credential_source": cred_source,
                    "order_count": int(len(open_result.get("orders", []))),
                    "margin_precheck": margin_precheck,
                    "leverage_adjustments": open_result.get("leverage_adjustments", []),
                },
            )
        else:
            raise ValueError(f"Unsupported execution_mode: {mode}")

        with self._lock:
            existing = self._workers.get(rid)
            if existing and existing.get("thread") and existing["thread"].is_alive():
                latest = self._store.get_robot(rid, include_events=True)
                if latest is None:
                    raise ValueError("Robot not found after start")
                return latest
            stop_event = Event()
            worker = Thread(target=self._worker_loop, args=(rid, stop_event), daemon=True)
            self._workers[rid] = {"stop_event": stop_event, "thread": worker}
            worker.start()

        latest = self._store.get_robot(rid, include_events=True)
        if latest is None:
            raise ValueError("Robot not found after start")
        return latest

    def stop(self, robot_id: str, *, reason: str = "manual_stop") -> dict[str, Any]:
        rid = str(robot_id or "").strip()
        if not rid:
            raise ValueError("robot_id is required")

        with self._lock:
            worker = self._workers.get(rid)
            if worker and isinstance(worker.get("stop_event"), Event):
                worker["stop_event"].set()

        now_iso = self._now_iso()
        robot = self._store.get_robot(rid, include_events=True)
        if robot is None:
            raise ValueError("Robot not found")
        state = robot.get("state", {})
        if bool(state.get("running")):
            self._store.update_state(
                rid,
                {
                    "status": "stopped",
                    "running": False,
                    "stopped_at": now_iso,
                    "trigger_reason": reason,
                    "last_heartbeat": now_iso,
                },
            )
        self._store.append_event(
            rid,
            level="info",
            event_type="stopped",
            message=f"Robot stopped: {reason}.",
            data={"reason": reason},
        )
        self._notify(
            rid,
            event_type="stopped",
            level="info",
            message=f"Robot stopped: {reason}.",
            data={"reason": reason},
        )
        latest = self._store.get_robot(rid, include_events=True)
        if latest is None:
            raise ValueError("Robot not found")
        return latest

    def close_all(self, robot_id: str) -> dict[str, Any]:
        rid = str(robot_id or "").strip()
        if not rid:
            raise ValueError("robot_id is required")
        robot = self._store.get_robot(rid, include_events=True)
        if robot is None:
            raise ValueError("Robot not found")

        config = robot.get("config", {})
        mode = str(config.get("execution_mode", "dry-run")).strip().lower()
        exchange = str(config.get("exchange", "bybit")).lower().strip()
        rows = [dict(x) for x in config.get("rows", []) if isinstance(x, dict)]
        symbols = self._symbols_from_rows(rows)
        close_data: dict[str, Any] = {"mode": mode}

        if mode == "live":
            executor, _ = self._build_live_executor(robot_id=rid, exchange=exchange)
            close_data = executor.close_all_positions(symbols=symbols)
            try:
                executor.cancel_all_orders(settle_coin="USDT")
            except Exception:
                pass
            msg = f"Close-all executed in live mode: closed {int(close_data.get('closed_count', 0))} orders."
        else:
            msg = "Close-all requested (dry-run): no real orders were sent."

        robot = self.stop(rid, reason="manual_close")
        self._store.append_event(
            rid,
            level="info",
            event_type="close_all",
            message=msg,
            data=close_data,
        )
        self._notify(
            rid,
            event_type="close_all",
            level="warn",
            message=msg,
            data=close_data,
        )
        latest = self._store.get_robot(rid, include_events=True)
        return latest or robot

    def check_status(self, robot_id: str) -> dict[str, Any]:
        rid = str(robot_id or "").strip()
        if not rid:
            raise ValueError("robot_id is required")
        robot = self._store.get_robot(rid, include_events=True)
        if robot is None:
            raise ValueError("Robot not found")

        config = robot.get("config", {})
        state = robot.get("state", {})
        mode = str(config.get("execution_mode", "dry-run")).strip().lower()
        exchange = str(config.get("exchange", "bybit")).lower().strip()
        rows = [dict(x) for x in config.get("rows", []) if isinstance(x, dict)]
        symbols = self._symbols_from_rows(rows)
        base_equity = float(state.get("base_equity") or config.get("total_capital_usdt") or 0.0)
        worker_alive = self._worker_alive(rid)
        now_iso = self._now_iso()

        patch: dict[str, Any] = {
            "running": bool(worker_alive),
            "last_heartbeat": now_iso,
        }
        info: dict[str, Any] = {
            "worker_alive": bool(worker_alive),
            "mode": mode,
            "exchange": exchange,
            "probe_success": False,
        }

        if mode == "live":
            try:
                executor, _ = self._build_live_executor(robot_id=rid, exchange=exchange)
                pnl_data = executor.get_portfolio_unrealized_pnl(symbols=symbols)
                unrealized_pnl = float(pnl_data.get("unrealized_pnl", 0.0))
                position_rows = [dict(x) for x in pnl_data.get("positions", []) if isinstance(x, dict)]
                position_open = bool(position_rows)
                current_equity = float(base_equity + unrealized_pnl)
                pnl_pct = float((unrealized_pnl / base_equity) * 100.0) if base_equity > 0 else 0.0
                patch.update(
                    {
                        "current_equity": current_equity,
                        "pnl_pct": pnl_pct,
                        "last_error": None,
                    }
                )
                if worker_alive:
                    patch["status"] = "running"
                else:
                    patch["status"] = "orphan_open_positions" if position_open else "stopped"
                    if not position_open and str(state.get("trigger_reason", "")).strip() == "":
                        patch["trigger_reason"] = "status_check"
                info.update(
                    {
                        "probe_success": True,
                        "position_open": bool(position_open),
                        "open_position_count": len(position_rows),
                        "unrealized_pnl": unrealized_pnl,
                    }
                )
            except Exception as exc:
                patch["last_error"] = str(exc)
                if not worker_alive:
                    patch["status"] = "unknown"
                info["check_error"] = str(exc)
        else:
            info["probe_success"] = True
            if worker_alive:
                patch["status"] = "running"
                patch["last_error"] = None
            else:
                patch["status"] = "stopped"

        self._store.update_state(rid, patch)
        if bool(info.get("probe_success")):
            msg = (
                f"Status checked: worker_alive={str(info.get('worker_alive')).lower()}, "
                f"position_open={str(info.get('position_open', False)).lower()}."
            )
            event_level = "info"
            event_type = "status_check"
        else:
            msg = (
                f"Status checked (probe_failed): worker_alive={str(info.get('worker_alive')).lower()}, "
                f"error={str(info.get('check_error', 'unknown'))}."
            )
            event_level = "warn"
            event_type = "status_check_failed"
        self._store.append_event(
            rid,
            level=event_level,
            event_type=event_type,
            message=msg,
            data=info,
        )
        if not bool(info.get("probe_success")):
            self._notify(
                rid,
                event_type=event_type,
                level=event_level,
                message=msg,
                data=info,
                min_interval_seconds=60.0,
                dedupe_key=f"status_check_failed:{rid}",
            )
        latest = self._store.get_robot(rid, include_events=True)
        if latest is None:
            raise ValueError("Robot not found")
        return latest

    def delete(self, robot_id: str) -> dict[str, Any]:
        rid = str(robot_id or "").strip()
        if not rid:
            raise ValueError("robot_id is required")
        robot = self._store.get_robot(rid, include_events=False)
        if robot is None:
            raise ValueError("Robot not found")
        state = robot.get("state", {}) if isinstance(robot, dict) else {}
        if bool(state.get("running")) or self._worker_alive(rid):
            raise ValueError("Robot is running; stop it before deleting")
        with self._lock:
            self._runtime_credentials.pop(rid, None)
            worker = self._workers.pop(rid, None)
            if worker and isinstance(worker.get("stop_event"), Event):
                worker["stop_event"].set()
        removed = self._store.delete_robot(rid)
        if removed is None:
            raise ValueError("Robot not found")
        return removed

    def _worker_loop(self, robot_id: str, stop_event: Event) -> None:
        rid = str(robot_id or "").strip()
        consecutive_errors = 0
        try:
            while not stop_event.is_set():
                robot = self._store.get_robot(rid, include_events=True)
                if robot is None:
                    break
                config = robot.get("config", {})
                state = robot.get("state", {})
                if not bool(state.get("running")):
                    break

                mode = str(config.get("execution_mode", "dry-run")).strip().lower()
                exchange = str(config.get("exchange", "bybit")).lower().strip()
                rows = [dict(x) for x in config.get("rows", []) if isinstance(x, dict)]
                symbols = self._symbols_from_rows(rows)
                base_equity = float(state.get("base_equity") or config.get("total_capital_usdt") or 0.0)

                try:
                    if mode == "live":
                        executor, _ = self._build_live_executor(robot_id=rid, exchange=exchange)
                        pnl_data = executor.get_portfolio_unrealized_pnl(symbols=symbols)
                        unrealized_pnl = float(pnl_data.get("unrealized_pnl", 0.0))
                        current_equity = float(base_equity + unrealized_pnl)
                        pnl_pct = float((unrealized_pnl / base_equity) * 100.0) if base_equity > 0 else 0.0
                    else:
                        entry_prices = {
                            str(k).upper().strip(): float(v)
                            for k, v in dict(state.get("entry_prices", {})).items()
                            if str(k).strip()
                        }
                        current_prices = self._exchange_registry.get_latest_prices(exchange=exchange, symbols=symbols)
                        current_equity, pnl_pct = self._compute_equity_from_snapshot(
                            rows=rows,
                            entry_prices=entry_prices,
                            current_prices=current_prices,
                            base_equity=base_equity,
                        )

                    decision = RiskGuard.evaluate(
                        pnl_pct=pnl_pct,
                        tp_pct=float(config.get("tp_pct") or 0.0),
                        sl_pct=float(config.get("sl_pct") or 0.0),
                    )
                    now_iso = self._now_iso()
                    consecutive_errors = 0

                    if decision.triggered:
                        status = "triggered_tp" if decision.reason == "tp" else "triggered_sl"
                        close_data: dict[str, Any] = {}
                        if mode == "live":
                            executor, _ = self._build_live_executor(robot_id=rid, exchange=exchange)
                            close_data = executor.close_all_positions(symbols=symbols)
                            try:
                                executor.cancel_all_orders(settle_coin="USDT")
                            except Exception:
                                pass
                        self._store.update_state(
                            rid,
                            {
                                "status": status,
                                "running": False,
                                "stopped_at": now_iso,
                                "trigger_reason": decision.reason,
                                "current_equity": current_equity,
                                "pnl_pct": pnl_pct,
                                "last_heartbeat": now_iso,
                            },
                        )
                        self._store.append_event(
                            rid,
                            level="warn",
                            event_type=f"risk_{decision.reason}",
                            message=f"Risk guard triggered {decision.reason.upper()} at {pnl_pct:.4f}%.",
                            data={
                                "mode": mode,
                                "pnl_pct": pnl_pct,
                                "current_equity": current_equity,
                                "close_data": close_data,
                            },
                        )
                        self._notify(
                            rid,
                            event_type=f"risk_{decision.reason}",
                            level="warn",
                            message=f"Risk guard triggered {decision.reason.upper()} at {pnl_pct:.4f}%.",
                            data={
                                "mode": mode,
                                "pnl_pct": pnl_pct,
                                "current_equity": current_equity,
                                "close_data": close_data,
                            },
                        )
                        break

                    self._store.update_state(
                        rid,
                        {
                            "status": "running",
                            "running": True,
                            "current_equity": current_equity,
                            "pnl_pct": pnl_pct,
                            "last_error": None,
                            "last_heartbeat": now_iso,
                        },
                    )
                    self._notify_heartbeat(
                        rid,
                        message=f"Robot running. pnl_pct={pnl_pct:.4f}%, current_equity={current_equity:.4f} USDT.",
                    )
                except Exception as loop_exc:
                    consecutive_errors += 1
                    now_iso = self._now_iso()
                    if consecutive_errors >= 3:
                        self._store.update_state(
                            rid,
                            {
                                "status": "error",
                                "running": False,
                                "stopped_at": now_iso,
                                "trigger_reason": "error",
                                "last_error": str(loop_exc),
                                "last_heartbeat": now_iso,
                            },
                        )
                        self._store.append_event(
                            rid,
                            level="error",
                            event_type="worker_error",
                            message=f"Robot loop failed after retries: {str(loop_exc)}",
                            data={"retry_count": consecutive_errors},
                        )
                        self._notify(
                            rid,
                            event_type="worker_error",
                            level="error",
                            message=f"Robot loop failed after retries: {str(loop_exc)}",
                            data={"retry_count": consecutive_errors},
                        )
                        break
                    self._store.update_state(
                        rid,
                        {
                            "status": "running",
                            "running": True,
                            "last_error": str(loop_exc),
                            "last_heartbeat": now_iso,
                        },
                    )
                    self._store.append_event(
                        rid,
                        level="warn",
                        event_type="worker_retry",
                        message=f"Robot loop transient error ({consecutive_errors}/3): {str(loop_exc)}",
                        data={"retry_count": consecutive_errors},
                    )
                    self._notify(
                        rid,
                        event_type="worker_retry",
                        level="warn",
                        message=f"Robot loop transient error ({consecutive_errors}/3): {str(loop_exc)}",
                        data={"retry_count": consecutive_errors},
                        min_interval_seconds=120.0,
                        dedupe_key=f"worker_retry:{rid}",
                    )

                poll_seconds = max(int(config.get("poll_interval_seconds", 10)), 1)
                if stop_event.wait(timeout=poll_seconds):
                    break
        finally:
            with self._lock:
                self._workers.pop(rid, None)
