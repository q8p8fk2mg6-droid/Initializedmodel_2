import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    val = raw.strip().lower()
    return val in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_csv(name: str, default: str) -> tuple[str, ...]:
    raw = os.getenv(name)
    text = default if raw is None or raw.strip() == "" else raw
    items = [item.strip() for item in str(text).split(",")]
    filtered = [item for item in items if item]
    return tuple(filtered)


@dataclass(frozen=True)
class Settings:
    binance_api_key: str = os.getenv("BINANCE_API_KEY", "")
    binance_api_secret: str = os.getenv("BINANCE_API_SECRET", "")
    bybit_api_key: str = os.getenv("BYBIT_API_KEY", "")
    bybit_api_secret: str = os.getenv("BYBIT_API_SECRET", "")
    bybit_base_url: str = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
    binance_futures_base_url: str = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com")
    bybit_recv_window_ms: int = _env_int("BYBIT_RECV_WINDOW_MS", 10000)
    default_binance_perp_taker_fee: float = _env_float("DEFAULT_BINANCE_PERP_TAKER_FEE", 0.0004)
    market_data_disk_cache_enabled: bool = _env_bool("MARKET_DATA_DISK_CACHE_ENABLED", True)
    market_data_cache_dir: str = os.getenv("MARKET_DATA_CACHE_DIR", ".cache/market_data")
    history_store_path: str = os.getenv("HISTORY_STORE_PATH", ".cache/history/top_runs.json")
    history_store_max_runs: int = _env_int("HISTORY_STORE_MAX_RUNS", 500)
    timeliness_history_store_path: str = os.getenv(
        "TIMELINESS_HISTORY_STORE_PATH",
        ".cache/history/timeliness_runs.json",
    )
    timeliness_history_store_max_runs: int = _env_int("TIMELINESS_HISTORY_STORE_MAX_RUNS", 300)
    live_robot_store_path: str = os.getenv("LIVE_ROBOT_STORE_PATH", ".cache/live/robots.json")
    live_robot_store_max_robots: int = _env_int("LIVE_ROBOT_STORE_MAX_ROBOTS", 200)
    live_robot_store_max_events: int = _env_int("LIVE_ROBOT_STORE_MAX_EVENTS", 1000)
    strategy_transfer_store_path: str = os.getenv("STRATEGY_TRANSFER_STORE_PATH", ".cache/live/strategy_transfer.json")
    strategy_transfer_store_max_items: int = _env_int("STRATEGY_TRANSFER_STORE_MAX_ITEMS", 1000)
    strategy_transfer_default_ttl_minutes: int = _env_int("STRATEGY_TRANSFER_DEFAULT_TTL_MINUTES", 60)
    strategy_transfer_max_ttl_minutes: int = _env_int("STRATEGY_TRANSFER_MAX_TTL_MINUTES", 1440)
    mobile_notify_enabled: bool = _env_bool("MOBILE_NOTIFY_ENABLED", False)
    mobile_notify_provider: str = os.getenv("MOBILE_NOTIFY_PROVIDER", "none")
    mobile_notify_timeout_seconds: float = _env_float("MOBILE_NOTIFY_TIMEOUT_SECONDS", 8.0)
    mobile_notify_heartbeat_minutes: int = _env_int("MOBILE_NOTIFY_HEARTBEAT_MINUTES", 15)
    mobile_notify_ntfy_base_url: str = os.getenv("MOBILE_NOTIFY_NTFY_BASE_URL", "https://ntfy.sh")
    mobile_notify_ntfy_topic: str = os.getenv("MOBILE_NOTIFY_NTFY_TOPIC", "")
    mobile_notify_ntfy_token: str = os.getenv("MOBILE_NOTIFY_NTFY_TOKEN", "")
    mobile_notify_telegram_bot_token: str = os.getenv("MOBILE_NOTIFY_TELEGRAM_BOT_TOKEN", "")
    mobile_notify_telegram_chat_id: str = os.getenv("MOBILE_NOTIFY_TELEGRAM_CHAT_ID", "")
    mobile_notify_webhook_url: str = os.getenv("MOBILE_NOTIFY_WEBHOOK_URL", "")
    mobile_notify_webhook_bearer_token: str = os.getenv("MOBILE_NOTIFY_WEBHOOK_BEARER_TOKEN", "")
    live_api_require_auth: bool = _env_bool("LIVE_API_REQUIRE_AUTH", True)
    live_api_auth_token: str = os.getenv("LIVE_API_AUTH_TOKEN", "")
    live_api_allowed_origins: tuple[str, ...] = _env_csv(
        "LIVE_API_ALLOWED_ORIGINS",
        "http://127.0.0.1,http://localhost",
    )


settings = Settings()
