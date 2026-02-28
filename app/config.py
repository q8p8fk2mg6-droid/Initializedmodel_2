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


@dataclass(frozen=True)
class Settings:
    binance_api_key: str = os.getenv("BINANCE_API_KEY", "")
    binance_api_secret: str = os.getenv("BINANCE_API_SECRET", "")
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


settings = Settings()
