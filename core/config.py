import os
from pathlib import Path
from typing import Mapping, Any
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


class ConfigError(RuntimeError):
    """Raised when a required runtime setting is missing or invalid."""


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def optional_float(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        return float(raw_value)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be a number") from exc


def optional_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be an integer") from exc


def optional_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise ConfigError(f"Environment variable {name} must be true or false")


def load_settings(require_database: bool = False) -> Mapping[str, Any]:
    """Extract and validate runtime settings from environment variables."""
    settings = {
        "KAFKA_BOOTSTRAP_SERVERS": require_env("KAFKA_BOOTSTRAP_SERVERS"),
        "KAFKA_TOPIC_NAME": require_env("KAFKA_TOPIC_NAME"),
        "SIMULATION_INTERVAL": optional_float("SIMULATION_INTERVAL_SECONDS", 1.0),
        "TOTAL_CUSTOMERS": optional_int("TOTAL_CUSTOMERS", 5),
        "KAFKA_PRODUCER_ACKS": os.getenv("KAFKA_PRODUCER_ACKS", "all"),
        "KAFKA_PRODUCER_ENABLE_IDEMPOTENCE": optional_bool(
            "KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", True
        ),
        "KAFKA_PRODUCER_RETRIES": optional_int("KAFKA_PRODUCER_RETRIES", 10),
        "KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS": optional_int(
            "KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS", 120000
        ),
        "KAFKA_PRODUCER_LINGER_MS": optional_int("KAFKA_PRODUCER_LINGER_MS", 20),
    }

    if require_database:
        settings["DB_DSN"] = (
            f"dbname={require_env('DB_NAME')} "
            f"user={require_env('DB_USER')} "
            f"password={require_env('DB_PASSWORD')} "
            f"host={require_env('DB_HOST')} "
            f"port={require_env('DB_PORT')}"
        )

    return settings
