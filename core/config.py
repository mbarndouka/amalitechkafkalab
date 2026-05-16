import os
from pathlib import Path
from typing import Mapping, Any
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def load_settings() -> Mapping[str, Any]:
    """Pure function that extracts, builds, and returns immutable config maps."""
    return {
        "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "KAFKA_TOPIC_NAME": os.getenv("KAFKA_TOPIC_NAME", "customer_heartbeats"),
        "SIMULATION_INTERVAL": float(os.getenv("SIMULATION_INTERVAL_SECONDS", "1.0")),
        "TOTAL_CUSTOMERS": int(os.getenv("TOTAL_CUSTOMERS", "5"))
    }