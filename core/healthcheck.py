import sys
from typing import Callable

import psycopg2
from confluent_kafka.admin import AdminClient

from core.config import load_settings


def check_kafka() -> None:
    config = load_settings()
    client = AdminClient({"bootstrap.servers": config["KAFKA_BOOTSTRAP_SERVERS"]})
    client.list_topics(timeout=5)


def check_postgres() -> None:
    config = load_settings(require_database=True)
    with psycopg2.connect(config["DB_DSN"], connect_timeout=5):
        return


def check_consumer_dependencies() -> None:
    check_kafka()
    check_postgres()


CHECKS: dict[str, Callable[[], None]] = {
    "kafka": check_kafka,
    "postgres": check_postgres,
    "consumer": check_consumer_dependencies,
    "producer": check_kafka,
}


def main() -> int:
    check_name = sys.argv[1] if len(sys.argv) > 1 else ""
    check = CHECKS.get(check_name)
    if check is None:
        print(f"Unknown healthcheck: {check_name}", file=sys.stderr)
        return 2

    try:
        check()
    except Exception as exc:
        print(f"Healthcheck failed for {check_name}: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
