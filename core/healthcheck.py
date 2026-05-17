import logging
import sys
from typing import Callable

import psycopg2
from confluent_kafka.admin import AdminClient

from core.config import load_settings
from core.logging import configure_logging

logger = logging.getLogger(__name__)


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
    configure_logging()
    check_name = sys.argv[1] if len(sys.argv) > 1 else ""
    check = CHECKS.get(check_name)
    if check is None:
        logger.error("unknown_healthcheck", extra={"healthcheck": check_name})
        return 2

    try:
        check()
    except Exception as exc:
        logger.error(
            "healthcheck_failed",
            extra={"healthcheck": check_name, "error": str(exc)},
            exc_info=True,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
