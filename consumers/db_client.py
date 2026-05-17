import logging
import time
from typing import Tuple, Any
from psycopg2.pool import SimpleConnectionPool
from core.models import ProcessedHeartbeat

logger = logging.getLogger(__name__)


def initialize_database_pool(
    dsn_string: str,
    max_retries: int = 5,
    backoff_seconds: float = 2.0,
) -> SimpleConnectionPool:
    """Side-effect factory preparing global thread-safe connection pooling handles."""
    max_retries = max(1, max_retries)
    backoff_seconds = max(0.0, backoff_seconds)
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            # Min 1 connection, max 10 connections pooled for asynchronous reuse
            pool = SimpleConnectionPool(1, 10, dsn=dsn_string)
            logger.info("database_pool_initialized", extra={"attempt": attempt})
            return pool
        except Exception as exc:
            last_error = exc
            logger.warning(
                "database_pool_initialization_failed",
                extra={
                    "attempt": attempt,
                    "max_retries": max_retries,
                    "backoff_seconds": backoff_seconds,
                    "error": str(exc),
                },
            )
            if attempt < max_retries:
                time.sleep(backoff_seconds)

    raise RuntimeError("Failed to initialize database pool") from last_error


def transform_record_to_sql_params(record: ProcessedHeartbeat) -> Tuple[Any, ...]:
    """
    Pure function mapping an immutable NamedTuple into a primitive data
    tuple suitable for parameterized SQL bindings. Zero side effects.
    """
    return (
        record.event_id,
        record.customer_id,
        record.heart_rate,
        record.status,
        record.recorded_at
    )


def execute_insert_transaction(db_pool: SimpleConnectionPool, record: ProcessedHeartbeat) -> None:
    """
    Side-effect isolation boundary function executing atomic commits to
    the external PostgreSQL cluster.
    """
    sql_query = """
                INSERT INTO heart_rate_logs (event_id, customer_id, heart_rate, status, recorded_at)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING; \
                """

    # Extract tuple parameters cleanly using our pure function
    params = transform_record_to_sql_params(record)

    # Borrow a network worker socket from the resource pool safely
    connection = db_pool.getconn()
    try:
        # Utilize context managers to auto-commit or roll back safely upon anomalies
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query, params)
    finally:
        # Always return the worker connection back to the reuse pool
        db_pool.putconn(connection)
