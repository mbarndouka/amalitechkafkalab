from typing import Tuple, Any
from psycopg2.pool import SimpleConnectionPool
from core.models import ProcessedHeartbeat


def initialize_database_pool(dsn_string: str) -> SimpleConnectionPool:
    """Side-effect factory preparing global thread-safe connection pooling handles."""
    # Min 1 connection, max 10 connections pooled for asynchronous reuse
    return SimpleConnectionPool(1, 10, dsn=dsn_string)


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
    except Exception as db_error:
        print(f"[-] Database persistence execution write crash: {db_error}")
    finally:
        # Always return the worker connection back to the reuse pool
        db_pool.putconn(connection)