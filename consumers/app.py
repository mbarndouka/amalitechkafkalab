import logging
import time
from typing import Any, Mapping, Optional
from confluent_kafka import Consumer, KafkaError, TopicPartition
from core.config import load_settings
from core.logging import configure_logging
from core.models import deserialize_heartbeat
from consumers.validator import clean_and_process
from consumers.db_client import initialize_database_pool, execute_insert_transaction

logger = logging.getLogger(__name__)


def initialize_consumer(config: Mapping[str, Any]) -> Consumer:
    """Configures and binds an active stream listener into the local runtime layer."""
    consumer = Consumer({
        'bootstrap.servers': config["KAFKA_BOOTSTRAP_SERVERS"],
        'group.id': 'heartbeat-processor-group-v1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,

        # FIX: Force the connection client to resolve strictly using IPv4
        'broker.address.family': 'v4'
    })
    consumer.subscribe([config["KAFKA_TOPIC_NAME"]])
    return consumer


def message_metadata(raw_message: Any) -> dict[str, Any]:
    return {
        "topic": raw_message.topic(),
        "partition": raw_message.partition(),
        "offset": raw_message.offset(),
    }


def process_message_payload(raw_message: Any, db_pool) -> Optional[bool]:
    """
    Orchestration processing function handling pipeline sequencing while
    insulating pure transformation calculations from systemic stream tracking.

    Returns True when the Kafka offset is safe to commit, False when the message
    should be retried, and None when there is no offset to handle.
    """
    if raw_message is None:
        return None

    if raw_message.error():
        if raw_message.error().code() != KafkaError._PARTITION_EOF:
            logger.warning(
                "kafka_message_error",
                extra={**message_metadata(raw_message), "error": str(raw_message.error())},
            )
        return None

    # Pipeline Chain Phase: Deserialize -> Filter Nulls -> Transform Purely -> Visual Feedback
    raw_bytes = raw_message.value()
    event = deserialize_heartbeat(raw_bytes)

    if event is None:
        logger.warning("malformed_message_discarded", extra=message_metadata(raw_message))
        return True

    # Invoke pure computation transformations
    processed_record = clean_and_process(event)

    # Render clear console outputs
    log_ingestion(processed_record, raw_message)
    # Persist record to Postgres (side-effect)
    try:
        execute_insert_transaction(db_pool, processed_record)
    except Exception as e:
        logger.error(
            "database_insert_failed",
            extra={
                **message_metadata(raw_message),
                "event_id": processed_record.event_id,
                "customer_id": processed_record.customer_id,
                "status": processed_record.status,
                "error": str(e),
            },
            exc_info=True,
        )
        return False

    return True


def commit_processed_message(consumer: Consumer, raw_message: Any) -> bool:
    """Synchronously commits an offset after processing has completed."""
    try:
        consumer.store_offsets(raw_message)
        committed_offsets = consumer.commit(asynchronous=False)
        failed_offsets = [
            partition
            for partition in committed_offsets or []
            if partition.error is not None
        ]
        if failed_offsets:
            logger.error(
                "kafka_offset_commit_returned_errors",
                extra={
                    **message_metadata(raw_message),
                    "failed_offsets": failed_offsets,
                },
            )
            return False
        return True
    except Exception as e:
        logger.error(
            "kafka_offset_commit_failed",
            extra={**message_metadata(raw_message), "error": str(e)},
            exc_info=True,
        )
        return False


def retry_message_later(consumer: Consumer, raw_message: Any, delay_seconds: float = 2.0) -> None:
    """Rewinds the consumer to the failed message so it can be retried."""
    partition = TopicPartition(
        raw_message.topic(),
        raw_message.partition(),
        raw_message.offset(),
    )
    consumer.seek(partition)
    logger.warning(
        "message_scheduled_for_retry",
        extra={**message_metadata(raw_message), "delay_seconds": delay_seconds},
    )
    time.sleep(delay_seconds)


def log_ingestion(record: Any, raw_message: Any) -> None:
    """Void side-effect wrapper displaying pipeline processing states."""
    logger.info(
        "heartbeat_processed",
        extra={
            **message_metadata(raw_message),
            "event_id": record.event_id,
            "customer_id": record.customer_id,
            "heart_rate": record.heart_rate,
            "status": record.status,
            "recorded_at": record.recorded_at,
        },
    )


def continuous_polling_stream(
    consumer: Consumer,
    db_pool,
    retry_backoff_seconds: float,
) -> None:
    """
    Infinite processing loop reading streaming data inputs and updating
    runtime states entirely via deterministic function pipelines.
    """
    try:
        while True:
            try:
                # Poll network sockets for active wire messages
                msg = consumer.poll(timeout=1.0)
            except Exception as e:
                logger.error(
                    "kafka_poll_failed",
                    extra={
                        "error": str(e),
                        "backoff_seconds": retry_backoff_seconds,
                    },
                    exc_info=True,
                )
                time.sleep(retry_backoff_seconds)
                continue

            processing_result = process_message_payload(msg, db_pool)

            if processing_result is True:
                if not commit_processed_message(consumer, msg):
                    retry_message_later(consumer, msg, retry_backoff_seconds)
            elif processing_result is False:
                retry_message_later(consumer, msg, retry_backoff_seconds)

    except KeyboardInterrupt:
        logger.info("consumer_shutdown_requested")
    finally:
        consumer.close()
        logger.info("consumer_closed")


def main() -> None:
    """System bootstrap boundary layer."""
    config = load_settings(require_database=True)
    configure_logging(config["LOG_LEVEL"])
    logger.info(
        "consumer_starting",
        extra={
            "consumer_group": "heartbeat-processor-group-v1",
            "topic": config["KAFKA_TOPIC_NAME"],
        },
    )

    consumer_client = initialize_consumer(config)

    # Initialize DB pool for persistence
    db_pool = initialize_database_pool(
        config["DB_DSN"],
        max_retries=config["DB_CONNECT_RETRIES"],
        backoff_seconds=config["DB_CONNECT_BACKOFF_SECONDS"],
    )

    continuous_polling_stream(
        consumer_client,
        db_pool,
        retry_backoff_seconds=config["CONSUMER_RETRY_BACKOFF_SECONDS"],
    )


if __name__ == "__main__":
    main()
