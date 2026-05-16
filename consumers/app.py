import sys
from typing import Any, Mapping
from confluent_kafka import Consumer, KafkaError
from core.config import load_settings
from core.models import deserialize_heartbeat
from consumers.validator import clean_and_process
from consumers.db_client import initialize_database_pool, execute_insert_transaction


def initialize_consumer(config: Mapping[str, Any]) -> Consumer:
    """Configures and binds an active stream listener into the local runtime layer."""
    consumer = Consumer({
        'bootstrap.servers': config["KAFKA_BOOTSTRAP_SERVERS"],
        'group.id': 'heartbeat-processor-group-v1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,

        # FIX: Force the connection client to resolve strictly using IPv4
        'broker.address.family': 'v4'
    })
    consumer.subscribe([config["KAFKA_TOPIC_NAME"]])
    return consumer


def process_message_payload(raw_message: Any, db_pool) -> None:
    """
    Orchestration processing function handling pipeline sequencing while
    insulating pure transformation calculations from systemic stream tracking.
    """
    if raw_message is None:
        return

    if raw_message.error():
        if raw_message.error().code() != KafkaError._PARTITION_EOF:
            print(f"[-] Broker tracking anomaly noticed: {raw_message.error()}", file=sys.stderr)
        return

    # Pipeline Chain Phase: Deserialize -> Filter Nulls -> Transform Purely -> Visual Feedback
    raw_bytes = raw_message.value()
    event = deserialize_heartbeat(raw_bytes)

    if event is None:
        print(f"[-] Malformed packet intercepted and discarded at edge boundary.", file=sys.stderr)
        return

    # Invoke pure computation transformations
    processed_record = clean_and_process(event)

    # Render clear console outputs
    print_ingestion_log(processed_record)
    # Persist record to Postgres (side-effect)
    try:
        execute_insert_transaction(db_pool, processed_record)
    except Exception as e:
        print(f"[-] Failed to persist record to DB: {e}", file=sys.stderr)


def print_ingestion_log(record: Any) -> None:
    """Void side-effect wrapper displaying pipeline processing states."""
    color_map = {"NORMAL": "\033[92m", "WARNING": "\033[93m", "CRITICAL": "\033[91m"}
    reset_color = "\033[0m"
    color = color_map.get(record.status, reset_color)

    print(f"[Stream Consumer] ID: {record.event_id} | Client: {record.customer_id} | "
          f"BPM: {record.heart_rate} | Status: {color}{record.status}{reset_color} | TS: {record.recorded_at}")


def continuous_polling_stream(consumer: Consumer, db_pool) -> None:
    """
    Infinite processing loop reading streaming data inputs and updating
    runtime states entirely via deterministic function pipelines.
    """
    try:
        while True:
            # Poll network sockets for active wire messages
            msg = consumer.poll(timeout=1.0)
            process_message_payload(msg, db_pool)

    except KeyboardInterrupt:
        print("\n[*] Halting continuous processing pipeline workers...")
    finally:
        consumer.close()
        print("[+] Stream handler fully offline.")


def main() -> None:
    """System bootstrap boundary layer."""
    config = load_settings(require_database=True)
    print(f"[*] Initializing Functional Ingestion Stream. Group: 'heartbeat-processor-group-v1'")

    consumer_client = initialize_consumer(config)

    # Initialize DB pool for persistence
    db_pool = initialize_database_pool(config["DB_DSN"])

    continuous_polling_stream(consumer_client, db_pool)


if __name__ == "__main__":
    main()
