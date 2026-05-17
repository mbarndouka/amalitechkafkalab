import logging
import time
from typing import Any, List, Mapping
from confluent_kafka import Producer
from core.config import load_settings
from core.logging import configure_logging
from core.models import serialize_heartbeat, HeartbeatEvent
from producers.generator import initialize_simulation_states, simulate_step, CustomerStates

logger = logging.getLogger(__name__)


def delivery_report_callback(err: Any, msg: Any) -> None:
    """Void logging side-effect callback function confirming message delivery."""
    if err is not None:
        metadata = {}
        if msg is not None:
            metadata = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            }
        logger.error(
            "kafka_delivery_failed",
            extra={**metadata, "error": str(err)},
        )


def initialize_producer(config: Mapping[str, Any]) -> Producer:
    """Configures and yields a functional instance wrapper over native C-Kafka."""
    return Producer({
        'bootstrap.servers': config["KAFKA_BOOTSTRAP_SERVERS"],
        'client.id': 'heartbeat-producer-fp-v1',
        'acks': config["KAFKA_PRODUCER_ACKS"],
        'enable.idempotence': config["KAFKA_PRODUCER_ENABLE_IDEMPOTENCE"],
        'retries': config["KAFKA_PRODUCER_RETRIES"],
        'delivery.timeout.ms': config["KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS"],
        'max.in.flight.requests.per.connection': 5,
        'compression.type': 'snappy',
        'linger.ms': config["KAFKA_PRODUCER_LINGER_MS"],
    })


def dispatch_events(
    producer: Producer,
    topic: str,
    events: List[HeartbeatEvent],
    buffer_backoff_seconds: float,
) -> None:
    """Functional iterative mapping function executing external broker writes."""
    for event in events:
        while True:
            try:
                producer.produce(
                    topic=topic,
                    key=event.customer_id.encode('utf-8'),
                    value=serialize_heartbeat(event),
                    on_delivery=delivery_report_callback
                )
                break
            except BufferError:
                logger.warning(
                    "producer_queue_full",
                    extra={
                        "event_id": event.event_id,
                        "customer_id": event.customer_id,
                        "backoff_seconds": buffer_backoff_seconds,
                    },
                )
                producer.poll(1.0)
                time.sleep(buffer_backoff_seconds)
        producer.poll(0)


def streaming_loop(producer: Producer, config: dict, customer_ids: List[str], baselines: CustomerStates,
                   current_states: CustomerStates) -> None:
    """
    Functional loop execution structure tracking global states cleanly
    via parameters rather than local mutation variables.
    """
    try:
        while True:
            events, current_states = simulate_step(customer_ids, baselines, current_states)
            dispatch_events(
                producer,
                config["KAFKA_TOPIC_NAME"],
                events,
                config["PRODUCER_BUFFER_RETRY_BACKOFF_SECONDS"],
            )
            logger.info(
                "heartbeat_batch_dispatched",
                extra={"topic": config["KAFKA_TOPIC_NAME"], "batch_size": len(events)},
            )
            time.sleep(config["SIMULATION_INTERVAL"])
    except KeyboardInterrupt:
        logger.info("producer_shutdown_requested")
        producer.flush(timeout=5.0)
        logger.info("producer_closed")


def main() -> None:
    """Main execution boundary mapping configs directly into functional runtimes."""
    config = load_settings()
    configure_logging(config["LOG_LEVEL"])
    producer = initialize_producer(config)
    customer_ids, baselines = initialize_simulation_states(config["TOTAL_CUSTOMERS"])

    logger.info(
        "producer_starting",
        extra={
            "topic": config["KAFKA_TOPIC_NAME"],
            "total_customers": config["TOTAL_CUSTOMERS"],
            "simulation_interval_seconds": config["SIMULATION_INTERVAL"],
        },
    )
    # Kick off processing loop passing baseline metrics as the initial starting state
    streaming_loop(producer, config, customer_ids, baselines, baselines)


if __name__ == "__main__":
    main()
