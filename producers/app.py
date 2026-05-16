import sys
import time
from typing import Any, List, Mapping
from confluent_kafka import Producer
from core.config import load_settings
from core.models import serialize_heartbeat, HeartbeatEvent
from producers.generator import initialize_simulation_states, simulate_step, CustomerStates


def delivery_report_callback(err: Any, msg: Any) -> None:
    """Void logging side-effect callback function confirming message delivery."""
    if err is not None:
        print(f"[-] Event transmission dispatch failure: {err}", file=sys.stderr)


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


def dispatch_events(producer: Producer, topic: str, events: List[HeartbeatEvent]) -> None:
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
                producer.poll(1.0)
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
            dispatch_events(producer, config["KAFKA_TOPIC_NAME"], events)
            print(f"[+] Dispatched metrics batch for {len(events)} customers successfully.")
            time.sleep(config["SIMULATION_INTERVAL"])
    except KeyboardInterrupt:
        print("\n[*] Intercepted termination signal. Initiating flush sequence...")
        producer.flush(timeout=5.0)
        print("[+] System gracefully offline.")


def main() -> None:
    """Main execution boundary mapping configs directly into functional runtimes."""
    config = load_settings()
    producer = initialize_producer(config)
    customer_ids, baselines = initialize_simulation_states(config["TOTAL_CUSTOMERS"])

    print(f"[*] Starting Functional Producer. Pipeline Destination Topic: {config['KAFKA_TOPIC_NAME']}")
    # Kick off processing loop passing baseline metrics as the initial starting state
    streaming_loop(producer, config, customer_ids, baselines, baselines)


if __name__ == "__main__":
    main()
