import sys
import time
import signal
from typing import List, Any
from confluent_kafka import Producer
from core.config import load_settings
from core.models import serialize_heartbeat, HeartbeatEvent
from producers.generator import initialize_simulation_states, simulate_step, CustomerStates

def delivery_report_callback(err: Any, msg: Any) -> None:
    """Void logging side-effect callback function confirming message delivery."""
    if err is not None:
        print(f"[-] Event transmission dispatch failure: {err}", file=sys.stderr)


def initialize_producer(bootstrap_servers: str) -> Producer:
    """Configures and yields a functional instance wrapper over native C-Kafka."""
    return Producer({
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'heartbeat-producer-fp-v1',
        'acks': '1',
        'compression.type': 'snappy',
        'linger.ms': 20
    })

def dispatch_events(producer: Producer, topic: str, events: List[HeartbeatEvent]) -> None:
    """Functional iterative mapping function executing external broker writes."""
    for event in events:
        producer.produce(
            topic=topic,
            key=event.customer_id.encode('utf-8'),
            value=serialize_heartbeat(event),
            on_delivery=delivery_report_callback
        )
    producer.poll(0)


def streaming_loop(producer: Producer, config: dict, customer_ids: List[str], baselines: CustomerStates,
                   current_states: CustomerStates) -> None:
    """
    Functional loop execution structure tracking global states cleanly
    via parameters rather than local mutation variables.
    """
    try:
        # 1. Compute transformations cleanly
        events, next_states = simulate_step(customer_ids, baselines, current_states)

        # 2. Side-effect pipeline dispatch execution
        dispatch_events(producer, config["KAFKA_TOPIC_NAME"], events)
        print(f"[+] Dispatched metrics batch for {len(events)} customers successfully.")

        # 3. Deliberate wait block
        time.sleep(config["SIMULATION_INTERVAL"])

        # Recursive-style processing step: continue processing using NEXT state immutably
        return streaming_loop(producer, config, customer_ids, baselines, next_states)

    except KeyboardInterrupt:
        print("\n[*] Intercepted termination signal. Initiating flush sequence...")
        producer.flush(timeout=5.0)
        print("[+] System gracefully offline.")
        sys.exit(0)


def main() -> None:
    """Main execution boundary mapping configs directly into functional runtimes."""
    config = load_settings()
    producer = initialize_producer(config["KAFKA_BOOTSTRAP_SERVERS"])
    customer_ids, baselines = initialize_simulation_states(config["TOTAL_CUSTOMERS"])

    print(f"[*] Starting Functional Producer. Pipeline Destination Topic: {config['KAFKA_TOPIC_NAME']}")
    # Kick off processing loop passing baseline metrics as the initial starting state
    streaming_loop(producer, config, customer_ids, baselines, baselines)


if __name__ == "__main__":
    main()