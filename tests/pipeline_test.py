import sys
import time
from typing import List, Tuple
from confluent_kafka import Producer
from core.config import load_settings
from core.models import HeartbeatEvent, serialize_heartbeat


def initialize_test_producer(servers: str) -> Producer:
    """Functional factory yielding an immediate-dispatch testing producer."""
    return Producer({
        'bootstrap.servers': servers,
        'client.id': 'pipeline-test-harness',
        'acks': '1'
    })


def generate_test_payloads() -> List[Tuple[str, int]]:
    """
    Pure matrix function returning explicit (customer_id, heart_rate) tuples
    designed to trigger every deterministic branch of our pipeline validation logic.
    """
    return [
        ("TEST-ALPHA", 72),  # Should resolve to: NORMAL
        ("TEST-ALPHA", 85),  # Should resolve to: NORMAL
        ("TEST-BETA", 55),  # Should resolve to: WARNING (Bradycardia line)
        ("TEST-BETA", 145),  # Should resolve to: CRITICAL (Tachycardia spike)
        ("TEST-BETA", 42),  # Should resolve to: CRITICAL
    ]


def inject_test_matrix(producer: Producer, topic: str, matrix: List[Tuple[str, int]]) -> None:
    """Asynchronously dispatches structural test vectors into the streaming broker."""
    print(f"[*] Dispatching {len(matrix)} distinct telemetry validation frames to Kafka...")

    for idx, (cid, bpm) in enumerate(matrix):
        try:
            # Construct immutable test point via our existing core domain logic
            event = HeartbeatEvent(
                event_id=f"00000000-0000-0000-0000-00000000000{idx}",  # Deterministic UUID trace
                customer_id=cid,
                heart_rate=bpm,
                recorded_at=f"2026-05-16T12:00:0{idx}Z"
            )

            producer.produce(
                topic=topic,
                key=cid.encode('utf-8'),
                value=serialize_heartbeat(event)
            )
        except ValueError as err:
            print(f"[!] Validation edge caught bad input cleanly for {cid} ({bpm} bpm): {err}")

    # Force an absolute network block flush to guarantee delivery before asserting
    producer.flush(timeout=5.0)
    print("[+] Transmission phase locked. Core buffers empty.")


def main() -> None:
    """Execution wrapper setting up and firing integration assertions."""
    config = load_settings()
    producer = initialize_test_producer(config["KAFKA_BOOTSTRAP_SERVERS"])
    test_matrix = generate_test_payloads()

    inject_test_matrix(producer, config["KAFKA_TOPIC_NAME"], test_matrix)
    print("[*] Test signals emitted. Check your consumer window for evaluation execution.")


if __name__ == "__main__":
    main()