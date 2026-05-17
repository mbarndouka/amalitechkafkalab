import os
import time
import unittest
import uuid

import psycopg2
from confluent_kafka import Producer

from core.config import load_settings
from core.models import HeartbeatEvent, serialize_heartbeat


@unittest.skipUnless(
    os.getenv("RUN_INTEGRATION_TESTS") == "1",
    "Set RUN_INTEGRATION_TESTS=1 to run Docker-backed pipeline tests.",
)
class PipelineIntegrationTests(unittest.TestCase):
    def test_kafka_to_postgres_pipeline_persists_heartbeat(self) -> None:
        """
        Requires the Docker stack to be running.

        Start services first:
        docker compose up -d

        Then run:
        RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests
        """
        config = load_settings(require_database=True)
        event = HeartbeatEvent(
            event_id=str(uuid.uuid4()),
            customer_id="TEST-INTEGRATION",
            heart_rate=145,
            recorded_at="2026-05-16T12:00:00Z",
        )

        with psycopg2.connect(config["DB_DSN"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM heart_rate_logs WHERE event_id = %s",
                    (event.event_id,),
                )

        producer = Producer(
            {
                "bootstrap.servers": config["KAFKA_BOOTSTRAP_SERVERS"],
                "client.id": "pipeline-integration-test",
                "acks": "all",
            }
        )
        producer.produce(
            topic=config["KAFKA_TOPIC_NAME"],
            key=event.customer_id.encode("utf-8"),
            value=serialize_heartbeat(event),
        )
        producer.flush(timeout=10)

        deadline = time.monotonic() + 20
        row = None
        while time.monotonic() < deadline:
            with psycopg2.connect(config["DB_DSN"]) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT customer_id, heart_rate, status
                        FROM heart_rate_logs
                        WHERE event_id = %s
                        """,
                        (event.event_id,),
                    )
                    row = cursor.fetchone()
            if row is not None:
                break
            time.sleep(1)

        self.assertEqual(row, ("TEST-INTEGRATION", 145, "CRITICAL"))


if __name__ == "__main__":
    unittest.main()
