import json
import unittest
import uuid

from core.models import HeartbeatEvent, create_heartbeat, deserialize_heartbeat, serialize_heartbeat


class HeartbeatModelTests(unittest.TestCase):
    def test_create_heartbeat_returns_valid_event(self) -> None:
        event = create_heartbeat("CUST-1", 72)

        self.assertEqual(event.customer_id, "CUST-1")
        self.assertEqual(event.heart_rate, 72)
        uuid.UUID(event.event_id)
        self.assertTrue(event.recorded_at.endswith("Z"))

    def test_create_heartbeat_rejects_impossible_values(self) -> None:
        for heart_rate in (-1, 301):
            with self.subTest(heart_rate=heart_rate):
                with self.assertRaises(ValueError):
                    create_heartbeat("CUST-1", heart_rate)

    def test_serialize_and_deserialize_round_trip(self) -> None:
        event = HeartbeatEvent(
            event_id="00000000-0000-0000-0000-000000000001",
            customer_id="CUST-1",
            heart_rate=72,
            recorded_at="2026-05-16T12:00:00Z",
        )

        restored = deserialize_heartbeat(serialize_heartbeat(event))

        self.assertEqual(restored, event)

    def test_deserialize_accepts_numeric_heart_rate_strings(self) -> None:
        payload = {
            "event_id": "00000000-0000-0000-0000-000000000001",
            "customer_id": "CUST-1",
            "heart_rate": "72",
            "recorded_at": "2026-05-16T12:00:00Z",
        }

        event = deserialize_heartbeat(json.dumps(payload).encode("utf-8"))

        self.assertEqual(event.heart_rate, 72)

    def test_deserialize_returns_none_for_malformed_payloads(self) -> None:
        malformed_payloads = [
            b"not-json",
            json.dumps({"customer_id": "CUST-1"}).encode("utf-8"),
            json.dumps(
                {
                    "event_id": "00000000-0000-0000-0000-000000000001",
                    "customer_id": "CUST-1",
                    "heart_rate": "not-a-number",
                    "recorded_at": "2026-05-16T12:00:00Z",
                }
            ).encode("utf-8"),
        ]

        for payload in malformed_payloads:
            with self.subTest(payload=payload):
                self.assertIsNone(deserialize_heartbeat(payload))


if __name__ == "__main__":
    unittest.main()
