import unittest

from core.models import HeartbeatEvent
from consumers.validator import clean_and_process, evaluate_heart_rate_status


class HeartRateValidatorTests(unittest.TestCase):
    def test_evaluate_heart_rate_status_boundaries(self) -> None:
        cases = [
            (49, "CRITICAL"),
            (50, "WARNING"),
            (59, "WARNING"),
            (60, "NORMAL"),
            (100, "NORMAL"),
            (101, "WARNING"),
            (140, "WARNING"),
            (141, "CRITICAL"),
        ]

        for heart_rate, expected_status in cases:
            with self.subTest(heart_rate=heart_rate):
                self.assertEqual(evaluate_heart_rate_status(heart_rate), expected_status)

    def test_clean_and_process_preserves_event_fields_and_adds_status(self) -> None:
        event = HeartbeatEvent(
            event_id="00000000-0000-0000-0000-000000000001",
            customer_id="CUST-1",
            heart_rate=145,
            recorded_at="2026-05-16T12:00:00Z",
        )

        processed = clean_and_process(event)

        self.assertEqual(processed.event_id, event.event_id)
        self.assertEqual(processed.customer_id, event.customer_id)
        self.assertEqual(processed.heart_rate, event.heart_rate)
        self.assertEqual(processed.recorded_at, event.recorded_at)
        self.assertEqual(processed.status, "CRITICAL")


if __name__ == "__main__":
    unittest.main()
