import logging
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError

from consumers.app import commit_processed_message, process_message_payload, retry_message_later
from core.models import HeartbeatEvent, serialize_heartbeat


class FakeMessage:
    def __init__(self, value: bytes = b"", error=None) -> None:
        self._value = value
        self._error = error

    def error(self):
        return self._error

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return "customer_heartbeats"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 7


class FakeKafkaError:
    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return f"fake kafka error {self._code}"


class ConsumerProcessingTests(unittest.TestCase):
    def setUp(self) -> None:
        logging.disable(logging.CRITICAL)

    def tearDown(self) -> None:
        logging.disable(logging.NOTSET)

    def test_none_message_has_no_offset_action(self) -> None:
        self.assertIsNone(process_message_payload(None, object()))

    def test_partition_eof_has_no_offset_action(self) -> None:
        message = FakeMessage(error=FakeKafkaError(KafkaError._PARTITION_EOF))

        self.assertIsNone(process_message_payload(message, object()))

    def test_non_eof_kafka_error_has_no_offset_action(self) -> None:
        message = FakeMessage(error=FakeKafkaError(-999))

        self.assertIsNone(process_message_payload(message, object()))

    def test_malformed_message_is_safe_to_commit_after_discard(self) -> None:
        with patch("consumers.app.execute_insert_transaction") as insert:
            result = process_message_payload(FakeMessage(b"not-json"), object())

        self.assertIs(result, True)
        insert.assert_not_called()

    def test_valid_message_is_safe_to_commit_after_database_insert(self) -> None:
        event = HeartbeatEvent(
            event_id="00000000-0000-0000-0000-000000000001",
            customer_id="CUST-1",
            heart_rate=72,
            recorded_at="2026-05-16T12:00:00Z",
        )

        with patch("consumers.app.execute_insert_transaction") as insert:
            result = process_message_payload(FakeMessage(serialize_heartbeat(event)), object())

        self.assertIs(result, True)
        processed_record = insert.call_args.args[1]
        self.assertEqual(processed_record.event_id, event.event_id)
        self.assertEqual(processed_record.customer_id, event.customer_id)
        self.assertEqual(processed_record.status, "NORMAL")

    def test_database_failure_marks_message_for_retry(self) -> None:
        event = HeartbeatEvent(
            event_id="00000000-0000-0000-0000-000000000001",
            customer_id="CUST-1",
            heart_rate=72,
            recorded_at="2026-05-16T12:00:00Z",
        )

        with patch(
            "consumers.app.execute_insert_transaction",
            side_effect=RuntimeError("db down"),
        ):
            result = process_message_payload(FakeMessage(serialize_heartbeat(event)), object())

        self.assertIs(result, False)

    def test_commit_processed_message_returns_true_when_commit_succeeds(self) -> None:
        consumer = Mock()
        consumer.commit.return_value = []
        message = FakeMessage()

        self.assertTrue(commit_processed_message(consumer, message))
        consumer.store_offsets.assert_called_once_with(message)
        consumer.commit.assert_called_once_with(asynchronous=False)

    def test_commit_processed_message_returns_false_on_partition_error(self) -> None:
        consumer = Mock()
        consumer.commit.return_value = [SimpleNamespace(error="commit failed")]

        self.assertFalse(commit_processed_message(consumer, FakeMessage()))

    def test_retry_message_later_seeks_to_failed_message(self) -> None:
        consumer = Mock()
        message = FakeMessage()

        with patch("consumers.app.time.sleep") as sleep:
            retry_message_later(consumer, message, delay_seconds=0.5)

        seek_target = consumer.seek.call_args.args[0]
        self.assertEqual(seek_target.topic, "customer_heartbeats")
        self.assertEqual(seek_target.partition, 0)
        self.assertEqual(seek_target.offset, 7)
        sleep.assert_called_once_with(0.5)


if __name__ == "__main__":
    unittest.main()
