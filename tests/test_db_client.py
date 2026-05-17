import unittest

from consumers.db_client import transform_record_to_sql_params
from core.models import ProcessedHeartbeat


class DatabaseClientTests(unittest.TestCase):
    def test_transform_record_to_sql_params_preserves_insert_order(self) -> None:
        record = ProcessedHeartbeat(
            event_id="00000000-0000-0000-0000-000000000001",
            customer_id="CUST-1",
            heart_rate=72,
            status="NORMAL",
            recorded_at="2026-05-16T12:00:00Z",
        )

        self.assertEqual(
            transform_record_to_sql_params(record),
            (
                "00000000-0000-0000-0000-000000000001",
                "CUST-1",
                72,
                "NORMAL",
                "2026-05-16T12:00:00Z",
            ),
        )


if __name__ == "__main__":
    unittest.main()
