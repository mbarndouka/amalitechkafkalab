import datetime
import uuid
import json
from datetime import datetime
from typing import NamedTuple

class HeartbeatEvent(NamedTuple):
    event_id: str
    costumer_id: str
    heart_rate: int
    recorded_at: str

def create_heartbeat(customer_id: str, heart_rate: int) -> HeartbeatEvent:
    """Pure factory function validating and generating immutable schema tokens."""
    if not (0 <=  heart_rate <= 300):
        raise ValueError(f"Biologically impossible heart rate value: {heart_rate}")

    return HeartbeatEvent(
        event_id=str(uuid.uuid4()),
        costumer_id=customer_id,
        heart_rate=heart_rate,
        recorded_at=datetime.utcnow().isoformat() + "Z"
    )

def serialize_heartbeat(heartbeat: HeartbeatEvent) -> bytes:
    """Pure function mapping a Heartbeat tuple into JSON byte streams."""
    return json.dumps(heartbeat._asdict()).encode("utf-8")