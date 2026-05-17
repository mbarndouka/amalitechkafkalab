import uuid
import json
from datetime import datetime, timezone
from typing import NamedTuple, Optional

class HeartbeatEvent(NamedTuple):
    event_id: str
    customer_id: str
    heart_rate: int
    recorded_at: str

class ProcessedHeartbeat(NamedTuple):
    event_id: str
    customer_id: str
    heart_rate: int
    status: str    #NORMAL, WARNING, CRITICAL
    recorded_at: str

def create_heartbeat(customer_id: str, heart_rate: int) -> HeartbeatEvent:
    """Pure factory function validating and generating immutable schema tokens."""
    if not (0 <=  heart_rate <= 300):
        raise ValueError(f"Biologically impossible heart rate value: {heart_rate}")

    return HeartbeatEvent(
        event_id=str(uuid.uuid4()),
        customer_id=customer_id,
        heart_rate=heart_rate,
        recorded_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )

def serialize_heartbeat(heartbeat: HeartbeatEvent) -> bytes:
    """Pure function mapping a Heartbeat tuple into JSON byte streams."""
    return json.dumps(heartbeat._asdict()).encode("utf-8")

def deserialize_heartbeat(raw_bytes: bytes) -> Optional[HeartbeatEvent]:
    """Pure function mapping incoming wire stream bytes back to structured records."""
    try:
        data = json.loads(raw_bytes.decode('utf-8'))
        return HeartbeatEvent(
            event_id=data["event_id"],
            customer_id=data["customer_id"],
            heart_rate=int(data["heart_rate"]),
            recorded_at=data["recorded_at"]
        )
    except (json.JSONDecodeError, KeyError, ValueError, TypeError):
        # Graceful error state representation without throwing runtime crashes
        return None
