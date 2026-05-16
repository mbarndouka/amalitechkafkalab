from core.models import HeartbeatEvent, ProcessedHeartbeat


def evaluate_heart_rate_status(heart_rate: int) -> str:
    """Pure mathematical classification of biological measurement boundaries."""
    if heart_rate < 50 or heart_rate > 140:
        return "CRITICAL"
    elif heart_rate < 60 or heart_rate > 100:
        return "WARNING"
    return "NORMAL"


def clean_and_process(event: HeartbeatEvent) -> ProcessedHeartbeat:
    """
    Pure transformation function. Takes an immutable raw event, passes
    metrics through rule bindings, and maps results onto an enriched structure.
    """
    status = evaluate_heart_rate_status(event.heart_rate)

    return ProcessedHeartbeat(
        event_id=event.event_id,
        customer_id=event.customer_id,
        heart_rate=event.heart_rate,
        status=status,
        recorded_at=event.recorded_at
    )