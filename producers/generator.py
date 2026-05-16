import random
from typing import List, Tuple, Mapping
from core.models import HeartbeatEvent, create_heartbeat

# Explicit Types for State Tracking
CustomerStates = Mapping[str, float]
GeneratorResult = Tuple[List[HeartbeatEvent], CustomerStates]

def initialize_simulation_states(total_customers: int) -> Tuple[List[str], CustomerStates]:
    """Pure setup factory yielding client identifiers and baseline metrics."""
    customer_ids = [f"CUST-{1000 + i}" for i in range(total_customers)]
    baselines = {cid: random.uniform(65.0, 85.0) for cid in customer_ids}
    return customer_ids, baselines


def simulate_step(customer_ids: List[str], baselines: CustomerStates,
                  current_states: CustomerStates) -> GeneratorResult:
    """
    Pure statistical step function. Takes the current system state, applies
    a random walk transform, and returns a NEW state alongside the data events.
    """
    events: List[HeartbeatEvent] = []
    next_states = {}

    for cid in customer_ids:
        # Calculate next step state using a Gaussian random walk delta
        step = random.normalvariate(0, 1.5)

        # Pull baseline and historical value out safely
        base = baselines[cid]
        prev_rate = current_states.get(cid, base)

        # Functional Drift Correction
        new_rate = prev_rate + step + ((base - prev_rate) * 0.05)
        new_rate = max(45.0, min(180.0, new_rate))

        next_states[cid] = new_rate
        events.append(create_heartbeat(customer_id=cid, heart_rate=int(round(new_rate))))

    return events, next_states