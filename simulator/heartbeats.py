"""Per-sensor heartbeat schedule, one every HEARTBEAT_INTERVAL_S of event time."""

import heapq
import random

from simulator.emission import Emission

HEARTBEAT_INTERVAL_S = 30.0


class HeartbeatSchedule:
    def __init__(self, lot_id: str, slot_ids: list[str], start_ts: float, rng: random.Random):
        self._lot_id = lot_id
        # Random phase per sensor so heartbeats are spread out, not synchronised.
        self._heap = [(start_ts + rng.random() * HEARTBEAT_INTERVAL_S, s) for s in slot_ids]
        heapq.heapify(self._heap)

    def due(self, until: float, burst: float = 1.0) -> list[Emission]:
        out: list[Emission] = []
        while self._heap and self._heap[0][0] <= until:
            ts, slot = heapq.heappop(self._heap)
            out.append(Emission(ts, self._lot_id, slot, heartbeat=True))
            heapq.heappush(self._heap, (ts + HEARTBEAT_INTERVAL_S / burst, slot))
        return out
