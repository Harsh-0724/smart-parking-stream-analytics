"""Simulator-internal event record and its wire encoding."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class Emission:
    ts: float  # event time, epoch seconds
    lot_id: str
    slot_id: str
    heartbeat: bool
    occupied: bool = False
    vehicle_token: str | None = None
    # True for the initial state report of a slot; a state sync, not a real arrival/departure.
    sync: bool = False

    @property
    def sensor_id(self) -> str:
        return f"S-{self.slot_id}"


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat(timespec="milliseconds")


def encode(e: Emission, ingest_ts: float, event_id: str | None = None) -> bytes:
    body: dict[str, str | None] = {
        "event_id": event_id or str(uuid4()),
        "event_type": "HEARTBEAT" if e.heartbeat else "OCCUPANCY",
        "sensor_id": e.sensor_id,
        "lot_id": e.lot_id,
        "slot_id": e.slot_id,
        "status": None if e.heartbeat else ("OCCUPIED" if e.occupied else "FREE"),
        "vehicle_token": e.vehicle_token if e.occupied and not e.heartbeat else None,
        "event_ts": iso(e.ts),
        "ingest_ts": iso(ingest_ts),
    }
    return json.dumps(body).encode()


def vehicle_token(salt: str, lot_id: str, vehicle: int) -> str:
    """Salted hash: the pipeline never sees a plate number."""
    return hashlib.sha256(f"{salt}:{lot_id}:{vehicle}".encode()).hexdigest()[:16]
