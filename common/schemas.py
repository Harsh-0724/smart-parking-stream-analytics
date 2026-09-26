"""Pydantic schemas for every message that crosses a topic boundary."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class EventType(StrEnum):
    OCCUPANCY = "OCCUPANCY"
    HEARTBEAT = "HEARTBEAT"


class SlotStatus(StrEnum):
    OCCUPIED = "OCCUPIED"
    FREE = "FREE"


class ParkingEvent(BaseModel):
    """A sensor reading on `parking.raw`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1)
    event_type: EventType
    sensor_id: str = Field(min_length=1)
    lot_id: str = Field(min_length=1)
    slot_id: str = Field(min_length=1)
    status: SlotStatus | None = None
    vehicle_token: str | None = None
    event_ts: AwareDatetime
    ingest_ts: AwareDatetime

    @model_validator(mode="after")
    def _check_consistency(self) -> Self:
        if self.event_type is EventType.OCCUPANCY:
            if self.status is None:
                raise ValueError("OCCUPANCY event requires status")
            if self.status is SlotStatus.OCCUPIED and not self.vehicle_token:
                raise ValueError("OCCUPIED event requires vehicle_token")
            if self.status is SlotStatus.FREE and self.vehicle_token:
                raise ValueError("FREE event must not carry vehicle_token")
        elif self.status is not None or self.vehicle_token is not None:
            raise ValueError("HEARTBEAT must not carry status or vehicle_token")
        return self


class LotMetadata(BaseModel):
    """Compacted on `lot.metadata`, key = lot_id."""

    lot_id: str
    name: str
    personality: Literal["office", "mall", "station"]
    capacity: int
    slot_ids: list[str]
    columns: int
    latitude: float
    longitude: float


class WindowResult(BaseModel):
    """One 5-minute window on `lot.occupancy.5min`, key = `lot_id|window_start`."""

    lot_id: str
    window_start: AwareDatetime
    window_end: AwareDatetime
    capacity: int
    avg_occupied: float
    avg_occupancy_pct: float
    min_occupied: int
    max_occupied: int
    entries: int
    exits: int
    unique_vehicles_est: float
    closed: bool
    # Occupied slots right now; only set on open-window results (None once closed).
    current_occupied: int | None = None
    emitted_at: AwareDatetime
    # Wall-clock time the newest contributing event was produced; drives e2e latency.
    latest_ingest_ts: AwareDatetime | None = None

    @property
    def key(self) -> str:
        return window_key(self.lot_id, self.window_start)


class AlertKind(StrEnum):
    FULL_LOT = "FULL_LOT"
    SENSOR_OFFLINE = "SENSOR_OFFLINE"


class AlertState(StrEnum):
    RAISED = "RAISED"
    CLEARED = "CLEARED"


class Alert(BaseModel):
    """On `lot.alerts`. RAISED and CLEARED share an alert_id."""

    alert_id: str
    kind: AlertKind
    state: AlertState
    lot_id: str
    sensor_id: str | None = None
    ts: AwareDatetime
    message: str
    occupancy_pct: float | None = None


class LateEvent(BaseModel):
    """On `parking.late`: an event whose window had already closed."""

    event: ParkingEvent
    window_start: AwareDatetime
    watermark: AwareDatetime
    late_by_s: float
    routed_at: AwareDatetime


class DeadLetter(BaseModel):
    """On `parking.dlq`: the original bytes (hex) and why they were rejected."""

    original_hex: str
    error: str
    source_partition: int
    source_offset: int
    failed_at: AwareDatetime


def window_key(lot_id: str, window_start: datetime) -> str:
    return f"{lot_id}|{window_start.astimezone(UTC).isoformat()}"
