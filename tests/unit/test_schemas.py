import pytest
from pydantic import ValidationError

from common.schemas import ParkingEvent

BASE = {
    "event_id": "e1",
    "event_type": "OCCUPANCY",
    "sensor_id": "S-A-001",
    "lot_id": "LOT-01",
    "slot_id": "A-001",
    "status": "OCCUPIED",
    "vehicle_token": "tok",
    "event_ts": "2026-01-01T10:00:00Z",
    "ingest_ts": "2026-01-01T10:00:01Z",
}


def test_valid_occupied_event() -> None:
    assert ParkingEvent.model_validate(BASE).lot_id == "LOT-01"


def test_occupied_requires_token() -> None:
    with pytest.raises(ValidationError):
        ParkingEvent.model_validate({**BASE, "vehicle_token": None})


def test_free_must_not_have_token() -> None:
    with pytest.raises(ValidationError):
        ParkingEvent.model_validate({**BASE, "status": "FREE"})


def test_naive_timestamp_rejected() -> None:
    with pytest.raises(ValidationError):
        ParkingEvent.model_validate({**BASE, "event_ts": "2026-01-01T10:00:00"})
