import json
import random
from datetime import UTC, datetime
from itertools import count

import pytest

from common.schemas import ParkingEvent
from processor.hll import STANDARD_ERROR, UniqueCounter
from processor.lot_state import LotState, Outcome, Params

B = 300 * 10_000  # a window boundary
PARAMS = Params(
    window_size_s=300,
    allowed_lateness_s=60,
    grace_s=120,
    dedupe_ttl_s=300,
    sensor_offline_after_s=90,
)
_ids = count()


def ev(ts: float, slot: str = "A-001", occupied: bool | None = True, token: str = "t1", **kw: str):
    """An event at `ts`; occupied=None makes a heartbeat."""
    body = {
        "event_id": kw.get("event_id", f"e{next(_ids)}"),
        "event_type": "HEARTBEAT" if occupied is None else "OCCUPANCY",
        "sensor_id": f"S-{slot}",
        "lot_id": "LOT-01",
        "slot_id": slot,
        "status": None if occupied is None else ("OCCUPIED" if occupied else "FREE"),
        "vehicle_token": token if occupied else None,
        "event_ts": datetime.fromtimestamp(ts, UTC).isoformat(),
        "ingest_ts": datetime.fromtimestamp(ts, UTC).isoformat(),
    }
    return ParkingEvent.model_validate(body)


def fresh(capacity: int = 10) -> LotState:
    return LotState("LOT-01", PARAMS, capacity)


def feed(state: LotState, *events: ParkingEvent) -> list[Outcome]:
    return [state.process(e, ingest_ts=e.event_ts.timestamp()) for e in events]


def window_of(state: LotState, start: int):
    return state.windows[start]


# ---- dedupe ---------------------------------------------------------------------------------


def test_duplicate_event_id_is_dropped_and_not_double_counted() -> None:
    s = fresh()
    first = ev(B + 10, event_id="dup")
    assert feed(s, first, first) == [Outcome.ACCEPTED, Outcome.DUPLICATE]
    assert s.count == 1
    assert window_of(s, B).entries == 0  # first sighting of a slot is a state sync, not an entry


def test_dedupe_entries_expire_after_ttl() -> None:
    s = fresh()
    feed(s, ev(B + 10, event_id="old"))
    feed(s, ev(B + 10 + PARAMS.dedupe_ttl_s + 1, slot="A-002", event_id="new"))
    assert "old" not in s.seen and "new" in s.seen


# ---- lateness and watermark -------------------------------------------------------------------


def test_out_of_order_within_grace_lands_in_the_correct_window() -> None:
    s = fresh()
    feed(s, ev(B + 10, "A-001", True))  # slot A occupied from B+10
    feed(s, ev(B + 330, "A-009", None))  # heartbeat pushes event time into the next window
    assert s.watermark == B + 270
    # Arrives late but W0 is still open (end + grace = B + 420 >= watermark)
    outcome = feed(s, ev(B + 200, "A-002", True, "t2"))
    assert outcome == [Outcome.ACCEPTED]
    w0 = window_of(s, B)
    assert s.count == 2
    # slot A: B+10..B+300 = 290 s; slot B retroactively from B+200: 100 s inside W0
    assert w0.area == pytest.approx(290 + 100)
    # the retro time after the window boundary is credited to W1
    assert window_of(s, B + 300).area == pytest.approx(30 + 30)  # slot A + the corrected slot B


def test_event_beyond_grace_goes_to_late_and_does_not_change_state() -> None:
    s = fresh()
    feed(s, ev(B + 10, "A-001", True))
    feed(s, ev(B + 700, "A-009", None))  # watermark B+640 > W0.end + grace (B+420)
    before = s.to_dict()
    assert feed(s, ev(B + 50, "A-002", True, "t2")) == [Outcome.LATE]
    after = s.to_dict()
    assert after["count"] == before["count"] and after["slots"] == before["slots"]
    assert B not in s.windows  # W0 is closed and gone from open state


def test_window_closes_only_when_watermark_passes_end_plus_grace() -> None:
    s = fresh()
    feed(s, ev(B + 10, "A-001", True))
    # watermark == W0.end + grace exactly -> still open (strictly-greater rule)
    feed(s, ev(B + 300 + 120 + 60, "A-009", None))
    assert s.watermark == B + 420 and B in s.windows and not s.closed
    feed(s, ev(B + 300 + 120 + 61, "A-009", None))
    assert B not in s.windows
    (result,) = s.drain_closed(now=B + 1000)
    assert result.closed and result.window_start.timestamp() == B
    assert result.current_occupied is None


def test_late_check_ignores_heartbeats() -> None:
    s = fresh()
    feed(s, ev(B + 700, "A-009", None))
    assert feed(s, ev(B + 10, "A-009", None)) == [Outcome.ACCEPTED]


# ---- slot state -------------------------------------------------------------------------------


def test_older_event_cannot_regress_slot_state() -> None:
    s = fresh()
    feed(s, ev(B + 100, "A-001", True))
    # A FREE event with an earlier timestamp arrives afterwards (still within grace)
    assert feed(s, ev(B + 50, "A-001", False)) == [Outcome.ACCEPTED]
    assert s.slots["A-001"] == (B + 100, True)
    assert s.count == 1


def test_same_timestamp_does_not_flip_state() -> None:
    s = fresh()
    feed(s, ev(B + 100, "A-001", True), ev(B + 100, "A-001", False))
    assert s.slots["A-001"][1] is True


def test_entries_and_exits_are_counted_after_first_sync() -> None:
    s = fresh()
    feed(
        s,
        ev(B + 1, "A-001", True),  # first report: sync
        ev(B + 20, "A-001", False),  # exit
        ev(B + 40, "A-001", True),  # entry
        ev(B + 60, "A-001", True),  # repeated status: nothing
    )
    w = window_of(s, B)
    assert (w.entries, w.exits, s.count) == (1, 1, 1)


def test_first_report_free_creates_no_exit() -> None:
    s = fresh()
    feed(s, ev(B + 1, "A-001", False))
    assert window_of(s, B).exits == 0 and s.count == 0


# ---- windows ----------------------------------------------------------------------------------


def test_window_boundaries_are_half_open() -> None:
    s = fresh()
    feed(s, ev(B + 299.999, "A-001", True, "t1"), ev(B + 300, "A-002", True, "t2"))
    assert window_of(s, B).vehicles.estimate() == pytest.approx(1, abs=0.5)
    assert window_of(s, B + 300).vehicles.estimate() == pytest.approx(1, abs=0.5)


def test_time_weighted_average_is_by_duration_not_by_event_count() -> None:
    s = fresh(capacity=10)
    # 2 slots occupied for the first 100 s, 0 afterwards; the window is observed for 300 s
    feed(s, ev(B, "A-001", True, "t1"), ev(B, "A-002", True, "t2"))
    feed(s, ev(B + 100, "A-001", False), ev(B + 100, "A-002", False))
    feed(s, ev(B + 300, "A-009", None))
    (r,) = [r for r in s.dirty_open_results(B + 300) if r.window_start.timestamp() == B]
    assert r.avg_occupied == pytest.approx(2 * 100 / 300, abs=0.01)
    assert (r.min_occupied, r.max_occupied) == (0, 2)
    assert (r.entries, r.exits) == (0, 2)


def test_gap_in_events_still_integrates_occupancy_over_empty_windows() -> None:
    s = fresh()
    feed(s, ev(B + 10, "A-001", True))
    feed(s, ev(B + 10 + 900, "A-009", None))  # jump across three window boundaries
    closed = {r.window_start.timestamp(): r for r in s.drain_closed(now=B + 2000)}
    assert closed[B + 300].avg_occupied == pytest.approx(1.0)  # 1 slot for the whole window
    assert window_of(s, B + 600).area == pytest.approx(300)  # this one is still open


def test_dirty_windows_are_emitted_once_until_changed() -> None:
    s = fresh()
    feed(s, ev(B + 10, "A-001", True))
    assert len(s.dirty_open_results(now=B + 20)) == 1
    assert s.dirty_open_results(now=B + 21) == []
    feed(s, ev(B + 30, "A-002", True, "t2"))
    assert len(s.dirty_open_results(now=B + 40)) == 1


# ---- sensors ----------------------------------------------------------------------------------


def test_sensor_offline_and_recovery_signals() -> None:
    s = fresh()
    feed(s, ev(B, "A-001", None), ev(B, "A-002", None))
    feed(s, ev(B + 100, "A-002", None))  # A-002 alive, A-001 silent for 100 s > 90 s
    (down,) = s.sensor_signals()
    assert (down.sensor_id, down.offline) == ("S-A-001", True)
    assert s.sensor_signals() == []  # signalled once
    feed(s, ev(B + 110, "A-001", None))
    (up,) = s.sensor_signals()
    assert (up.sensor_id, up.offline) == ("S-A-001", False)


# ---- checkpoint / recovery --------------------------------------------------------------------


def _script() -> list[ParkingEvent]:
    rng = random.Random(7)
    events, state = [], {}
    for i in range(400):
        slot = f"A-{rng.randrange(8):03d}"
        occ = not state.get(slot, False)
        state[slot] = occ
        events.append(ev(B + i * 4.0, slot, occ, f"tok{rng.randrange(50)}"))
        if i % 10 == 0:
            events.append(ev(B + i * 4.0 + 1, f"A-{rng.randrange(8):03d}", None))
    return events


def _outputs(s: LotState, now: float) -> list[str]:
    results = s.drain_closed(now) + s.dirty_open_results(now)
    return [r.model_dump_json(exclude={"emitted_at"}) for r in results]


def test_restore_from_checkpoint_matches_uninterrupted_run() -> None:
    events = _script()
    straight = fresh()
    feed(straight, *events)

    first, second = events[:250], events[250:]
    a = fresh()
    feed(a, *first)
    # Closed windows are emitted before each checkpoint, so they are not part of the saved state.
    emitted_before = [r.model_dump_json(exclude={"emitted_at"}) for r in a.drain_closed(1e9)]
    restored = LotState.from_dict(json.loads(json.dumps(a.to_dict())), PARAMS)
    feed(restored, *second)

    assert emitted_before + _outputs(restored, 1e9) == _outputs(straight, 1e9)


def test_replayed_events_after_restore_are_dropped_as_duplicates() -> None:
    events = _script()
    a = fresh()
    feed(a, *events[:250])
    restored = LotState.from_dict(json.loads(json.dumps(a.to_dict())), PARAMS)
    replayed = feed(restored, *events[200:250])  # crash before commit: 50 events come again
    assert set(replayed) == {Outcome.DUPLICATE}
    assert restored.to_dict()["count"] == a.to_dict()["count"]


def test_invalid_dedupe_ttl_is_rejected() -> None:
    with pytest.raises(ValueError):
        Params(300, 60, 120, dedupe_ttl_s=100, sensor_offline_after_s=90)


# ---- HyperLogLog ------------------------------------------------------------------------------


@pytest.mark.parametrize("n", [50, 500, 5_000, 50_000])
def test_hll_error_is_within_three_standard_errors(n: int) -> None:
    hll = UniqueCounter()
    for i in range(n):
        hll.add(f"vehicle-{i}")
    error = abs(hll.estimate() - n) / n
    assert error < 3 * STANDARD_ERROR


def test_hll_ignores_repeats_and_survives_serialisation() -> None:
    hll = UniqueCounter()
    for _ in range(3):
        for i in range(1_000):
            hll.add(f"v{i}")
    clone = UniqueCounter.from_b64(hll.to_b64())
    assert clone.estimate() == hll.estimate()
    assert hll.memory_bytes == 1024
