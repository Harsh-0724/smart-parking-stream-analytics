"""End to end: known events into Kafka, exact rows out of TimescaleDB.

Runs against the real Compose stack (kafka x3, processor, sink, alerter, prometheus).
Expected values are derived by hand from the window rules, not from the implementation.

Timeline for one lot (capacity 10), seconds after a 5-minute boundary T:

    arrival order  event_ts  event
    1              0         A-001 FREE      (first report: state sync)
    2              0         A-002 FREE      (sync)
    3              5         A-004 FREE      (sync)
    4              60        A-001 OCCUPIED  token A     -> entry
    5              120       A-002 OCCUPIED  token B     -> entry
    6              120       (byte-identical duplicate of 5)
    7              180       A-001 FREE                  -> exit
    8              330       A-003 FREE      (sync)
    9              400       A-003 OCCUPIED  token C     -> entry (window 2)
    10             250       A-004 OCCUPIED  token D     -> LATE but inside grace: window 1
    11             -         b"not json" (malformed)
    12             800       heartbeat S-A-009           -> watermark 740 closes windows 1 and 2
    13             30        A-002 FREE      arrives after window 1 closed -> parking.late
    14             1100      heartbeat                   -> watermark 1040 closes window 3

Window 1 [0,300):   occupied 0 for 60 s, 1 for 60 s, 2 for 60 s, 1 for 70 s (until the late
                    A-004 at 250), 2 for 50 s  -> area 350 -> avg 350/300; entries 3
                    (A-001, A-002, A-004), exits 1, vehicles {A, B, D}.
Window 2 [300,600): 2 for 100 s (A-002, A-004), then 3 -> area 200 + 600 = 800 -> avg 800/300;
                    entries 1 (A-003), exits 0, vehicles {C}.
Window 3 [600,900): 3 for 300 s -> avg 3.0, no events at all, but still emitted.
"""

import json
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import Producer

from common import topics
from common.kafka import producer_config, read_to_end
from common.schemas import DeadLetter, LateEvent, LotMetadata
from simulator.emission import iso

pytestmark = pytest.mark.integration

WINDOW_S = 300
WAIT_S = 120


def _wait(check: Callable[[], Any], timeout: float = WAIT_S, interval: float = 1.0) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = check()
        except (psycopg.Error, OSError):
            last = None
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout} s (last value: {last!r})")


def _event(
    lot: str, n: str, ts: float, slot: str, status: str | None, token: str | None = None
) -> bytes:
    heartbeat = status is None
    return json.dumps(
        {
            "event_id": f"{lot}-{n}",
            "event_type": "HEARTBEAT" if heartbeat else "OCCUPANCY",
            "sensor_id": f"S-{slot}",
            "lot_id": lot,
            "slot_id": slot,
            "status": status,
            "vehicle_token": token,
            "event_ts": iso(ts),
            "ingest_ts": iso(time.time()),
        }
    ).encode()


def _metadata(lot: str, capacity: int) -> bytes:
    slots = [f"A-{i:03d}" for i in range(1, capacity + 1)]
    return LotMetadata(
        lot_id=lot, name=lot, personality="office", capacity=capacity, slot_ids=slots,
        columns=10, latitude=12.9, longitude=77.5,
    ).model_dump_json().encode()  # fmt: skip


def _send(bootstrap: str, lot: str, payloads: list[bytes], metadata: bytes) -> None:
    producer = Producer(producer_config(bootstrap, "integration-test"))
    producer.produce(topics.LOT_METADATA, key=lot.encode(), value=metadata)
    producer.flush(30)  # the processor reads capacity when it first sees the lot
    for payload in payloads:
        producer.produce(topics.RAW, key=lot.encode(), value=payload)
    assert producer.flush(30) == 0


def _prom(query: str) -> float:
    url = "http://localhost:9090/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310
        result = json.load(response)["data"]["result"]
    return float(result[0]["value"][1]) if result else 0.0


def test_known_event_set_produces_exact_rows(bootstrap: str, dsn: str) -> None:
    lot = f"LOT-IT-{uuid4().hex[:6]}"
    t = int(time.time()) // WINDOW_S * WINDOW_S - 3600  # a past window boundary
    duplicates_before = _prom("sum(parking_events_duplicate_total)")

    e = lambda n, off, slot, status, token=None: _event(lot, n, t + off, slot, status, token)  # noqa: E731
    dup = e("e4", 120, "A-002", "OCCUPIED", "tokB")
    payloads = [
        e("e1", 0, "A-001", "FREE"),
        e("e2", 0, "A-002", "FREE"),
        e("e3", 5, "A-004", "FREE"),
        e("e4a", 60, "A-001", "OCCUPIED", "tokA"),
        dup,
        dup,
        e("e5", 180, "A-001", "FREE"),
        e("e6", 330, "A-003", "FREE"),
        e("e7", 400, "A-003", "OCCUPIED", "tokC"),
        e("e8-late-in-grace", 250, "A-004", "OCCUPIED", "tokD"),
        b"not json at all",
        e("e9-hb", 800, "A-009", None),
        e("e10-beyond-grace", 30, "A-002", "FREE"),
        e("e11-hb", 1100, "A-009", None),
    ]
    _send(bootstrap, lot, payloads, _metadata(lot, capacity=10))

    def closed_rows() -> list[tuple[Any, ...]] | None:
        with psycopg.connect(dsn) as conn:
            rows = conn.execute(
                """SELECT extract(epoch FROM window_start)::bigint, capacity, avg_occupied,
                          avg_occupancy_pct, min_occupied, max_occupied, entries, exits,
                          unique_vehicles_est, closed
                   FROM lot_occupancy_5min WHERE lot_id = %s AND closed ORDER BY window_start""",
                (lot,),
            ).fetchall()
        return rows if len(rows) >= 3 else None

    rows = _wait(closed_rows)
    assert len(rows) == 3, "exactly windows 1-3 are closed"

    (w1, w2, w3) = rows
    assert w1[0] == t and w2[0] == t + WINDOW_S and w3[0] == t + 2 * WINDOW_S
    assert all(r[1] == 10 and r[9] is True for r in rows)  # capacity from lot.metadata, closed

    assert w1[2] == pytest.approx(350 / 300, abs=1e-3)  # avg_occupied
    assert w1[3] == pytest.approx(100 * 350 / 300 / 10, abs=0.01)  # percent of capacity
    assert (w1[4], w1[5]) == (0, 2)  # min / max occupied
    assert (w1[6], w1[7]) == (3, 1)  # entries / exits: the late A-004 landed in window 1
    assert w1[8] == pytest.approx(3, abs=0.5)  # vehicles A, B, D

    assert w2[2] == pytest.approx(800 / 300, abs=1e-3)
    assert (w2[6], w2[7]) == (1, 0)
    assert w2[8] == pytest.approx(1, abs=0.5)  # vehicle C

    assert w3[2] == pytest.approx(3.0, abs=1e-3)  # no events in the window, occupancy carried over
    assert (w3[4], w3[5], w3[6], w3[7]) == (3, 3, 0, 0)

    # The malformed message is in the DLQ with its original bytes; the beyond-grace event is in
    # parking.late; the duplicate was dropped (visible in the processor's counter).
    dlq = [
        DeadLetter.model_validate_json(v) for _, v, _ in read_to_end(bootstrap, topics.DLQ, 0) if v
    ]
    assert any(bytes.fromhex(d.original_hex) == b"not json at all" and d.error for d in dlq)

    late = [
        LateEvent.model_validate_json(v) for _, v, _ in read_to_end(bootstrap, topics.LATE, 0) if v
    ]
    mine = [x for x in late if x.event.event_id == f"{lot}-e10-beyond-grace"]
    assert len(mine) == 1 and mine[0].late_by_s > 0

    _wait(lambda: _prom("sum(parking_events_duplicate_total)") >= duplicates_before + 1, timeout=30)

    # Window 1 was final before the late event arrived: the stored row must not have changed.
    with psycopg.connect(dsn) as conn:
        again = conn.execute(
            "SELECT entries FROM lot_occupancy_5min"
            " WHERE lot_id=%s AND window_start=to_timestamp(%s)",
            (lot, t),
        ).fetchone()
    assert again == (3,)

    # Sensors went silent (event time moved 700+ s past their last report): the processor raised
    # SENSOR_OFFLINE alerts and the sink stored them.
    def offline_alert() -> Any:
        with psycopg.connect(dsn) as conn:
            return conn.execute(
                "SELECT status, message FROM alerts WHERE lot_id=%s AND sensor_id='S-A-004'"
                " AND kind='SENSOR_OFFLINE'",
                (lot,),
            ).fetchone()

    status, message = _wait(offline_alert, timeout=60)
    assert status == "active" and "S-A-004" in message

    # The processor checkpointed this lot to the compacted changelog.
    def checkpointed() -> bool:
        return any(
            key == lot.encode()
            for p in range(topics.RAW_PARTITIONS)
            for key, _, _ in read_to_end(bootstrap, topics.CHANGELOG, p)
        )

    assert _wait(checkpointed, timeout=60, interval=5)


def test_full_lot_alert_is_raised_and_cleared_with_hysteresis(bootstrap: str, dsn: str) -> None:
    lot = f"LOT-FL-{uuid4().hex[:6]}"
    t = int(time.time()) // WINDOW_S * WINDOW_S
    slots = [f"A-{i:03d}" for i in range(1, 11)]
    e = lambda n, off, slot, status, token=None: _event(lot, n, t + off, slot, status, token)  # noqa: E731

    fill = [e(f"free{i}", 0, s, "FREE") for i, s in enumerate(slots)]
    fill += [
        e(f"occ{i}", 10 + i, s, "OCCUPIED", f"tok{i}") for i, s in enumerate(slots[:9])
    ]  # 9/10 = 90%
    _send(bootstrap, lot, fill, _metadata(lot, capacity=10))

    def alert_status() -> str | None:
        with psycopg.connect(dsn) as conn:
            row = conn.execute(
                "SELECT status FROM alerts WHERE lot_id=%s AND kind='FULL_LOT'", (lot,)
            ).fetchone()
        return row[0] if row else None

    assert _wait(lambda: alert_status() == "active", timeout=90)

    # 9 -> 8 occupied is 80%: below the 85% clear threshold. (85%..90% would have kept it active.)
    release = [e("leave0", 60, slots[0], "FREE"), e("leave1", 61, slots[1], "FREE")]
    _send(bootstrap, lot, release, _metadata(lot, capacity=10))
    assert _wait(lambda: alert_status() == "cleared", timeout=90)


def test_all_pipeline_targets_are_scraped_by_prometheus(stack: dict[str, str]) -> None:
    def targets_up() -> bool:
        url = "http://localhost:9090/api/v1/targets"
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310
            targets = json.load(response)["data"]["activeTargets"]
        up = {t["labels"]["job"] for t in targets if t["health"] == "up"}
        return {"processor", "sink", "alerter", "kafka-exporter"} <= up

    assert _wait(targets_up, timeout=60)


def _api(path: str) -> Any:
    with urllib.request.urlopen(f"http://localhost:8080{path}", timeout=10) as response:  # noqa: S310
        return json.load(response)


def test_api_serves_pipeline_output_and_survives_malformed_messages(
    bootstrap: str, dsn: str
) -> None:
    import asyncio

    import websockets

    lot = f"LOT-API-{uuid4().hex[:6]}"
    t = int(time.time()) // WINDOW_S * WINDOW_S - 1800
    e = lambda n, off, slot, status, token=None: _event(lot, n, t + off, slot, status, token)  # noqa: E731

    assert _wait(lambda: _api("/api/health")["kafka_feed"], timeout=60)

    first = [
        e("f1", 0, "A-001", "FREE"),
        e("f2", 0, "A-002", "FREE"),
        e("f3", 0, "A-003", "FREE"),
        e("o1", 60, "A-001", "OCCUPIED", "tokA"),
        e("o2", 120, "A-002", "OCCUPIED", "tokB"),
        e("hb", 800, "A-009", None),  # closes the first window
    ]
    _send(bootstrap, lot, first, _metadata(lot, capacity=10))

    _wait(
        lambda: (
            psycopg.connect(dsn)
            .execute("SELECT 1 FROM lot_occupancy_5min WHERE lot_id=%s AND closed", (lot,))
            .fetchone()
        ),
    )

    mine = _wait(lambda: {x["lot_id"]: x for x in _api("/api/lots")}.get(lot))  # cached for 2 s
    assert mine["capacity"] == 10 and mine["name"] == lot

    history = _wait(lambda: _api(f"/api/lots/{lot}/history")["points"])
    assert any(p["closed"] for p in history)
    assert history[0]["entries"] == 2  # A-001 and A-002 entered in window 1

    async def watch() -> dict[str, bool]:
        async with websockets.connect("ws://localhost:8080/ws") as ws:
            await ws.send(json.dumps({"type": "subscribe", "lots": [lot]}))
            snapshot: dict[str, Any] = {}
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not snapshot:
                message = json.loads(await asyncio.wait_for(ws.recv(), 10))
                if message["type"] == "snapshot" and message["lot_id"] == lot and message["slots"]:
                    snapshot = {s["slot_id"]: s["occupied"] for s in message["slots"]}
            assert snapshot, "no slot snapshot received"
            # A malformed message, then a valid event: the feed must survive the first
            producer = Producer(producer_config(bootstrap, "integration-test-2"))
            producer.produce(
                topics.RAW, key=lot.encode(), value=b'{"event_type":"OCCUPANCY" broken'
            )
            producer.produce(
                topics.RAW, key=lot.encode(), value=e("o3", 900, "A-003", "OCCUPIED", "tokC")
            )
            producer.flush(30)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                message = json.loads(await asyncio.wait_for(ws.recv(), 10))
                if message["type"] == "slots" and message["lot_id"] == lot:
                    for d in message["deltas"]:
                        snapshot[d["slot_id"]] = d["occupied"]
                    if snapshot.get("A-003") is True:
                        break
            return snapshot

    state = asyncio.run(watch())
    assert state["A-001"] is True and state["A-002"] is True and state["A-003"] is True
    assert _api("/api/health")["kafka_feed"] is True
