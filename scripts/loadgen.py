"""Rate-controlled load generator: valid events at an exact target rate, in real time.

    python scripts/loadgen.py --rate 10000 --duration 30 --workers 2

Uses the production producer settings (idempotent, acks=all, lz4). Event time is wall-clock
time, so windows close naturally and latency measurements are meaningful. The mix is 30%
OCCUPANCY (alternating FREE/OCCUPIED per slot) and 70% HEARTBEAT, over the same 12 partition-
balanced lots the simulator uses, 200 slots each.
"""

import argparse
import json
import multiprocessing as mp
import os
import random
import time
from datetime import UTC, datetime

from confluent_kafka import Producer

from common import topics
from common.kafka import producer_config
from common.partitioning import balanced_lot_ids
from common.schemas import LotMetadata
from simulator.emission import vehicle_token
from simulator.source import slot_ids

LOTS = 12
SLOTS = 200
OCCUPANCY_SHARE = 0.30
SLICE_S = 0.05


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat(timespec="milliseconds")


def publish_metadata(bootstrap: str) -> list[str]:
    lots = balanced_lot_ids(LOTS, topics.RAW_PARTITIONS)
    producer = Producer(producer_config(bootstrap, "loadgen-meta"))
    for i, lot in enumerate(lots):
        meta = LotMetadata(
            lot_id=lot, name=lot, personality="office", capacity=SLOTS, slot_ids=slot_ids(SLOTS),
            columns=20, latitude=12.9 + i * 0.001, longitude=77.5,
        )  # fmt: skip
        producer.produce(
            topics.LOT_METADATA, key=lot.encode(), value=meta.model_dump_json().encode()
        )
    producer.flush(30)
    return lots


def worker(
    idx: int, bootstrap: str, lots: list[str], rate: float, duration: float, out: "mp.Queue[int]"
) -> None:
    rng = random.Random(idx)
    producer = Producer(
        producer_config(bootstrap, f"loadgen-{idx}") | {"queue.buffering.max.messages": 500_000}
    )
    slots = slot_ids(SLOTS)
    occupied: dict[tuple[str, str], bool] = {}
    tokens = [vehicle_token("load", "x", n) for n in range(2000)]
    delivered = 0

    def on_delivery(err: object, _msg: object) -> None:
        nonlocal delivered
        if err is None:
            delivered += 1

    start = time.monotonic()
    sent = 0
    counter = 0
    while (elapsed := time.monotonic() - start) < duration:
        due = int(rate * (elapsed + SLICE_S)) - sent
        now = time.time()
        stamp = _iso(now)
        for _ in range(max(0, due)):
            counter += 1
            lot = lots[rng.randrange(len(lots))]
            slot = slots[rng.randrange(SLOTS)]
            event_id = f"lg{idx}-{counter}"
            if rng.random() < OCCUPANCY_SHARE:
                now_occupied = not occupied.get((lot, slot), False)
                occupied[(lot, slot)] = now_occupied
                status = "OCCUPIED" if now_occupied else "FREE"
                token = f'"{tokens[rng.randrange(2000)]}"' if now_occupied else "null"
                body = (
                    f'{{"event_id":"{event_id}","event_type":"OCCUPANCY","sensor_id":"S-{slot}",'
                    f'"lot_id":"{lot}","slot_id":"{slot}","status":"{status}","vehicle_token":{token},'
                    f'"event_ts":"{stamp}","ingest_ts":"{stamp}"}}'
                )
            else:
                body = (
                    f'{{"event_id":"{event_id}","event_type":"HEARTBEAT","sensor_id":"S-{slot}",'
                    f'"lot_id":"{lot}","slot_id":"{slot}","status":null,"vehicle_token":null,'
                    f'"event_ts":"{stamp}","ingest_ts":"{stamp}"}}'
                )
            while True:
                try:
                    producer.produce(
                        topics.RAW, key=lot.encode(), value=body.encode(), on_delivery=on_delivery
                    )
                    break
                except BufferError:
                    producer.poll(0.05)
            sent += 1
        producer.poll(0)
        time.sleep(SLICE_S / 2)
    producer.flush(60)
    out.put(delivered)


def run(rate: float, duration: float, workers: int, bootstrap: str, lots: list[str]) -> int:
    """Produce `rate` events/s for `duration` seconds; returns the number delivered."""
    queue: mp.Queue[int] = mp.Queue()
    procs = [
        mp.Process(target=worker, args=(i, bootstrap, lots, rate / workers, duration, queue))
        for i in range(workers)
    ]
    for p in procs:
        p.start()
    total = sum(queue.get() for _ in procs)
    for p in procs:
        p.join()
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, required=True)
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--workers", type=int, default=1)
    args = ap.parse_args()
    servers = os.environ.get("KAFKA_BOOTSTRAP_HOST", "localhost:19092")
    ids = publish_metadata(servers)
    delivered = run(args.rate, args.duration, args.workers, servers, ids)
    print(
        json.dumps(
            {
                "target_rate": args.rate,
                "delivered": delivered,
                "achieved_rate": delivered / args.duration,
            }
        )
    )
