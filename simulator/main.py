"""Simulator / producer entry point: python -m simulator.main --help"""

import argparse
import json
import os
import random
import signal
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import FrameType

from confluent_kafka import KafkaError, Message, Producer

from common import topics
from common.kafka import producer_config
from common.logging import configure_logging
from simulator.faults import FaultConfig, FaultInjector
from simulator.faults import Message as OutMessage
from simulator.source import ReplaySource, Source, SyntheticSource
from simulator.truth import TruthTracker

TICK_S = 0.05
REPORT_EVERY_S = 5.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Smart parking event simulator (Kafka producer)")
    p.add_argument("--mode", choices=["synthetic", "replay"], default="synthetic")
    p.add_argument(
        "--bootstrap", default=None, help="default: $KAFKA_BOOTSTRAP_HOST or $KAFKA_BOOTSTRAP"
    )
    p.add_argument("--speed", type=float, default=1.0, help="event-time seconds per wall second")
    p.add_argument("--duration", type=float, default=0, help="wall seconds to run; 0 = forever")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--lots", type=int, default=int(os.environ.get("SIM_LOTS", 12)))
    p.add_argument(
        "--slots-per-lot", type=int, default=int(os.environ.get("SIM_SLOTS_PER_LOT", 60))
    )
    p.add_argument("--start", default=None, help="synthetic start, ISO-8601 UTC; default now")
    p.add_argument("--dataset", type=Path, default=Path("data/dataset.csv"))
    p.add_argument("--replay-scale", type=float, default=0.1, help="capacity scale for replay")
    p.add_argument("--replay-days", type=float, default=3.0)
    p.add_argument("--late-pct", type=float, default=0.0)
    p.add_argument("--late-max-s", type=float, default=600.0, help="max lateness of late events")
    p.add_argument("--dup-pct", type=float, default=0.0)
    p.add_argument("--malformed-pct", type=float, default=0.0)
    p.add_argument("--sensor-dropout", action="append", default=[], metavar="[LOT:]SENSOR")
    p.add_argument("--dropout-after", type=float, default=20.0, help="event-time s before dropout")
    p.add_argument("--burst", type=float, default=1.0, help="event-rate multiplier during bursts")
    p.add_argument("--burst-every", type=float, default=120.0, help="wall seconds between bursts")
    p.add_argument("--burst-len", type=float, default=20.0, help="wall seconds a burst lasts")
    p.add_argument("--summary-file", type=Path, default=None)
    p.add_argument("--truth-file", type=Path, default=None)
    p.add_argument("--window-s", type=int, default=int(os.environ.get("WINDOW_SIZE_S", 300)))
    return p.parse_args()


def build_source(args: argparse.Namespace, start_ts: float) -> Source:
    salt = os.environ.get("SIM_SALT", "dev-salt")
    if args.mode == "replay":
        if not args.dataset.exists():
            raise SystemExit(
                f"Dataset not found at {args.dataset}. Fetch "
                "https://archive.ics.uci.edu/static/public/482/parking+birmingham.zip, "
                "unzip it and place dataset.csv there (or run `make fetch-data`)."
            )
        return ReplaySource(
            args.dataset, start_ts, args.seed, salt, args.replay_scale, args.replay_days, args.lots
        )
    return SyntheticSource(args.lots, args.slots_per_lot, start_ts, args.seed, salt)


def parse_dropouts(specs: list[str]) -> frozenset[tuple[str | None, str]]:
    parsed: set[tuple[str | None, str]] = set()
    for spec in specs:
        lot, _, sensor = spec.rpartition(":")
        parsed.add((lot or None, sensor))
    return frozenset(parsed)


class Sender:
    def __init__(self, producer: Producer) -> None:
        self._p = producer
        self.by_partition: Counter[int] = Counter()
        self.errors = 0

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.errors += 1
        else:
            self.by_partition[msg.partition() or 0] += 1

    def send(self, topic: str, key: bytes, value: bytes, track: bool = True) -> None:
        cb = self._on_delivery if track else None
        while True:
            try:
                self._p.produce(topic, key=key, value=value, on_delivery=cb)
                return
            except BufferError:
                self._p.poll(0.5)

    def poll(self) -> None:
        self._p.poll(0)

    def flush(self) -> None:
        self._p.flush(30)


def burst_factor(args: argparse.Namespace, wall_elapsed: float) -> float:
    if args.burst <= 1.0:
        return 1.0
    return args.burst if wall_elapsed % args.burst_every < args.burst_len else 1.0


def main() -> None:
    args = parse_args()
    log = configure_logging("simulator")
    bootstrap = (
        args.bootstrap
        or os.environ.get("KAFKA_BOOTSTRAP_HOST")
        or os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    )
    start_ts = datetime.fromisoformat(args.start).timestamp() if args.start else time.time()
    source = build_source(args, start_ts)
    rng = random.Random(args.seed + 1)
    faults = FaultInjector(
        FaultConfig(
            late_pct=args.late_pct,
            late_max_s=args.late_max_s,
            dup_pct=args.dup_pct,
            malformed_pct=args.malformed_pct,
            dropout_sensors=parse_dropouts(args.sensor_dropout),
            dropout_after_s=args.dropout_after,
        ),
        rng,
        start_ts,
    )
    truth = TruthTracker(args.window_s) if args.truth_file else None
    sender = Sender(Producer(producer_config(bootstrap, "simulator")))

    for meta in source.lots:
        sender.send(
            topics.LOT_METADATA, meta.lot_id.encode(), meta.model_dump_json().encode(), False
        )
    sender.flush()
    log.info(
        "started", mode=args.mode, lots=len(source.lots), speed=args.speed, bootstrap=bootstrap
    )

    stop = False

    def _stop(_signum: int, _frame: FrameType | None) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    wall0 = time.monotonic()
    sim_now = start_ts
    last_report, sent_at_report = wall0, 0
    while not stop:
        elapsed = time.monotonic() - wall0
        if args.duration and elapsed >= args.duration:
            break
        sim_now = start_ts + elapsed * args.speed
        batch = sorted(source.advance(sim_now, burst_factor(args, elapsed)), key=lambda e: e.ts)
        if truth:
            truth.observe(batch)
        wall = time.time()
        out: list[OutMessage] = faults.release_due(sim_now, wall)
        for e in batch:
            out.extend(faults.process(e, wall))
        for m in out:
            sender.send(topics.RAW, m.key, m.value)
        sender.poll()
        if source.exhausted and args.mode == "replay":
            break
        if time.monotonic() - last_report >= REPORT_EVERY_S:
            total = faults.counters.total
            log.info(
                "progress",
                events_per_s=round((total - sent_at_report) / (time.monotonic() - last_report)),
                sim_time=datetime.fromtimestamp(sim_now).isoformat(timespec="seconds"),
                **faults.counters.__dict__,
            )
            last_report, sent_at_report = time.monotonic(), total
        time.sleep(max(0.0, TICK_S - (time.monotonic() - wall0 - elapsed)))

    for m in faults.flush_held(time.time()):
        sender.send(topics.RAW, m.key, m.value)
    sender.flush()
    summary = {
        "started_at": start_ts,
        "ended_sim_ts": sim_now,
        "wall_seconds": time.monotonic() - wall0,
        **faults.counters.__dict__,
        "delivery_errors": sender.errors,
        "by_partition": dict(sorted(sender.by_partition.items())),
    }
    log.info("finished", **summary)
    if args.summary_file:
        args.summary_file.write_text(json.dumps(summary, indent=1))
    if truth and args.truth_file:
        truth.write(args.truth_file, sim_now)


if __name__ == "__main__":
    main()
