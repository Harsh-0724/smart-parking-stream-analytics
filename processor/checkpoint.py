"""Processor state <-> the compacted `state.changelog` topic.

Changelog partition N holds the state of the lots that live on `parking.raw` partition N:
messages are keyed by lot_id and written to the same partition number as the raw events."""

import json
from typing import Any

from common import topics
from common.kafka import read_to_end
from processor.lot_state import LotState, Params


def serialise(state: LotState) -> bytes:
    return json.dumps(state.to_dict(), separators=(",", ":")).encode()


def restore_partition(bootstrap: str, partition: int, params: Params) -> dict[str, LotState]:
    """Latest checkpoint of every lot in `partition`, by replaying the compacted changelog."""
    latest: dict[str, dict[str, Any]] = {}
    for key, value, _ in read_to_end(bootstrap, topics.CHANGELOG, partition):
        if key is None:
            continue
        if value is None:
            latest.pop(key.decode(), None)  # tombstone
        else:
            latest[key.decode()] = json.loads(value)
    return {lot: LotState.from_dict(d, params) for lot, d in latest.items()}
