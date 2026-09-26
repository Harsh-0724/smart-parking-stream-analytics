"""Lot metadata (capacity, layout) from the compacted `lot.metadata` topic."""

from common import topics
from common.kafka import read_to_end
from common.schemas import LotMetadata


def load_metadata(bootstrap: str) -> dict[str, LotMetadata]:
    lots: dict[str, LotMetadata] = {}
    for key, value, _ in read_to_end(bootstrap, topics.LOT_METADATA, 0):
        if key is not None and value is not None:
            lots[key.decode()] = LotMetadata.model_validate_json(value)
    return lots
