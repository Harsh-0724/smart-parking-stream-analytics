from collections import Counter

from common.partitioning import balanced_lot_ids, murmur2, partition_for


def test_murmur2_matches_kafka_reference_values() -> None:
    # Reference values from Kafka's own Utils.murmur2 unit test.
    assert murmur2(b"21") == -973932308 & 0xFFFFFFFF
    assert murmur2(b"foobar") == -790332482 & 0xFFFFFFFF
    assert murmur2(b"a-little-bit-long-string") == -985981536 & 0xFFFFFFFF


def test_balanced_ids_spread_evenly() -> None:
    ids = balanced_lot_ids(12, 6)
    assert len(set(ids)) == 12
    assert set(Counter(partition_for(i, 6) for i in ids).values()) == {2}
