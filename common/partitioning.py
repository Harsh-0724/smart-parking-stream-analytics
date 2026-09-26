"""Kafka default (Java) key partitioner, reimplemented to choose evenly-hashing lot ids."""

MURMUR_SEED = 0x9747B28C
MURMUR_M = 0x5BD1E995
MASK32 = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    h = (MURMUR_SEED ^ len(data)) & MASK32
    for i in range(len(data) // 4):
        k = int.from_bytes(data[i * 4 : i * 4 + 4], "little")
        k = (k * MURMUR_M) & MASK32
        k ^= k >> 24
        k = (k * MURMUR_M) & MASK32
        h = ((h * MURMUR_M) & MASK32) ^ k
    tail = len(data) % 4
    offset = len(data) - tail
    if tail == 3:
        h ^= data[offset + 2] << 16
    if tail >= 2:
        h ^= data[offset + 1] << 8
    if tail >= 1:
        h ^= data[offset]
        h = (h * MURMUR_M) & MASK32
    h ^= h >> 13
    h = (h * MURMUR_M) & MASK32
    h ^= h >> 15
    return h


def partition_for(key: str, partitions: int) -> int:
    return (murmur2(key.encode()) & 0x7FFFFFFF) % partitions


def balanced_lot_ids(count: int, partitions: int, prefix: str = "LOT-") -> list[str]:
    """First `count` ids of the form LOT-NN that fill every partition as evenly as possible.

    With few keys, plain LOT-01..LOT-12 hash unevenly (one partition gets 4 lots, another 0), which
    would make the partitioning demo look broken. Real deployments accept this skew."""
    quota = -(-count // partitions)
    used = [0] * partitions
    ids: list[str] = []
    for n in range(1, 1000):
        lot = f"{prefix}{n:02d}"
        p = partition_for(lot, partitions)
        if used[p] < quota:
            used[p] += 1
            ids.append(lot)
            if len(ids) == count:
                return ids
    raise ValueError("not enough candidate ids")
