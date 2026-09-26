"""Approximate distinct-vehicle counter (HyperLogLog, via datasketch).

p=10 gives 1024 one-byte registers (1 KiB) and a standard error of 1.04/sqrt(1024) = 3.25%,
independent of how many vehicles are counted."""

import base64

import numpy as np
from datasketch import HyperLogLog

PRECISION = 10
STANDARD_ERROR = 1.04 / (2**PRECISION) ** 0.5


class UniqueCounter:
    def __init__(self, registers: bytes | None = None) -> None:
        if registers is None:
            self._hll = HyperLogLog(p=PRECISION)
        else:
            self._hll = HyperLogLog(p=PRECISION, reg=np.frombuffer(registers, dtype=np.int8).copy())

    def add(self, token: str) -> None:
        self._hll.update(token.encode())

    def estimate(self) -> float:
        return float(self._hll.count())

    @property
    def memory_bytes(self) -> int:
        return int(self._hll.reg.nbytes)

    def to_b64(self) -> str:
        return base64.b64encode(self._hll.reg.tobytes()).decode()

    @classmethod
    def from_b64(cls, encoded: str) -> "UniqueCounter":
        return cls(base64.b64decode(encoded))
