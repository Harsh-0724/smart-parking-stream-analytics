"""HyperLogLog estimate vs exact distinct count: error and memory across cardinalities.

make hll-accuracy       (appends a table to docs/RESULTS.md)
"""

import hashlib
import sys
import tracemalloc
from datetime import UTC, datetime

from processor.hll import PRECISION, STANDARD_ERROR, UniqueCounter
from scripts.chaoslib import RESULTS

CARDINALITIES = (100, 1_000, 10_000, 100_000, 1_000_000)
TRIALS = {100: 20, 1_000: 20, 10_000: 10, 100_000: 5, 1_000_000: 2}


def token(trial: int, i: int) -> str:
    return hashlib.sha256(f"trial{trial}-vehicle{i}".encode()).hexdigest()[:16]


def measure(n: int, trial: int) -> tuple[float, int]:
    """(relative error, bytes used by an exact set of the same tokens)."""
    hll = UniqueCounter()
    tracemalloc.start()
    exact: set[str] = set()
    for i in range(n):
        t = token(trial, i)
        exact.add(t)
        hll.add(t)
    used, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return abs(hll.estimate() - len(exact)) / len(exact), used


def main() -> int:
    rows = []
    for n in CARDINALITIES:
        errors, exact_bytes = [], 0
        for trial in range(TRIALS[n]):
            err, used = measure(n, trial)
            errors.append(err)
            exact_bytes = used
        mean, worst = 100 * sum(errors) / len(errors), 100 * max(errors)
        rows.append((n, TRIALS[n], mean, worst, exact_bytes))
        print(
            f"n={n:>9,}  trials={TRIALS[n]:>2}  mean err {mean:5.2f}%  max err {worst:5.2f}%  exact set {exact_bytes / 1024:>9,.0f} KiB"
        )

    ok = all(worst < 3 * 100 * STANDARD_ERROR for _, _, _, worst, _ in rows)
    lines = [
        f"\n### HLL estimate vs exact distinct count: {'PASS' if ok else 'FAIL'} "
        f"({datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')})",
        f"HyperLogLog p={PRECISION}: 1,024 bytes of registers, theoretical standard error "
        f"{100 * STANDARD_ERROR:.2f}%. Expectation: worst error under 3 standard errors "
        f"({300 * STANDARD_ERROR:.2f}%).",
        "",
        "| Distinct vehicles | Trials | Mean error | Max error | Exact set memory | HLL memory |",
        "|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {n:,} | {t} | {mean:.2f}% | {worst:.2f}% | {mem / 1024:,.0f} KiB | 1 KiB |"
        for n, t, mean, worst, mem in rows
    ]
    RESULTS.write_text(RESULTS.read_text().rstrip("\n") + "\n" + "\n".join(lines) + "\n")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
