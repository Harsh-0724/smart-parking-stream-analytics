"""Daily/weekly demand curves. Each curve is a piecewise-linear target occupancy
(0..1) over local hour-of-day, chosen per lot personality and weekday/weekend."""

import math
from collections.abc import Sequence

Curve = Sequence[tuple[float, float]]

# (hour, target occupancy)
OFFICE_WEEKDAY: Curve = [
    (0, 0.04), (6, 0.05), (7, 0.20), (9.5, 0.92), (12, 0.90), (12.5, 0.74),
    (13.5, 0.76), (14.5, 0.90), (17, 0.80), (19, 0.12), (21, 0.06), (24, 0.04),
]  # fmt: skip
OFFICE_WEEKEND: Curve = [(0, 0.03), (10, 0.06), (14, 0.10), (20, 0.05), (24, 0.03)]

MALL_WEEKDAY: Curve = [
    (0, 0.02), (9, 0.04), (11, 0.35), (13, 0.55), (16, 0.50), (18, 0.78),
    (20.5, 0.72), (22, 0.15), (24, 0.03),
]  # fmt: skip
MALL_WEEKEND: Curve = [
    (0, 0.02), (9, 0.05), (11, 0.55), (14, 0.90), (19, 0.93), (21, 0.60),
    (22.5, 0.12), (24, 0.03),
]  # fmt: skip

STATION_WEEKDAY: Curve = [
    (0, 0.08), (5, 0.10), (6.5, 0.40), (9, 0.88), (16.5, 0.85), (18, 0.60),
    (20, 0.25), (24, 0.10),
]  # fmt: skip
STATION_WEEKEND: Curve = [(0, 0.08), (8, 0.15), (12, 0.32), (18, 0.30), (24, 0.10)]

CURVES: dict[str, tuple[Curve, Curve]] = {
    "office": (OFFICE_WEEKDAY, OFFICE_WEEKEND),
    "mall": (MALL_WEEKDAY, MALL_WEEKEND),
    "station": (STATION_WEEKDAY, STATION_WEEKEND),
}

# Log-normal dwell time: median seconds and sigma of the underlying normal.
DWELL: dict[str, tuple[float, float]] = {
    "office": (7.5 * 3600, 0.35),
    "mall": (1.5 * 3600, 0.60),
    "station": (5.0 * 3600, 0.50),
}


def interpolate(curve: Curve, hour: float) -> float:
    for (h0, v0), (h1, v1) in zip(curve, curve[1:], strict=False):
        if h0 <= hour <= h1:
            return v0 + (v1 - v0) * (hour - h0) / (h1 - h0)
    return curve[-1][1]


def target_occupancy(personality: str, weekday: int, hour: float) -> float:
    """weekday: Monday=0 .. Sunday=6."""
    weekday_curve, weekend_curve = CURVES[personality]
    return interpolate(weekend_curve if weekday >= 5 else weekday_curve, hour)


def mean_dwell_s(personality: str) -> float:
    median, sigma = DWELL[personality]
    return median * math.exp(sigma**2 / 2)
