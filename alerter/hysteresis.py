"""Full-lot alert logic with hysteresis (pure, no Kafka).

Raise when occupancy reaches `raise_at`; clear only when it falls to `clear_at` or below.
Between the two thresholds nothing changes, so a lot hovering around 90% cannot flap."""

from dataclasses import dataclass

from common.schemas import Alert, AlertKind, AlertState, WindowResult


@dataclass
class _LotAlertState:
    active_alert_id: str | None = None
    last_emitted_ts: float = 0.0


class FullLotDetector:
    def __init__(self, raise_at: float, clear_at: float) -> None:
        if not 0 < clear_at < raise_at <= 1:
            raise ValueError("need 0 < clear_at < raise_at <= 1")
        self._raise_at = raise_at
        self._clear_at = clear_at
        self._lots: dict[str, _LotAlertState] = {}

    def restore(self, history: list[Alert]) -> None:
        """Rebuild the active set from past FULL_LOT alerts (oldest first)."""
        for alert in history:
            if alert.kind is not AlertKind.FULL_LOT:
                continue
            state = self._lots.setdefault(alert.lot_id, _LotAlertState())
            state.active_alert_id = alert.alert_id if alert.state is AlertState.RAISED else None

    def is_active(self, lot_id: str) -> bool:
        state = self._lots.get(lot_id)
        return state is not None and state.active_alert_id is not None

    def observe(self, result: WindowResult) -> Alert | None:
        """Feed an open-window result; returns an alert if the lot crossed a threshold."""
        if result.closed or result.current_occupied is None:
            return None  # only open windows carry the live count
        state = self._lots.setdefault(result.lot_id, _LotAlertState())
        ts = result.emitted_at.timestamp()
        if ts < state.last_emitted_ts:
            return None  # stale: another window's result from before one we already handled
        state.last_emitted_ts = ts

        fraction = result.current_occupied / result.capacity
        pct = round(100 * fraction, 1)
        if state.active_alert_id is None and fraction >= self._raise_at:
            state.active_alert_id = f"{result.lot_id}|FULL_LOT|{int(ts)}"
            return self._alert(state.active_alert_id, AlertState.RAISED, result, pct)
        if state.active_alert_id is not None and fraction <= self._clear_at:
            alert_id, state.active_alert_id = state.active_alert_id, None
            return self._alert(alert_id, AlertState.CLEARED, result, pct)
        return None

    @staticmethod
    def _alert(alert_id: str, state: AlertState, r: WindowResult, pct: float) -> Alert:
        occupied = f"{r.current_occupied}/{r.capacity} slots"
        message = (
            f"Lot {r.lot_id} full: {pct:.0f}% ({occupied})"
            if state is AlertState.RAISED
            else f"Lot {r.lot_id} has space again: {pct:.0f}% ({occupied})"
        )
        return Alert(
            alert_id=alert_id,
            kind=AlertKind.FULL_LOT,
            state=state,
            lot_id=r.lot_id,
            ts=r.emitted_at,
            message=message,
            occupancy_pct=pct,
        )
