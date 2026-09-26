import { useEffect } from "react";
import { Link, useParams } from "react-router-dom";
import { useHistory, useLotDetail } from "../api/hooks";
import { liveStore, useLive } from "../api/live";
import { fmtDateTime, fmtInt, fmtPct } from "../format";
import { Badge, Glyph, SlotGrid, Stat, State, TimeSeries, type SeriesPoint } from "../ui";
import { lotState } from "./status";

const Kv = ({ k, v, hint }: { k: string; v: string; hint?: string }) => (
  <div className="kv" title={hint}>
    <span className="kv__k">{k}</span>
    <span className="num">{v}</span>
  </div>
);

export default function LotDetail() {
  const { lotId = "" } = useParams();
  const detail = useLotDetail(lotId);
  const history = useHistory(lotId);
  const live = useLive();

  useEffect(() => {
    liveStore.subscribeLots([lotId]);
    return () => liveStore.subscribeLots([]);
  }, [lotId]);

  if (detail.isPending) return <State kind="loading" title="Loading lot" />;
  if (detail.isError)
    return <State kind="error" title={`Lot ${lotId} not found`} hint="It may not have reported yet." onRetry={() => void detail.refetch()} />;

  const d = detail.data;
  const states = liveStore.slots(lotId);
  void live.slotVersion; // re-render when the slot map changed (one pass per animation frame)
  let occupied = 0;
  for (const id of d.slot_ids) if (states.get(id) === true) occupied++;
  const known = states.size > 0;
  const freshOcc = live.occupancy.get(lotId)?.current_occupied;
  const shown = known ? occupied : (freshOcc ?? d.occupied);
  const pct = shown == null ? null : Math.round((1000 * shown) / d.capacity) / 10;
  const s = lotState(pct);

  const points: SeriesPoint[] = (history.data?.points ?? []).map((p) => ({
    t: new Date(p.window_start).getTime(),
    v: p.avg_occupancy_pct,
    closed: p.closed,
    detail: `${p.avg_occupancy_pct.toFixed(1)}% · ${p.entries} in · ${p.exits} out`,
  }));

  return (
    <>
      <div className="page-head">
        <Link to="/" className="crumb">
          Overview /
        </Link>
        <h1>{d.name}</h1>
        <span className="muted num">{d.lot_id}</span>
        <Badge kind={s.kind}>{s.label}</Badge>
        {d.full_since && (
          <span className="muted num" title="Full-lot alerts raise at 90% and clear at 85%">
            Full-lot alert active since {fmtDateTime(d.full_since)}
          </span>
        )}
      </div>

      <div className="cols">
        <div className="stack">
          <section aria-label="Floor plan">
            <div className="section__head">
              <h2>Floor plan, live</h2>
              <span className="aside num">{known ? `${occupied} of ${d.capacity} occupied` : "waiting for snapshot"}</span>
            </div>
            <div className="plan">
              <SlotGrid slotIds={d.slot_ids} columns={d.columns} states={states} size={d.slot_ids.length > 150 ? 16 : 22} />
            </div>
            <div className="legend">
              <span className="legend__item"><Glyph shape="square" /> Occupied</span>
              <span className="legend__item"><Glyph shape="square-outline" /> Free</span>
              <span className="legend__item muted">Dashed: no report yet</span>
            </div>
          </section>

          <section aria-label="Occupancy history">
            <div className="section__head">
              <h2>Occupancy, last 24 h</h2>
              <span className="aside">5-minute windows</span>
            </div>
            {history.isPending ? (
              <State kind="loading" title="Loading history" />
            ) : points.length < 2 ? (
              <State kind="empty" title="Not enough history yet" hint="The chart appears after two windows have closed." />
            ) : (
              <TimeSeries points={points} label="Average occupancy" />
            )}
          </section>
        </div>

        <aside aria-label="Lot statistics">
          <Stat size="md" label="Occupied now" value={fmtInt(shown)} unit={`of ${fmtInt(d.capacity)}`} sub={fmtPct(pct, 1)} />
          <div style={{ marginTop: "var(--sp-5)" }}>
            <Kv k="Entries, 24 h" v={fmtInt(d.entries_24h)} />
            <Kv k="Exits, 24 h" v={fmtInt(d.exits_24h)} />
            <Kv k="Vehicles, last closed window (approx.)" v={d.unique_vehicles_est == null ? "-" : `~${fmtInt(d.unique_vehicles_est)}`} hint="HyperLogLog estimate of distinct vehicles in the newest closed 5-minute window, about 3% standard error" />
            <Kv k="Avg dwell (estimate)" v={d.dwell_minutes_est == null ? "-" : `${d.dwell_minutes_est} min`} hint="Little's law: occupied time divided by entries over 24 h" />
            <Kv k="Type" v={d.personality} />
          </div>
        </aside>
      </div>
    </>
  );
}
