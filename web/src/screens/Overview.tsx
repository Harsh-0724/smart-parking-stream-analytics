import { Link } from "react-router-dom";
import { useOverview } from "../api/hooks";
import { useLive } from "../api/live";
import type { LotSummary } from "../api/types";
import { fmtInt, fmtPct, fmtRate, fmtSeconds, fmtTime } from "../format";
import { Badge, OccupancyBar, Sparkline, Stat, State, Table, type Column } from "../ui";
import { AlertFeed } from "./AlertFeed";
import { lotState } from "./status";

const PERSONALITY = { office: "Office", mall: "Mall", station: "Station" } as const;

export default function Overview() {
  const q = useOverview();
  const live = useLive();

  if (q.isPending) return <State kind="loading" title="Loading overview" />;
  if (q.isError)
    return <State kind="error" title="Cannot reach the API" hint="Check that the gateway is running." onRetry={() => void q.refetch()} />;

  // Numbers pushed over the WebSocket are fresher than the last poll.
  const lots: LotSummary[] = q.data.lots
    .map((l) => {
      const fresh = live.occupancy.get(l.lot_id);
      const occupied = fresh?.current_occupied ?? l.occupied;
      return { ...l, occupied, occupancy_pct: occupied == null ? null : Math.round((1000 * occupied) / l.capacity) / 10 };
    })
    .sort((a, b) => (b.occupancy_pct ?? -1) - (a.occupancy_pct ?? -1));
  const capacity = lots.reduce((s, l) => s + l.capacity, 0);
  const occupied = lots.reduce((s, l) => s + (l.occupied ?? 0), 0);
  const pct = capacity ? (100 * occupied) / capacity : 0;

  const p = live.pipeline;
  const processor = p?.groups.find((g) => g.group_id === "occupancy-processor");
  const brokersUp = p?.brokers.length;
  const rebalancing = p?.groups.some((g) => g.rebalancing);

  const columns: Column<LotSummary>[] = [
    {
      key: "lot",
      header: "Lot",
      render: (l) => (
        <span className="lotcell">
          <span>{l.name}</span>
          <span className="lotcell__id num">{l.lot_id}</span>
        </span>
      ),
    },
    { key: "type", header: "Type", render: (l) => PERSONALITY[l.personality] },
    {
      key: "occ",
      header: "Occupancy",
      width: "22%",
      render: (l) => (
        <span className="barcell">
          <OccupancyBar pct={l.occupancy_pct} />
          <span className="num right">{fmtPct(l.occupancy_pct)}</span>
        </span>
      ),
    },
    { key: "trend", header: "Last 2 h", render: (l) => <Sparkline values={l.spark} label={`${l.name} last two hours`} /> },
    { key: "free", header: "Free / capacity", align: "right", mono: true, render: (l) => `${fmtInt(l.occupied == null ? null : l.capacity - l.occupied)} / ${fmtInt(l.capacity)}` },
    {
      key: "state",
      header: "State",
      render: (l) => {
        const s = lotState(l.occupancy_pct);
        return <Badge kind={s.kind}>{s.label}</Badge>;
      },
    },
  ];

  return (
    <>
      <div className="page-head">
        <h1>Overview</h1>
        <span className="page-head__meta num">Data as of {fmtTime(q.data.generated_at)}</span>
      </div>

      <div className="row" style={{ marginBottom: "var(--sp-5)" }}>
        <Stat size="xl" label="Occupied, all lots" value={fmtInt(occupied)} unit={`of ${fmtInt(capacity)}`} sub={`${fmtPct(pct, 1)} full · ${lots.length} lots`} />
      </div>

      <div className="strip" aria-label="Pipeline heartbeat">
        <Stat size="sm" label="Events / s" value={fmtRate(p?.events_per_s)} />
        <Stat size="sm" label="Windowed-emit p95" value={fmtSeconds(p?.latency_p95_s)} title="Time from a sensor event being produced to the 5-second window emit that reflects it. Includes the wait for the next emit; raw ingestion latency (produce to consume) is tens of milliseconds and is measured separately in Prometheus." />
        <Stat size="sm" label="Committed lag" value={fmtInt(processor?.total_lag)} unit="msgs" title="Offsets are committed at 10 s checkpoints, so this rises and falls in a sawtooth; it is not the true backlog" />
        <Stat size="sm" label="Brokers up" value={brokersUp == null ? "-" : `${brokersUp}`} unit="of 3" />
        <Stat size="sm" label="Consumer group" value={rebalancing ? "Rebalancing" : processor?.state ?? "-"} />
      </div>

      <div className="cols">
        <section aria-label="Lots">
          <div className="section__head">
            <h2>Lots, most occupied first</h2>
          </div>
          <Table caption="Lots ranked by occupancy" columns={columns} rows={lots} rowKey={(l) => l.lot_id} rowHref={(l) => `/lots/${l.lot_id}`} />
        </section>
        <aside aria-label="Alerts">
          <div className="section__head">
            <h2>Alerts</h2>
            <Link to="/alerts" className="aside">
              All alerts
            </Link>
          </div>
          <AlertFeed />
        </aside>
      </div>
    </>
  );
}
