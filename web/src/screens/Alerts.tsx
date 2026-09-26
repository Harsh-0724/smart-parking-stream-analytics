import { Link, useSearchParams } from "react-router-dom";
import { useAlerts } from "../api/hooks";
import type { AlertRow } from "../api/types";
import { fmtDateTime, fmtDuration } from "../format";
import { Badge, Button, State, Table, type Column } from "../ui";
import { useAlertRefresh } from "./AlertFeed";

const PAGE = 25;
const KINDS = [
  ["", "All"],
  ["FULL_LOT", "Full lot"],
  ["SENSOR_OFFLINE", "Sensor offline"],
] as const;
const STATUSES = [
  ["", "All"],
  ["active", "Active"],
  ["cleared", "Cleared"],
] as const;

const columns: Column<AlertRow>[] = [
  { key: "raised", header: "Raised", mono: true, render: (a) => fmtDateTime(a.raised_at) },
  { key: "lot", header: "Lot", mono: true, render: (a) => <Link to={`/lots/${a.lot_id}`}>{a.lot_id}</Link> },
  {
    key: "type",
    header: "Type",
    render: (a) => (a.kind === "FULL_LOT" ? <Badge kind="crit">Full lot</Badge> : <Badge kind="warn">Sensor offline</Badge>),
  },
  { key: "msg", header: "Detail", render: (a) => a.message },
  { key: "dur", header: "Duration", align: "right", mono: true, render: (a) => fmtDuration(a.duration_s) },
  { key: "status", header: "Status", render: (a) => (a.status === "active" ? <Badge kind="accent">Active</Badge> : <Badge>Cleared</Badge>) },
];

export default function Alerts() {
  useAlertRefresh();
  const [params, setParams] = useSearchParams();
  const kind = params.get("kind") ?? "";
  const status = params.get("status") ?? "";
  const offset = Number(params.get("offset") ?? 0);
  const q = useAlerts({ kind: kind || undefined, status: status || undefined, limit: PAGE, offset });

  const set = (key: string, value: string) => {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value);
    else next.delete(key);
    if (key !== "offset") next.delete("offset");
    setParams(next);
  };

  return (
    <>
      <div className="page-head">
        <h1>Alerts</h1>
        {q.data && <span className="page-head__meta num">{q.data.total} matching</span>}
      </div>
      <div className="filters">
        <div role="group" aria-label="Type">
          <span className="filters__label">Type</span>
          <span className="seg">
            {KINDS.map(([v, label]) => (
              <Button key={v} pressed={kind === v} onClick={() => set("kind", v)}>
                {label}
              </Button>
            ))}
          </span>
        </div>
        <div role="group" aria-label="Status">
          <span className="filters__label">Status</span>
          <span className="seg">
            {STATUSES.map(([v, label]) => (
              <Button key={v} pressed={status === v} onClick={() => set("status", v)}>
                {label}
              </Button>
            ))}
          </span>
        </div>
      </div>
      {q.isPending ? (
        <State kind="loading" title="Loading alerts" />
      ) : q.isError ? (
        <State kind="error" title="Could not load alerts" onRetry={() => void q.refetch()} />
      ) : q.data.items.length === 0 ? (
        <State kind="empty" title="No alerts match" hint="Clear a filter, or wait: full-lot alerts appear when a lot reaches 90%." />
      ) : (
        <>
          <Table caption="Alerts" columns={columns} rows={q.data.items} rowKey={(a) => a.alert_id} />
          <div className="pager num">
            <Button disabled={offset === 0} onClick={() => set("offset", String(Math.max(0, offset - PAGE)))}>
              Previous
            </Button>
            <span>
              {offset + 1}-{Math.min(offset + PAGE, q.data.total)} of {q.data.total}
            </span>
            <Button disabled={offset + PAGE >= q.data.total} onClick={() => set("offset", String(offset + PAGE))}>
              Next
            </Button>
          </div>
        </>
      )}
    </>
  );
}
