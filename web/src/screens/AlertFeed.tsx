import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { useAlerts } from "../api/hooks";
import { useLive } from "../api/live";
import { fmtTime } from "../format";
import { Badge, State } from "../ui";

/** Refetches whenever the WebSocket delivers an alert, so new alerts appear within a second. */
export function useAlertRefresh() {
  const qc = useQueryClient();
  const { alertVersion } = useLive();
  useEffect(() => {
    if (alertVersion > 0) void qc.invalidateQueries({ queryKey: ["alerts"] });
  }, [alertVersion, qc]);
}

export function AlertFeed({ limit = 8 }: { limit?: number }) {
  useAlertRefresh();
  const q = useAlerts({ limit, offset: 0 });
  if (q.isPending) return <State kind="loading" title="Loading alerts" />;
  if (q.isError) return <State kind="error" title="Could not load alerts" hint="The API did not answer." onRetry={() => void q.refetch()} />;
  if (q.data.items.length === 0)
    return <State kind="empty" title="No alerts" hint="Every lot is below 90% and all sensors are reporting." />;
  return (
    <ul className="feed">
      {q.data.items.map((a) => (
        <li key={a.alert_id} className="feed__item">
          <span className="feed__time num">{fmtTime(a.raised_at)}</span>
          <div>
            <span style={{ display: "inline-flex", gap: "var(--sp-2)" }}>
              {a.kind === "FULL_LOT" ? <Badge kind="crit">Full lot</Badge> : <Badge kind="warn">Sensor offline</Badge>}
              {a.status === "cleared" && <Badge>Cleared</Badge>}
            </span>
            <div className="feed__msg">
              <Link to={`/lots/${a.lot_id}`}>{a.message}</Link>
            </div>
          </div>
        </li>
      ))}
    </ul>
  );
}
