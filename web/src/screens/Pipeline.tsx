import { useEffect, useRef, useState } from "react";
import { usePipelineRest } from "../api/hooks";
import { useLive } from "../api/live";
import type { BrokerInfo, ConsumerGroupInfo, PipelineStats, RebalanceEvent } from "../api/types";
import { fmtInt, fmtRate, fmtSeconds, fmtTime } from "../format";
import { Badge, Stat, State, Table, type Column } from "../ui";

const RAW = "parking.raw";

/** Remember every broker ever seen, so one that disappears from metadata shows as Down. */
function useBrokerRows(brokers: BrokerInfo[] | undefined) {
  const seen = useRef(new Map<number, BrokerInfo>());
  const downSince = useRef(new Map<number, number>());
  const rows: { broker: BrokerInfo; up: boolean; since?: number }[] = [];
  if (brokers) {
    const upIds = new Set(brokers.map((b) => b.id));
    brokers.forEach((b) => {
      seen.current.set(b.id, b);
      downSince.current.delete(b.id);
    });
    seen.current.forEach((_b, id) => {
      if (!upIds.has(id) && !downSince.current.has(id)) downSince.current.set(id, Date.now());
    });
    [...seen.current.values()]
      .sort((a, b) => a.id - b.id)
      .forEach((b) => rows.push({ broker: b, up: upIds.has(b.id), since: downSince.current.get(b.id) }));
  }
  return rows;
}

function PartitionMap({ group }: { group: ConsumerGroupInfo }) {
  const cells = group.partitions.filter((p) => p.topic === RAW);
  const previous = useRef(new Map<number, string | null>());
  const [moved, setMoved] = useState<Set<number>>(new Set());

  useEffect(() => {
    const changed = new Set<number>();
    cells.forEach((c) => {
      if (previous.current.has(c.partition) && previous.current.get(c.partition) !== c.member) changed.add(c.partition);
      previous.current.set(c.partition, c.member);
    });
    if (changed.size === 0) return;
    setMoved(changed);
    const t = setTimeout(() => setMoved(new Set()), 2500);
    return () => clearTimeout(t);
  }, [cells]);

  if (cells.length === 0) return null;
  return (
    <div className="pmap" aria-label={`${RAW} partitions and their owners`}>
      {cells.map((c) => (
        <div key={c.partition} className={`pmap__cell${moved.has(c.partition) ? " is-moved" : ""}`}>
          <div className="pmap__p num">partition {c.partition}</div>
          <div className={`pmap__owner num${c.member ? "" : " pmap__owner--none"}`}>{c.member ? c.member.slice(0, 8) : "unassigned"}</div>
        </div>
      ))}
    </div>
  );
}

function Group({ group }: { group: ConsumerGroupInfo }) {
  const maxLag = Math.max(1, ...group.partitions.map((p) => p.lag));
  const partitionCols: Column<ConsumerGroupInfo["partitions"][number]>[] = [
    { key: "tp", header: "Partition", mono: true, render: (p) => `${p.topic}[${p.partition}]` },
    { key: "owner", header: "Owner", mono: true, render: (p) => p.member?.slice(0, 8) ?? "-" },
    { key: "c", header: "Committed", align: "right", mono: true, render: (p) => fmtInt(p.committed) },
    { key: "e", header: "End", align: "right", mono: true, render: (p) => fmtInt(p.end_offset) },
    {
      key: "lag",
      header: "Lag",
      align: "right",
      mono: true,
      render: (p) => (
        <>
          <span className="lagbar" style={{ width: `${Math.round((p.lag / maxLag) * 48)}px` }} />
          {fmtInt(p.lag)}
        </>
      ),
    },
  ];
  return (
    <section className="group" aria-label={`Consumer group ${group.group_id}`}>
      <div className="section__head group__head">
        <h2 className="num" style={{ color: "var(--text)" }}>
          {group.group_id}
        </h2>
        {group.rebalancing ? <Badge kind="warn">{group.state === "PreparingRebalance" ? "Rebalancing" : "Completing rebalance"}</Badge> : <Badge>{group.state}</Badge>}
        <span className="aside num">
          {group.members.length} member{group.members.length === 1 ? "" : "s"} · lag {fmtInt(group.total_lag)}
        </span>
      </div>
      <PartitionMap group={group} />
      <Table caption={`${group.group_id} partitions`} columns={partitionCols} rows={group.partitions} rowKey={(p) => `${p.topic}-${p.partition}`} />
    </section>
  );
}

const RECENT_REBALANCE_MS = 15_000;

/** A rebalance can finish faster than the 2 s poll, so keep the newest one visible for a while. */
function useRecentRebalance(latest: RebalanceEvent | undefined, loaded: boolean) {
  const initialised = useRef(false);
  const seen = useRef<string | null>(null);
  const [recent, setRecent] = useState<RebalanceEvent | null>(null);
  const ts = latest?.ts ?? null;
  useEffect(() => {
    if (!loaded) return;
    if (!initialised.current) {
      // Whatever history exists when the screen first has data (possibly none) is not news.
      initialised.current = true;
      seen.current = ts;
      return;
    }
    if (!latest || ts === seen.current) return;
    seen.current = ts;
    setRecent(latest);
  }, [loaded, latest, ts]);
  useEffect(() => {
    if (!recent) return;
    const t = setTimeout(() => setRecent(null), RECENT_REBALANCE_MS);
    return () => clearTimeout(t);
  }, [recent]);
  return recent;
}

export default function Pipeline() {
  const live = useLive();
  const rest = usePipelineRest();
  const p: PipelineStats | null = live.pipeline ?? rest.data ?? null;
  const brokerRows = useBrokerRows(p?.brokers);
  const recentRebalance = useRecentRebalance(p?.rebalances[0], p !== null);

  if (!p) {
    return rest.isError ? (
      <State kind="error" title="Pipeline stats unavailable" hint="The API could not reach Kafka." onRetry={() => void rest.refetch()} />
    ) : (
      <State kind="loading" title="Reading the pipeline" />
    );
  }

  const rebalancing = p.groups.filter((g) => g.rebalancing);
  const raw = p.topics.find((t) => t.name === RAW);
  return (
    <>
      <div className="page-head">
        <h1>Pipeline</h1>
        <span className="page-head__meta num">Read from Kafka at {fmtTime(p.generated_at)}</span>
      </div>

      {p.error && <State kind="error" title="Kafka is not answering" hint={p.error} />}
      {(rebalancing.length > 0 || recentRebalance) && (
        <div className="banner" role="status">
          <Badge kind="warn">{rebalancing.length > 0 ? "Rebalancing" : "Rebalanced"}</Badge>
          <span>
            {rebalancing.length > 0
              ? `${rebalancing.map((g) => g.group_id).join(", ")}: partitions are being reassigned. Consumers pause briefly, then resume from their checkpoints.`
              : `${recentRebalance?.group_id} at ${fmtTime(recentRebalance?.ts)}: ${recentRebalance?.description}`}
          </span>
        </div>
      )}

      <div className="strip">
        <Stat size="md" label="Events / s (parking.raw)" value={fmtRate(p.events_per_s)} />
        <Stat size="md" label="Windowed-emit p50" value={fmtSeconds(p.latency_p50_s)} />
        <Stat size="md" label="Windowed-emit p95" value={fmtSeconds(p.latency_p95_s)} />
        <Stat size="md" label="Dead-lettered" value={fmtInt(p.dlq_messages)} unit="msgs" sub="malformed, in parking.dlq" />
        <Stat size="md" label="Late" value={fmtInt(p.late_messages)} unit="msgs" sub="past grace, in parking.late" />
      </div>

      <div className="cols cols--wide">
        <div>
          <div className="section__head">
            <h2>Consumer groups</h2>
          </div>
          {p.groups.length === 0 ? <State kind="empty" title="No consumer groups" hint="Start the processor, sink and alerter." /> : p.groups.map((g) => <Group key={g.group_id} group={g} />)}
        </div>

        <aside>
          <section className="section" aria-label="Brokers">
            <div className="section__head">
              <h2>Brokers</h2>
            </div>
            <Table
              caption="Kafka brokers"
              rowKey={(r) => String(r.broker.id)}
              rows={brokerRows}
              columns={[
                { key: "b", header: "Broker", mono: true, render: (r) => `kafka-${r.broker.id}` },
                { key: "l", header: "Leads", align: "right", mono: true, render: (r) => (r.up ? `${r.broker.leader_partitions} partitions` : "-") },
                {
                  key: "s",
                  header: "State",
                  render: (r) => (r.up ? <Badge kind="free">Up</Badge> : <Badge kind="crit">Down {r.since ? `since ${fmtTime(r.since)}` : ""}</Badge>),
                },
              ]}
            />
          </section>

          <section className="section" aria-label="Topics">
            <div className="section__head">
              <h2>Topics</h2>
            </div>
            <Table
              caption="Kafka topics"
              rowKey={(t) => t.name}
              rows={p.topics}
              columns={[
                { key: "n", header: "Topic", mono: true, render: (t) => t.name },
                { key: "p", header: "Parts", align: "right", mono: true, render: (t) => t.partitions.length },
                { key: "m", header: "Messages", align: "right", mono: true, render: (t) => fmtInt(t.total_messages) },
                {
                  key: "i",
                  header: "ISR",
                  render: (t) => {
                    const short = t.partitions.filter((x) => x.isr.length < x.replicas.length).length;
                    return short ? <Badge kind="warn">{short} degraded</Badge> : <Badge kind="free">full</Badge>;
                  },
                },
              ]}
            />
            {raw && <p className="muted num" style={{ marginTop: "var(--sp-2)" }}>{raw.name}: keyed by lot_id, {raw.partitions.length} partitions</p>}
          </section>

          <section className="section" aria-label="Rebalance log">
            <div className="section__head">
              <h2>Rebalance log</h2>
            </div>
            {p.rebalances.length === 0 ? (
              <State kind="empty" title="No rebalances yet" hint="Kill a processor or add one to see partitions move." />
            ) : (
              <ul className="feed">
                {p.rebalances.map((r) => (
                  <li key={`${r.ts}-${r.group_id}`} className="feed__item">
                    <span className="feed__time num">{fmtTime(r.ts)}</span>
                    <div>
                      <span className="num">{r.group_id}</span>
                      <div>{r.description}</div>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </section>
        </aside>
      </div>
    </>
  );
}
