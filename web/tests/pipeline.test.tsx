import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { liveStore } from "../src/api/live";
import Pipeline from "../src/screens/Pipeline";

const stats = (rebalances: { ts: string; group_id: string; description: string }[]) => ({
  generated_at: "2026-01-05T10:00:00Z",
  error: null,
  events_per_s: 100,
  latency_p50_s: 2.5,
  latency_p95_s: 4.8,
  dlq_messages: 0,
  late_messages: 0,
  brokers: [{ id: 1, host: "kafka-1", port: 9092, leader_partitions: 6 }],
  topics: [],
  groups: [
    {
      group_id: "occupancy-processor",
      state: "Stable",
      rebalancing: false,
      members: [],
      total_lag: 0,
      partitions: [],
    },
  ],
  rebalances,
});

const push = (data: unknown) =>
  act(async () => {
    (liveStore as unknown as { handle: (m: unknown) => void }).handle({ type: "pipeline", data });
  });

describe("Pipeline rebalance indicator", () => {
  it("shows a banner for the first-ever rebalance even though the history was empty at load", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => ({ ok: true, json: async () => stats([]) })));
    render(
      <QueryClientProvider client={new QueryClient()}>
        <MemoryRouter>
          <Pipeline />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await push(stats([]));
    expect(screen.queryByText("Rebalanced")).toBeNull();

    // The rebalance completes between two polls, so the group is already Stable again
    await push(stats([{ ts: "2026-01-05T10:00:05Z", group_id: "occupancy-processor", description: "assignment changed: a holds 6" }]));
    expect(await screen.findByText("Rebalanced")).toBeInTheDocument();
    expect(document.querySelector(".banner")).toHaveTextContent(/assignment changed: a holds 6/);
    vi.unstubAllGlobals();
  });

  it("does not treat history that already existed at load as news", async () => {
    await push(stats([{ ts: "2026-01-05T09:00:00Z", group_id: "occupancy-processor", description: "old event" }]));
    render(
      <QueryClientProvider client={new QueryClient()}>
        <MemoryRouter>
          <Pipeline />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    expect(screen.queryByText("Rebalanced")).toBeNull();
    await push(stats([{ ts: "2026-01-05T09:00:00Z", group_id: "occupancy-processor", description: "old event" }]));
    expect(screen.queryByText("Rebalanced")).toBeNull(); // same event again is still not news
  });
});
