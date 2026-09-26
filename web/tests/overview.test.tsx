import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { liveStore } from "../src/api/live";
import Overview from "../src/screens/Overview";

const lot = (id: string, name: string, capacity: number, occupied: number) => ({
  lot_id: id,
  name,
  personality: "office",
  capacity,
  occupied,
  occupancy_pct: (100 * occupied) / capacity,
  as_of: "2026-01-05T10:00:00Z",
  spark: [10, 20, 30],
});

const overview = {
  total_capacity: 200,
  total_occupied: 150,
  occupancy_pct: 75,
  active_alerts: 0,
  generated_at: "2026-01-05T10:00:00Z",
  lots: [lot("LOT-01", "Tech Park 1", 100, 90), lot("LOT-02", "Galleria Mall 1", 100, 60)],
};

function renderOverview() {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => ({
      ok: true,
      json: async () => (String(url).includes("/alerts") ? { total: 0, limit: 8, offset: 0, items: [] } : overview),
    })),
  );
  return render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <MemoryRouter>
        <Overview />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("Overview", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("shows the system number and ranks lots by occupancy", async () => {
    renderOverview();
    expect(await screen.findByText("150")).toBeInTheDocument();
    expect(screen.getByText("of 200")).toBeInTheDocument();
    const rows = screen.getAllByRole("row").slice(1);
    expect(rows[0]).toHaveTextContent("Tech Park 1"); // 90% before 60%
    expect(rows[0]).toHaveTextContent("Full"); // 90% is the alert threshold
    expect(rows[1]).toHaveTextContent("Galleria Mall 1");
  });

  it("updates numbers from WebSocket occupancy messages without waiting for a poll", async () => {
    renderOverview();
    await screen.findByText("150");
    const socket = { onopen: null, onclose: null, onmessage: null, send: () => {}, close: () => {} } as never;
    void socket;
    await act(async () => {
      // Feed the shared store exactly as the WebSocket handler would.
      (liveStore as unknown as { handle: (m: unknown) => void }).handle({
        type: "occupancy",
        data: { lot_id: "LOT-02", current_occupied: 95, capacity: 100, closed: false, emitted_at: "t", window_start: "t", avg_occupancy_pct: 90 },
      });
    });
    await waitFor(() => expect(screen.getByText("185")).toBeInTheDocument()); // 90 + 95
    expect(screen.getAllByText("Full")).toHaveLength(2);
  });

  it("shows a designed error state when the API is down", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => ({ ok: false, status: 502, statusText: "Bad Gateway" })));
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter>
          <Overview />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    expect(await screen.findByRole("alert")).toHaveTextContent("Cannot reach the API");
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });
});
