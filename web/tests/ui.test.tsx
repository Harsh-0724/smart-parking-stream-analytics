import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { Badge, SlotGrid, Sparkline, Stat, Table, TimeSeries, type SeriesPoint } from "../src/ui";

describe("Stat", () => {
  it("renders monospaced tabular numbers with label and unit", () => {
    render(<Stat label="Occupied" value="339" unit="of 450" size="xl" />);
    const value = screen.getByText("339");
    expect(value.closest(".stat__value")).toHaveClass("num");
    expect(screen.getByText("Occupied")).toBeInTheDocument();
    expect(screen.getByText("of 450")).toBeInTheDocument();
  });
});

describe("Badge", () => {
  it("pairs colour with a shape and a text label", () => {
    const { container } = render(<Badge kind="crit">Full</Badge>);
    expect(screen.getByText("Full")).toBeInTheDocument();
    expect(container.querySelector("svg.glyph")).not.toBeNull();
    expect(container.querySelector(".badge--crit")).not.toBeNull();
  });
  it("gives every state a different shape", () => {
    const kinds = ["free", "occupied", "warn", "crit"] as const;
    const shapes = kinds.map((k) => {
      const { container, unmount } = render(<Badge kind={k}>x</Badge>);
      const html = container.querySelector("svg")!.innerHTML;
      unmount();
      return html;
    });
    expect(new Set(shapes).size).toBe(kinds.length);
  });
});

describe("Table", () => {
  it("makes the first cell a link when rows are navigable", () => {
    render(
      <MemoryRouter>
        <Table
          caption="Lots"
          columns={[{ key: "a", header: "Lot", render: (r: { id: string }) => r.id }]}
          rows={[{ id: "LOT-01" }]}
          rowKey={(r) => r.id}
          rowHref={(r) => `/lots/${r.id}`}
        />
      </MemoryRouter>,
    );
    expect(screen.getByRole("link", { name: "LOT-01" })).toHaveAttribute("href", "/lots/LOT-01");
    expect(screen.getByRole("columnheader", { name: "Lot" })).toBeInTheDocument();
  });
});

describe("Sparkline", () => {
  it("shows a designed empty state with too little data", () => {
    render(<Sparkline values={[5]} />);
    expect(screen.getByRole("img", { name: /no trend data/ })).toBeInTheDocument();
  });
  it("marks the latest point", () => {
    const { container } = render(<Sparkline values={[10, 20, 35]} />);
    expect(container.querySelector("circle")).not.toBeNull();
    expect(screen.getByRole("img")).toHaveAccessibleName(/latest 35%/);
  });
});

describe("SlotGrid", () => {
  const ids = Array.from({ length: 40 }, (_, i) => `A-${String(i + 1).padStart(3, "0")}`);

  it("draws state by fill vs outline and announces the count", () => {
    const states = new Map([
      ["A-001", true],
      ["A-002", false],
    ]);
    const { container } = render(<SlotGrid slotIds={ids} columns={20} states={states} />);
    expect(container.querySelector('[data-slot="A-001"]')).toHaveAttribute("data-status", "occupied");
    expect(container.querySelector('[data-slot="A-002"]')).toHaveAttribute("data-status", "free");
    expect(container.querySelector('[data-slot="A-003"]')).toHaveAttribute("data-status", "unknown");
    expect(screen.getByRole("img")).toHaveAccessibleName("Floor plan: 1 of 40 slots occupied");
  });

  it("re-renders only the slot that changed", () => {
    const states = new Map<string, boolean>([["A-001", false]]);
    const { container, rerender } = render(<SlotGrid slotIds={ids} columns={20} states={states} />);
    const untouched = container.querySelector('[data-slot="A-010"]');
    const changed = container.querySelector('[data-slot="A-001"]');
    states.set("A-001", true);
    rerender(<SlotGrid slotIds={ids} columns={20} states={states} />);
    expect(container.querySelector('[data-slot="A-010"]')).toBe(untouched);
    expect(container.querySelector('[data-slot="A-001"]')).toBe(changed); // same node, new status
    expect(changed).toHaveAttribute("data-status", "occupied");
  });

  it("stays fast with 600 slots", () => {
    const many = Array.from({ length: 600 }, (_, i) => `B-${String(i + 1).padStart(3, "0")}`);
    const states = new Map<string, boolean>();
    const { rerender } = render(<SlotGrid slotIds={many} columns={30} states={states} />);
    const start = performance.now();
    for (let i = 0; i < 20; i++) {
      states.set(many[i]!, true);
      rerender(<SlotGrid slotIds={many} columns={30} states={states} />);
    }
    // 20 update passes over 600 slots; generous bound so the test is stable on slow CI (jsdom)
    expect(performance.now() - start).toBeLessThan(2000);
  });
});

describe("TimeSeries", () => {
  const t0 = Date.parse("2026-01-05T10:00:00Z");
  const pts = (openLast: boolean): SeriesPoint[] =>
    Array.from({ length: 10 }, (_, i) => ({
      t: t0 + i * 300_000,
      v: 20 + i * 5,
      closed: !(openLast && i === 9),
      detail: `${20 + i * 5}% · 1 in · 1 out`,
    }));

  it("draws the open window differently from closed windows", () => {
    const { container } = render(<TimeSeries points={pts(true)} label="Average occupancy" />);
    expect(container.querySelector(".ts__line--open")).not.toBeNull();
    expect(container.querySelector(".ts__marker--open")).not.toBeNull();
    expect(screen.getByText("open window")).toBeInTheDocument();
  });
  it("has no open styling when every window is closed", () => {
    const { container } = render(<TimeSeries points={pts(false)} label="Average occupancy" />);
    expect(container.querySelector(".ts__marker--open")).toBeNull();
  });
});
