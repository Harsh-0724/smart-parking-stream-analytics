import { LiveStore, backoffDelay, type SocketLike } from "../src/api/live";

class FakeSocket implements SocketLike {
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  sent: string[] = [];
  send(d: string) {
    this.sent.push(d);
  }
  close() {
    this.onclose?.();
  }
  open() {
    this.onopen?.();
  }
  push(m: unknown) {
    this.onmessage?.({ data: JSON.stringify(m) });
  }
}

function setup() {
  const sockets: FakeSocket[] = [];
  const frames: (() => void)[] = [];
  let now = 1000;
  const store = new LiveStore(
    () => {
      const s = new FakeSocket();
      sockets.push(s);
      return s;
    },
    () => now,
    (cb) => frames.push(cb),
  );
  return { store, sockets, frames, advance: (ms: number) => (now += ms) };
}

describe("LiveStore", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("goes live on open and sends the subscription", () => {
    const { store, sockets } = setup();
    store.subscribeLots(["LOT-01"]);
    store.start();
    expect(store.getSnapshot().status).toBe("connecting");
    sockets[0]!.open();
    expect(store.getSnapshot().status).toBe("live");
    expect(JSON.parse(sockets[0]!.sent[0]!)).toEqual({ type: "subscribe", lots: ["LOT-01"] });
    store.stop();
  });

  it("shows reconnecting, backs off, reconnects and resubscribes", () => {
    const { store, sockets } = setup();
    store.subscribeLots(["LOT-02"]);
    store.start();
    sockets[0]!.open();
    sockets[0]!.close();
    expect(store.getSnapshot().status).toBe("reconnecting");
    expect(sockets).toHaveLength(1);
    vi.advanceTimersByTime(1000);
    expect(sockets).toHaveLength(2);
    sockets[1]!.open();
    expect(store.getSnapshot().status).toBe("live");
    expect(JSON.parse(sockets[1]!.sent[0]!).lots).toEqual(["LOT-02"]);
    store.stop();
  });

  it("backs off exponentially up to a cap", () => {
    const d = (n: number) => backoffDelay(n, 0.5);
    expect(d(0)).toBe(500);
    expect(d(1)).toBe(1000);
    expect(d(2)).toBe(2000);
    expect(d(10)).toBe(10_000);
  });

  it("batches slot deltas into one notification per animation frame", () => {
    const { store, sockets, frames } = setup();
    store.start();
    sockets[0]!.open();
    let notified = 0;
    store.subscribe(() => notified++);
    sockets[0]!.push({ type: "snapshot", lot_id: "L", slots: [{ slot_id: "A-1", occupied: false, ts: "t" }] });
    for (let i = 0; i < 50; i++) sockets[0]!.push({ type: "slots", lot_id: "L", deltas: [{ slot_id: "A-1", occupied: i % 2 === 0, ts: "t" }] });
    const before = store.getSnapshot().slotVersion;
    expect(frames).toHaveLength(1); // 51 messages, a single scheduled frame
    frames[0]!();
    expect(store.getSnapshot().slotVersion).toBe(before + 1);
    expect(store.slots("L").get("A-1")).toBe(false); // last delta (i = 49) wins
    expect(notified).toBeGreaterThan(0);
    store.stop();
  });

  it("keeps the latest occupancy per lot and a bounded alert list", () => {
    const { store, sockets } = setup();
    store.start();
    sockets[0]!.open();
    sockets[0]!.push({ type: "occupancy", data: { lot_id: "L", current_occupied: 7, capacity: 10 } });
    sockets[0]!.push({ type: "occupancy", data: { lot_id: "L", current_occupied: 8, capacity: 10 } });
    for (let i = 0; i < 60; i++) sockets[0]!.push({ type: "alert", data: { alert_id: `a${i}`, message: "m" } });
    const s = store.getSnapshot();
    expect(s.occupancy.get("L")?.current_occupied).toBe(8);
    expect(s.alerts).toHaveLength(50);
    expect(s.alerts[0]?.alert_id).toBe("a59");
    expect(s.alertVersion).toBe(60);
    store.stop();
  });

  it("treats a silent connection as dead and reconnects", () => {
    const { store, sockets, advance } = setup();
    store.start();
    sockets[0]!.open();
    advance(11_000);
    vi.advanceTimersByTime(2500);
    expect(store.getSnapshot().status).toBe("reconnecting");
    store.stop();
  });
});
