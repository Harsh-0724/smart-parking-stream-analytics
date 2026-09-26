import { useSyncExternalStore } from "react";
import type { AlertMessage, OccupancyMessage, PipelineStats, ServerMessage } from "./types";

export type LiveStatus = "connecting" | "live" | "reconnecting";

export interface SocketLike {
  onopen: (() => void) | null;
  onclose: (() => void) | null;
  onmessage: ((e: { data: string }) => void) | null;
  send(data: string): void;
  close(): void;
}

export interface LiveState {
  status: LiveStatus;
  pipeline: PipelineStats | null;
  occupancy: ReadonlyMap<string, OccupancyMessage>;
  alerts: readonly AlertMessage[];
  alertVersion: number;
  slotVersion: number;
  lastMessageAt: number;
}

const BACKOFF_MIN_MS = 500;
const BACKOFF_MAX_MS = 10_000;
const STALE_AFTER_MS = 10_000; // pipeline stats arrive every 2 s; silence means a dead connection
const ALERT_LIMIT = 50;

export const backoffDelay = (attempt: number, random = Math.random()): number =>
  Math.min(BACKOFF_MAX_MS, BACKOFF_MIN_MS * 2 ** attempt) * (0.75 + random * 0.5);

/**
 * WebSocket client with reconnect + exponential backoff and an external store for React.
 * Slot state is kept in a mutable normalised map and announced once per animation frame, so a
 * burst of deltas causes one render pass, and only the slots that changed re-render.
 */
export class LiveStore {
  private state: LiveState = {
    status: "connecting",
    pipeline: null,
    occupancy: new Map(),
    alerts: [],
    alertVersion: 0,
    slotVersion: 0,
    lastMessageAt: 0,
  };
  private listeners = new Set<() => void>();
  private socket: SocketLike | null = null;
  private attempt = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private watchdog: ReturnType<typeof setInterval> | null = null;
  private wanted: string[] = [];
  private frame: number | null = null;
  private slotsByLot = new Map<string, Map<string, boolean>>();
  private stopped = false;

  constructor(
    private readonly create: () => SocketLike,
    private readonly now: () => number = () => Date.now(),
    private readonly raf: (cb: () => void) => number = (cb) => requestAnimationFrame(cb),
  ) {}

  // ---- React binding ----
  subscribe = (l: () => void) => {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  };
  getSnapshot = () => this.state;
  slots(lot: string): Map<string, boolean> {
    let m = this.slotsByLot.get(lot);
    if (!m) this.slotsByLot.set(lot, (m = new Map()));
    return m;
  }

  // ---- connection ----
  start() {
    this.stopped = false;
    this.connect();
    this.watchdog = setInterval(() => {
      if (this.state.status === "live" && this.now() - this.state.lastMessageAt > STALE_AFTER_MS) this.socket?.close();
    }, 2000);
  }

  stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    if (this.watchdog) clearInterval(this.watchdog);
    this.socket?.close();
  }

  subscribeLots(lots: string[]) {
    this.wanted = lots;
    if (this.state.status === "live") this.sendSubscription();
  }

  private connect() {
    const socket = this.create();
    this.socket = socket;
    socket.onopen = () => {
      this.attempt = 0;
      this.set({ status: "live", lastMessageAt: this.now() });
      this.sendSubscription();
    };
    socket.onmessage = (e) => this.handle(JSON.parse(e.data) as ServerMessage);
    socket.onclose = () => {
      if (this.stopped) return;
      this.set({ status: "reconnecting" });
      this.timer = setTimeout(() => this.connect(), backoffDelay(this.attempt++));
    };
  }

  private sendSubscription() {
    this.socket?.send(JSON.stringify({ type: "subscribe", lots: this.wanted }));
  }

  private handle(m: ServerMessage) {
    const patch: Partial<LiveState> = { lastMessageAt: this.now() };
    switch (m.type) {
      case "pipeline":
        patch.pipeline = m.data;
        break;
      case "occupancy": {
        const next = new Map(this.state.occupancy);
        next.set(m.data.lot_id, m.data);
        patch.occupancy = next;
        break;
      }
      case "alert":
        patch.alerts = [m.data, ...this.state.alerts].slice(0, ALERT_LIMIT);
        patch.alertVersion = this.state.alertVersion + 1;
        break;
      case "snapshot": {
        const map = this.slots(m.lot_id);
        map.clear();
        for (const s of m.slots) if (s.occupied !== null) map.set(s.slot_id, s.occupied);
        this.bumpSlots();
        break;
      }
      case "slots": {
        const map = this.slots(m.lot_id);
        for (const d of m.deltas) map.set(d.slot_id, d.occupied);
        this.bumpSlots();
        break;
      }
      case "hello":
        break;
    }
    this.set(patch);
  }

  private bumpSlots() {
    if (this.frame !== null) return;
    this.frame = this.raf(() => {
      this.frame = null;
      this.set({ slotVersion: this.state.slotVersion + 1 });
    });
  }

  private set(patch: Partial<LiveState>) {
    this.state = { ...this.state, ...patch };
    this.listeners.forEach((l) => l());
  }
}

export function webSocketFactory(): SocketLike {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  return new WebSocket(`${proto}://${location.host}/ws`) as unknown as SocketLike;
}

export const liveStore = new LiveStore(webSocketFactory);

export const useLive = (): LiveState => useSyncExternalStore(liveStore.subscribe, liveStore.getSnapshot);
