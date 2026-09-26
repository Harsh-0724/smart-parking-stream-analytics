import type { components } from "./schema";

type S = components["schemas"];
export type Overview = S["Overview"];
export type LotSummary = S["LotSummary"];
export type LotDetail = S["LotDetail"];
export type SlotSnapshot = S["SlotSnapshot"];
export type History = S["History"];
export type HistoryPoint = S["HistoryPoint"];
export type AlertRow = S["AlertRow"];
export type AlertPage = S["AlertPage"];
export type PipelineStats = S["PipelineStats"];
export type ConsumerGroupInfo = S["ConsumerGroupInfo"];
export type BrokerInfo = S["BrokerInfo"];
export type TopicInfo = S["TopicInfo"];
export type RebalanceEvent = S["RebalanceEvent"];

/** Messages pushed on /ws (documented on the FastAPI websocket route). */
export interface OccupancyMessage {
  lot_id: string;
  current_occupied: number | null;
  capacity: number;
  closed: boolean;
  emitted_at: string;
  window_start: string;
  avg_occupancy_pct: number;
}
export interface AlertMessage {
  alert_id: string;
  kind: "FULL_LOT" | "SENSOR_OFFLINE";
  state: "RAISED" | "CLEARED";
  lot_id: string;
  sensor_id: string | null;
  ts: string;
  message: string;
}
export interface SlotDelta {
  slot_id: string;
  occupied: boolean;
  ts: string;
}
export type ServerMessage =
  | { type: "hello"; server_time: string }
  | { type: "pipeline"; data: PipelineStats }
  | { type: "occupancy"; data: OccupancyMessage }
  | { type: "alert"; data: AlertMessage }
  | { type: "snapshot"; lot_id: string; slots: { slot_id: string; occupied: boolean | null; ts: string }[] }
  | { type: "slots"; lot_id: string; deltas: SlotDelta[] };
