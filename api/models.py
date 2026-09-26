"""Typed response models; the frontend's TypeScript types are generated from these."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class LotSummary(BaseModel):
    lot_id: str
    name: str
    personality: Literal["office", "mall", "station"]
    capacity: int
    occupied: int | None
    occupancy_pct: float | None
    as_of: datetime | None
    spark: list[float]  # average occupancy % of the most recent windows, oldest first


class Overview(BaseModel):
    total_capacity: int
    total_occupied: int
    occupancy_pct: float
    lots: list[LotSummary]
    active_alerts: int
    generated_at: datetime


class LotDetail(BaseModel):
    lot_id: str
    name: str
    personality: Literal["office", "mall", "station"]
    capacity: int
    columns: int
    latitude: float
    longitude: float
    slot_ids: list[str]
    occupied: int | None
    occupancy_pct: float | None
    entries_24h: int
    exits_24h: int
    unique_vehicles_est: float | None  # HyperLogLog estimate, latest window
    dwell_minutes_est: float | None  # Little's law: occupied-seconds / entries over 24 h
    full_since: datetime | None


class SlotState(BaseModel):
    slot_id: str
    occupied: bool | None  # None: no report seen yet
    ts: datetime | None


class SlotSnapshot(BaseModel):
    lot_id: str
    slots: list[SlotState]


class HistoryPoint(BaseModel):
    window_start: datetime
    avg_occupied: float
    avg_occupancy_pct: float
    min_occupied: int
    max_occupied: int
    entries: int
    exits: int
    unique_vehicles_est: float | None
    closed: bool


class History(BaseModel):
    lot_id: str
    resolution: Literal["5m", "1h"]
    range_start: datetime
    range_end: datetime
    points: list[HistoryPoint]


class AlertRow(BaseModel):
    alert_id: str
    kind: Literal["FULL_LOT", "SENSOR_OFFLINE"]
    lot_id: str
    sensor_id: str | None
    status: Literal["active", "cleared"]
    raised_at: datetime
    cleared_at: datetime | None
    duration_s: float
    message: str
    occupancy_pct: float | None


class AlertPage(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AlertRow]


class BrokerInfo(BaseModel):
    id: int
    host: str
    port: int
    leader_partitions: int


class PartitionInfo(BaseModel):
    partition: int
    leader: int
    replicas: list[int]
    isr: list[int]
    end_offset: int


class TopicInfo(BaseModel):
    name: str
    partitions: list[PartitionInfo]
    total_messages: int  # sum of (end - start) offsets


class GroupPartition(BaseModel):
    topic: str
    partition: int
    committed: int | None
    end_offset: int
    lag: int
    member: str | None  # client id of the owning consumer


class GroupMember(BaseModel):
    client_id: str
    host: str
    partitions: list[str]  # "topic[partition]"


class ConsumerGroupInfo(BaseModel):
    group_id: str
    state: str  # Stable | PreparingRebalance | CompletingRebalance | Empty | Dead
    rebalancing: bool
    members: list[GroupMember]
    total_lag: int
    partitions: list[GroupPartition]


class RebalanceEvent(BaseModel):
    ts: datetime
    group_id: str
    description: str


class PipelineStats(BaseModel):
    generated_at: datetime
    error: str | None
    events_per_s: float | None
    latency_p50_s: float | None
    latency_p95_s: float | None
    dlq_messages: int
    late_messages: int
    brokers: list[BrokerInfo]
    topics: list[TopicInfo]
    groups: list[ConsumerGroupInfo]
    rebalances: list[RebalanceEvent]


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    database: bool
    kafka_feed: bool
    websocket_clients: int
