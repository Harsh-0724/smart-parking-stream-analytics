from api.live import Batch, Hub, SlotStore
from api.main import TtlCache
from api.models import ConsumerGroupInfo, GroupMember
from api.pipeline import PipelineCollector


def test_slot_store_rejects_older_events() -> None:
    store = SlotStore()
    assert store.apply("L", "A-001", True, 100.0)
    assert not store.apply("L", "A-001", False, 90.0)  # older: ignored
    assert not store.apply("L", "A-001", False, 100.0)  # same timestamp: ignored
    assert store.apply("L", "A-001", False, 110.0)
    assert store.occupied_count("L") == 0


def test_hub_sends_snapshot_then_only_subscribed_deltas() -> None:
    hub = Hub()
    hub.load_snapshot({"L1": {"A-001": (1.0, True)}})
    watcher, other = hub.add(), hub.add()
    hub.subscribe(watcher, ["L1"])
    hub.subscribe(other, ["L2"])
    assert watcher.drain()[0]["type"] == "snapshot"
    other.drain()

    hub.handle(Batch(slots=[("L1", "A-002", True, 2.0), ("L2", "B-001", True, 2.0)]))
    (msg,) = watcher.drain()
    assert msg["type"] == "slots" and msg["lot_id"] == "L1"
    assert [d["slot_id"] for d in msg["deltas"]] == ["A-002"]


def test_slot_deltas_are_coalesced_per_flush() -> None:
    hub = Hub()
    client = hub.add()
    hub.subscribe(client, ["L"])
    client.drain()
    hub.handle(Batch(slots=[("L", "A-001", True, 1.0), ("L", "A-001", False, 2.0)]))
    (msg,) = client.drain()
    assert msg["deltas"] == [{"slot_id": "A-001", "occupied": False, "ts": msg["deltas"][0]["ts"]}]


def test_occupancy_and_alerts_reach_every_client_and_update_live_count() -> None:
    hub = Hub()
    a, b = hub.add(), hub.add()
    hub.handle(
        Batch(
            occupancy=[{"lot_id": "L", "current_occupied": 7, "emitted_at": "t"}],
            alerts=[{"alert_id": "x"}],
        )
    )
    assert [m["type"] for m in a.drain()] == ["occupancy", "alert"]
    assert len(b.drain()) == 2
    assert hub.live_current["L"][0] == 7


def test_slow_client_outbox_is_bounded() -> None:
    hub = Hub()
    client = hub.add()
    for i in range(5000):
        client.push({"type": "alert", "n": i})
    assert len(client.outbox) <= 2000
    assert client.outbox[-1]["n"] == 4999  # newest kept


def test_ttl_cache_expires() -> None:
    cache = TtlCache()
    cache.put("k", 1)
    assert cache.get("k", ttl=60) == 1
    assert cache.get("k", ttl=0) is None


def _group(state: str, members: dict[str, list[str]]) -> ConsumerGroupInfo:
    return ConsumerGroupInfo(
        group_id="g",
        state=state,
        rebalancing="Rebalance" in state,
        members=[GroupMember(client_id=c, host="h", partitions=p) for c, p in members.items()],
        total_lag=0,
        partitions=[],
    )


def test_rebalance_detection_reports_start_and_finish() -> None:
    collector = PipelineCollector.__new__(PipelineCollector)
    from collections import deque

    collector._signatures = {}
    collector.rebalances = deque(maxlen=10)

    collector._detect_rebalance(_group("Stable", {"a": ["t[0]", "t[1]"], "b": ["t[2]"]}))
    assert not collector.rebalances  # first sight is a baseline, not an event
    collector._detect_rebalance(_group("Stable", {"a": ["t[0]", "t[1]"], "b": ["t[2]"]}))
    assert not collector.rebalances
    collector._detect_rebalance(_group("PreparingRebalance", {"a": ["t[0]"]}))
    collector._detect_rebalance(_group("Stable", {"a": ["t[0]", "t[1]", "t[2]"]}))
    texts = [e.description for e in collector.rebalances]
    assert texts[0] == "rebalance started" and texts[1].startswith("rebalance finished: a holds 3")
