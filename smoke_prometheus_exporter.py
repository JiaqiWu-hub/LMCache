# SPDX-License-Identifier: Apache-2.0

"""Verify that the new latency histograms use Prometheus histogram encoding."""

import prometheus_client

from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.otel_init import init_otel_metrics
from lmcache.v1.mp_observability.subscribers.metrics.l0_l1_throughput import (
    L0L1ThroughputSubscriber,
)
from lmcache.v1.mp_observability.subscribers.metrics.l2_throughput import (
    L2ThroughputSubscriber,
)
from lmcache.v1.mp_observability.subscribers.metrics.prefetch_latency import (
    PrefetchLatencySubscriber,
)


def event(
    event_type: EventType,
    timestamp: float,
    *,
    session_id: str = "",
    metadata: dict[str, object] | None = None,
) -> Event:
    """Build one event for the exporter smoke test."""
    return Event(
        event_type=event_type,
        timestamp=timestamp,
        session_id=session_id,
        metadata=metadata or {},
    )


init_otel_metrics(start_http_server=False)

l0_l1 = L0L1ThroughputSubscriber()
l2 = L2ThroughputSubscriber()
prefetch = PrefetchLatencySubscriber()

l0_l1._on_store_submitted(
    event(
        EventType.MP_STORE_SUBMITTED,
        0.0,
        session_id="store",
        metadata={"device": "cuda:0"},
    )
)
l0_l1._on_store_start(
    event(
        EventType.MP_STORE_START,
        0.1,
        session_id="store",
        metadata={"device": "cuda:0"},
    )
)
l0_l1._on_store_end(
    event(
        EventType.MP_STORE_END,
        0.5,
        session_id="store",
        metadata={"device": "cuda:0", "total_bytes": 1},
    )
)

l0_l1._on_retrieve_submitted(
    event(
        EventType.MP_RETRIEVE_SUBMITTED,
        1.0,
        session_id="load",
        metadata={"device": "cuda:0"},
    )
)
l0_l1._on_retrieve_start(
    event(
        EventType.MP_RETRIEVE_START,
        1.1,
        session_id="load",
        metadata={"device": "cuda:0"},
    )
)
l0_l1._on_retrieve_end(
    event(
        EventType.MP_RETRIEVE_END,
        1.5,
        session_id="load",
        metadata={"device": "cuda:0", "total_bytes": 1},
    )
)

l2._on_store_submitted(
    event(
        EventType.L2_STORE_SUBMITTED,
        2.0,
        metadata={"adapter_index": 0, "task_id": 1, "total_bytes": 1},
    )
)
l2._on_store_completed(
    event(
        EventType.L2_STORE_COMPLETED,
        2.5,
        metadata={
            "adapter_index": 0,
            "task_id": 1,
            "bytes_transferred": 1,
        },
    )
)
l2._on_load_submitted(
    event(
        EventType.L2_LOAD_TASK_SUBMITTED,
        3.0,
        metadata={
            "request_id": 1,
            "adapter_index": 0,
            "task_id": 1,
            "total_bytes": 1,
        },
    )
)
l2._on_load_completed(
    event(
        EventType.L2_LOAD_TASK_COMPLETED,
        3.5,
        metadata={"request_id": 1, "adapter_index": 0},
    )
)

prefetch._on_lookup_prefetch_start(
    event(EventType.MP_LOOKUP_PREFETCH_START, 4.0, session_id="lookup")
)
prefetch._on_lookup_prefetch_end(
    event(EventType.MP_LOOKUP_PREFETCH_END, 4.7, session_id="lookup")
)
for start_type, end_type, start_ts, end_ts in (
    (EventType.L1_LOOKUP_START, EventType.L1_LOOKUP_END, 5.0, 5.1),
    (EventType.L2_LOOKUP_START, EventType.L2_LOOKUP_END, 5.2, 5.5),
    (EventType.L1_ALLOCATE_START, EventType.L1_ALLOCATE_END, 5.6, 5.7),
):
    metadata = {"request_id": "prefetch"}
    subscriptions = prefetch.get_subscriptions()
    subscriptions[start_type](event(start_type, start_ts, metadata=metadata))
    subscriptions[end_type](event(end_type, end_ts, metadata=metadata))

prefix = "lmcache_mp_"
metric_names = (
    "l0_l1_store_duration",
    "store_total_duration",
    "l0_l1_load_duration",
    "retrieve_total_duration",
    "l2_store_duration",
    "l2_load_duration",
    "lookup_prefetch_duration",
    "l1_lookup_duration",
    "l2_lookup_duration",
    "l1_allocate_duration",
)
payload = prometheus_client.generate_latest(
    prometheus_client.REGISTRY
).decode("utf-8")

for metric_name in metric_names:
    for suffix in ("_bucket", "_count", "_sum"):
        base = f"{prefix}{metric_name}"
        candidates = (f"{base}{suffix}", f"{base}_seconds{suffix}")
        expected = next(
            (candidate for candidate in candidates if candidate in payload),
            None,
        )
        assert expected is not None, f"missing {candidates}"
    print(f"{base}: bucket/count/sum exposed")
