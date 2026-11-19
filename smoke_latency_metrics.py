# SPDX-License-Identifier: Apache-2.0

"""Standalone smoke test for the LMCache 0.5.3 latency histograms."""

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.otel_init import _latency_views
from lmcache.v1.mp_observability.subscribers.metrics.l0_l1_throughput import (
    L0L1ThroughputSubscriber,
)
from lmcache.v1.mp_observability.subscribers.metrics.l2_throughput import (
    L2ThroughputSubscriber,
)
from lmcache.v1.mp_observability.subscribers.metrics.prefetch_latency import (
    PrefetchLatencySubscriber,
)


def read_histograms() -> dict[str, tuple[int, float, tuple[float, ...]]]:
    data = reader.get_metrics_data()
    result: dict[str, tuple[int, float, tuple[float, ...]]] = {}
    if data is None:
        return result
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                count = 0
                total = 0.0
                bounds: tuple[float, ...] = ()
                for point in metric.data.data_points:
                    if not hasattr(point, "count"):
                        continue
                    count += int(point.count)
                    total += float(point.sum)
                    bounds = tuple(float(value) for value in point.explicit_bounds)
                if count == 0:
                    continue
                result[metric.name] = (count, total, bounds)
    return result


reader = InMemoryMetricReader()
provider = MeterProvider(metric_readers=[reader], views=_latency_views())
metrics.set_meter_provider(provider)

l0_l1 = L0L1ThroughputSubscriber()
l2 = L2ThroughputSubscriber()
prefetch = PrefetchLatencySubscriber()

l0_l1._on_store_submitted(
    Event(
        event_type=EventType.MP_STORE_SUBMITTED,
        timestamp=0.0,
        session_id="req-d2h",
        metadata={"device": "cuda:0"},
    )
)
l0_l1._on_store_start(
    Event(
        event_type=EventType.MP_STORE_START,
        timestamp=0.1,
        session_id="req-d2h",
        metadata={"device": "cuda:0", "engine_id": 0},
    )
)
l0_l1._on_store_end(
    Event(
        event_type=EventType.MP_STORE_END,
        timestamp=0.5,
        session_id="req-d2h",
        metadata={
            "device": "cuda:0",
            "engine_id": 0,
            "model_name": "test-model",
            "total_bytes": 1_000_000_000,
        },
    )
)

l0_l1._on_retrieve_submitted(
    Event(
        event_type=EventType.MP_RETRIEVE_SUBMITTED,
        timestamp=1.0,
        session_id="req-h2d",
        metadata={"device": "cuda:1"},
    )
)
l0_l1._on_retrieve_start(
    Event(
        event_type=EventType.MP_RETRIEVE_START,
        timestamp=1.05,
        session_id="req-h2d",
        metadata={"device": "cuda:1", "engine_id": 1},
    )
)
l0_l1._on_retrieve_end(
    Event(
        event_type=EventType.MP_RETRIEVE_END,
        timestamp=1.25,
        session_id="req-h2d",
        metadata={
            "device": "cuda:1",
            "engine_id": 1,
            "model_name": "test-model",
            "total_bytes": 1_000_000_000,
        },
    )
)

l2._on_store_submitted(
    Event(
        event_type=EventType.L2_STORE_SUBMITTED,
        timestamp=2.0,
        metadata={
            "adapter_index": 0,
            "task_id": 1,
            "l2_name": "fs_native",
            "total_bytes": 2_000_000_000,
        },
    )
)
l2._on_store_completed(
    Event(
        event_type=EventType.L2_STORE_COMPLETED,
        timestamp=2.5,
        metadata={
            "adapter_index": 0,
            "task_id": 1,
            "l2_name": "fs_native",
            "bytes_transferred": 2_000_000_000,
        },
    )
)
l2._on_load_submitted(
    Event(
        event_type=EventType.L2_LOAD_TASK_SUBMITTED,
        timestamp=3.0,
        metadata={
            "request_id": 7,
            "adapter_index": 1,
            "task_id": 2,
            "l2_name": "fs_native",
            "total_bytes": 1_000_000_000,
        },
    )
)
l2._on_load_completed(
    Event(
        event_type=EventType.L2_LOAD_TASK_COMPLETED,
        timestamp=3.2,
        metadata={
            "request_id": 7,
            "adapter_index": 1,
            "task_id": 2,
            "l2_name": "fs_native",
        },
    )
)

prefetch._on_lookup_prefetch_start(
    Event(
        event_type=EventType.MP_LOOKUP_PREFETCH_START,
        timestamp=4.0,
        session_id="req-lookup",
    )
)
prefetch._on_lookup_prefetch_end(
    Event(
        event_type=EventType.MP_LOOKUP_PREFETCH_END,
        timestamp=4.7,
        session_id="req-lookup",
        metadata={"model_name": "test-model", "cache_salt": "tenant-a"},
    )
)
prefetch._on_l1_lookup_start(
    Event(
        event_type=EventType.L1_LOOKUP_START,
        timestamp=5.0,
        metadata={"request_id": "prefetch-1"},
    )
)
prefetch._on_l1_lookup_end(
    Event(
        event_type=EventType.L1_LOOKUP_END,
        timestamp=5.05,
        metadata={"request_id": "prefetch-1"},
    )
)
prefetch._on_l2_lookup_start(
    Event(
        event_type=EventType.L2_LOOKUP_START,
        timestamp=6.0,
        metadata={"request_id": "prefetch-1", "adapter_count": 2},
    )
)
prefetch._on_l2_lookup_end(
    Event(
        event_type=EventType.L2_LOOKUP_END,
        timestamp=6.3,
        metadata={"request_id": "prefetch-1", "adapter_count": 2},
    )
)
prefetch._on_l1_allocate_start(
    Event(
        event_type=EventType.L1_ALLOCATE_START,
        timestamp=7.0,
        metadata={"request_id": "prefetch-1"},
    )
)
prefetch._on_l1_allocate_end(
    Event(
        event_type=EventType.L1_ALLOCATE_END,
        timestamp=7.08,
        metadata={"request_id": "prefetch-1"},
    )
)

histograms = read_histograms()
expected = {
    "lmcache_mp.l0_l1_store_duration": 0.4,
    "lmcache_mp.store_total_duration": 0.5,
    "lmcache_mp.l0_l1_load_duration": 0.2,
    "lmcache_mp.retrieve_total_duration": 0.25,
    "lmcache_mp.l2_load_duration": 0.2,
    "lmcache_mp.l2_store_duration": 0.5,
    "lmcache_mp.lookup_prefetch_duration": 0.7,
    "lmcache_mp.l1_lookup_duration": 0.05,
    "lmcache_mp.l2_lookup_duration": 0.3,
    "lmcache_mp.l1_allocate_duration": 0.08,
}
for name, expected_sum in expected.items():
    count, observed_sum, bounds = histograms[name]
    assert count == 1, (name, count)
    assert abs(observed_sum - expected_sum) < 1e-9, (name, observed_sum)
    assert len(bounds) > 5, (name, bounds)
    print(f"{name}: count={count} sum={observed_sum:.6f}s")
