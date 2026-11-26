# SPDX-License-Identifier: Apache-2.0

"""Tests for H2DRetrieveMetricsSubscriber."""

# Standard
from typing import Any

# Third Party
import pytest

# First Party
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.subscribers.metrics.h2d import (
    H2DRetrieveMetricsSubscriber,
)
from tests.v1.mp_observability.subscribers.metrics.otel_setup import reader

_CHUNK_DURATION = "lmcache_mp.h2d_chunk_duration"
_INFLIGHT_REQUESTS = "lmcache_mp.h2d_inflight_requests"
_PEAK_REQUESTS = "lmcache_mp.h2d_peak_inflight_requests"
_INFLIGHT_CHUNKS = "lmcache_mp.h2d_inflight_chunks"
_PEAK_CHUNKS = "lmcache_mp.h2d_peak_inflight_chunks"


def _start(
    session_id: str,
    transfer_key: str,
    timestamp: float,
    num_chunks: int | None,
    model_name: str,
) -> Event:
    metadata: dict[str, Any] = {
        "device": "cuda:0",
        "engine_id": 7,
        "model_name": model_name,
        "transfer_key": transfer_key,
    }
    if num_chunks is not None:
        metadata["num_chunks"] = num_chunks
    return Event(
        event_type=EventType.MP_RETRIEVE_START,
        session_id=session_id,
        timestamp=timestamp,
        metadata=metadata,
    )


def _end(
    session_id: str,
    transfer_key: str,
    timestamp: float,
    model_name: str,
) -> Event:
    return Event(
        event_type=EventType.MP_RETRIEVE_END,
        session_id=session_id,
        timestamp=timestamp,
        metadata={
            "device": "cuda:0",
            "engine_id": 7,
            "model_name": model_name,
            "transfer_key": transfer_key,
        },
    )


def _snapshot() -> dict[str, list[Any]]:
    data = reader.get_metrics_data()
    result: dict[str, list[Any]] = {}
    if data is None:
        return result
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                result.setdefault(metric.name, []).extend(metric.data.data_points)
    return result


def _value(
    snapshot: dict[str, list[Any]],
    metric_name: str,
    **expected_attributes: str,
) -> int | float:
    total = 0
    for data_point in snapshot.get(metric_name, []):
        attributes = {
            key: str(value) for key, value in dict(data_point.attributes).items()
        }
        if all(
            attributes.get(key) == value for key, value in expected_attributes.items()
        ):
            total += data_point.value
    return total


def _histogram_stats(
    snapshot: dict[str, list[Any]],
    metric_name: str,
    **expected_attributes: str,
) -> tuple[int, float]:
    count = 0
    total = 0.0
    for data_point in snapshot.get(metric_name, []):
        attributes = {
            key: str(value) for key, value in dict(data_point.attributes).items()
        }
        if all(
            attributes.get(key) == value for key, value in expected_attributes.items()
        ):
            count += data_point.count
            total += data_point.sum
    return count, total


def _model_attributes(model_name: str) -> dict[str, str]:
    return {"device": "cuda:0", "engine_id": "7", "model_name": model_name}


@pytest.fixture(scope="module")
def subscriber() -> H2DRetrieveMetricsSubscriber:
    """Share one OTel instrument registration across the test module."""
    return H2DRetrieveMetricsSubscriber()


class TestH2DRetrieveMetrics:
    def test_tracks_request_and_chunk_concurrency_and_histogram_weight(
        self, subscriber: H2DRetrieveMetricsSubscriber
    ):
        handlers = subscriber.get_subscriptions()
        model_name = "h2d-concurrency-test"
        handlers[EventType.MP_RETRIEVE_START](
            _start("req-a", "req-a#1", 10.0, 3, model_name)
        )
        handlers[EventType.MP_RETRIEVE_START](
            _start("req-b", "req-b#1", 10.2, 2, model_name)
        )

        attributes = _model_attributes(model_name)
        active = _snapshot()
        assert _value(active, _INFLIGHT_REQUESTS, **attributes) == 2
        assert _value(active, _INFLIGHT_CHUNKS, **attributes) == 5
        assert _value(active, _PEAK_REQUESTS, **attributes) == 2
        assert _value(active, _PEAK_CHUNKS, **attributes) == 5

        handlers[EventType.MP_RETRIEVE_END](_end("req-a", "req-a#1", 11.0, model_name))

        after_end = _snapshot()
        assert _value(after_end, _INFLIGHT_REQUESTS, **attributes) == 1
        assert _value(after_end, _INFLIGHT_CHUNKS, **attributes) == 2
        assert _value(after_end, _PEAK_REQUESTS, **attributes) == 2
        assert _value(after_end, _PEAK_CHUNKS, **attributes) == 5
        count, total = _histogram_stats(after_end, _CHUNK_DURATION, **attributes)
        assert count == 3
        assert total == pytest.approx(1.0)

        handlers[EventType.MP_RETRIEVE_END](_end("req-b", "req-b#1", 12.2, model_name))

        after_both_end = _snapshot()
        assert _value(after_both_end, _INFLIGHT_REQUESTS, **attributes) == 0
        assert _value(after_both_end, _INFLIGHT_CHUNKS, **attributes) == 0
        assert _value(after_both_end, _PEAK_REQUESTS, **attributes) == 1
        assert _value(after_both_end, _PEAK_CHUNKS, **attributes) == 2
        count, total = _histogram_stats(after_both_end, _CHUNK_DURATION, **attributes)
        assert count == 5
        assert total == pytest.approx(3.0)

        idle = _snapshot()
        assert _value(idle, _PEAK_REQUESTS, **attributes) == 0
        assert _value(idle, _PEAK_CHUNKS, **attributes) == 0

    def test_peak_tracks_concurrency_not_start_event_count(
        self, subscriber: H2DRetrieveMetricsSubscriber
    ):
        handlers = subscriber.get_subscriptions()
        model_name = "h2d-sequential-concurrency-test"
        handlers[EventType.MP_RETRIEVE_START](
            _start("req-a", "req-a#1", 40.0, 3, model_name)
        )
        handlers[EventType.MP_RETRIEVE_END](
            _end("req-a", "req-a#1", 40.1, model_name)
        )
        handlers[EventType.MP_RETRIEVE_START](
            _start("req-b", "req-b#1", 40.2, 2, model_name)
        )
        handlers[EventType.MP_RETRIEVE_END](
            _end("req-b", "req-b#1", 40.3, model_name)
        )

        attributes = _model_attributes(model_name)
        snapshot = _snapshot()
        assert _value(snapshot, _INFLIGHT_REQUESTS, **attributes) == 0
        assert _value(snapshot, _INFLIGHT_CHUNKS, **attributes) == 0
        assert _value(snapshot, _PEAK_REQUESTS, **attributes) == 1
        assert _value(snapshot, _PEAK_CHUNKS, **attributes) == 3

    def test_missing_num_chunks_is_ignored(
        self, subscriber: H2DRetrieveMetricsSubscriber
    ):
        handlers = subscriber.get_subscriptions()
        model_name = "h2d-missing-num-chunks-test"
        handlers[EventType.MP_RETRIEVE_START](
            _start("req-old", "req-old#1", 20.0, None, model_name)
        )

        attributes = _model_attributes(model_name)
        active = _snapshot()
        assert _value(active, _INFLIGHT_REQUESTS, **attributes) == 0
        assert _value(active, _INFLIGHT_CHUNKS, **attributes) == 0

        handlers[EventType.MP_RETRIEVE_END](
            _end("req-old", "req-old#1", 21.0, model_name)
        )

        after_end = _snapshot()
        assert _value(after_end, _INFLIGHT_REQUESTS, **attributes) == 0
        count, total = _histogram_stats(after_end, _CHUNK_DURATION, **attributes)
        assert count == 0
        assert total == 0.0

    def test_end_without_start_does_not_go_negative(
        self, subscriber: H2DRetrieveMetricsSubscriber
    ):
        handlers = subscriber.get_subscriptions()
        model_name = "h2d-orphan-end-test"
        handlers[EventType.MP_RETRIEVE_END](
            _end("orphan", "orphan#1", 30.0, model_name)
        )

        attributes = _model_attributes(model_name)
        snapshot = _snapshot()
        assert _value(snapshot, _INFLIGHT_REQUESTS, **attributes) == 0
        assert _value(snapshot, _INFLIGHT_CHUNKS, **attributes) == 0
