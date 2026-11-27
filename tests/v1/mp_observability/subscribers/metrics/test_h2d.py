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

_CHUNK_COPY_DURATION = "lmcache_mp.h2d_chunk_copy_duration"
_INFLIGHT_REQUESTS = "lmcache_mp.h2d_inflight_requests"
_PEAK_REQUESTS = "lmcache_mp.h2d_peak_inflight_requests"
_INFLIGHT_CHUNKS = "lmcache_mp.h2d_inflight_chunks"
_PEAK_CHUNKS = "lmcache_mp.h2d_peak_inflight_chunks"
_REQUEST_QUEUE_DURATION = "lmcache_mp.h2d_request_queue_duration"
_PRODUCER_WAIT_DURATION = "lmcache_mp.h2d_producer_wait_duration"
_TRANSFER_DURATION = "lmcache_mp.h2d_transfer_duration"
_COPY_DURATION = "lmcache_mp.h2d_copy_duration"
_SCATTER_DURATION = "lmcache_mp.h2d_scatter_duration"


def _event(
    event_type: EventType,
    session_id: str,
    transfer_key: str,
    timestamp: float,
    model_name: str,
    *,
    num_chunks: int | None = None,
    chunk_index: int | None = None,
) -> Event:
    metadata: dict[str, Any] = {
        "device": "cuda:0",
        "engine_id": 7,
        "model_name": model_name,
        "transfer_key": transfer_key,
    }
    if num_chunks is not None:
        metadata["num_chunks"] = num_chunks
    if chunk_index is not None:
        metadata["chunk_index"] = chunk_index
    return Event(
        event_type=event_type,
        session_id=session_id,
        timestamp=timestamp,
        metadata=metadata,
    )


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
    def test_tracks_request_and_chunk_concurrency(
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

        handlers[EventType.MP_RETRIEVE_END](_end("req-b", "req-b#1", 12.2, model_name))

        after_both_end = _snapshot()
        assert _value(after_both_end, _INFLIGHT_REQUESTS, **attributes) == 0
        assert _value(after_both_end, _INFLIGHT_CHUNKS, **attributes) == 0
        assert _value(after_both_end, _PEAK_REQUESTS, **attributes) == 1
        assert _value(after_both_end, _PEAK_CHUNKS, **attributes) == 2

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
        handlers[EventType.MP_RETRIEVE_END](_end("req-a", "req-a#1", 40.1, model_name))
        handlers[EventType.MP_RETRIEVE_START](
            _start("req-b", "req-b#1", 40.2, 2, model_name)
        )
        handlers[EventType.MP_RETRIEVE_END](_end("req-b", "req-b#1", 40.3, model_name))

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
        count, total = _histogram_stats(after_end, _CHUNK_COPY_DURATION, **attributes)
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

    def test_records_request_level_h2d_segments(
        self, subscriber: H2DRetrieveMetricsSubscriber
    ):
        handlers = subscriber.get_subscriptions()
        model_name = "h2d-request-segments-test"
        session_id = "req-segments"
        transfer_key = "req-segments#1"

        handlers[EventType.MP_RETRIEVE_SUBMITTED](
            _event(
                EventType.MP_RETRIEVE_SUBMITTED,
                session_id,
                transfer_key,
                10.0,
                model_name,
                num_chunks=2,
            )
        )
        handlers[EventType.MP_RETRIEVE_START](
            _start(session_id, transfer_key, 12.0, 2, model_name)
        )
        handlers[EventType.MP_RETRIEVE_PRODUCER_READY](
            _event(
                EventType.MP_RETRIEVE_PRODUCER_READY,
                session_id,
                transfer_key,
                14.0,
                model_name,
            )
        )

        handlers[EventType.MP_RETRIEVE_COPY_START](
            _event(
                EventType.MP_RETRIEVE_COPY_START,
                session_id,
                transfer_key,
                15.0,
                model_name,
            )
        )
        handlers[EventType.MP_RETRIEVE_COPY_END](
            _event(
                EventType.MP_RETRIEVE_COPY_END,
                session_id,
                transfer_key,
                16.0,
                model_name,
            )
        )
        handlers[EventType.MP_RETRIEVE_SCATTER_START](
            _event(
                EventType.MP_RETRIEVE_SCATTER_START,
                session_id,
                transfer_key,
                16.0,
                model_name,
            )
        )
        handlers[EventType.MP_RETRIEVE_SCATTER_END](
            _event(
                EventType.MP_RETRIEVE_SCATTER_END,
                session_id,
                transfer_key,
                17.0,
                model_name,
            )
        )

        handlers[EventType.MP_RETRIEVE_COPY_START](
            _event(
                EventType.MP_RETRIEVE_COPY_START,
                session_id,
                transfer_key,
                18.0,
                model_name,
            )
        )
        handlers[EventType.MP_RETRIEVE_COPY_END](
            _event(
                EventType.MP_RETRIEVE_COPY_END,
                session_id,
                transfer_key,
                18.5,
                model_name,
            )
        )
        handlers[EventType.MP_RETRIEVE_SCATTER_START](
            _event(
                EventType.MP_RETRIEVE_SCATTER_START,
                session_id,
                transfer_key,
                18.5,
                model_name,
            )
        )
        handlers[EventType.MP_RETRIEVE_SCATTER_END](
            _event(
                EventType.MP_RETRIEVE_SCATTER_END,
                session_id,
                transfer_key,
                19.0,
                model_name,
            )
        )
        handlers[EventType.MP_RETRIEVE_END](
            _end(session_id, transfer_key, 20.0, model_name)
        )

        attributes = _model_attributes(model_name)
        snapshot = _snapshot()
        expected = {
            _REQUEST_QUEUE_DURATION: 2.0,
            _PRODUCER_WAIT_DURATION: 2.0,
            _TRANSFER_DURATION: 6.0,
            _COPY_DURATION: 1.5,
            _SCATTER_DURATION: 1.5,
        }
        for metric_name, expected_sum in expected.items():
            count, total = _histogram_stats(snapshot, metric_name, **attributes)
            assert count == 1
            assert total == pytest.approx(expected_sum)

    def test_records_actual_chunk_copy_durations_across_object_groups(
        self, subscriber: H2DRetrieveMetricsSubscriber
    ):
        handlers = subscriber.get_subscriptions()
        model_name = "h2d-chunk-copy-test"
        session_id = "req-chunk-copy"
        transfer_key = "req-chunk-copy#1"
        handlers[EventType.MP_RETRIEVE_START](
            _start(session_id, transfer_key, 50.0, 2, model_name)
        )

        for chunk_index, started_at, ended_at in (
            (0, 51.0, 51.2),
            (1, 51.2, 51.9),
            (0, 52.0, 52.1),
            (1, 52.1, 52.4),
        ):
            handlers[EventType.MP_RETRIEVE_CHUNK_COPY_START](
                _event(
                    EventType.MP_RETRIEVE_CHUNK_COPY_START,
                    session_id,
                    transfer_key,
                    started_at,
                    model_name,
                    chunk_index=chunk_index,
                )
            )
            handlers[EventType.MP_RETRIEVE_CHUNK_COPY_END](
                _event(
                    EventType.MP_RETRIEVE_CHUNK_COPY_END,
                    session_id,
                    transfer_key,
                    ended_at,
                    model_name,
                    chunk_index=chunk_index,
                )
            )

        handlers[EventType.MP_RETRIEVE_END](
            _end(session_id, transfer_key, 53.0, model_name)
        )

        attributes = _model_attributes(model_name)
        count, total = _histogram_stats(_snapshot(), _CHUNK_COPY_DURATION, **attributes)
        assert count == 2
        assert total == pytest.approx(1.3)
