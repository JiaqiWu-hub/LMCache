# SPDX-License-Identifier: Apache-2.0

"""Tests for per-object L2 native connector I/O metrics."""

# Standard
from typing import Any

# First Party
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.subscribers.metrics.l2_io import L2IOSubscriber


class _RecordingHistogram:
    def __init__(self) -> None:
        self.records: list[tuple[float, dict[str, Any]]] = []

    def record(self, value: float, attributes: dict[str, Any]) -> None:
        self.records.append((value, attributes))


def _subscriber_with_recorders() -> tuple[
    L2IOSubscriber,
    _RecordingHistogram,
    _RecordingHistogram,
    _RecordingHistogram,
]:
    subscriber = L2IOSubscriber.__new__(L2IOSubscriber)
    queue_wait = _RecordingHistogram()
    service = _RecordingHistogram()
    size = _RecordingHistogram()
    subscriber._queue_wait_hist = queue_wait
    subscriber._service_hist = service
    subscriber._size_hist = size
    return subscriber, queue_wait, service, size


def test_records_per_object_io_metrics() -> None:
    subscriber, queue_wait, service, size = _subscriber_with_recorders()

    subscriber._on_io_sampled(
        Event(
            event_type=EventType.L2_IO_SAMPLED,
            metadata={
                "l2_name": "fs_native",
                "samples": [
                    (False, 4096, 0.01, 0.02, True),
                    (True, 8192, 0.03, 0.04, False),
                ],
            },
        )
    )

    read_attrs = {
        "l2_name": "fs_native",
        "operation": "read",
        "status": "success",
    }
    write_attrs = {
        "l2_name": "fs_native",
        "operation": "write",
        "status": "failed",
    }
    assert queue_wait.records == [(0.01, read_attrs), (0.03, write_attrs)]
    assert service.records == [(0.02, read_attrs), (0.04, write_attrs)]
    assert size.records == [(4096, read_attrs), (8192, write_attrs)]


def test_omits_zero_size_sample_from_size_histogram() -> None:
    subscriber, queue_wait, service, size = _subscriber_with_recorders()

    subscriber._on_io_sampled(
        Event(
            event_type=EventType.L2_IO_SAMPLED,
            metadata={
                "l2_name": "fs_native",
                "samples": [(False, 0, 0.01, 0.02, False)],
            },
        )
    )

    assert len(queue_wait.records) == 1
    assert len(service.records) == 1
    assert size.records == []


def test_records_request_level_read_metrics() -> None:
    subscriber = L2IOSubscriber.__new__(L2IOSubscriber)
    queue_wait = _RecordingHistogram()
    read_duration = _RecordingHistogram()
    subscriber._request_read_queue_wait_hist = queue_wait
    subscriber._request_read_duration_hist = read_duration

    subscriber._on_request_read_io(
        Event(
            event_type=EventType.L2_PREFETCH_READ_IO_COMPLETED,
            metadata={
                "l2_name": "fs_native",
                "status": "success",
                "adapter_count": 2,
                "queue_wait_seconds": 0.12,
                "read_duration_seconds": 0.34,
            },
        )
    )

    attrs = {
        "l2_name": "fs_native",
        "status": "success",
        "adapter_count": 2,
    }
    assert queue_wait.records == [(0.12, attrs)]
    assert read_duration.records == [(0.34, attrs)]


def test_subscribes_to_request_level_read_event() -> None:
    subscriber = L2IOSubscriber.__new__(L2IOSubscriber)
    assert (
        subscriber.get_subscriptions()[EventType.L2_PREFETCH_READ_IO_COMPLETED]
        == subscriber._on_request_read_io
    )
