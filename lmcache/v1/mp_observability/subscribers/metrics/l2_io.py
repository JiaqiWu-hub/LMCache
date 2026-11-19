# SPDX-License-Identifier: Apache-2.0

"""Per-object L2 native connector I/O metrics."""

# Future
from __future__ import annotations

# Third Party
from opentelemetry import metrics

# First Party
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.event_bus import EventCallback, EventSubscriber


class L2IOSubscriber(EventSubscriber):
    """Export queue wait, service time, and size for logical L2 object I/O."""

    def __init__(self) -> None:
        meter = metrics.get_meter("lmcache_mp.l2_io")
        self._queue_wait_hist = meter.create_histogram(
            "lmcache_mp.l2_io_queue_wait_duration",
            description=(
                "Histogram of time from native L2 batch submission to the "
                "start of one logical object I/O, in seconds."
            ),
            unit="s",
        )
        self._service_hist = meter.create_histogram(
            "lmcache_mp.l2_io_service_duration",
            description=(
                "Histogram of one logical object read/write service time, "
                "including open and rename, in seconds."
            ),
            unit="s",
        )
        self._size_hist = meter.create_histogram(
            "lmcache_mp.l2_io_size",
            description=(
                "Histogram of bytes requested by one logical object read/write."
            ),
            unit="By",
        )

    def get_subscriptions(self) -> dict[EventType, EventCallback]:
        return {EventType.L2_IO_SAMPLED: self._on_io_sampled}

    def _on_io_sampled(self, event: Event) -> None:
        l2_name = str(event.metadata.get("l2_name", "unknown"))
        for sample in event.metadata.get("samples", []):
            is_write, io_size, queue_wait, service, ok = sample
            attrs = {
                "l2_name": l2_name,
                "operation": "write" if bool(is_write) else "read",
                "status": "success" if bool(ok) else "failed",
            }
            self._queue_wait_hist.record(float(queue_wait), attributes=attrs)
            self._service_hist.record(float(service), attributes=attrs)
            if int(io_size) > 0:
                self._size_hist.record(int(io_size), attributes=attrs)
