# SPDX-License-Identifier: Apache-2.0

"""H2D retrieve latency and concurrency metrics."""

# Future
from __future__ import annotations

# Standard
from dataclasses import dataclass
import threading
from typing import Any

# Third Party
from opentelemetry import metrics

# First Party
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.event_bus import EventCallback, EventSubscriber

_AttributeKey = tuple[tuple[str, str], ...]


def _positive_int(value: object) -> int:
    """Return a non-negative integer view of a metadata value.

    Args:
        value: Metadata value to validate.

    Returns:
        ``value`` when it is an integer greater than zero, otherwise ``0``.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return max(value, 0)


@dataclass(frozen=True)
class _InflightRetrieve:
    """State retained for one H2D retrieve between START and END.

    Attributes:
        started_at: GPU-stream timestamp from ``MP_RETRIEVE_START``.
        num_chunks: Logical chunks requested by this transfer.
        attributes: Prometheus label set captured at START.
    """

    started_at: float
    num_chunks: int
    attributes: _AttributeKey


class H2DRetrieveMetricsSubscriber(EventSubscriber):
    """Tracks H2D chunk latency and in-flight request/chunk concurrency.

    ``MP_RETRIEVE_START`` and ``MP_RETRIEVE_END`` are published on the GPU
    stream, so their interval is the H2D retrieve window. A request remains
    active from the stream reaching START until it reaches END. Its requested
    chunks are counted as in flight for that whole window, including copies
    that are still queued inside the transfer.

    Peak gauges report the maximum since the previous metric collection and
    are drained by the callback, matching the L2 concurrency gauges.
    """

    def __init__(self) -> None:
        self._state_lock = threading.Lock()
        self._inflight: dict[tuple[str, str], _InflightRetrieve] = {}
        self._inflight_requests: dict[_AttributeKey, int] = {}
        self._peak_inflight_requests: dict[_AttributeKey, int] = {}
        self._inflight_chunks: dict[_AttributeKey, int] = {}
        self._peak_inflight_chunks: dict[_AttributeKey, int] = {}

        meter = metrics.get_meter("lmcache_mp.h2d")
        self._chunk_duration = meter.create_histogram(
            "lmcache_mp.h2d_chunk_duration",
            description=(
                "Histogram of estimated H2D service time per logical chunk in "
                "seconds. Each completed retrieve contributes num_chunks "
                "observations of retrieve-window duration / num_chunks, "
                "preserving chunk weighting but not within-request variance."
            ),
            unit="s",
        )
        meter.create_observable_gauge(
            "lmcache_mp.h2d_inflight_requests",
            callbacks=[self._observe_inflight_requests],
            description=(
                "H2D retrieve requests currently active between GPU-stream "
                "START and END."
            ),
        )
        meter.create_observable_gauge(
            "lmcache_mp.h2d_peak_inflight_requests",
            callbacks=[self._observe_peak_inflight_requests],
            description=(
                "Peak H2D retrieve requests active at once since the previous "
                "metric collection."
            ),
        )
        meter.create_observable_gauge(
            "lmcache_mp.h2d_inflight_chunks",
            callbacks=[self._observe_inflight_chunks],
            description=(
                "Logical chunks belonging to H2D retrieve windows currently "
                "active between GPU-stream START and END."
            ),
        )
        meter.create_observable_gauge(
            "lmcache_mp.h2d_peak_inflight_chunks",
            callbacks=[self._observe_peak_inflight_chunks],
            description=(
                "Peak logical chunks in active H2D retrieve windows since the "
                "previous metric collection."
            ),
        )

    def get_subscriptions(self) -> dict[EventType, EventCallback]:
        """Return the H2D retrieve events consumed by this subscriber."""
        return {
            EventType.MP_RETRIEVE_START: self._on_retrieve_start,
            EventType.MP_RETRIEVE_END: self._on_retrieve_end,
        }

    def _on_retrieve_start(self, event: Event) -> None:
        """Open one H2D retrieve window and update current and peak gauges."""
        key = self._correlation_key(event)
        if key is None:
            return
        attributes = tuple(sorted(self._metric_attributes(event).items()))
        num_chunks = _positive_int(event.metadata.get("num_chunks"))
        if num_chunks == 0:
            return

        with self._state_lock:
            if key in self._inflight:
                return
            self._inflight[key] = _InflightRetrieve(
                started_at=event.timestamp,
                num_chunks=num_chunks,
                attributes=attributes,
            )
            self._increment(self._inflight_requests, attributes, 1)
            self._increment(self._peak_inflight_requests, attributes, 1)
            self._increment(self._inflight_chunks, attributes, num_chunks)
            self._increment(self._peak_inflight_chunks, attributes, num_chunks)

    def _on_retrieve_end(self, event: Event) -> None:
        """Close one H2D retrieve window and record per-chunk estimates."""
        key = self._correlation_key(event)
        if key is None:
            return

        with self._state_lock:
            retrieve = self._inflight.pop(key, None)
            if retrieve is None:
                return
            self._increment(self._inflight_requests, retrieve.attributes, -1)
            self._increment(
                self._inflight_chunks,
                retrieve.attributes,
                -retrieve.num_chunks,
            )

        duration_s = event.timestamp - retrieve.started_at
        if duration_s <= 0:
            return
        attributes = dict(retrieve.attributes)
        per_chunk_duration_s = duration_s / retrieve.num_chunks
        for _ in range(retrieve.num_chunks):
            self._chunk_duration.record(per_chunk_duration_s, attributes=attributes)

    def _observe_inflight_requests(self, _options: object) -> list[metrics.Observation]:
        """Return current active H2D request counts."""
        with self._state_lock:
            return self._observations(self._inflight_requests)

    def _observe_peak_inflight_requests(
        self, _options: object
    ) -> list[metrics.Observation]:
        """Return and reset the interval peak of active H2D requests."""
        with self._state_lock:
            return self._drain_peak_observations(self._peak_inflight_requests)

    def _observe_inflight_chunks(self, _options: object) -> list[metrics.Observation]:
        """Return current active H2D chunk counts."""
        with self._state_lock:
            return self._observations(self._inflight_chunks)

    def _observe_peak_inflight_chunks(
        self, _options: object
    ) -> list[metrics.Observation]:
        """Return and reset the interval peak of active H2D chunks."""
        with self._state_lock:
            return self._drain_peak_observations(self._peak_inflight_chunks)

    @staticmethod
    def _correlation_key(event: Event) -> tuple[str, str] | None:
        """Build the transfer key shared by START and END."""
        device = event.metadata.get("device")
        if not event.session_id or device is None:
            return None
        return (
            str(event.metadata.get("transfer_key", event.session_id)),
            str(device),
        )

    @staticmethod
    def _metric_attributes(event: Event) -> dict[str, Any]:
        """Build the low-cardinality labels shared by all H2D metrics."""
        attributes: dict[str, Any] = {}
        for name in ("device", "engine_id", "model_name"):
            value = event.metadata.get(name)
            if value is not None:
                attributes[name] = str(value)
        return attributes

    @staticmethod
    def _increment(
        values: dict[_AttributeKey, int],
        attributes: _AttributeKey,
        delta: int,
    ) -> None:
        """Apply one non-negative concurrency update under the state lock."""
        values[attributes] = max(0, values.get(attributes, 0) + delta)

    @staticmethod
    def _observations(
        values: dict[_AttributeKey, int],
    ) -> list[metrics.Observation]:
        """Convert an attribute-indexed snapshot to OTel observations."""
        return [
            metrics.Observation(value, dict(attributes))
            for attributes, value in values.items()
        ]

    @classmethod
    def _drain_peak_observations(
        cls, values: dict[_AttributeKey, int]
    ) -> list[metrics.Observation]:
        """Return peak observations and reset the counters for the next interval."""
        observations = cls._observations(values)
        for attributes in values:
            values[attributes] = 0
        return observations
