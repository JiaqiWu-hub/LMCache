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


@dataclass
class _InflightRetrieve:
    """State retained for one H2D retrieve between START and END.

    Attributes:
        submitted_at: Server-side submission timestamp from
            ``MP_RETRIEVE_SUBMITTED``.
        started_at: GPU-stream timestamp from ``MP_RETRIEVE_START``.
        producer_ready_at: GPU-stream timestamp after the vLLM producer event
            completes.
        copy_started_at: Start timestamp of the current copy interval.
        copy_duration: Sum of completed copy intervals in this request.
        scatter_started_at: Start timestamp of the current scatter interval.
        scatter_duration: Sum of completed scatter intervals in this request.
        chunk_copy_started_at: Start timestamp of each active chunk copy.
        chunk_copy_durations: Completed copy duration accumulated by chunk.
        num_chunks: Logical chunks requested by this transfer.
        attributes: Prometheus label set captured at START.
    """

    submitted_at: float | None
    started_at: float
    producer_ready_at: float | None
    copy_started_at: float | None
    copy_duration: float
    scatter_started_at: float | None
    scatter_duration: float
    chunk_copy_started_at: dict[int, float]
    chunk_copy_durations: dict[int, float]
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
        self._submitted: dict[tuple[str, str], float] = {}
        self._inflight: dict[tuple[str, str], _InflightRetrieve] = {}
        self._inflight_requests: dict[_AttributeKey, int] = {}
        self._peak_inflight_requests: dict[_AttributeKey, int] = {}
        self._inflight_chunks: dict[_AttributeKey, int] = {}
        self._peak_inflight_chunks: dict[_AttributeKey, int] = {}

        meter = metrics.get_meter("lmcache_mp.h2d")
        self._chunk_copy_duration = meter.create_histogram(
            "lmcache_mp.h2d_chunk_copy_duration",
            description=(
                "Histogram of actual H2D staging-copy stream time per logical "
                "chunk in seconds, accumulated across object groups."
            ),
            unit="s",
        )
        self._request_queue_duration = meter.create_histogram(
            "lmcache_mp.h2d_request_queue_duration",
            description=(
                "Request-level time from MP_RETRIEVE_SUBMITTED until the "
                "LMCache retrieve stream reaches MP_RETRIEVE_START."
            ),
            unit="s",
        )
        self._producer_wait_duration = meter.create_histogram(
            "lmcache_mp.h2d_producer_wait_duration",
            description=(
                "Request-level time from MP_RETRIEVE_START until the imported "
                "vLLM producer event completes."
            ),
            unit="s",
        )
        self._transfer_duration = meter.create_histogram(
            "lmcache_mp.h2d_transfer_duration",
            description=(
                "Request-level H2D transfer time from producer event "
                "completion until MP_RETRIEVE_END."
            ),
            unit="s",
        )
        self._copy_duration = meter.create_histogram(
            "lmcache_mp.h2d_copy_duration",
            description=(
                "Request-level sum of pinned CPU to GPU staging-copy "
                "intervals on the retrieve stream."
            ),
            unit="s",
        )
        self._scatter_duration = meter.create_histogram(
            "lmcache_mp.h2d_scatter_duration",
            description=(
                "Request-level sum of GPU scatter-kernel intervals on the "
                "retrieve stream."
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
            EventType.MP_RETRIEVE_SUBMITTED: self._on_retrieve_submitted,
            EventType.MP_RETRIEVE_START: self._on_retrieve_start,
            EventType.MP_RETRIEVE_PRODUCER_READY: self._on_producer_ready,
            EventType.MP_RETRIEVE_COPY_START: self._on_copy_start,
            EventType.MP_RETRIEVE_COPY_END: self._on_copy_end,
            EventType.MP_RETRIEVE_SCATTER_START: self._on_scatter_start,
            EventType.MP_RETRIEVE_SCATTER_END: self._on_scatter_end,
            EventType.MP_RETRIEVE_CHUNK_COPY_START: self._on_chunk_copy_start,
            EventType.MP_RETRIEVE_CHUNK_COPY_END: self._on_chunk_copy_end,
            EventType.MP_RETRIEVE_END: self._on_retrieve_end,
        }

    def _on_retrieve_submitted(self, event: Event) -> None:
        """Remember the request-level submission timestamp."""
        key = self._correlation_key(event)
        if key is None:
            return
        with self._state_lock:
            self._submitted[key] = event.timestamp

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
                submitted_at=self._submitted.pop(key, None),
                started_at=event.timestamp,
                producer_ready_at=None,
                copy_started_at=None,
                copy_duration=0.0,
                scatter_started_at=None,
                scatter_duration=0.0,
                chunk_copy_started_at={},
                chunk_copy_durations={},
                num_chunks=num_chunks,
                attributes=attributes,
            )
            self._increment(self._inflight_requests, attributes, 1)
            self._increment(self._inflight_chunks, attributes, num_chunks)
            self._update_peak(
                self._peak_inflight_requests,
                self._inflight_requests,
                attributes,
            )
            self._update_peak(
                self._peak_inflight_chunks,
                self._inflight_chunks,
                attributes,
            )

    def _on_producer_ready(self, event: Event) -> None:
        """Close the vLLM producer wait segment for an active retrieve."""
        key = self._correlation_key(event)
        if key is None:
            return
        with self._state_lock:
            retrieve = self._inflight.get(key)
            if retrieve is None or retrieve.producer_ready_at is not None:
                return
            retrieve.producer_ready_at = event.timestamp

    def _on_copy_start(self, event: Event) -> None:
        """Open one staging-copy interval."""
        key = self._correlation_key(event)
        if key is None:
            return
        with self._state_lock:
            retrieve = self._inflight.get(key)
            if retrieve is None or retrieve.copy_started_at is not None:
                return
            retrieve.copy_started_at = event.timestamp

    def _on_copy_end(self, event: Event) -> None:
        """Accumulate one completed staging-copy interval."""
        key = self._correlation_key(event)
        if key is None:
            return
        with self._state_lock:
            retrieve = self._inflight.get(key)
            if retrieve is None or retrieve.copy_started_at is None:
                return
            retrieve.copy_duration += max(
                0.0, event.timestamp - retrieve.copy_started_at
            )
            retrieve.copy_started_at = None

    def _on_scatter_start(self, event: Event) -> None:
        """Open one scatter-kernel interval."""
        key = self._correlation_key(event)
        if key is None:
            return
        with self._state_lock:
            retrieve = self._inflight.get(key)
            if retrieve is None or retrieve.scatter_started_at is not None:
                return
            retrieve.scatter_started_at = event.timestamp

    def _on_scatter_end(self, event: Event) -> None:
        """Accumulate one completed scatter-kernel interval."""
        key = self._correlation_key(event)
        if key is None:
            return
        with self._state_lock:
            retrieve = self._inflight.get(key)
            if retrieve is None or retrieve.scatter_started_at is None:
                return
            retrieve.scatter_duration += max(
                0.0, event.timestamp - retrieve.scatter_started_at
            )
            retrieve.scatter_started_at = None

    def _on_chunk_copy_start(self, event: Event) -> None:
        """Open one logical chunk's staging-copy interval."""
        key = self._correlation_key(event)
        chunk_index = self._chunk_index(event)
        if key is None or chunk_index is None:
            return
        with self._state_lock:
            retrieve = self._inflight.get(key)
            if (
                retrieve is None
                or chunk_index >= retrieve.num_chunks
                or chunk_index in retrieve.chunk_copy_started_at
            ):
                return
            retrieve.chunk_copy_started_at[chunk_index] = event.timestamp
            retrieve.chunk_copy_durations.setdefault(chunk_index, 0.0)

    def _on_chunk_copy_end(self, event: Event) -> None:
        """Accumulate one logical chunk's completed staging-copy interval."""
        key = self._correlation_key(event)
        chunk_index = self._chunk_index(event)
        if key is None or chunk_index is None:
            return
        with self._state_lock:
            retrieve = self._inflight.get(key)
            if retrieve is None:
                return
            started_at = retrieve.chunk_copy_started_at.pop(chunk_index, None)
            if started_at is None:
                return
            retrieve.chunk_copy_durations[chunk_index] += max(
                0.0, event.timestamp - started_at
            )

    def _on_retrieve_end(self, event: Event) -> None:
        """Close one H2D retrieve window and record actual chunk latencies."""
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
        if retrieve.submitted_at is not None:
            queue_duration = retrieve.started_at - retrieve.submitted_at
            if queue_duration >= 0:
                self._request_queue_duration.record(
                    queue_duration, attributes=attributes
                )
        if retrieve.producer_ready_at is not None:
            producer_wait = retrieve.producer_ready_at - retrieve.started_at
            transfer_duration = event.timestamp - retrieve.producer_ready_at
            if producer_wait >= 0:
                self._producer_wait_duration.record(
                    producer_wait, attributes=attributes
                )
            if transfer_duration >= 0:
                self._transfer_duration.record(transfer_duration, attributes=attributes)
        self._copy_duration.record(retrieve.copy_duration, attributes=attributes)
        self._scatter_duration.record(retrieve.scatter_duration, attributes=attributes)
        for chunk_duration in retrieve.chunk_copy_durations.values():
            self._chunk_copy_duration.record(chunk_duration, attributes=attributes)

    def _observe_inflight_requests(self, _options: object) -> list[metrics.Observation]:
        """Return current active H2D request counts."""
        with self._state_lock:
            return self._observations(self._inflight_requests)

    def _observe_peak_inflight_requests(
        self, _options: object
    ) -> list[metrics.Observation]:
        """Return and reset the interval peak of active H2D requests."""
        with self._state_lock:
            return self._drain_peak_observations(
                self._peak_inflight_requests,
                self._inflight_requests,
            )

    def _observe_inflight_chunks(self, _options: object) -> list[metrics.Observation]:
        """Return current active H2D chunk counts."""
        with self._state_lock:
            return self._observations(self._inflight_chunks)

    def _observe_peak_inflight_chunks(
        self, _options: object
    ) -> list[metrics.Observation]:
        """Return and reset the interval peak of active H2D chunks."""
        with self._state_lock:
            return self._drain_peak_observations(
                self._peak_inflight_chunks,
                self._inflight_chunks,
            )

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
    def _chunk_index(event: Event) -> int | None:
        """Return a non-negative logical chunk index from event metadata."""
        value = event.metadata.get("chunk_index")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        return value

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
    def _update_peak(
        peaks: dict[_AttributeKey, int],
        current: dict[_AttributeKey, int],
        attributes: _AttributeKey,
    ) -> None:
        """Raise the interval peak to the current concurrency level."""
        peaks[attributes] = max(
            peaks.get(attributes, 0),
            current.get(attributes, 0),
        )

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
        cls,
        values: dict[_AttributeKey, int],
        current_values: dict[_AttributeKey, int],
    ) -> list[metrics.Observation]:
        """Return interval peaks and seed the next interval with active values."""
        for attributes, current in current_values.items():
            values[attributes] = max(values.get(attributes, 0), current)
        observations = cls._observations(values)
        for attributes in values:
            values[attributes] = current_values.get(attributes, 0)
        return observations
