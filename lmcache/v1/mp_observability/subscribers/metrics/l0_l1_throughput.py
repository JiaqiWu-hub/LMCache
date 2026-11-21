# SPDX-License-Identifier: Apache-2.0

"""L0↔L1 throughput metrics subscriber.

Emits two OTel histograms in GB/s, labeled by ``engine_id``, ``device``,
and ``model_name``:
  - ``lmcache_mp.l0_l1_store_throughput``  — GPU→CPU (L0→L1) store
  - ``lmcache_mp.l0_l1_load_throughput``   — CPU→GPU (L1→L0) load

Implementation:
  - Correlates ``MP_STORE_SUBMITTED``/``MP_STORE_START`` → ``MP_STORE_END``
    and the corresponding retrieve events by ``(transfer_key, device)``.
    ``transfer_key`` is unique per store/retrieve operation; the legacy
    ``session_id`` fallback keeps older publishers working. A request can
    issue multiple transfers, so ``session_id`` alone is insufficient.
  - START/END events fire on the GPU cupy stream (``publish_on_stream``),
    so their timestamps reflect true GPU-stream time for the D2H/H2D
    copies — not Python/lock overhead.
"""

# Future
from __future__ import annotations

# Standard
from typing import Any

# Third Party
from opentelemetry import metrics

# First Party
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.event_bus import EventCallback, EventSubscriber


class L0L1ThroughputSubscriber(EventSubscriber):
    """Records L0↔L1 throughput by correlating MP_*_START→MP_*_END pairs."""

    def __init__(self) -> None:
        # (transfer_key or session_id, device) -> timestamp.
        self._pending_store: dict[tuple[str, str], float] = {}
        self._pending_load: dict[tuple[str, str], float] = {}
        self._pending_store_total: dict[tuple[str, str], float] = {}
        self._pending_load_total: dict[tuple[str, str], float] = {}

        meter = metrics.get_meter("lmcache_mp.perf")
        self._store_hist = meter.create_histogram(
            "lmcache_mp.l0_l1_store_throughput",
            description=(
                "Histogram of L0→L1 (GPU→CPU) store throughput in GB/s, "
                "measured per request as total_bytes / (end_ts - start_ts) "
                "on the GPU cupy stream."
            ),
            unit="GB/s",
        )
        self._load_hist = meter.create_histogram(
            "lmcache_mp.l0_l1_load_throughput",
            description=(
                "Histogram of L1→L0 (CPU→GPU) load throughput in GB/s, "
                "measured per request as total_bytes / (end_ts - start_ts) "
                "on the GPU cupy stream."
            ),
            unit="GB/s",
        )
        self._store_duration_hist = meter.create_histogram(
            "lmcache_mp.l0_l1_store_duration",
            description=(
                "Histogram of L0->L1 (GPU->CPU) store duration in seconds, "
                "measured on the GPU cupy stream."
            ),
            unit="s",
        )
        self._load_duration_hist = meter.create_histogram(
            "lmcache_mp.l0_l1_load_duration",
            description=(
                "Histogram of L1->L0 (CPU->GPU) load duration in seconds, "
                "measured on the GPU cupy stream."
            ),
            unit="s",
        )
        self._store_total_duration_hist = meter.create_histogram(
            "lmcache_mp.store_total_duration",
            description=(
                "Server-side store latency from MP_STORE_SUBMITTED through "
                "MP_STORE_END (seconds). Excludes connector queueing before "
                "the request reaches the LMCache server."
            ),
            unit="s",
        )
        self._load_total_duration_hist = meter.create_histogram(
            "lmcache_mp.retrieve_total_duration",
            description=(
                "Server-side retrieve latency from MP_RETRIEVE_SUBMITTED "
                "through MP_RETRIEVE_END (seconds). Excludes connector "
                "queueing before the request reaches the LMCache server."
            ),
            unit="s",
        )
        self._store_bytes = meter.create_counter(
            "lmcache_mp.l0_l1_store_bytes",
            description="Total bytes transferred from GPU memory to CPU memory.",
            unit="By",
        )
        self._load_bytes = meter.create_counter(
            "lmcache_mp.l0_l1_load_bytes",
            description="Total bytes transferred from CPU memory to GPU memory.",
            unit="By",
        )

    # -- EventSubscriber interface -----------------------------------------

    def get_subscriptions(self) -> dict[EventType, EventCallback]:
        return {
            EventType.MP_STORE_SUBMITTED: self._on_store_submitted,
            EventType.MP_STORE_START: self._on_store_start,
            EventType.MP_STORE_END: self._on_store_end,
            EventType.MP_RETRIEVE_SUBMITTED: self._on_retrieve_submitted,
            EventType.MP_RETRIEVE_START: self._on_retrieve_start,
            EventType.MP_RETRIEVE_END: self._on_retrieve_end,
        }

    # -- Store path (L0→L1, GPU→CPU) ---------------------------------------

    def _on_store_submitted(self, event: Event) -> None:
        key = self._correlation_key(event)
        if key is not None:
            self._pending_store_total[key] = event.timestamp

    def _on_store_start(self, event: Event) -> None:
        key = self._correlation_key(event)
        if key is not None:
            self._pending_store[key] = event.timestamp

    def _on_store_end(self, event: Event) -> None:
        self._record_pair(
            event=event,
            pending_start=self._pending_store,
            pending_submitted=self._pending_store_total,
            throughput_hist=self._store_hist,
            duration_hist=self._store_duration_hist,
            total_duration_hist=self._store_total_duration_hist,
            bytes_counter=self._store_bytes,
        )

    # -- Retrieve path (L1→L0, CPU→GPU) ------------------------------------

    def _on_retrieve_submitted(self, event: Event) -> None:
        key = self._correlation_key(event)
        if key is not None:
            self._pending_load_total[key] = event.timestamp

    def _on_retrieve_start(self, event: Event) -> None:
        key = self._correlation_key(event)
        if key is not None:
            self._pending_load[key] = event.timestamp

    def _on_retrieve_end(self, event: Event) -> None:
        self._record_pair(
            event=event,
            pending_start=self._pending_load,
            pending_submitted=self._pending_load_total,
            throughput_hist=self._load_hist,
            duration_hist=self._load_duration_hist,
            total_duration_hist=self._load_total_duration_hist,
            bytes_counter=self._load_bytes,
        )

    # -- Core computation --------------------------------------------------

    @staticmethod
    def _correlation_key(event: Event) -> tuple[str, str] | None:
        """Build the ``(transfer_key or session_id, device)`` key.

        Returns ``None`` if either field is missing; such events cannot
        be paired safely and are dropped.
        """
        device = event.metadata.get("device")
        if not event.session_id or device is None:
            return None
        return (
            str(event.metadata.get("transfer_key", event.session_id)),
            str(device),
        )

    @classmethod
    def _record_pair(
        cls,
        event: Event,
        pending_start: dict[tuple[str, str], float],
        pending_submitted: dict[tuple[str, str], float],
        throughput_hist: Any,
        duration_hist: Any,
        total_duration_hist: Any,
        bytes_counter: Any,
    ) -> None:
        """Record durations and transferred bytes from one matched transfer."""
        key = cls._correlation_key(event)
        if key is None:
            return

        t_start = pending_start.pop(key, None)
        t_submitted = pending_submitted.pop(key, None)
        if t_start is None:
            return

        duration = event.timestamp - t_start
        if duration <= 0:
            return

        attrs = cls._metric_attributes_from_key(event, key)
        if t_submitted is not None:
            total_duration = event.timestamp - t_submitted
            if total_duration >= duration:
                duration_hist.record(duration, attributes=attrs)
                total_duration_hist.record(total_duration, attributes=attrs)

        total_bytes = event.metadata.get("total_bytes", 0)
        if total_bytes > 0:
            bytes_counter.add(total_bytes, attributes=attrs)
            throughput_hist.record(total_bytes / duration / 1e9, attributes=attrs)

    @staticmethod
    def _metric_attributes_from_key(
        event: Event, key: tuple[str, str]
    ) -> dict[str, Any]:
        """Build the shared duration attributes from an end event."""
        attrs: dict[str, Any] = {"device": key[1]}
        engine_id = event.metadata.get("engine_id")
        model_name = event.metadata.get("model_name")
        if engine_id is not None:
            attrs["engine_id"] = str(engine_id)
        if model_name is not None:
            attrs["model_name"] = str(model_name)
        return attrs
