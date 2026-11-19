# SPDX-License-Identifier: Apache-2.0

"""Lookup and prefetch phase latency metrics.

The LMCache 0.5.3 lookup path has four observable phases:

  - L1 lookup: read-lock the L1-resident prefix.
  - L2 lookup: lookup-and-lock the same prefix on all live L2 adapters.
  - L1 allocation: reserve write buffers for objects that L2 will load.
  - Total lookup/prefetch: request submission through the final lookup result.

The per-adapter L2 data-load phase is recorded by ``L2ThroughputSubscriber``;
the L1<->GPU copy is recorded by ``L0L1ThroughputSubscriber``.
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


def _request_key(event: Event) -> str | None:
    """Return the request correlation key carried by a timing event."""
    request_id = event.metadata.get("request_id")
    if request_id is not None and str(request_id):
        return str(request_id)
    if event.session_id:
        return event.session_id
    return None


class PrefetchLatencySubscriber(EventSubscriber):
    """Records lookup and L1 allocation latency as OTel histograms."""

    def __init__(self) -> None:
        self._pending_lookup_prefetch: dict[str, float] = {}
        self._pending_l1_lookup: dict[str, float] = {}
        self._pending_l2_lookup: dict[str, float] = {}
        self._pending_l1_allocate: dict[str, float] = {}
        self._l2_lookup_adapter_counts: dict[str, int] = {}

        meter = metrics.get_meter("lmcache.lookup_latency")
        self._lookup_prefetch_hist = meter.create_histogram(
            "lmcache_mp.lookup_prefetch_duration",
            description=(
                "End-to-end lookup/prefetch latency from lookup submission "
                "through the final L1+L2 lookup result (seconds)."
            ),
            unit="s",
        )
        self._l1_lookup_hist = meter.create_histogram(
            "lmcache_mp.l1_lookup_duration",
            description=(
                "Latency of the initial L1 read-lock lookup for a prefetch "
                "request (seconds)."
            ),
            unit="s",
        )
        self._l2_lookup_hist = meter.create_histogram(
            "lmcache_mp.l2_lookup_duration",
            description=(
                "Latency from submitting L2 lookup-and-lock tasks through "
                "the last lookup result across all live adapters (seconds)."
            ),
            unit="s",
        )
        self._l1_allocate_hist = meter.create_histogram(
            "lmcache_mp.l1_allocate_duration",
            description=(
                "Latency to reserve L1 write buffers for objects selected "
                "for L2-to-L1 load (seconds)."
            ),
            unit="s",
        )

    def get_subscriptions(self) -> dict[EventType, EventCallback]:
        return {
            EventType.MP_LOOKUP_PREFETCH_START: self._on_lookup_prefetch_start,
            EventType.MP_LOOKUP_PREFETCH_END: self._on_lookup_prefetch_end,
            EventType.L1_LOOKUP_START: self._on_l1_lookup_start,
            EventType.L1_LOOKUP_END: self._on_l1_lookup_end,
            EventType.L2_LOOKUP_START: self._on_l2_lookup_start,
            EventType.L2_LOOKUP_END: self._on_l2_lookup_end,
            EventType.L1_ALLOCATE_START: self._on_l1_allocate_start,
            EventType.L1_ALLOCATE_END: self._on_l1_allocate_end,
        }

    def _on_lookup_prefetch_start(self, event: Event) -> None:
        if event.session_id:
            self._pending_lookup_prefetch[event.session_id] = event.timestamp

    def _on_lookup_prefetch_end(self, event: Event) -> None:
        key = event.session_id
        self._record(
            hist=self._lookup_prefetch_hist,
            pending=self._pending_lookup_prefetch,
            key=key,
            end_timestamp=event.timestamp,
            attributes=_lookup_attributes(event),
        )

    def _on_l1_lookup_start(self, event: Event) -> None:
        key = _request_key(event)
        if key is not None:
            self._pending_l1_lookup[key] = event.timestamp

    def _on_l1_lookup_end(self, event: Event) -> None:
        key = _request_key(event)
        self._record(
            hist=self._l1_lookup_hist,
            pending=self._pending_l1_lookup,
            key=key,
            end_timestamp=event.timestamp,
        )

    def _on_l2_lookup_start(self, event: Event) -> None:
        key = _request_key(event)
        if key is None:
            return
        self._pending_l2_lookup[key] = event.timestamp
        self._l2_lookup_adapter_counts[key] = int(
            event.metadata.get("adapter_count", 0)
        )

    def _on_l2_lookup_end(self, event: Event) -> None:
        key = _request_key(event)
        adapter_count = (
            self._l2_lookup_adapter_counts.pop(key, 0) if key is not None else 0
        )
        attrs: dict[str, Any] = {"adapter_count": str(adapter_count)}
        self._record(
            hist=self._l2_lookup_hist,
            pending=self._pending_l2_lookup,
            key=key,
            end_timestamp=event.timestamp,
            attributes=attrs,
        )

    def _on_l1_allocate_start(self, event: Event) -> None:
        key = _request_key(event)
        if key is not None:
            self._pending_l1_allocate[key] = event.timestamp

    def _on_l1_allocate_end(self, event: Event) -> None:
        key = _request_key(event)
        self._record(
            hist=self._l1_allocate_hist,
            pending=self._pending_l1_allocate,
            key=key,
            end_timestamp=event.timestamp,
        )

    @staticmethod
    def _record(
        hist: Any,
        pending: dict[str, float],
        key: str | None,
        end_timestamp: float,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """Record one matched start/end pair, dropping invalid durations."""
        if key is None:
            return
        start_timestamp = pending.pop(key, None)
        if start_timestamp is None:
            return
        duration = end_timestamp - start_timestamp
        if duration <= 0:
            return
        hist.record(duration, attributes=attributes or {})


def _lookup_attributes(event: Event) -> dict[str, Any]:
    """Build the low-cardinality labels available on lookup completion."""
    attrs: dict[str, Any] = {}
    model_name = event.metadata.get("model_name")
    if model_name is not None:
        attrs["model_name"] = str(model_name)
    cache_salt = event.metadata.get("cache_salt")
    if cache_salt is not None:
        attrs["cache_salt"] = str(cache_salt)
    return attrs
