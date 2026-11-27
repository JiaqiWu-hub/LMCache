# SPDX-License-Identifier: Apache-2.0

"""OpenTelemetry SDK initialization for the MP observability system.

Supports two modes, controlled by the ``otlp_endpoint`` field in
``ObservabilityConfig``:

- **OTLP push** (production): metrics/traces are pushed to an OTel collector.
- **Prometheus pull** (dev/debug): metrics are served on a local ``/metrics``
  endpoint via ``prometheus_client``, no collector needed.
"""

# Future
from __future__ import annotations

# Standard
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Third Party
    from opentelemetry.sdk.resources import Resource

# First Party
from lmcache.logging import init_logger

logger = init_logger(__name__)

_LATENCY_BUCKETS_S = (
    0.0001,
    0.00025,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)
_LATENCY_INSTRUMENTS = (
    "lmcache_mp.lookup_prefetch_duration",
    "lmcache_mp.l1_lookup_duration",
    "lmcache_mp.l2_lookup_duration",
    "lmcache_mp.l1_allocate_duration",
    "lmcache_mp.l2_load_duration",
    "lmcache_mp.l0_l1_load_duration",
    "lmcache_mp.l0_l1_store_duration",
    "lmcache_mp.h2d_chunk_copy_duration",
    "lmcache_mp.h2d_request_queue_duration",
    "lmcache_mp.h2d_producer_wait_duration",
    "lmcache_mp.h2d_transfer_duration",
    "lmcache_mp.h2d_copy_duration",
    "lmcache_mp.h2d_scatter_duration",
    "lmcache_mp.retrieve_total_duration",
    "lmcache_mp.store_total_duration",
    "lmcache_mp.l2_store_duration",
    "lmcache_mp.l2_io_queue_wait_duration",
    "lmcache_mp.l2_io_service_duration",
)
_SIZE_BUCKETS_BYTES = (
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
    4 * 1024 * 1024,
    16 * 1024 * 1024,
    64 * 1024 * 1024,
    256 * 1024 * 1024,
    1024 * 1024 * 1024,
)
_SIZE_INSTRUMENTS = ("lmcache_mp.l2_io_size",)


def _metric_views() -> list:
    """Return explicit buckets for LMCache latency and I/O size histograms."""
    # Third Party
    from opentelemetry.sdk.metrics.view import (
        ExplicitBucketHistogramAggregation,
        View,
    )

    latency_aggregation = ExplicitBucketHistogramAggregation(_LATENCY_BUCKETS_S)
    size_aggregation = ExplicitBucketHistogramAggregation(_SIZE_BUCKETS_BYTES)
    return [
        View(instrument_name=name, aggregation=latency_aggregation)
        for name in _LATENCY_INSTRUMENTS
    ] + [
        View(instrument_name=name, aggregation=size_aggregation)
        for name in _SIZE_INSTRUMENTS
    ]


def _build_resource(resource_attributes: dict[str, str] | None) -> "Resource":
    """Build an OTel ``Resource`` from the given attribute dict.

    Returns an empty ``Resource`` when *resource_attributes* is empty or
    ``None`` so that telemetry carries no stale process-level tags.
    """
    # Third Party
    from opentelemetry.sdk.resources import Resource

    if not resource_attributes:
        return Resource.create({})
    return Resource.create(dict(resource_attributes))


def init_otel_metrics(
    otlp_endpoint: str | None = None,
    prometheus_port: int | None = None,
    resource_attributes: dict[str, str] | None = None,
    start_http_server: bool = True,
) -> None:
    """Set up the OpenTelemetry MeterProvider.

    Args:
        otlp_endpoint: OTLP gRPC endpoint (e.g. ``http://localhost:4317``).
            When set, metrics are pushed to an OTel collector.
            When ``None``, falls back to Prometheus pull mode.
        prometheus_port: Port for the fallback Prometheus ``/metrics``
            endpoint.  Only used when *otlp_endpoint* is ``None``.
            Defaults to 9090.
        resource_attributes: Optional ``{attr_name: value}`` map attached
            to the ``MeterProvider`` ``Resource``.  Every metric emitted
            through the provider carries these attributes.  Intended for
            process-level identity (e.g. ``service.instance.id``) — never
            for per-request tags.
        start_http_server: Whether to start a standalone Prometheus
            HTTP server.  Set to ``False`` when metrics are already
            served by an external HTTP framework (e.g. FastAPI).
    """
    # Third Party
    from opentelemetry import metrics
    from opentelemetry.sdk.metrics import MeterProvider

    resource = _build_resource(resource_attributes)

    if otlp_endpoint is not None:
        # OTLP push mode
        # Third Party
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.sdk.metrics.export import (
            PeriodicExportingMetricReader,
        )

        exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=10000)
        provider = MeterProvider(
            metric_readers=[reader], resource=resource, views=_metric_views()
        )
        metrics.set_meter_provider(provider)
        logger.info(
            "OTel MeterProvider initialised with OTLP exporter (%s), resource=%s",
            otlp_endpoint,
            dict(resource.attributes),
        )
    else:
        # Prometheus pull fallback — no collector needed
        # Third Party
        from opentelemetry.exporter.prometheus import PrometheusMetricReader
        import prometheus_client

        if prometheus_port is None:
            prometheus_port = 9090

        reader = PrometheusMetricReader()
        provider = MeterProvider(
            metric_readers=[reader], resource=resource, views=_metric_views()
        )
        metrics.set_meter_provider(provider)
        if start_http_server:
            prometheus_client.start_http_server(prometheus_port)
            logger.info(
                "OTel MeterProvider initialised with Prometheus fallback "
                "(http://0.0.0.0:%d/metrics), resource=%s",
                prometheus_port,
                dict(resource.attributes),
            )
        else:
            logger.info(
                "OTel MeterProvider initialised with Prometheus fallback "
                "(standalone metrics HTTP server disabled; "
                "/metrics must be exposed by the caller), resource=%s",
                dict(resource.attributes),
            )


def init_otel_tracing(
    otlp_endpoint: str | None = None,
    resource_attributes: dict[str, str] | None = None,
) -> None:
    """Set up the OpenTelemetry TracerProvider with an OTLP exporter.

    Tracing requires an OTLP endpoint — there is no local fallback.
    When *otlp_endpoint* is ``None``, tracing init is skipped.

    Args:
        otlp_endpoint: OTLP gRPC endpoint.  When ``None``, tracing
            init is skipped (no-op).
        resource_attributes: Optional ``{attr_name: value}`` map attached
            to the ``TracerProvider`` ``Resource``.  Every span emitted
            through the provider carries these attributes.
    """
    if otlp_endpoint is None:
        logger.debug("No OTLP endpoint configured, skipping tracing init")
        return

    # Third Party
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = _build_resource(resource_attributes)
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    logger.info(
        "OTel TracerProvider initialised with OTLP exporter (%s), resource=%s",
        otlp_endpoint,
        dict(resource.attributes),
    )


def register_gauge(
    meter_name: str,
    gauge_name: str,
    description: str,
    func: Callable[[], int | float]
    | Callable[[], list[tuple[int | float, dict[str, object]]]],
) -> None:
    """Register an OTel observable gauge with a callback.

    This is a convenience wrapper that hides the OTel boilerplate.
    If OTel is not available, the call is silently ignored.

    Two callback shapes are accepted:

    - **Single value (no attributes).** ``func`` returns ``int`` / ``float``
      and the gauge emits one datapoint per scrape with no attributes.
      Use this for whole-process metrics like
      ``lmcache_mp.active_prefetch_jobs``.
    - **Per-attribute-set values.** ``func`` returns a list of
      ``(value, attrs)`` tuples; the gauge emits one datapoint per tuple
      with the given attributes.  Use this for per-adapter or per-tier
      metrics that share a name but vary by attribute.  An empty list
      reports no datapoints (the metric simply does not appear in the
      next scrape, which is the correct shape when there is nothing to
      observe).

    Args:
        meter_name: OTel meter name (e.g. ``lmcache.mp_server``).
        gauge_name: Metric name (e.g.
            ``lmcache_mp.active_prefetch_jobs``).
        description: Human-readable description of the gauge.
        func: Zero-arg callable.  Either a function returning the current
            scalar value, or a function returning a list of
            ``(value, attrs)`` tuples for tagged observations.
    """
    try:
        # Third Party
        from opentelemetry import metrics as otel_metrics

        def _callback(_options):
            result = func()
            if isinstance(result, list):
                return [
                    otel_metrics.Observation(value, attrs) for value, attrs in result
                ]
            return [otel_metrics.Observation(result)]

        meter = otel_metrics.get_meter(meter_name)
        meter.create_observable_gauge(
            gauge_name,
            callbacks=[_callback],
            description=description,
        )
    except ImportError:
        logger.debug(
            "opentelemetry package not found, skipping gauge %s",
            gauge_name,
        )
