# SPDX-License-Identifier: Apache-2.0

"""Smoke-test Prometheus exposure of per-object L2 I/O metrics."""

# Standard
import time
from urllib.request import urlopen

# First Party
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.event_bus import EventBus, EventBusConfig
from lmcache.v1.mp_observability.otel_init import init_otel_metrics
from lmcache.v1.mp_observability.subscribers.metrics.l2_io import L2IOSubscriber


def main() -> None:
    port = 19123
    init_otel_metrics(prometheus_port=port)
    bus = EventBus(EventBusConfig(enabled=True, max_queue_size=100))
    bus.register_subscriber(L2IOSubscriber())
    bus.start()
    bus.publish(
        Event(
            event_type=EventType.L2_IO_SAMPLED,
            metadata={
                "l2_name": "fs_native",
                "samples": [(False, 8192, 0.001, 0.002, True)],
            },
        )
    )
    time.sleep(0.2)
    bus.stop()

    with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        body = response.read().decode()

    expected = (
        "lmcache_mp_l2_io_queue_wait_duration_seconds",
        "lmcache_mp_l2_io_service_duration_seconds",
        "lmcache_mp_l2_io_size_bytes",
    )
    missing = [name for name in expected if name not in body]
    assert not missing, missing
    print("PASS Prometheus metrics:", ", ".join(expected))


if __name__ == "__main__":
    main()
