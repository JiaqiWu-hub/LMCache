# SPDX-License-Identifier: Apache-2.0

"""Tests for StorageManager-backed L2 object I/O concurrency gauges."""

# Standard
from types import SimpleNamespace
import threading

# First Party
from lmcache.v1.distributed.storage_manager import StorageManager


class _FakeIOConcurrencyAdapter:
    def __init__(
        self,
        current: tuple[int, int],
        peak: tuple[int, int],
    ) -> None:
        self.current = current
        self.peak = peak

    def get_io_inflight_counts(self) -> tuple[int, int]:
        return self.current

    def drain_io_peak_counts(self) -> tuple[int, int]:
        peak = self.peak
        self.peak = (0, 0)
        return peak


class _SerdeWrapper:
    def __init__(self, inner: _FakeIOConcurrencyAdapter) -> None:
        self.inner_adapter = inner


def _make_manager(
    adapters: list[object],
) -> StorageManager:
    manager = StorageManager.__new__(StorageManager)
    manager._adapters_lock = threading.Lock()
    manager._adapter_descriptors = {
        index: SimpleNamespace(type_name="fs_native") for index in range(len(adapters))
    }
    manager._l2_adapters = dict(enumerate(adapters))
    return manager


def test_inflight_observations_have_adapter_and_operation_attrs() -> None:
    adapter = _FakeIOConcurrencyAdapter(current=(2, 3), peak=(4, 5))
    manager = _make_manager([adapter])

    observations = manager.get_l2_io_inflight_observations()

    assert observations == [
        (
            2,
            {
                "l2_name": "fs_native",
                "adapter_index": 0,
                "operation": "read",
            },
        ),
        (
            3,
            {
                "l2_name": "fs_native",
                "adapter_index": 0,
                "operation": "write",
            },
        ),
    ]
    assert adapter.current == (2, 3)


def test_peak_observations_drain_native_peak_counters() -> None:
    adapter = _FakeIOConcurrencyAdapter(current=(1, 0), peak=(4, 5))
    manager = _make_manager([adapter])

    first = manager.get_l2_io_peak_inflight_observations()
    second = manager.get_l2_io_peak_inflight_observations()

    assert [value for value, _ in first] == [4, 5]
    assert [attrs["operation"] for _, attrs in first] == ["read", "write"]
    assert [value for value, _ in second] == [0, 0]


def test_unwraps_serde_adapter_for_concurrency_observations() -> None:
    inner = _FakeIOConcurrencyAdapter(current=(6, 7), peak=(8, 9))
    manager = _make_manager([_SerdeWrapper(inner)])

    observations = manager.get_l2_io_inflight_observations()

    assert [value for value, _ in observations] == [6, 7]


def test_skips_adapters_without_concurrency_counters() -> None:
    manager = _make_manager([object()])

    assert manager.get_l2_io_inflight_observations() == []
    assert manager.get_l2_io_peak_inflight_observations() == []
