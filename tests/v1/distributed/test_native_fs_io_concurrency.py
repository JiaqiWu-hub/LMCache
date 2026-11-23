# SPDX-License-Identifier: Apache-2.0

"""Integration tests for native FS I/O concurrency counters."""

# Standard
from pathlib import Path
from typing import Any
import select

# Third Party
import pytest

lmcache_fs = pytest.importorskip("lmcache.lmcache_fs")

_KEY = "test-model@00000000@0000000000000001"
_PAYLOAD_SIZE = 4096


def _wait_for_completion(client: Any) -> list[tuple]:
    event_poll = select.poll()
    event_poll.register(client.event_fd(), select.POLLIN)
    assert event_poll.poll(5_000), "timed out waiting for native FS completion"
    return client.drain_completions()


@pytest.fixture
def client(tmp_path: Path):
    native_client = lmcache_fs.LMCacheFSClient(str(tmp_path), 4)
    yield native_client
    native_client.close()


def test_write_concurrency_peak_and_duplicate_skip(client: Any) -> None:
    payload = bytearray(b"x" * _PAYLOAD_SIZE)
    memoryview_payload = memoryview(payload)

    task_id = client.submit_batch_set([_KEY], [memoryview_payload])
    completions = _wait_for_completion(client)

    assert any(completion[0] == task_id and completion[1] for completion in completions)
    assert client.get_io_inflight_counts() == (0, 0)
    assert client.drain_io_peak_counts()[1] >= 1
    samples = client.drain_io_samples()
    assert len(samples) == 1
    assert samples[0][0] is True

    duplicate_task_id = client.submit_batch_set([_KEY], [memoryview_payload])
    duplicate_completions = _wait_for_completion(client)

    assert any(
        completion[0] == duplicate_task_id and completion[1]
        for completion in duplicate_completions
    )
    assert client.drain_io_peak_counts()[1] == 0
    assert client.drain_io_samples() == []


def test_read_concurrency_peak(client: Any) -> None:
    payload = bytearray(b"x" * _PAYLOAD_SIZE)
    set_task_id = client.submit_batch_set([_KEY], [memoryview(payload)])
    set_completions = _wait_for_completion(client)
    assert any(
        completion[0] == set_task_id and completion[1] for completion in set_completions
    )
    client.drain_io_samples()
    client.drain_io_peak_counts()

    read_buffer = bytearray(_PAYLOAD_SIZE)
    get_task_id = client.submit_batch_get([_KEY], [memoryview(read_buffer)])
    get_completions = _wait_for_completion(client)

    completion = next(
        completion
        for completion in get_completions
        if completion[0] == get_task_id and completion[1]
    )
    assert len(completion) == 5
    submitted_at, last_started_at, last_finished_at = completion[4]
    assert submitted_at <= last_started_at <= last_finished_at
    assert read_buffer == payload
    assert client.get_io_inflight_counts() == (0, 0)
    assert client.drain_io_peak_counts()[0] >= 1
    samples = client.drain_io_samples()
    assert len(samples) == 1
    assert samples[0][0] is False
