# SPDX-License-Identifier: Apache-2.0

"""Smoke-test per-object byte I/O samples from the native FS connector."""

# Standard
from pathlib import Path
import select
import tempfile

# First Party
from lmcache.lmcache_fs import LMCacheFSClient


def _wait_for_completion(client: LMCacheFSClient) -> None:
    poller = select.poll()
    poller.register(client.event_fd(), select.POLLIN)
    assert poller.poll(5000), "timed out waiting for native FS completion"
    assert client.drain_completions(), "completion queue unexpectedly empty"


def main() -> None:
    payload = bytes(range(256)) * 32
    with tempfile.TemporaryDirectory(prefix="lmcache-io-metrics-") as root:
        client = LMCacheFSClient(root, 2)
        key = "smoke-model@00000000@0@0123456789abcdef"
        try:
            client.submit_batch_set([key], [memoryview(payload)])
            _wait_for_completion(client)
            write_samples = client.drain_io_samples()
            assert len(write_samples) == 1, write_samples
            is_write, io_size, queue_wait, service, ok = write_samples[0]
            assert is_write and ok
            assert io_size == len(payload)
            assert queue_wait >= 0
            assert service > 0

            output = bytearray(len(payload))
            client.submit_batch_get([key], [memoryview(output)])
            _wait_for_completion(client)
            read_samples = client.drain_io_samples()
            assert len(read_samples) == 1, read_samples
            is_write, io_size, queue_wait, service, ok = read_samples[0]
            assert not is_write and ok
            assert io_size == len(payload)
            assert queue_wait >= 0
            assert service > 0
            assert output == payload

            client.submit_batch_set([key], [memoryview(payload)])
            _wait_for_completion(client)
            assert client.drain_io_samples() == []
        finally:
            client.close()

    print(
        "PASS write/read samples:",
        {
            "write": write_samples[0],
            "read": read_samples[0],
            "duplicate_write_samples": 0,
            "root": str(Path(root)),
        },
    )


if __name__ == "__main__":
    main()
