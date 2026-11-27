# LMCache 0.5.3 latency metrics

This branch carries the customized MP latency metrics used by the test
environment. It is based on upstream LMCache `v0.5.3`; see
[`CUSTOM_METRICS.md`](CUSTOM_METRICS.md) for the upgrade and deployment
workflow.

## Activate the customized source tree

Use the wrapper instead of the original start script:

```bash
./start_lmcache_latency.sh
./start_lmcache_vllm_latency.sh
```

The wrapper prepends the repository root to `PYTHONPATH`, verifies the imported
module path, then delegates to the existing start script. The original HTTP
metrics endpoint and server arguments remain unchanged.

Do not start this copy while the original LMCache server is already running:
both processes use port `5555` and metrics port `18080`.

## Added histograms

The Prometheus exporter adds `_seconds` to second-valued instruments.
Each instrument is exposed with `_bucket`, `_count`, and `_sum`.

| Logical metric | Prometheus family | Phase measured |
| --- | --- | --- |
| `lmcache_mp.lookup_prefetch_duration` | `lmcache_mp_lookup_prefetch_duration_seconds` | Lookup submission through final L1/L2 lookup result |
| `lmcache_mp.l1_lookup_duration` | `lmcache_mp_l1_lookup_duration_seconds` | Initial L1 read-lock lookup |
| `lmcache_mp.l2_lookup_duration` | `lmcache_mp_l2_lookup_duration_seconds` | All-adapter L2 lookup and lock |
| `lmcache_mp.l1_allocate_duration` | `lmcache_mp_l1_allocate_duration_seconds` | L1 write-buffer allocation after L2 hits |
| `lmcache_mp.l0_l1_store_duration` | `lmcache_mp_l0_l1_store_duration_seconds` | GPU-to-CPU KV copy |
| `lmcache_mp.l0_l1_load_duration` | `lmcache_mp_l0_l1_load_duration_seconds` | CPU-to-GPU KV copy |
| `lmcache_mp.h2d_request_queue_duration` | `lmcache_mp_h2d_request_queue_duration_seconds` | Retrieve queueing from `MP_RETRIEVE_SUBMITTED` to the retrieve stream reaching `MP_RETRIEVE_START` |
| `lmcache_mp.h2d_producer_wait_duration` | `lmcache_mp_h2d_producer_wait_duration_seconds` | Retrieve stream wait from `MP_RETRIEVE_START` until the vLLM producer event completes |
| `lmcache_mp.h2d_transfer_duration` | `lmcache_mp_h2d_transfer_duration_seconds` | H2D work from producer-event completion through `MP_RETRIEVE_END` |
| `lmcache_mp.h2d_copy_duration` | `lmcache_mp_h2d_copy_duration_seconds` | Request-level sum of pinned CPU-to-GPU staging-copy intervals |
| `lmcache_mp.h2d_scatter_duration` | `lmcache_mp_h2d_scatter_duration_seconds` | Request-level sum of paged-scatter kernel intervals |
| `lmcache_mp.retrieve_total_duration` | `lmcache_mp_retrieve_total_duration_seconds` | Server-side retrieve submission through completion |
| `lmcache_mp.store_total_duration` | `lmcache_mp_store_total_duration_seconds` | Server-side store submission through completion |
| `lmcache_mp.l2_load_duration` | `lmcache_mp_l2_load_duration_seconds` | Per-adapter L2-to-L1 load |
| `lmcache_mp.l2_store_duration` | `lmcache_mp_l2_store_duration_seconds` | Per-adapter L1-to-L2 store |

`retrieve_total_duration` and `store_total_duration` start when the request
reaches the LMCache server. They do not include time spent queueing in the
external connector before that submission.

For each retrieve, the new H2D request segments satisfy:

```text
retrieve_total = h2d_request_queue
               + h2d_producer_wait
               + h2d_transfer

h2d_transfer >= h2d_copy + h2d_scatter
```

The inequality is expected: `h2d_transfer` also includes plan setup,
storage-manager handoff, callback dispatch, read-lock release, and other
GPU-stream work that is not part of the staging copy or scatter kernel.

## PromQL examples

P95 server-side retrieve latency over five minutes:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(lmcache_mp_retrieve_total_duration_seconds_bucket[5m])
  )
)
```

Average server-side retrieve latency over five minutes:

```promql
sum(rate(lmcache_mp_retrieve_total_duration_seconds_sum[5m]))
/
sum(rate(lmcache_mp_retrieve_total_duration_seconds_count[5m]))
```

P95 L1-to-GPU copy latency over five minutes:

```promql
histogram_quantile(
  0.95,
  sum by (le, device) (
    rate(lmcache_mp_l0_l1_load_duration_seconds_bucket[5m])
  )
)
```

P95 L2 load latency by adapter over five minutes:

```promql
histogram_quantile(
  0.95,
  sum by (le, l2_name) (
    rate(lmcache_mp_l2_load_duration_seconds_bucket[5m])
  )
)
```

## Validation

Run both standalone checks with this directory first on `PYTHONPATH`:

```bash
cd /mooncake_test/workdir/lmcache-metrics
PYTHONPATH="$PWD" /mooncake_test/venv/bin/python smoke_latency_metrics.py
PYTHONPATH="$PWD" /mooncake_test/venv/bin/python smoke_prometheus_exporter.py
```
