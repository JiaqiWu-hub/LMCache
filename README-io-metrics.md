# L2 per-object I/O metrics

This isolated LMCache source adds per-object I/O instrumentation to the native
FS connector. It does not change the existing batch scheduler or overwrite the
installed LMCache tree.

## Metrics

Common labels:

| Label | Values |
| --- | --- |
| `l2_name` | `fs_native` |
| `operation` | `read`, `write` |

The duration and size histograms also carry `status` (`success`, `failed`).
The two in-flight gauges also carry `adapter_index`, matching the other L2
per-adapter gauges.

Prometheus metric names and segment boundaries:

| 统计名称 | 意义（从哪里到哪里，中间包括） | 类型 | Start Point | End Point |
| --- | --- | --- | --- | --- |
| `lmcache_mp_l2_io_queue_wait_duration_seconds` | 单个逻辑对象从原生 FS batch 提交到 worker 开始处理它之间的等待时间。包含 LMCache worker 排队、调度等待，以及同一 worker 上前面对象的处理时间；不包含该对象自身的 open/read/write/close，也不是 Linux 块层排队时间。每个逻辑对象读写产生一个样本。 | Histogram | `ConnectorBase` 创建原生 batch 时记录 `batch->submitted_at = steady_clock::now()`，之后将各个 tile 投递给 worker。 | `FSConnector::do_batch_get()` 或 `do_batch_set()` 开始处理该对象时记录 `started_at = Clock::now()`；最终在 `record_io_sample()` 中计算 `started_at - submitted_at`。 |
| `lmcache_mp_l2_io_service_duration_seconds` | 单个逻辑对象自身的读写服务耗时。包含 open、循环 read/write、close，以及 write 场景下临时文件 rename；read-ahead 即使产生多次系统调用仍算一个逻辑对象样本。不包含 batch 排队时间、adapter 提交开销、完成结果交付或 GPU 拷贝。重复 store 在真正 I/O 前被跳过时不产生样本；失败样本以 `status="failed"` 记录。 | Histogram | read 在 `FSConnector::do_batch_get()` 调用 `do_single_get()` 前取 `started_at`；write 在 `do_single_set_impl()` 确认 `did_attempt=true` 后取 `started_at`，因此不包含已存在文件的跳过路径。 | 单对象读写在 `do_single_get()` 或 `do_single_set_impl()` 中返回或抛出后记录 `finished_at = Clock::now()`；最终在 `record_io_sample()` 中计算 `finished_at - started_at`。 |
| `lmcache_mp_l2_io_size_bytes` | 单个逻辑对象读写请求的字节数，不是时间跨度。取值为传给 read/write 的对象 buffer 长度 `req.buf_lens[i]`；0 字节样本不记录，重复 store 在 I/O 前被跳过时也不产生 write 样本。主要用于计算单次对象 I/O 大小、平均对象大小和有效带宽。 | Histogram | 不适用，不是时间区间。`do_batch_get()` 或 `do_batch_set()` 处理第 i 个对象时取得 `req.buf_lens[i]`。 | 不适用。该值随对象的时长样本一起在 `record_io_sample()` 中写入 `IOSample.io_size`。 |
| `lmcache_mp_l2_io_inflight_operations` | 瞬时实际对象级并发数。一次逻辑文件对象的 read 或 write 从进入实际 I/O 路径开始计 1，完成后减 1；不是底层 `read(2)`/`write(2)` 次数，也不是 L2 task 数。重复 store 被跳过时不增加 write 并发。 | ObservableGauge | 不适用，scrape 时读取原子计数。 | 不适用。 |
| `lmcache_mp_l2_io_peak_inflight_operations` | 短窗口峰值实际对象级并发数。每次采集该 gauge 时读取并重置峰值窗口，因此表示上一次采集到本次采集之间的最大值，通常等于 Prometheus scrape interval。正在进行中的 I/O 会带入下一个窗口，避免跨采集边界的样本丢失。 | ObservableGauge | 不适用，峰值在 I/O 进入时更新，在 scrape callback 中 drain。 | 不适用。 |

Definitions:

```text
queue_wait = object_worker_start - batch_submit
service    = object_finish     - object_worker_start
io_size    = bytes passed to read/write for the object
```

For writes, `object_worker_start` is recorded only after the duplicate-file
check reports that an actual write attempt will be made. A skipped duplicate
store therefore contributes neither a write sample nor write concurrency.

`queue_wait` is LMCache worker queueing time. If one worker handles several
objects from the same tile, later objects include the earlier object's
processing time in this value. It is not Linux block-layer queue latency.

`service` includes open, read/write, close, and write-side rename. A read with
read-ahead may issue more than one syscall, but it is still one logical object
sample.

A duplicate store whose final file already exists is skipped and emits no
write sample. Failed reads and writes emit failure samples.

## Segment relationships

For one logical object, the two duration samples form an exact decomposition
from native batch submission to object completion:

```text
object_completion - batch_submit
= queue_wait + service
```

For one native batch, the objects can be distributed across connector workers,
so only the last object determines the batch's work window:

```text
native_batch_work_window
~= max_i(queue_wait_i + service_i)
```

The scheduler-visible native batch duration additionally contains completion
delivery overhead:

```text
native_batch_duration
~= native_batch_work_window + completion_handoff
```

An adapter-level L2 task has wider boundaries than the per-object samples:

```text
l2_load_duration
~= adapter_submit_overhead
   + max_i(queue_wait_i + service_i)
   + completion_delivery

l2_store_duration
~= adapter_submit_overhead
   + max_i(queue_wait_i + service_i)
   + completion_delivery
```

Multiple L2 adapters can execute concurrently. Their wall-clock contribution is
therefore the slower adapter, not the sum:

```text
l2_parallel_wall_time ~= max_j(l2_load_duration_j)
```

Lookup/prefetch normally includes L2-to-L1 loading before the lookup result is
returned:

```text
lookup_prefetch_duration
~= l1_lookup_duration
   + l2_lookup_duration
   + l1_allocate_duration
   + l2_prefetch_load_wall_time
   + lookup/controller glue

l2_lookup_duration ~= max_j(adapter_lookup_j) + controller glue
```

The retrieve span starts after lookup/prefetch and covers the later L1-to-GPU
copy, not the earlier L2-to-L1 prefetch:

```text
retrieve_total_duration
~= submit_to_stream_start
   + l0_l1_load_duration
   + stream_completion_handoff
```

The store span covers the GPU-to-CPU path. L2 store work is asynchronous and
must not be treated as an always-contained sub-span of `store_total_duration`:

```text
store_total_duration
~= submit_to_stream_start
   + l0_l1_store_duration
   + stream_completion_handoff
```

At the request level, a cache-hit TTFT path can be reasoned about as:

```text
TTFT
~= request_queue
   + tokenize/scheduler
   + lookup_prefetch_duration
   + KV-block allocation
   + retrieve_total_duration
   + prefill_compute
   + first_token_handling
```

These request-level formulas are attribution models, not exact identities:
phase boundaries are recorded by different components and some work may overlap
or remain unmeasured.

Do not add P95 values from different phase histograms. The exact
`queue_wait + service` identity is defined per sample; P95 is a distribution
statistic over different samples and is not additive. Averages are additive
only when all phase histograms use the same matching sample population.

## PromQL examples

P95 service time by operation:

```promql
histogram_quantile(
  0.95,
  sum by (le, operation) (
    rate(lmcache_mp_l2_io_service_duration_seconds_bucket{l2_name="fs_native"}[5m])
  )
)
```

Average bytes per logical read:

```promql
sum(rate(lmcache_mp_l2_io_size_bytes_sum{l2_name="fs_native",operation="read"}[5m]))
/
sum(rate(lmcache_mp_l2_io_size_bytes_count{l2_name="fs_native",operation="read"}[5m]))
```

Read throughput from successful logical object sizes:

```promql
sum(
  rate(
    lmcache_mp_l2_io_size_bytes_sum{
      l2_name="fs_native",
      operation="read",
      status="success"
    }[5m]
  )
) / 1e6
```

Instantaneous read concurrency:

```promql
sum by (l2_name, operation) (
  lmcache_mp_l2_io_inflight_operations{l2_name="fs_native"}
)
```

Peak read/write concurrency during each scrape interval:

```promql
max by (l2_name, operation) (
  lmcache_mp_l2_io_peak_inflight_operations{l2_name="fs_native"}
)
```

One logical file-object I/O counts as `1`. A read-ahead that issues multiple
kernel reads still counts as one read operation.

## Isolated startup

The wrappers load LMCache from the directory containing the script by default.
They also verify that Python resolves the intended source tree before starting
a service. Override `LMCACHE_ROOT`, `PYTHON_BIN`, or the delegated start script
with environment variables when the deployment layout differs.

Use one of the wrappers from the deployed repository root:

```bash
./start_lmcache_io_metrics.sh
./start_lmcache_vllm_io_metrics.sh
```

Set `VERIFY_ONLY=1` to check the Python module and native extension paths
without starting a service.
