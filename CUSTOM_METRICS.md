# LMCache custom metrics branch

This branch carries the local observability and metrics changes used by the
YRCache/LMCache test environment.

## Baseline

- Upstream repository: `https://github.com/LMCache/LMCache`
- Baseline tag: `v0.5.3`
- Baseline commit: `140819c9d57a975dbc5678a6459a218e544cb58b`
- Custom branch: `custom/metrics-v0.5.3`

The branch is based on the exact upstream tag rather than on a copied package
tree. This keeps upstream history available for future upgrades and makes the
custom changes visible as normal Git commits.

## Customized areas

- MP latency histograms for lookup, prefetch, retrieve, store, allocation, and
  L0/L1 transfers.
- Per-object L2 filesystem I/O metrics for queue wait, service time, and I/O
  size.
- L2 throughput metrics and metric event pairing fixes.
- Native FS connector instrumentation.
- Smoke tests and isolated start scripts.
- Grafana TTFT Analysis dashboard export.

Generated files such as `.so`, `.pyc`, `__pycache__`, `build/`, egg-info, and
downloaded archives must not be committed.

## Upgrade to a new LMCache release

The simplest and safest workflow is to replay the custom commit on top of the
new upstream tag:

```bash
export PATH="/d/工作文档/codex/LMCache/.tools/git/cmd:$PATH"

cd "/d/工作文档/codex/LMCache/repos/lmcache-metrics"
git fetch origin --tags

# Replace the custom commit with the new upstream release.
git switch -c custom/metrics-v0.5.4 v0.5.4
git cherry-pick <custom-commit-sha>
```

If there is more than one custom commit:

```bash
git cherry-pick v0.5.3..custom/metrics-v0.5.3
```

Resolve conflicts by preserving the metric semantics, then run the smoke tests
and build the native extensions before deploying.

## Rebase alternative

For a linear history, rebase the custom branch onto the new release:

```bash
git switch custom/metrics-v0.5.3
git fetch origin --tags
git rebase --onto v0.5.4 v0.5.3
```

Create a new versioned branch after a successful rebase. Do not force-push a
branch that is already used by a deployed environment.

## Export a deployable source tree

The test node should deploy a clean source archive, not a Git working tree with
build artifacts:

```bash
git archive \
  --format=tar.gz \
  --prefix=lmcache-metrics/ \
  -o /tmp/lmcache-metrics.tar.gz \
  custom/metrics-v0.5.3
```

Upload and extract it under `/mooncake_test/workdir`. Build the native
extensions on the target Linux host when C++ or CUDA sources change:

```bash
cd /mooncake_test/workdir/lmcache-metrics
python3 -m pip install -e . --no-build-isolation
```

Use `PYTHONPATH` only after verifying that it resolves to the intended source
tree:

```bash
PYTHONPATH="$PWD" python3 -c "import lmcache; print(lmcache.__file__)"
```

The start wrappers use the repository root as the source tree by default. They
also verify the resolved module paths before launching:

```bash
VERIFY_ONLY=1 ./start_lmcache_io_metrics.sh
VERIFY_ONLY=1 ./start_lmcache_vllm_io_metrics.sh
VERIFY_ONLY=1 ./start_lmcache_latency.sh
VERIFY_ONLY=1 ./start_lmcache_vllm_latency.sh
```

## Grafana dashboard

The validated Grafana export is:

```text
dashboards/grafana/lmcache-ttft-analysis.json
```

Import it with Grafana's dashboard import UI. The JSON preserves the
environment-specific Prometheus datasource UID and `service_name` choices from
the test cluster, so select or remap the target Prometheus datasource during
import if the new environment uses different UIDs.

## Validation

Run source-level smoke checks before deploying:

```bash
PYTHONPATH="$PWD" python smoke_latency_metrics.py
PYTHONPATH="$PWD" python smoke_prometheus_exporter.py
PYTHONPATH="$PWD" python smoke_io_metrics.py
PYTHONPATH="$PWD" python smoke_io_prometheus.py
```

Run focused tests when the target environment has the required dependencies:

```bash
python -m pytest -q \
  tests/v1/mp_observability/subscribers/metrics/test_l0_l1_throughput.py
python -m pytest -q \
  tests/v1/mp_observability/subscribers/metrics/test_l2_io.py
```
