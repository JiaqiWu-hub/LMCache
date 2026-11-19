# Grafana dashboard export

`lmcache-ttft-analysis.json` is the validated LMCache TTFT Analysis dashboard.
It contains the custom latency, per-object I/O, hit-rate, memory-LRU, and
chunk-level metrics used by this branch.

Import steps:

1. In Grafana, open **Dashboards > New > Import**.
2. Upload `lmcache-ttft-analysis.json`.
3. Select the Prometheus datasource that scrapes the LMCache metrics endpoint.
4. Adjust the `service_name` custom variable for the target deployment.

The exported JSON keeps the datasource UID from the original test cluster. If
that UID does not exist in the target Grafana instance, remap the datasource
during import instead of editing every panel query.
