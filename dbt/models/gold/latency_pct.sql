-- request->delivery latency percentiles by operator route
select
  mcc,
  mnc,
  approx_percentile(operator_latency_ms, 0.50) as p50_latency_ms,
  approx_percentile(operator_latency_ms, 0.90) as p90_latency_ms,
  approx_percentile(operator_latency_ms, 0.99) as p99_latency_ms
from iceberg.silver.events
where operator_latency_ms is not null
group by 1, 2
