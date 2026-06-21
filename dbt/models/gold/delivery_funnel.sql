select
  enterprise_id,
  mcc,
  mnc,
  count(*) as requested,
  count_if(delivery_status = 'delivered') as delivered,
  count_if(is_verified) as verified,
  round(1.0 * count_if(is_verified) / nullif(count(*), 0), 4) as verify_rate
from iceberg.silver.events
group by 1, 2, 3
