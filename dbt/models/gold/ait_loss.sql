-- suspected AIT volume (high volume + low verify) * per-message cost
with funnel as (select * from {{ ref('delivery_funnel') }})
select
  enterprise_id, mcc, mnc, requested, verify_rate,
  case when requested > 100 and verify_rate < 0.10
       then requested * 0.03 else 0 end as estimated_ait_loss_usd
from funnel
