select
  date(created_at) as day,
  severity,
  count(*) as alert_count
from postgres.public.fraud_alerts
group by 1, 2
