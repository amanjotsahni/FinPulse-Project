select 
     payment_format,
     count(*) as total_transactions,
     sum(is_laundering) as fraud_count,
     round(sum(is_laundering) * 100.0 / count(*), 4) as fraud_rate_pct
from {{ source('finpulse', 'transactions_silver') }}
group by 1