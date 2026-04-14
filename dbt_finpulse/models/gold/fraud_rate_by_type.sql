select 
     type,
     count(*) as total_transactions,
     sum(isFraud) as fraud_count,
     round(sum(isFraud) * 100.0 / count(*), 4) as fraud_rate_pct
from {{ source('finpulse', 'transactions_silver') }}
group by 1