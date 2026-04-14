select 
      date_trunc('day', ingestion_timestamp) as transaction_date,
      type,
      count(*) as total_transactions,
      sum(amount) as total_amount,
      sum(isFraud) as fraud_count
from {{ source('finpulse', 'transactions_silver') }}
group by 1,2