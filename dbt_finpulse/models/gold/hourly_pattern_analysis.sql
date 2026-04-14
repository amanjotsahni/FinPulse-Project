select
     step as hour_of_simulation,
     type,
     count(*) as transaction_count,
     avg(amount) as avg_amount,
     sum(isFraud) as fraud_count
from {{ source('finpulse', 'transactions_silver') }}
group by 1,2