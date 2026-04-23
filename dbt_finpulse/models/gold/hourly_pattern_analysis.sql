select
     HOUR(timestamp) as hour_of_day,
     payment_format,
     count(*) as transaction_count,
     avg(amount_paid) as avg_amount,
     sum(is_laundering) as fraud_count
from {{ source('finpulse', 'transactions_silver') }}
group by 1, 2