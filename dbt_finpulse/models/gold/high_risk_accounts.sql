select 
     nameOrig,
     count(*) as total_transactions,
     sum(cast(is_balance_discrepancy as int)) as discrepancy_count,
     sum(amount) as total_amount_transacted
from {{ source('finpulse', 'transactions_silver') }}
where is_balance_discrepancy = true 
group by 1
having count(*) > 1
