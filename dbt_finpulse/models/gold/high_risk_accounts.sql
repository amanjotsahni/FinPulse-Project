select 
     account                                 as account_id,
     count(*)                                as total_transactions,
     sum(cast(risk_flag as int))             as indicator_count,
     sum(amount_paid)                        as total_amount_transacted
from {{ source('finpulse', 'transactions_silver') }}
where risk_flag = true 
group by 1
having count(*) > 1
