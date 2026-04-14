select 
     nameOrig as account_id,
     type,
     amount,
     oldbalanceOrg,
     newbalanceOrig,
     (oldbalanceOrg - amount) as expected_balance,
     newbalanceOrig as actual_balance,
     isFraud 
from {{ source('finpulse', 'transactions_silver') }}
where is_balance_discrepancy = true 