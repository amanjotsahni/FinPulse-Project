-- balance_discrepancy_summary
-- One row per transaction where a balance discrepancy was detected.
-- Fraud-relevant transaction types only: TRANSFER and CASH_OUT.
-- Agent 1 (HUNT) loads a stratified sample from this table for Isolation Forest.
-- Full table preserved for audit trail — no data loss.

select
    nameOrig                                            as account_id,
    type,
    amount,
    oldbalanceOrg,
    newbalanceOrig,
    oldbalanceDest,
    newbalanceDest,
    oldbalanceOrg - amount - newbalanceOrig             as balance_gap,
    isFraud                                             as is_fraud
from {{ source('finpulse', 'transactions_silver') }}
where is_balance_discrepancy = true
  and type in ('TRANSFER', 'CASH_OUT')
