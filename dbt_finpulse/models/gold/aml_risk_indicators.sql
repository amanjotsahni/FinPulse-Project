-- aml_risk_indicators
-- Full transactions_silver exposed for Isolation Forest scoring.
-- Removing the risk_flag filter is intentional: Isolation Forest needs the full
-- population (normal + suspicious) to learn what "normal" looks like and surface
-- true anomalies. Filtering to risk_flag=true beforehand means the model only
-- sees pre-flagged rows and loses its baseline — defeating the purpose.
-- Stratified sampling in Agent 1 (ALL laundering + 10% TABLESAMPLE normal) handles volume.

select
    account                                             as account_id,
    from_bank,
    to_bank,
    payment_format,
    amount_paid                                         as amount,
    receiving_currency,
    payment_currency,
    is_cross_currency,
    is_high_value_wire,
    is_laundering                                       as is_fraud,
    timestamp                                           as txn_time
from {{ source('finpulse', 'transactions_silver') }}
