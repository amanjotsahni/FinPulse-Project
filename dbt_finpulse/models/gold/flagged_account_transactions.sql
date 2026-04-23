-- flagged_account_transactions (IBM AML Version)
-- ─────────────────────────────────────────────────────────────────────────────
-- PURPOSE:
--   Dedicated investigation table for Agent 2 (INVESTIGATE).
--   Contains full history for any account flagged with a risk indicator.

with flagged_accounts as (
    select distinct account as account_id
    from {{ source('finpulse', 'transactions_silver') }}
    where risk_flag = true
)

select
    t.account                                              as account_id,
    t.from_bank,
    t.to_bank,
    t.payment_format,
    t.amount_paid                                          as amount,
    t.payment_currency,
    t.receiving_currency,
    t.is_cross_currency,
    t.is_high_value_wire,
    t.is_laundering                                        as is_fraud,
    t.risk_flag,
    t.timestamp                                            as txn_time

from {{ source('finpulse', 'transactions_silver') }} t
inner join flagged_accounts f
    on t.account = f.account_id
order by t.account, t.timestamp desc
