-- investigation_queue (IBM AML Version)
-- ─────────────────────────────────────────────────────────────────────────────
-- PURPOSE:
--   Definitive list of ALL accounts that warrant investigation based on IBM AML indicators.
--   This is Agent 2's account list — every account here gets a row,
--   sorted by priority score.
--
-- INCLUSION CRITERIA (any one qualifies):
--   1. Confirmed Laundering (is_fraud = 1)
--   2. Multiple Risk Flags (2+ cross-currency or high-value wire transfers)
--
-- PRIORITY SCORE (0-100, higher = investigate first):
--   Base 80  if confirmed laundering
--   Base 70  if multiple risk flags
--   Base 65  if both
--   + up to 20 from Amount_Paid scale
-- ─────────────────────────────────────────────────────────────────────────────

with
-- ── Aggregate risk indicators per account ──────────────────────
account_stats as (
    select
        t.account                                              as account_id,
        count(*)                                               as risk_indicator_count,
        sum(t.amount_paid)                                     as total_amount,
        max(t.amount_paid)                                     as max_amount,
        sum(case when t.is_cross_currency then 1 else 0 end)   as cross_currency_count,
        sum(case when t.is_high_value_wire then 1 else 0 end)  as high_value_wire_count,
        max(cast(t.is_laundering as int))                      as is_fraud,
        collect_set(t.payment_format)                          as txn_formats
    from {{ source('finpulse', 'transactions_silver') }} t
    where t.risk_flag = true
    group by t.account
),

-- ── Tag and Score ───────────────────────────────────────────
qualified as (
    select
        s.account_id,
        s.risk_indicator_count,
        s.total_amount,
        s.max_amount,
        s.cross_currency_count,
        s.high_value_wire_count,
        s.is_fraud,
        s.txn_formats,

        -- Qualification flags
        (s.is_fraud = 1)                                       as flag_confirmed_fraud,
        (s.risk_indicator_count >= 2)                          as flag_multi_risk,

        -- Base priority score
        case
            when s.is_fraud = 1 and s.risk_indicator_count >= 2 then 90
            when s.is_fraud = 1                                 then 80
            when s.risk_indicator_count >= 2                    then 70
            else                                                   60
        end                                                    as base_score
    from account_stats s
    where s.is_fraud = 1 or s.risk_indicator_count >= 2
),

-- ── Amount Boost ──────────────────────────────────────────
scored as (
    select
        q.*,
        least(
            greatest(
                cast((log10(greatest(q.total_amount, 1)) - 4.0) / 3.0 * 20 as int),
                0
            ),
            20
        )                                                      as amount_boost,
        a.entity_name,
        a.bank_name
    from qualified q
    left join {{ source('finpulse', 'accounts_silver') }} a 
      on q.account_id = a.account_number
)

select
    account_id,
    entity_name,
    bank_name,
    risk_indicator_count                                       as indicator_count,
    round(total_amount, 2)                                     as total_amount,
    round(max_amount, 2)                                       as max_amount,
    cross_currency_count,
    high_value_wire_count,
    cast(is_fraud as int)                                      as is_laundering,
    txn_formats,
    cast(flag_confirmed_fraud as int)                          as flag_confirmed_fraud,
    cast(flag_multi_risk as int)                               as flag_multi_risk,

    -- Final priority score (0-100) — Agent 2 sorts dropdown by this
    least(base_score + amount_boost, 100)                      as priority_score

from scored
order by priority_score desc, total_amount desc
