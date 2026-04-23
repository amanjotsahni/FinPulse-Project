SELECT
    a.entity_type_encoded,
    a.entity_name,
    COUNT(t.account)                 AS transaction_count,
    SUM(t.amount_paid)               AS total_volume_usd,
    SUM(t.is_laundering)             AS laundering_count,
    ROUND(SUM(t.is_laundering)*100.0
        /NULLIF(COUNT(*),0),4)       AS laundering_rate_pct,
    COUNT(DISTINCT t.from_bank)      AS banks_involved
FROM {{ source('finpulse','transactions_silver') }} t
LEFT JOIN {{ source('finpulse','accounts_silver') }} a
    ON t.account = a.account_number
GROUP BY a.entity_type_encoded, a.entity_name
ORDER BY laundering_rate_pct DESC
