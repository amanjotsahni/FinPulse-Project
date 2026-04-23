SELECT
    DATE(timestamp)                     AS transaction_date,
    COUNT(*)                            AS total_transactions,
    SUM(amount_paid)                    AS total_volume_usd,
    SUM(is_laundering)                  AS laundering_count,
    ROUND(SUM(is_laundering)*100.0
        / NULLIF(COUNT(*),0), 4)        AS laundering_rate_pct,
    COUNT(DISTINCT account)             AS unique_senders,
    SUM(CASE WHEN is_cross_currency
        THEN 1 ELSE 0 END)              AS cross_currency_count,
    SUM(CASE WHEN is_high_value_wire
        THEN 1 ELSE 0 END)              AS high_value_wire_count,
    SUM(CASE WHEN payment_format='Wire Transfer'
        THEN 1 ELSE 0 END)              AS wire_transfer_count
FROM {{ source('finpulse', 'transactions_silver') }}
GROUP BY DATE(timestamp)
ORDER BY transaction_date