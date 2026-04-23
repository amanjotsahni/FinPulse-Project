WITH daily_laundering AS (
    SELECT
        DATE(timestamp)              AS transaction_date,
        COUNT(*)                     AS total_transactions,
        SUM(amount_paid)             AS total_volume_usd,
        SUM(is_laundering)           AS laundering_count,
        ROUND(SUM(is_laundering)*100.0
            /NULLIF(COUNT(*),0),4)   AS laundering_rate_pct,
        SUM(CASE WHEN is_cross_currency
            THEN 1 ELSE 0 END)       AS cross_currency_count,
        SUM(CASE WHEN is_high_value_wire
            THEN 1 ELSE 0 END)       AS high_value_wire_count
    FROM {{ source('finpulse','transactions_silver') }}
    GROUP BY DATE(timestamp)
),

banking_stocks AS (
    SELECT
        date                         AS stock_date,
        MAX(CASE WHEN ticker='JPM'
            THEN close END)          AS jpm_close,
        MAX(CASE WHEN ticker='JPM'
            THEN daily_return END)   AS jpm_daily_return_pct,
        MAX(CASE WHEN ticker='JPM'
            THEN price_range END)    AS jpm_volatility,
        MAX(CASE WHEN ticker='GS'
            THEN close END)          AS gs_close,
        MAX(CASE WHEN ticker='GS'
            THEN daily_return END)   AS gs_daily_return_pct,
        MAX(CASE WHEN ticker='GS'
            THEN price_range END)    AS gs_volatility
    FROM {{ source('finpulse','silver_stocks') }}
    WHERE ticker IN ('JPM','GS')
    GROUP BY date
)

SELECT
    t.transaction_date,
    t.total_transactions,
    t.total_volume_usd,
    t.laundering_count,
    t.laundering_rate_pct,
    t.cross_currency_count,
    t.high_value_wire_count,
    s.jpm_close,
    s.jpm_daily_return_pct,
    s.jpm_volatility,
    s.gs_close,
    s.gs_daily_return_pct,
    s.gs_volatility,
    CASE
        WHEN s.jpm_daily_return_pct < 0
         AND s.gs_daily_return_pct < 0
        THEN TRUE ELSE FALSE
    END                              AS is_market_stress_day,
    ROUND(
        t.laundering_count * 1.0 /
        NULLIF(AVG(t.laundering_count) OVER (
            ORDER BY t.transaction_date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ),0), 4
    )                                AS laundering_intensity_vs_10d_avg
FROM daily_laundering t
LEFT JOIN banking_stocks s
    ON t.transaction_date = s.stock_date
ORDER BY t.transaction_date
