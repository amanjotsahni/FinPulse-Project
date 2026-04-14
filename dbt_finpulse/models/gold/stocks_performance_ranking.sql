-- Gold Model: stocks_performance_ranking
-- Purpose: Rank all 5 tickers by total return over the analysis period
-- One row per ticker, ranked 1 (best) to 5 (worst)

WITH price_endpoints AS (
    -- Get first and last close price per ticker
    SELECT
        ticker,
        FIRST_VALUE(close) OVER (
            PARTITION BY ticker
            ORDER BY date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS start_price,
        LAST_VALUE(close) OVER (
            PARTITION BY ticker
            ORDER BY date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS latest_price,
        MAX(close) OVER (PARTITION BY ticker) AS peak_price,
        MIN(close) OVER (PARTITION BY ticker) AS trough_price,
        AVG(daily_return) OVER (PARTITION BY ticker) AS avg_daily_return,
        COUNT(*) OVER (PARTITION BY ticker) AS trading_days
    FROM {{ source('finpulse', 'stocks_silver') }}
),

one_row_per_ticker AS (
    -- Collapse to one row per ticker
    SELECT DISTINCT
        ticker,
        ROUND(start_price, 2)   AS start_price,
        ROUND(latest_price, 2)  AS latest_price,
        ROUND(peak_price, 2)    AS peak_price,
        ROUND(trough_price, 2)  AS trough_price,
        ROUND(
            (latest_price - start_price) / start_price * 100,
            4
        )                       AS total_return_pct,
        ROUND(avg_daily_return, 4) AS avg_daily_return_pct,
        trading_days
    FROM price_endpoints
)

SELECT
    ticker,
    start_price,
    latest_price,
    peak_price,
    trough_price,
    total_return_pct,
    avg_daily_return_pct,
    trading_days,
    RANK() OVER (ORDER BY total_return_pct DESC) AS performance_rank
FROM one_row_per_ticker
ORDER BY performance_rank