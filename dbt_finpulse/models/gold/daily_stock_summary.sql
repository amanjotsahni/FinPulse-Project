select
     date,
     ticker,
     open, high, low, close, volume,
     round((close - open)/open * 100, 4) as daily_return_pct
     from {{ source('finpulse', 'stocks_silver') }}
     