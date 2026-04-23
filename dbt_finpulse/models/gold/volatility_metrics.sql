select 
     ticker,
     date,
     close,
     stddev(close) over (
        partition by ticker
        order by date
        rows between 6 preceding and current row 
     ) as volatility_7d,
     stddev(close) over (
        partition by ticker
        order by date 
        rows between 29 preceding and current row
     ) as volatility_30d
from {{ source('finpulse', 'silver_stocks') }}
