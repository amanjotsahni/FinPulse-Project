select 
     ticker,
     date,
     close,
     avg(close) over (
        partition by ticker
        order by date
        rows between 6 preceding and current row
     ) as ma_7d,
     avg(close) over (
        partition by ticker
        order by date 
        rows between 29 preceding and current row
     ) as ma_30d 
from {{ source('finpulse', 'silver_stocks') }}
