with source as (
    select * from {{ source('btc_raw', 'mstr_daily_ohlcv') }}
)

select
    date_est,
    open    as mstr_open,
    high    as mstr_high,
    low     as mstr_low,
    close   as mstr_close,
    volume  as mstr_volume
from source
