{{
    config(materialized='table')
}}

-- Hourly BTC + MSTR aligned on market hours (Apr 2024 – present)
-- Joined on the hour; only hours where MSTR was trading are included

with btc as (
    select * from {{ ref('stg_btc_ohlcv') }}
),

mstr as (
    select * from {{ ref('stg_mstr_hourly') }}
),

joined as (
    select
        mstr.open_time_est,

        -- BTC columns
        btc.open        as btc_open,
        btc.high        as btc_high,
        btc.low         as btc_low,
        btc.close       as btc_close,
        btc.volume      as btc_volume,

        -- MSTR columns
        mstr.mstr_open,
        mstr.mstr_high,
        mstr.mstr_low,
        mstr.mstr_close,
        mstr.mstr_volume

    from mstr
    inner join btc
        on mstr.open_time_est = btc.open_time_est
)

select * from joined
order by open_time_est
