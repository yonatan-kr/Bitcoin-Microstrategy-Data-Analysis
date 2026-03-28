{{
    config(materialized='table')
}}

-- BTC aggregated over MSTR market hours (9:30 AM – 4:00 PM EST) per trading day,
-- joined with MSTR daily OHLCV so both reflect the same window.

with btc_hourly as (
    select * from {{ ref('stg_btc_ohlcv') }}
),

mstr_daily as (
    select * from {{ ref('stg_mstr_daily') }}
),

-- Filter BTC to candles that fall within market hours on MSTR trading days
btc_market_hours as (
    select
        date_est,
        open_time_est,
        open,
        high,
        low,
        close,
        volume
    from btc_hourly
    where
        -- Only MSTR trading days
        date_est in (select date_est from mstr_daily)
        -- Only candles that open between 9:00 AM and 3:59 PM EST (covers 9:30 open → 4:00 close)
        and extract(hour from open_time_est) >= 9
        and extract(hour from open_time_est) < 16
),

-- Aggregate BTC to one row per trading day
btc_agg as (
    select
        date_est,
        -- BTC price at market open (first candle at or after 9:00 AM)
        array_agg(open  order by open_time_est asc  limit 1)[offset(0)] as btc_open,
        max(high)                                                         as btc_high,
        min(low)                                                          as btc_low,
        -- BTC price at market close (last candle before 4:00 PM)
        array_agg(close order by open_time_est desc limit 1)[offset(0)]  as btc_close,
        sum(volume)                                                       as btc_volume
    from btc_market_hours
    group by date_est
),

joined as (
    select
        mstr.date_est,

        -- MSTR
        mstr.mstr_open,
        mstr.mstr_high,
        mstr.mstr_low,
        mstr.mstr_close,
        mstr.mstr_volume,
        round(
            (mstr.mstr_close - mstr.mstr_open)
            / nullif(mstr.mstr_open, 0) * 100,
            4
        )                                       as mstr_return_pct,

        -- BTC during same window
        btc.btc_open,
        btc.btc_high,
        btc.btc_low,
        btc.btc_close,
        btc.btc_volume,
        round(
            (btc.btc_close - btc.btc_open)
            / nullif(btc.btc_open, 0) * 100,
            4
        )                                       as btc_return_pct

    from mstr_daily as mstr
    inner join btc_agg as btc
        on mstr.date_est = btc.date_est
)

select * from joined
order by date_est
