{{
    config(materialized='table')
}}

-- Daily BTC + MSTR aligned (2017 – present)

with btc as (
    select * from {{ ref('btc_daily') }}
),

mstr as (
    select * from {{ ref('stg_mstr_daily') }}
),

joined as (
    select
        mstr.date_est,

        -- BTC columns
        btc.open            as btc_open,
        btc.high            as btc_high,
        btc.low             as btc_low,
        btc.close           as btc_close,
        btc.volume          as btc_volume,
        btc.daily_return_pct as btc_daily_return_pct,

        -- MSTR columns
        mstr.mstr_open,
        mstr.mstr_high,
        mstr.mstr_low,
        mstr.mstr_close,
        mstr.mstr_volume,

        -- MSTR daily return %
        round(
            (mstr.mstr_close - mstr.mstr_open)
            / nullif(mstr.mstr_open, 0) * 100,
            4
        ) as mstr_daily_return_pct

    from mstr
    inner join btc
        on mstr.date_est = btc.date_est
)

select * from joined
order by date_est
