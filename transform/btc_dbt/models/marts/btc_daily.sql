with hourly as (
    select * from {{ ref('stg_btc_ohlcv') }}
),

daily as (
    select
        date_est,
        year,
        month,

        -- OHLC at daily grain (first open, last close, max high, min low)
        array_agg(open  order by open_time_est limit 1)[offset(0)] as open,
        max(high)                                                    as high,
        min(low)                                                     as low,
        array_agg(close order by open_time_est desc limit 1)[offset(0)] as close,

        sum(volume)       as volume,
        sum(quote_volume) as quote_volume,
        sum(trades)       as trades,

        -- daily return %
        round(
            (array_agg(close order by open_time_est desc limit 1)[offset(0)]
             - array_agg(open  order by open_time_est limit 1)[offset(0)])
            / nullif(array_agg(open order by open_time_est limit 1)[offset(0)], 0) * 100,
            4
        ) as daily_return_pct
    from hourly
    group by date_est, year, month
)

select * from daily
order by date_est
