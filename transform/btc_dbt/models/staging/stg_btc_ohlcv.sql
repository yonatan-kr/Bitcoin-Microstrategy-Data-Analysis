with source as (
    select * from {{ source('btc_raw', 'btc_hourly_ohlcv') }}
),

renamed as (
    select
        open_time_est,
        close_time_est,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        trades,
        taker_buy_base,
        taker_buy_quote,
        load_datetime_est,

        -- derived
        cast(open_time_est as date) as date_est,
        extract(year  from open_time_est) as year,
        extract(month from open_time_est) as month,
        extract(hour  from open_time_est) as hour
    from source
)

select * from renamed
