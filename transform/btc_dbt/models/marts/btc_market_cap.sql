{{
    config(materialized='table')
}}

with daily as (
    select
        date_est,
        close
    from {{ ref('btc_daily') }}
),

supply_calc as (
    select
        date_est,
        close,

        -- Estimated block height: avg 144 blocks/day since genesis (2009-01-03)
        date_diff(date_est, date '2009-01-03', day) * 144 as est_block_height

    from daily
),

with_supply as (
    select
        date_est,
        close,
        est_block_height,

        -- Circulating supply from halving schedule (210,000 blocks per epoch)
        -- Epoch 0: 50 BTC  | Epoch 1: 25 BTC  | Epoch 2: 12.5 BTC
        -- Epoch 3: 6.25 BTC | Epoch 4: 3.125 BTC | Epoch 5: 1.5625 BTC
        (
            least(greatest(est_block_height,           0), 210000) * 50.0     +
            least(greatest(est_block_height - 210000,  0), 210000) * 25.0     +
            least(greatest(est_block_height - 420000,  0), 210000) * 12.5     +
            least(greatest(est_block_height - 630000,  0), 210000) * 6.25     +
            least(greatest(est_block_height - 840000,  0), 210000) * 3.125    +
            least(greatest(est_block_height - 1050000, 0), 210000) * 1.5625
        ) as circulating_supply_btc

    from supply_calc
)

select
    date_est,
    close                                                      as price_usd,
    round(circulating_supply_btc, 2)                           as circulating_supply_btc,
    round(circulating_supply_btc * close, 2)                   as market_cap_usd
from with_supply
order by date_est
