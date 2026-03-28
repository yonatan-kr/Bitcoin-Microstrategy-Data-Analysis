{{
    config(materialized='table')
}}

-- BTC 200-Week Moving Average
-- Methodology:
--   1. Aggregate daily closes to weekly (last close of each ISO week)
--   2. Calculate 200-period rolling average over weekly closes
--   3. Join back to daily for charting alongside price

with daily as (
    select
        date_est,
        close
    from {{ ref('btc_daily') }}
),

-- Get the last trading day of each ISO week
weekly as (
    select
        date_trunc(date_est, week(monday))            as week_start,
        array_agg(close order by date_est desc limit 1)[offset(0)] as weekly_close
    from daily
    group by week_start
),

-- 200-week rolling average
weekly_with_ma as (
    select
        week_start,
        weekly_close,
        round(
            avg(weekly_close) over (
                order by week_start
                rows between 199 preceding and current row
            ),
            2
        ) as wma_200,
        count(*) over (
            order by week_start
            rows between 199 preceding and current row
        ) as weeks_in_window   -- < 200 means still warming up
    from weekly
),

-- Join back to daily so every day carries the current week's 200wma
joined as (
    select
        d.date_est,
        d.close                                         as price_usd,
        w.wma_200,
        w.weeks_in_window,
        w.weekly_close,
        -- Price relative to 200wma (useful for cycle analysis)
        case
            when w.wma_200 is not null and w.wma_200 > 0
            then round(d.close / w.wma_200, 4)
        end                                             as price_to_200wma_ratio
    from daily d
    inner join weekly_with_ma w
        on date_trunc(d.date_est, week(monday)) = w.week_start
)

select * from joined
order by date_est
