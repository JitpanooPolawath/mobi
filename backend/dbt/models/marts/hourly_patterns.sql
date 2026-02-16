-- Aggregate table by hour of the day
-- FastAPI queries
-- Time based animation controls

with station_metrics as (
    select * from {{ ref('int_station_metrics') }}
),

-- Roll up all stations into system-wide hourly totals
system_hourly as (
    select
        departed_hour,
        departed_date,
        hour_of_day,
        day_of_week,
        month_of_year,
        year,
        sum(departures)                             as total_departures,
        sum(arrivals)                               as total_arrivals,
        sum(total_activity)                         as total_activity,
        avg(avg_duration_sec)                       as avg_duration_sec,
        avg(avg_distance_m)                         as avg_distance_m,
        sum(electric_departures)                    as electric_departures,
        count(distinct station)                     as active_stations,

        -- Percentage of trips that are electric
        round(
            sum(electric_departures) /
            nullif(sum(departures), 0) * 100
        , 2)                                        as electric_pct

    from station_metrics
    group by 1, 2, 3, 4, 5, 6
)

select * from system_hourly
order by departed_hour