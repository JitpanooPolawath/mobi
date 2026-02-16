-- Per-station per-hour departure and arrival counts
-- Used by both station_activity mart and hourly_patterns mart

with trips as (
    select * from {{ ref('stg_bike_trips') }}
),

departures as (
    select
        departure_station           as station,
        departed_hour,
        departed_date,
        hour_of_day,
        day_of_week,
        month_of_year,
        year,
        count(*)                    as departures,
        avg(duration_sec)           as avg_duration_sec,
        avg(distance_m)             as avg_distance_m,
        sum(case when is_electric then 1 else 0 end) as electric_departures
    from trips
    group by 1, 2, 3, 4, 5, 6, 7
),

arrivals as (
    select
        return_station              as station,
        departed_hour,             
        count(*)                    as arrivals
    from trips
    group by 1, 2
),

joined as (
    select
        d.station,
        d.departed_hour,
        d.departed_date,
        d.hour_of_day,
        d.day_of_week,
        d.month_of_year,
        d.year,
        coalesce(d.departures, 0)           as departures,
        coalesce(a.arrivals, 0)             as arrivals,
        coalesce(d.departures, 0) +
            coalesce(a.arrivals, 0)         as total_activity,
        d.avg_duration_sec,
        d.avg_distance_m,
        d.electric_departures
    from departures d
    left join arrivals a
        on  d.station       = a.station
        and d.departed_hour = a.departed_hour
)

select * from joined