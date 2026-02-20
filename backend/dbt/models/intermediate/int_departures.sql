with trips as (
    select * from {{ ref('stg_bike_trips') }}
),departures as (
    select
        departure_station           as station,
        departure_longitude,
        departure_latitude,
        departed_hour,
        departed_date,
        departure_hour,
        departed_day_of_week,
        departure_month,
        departure_year,
        count(*)                    as departures,
        avg(duration_sec)           as avg_departure_duration_sec,
        avg(distance_m)             as avg_departure_distance_m,
        sum(case when is_electric then 1 else 0 end) as electric_departures
    from trips
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select * from departures