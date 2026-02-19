with trips as (
    select * from {{ ref('stg_bike_trips') }}
),arrivals as (
    select
        return_station              as station,
        returned_hour_trun          as arrived_hour,
        returned_day_trun           as arrived_date,
        return_hour,
        returned_day_of_week        as arrived_day_of_week,
        return_month,
        return_year,
        count(*)                    as arrivals,
        avg(duration_sec)           as avg_arrival_duration_sec,
        avg(distance_m)             as avg_arrival_distance_m
    from trips
    group by 1, 2, 3, 4, 5, 6, 7
)
SELECT * FROM arrivals