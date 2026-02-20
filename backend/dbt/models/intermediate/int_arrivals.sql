with trips as (
    select * from {{ ref('stg_bike_trips') }}
),arrivals as (
    select
        return_station              as station,
        return_longitude            as arrival_longitude,
        return_latitude             as arrival_latitude,
        returned_hour_trun          as arrived_hour,
        returned_day_trun           as arrived_date,
        return_hour                 as arrival_hour,
        returned_day_of_week        as arrived_day_of_week,
        return_month                as arrival_month,
        return_year                 as arrival_year,
        count(*)                    as arrivals,
        avg(duration_sec)           as avg_arrival_duration_sec,
        avg(distance_m)             as avg_arrival_distance_m
    from trips
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)
SELECT * FROM arrivals