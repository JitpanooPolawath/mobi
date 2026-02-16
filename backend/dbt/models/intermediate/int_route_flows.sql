-- (A<->B treated as same route)

with trips as (
    select * from {{ ref('stg_bike_trips') }}
),

-- Station name normalize (A->B and B->A) are the same pairs
normalized as (
    select
        case
            when departure_station <= return_station
            then departure_station
            else return_station
        end                                         as station_a,

        case
            when departure_station <= return_station
            then return_station
            else departure_station
        end                                         as station_b,

        departed_hour,
        departed_date,
        hour_of_day,
        day_of_week,
        month_of_year,
        year,
        duration_sec,
        distance_m,
        is_electric
    from trips
    -- Exclude round trips (same station departure and return)
    where departure_station != return_station
),

aggregated as (
    select
        station_a,
        station_b,
        station_a || ' <> ' || station_b            as route_key,
        departed_hour,
        departed_date,
        hour_of_day,
        day_of_week,
        month_of_year,
        year,
        count(*)                                    as trip_count,
        avg(duration_sec)                           as avg_duration_sec,
        avg(distance_m)                             as avg_distance_m,
        sum(case when is_electric then 1 else 0 end) as electric_trips
    from normalized
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select * from aggregated