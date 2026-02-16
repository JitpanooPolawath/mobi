-- Undirected routes
-- FastAPI queries this for flow lines on the map

with route_flows as (
    select * from {{ ref('int_route_flows') }}
),

-- Overall ranking
-- Top N routes per day and hours
ranked as (
    select
        station_a,
        station_b,
        route_key,
        departed_hour,
        departed_date,
        hour_of_day,
        day_of_week,
        month_of_year,
        year,
        trip_count,
        avg_duration_sec,
        avg_distance_m,
        electric_trips,

        -- Rank routes within each hour (1 = busiest route that hour)
        rank() over (
            partition by departed_hour
            order by trip_count desc
        )                                           as hourly_rank,

        -- Rank routes within each day
        rank() over (
            partition by departed_date
            order by trip_count desc
        )                                           as daily_rank

    from route_flows
)

select * from ranked
order by departed_hour, hourly_rank