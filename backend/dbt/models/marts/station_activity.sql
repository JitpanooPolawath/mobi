-- Per station - per hour
-- FastAPI queries this for station markers (sized/colored by activity)
-- and for the station detail panel

with station_metrics as (
    select * from {{ ref('int_station_metrics') }}
),

-- Compute trips_per_hour ratio used to size/color map markers
final as (
    select
        station,
        departed_hour,
        departed_date,
        hour_of_day,
        day_of_week,
        month_of_year,
        year,
        departures,
        arrivals,
        total_activity,
        avg_duration_sec,
        avg_distance_m,
        electric_departures,

        -- Trips per hour (total activity / 1 since we're already at hourly grain)
        total_activity                              as trips_per_hour,

        -- Rolling 7-day average activity for trend context (smoothing)
        avg(total_activity) over (
            partition by station, hour_of_day
            order by departed_date
            rows between 6 preceding and current row
        )                                           as rolling_7d_avg_activity

    from station_metrics
)

select * from final
order by departed_hour, station