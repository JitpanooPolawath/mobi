-- with station_metrics as (
--     select * from {{ ref('int_station_location') }}
-- ),

-- -- Calculate peak hours per station
-- peak_hours as (
--     select
--         station,
--         hour_of_day,
--         sum(total_activity) as total_hourly_activity
--     from station_metrics
--     group by 1, 2
-- ),

-- peak_hours_ranked as (
--     select
--         station,
--         hour_of_day,
--         total_hourly_activity,
--         rank() over (partition by station order by total_hourly_activity desc) as hour_rank
--     from peak_hours
-- ),

-- top_peak_hours as (
--     select
--         station,
--         hour_of_day as peak_hour,
--         total_hourly_activity as peak_hour_activity
--     from peak_hours_ranked
--     where hour_rank = 1
-- ),

-- -- Calculate rolling averages for trends
-- final as (
--     select
--         sm.station,
--         sm.activity_hour,
--         sm.activity_date,
--         sm.hour_of_day,
--         sm.day_of_week,
--         sm.month_of_year,
--         sm.year,
        
--         sm.departures,
--         sm.arrivals,
--         sm.total_activity,
        
--         sm.avg_departure_duration_sec,
--         sm.avg_departure_distance_m,
--         sm.avg_arrival_duration_sec,
--         sm.avg_arrival_distance_m,
--         sm.electric_departures,
        
--         -- Trips per hour metric for map visualization
--         sm.total_activity as trips_per_hour,
        
--         -- Peak hour info
--         ph.peak_hour,
--         ph.peak_hour_activity,
        
--         -- Rolling 7-day average for trend analysis
--         avg(sm.total_activity) over (
--             partition by sm.station, sm.hour_of_day
--             order by sm.activity_date
--             rows between 6 preceding and current row
--         ) as rolling_7d_avg_activity,
        
--         -- Rolling 30-day average
--         avg(sm.total_activity) over (
--             partition by sm.station
--             order by sm.activity_date
--             rows between 29 preceding and current row
--         ) as rolling_30d_avg_activity
        
--     from station_metrics sm
--     left join top_peak_hours ph
--         on sm.station = ph.station
-- )

-- select * from final
-- order by activity_hour, station