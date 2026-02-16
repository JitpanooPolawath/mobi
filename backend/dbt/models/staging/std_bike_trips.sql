with source as (
    select * from {{ source('raw', 'raw_bike_trips') }}
),

cleaned as (
    select
        departed_at,
        returned_at,

        bike::int                                       as bike_id,
        coalesce(electric_bike, false)                  as is_electric,

        trim(departure_station)                         as departure_station,
        trim(return_station)                            as return_station,

        trim(membership_type)                           as membership_type,

        covered_distance_m::float                         as distance_m,
        duration_sec::float                               as duration_sec,
        departure_temperature_c::float                    as departure_temp_c,
        return_temperature_c::float                       as return_temp_c,
        stopover_duration_sec::float                      as stopover_duration_sec,
        number_of_stopovers::float                        as number_of_stopovers,

        date_trunc('hour', departure)                   as departed_hour,
        date_trunc('day',  departure)                   as departed_date,
        dayofweek(departure)                            as day_of_week,
        hour(departure)                                 as hour_of_day,
        month(departure)                                as month_of_year,
        year(departure)                                 as year,

        source_file,
        loaded_at

    from source
    where
        departure       is not null
        and departure_station is not null
        and return_station    is not null
)

-- Create a materialize view (Faster read and won't be update a lot)
select * from cleaned