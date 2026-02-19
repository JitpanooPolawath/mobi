with source as (
    select * from {{ source('raw', 'raw_bike_trips') }}
),

-- Snowflake
-- cleaned as (
--     select
--         to_timestamp(LEFT("departure",10)) as departed_at,
--         to_timestamp(LEFT("return",10))  as returned_at,

--         bike                                  as bike_id,
--         coalesce(electric_bike, false)                  as is_electric,

--         departure_station                         as departure_station,
--         return_station                          as return_station,

--         membership_type                           as membership_type,

--         covered_distance_m::float                         as distance_m,
--         duration_sec::float                               as duration_sec,
--         departure_temperature_c::float                    as departure_temp_c,
--         return_temperature_c::float                       as return_temp_c,
--         stopover_duration_sec::float                      as stopover_duration_sec,
--         number_of_stopovers::float                        as number_of_stopovers,


--         date_trunc('hour',to_timestamp(LEFT("departure",10))) as departed_hour, 
--         date_trunc('day',to_timestamp(LEFT("departure",10))) as departed_date,
--         dayofweek(to_timestamp(LEFT("departure",10))) as departed_day_of_week,
--         hour(to_timestamp(LEFT("departure",10))) as departure_hour,
--         month(to_timestamp(LEFT("departure",10))) as departure_month,
--         year(to_timestamp(LEFT("departure",10))) as departure_year,
--         -- return
--         date_trunc('hour',to_timestamp(LEFT("return",10))) as returned_hour_trun, 
--         date_trunc('day',to_timestamp(LEFT("return",10))) as returned_day_trun,
--         dayofweek(to_timestamp(LEFT("return",10))) as returned_day_of_week,
--         hour(to_timestamp(LEFT("return",10))) as return_hour,
--         month(to_timestamp(LEFT("return",10))) as return_month,
--         year(to_timestamp(LEFT("return",10))) as return_year,

--         source_file,
--         loaded_at

--     from source
--     where
--         departure       is not null
--         and departure_station is not null
--         and return_station    is not null
-- )


-- Postgres
stations as (
    SELECT * FROM {{ source('raw', 'stations') }}
),
cleaned as (
    SELECT
        departure AS departed_at,
        "return" AS returned_at,
        bike AS bike_id,
        COALESCE(electric_bike, FALSE) AS is_electric,
        REGEXP_REPLACE(departure_station, '^[0-9]{4}\s', '') AS departure_station,
        REGEXP_REPLACE(return_station, '^[0-9]{4}\s', '') AS return_station,
        membership_type,
        covered_distance_m::FLOAT AS distance_m,
        duration_sec::FLOAT AS duration_sec,
        departure_temperature_c::FLOAT AS departure_temp_c,
        return_temperature_c::FLOAT AS return_temp_c,
        stopover_duration_sec::FLOAT AS stopover_duration_sec,
        number_of_stopovers::FLOAT AS number_of_stopovers,

        DATE_TRUNC('hour', departure) AS departed_hour, 
        DATE_TRUNC('day', departure) AS departed_date,
        EXTRACT(DOW FROM departure) AS departed_day_of_week,
        EXTRACT(HOUR FROM departure) AS departure_hour,
        EXTRACT(MONTH FROM departure) AS departure_month,
        EXTRACT(YEAR FROM departure) AS departure_year,

        DATE_TRUNC('hour', "return") AS returned_hour_trun, 
        DATE_TRUNC('day', "return") AS returned_day_trun,
        EXTRACT(DOW FROM "return") AS returned_day_of_week,
        EXTRACT(HOUR FROM "return") AS return_hour,
        EXTRACT(MONTH FROM "return") AS return_month,
        EXTRACT(YEAR FROM "return") AS return_year,

        source_file
    FROM source
    WHERE departure IS NOT NULL
      AND departure_station IS NOT NULL
      AND departure_station != 'nan'
      AND return_station IS NOT NULL
      AND return_station != 'nan'
)

-- Create a materialize view (Faster read and won't be update a lot)
select * from cleaned