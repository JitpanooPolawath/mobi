with trips as (
    select * from {{ ref('stg_bike_trips') }}
)