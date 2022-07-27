{{ config(materialized='table') }}

with source_traffic_table as (
    select * from {{ source('postgres_sample_data_reference', 'traffic_table') }}
), 

final as (
    select * from source_traffic_table
)

select * from final



-- docker-compose run --rm server create_db, command to initialize postgres database to connect with redash