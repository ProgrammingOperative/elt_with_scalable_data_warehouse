with source_traffic_table as (
    select * from {{ source('postgres_sample_data_reference', 'traffic_table') }}
), 

final as (
    select * from source_traffic_table
)

select * from final