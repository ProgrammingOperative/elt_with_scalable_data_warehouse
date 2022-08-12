{{ config(materialized='table') }}

with source_person as(
    SELECT * FROM {{ source('masterdb', 'person') }}
),

with source_car as (
    SELECT * FROM {{ source('masterdb', 'car') }}
)

final as (
    SELECT * FROM source_person
)

select * FROM final