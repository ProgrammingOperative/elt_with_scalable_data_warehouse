{{ config(materialized='table') }}

with source_person as(
    SELECT * FROM {{ source('masterdb', 'person') }}
),

final as (
    SELECT * FROM source_person
)

select * FROM final