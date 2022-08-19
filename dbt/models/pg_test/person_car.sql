{{ config(materialized='table') }}

with source_person as(
    SELECT * FROM {{ source('masterdb', 'person') }}
),

source_car as (
    SELECT * FROM {{ source('masterdb', 'car') }}
),

joined as (
    SELECT * FROM source_person LEFT JOIN source_car  USING (car_uid)
),

final as (
    SELECT * FROM joined
)

select * FROM final