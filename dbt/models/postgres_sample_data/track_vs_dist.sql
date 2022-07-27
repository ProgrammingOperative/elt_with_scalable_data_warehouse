
-- Use the `ref` function to select from other models

select track_id, traveled_d
from {{ ref('postgres_sample_data_traffic_table') }}