{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata') }}
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid, 

    -- timestamps
    TIMESTAMP_MICROS(CAST(pickup_datetime / 1000 as int64)) as pickup_datetime,
    TIMESTAMP_MICROS(CAST(dropoff_datetime / 1000 as int64)) as dropoff_datetime,
    
    -- other columns
    sr_flag,
    affiliated_base_number,
    __index_level_0__

from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
