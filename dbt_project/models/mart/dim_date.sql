select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} date_sk,

    -- SCD
    {{ default_timestamp_minimum() }} valid_from,
    {{ default_timestamp_maximum() }} valid_to,

    -- Natural key
    date_day,

    -- Attributes
    year,
    quarter,
    month,
    week_no,
    day_of_week,
    day_of_year,
    day_name,
    month_name,
    week_start,
    week_end,
    month_start,
    month_end,

    -- Flags
    is_workday,
    is_weekend,

    -- Metadata
    {{ run_id() }} run_id,
    current_timestamp ingest_dttm,
    null update_dttm,
    {{ default_string() }} origin
from {{ ref('stg_date') }}
union all
select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['1900-01-01']) }} date_sk,

    -- SCD
    {{ default_timestamp_minimum() }} valid_from,
    {{ default_timestamp_maximum() }} valid_to,

    -- Natural key
    {{ default_date_minimum() }} date_day,

    -- Attributes
    {{ default_int64() }} as year,
    {{ default_int64() }} quarter,
    {{ default_int64() }} as month,
    {{ default_int64() }} week_no,
    {{ default_int64() }} day_of_week,
    {{ default_int64() }} day_of_year,
    {{ default_string() }} day_name,
    {{ default_string() }} month_name,
    {{ default_date_minimum() }} week_start,
    {{ default_date_minimum() }} week_end,
    {{ default_date_minimum() }} month_start,
    {{ default_date_minimum() }} month_end,
    -- Flags
    false is_workday,
    false is_weekend,

    -- Metadata
    {{ run_id() }} run_id,
    current_timestamp ingest_dttm,
    null update_dttm,
    {{ default_string() }} origin
