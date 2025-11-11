with raw as (
    select *
    from {{ source('raw', 'countries') }}
), default_row as (
    select
        {{ default_string() }} name,
        {{ default_string() }} code,
        {{ default_int64() }} code_numeric,
        {{ default_string() }} region,
        {{ default_string() }} subregion,
        {{ default_timestamp_minimum() }} ingest_dttm,
        {{ default_timestamp_minimum() }} update_dttm,
        {{ default_string() }} origin
)
select
    name,
    alpha_2 code,
    -- alpha_3 as iso_alpha3,
    country_code code_numeric,
    region,
    sub_region as subregion,
    current_timestamp ingest_dttm,
    null update_dttm,
    'seed' origin
from raw
union all
select * from default_row
