with raw as (
    select *
    from {{ source('raw', 'countries') }}
), default_row as (
    select
        gen_random_uuid() id,
        {{ default_string() }} name,
        {{ default_string() }} code,
        {{ default_int64() }} code_numeric,
        {{ default_string() }} region,
        {{ default_string() }} subregion
)
select
    gen_random_uuid() id,
    name,
    alpha_2 code,
    -- alpha_3 as iso_alpha3,
    country_code code_numeric,
    region,
    sub_region as subregion
from raw
union all
select * from default_row
