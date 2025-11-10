with stg as (
    select *
    from {{ ref('country_stg') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['code']) }} country_sk,

    -- Natural/business keys
    coalesce(code, {{ default_string() }}) country_code,
    coalesce(code_numeric, {{ default_int64() }}) country_code_numeric,

    -- Attributes
    coalesce(name, {{ default_string() }}) country_name,
    coalesce(region, {{ default_string() }}) region,
    coalesce(subregion, {{ default_string() }}) subregion,

    -- Metadata
    {{ run_id() }} run_id,
    {{ default_timestamp_minimum() }} valid_from,
    {{ default_timestamp_maximum() }} valid_to
from stg