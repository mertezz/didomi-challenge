with stg as (
    select *
    from {{ ref('stg_company') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['company_id']) }} company_sk,

    -- SCD
    {{ default_timestamp_minimum() }} valid_from,
    {{ default_timestamp_maximum() }} valid_to,

    -- Natural keys
    coalesce(company_id, {{ default_string() }}) company_id,

    -- Attributes
    coalesce(industry_back, {{ default_string() }}) industry_back,
    coalesce(industry_front, {{ default_string() }}) industry_front,
    coalesce(hq_country, {{ default_string() }}) hq_country,

    -- Metadata
    {{ run_id() }} run_id,
    current_timestamp ingest_dttm,
    null update_dttm,
    origin
from stg
