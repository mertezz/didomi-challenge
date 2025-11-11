with raw as (
    select
        "PUBLIC_API_KEY"  public_api_key,
        "INDUSTRY_BACK"  industry_back,
        "INDUSTRY_FRONT"  industry_front,
        "HQ_COUNTRY"  hq_country,
        load_time,
        load_no,
        filename
    from {{ source('raw', 'country_and_industry') }}
), default_row as (
    select
        {{ default_string() }} as company_id,
        {{ default_string() }} as industry_back,
        {{ default_string() }} as industry_front,
        {{ default_string() }} as hq_country,
        {{ default_timestamp_minimum() }} as ingest_dttm,
        {{ default_timestamp_minimum() }} as update_dttm,
        {{ default_string() }} as origin
)
select
    trim(public_api_key) company_id,
    trim(industry_back) industry_back,
    trim(industry_front) industry_front,
    trim(hq_country) hq_country,
    current_timestamp ingest_dttm,
    null update_dttm,
    filename origin
from raw
union all
select * from default_row
