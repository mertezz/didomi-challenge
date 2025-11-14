{{
  config(
    materialized            = 'incremental',
    unique_key              = ['event_uq'],
    incremental_strategy    = 'delete+insert',
    on_schema_change        = 'append_new_columns',
    tags                    = ['daily', 'cm']
  )
}}

-- Define daily load interval (inclusive)
-- default: yesterday → today, overridable via --vars
{% set start_date = var('start_date', "current_date - interval '1 day'") %}
{% set end_date   = var('end_date',   "current_date") %}

with raw as (
    select
        "EVENT_ID" event_id,
        "USER_ID" user_id,
        "TYPE" type,
        "RATE" rate,
        "PARAMETERS" parameters,
        "EVENT_TIME" event_time,
        "WINDOW_START" window_start,
        "APIKEY" apikey,
        "CONSENT" consent,
        "COUNT" count,
        "EXPERIMENT" experiment,
        "SDK_TYPE" sdk_type,
        "DOMAIN" domain,
        "DEPLOYMENT_ID" deployment_id,
        "COUNTRY" country_code,
        "REGION" region,
        "BROWSER_FAMILY" browser_family,
        "DEVICE_TYPE" device_type,
        load_time,
        load_no,
        filename
    from {{ source('raw', 'events') }}
    {{ incremental_filter('"EVENT_TIME"', start_date, end_date) }}
)
select
    -- Keys
    {{ dbt_utils.generate_surrogate_key(['event_id','event_time::date']) }} event_uq,
    {{ dbt_utils.generate_surrogate_key(['apikey']) }} company_fk,
    {{ dbt_utils.generate_surrogate_key(['country_code']) }} country_fk,
    {{ dbt_utils.generate_surrogate_key(["event_time::date"]) }} date_fk,

    -- Attributes (degenerative dimensions)
    event_id,
    user_id,
    apikey company_id,
    deployment_id,

    event_time event_dttm,
    type event_type,
    consent consent_status,
    rate,
    parameters,
    sdk_type,
    domain domain_name,
    btrim(country_code, '"') country_code,
    btrim(region, '"') region_code,
    browser_family,
    device_type,

    -- Metrics
    count sample_count,
    round(count/rate) event_count, -- fail-fast if count is 0

    -- Metadata
    {{ run_id() }} run_id,
    current_timestamp ingest_dttm,
    null update_dttm, -- transactional facts are immutable
    filename origin
from raw
