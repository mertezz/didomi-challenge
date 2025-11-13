-- Example:
--  dbt run -s +fct_consent --vars '{"start_date": "2025-11-01", "end_date": "2025-11-12"}'
--  vars: variables controlling the upstream staging table load

{{
  config(
    materialized            = 'incremental',
    unique_key              = ['consent_uq'],
    incremental_strategy    = 'delete+insert',
    on_schema_change        = 'append_new_columns',
    tags                    = ['daily', 'cm']
  )
}}

select
    -- Keys
    consent_uq,
    date_fk,
    company_fk,

    -- Attributes
    company_id,
    event_dt,

    -- Metrics
    event_count,
    event_pageview_count,
    event_consent_asked_count,
    event_consent_given_count,
    event_ui_action_count,
    consent_given_full_opt_in_count,
    consent_given_opt_out_count,
    consent_given_partial_opt_in_count,
    consent_given_empty_count,
    consent_conversion_rate,

    -- Metadata
    run_id,
    ingest_dttm,
    update_dttm,
    origin
from {{ref('stg_consent')}}