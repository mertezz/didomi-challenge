-- DELETE+INSERT strategy
-- The incremental stage table drives the load (defines the period, batch and keys for delete+insert)
-- Unique key is used to first delete records from the target fact. This enables backfills of the desired period
-- Target records are deleted based on distinct keys from the stage table
-- Full column scan of target keys is required for deletion
-- Incremental mode creates an additional _temp table from the stage
-- Event time filters are applied only in the stage table, here are not needed
-- Additional performance improvements could be achieved with dbt predicates and microbatching
-- Example:
--  dbt run -s +fct_event --vars '{"start_date": "2025-11-01", "end_date": "2025-11-12"}'

{{
  config(
    materialized            = 'incremental',
    unique_key              = ['event_uq'],
    incremental_strategy    = 'delete+insert',
    on_schema_change        = 'append_new_columns',
    tags                    = ['daily', 'cm']
  )
}}

 select
    event_uq,
    company_fk,
    country_fk,
    date_fk,
    event_id,
    user_id,
    company_id,
    deployment_id,
    event_dttm,
    event_type,
    consent_status,
    rate,
    parameters,
    sdk_type,
    domain_name,
    country,
    region_code,
    browser_family,
    device_type,
    sample_count,
    event_count,
    run_id,
    ingest_dttm,
    update_dttm,
    origin
 from {{ ref('stg_event') }}
