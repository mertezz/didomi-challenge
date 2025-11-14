{{
  config(
    materialized            = 'incremental',
    unique_key              = ['consent_uq'],
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
    select *
    from {{ref('fct_event')}}
    {{ incremental_filter('event_dttm', start_date, end_date) }}
),  trf as (
    -- Calculate time-to-consent for `consent.asked` events.
    -- Matching is based on `user_id` and chronological order (`event_dttm`) of events.
    select
       case
        when event_type = 'consent.asked' and event_type_next = 'consent.given'
            then  floor(extract(epoch from (event_dttm_next - event_dttm)))
       end lag_consent_asked_given_second,
       event_dttm::date event_dt,
       *
    from (
        select
            lead(event_type) over (partition by user_id order by event_dttm) event_type_next,
            lead(event_dttm) over (partition by user_id order by event_dttm) event_dttm_next,
            *
        from raw
    ) r
), totals as (
    select
        company_id,
        event_dt,

        -- General
        sum(event_count) event_count,
        sum(case when event_type = 'pageview' then event_count end) event_pageview_count,
        sum(case when event_type = 'consent.asked' then event_count end) event_consent_asked_count,
        sum(case when event_type = 'consent.given' then event_count end) event_consent_given_count,
        sum(case when event_type = 'ui.action' then event_count end) event_ui_action_count,
        avg(case when event_type = 'consent.asked' then lag_consent_asked_given_second end) lag_consent_asked_given_second_avg,

        -- Consent
        sum(case when event_type = 'consent.given' and consent_status = 'full opt-in' then event_count end) consent_given_full_opt_in_count,
        sum(case when event_type = 'consent.given' and consent_status = 'opt-out' then event_count end) consent_given_opt_out_count,
        sum(case when event_type = 'consent.given' and consent_status = 'partial opt-in' then event_count end) consent_given_partial_opt_in_count,
        sum(case when event_type = 'consent.given' and consent_status = 'empty' then event_count end) consent_given_empty_count
    from trf t
    group by company_id, event_dt
), metrics as (
   select
      *,
      round((event_consent_given_count::numeric / event_consent_asked_count::numeric),2) consent_conversion_rate
   from totals
)
select
    -- Keys
    {{ dbt_utils.generate_surrogate_key(['company_id', 'event_dt']) }} consent_uq,
    {{ dbt_utils.generate_surrogate_key(['company_id']) }} company_fk,
    {{ dbt_utils.generate_surrogate_key(['event_dt']) }} date_fk,

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
    -- lag_consent_asked_given_second_avg, -- unreliable data, metric is left out

    -- Metadata
    {{ run_id() }} run_id,
    current_timestamp ingest_dttm,
    null update_dttm, -- transactional facts are immutable
    'fct_event' origin
from  metrics m
