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

with consents as (
    select
        *
    from {{ref('fct_consent')}} cons
    {{ incremental_filter('event_dt', start_date, end_date) }}
)
select
    cons.consent_uq,

   -- Date
    d.date_day,
    d.is_workday,
    d.year,
    d.month,
    d.week_no,

    -- Company
    comp.industry_front,
    comp.hq_country,

    -- Metrics
    cons.company_id,
    cons.event_count,
    cons.event_pageview_count,
    cons.event_consent_asked_count,
    cons.event_consent_given_count,
    cons.event_ui_action_count,
    cons.consent_given_full_opt_in_count,
    cons.consent_given_opt_out_count,
    cons.consent_given_partial_opt_in_count,
    cons.consent_given_empty_count,
    cons.consent_conversion_rate,

    -- Metadata
    {{ run_id() }} run_id,
    current_timestamp ingest_dttm,
    null update_dttm,
    'fct_consent' origin
from consents cons
left outer join {{ref('dim_company')}} comp on cons.company_fk = comp.company_sk
left outer join {{ref('dim_date')}} d on cons.date_fk = d.date_sk
where 1=1
    and current_date between comp.valid_from and comp.valid_to
    and current_date between d.valid_from and d.valid_to
