select
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
    cons.consent_conversion_rate
from {{ref('fct_consent')}} cons
left outer join {{ref('dim_company')}} comp on cons.company_fk = comp.company_sk
left outer join {{ref('dim_date')}} d on cons.date_fk = d.date_sk
where 1=1
    and current_date between comp.valid_from and comp.valid_to
    and current_date between d.valid_from and d.valid_to
