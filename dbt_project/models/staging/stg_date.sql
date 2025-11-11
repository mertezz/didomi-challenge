with date_spine as (
    select
        generate_series(
            date '2000-01-01',
            date '2100-12-31',
            interval '1 day'
        )::date as date_day
)
select
    date_day,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    extract(quarter from date_day)::int as quarter,
    extract(week from date_day)::int as week_no,
    extract(dow from date_day)::int as day_of_week,
    extract(doy from date_day)::int as day_of_year,
    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Month') as month_name,
    date_trunc('week', date_day)::date as week_start,
    (date_trunc('week', date_day) + interval '6 days')::date as week_end,
    date_trunc('month', date_day)::date as month_start,
    (date_trunc('month', date_day) + interval '1 month - 1 day')::date as month_end,
    case when extract(dow from date_day) in (6,0) then false else true end as is_workday,
    case when extract(dow from date_day) in (6,0) then true else false end as is_weekend
from date_spine
