-- Example run:
--  explicit: dbt test -s not_empty_consent_company_day
--  all related tests to the model: dbt test -s consent_company_day

{{ config(severity='error') }}

select
  count(*) as row_count
from {{ ref('consent_company_day') }}
having count(*) = 0
