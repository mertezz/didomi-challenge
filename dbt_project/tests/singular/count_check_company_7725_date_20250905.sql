select *
from {{ ref('consent_company_day') }}
where company_id = '7725cda3-efd1-440b-8cc4-f80972acee43'
  and date_day = '2025-09-05'
  and event_count <> 64937