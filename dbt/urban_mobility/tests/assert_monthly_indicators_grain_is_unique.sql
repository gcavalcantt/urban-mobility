-- Grão da materialized view: uma linha por mês e fornecedor.
select year_month, vendor_id, count(*) as rows_in_grain
from {{ ref('mv_monthly_indicators') }}
group by year_month, vendor_id
having count(*) > 1
