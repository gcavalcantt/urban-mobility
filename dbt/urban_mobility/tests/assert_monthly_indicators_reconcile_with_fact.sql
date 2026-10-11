-- A receita somada na materialized view precisa bater com a fato (tolerância de 1 centavo).
with fact as (
    select coalesce(sum(valid_revenue_amount), 0) as revenue
    from {{ ref('fct_trips') }}
    where is_valid_trip
),

indicators as (
    select coalesce(sum(total_valid_revenue), 0) as revenue
    from {{ ref('mv_monthly_indicators') }}
)

select fact.revenue as fact_revenue, indicators.revenue as indicators_revenue
from fact
cross join indicators
where abs(fact.revenue - indicators.revenue) > 0.01
