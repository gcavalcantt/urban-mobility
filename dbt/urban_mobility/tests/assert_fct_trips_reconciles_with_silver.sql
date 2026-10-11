-- Reconciliação: cada mês de origem precisa ter o mesmo número de corridas na silver e na fato.
-- O teste falha se retornar alguma linha.
with silver as (
    select source_year_month, count(*) as silver_rows
    from {{ source('silver', 'yellow_trips') }}
    group by source_year_month
),

gold as (
    select source_year_month, count(*) as gold_rows
    from {{ ref('fct_trips') }}
    group by source_year_month
)

select
    coalesce(s.source_year_month, g.source_year_month) as source_year_month,
    s.silver_rows,
    g.gold_rows
from silver as s
full outer join gold as g
    on g.source_year_month = s.source_year_month
where s.silver_rows is distinct from g.gold_rows
