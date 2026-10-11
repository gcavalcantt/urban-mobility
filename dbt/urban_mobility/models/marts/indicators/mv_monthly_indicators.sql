{#
  Item 7: indicadores mensais por fornecedor. Materialized view: o dbt cria na
  primeira execução e faz REFRESH nas seguintes.

  Definições (todas sobre corridas válidas, competência pela data de embarque):
  - total_trips:          corridas válidas no mês
  - revenue_trips:        corridas válidas com pagamento válido (cartão ou dinheiro)
  - total_valid_revenue:  soma do total_amount das revenue_trips
  - avg_ticket:           total_valid_revenue / revenue_trips (ticket de quem gerou receita)
  - avg_distance_miles:   distância média, sem as anomalias de distância (> 100 milhas),
                          que distorcem a média (há registros com mais de 200 mil milhas)

  A view lê só a fct_trips (incremental, nunca recriada). Se dependesse de uma dimensão
  materializada como tabela, o Postgres a apagaria em cascata a cada rebuild da dimensão,
  e o dbt a recriaria do zero em vez de fazer REFRESH. O nome do fornecedor vem da
  dim_vendor no momento da consulta.
#}
select
    f.pickup_year_month                                         as year_month,
    f.vendor_key                                                as vendor_id,
    count(*)                                                    as total_trips,
    count(*) filter (where f.is_valid_payment)                  as revenue_trips,
    sum(f.valid_revenue_amount)                                 as total_valid_revenue,
    round(sum(f.valid_revenue_amount)
          / nullif(count(*) filter (where f.is_valid_payment), 0), 2)       as avg_ticket,
    round(avg(f.trip_distance) filter (where not f.is_distance_anomaly), 2) as avg_distance_miles
from {{ ref('fct_trips') }} as f
where f.is_valid_trip
group by f.pickup_year_month, f.vendor_key
