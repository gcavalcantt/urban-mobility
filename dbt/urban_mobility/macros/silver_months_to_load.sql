{#
  Meses da silver que precisam ser (re)carregados na gold: os que ainda não existem
  na tabela de destino ou que foram reprocessados na silver depois da última carga.
  Usado pela fct_trips tanto para apagar quanto para inserir, então o mês é sempre
  substituído por inteiro (idempotente, nunca duplica).
#}
{% macro silver_months_to_load(target_relation) %}
    select s.source_year_month
    from (
        select distinct source_year_month, processed_at
        from {{ source('silver', 'yellow_trips') }}
    ) as s
    left join (
        select distinct source_year_month, processed_at
        from {{ target_relation }}
    ) as t
        on t.source_year_month = s.source_year_month
    where t.processed_at is null
       or s.processed_at > t.processed_at
{% endmacro %}
