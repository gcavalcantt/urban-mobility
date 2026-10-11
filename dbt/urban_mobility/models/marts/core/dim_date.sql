-- Calendário diário. O membro -1 ("desconhecido") recebe as corridas sem data
-- válida ou fora do intervalo, para a fato nunca ter chave estrangeira nula.
with calendar as (
    select day::date as full_date
    from generate_series(
        '{{ var("date_spine_start") }}'::date,
        '{{ var("date_spine_end") }}'::date,
        interval '1 day'
    ) as day
)

select
    (extract(year from full_date) * 10000
        + extract(month from full_date) * 100
        + extract(day from full_date))::integer                     as date_key,
    full_date,
    extract(year from full_date)::integer                           as year,
    extract(quarter from full_date)::integer                        as quarter,
    extract(month from full_date)::integer                          as month,
    (array['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho',
           'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    )[extract(month from full_date)::integer]                       as month_name,
    (extract(year from full_date) * 100
        + extract(month from full_date))::integer                   as year_month,
    extract(day from full_date)::integer                            as day,
    extract(isodow from full_date)::integer                         as day_of_week,
    (array['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    )[extract(isodow from full_date)::integer]                      as day_name,
    extract(isodow from full_date) in (6, 7)                        as is_weekend
from calendar

union all

select -1, null, null, null, null, 'Desconhecido', null, null, null, 'Desconhecido', null
