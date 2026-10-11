{#
  Fato de corridas: uma linha por corrida, inclusive as inválidas (sinalizadas).
  Incremental por mês de origem: carrega só os meses novos ou reprocessados na silver,
  apagando antes a versão anterior do mês (o pre_hook roda na mesma transação).
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        pre_hook="{% if is_incremental() %}delete from {{ this }} where source_year_month in ({{ silver_months_to_load(this) }}){% endif %}",
        indexes=[
            {'columns': ['source_year_month', 'processed_at']},
            {'columns': ['pickup_year_month', 'vendor_key']},
        ]
    )
}}

with trips as (
    select *
    from {{ ref('stg_yellow_trips') }}
    {% if is_incremental() %}
    where source_year_month in ({{ silver_months_to_load(this) }})
    {% endif %}
)

select
    -- chaves
    t.trip_id,
    coalesce(d.date_key, -1)           as date_key,
    coalesce(v.vendor_key, -1)         as vendor_key,
    coalesce(p.payment_type_key, -1)   as payment_type_key,

    -- atributos da corrida
    t.vendor_id,
    t.payment_type,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_date,
    t.pickup_year_month,
    t.pu_location_id,
    t.do_location_id,
    t.passenger_count,
    t.rate_code_id,
    t.store_and_fwd_flag,

    -- métricas
    t.trip_distance,
    t.trip_duration_minutes,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.total_amount,
    t.valid_revenue_amount,

    -- qualidade
    t.is_valid_trip,
    t.invalid_reason,
    t.is_valid_payment,
    t.is_duration_anomaly,
    t.is_distance_anomaly,

    -- linhagem
    t.source_year_month,
    t.processed_at
from trips as t
left join {{ ref('dim_date') }} as d
    on d.date_key = (extract(year from t.pickup_date) * 10000
                     + extract(month from t.pickup_date) * 100
                     + extract(day from t.pickup_date))::integer
left join {{ ref('dim_vendor') }} as v
    on v.vendor_key = t.vendor_id
left join {{ ref('dim_payment_type') }} as p
    on p.payment_type_key = t.payment_type
