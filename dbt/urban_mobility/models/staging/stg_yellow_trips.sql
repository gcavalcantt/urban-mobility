-- Leitura padronizada da silver + chave surrogate da corrida.
-- A fonte não tem identificador único: a chave é o hash do arquivo de origem com todas
-- as colunas originais (o job da silver mede duplicatas exatas, e o teste unique da
-- fct_trips falha se alguma aparecer).
select
    md5(concat_ws('|',
        source_file,
        coalesce(vendor_id::text, '∅'),
        coalesce(pickup_datetime::text, '∅'),
        coalesce(dropoff_datetime::text, '∅'),
        coalesce(passenger_count::text, '∅'),
        coalesce(trip_distance::text, '∅'),
        coalesce(rate_code_id::text, '∅'),
        coalesce(store_and_fwd_flag, '∅'),
        coalesce(pu_location_id::text, '∅'),
        coalesce(do_location_id::text, '∅'),
        coalesce(payment_type::text, '∅'),
        coalesce(fare_amount::text, '∅'),
        coalesce(extra::text, '∅'),
        coalesce(mta_tax::text, '∅'),
        coalesce(tip_amount::text, '∅'),
        coalesce(tolls_amount::text, '∅'),
        coalesce(improvement_surcharge::text, '∅'),
        coalesce(total_amount::text, '∅'),
        coalesce(congestion_surcharge::text, '∅'),
        coalesce(airport_fee::text, '∅'),
        coalesce(cbd_congestion_fee::text, '∅')
    )) as trip_id,
    *
from {{ source('silver', 'yellow_trips') }}
