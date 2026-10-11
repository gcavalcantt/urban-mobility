-- Regra de negócio: só há receita válida em corrida válida com pagamento válido.
select trip_id, is_valid_trip, is_valid_payment, valid_revenue_amount
from {{ ref('fct_trips') }}
where valid_revenue_amount <> 0
  and not (is_valid_trip and is_valid_payment)
