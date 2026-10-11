select
    payment_type,
    payment_description,
    is_valid_payment
from {{ source('silver', 'payment_type') }}
