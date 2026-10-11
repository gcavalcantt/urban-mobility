-- Fornecedores da TLC (seed) + membro -1 para códigos ausentes ou fora do domínio
select
    vendor_id    as vendor_key,
    vendor_id,
    vendor_name
from {{ ref('tlc_vendors') }}

union all

select -1, null, 'Desconhecido'
