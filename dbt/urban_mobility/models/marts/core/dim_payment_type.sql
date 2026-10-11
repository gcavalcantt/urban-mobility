-- Tipos de pagamento: os 6 da tabela de referência da prova, o código 0 do dicionário
-- da TLC ("Flex Fare trip", que existe nos dados de 2025 mas não está na tabela da prova)
-- e o membro -1 para códigos ausentes ou inexistentes.
with reference_table as (
    select
        payment_type,
        payment_description,
        is_valid_payment,
        true as is_in_reference_table
    from {{ ref('stg_payment_type') }}
),

tlc_codes_outside_reference as (
    select
        0                 as payment_type,
        'Flex Fare trip'  as payment_description,
        false             as is_valid_payment,   -- fora da tabela da prova: não conta como receita
        false             as is_in_reference_table
)

select payment_type as payment_type_key, * from reference_table
union all
select payment_type as payment_type_key, * from tlc_codes_outside_reference
union all
select -1, null, 'Desconhecido', false, false
