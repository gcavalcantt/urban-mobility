-- Sem esta macro, o dbt concatena o schema do profile com o do modelo e cria gold_gold

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
