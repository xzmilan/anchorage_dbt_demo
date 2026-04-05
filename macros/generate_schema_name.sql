-- Schema override: use exact schema names (bronze, silver, gold, platinum, raw)
-- without the dbt default of prepending target schema (e.g., main_bronze).
-- This keeps the DuckDB namespace clean for demo and production alike.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
