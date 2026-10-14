{# Use the configured schema name as is (bronze, staging, silver, gold) instead of dbt's default
   "<target>_<custom>" prefixing, so layer names match the medallion architecture. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
