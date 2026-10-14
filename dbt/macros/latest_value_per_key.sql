{#
  Business dedup for silver: one value per key, from the most recent download.
  A non null value beats a null one, so a later download that is still empty (e.g. ERA5 not yet
  published) never overwrites a real value, and late arriving values replace earlier nulls.
#}
{% macro latest_value_per_key(source_name, key_columns) %}
    select *
    from {{ ref('stg_bronze__hourly_values') }}
    where source = '{{ source_name }}'
    qualify row_number() over (
        partition by {{ key_columns | join(', ') }}
        order by (value is null), ingested_at_utc desc, request_id
    ) = 1
{% endmacro %}
