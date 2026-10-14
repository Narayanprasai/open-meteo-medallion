{#
  Unpack one bronze source into long rows: file_path, item_index, valid_time, field, value.

  Files are read as typed columns (not raw JSON text) and unpivoted, which keeps memory low:
  one month of previous runs for 12 locations and 2 models is ~250k hours x 56 fields.
  UNPIVOT INCLUDE NULLS keeps missing values so silver_coverage can report upstream gaps.
  Each source is read separately because their fields differ (e.g. weather_code only in forecast).
#}
{% macro unpack_bronze_source(source_name) %}
    {%- set file_glob = var('bronze_dir') ~ '/' ~ source_name ~ '/*/*.json' -%}
    with files as (
        select filename, json as items
        from read_json('{{ file_glob }}', format = 'unstructured', records = false,
                       filename = true, maximum_object_size = 67108864)
    ),
    items as (
        select filename, unnest(items) as item, generate_subscripts(items, 1) as item_index
        from files
    ),
    hourly as (
        select filename, item_index, unnest(item.hourly)
        from items
    ),
    wide as (
        -- the lists in one item have equal length, so unnesting them together keeps them aligned
        select filename, item_index, unnest(time) as valid_time,
               unnest(columns(* exclude (filename, item_index, time)))::double
        from hourly
    )
    select
        '{{ source_name }}' as source,
        filename           as file_path,
        item_index,
        valid_time::varchar as valid_time,
        field,
        value
    from wide
    unpivot include nulls (value for field in (columns(* exclude (filename, item_index, valid_time))))
{% endmacro %}


{# Sources that have at least one file, so a fresh checkout without e.g. forecast files still builds. #}
{% macro bronze_sources_with_files(candidates) %}
    {%- set present = [] -%}
    {%- if execute -%}
        {%- for name in candidates -%}
            {%- set result = run_query("select count(*) from glob('" ~ var('bronze_dir') ~ "/" ~ name ~ "/*/*.json')") -%}
            {%- if result.columns[0].values()[0] > 0 -%}{%- do present.append(name) -%}{%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(present) }}
{% endmacro %}
