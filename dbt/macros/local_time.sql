{# Naive UTC timestamp -> naive local wall clock time in an IANA zone (handles DST and :30 offsets). #}
{% macro to_local(ts_utc, tz) -%}
    timezone({{ tz }}, timezone('UTC', {{ ts_utc }}))
{%- endmacro %}

{# Length of a local calendar day in hours: 23 on DST start, 25 on DST end, otherwise 24. #}
{% macro local_day_hours(local_date, tz) -%}
    ((epoch(timezone({{ tz }}, ({{ local_date }} + 1)::timestamp))
      - epoch(timezone({{ tz }}, {{ local_date }}::timestamp))) / 3600)::integer
{%- endmacro %}

{# Southern Hemisphere meteorological seasons. #}
{% macro au_season(month) -%}
    case when {{ month }} in (12, 1, 2) then 'summer'
         when {{ month }} in (3, 4, 5)  then 'autumn'
         when {{ month }} in (6, 7, 8)  then 'winter'
         else 'spring' end
{%- endmacro %}
