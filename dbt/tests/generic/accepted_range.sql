{# Fails on values outside [min_value, max_value]. Nulls pass: missing data is reported by silver_coverage. #}
{% test accepted_range(model, column_name, min_value, max_value) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
