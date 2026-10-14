{# Every configured location appears in the model. #}
{% test has_all_locations(model, column_name) %}
    select l.location_id
    from {{ ref('dim_location') }} l
    left join (select distinct {{ column_name }} as location_id from {{ model }}) m using (location_id)
    where m.location_id is null
{% endtest %}
