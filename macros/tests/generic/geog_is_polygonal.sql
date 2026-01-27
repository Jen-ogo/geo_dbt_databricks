{% test geog_is_polygonal(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and not (
    upper(ST_AsText({{ column_name }})) like 'POLYGON(%'
    or upper(ST_AsText({{ column_name }})) like 'MULTIPOLYGON(%'
  )
{% endtest %}