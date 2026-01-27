{% test geog_is_point(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and upper(ST_AsText({{ column_name }})) not like 'POINT(%'
{% endtest %}