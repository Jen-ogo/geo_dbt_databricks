{% test wkt_not_empty(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null
   or length(trim({{ column_name }})) = 0

{% endtest %}