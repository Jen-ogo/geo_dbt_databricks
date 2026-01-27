{% test values_in_or_null(model, column_name, values) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not in (
    {% for v in values %}'{{ v }}'{% if not loop.last %},{% endif %}{% endfor %}
  )
{% endtest %}