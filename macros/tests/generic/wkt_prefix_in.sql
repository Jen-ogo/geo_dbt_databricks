{% test wkt_prefix_in(model, column_name, prefixes) %}
select *
from {{ model }}
where {{ column_name }} is null
   or trim({{ column_name }}) = ''
   or (
        {% for p in prefixes %}
          upper({{ column_name }}) not like '{{ p }}%' {% if not loop.last %} and {% endif %}
        {% endfor %}
      )
{% endtest %}