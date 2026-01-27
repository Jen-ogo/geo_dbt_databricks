{% test wkt_prefix_any(model, column_name, prefixes) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and not (
    {% for p in prefixes -%}
      {{ column_name }} like '{{ p }}%'
      {%- if not loop.last %} or {% endif -%}
    {%- endfor %}
  )
{% endtest %}