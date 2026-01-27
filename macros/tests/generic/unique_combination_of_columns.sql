{% test unique_combination_of_columns(model, columns) %}

select
  {{ columns | join(', ') }},
  count(*) as cnt
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}