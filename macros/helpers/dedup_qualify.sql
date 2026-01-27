{% macro dedup_qualify(partition_by, order_by) -%}
qualify row_number() over (
  partition by {{ partition_by | join(', ') }}
  order by {{ order_by | join(', ') }}
) = 1
{%- endmacro %}