{% macro nullif_neg9999(col) -%}
  nullif({{ col }}, -9999)
{%- endmacro %}