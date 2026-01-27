{% macro safe_num(col) -%}
  try_cast({{ col }} as double)
{%- endmacro %}