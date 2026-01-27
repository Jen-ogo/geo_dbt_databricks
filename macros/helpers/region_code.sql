{% macro region_code(col_country, col_region_code) -%}
  coalesce({{ col_region_code }}, {{ col_country }})
{%- endmacro %}