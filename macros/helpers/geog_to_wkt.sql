{% macro geog_to_wkt(geog_col) -%}
  ST_AsText({{ geog_col }})
{%- endmacro %}