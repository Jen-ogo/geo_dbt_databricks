{% macro area_m2(geom_col) -%}
  {{ adapter.dispatch('area_m2')(geom_col) }}
{%- endmacro %}

{% macro databricks__area_m2(geom_col) -%}
  ST_Area(ST_Transform({{ geom_col }}, 3035))
{%- endmacro %}

{% macro spark__area_m2(geom_col) -%}
  ST_Area(ST_Transform({{ geom_col }}, 3035))
{%- endmacro %}

{% macro default__area_m2(geom_col) -%}
  ST_Area(ST_Transform({{ geom_col }}, 3035))
{%- endmacro %}