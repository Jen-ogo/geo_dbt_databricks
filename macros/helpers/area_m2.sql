{# Public API #}
{% macro area_m2(geom_col) -%}
  {{ adapter.dispatch('area_m2')(geom_col) }}
{%- endmacro %}

{# Shared implementation (EPSG:3035) #}
{% macro area_3035_m2(geom_col) -%}
  ST_Area(ST_Transform({{ geom_col }}, 'EPSG:3035'))
{%- endmacro %}

{# Adapter-specific entrypoints that dispatch will look for #}
{% macro databricks__area_m2(geom_col) -%}
  {{ area_3035_m2(geom_col) }}
{%- endmacro %}

{% macro spark__area_m2(geom_col) -%}
  {{ area_3035_m2(geom_col) }}
{%- endmacro %}

{% macro default__area_m2(geom_col) -%}
  {{ area_3035_m2(geom_col) }}
{%- endmacro %}