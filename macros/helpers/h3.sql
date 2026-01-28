{% macro h3_r10_from_geom_point(geom_point_expr) -%}
  h3_pointash3string(ST_AsText({{ geom_point_expr }}), 10)
{%- endmacro %}

{% macro h3_r10_from_geom_centroid(geom_poly_expr) -%}
  h3_pointash3string(ST_AsText(ST_Centroid({{ geom_poly_expr }})), 10)
{%- endmacro %}

{# backwards-compat (geog_*) #}
{% macro h3_r10_from_geog_point(geog_point_col) -%}
  {{ h3_r10_from_geom_point(geog_point_col) }}
{%- endmacro %}

{% macro h3_r10_from_geog_centroid(geog_poly_col) -%}
  {{ h3_r10_from_geom_centroid(geog_poly_col) }}
{%- endmacro %}