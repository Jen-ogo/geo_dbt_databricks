{% macro h3_r10_from_geog_point(point_geom_expr) -%}
  h3_pointash3string(st_aswkb({{ point_geom_expr }}), 10)
{%- endmacro %}

{% macro h3_r10_from_geog_centroid(poly_geom_expr) -%}
  h3_pointash3string(st_aswkb(st_centroid({{ poly_geom_expr }})), 10)
{%- endmacro %}