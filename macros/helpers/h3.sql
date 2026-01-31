{% macro h3_from_geom_point(geom_point_expr, res) -%}
  h3_pointash3string(ST_AsText({{ geom_point_expr }}), {{ res }})
{%- endmacro %}

{% macro h3_from_geom_centroid(geom_poly_expr, res) -%}
  h3_pointash3string(ST_AsText(ST_Centroid({{ geom_poly_expr }})), {{ res }})
{%- endmacro %}

{% macro h3_from_wkt_point(wkt_point_expr, res) -%}
  h3_pointash3string({{ wkt_point_expr }}, {{ res }})
{%- endmacro %}


-- =========================
-- Backward compatible R10
-- =========================
{% macro h3_r10_from_geom_point(geom_point_expr) -%}
  {{ h3_from_geom_point(geom_point_expr, 10) }}
{%- endmacro %}

{% macro h3_r10_from_geom_centroid(geom_poly_expr) -%}
  {{ h3_from_geom_centroid(geom_poly_expr, 10) }}
{%- endmacro %}

{% macro h3_r10_from_wkt_point(wkt_point_expr) -%}
  {{ h3_from_wkt_point(wkt_point_expr, 10) }}
{%- endmacro %}

{% macro h3_r10_from_geog_point(geog_point_col) -%}
  {{ h3_r10_from_geom_point(geog_point_col) }}
{%- endmacro %}

{% macro h3_r10_from_geog_centroid(geog_poly_col) -%}
  {{ h3_r10_from_geom_centroid(geog_poly_col) }}
{%- endmacro %}


-- =========================
-- NEW: R7
-- =========================
{% macro h3_r7_from_geom_point(geom_point_expr) -%}
  {{ h3_from_geom_point(geom_point_expr, 7) }}
{%- endmacro %}

{% macro h3_r7_from_geom_centroid(geom_poly_expr) -%}
  {{ h3_from_geom_centroid(geom_poly_expr, 7) }}
{%- endmacro %}

{% macro h3_r7_from_wkt_point(wkt_point_expr) -%}
  {{ h3_from_wkt_point(wkt_point_expr, 7) }}
{%- endmacro %}

{% macro h3_r7_from_geog_point(geog_point_col) -%}
  {{ h3_r7_from_geom_point(geog_point_col) }}
{%- endmacro %}

{% macro h3_r7_from_geog_centroid(geog_poly_col) -%}
  {{ h3_r7_from_geom_centroid(geog_poly_col) }}
{%- endmacro %}