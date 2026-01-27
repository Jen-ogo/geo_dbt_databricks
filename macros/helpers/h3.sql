{% macro h3_r10_from_geog_point(geog_point_col) -%}

  h3_point_to_cell_string({{ geog_point_col }}, 10)
{%- endmacro %}

{% macro h3_r10_from_geog_centroid(geog_poly_col) -%}
  h3_point_to_cell_string(ST_Centroid({{ geog_poly_col }}), 10)
{%- endmacro %}