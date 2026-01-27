{% macro wkt_to_geog(wkt_col) -%}
  coalesce(
    {{ databricks__wkt_to_geog_strict(wkt_col) }},
    {{ databricks__wkt_to_geog_allow(wkt_col) }}
  )
{%- endmacro %}

{% macro wkt_to_geog_strict(wkt_col) -%}
  case
    when {{ wkt_col }} is null then null
    when ST_IsValid(ST_GeomFromWKT({{ wkt_col }})) then ST_GeomFromWKT({{ wkt_col }})
    else null
  end
{%- endmacro %}

{% macro wkt_to_geog_allow(wkt_col) -%}
  try(ST_GeomFromWKT({{ wkt_col }}))
{%- endmacro %}