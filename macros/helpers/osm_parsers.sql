{% macro osm_int(tags_col, key) -%}
  try_cast(regexp_extract(element_at({{ tags_col }}, '{{ key }}'), '([0-9]+)', 1) as int)
{%- endmacro %}

{% macro osm_double(tags_col, key) -%}
  try_cast(regexp_extract(element_at({{ tags_col }}, '{{ key }}'), '([0-9]+(?:\\.[0-9]+)?)', 1) as double)
{%- endmacro %}

{% macro osm_bool(tags_col, key, default=false) -%}
  case
    when lower(coalesce(element_at({{ tags_col }}, '{{ key }}'), '')) in ('yes','true','1') then true
    when lower(coalesce(element_at({{ tags_col }}, '{{ key }}'), '')) in ('no','false','0') then false
    else {{ 'true' if default else 'false' }}
  end
{%- endmacro %}

{% macro osm_speed_kph(tags_col, key) -%}
  case
    when element_at({{ tags_col }}, '{{ key }}') is null then null
    when lower(element_at({{ tags_col }}, '{{ key }}')) like '%mph%' then
      try_cast(regexp_extract(element_at({{ tags_col }}, '{{ key }}'), '([0-9]+(?:\\.[0-9]+)?)', 1) as double) * 1.60934
    else
      try_cast(regexp_extract(element_at({{ tags_col }}, '{{ key }}'), '([0-9]+(?:\\.[0-9]+)?)', 1) as double)
  end
{%- endmacro %}