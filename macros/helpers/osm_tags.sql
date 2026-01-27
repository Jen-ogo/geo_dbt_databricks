{% macro osm_tags_json(other_tags_col) -%}
  case
    when {{ other_tags_col }} is null or trim({{ other_tags_col }}) = '' then null
    else
      from_json(
        concat('{', replace({{ other_tags_col }}, '"=>"', '":"'), '}'),
        'map<string,string>'
      )
  end
{%- endmacro %}

{% macro tags_get(tags_col, key) -%}
  element_at({{ tags_col }}, '{{ key }}')
{%- endmacro %}

{% macro tags_int(tags_col, key) -%}
  try_cast({{ tags_get(tags_col, key) }} as int)
{%- endmacro %}