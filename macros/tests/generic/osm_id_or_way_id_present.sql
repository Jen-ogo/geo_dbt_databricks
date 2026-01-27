{% test osm_id_or_way_id_present(model) %}

select *
from {{ model }}
where osm_id is null
  and osm_way_id is null

{% endtest %}