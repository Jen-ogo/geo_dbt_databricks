{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with src as (
  select
    cast(osm_id as string)            as osm_id,
    cast(nullif(name,'') as string)   as name,
    cast(nullif(ref,'')  as string)   as ref,
    cast(other_tags as string)        as other_tags_raw,
    cast(geom_wkb as binary)          as geom_wkb,

    cast(country as string)           as country,
    cast(region  as string)           as region,

    cast(dt as date)                  as dt,
    cast(source_file as string)       as source_file,
    cast(load_ts as timestamp)        as load_ts,

    {{ osm_tags_json('cast(other_tags as string)') }} as tags
  from {{ source('bronze','osm_charging') }}
  where osm_id is not null
    and geom_wkb is not null
),

base as (
  select
    concat('N', osm_id) as feature_id,

    osm_id,
    name,
    ref,

    lower(country) as region_code,
    region,

    element_at(tags,'name:en')   as name_en,
    element_at(tags,'amenity')   as amenity,
    element_at(tags,'operator')  as operator,

    case
      when lower(element_at(tags,'fee')) in ('yes','true','1') then true
      when lower(element_at(tags,'fee')) in ('no','false','0')  then false
      else null
    end as fee_bool,

    {{ osm_int('tags', 'capacity') }} as capacity,
    {{ osm_int('tags', 'socket:type2') }} as socket_type2_cnt,
    {{ osm_int('tags', 'socket:chademo') }} as socket_chademo_cnt,
    {{ osm_int('tags', 'socket:type2_combo') }} as socket_type2_combo_cnt,

    element_at(tags,'ref:EU:EVSE')      as ref_eu_evse,
    element_at(tags,'ref:EU:EVSE:pool') as ref_eu_evse_pool,

    st_setsrid(st_geomfromwkb(geom_wkb), 4326) as geom,
    st_astext(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as geom_wkt_4326,

    tags,
    other_tags_raw,

    dt,
    source_file,
    load_ts
  from src
),

final as (
  select
    *,
    coalesce(socket_type2_cnt,0)
    + coalesce(socket_chademo_cnt,0)
    + coalesce(socket_type2_combo_cnt,0) as total_sockets_cnt,

    case when coalesce(socket_chademo_cnt,0) + coalesce(socket_type2_combo_cnt,0) > 0 then true else false end as has_dc,
    case when coalesce(socket_type2_cnt,0) > 0 then true else false end as has_ac
  from base
  qualify row_number() over (
    partition by region_code, region, osm_id
    order by dt desc, load_ts desc, source_file desc
  ) = 1
)

select *
from final
where geom is not null