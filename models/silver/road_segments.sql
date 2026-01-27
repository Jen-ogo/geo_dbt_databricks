{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with src as (
  select
    b.*,
    {{ osm_tags_json('b.other_tags') }} as tags
  from {{ source('bronze','osm_roads') }} b
  where b.osm_id is not null
    and b.geom_wkb is not null
    and b.highway is not null
),

base as (
  select
    concat('W', cast(osm_id as string)) as feature_id,
    cast(osm_id as string)             as osm_id,
    nullif(cast(name as string), '')   as name,

    cast(highway as string)            as highway,
    cast(z_order as int)               as z_order,
    cast(other_tags as string)         as other_tags_raw,

    element_at(tags,'ref')             as ref,

    -- access/service
    coalesce(
      element_at(tags,'motorcar'),
      element_at(tags,'motor_vehicle'),
      element_at(tags,'vehicle'),
      element_at(tags,'access')
    ) as access_raw,
    element_at(tags,'service')         as service,

    -- typed fields (tolerant parsing)
    lower(coalesce(element_at(tags,'oneway'), 'no')) as oneway_raw,
    {{ osm_bool('tags','oneway', default=false) }}   as oneway,

    {{ osm_int('tags','lanes') }}                   as lanes,

    element_at(tags,'surface')                      as surface,
    {{ osm_bool('tags','lit', default=false) }}     as lit,
    {{ osm_bool('tags','bridge', default=false) }}  as bridge,
    {{ osm_bool('tags','tunnel', default=false) }}  as tunnel,

    -- layer может быть "-1", "1;2", "ground" → берём первое число со знаком
    try_cast(regexp_extract(element_at(tags,'layer'), '(-?[0-9]+)', 1) as int) as layer,

    element_at(tags,'maxspeed')                     as maxspeed_raw,
    {{ osm_speed_kph('tags','maxspeed') }}          as maxspeed_kph,

    -- geometry
    st_setsrid(st_geomfromwkb(geom_wkb), 4326)      as geom,
    st_astext(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as geom_wkt_4326,

    -- canonical region fields
    cast(country as string) as region_code,
    cast(region  as string) as region,

    cast(dt as date)        as dt,
    cast(source_file as string) as source_file,
    cast(load_ts as timestamp)  as load_ts
  from src
),

filtered as (
  select *
  from base
  where lower(highway) not in ('footway','path','steps','corridor','bridleway','cycleway','pedestrian')
    and not (
      lower(highway) = 'service'
      and lower(coalesce(service,'')) in ('driveway','parking_aisle','alley','emergency_access','private')
    )
    and lower(coalesce(access_raw,'yes')) not in ('no','private')
),

dedup as (
  select *
  from filtered
  qualify row_number() over (
    partition by region_code, region, osm_id
    order by dt desc, load_ts desc, source_file desc
  ) = 1
)

select *
from dedup
where geom_wkt_4326 is not null