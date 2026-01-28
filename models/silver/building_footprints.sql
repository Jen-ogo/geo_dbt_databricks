{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with src as (
  select
    cast(osm_id as string)        as osm_id,
    cast(osm_way_id as string)    as osm_way_id,
    nullif(cast(name as string), '') as name,

    cast(building as string)      as building_native,
    cast(other_tags as string)    as other_tags_raw,

    cast(geom_wkb as binary)      as geom_wkb,

    cast(country as string)       as country,
    cast(region  as string)       as region,

    cast(dt as date)              as dt,
    cast(source_file as string)   as source_file,
    cast(load_ts as timestamp)    as load_ts,

    {{ osm_tags_json('other_tags') }} as tags
  from {{ source('bronze','osm_buildings_activity') }}
  where geom_wkb is not null
),

base as (
  select
    coalesce(
      case when osm_id is not null and trim(osm_id) <> '' then concat('N', osm_id) end,
      case when osm_way_id is not null and trim(osm_way_id) <> '' then concat('W', osm_way_id) end
    ) as feature_id,

    nullif(osm_id,'')     as osm_id,
    nullif(osm_way_id,'') as osm_way_id,

    coalesce(nullif(building_native,''), element_at(tags,'building')) as building_type,
    {{ osm_int('tags','building:levels') }} as building_levels,

    -- geometry (EPSG:4326)
    st_setsrid(st_geomfromwkb(geom_wkb), 4326) as geom,
    st_astext(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as geom_wkt_4326,

    st_centroid(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as centroid_geom,
    {{ h3_r10_from_geog_point('st_centroid(st_setsrid(st_geomfromwkb(geom_wkb), 4326))') }} as h3_r10,

    lower(country) as region_code,
    region,

    dt,
    source_file,
    load_ts,

    tags,
    other_tags_raw
  from src
),

filtered as (
  select *
  from base
  where feature_id is not null
    and geom is not null
    and building_type is not null
    and lower(building_type) <> 'no'
),

dedup as (
  select *
  from filtered
  qualify row_number() over (
    partition by region_code, region, feature_id
    order by dt desc, load_ts desc, source_file desc
  ) = 1
)

select *
from dedup
where geom_wkt_4326 is not null