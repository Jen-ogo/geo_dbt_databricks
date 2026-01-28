{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code']
) }}

with src as (
  select
    cast(osm_id as string) as osm_id,
    cast(osm_way_id as string) as osm_way_id,
    cast(nullif(name,'') as string) as name,

    cast(amenity as string) as amenity,
    cast(shop as string) as shop,
    cast(office as string) as office,
    cast(tourism as string) as tourism,
    cast(leisure as string) as leisure,
    cast(sport as string) as sport,
    cast(craft as string) as craft,
    cast(building as string) as building,

    cast(other_tags as string) as other_tags_raw,
    cast(geom_wkb as binary) as geom_wkb,

    cast(lower(country) as string) as region_code,
    cast(region as string) as region,

    cast(source_file as string) as source_file,
    cast(load_ts as timestamp) as load_ts,

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

    nullif(trim(osm_id),'') as osm_id,
    nullif(trim(osm_way_id),'') as osm_way_id,

    name,
    element_at(tags,'name:en') as name_en,

    case
      when coalesce(nullif(amenity,''), element_at(tags,'amenity')) is not null then 'amenity'
      when coalesce(nullif(shop,''), element_at(tags,'shop')) is not null then 'shop'
      when coalesce(nullif(office,''), element_at(tags,'office')) is not null then 'office'
      when coalesce(nullif(tourism,''), element_at(tags,'tourism')) is not null then 'tourism'
      when coalesce(nullif(leisure,''), element_at(tags,'leisure')) is not null then 'leisure'
      when coalesce(nullif(sport,''), element_at(tags,'sport')) is not null then 'sport'
      when coalesce(nullif(craft,''), element_at(tags,'craft')) is not null then 'craft'
      when coalesce(nullif(building,''), element_at(tags,'building')) is not null then 'building'
      else null
    end as activity_class,

    lower(coalesce(
      nullif(amenity,''), element_at(tags,'amenity'),
      nullif(shop,''), element_at(tags,'shop'),
      nullif(office,''), element_at(tags,'office'),
      nullif(tourism,''), element_at(tags,'tourism'),
      nullif(leisure,''), element_at(tags,'leisure'),
      nullif(sport,''), element_at(tags,'sport'),
      nullif(craft,''), element_at(tags,'craft'),
      nullif(building,''), element_at(tags,'building')
    )) as activity_type_lc,

    {{ osm_int('tags','building:levels') }} as building_levels,

    coalesce(
      element_at(tags,'operator'),
      element_at(tags,'network'),
      element_at(tags,'brand')
    ) as operator_name,
    element_at(tags,'opening_hours') as opening_hours,

    st_setsrid(st_geomfromwkb(geom_wkb), 4326) as geom,
    st_astext(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as geom_wkt_4326,

    region_code,
    region,
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
    and activity_class is not null
    and activity_type_lc is not null
),

dedup as (
  select *
  from filtered
  qualify row_number() over (
    partition by feature_id
    order by load_ts desc, source_file desc
  ) = 1
)

select *
from dedup
where geom_wkt_4326 is not null