{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with src as (
  select
    cast(nullif(osm_id,'') as string)         as osm_id,
    cast(osm_way_id as string)                as osm_way_id,
    cast(nullif(name,'') as string)           as name,
    cast(other_tags as string)                as other_tags_raw,

    -- sometimes columns exist, sometimes not; keep tolerant pattern:
    cast(nullif(amenity,'')  as string)       as amenity_col,
    cast(nullif(shop,'')     as string)       as shop_col,
    cast(nullif(tourism,'')  as string)       as tourism_col,
    cast(nullif(office,'')   as string)       as office_col,
    cast(nullif(leisure,'')  as string)       as leisure_col,
    cast(nullif(sport,'')    as string)       as sport_col,
    cast(nullif(building,'') as string)       as building_col,
    cast(nullif(landuse,'')  as string)       as landuse_col,

    cast(geom_wkb as binary)                  as geom_wkb,

    cast(country as string)                   as country,
    cast(region  as string)                   as region,

    cast(dt as date)                          as dt,
    cast(source_file as string)               as source_file,
    cast(load_ts as timestamp)                as load_ts,

    {{ osm_tags_json('cast(other_tags as string)') }} as tags
  from {{ source('bronze','osm_poi_polygons') }}
  where geom_wkb is not null
),

base as (
  select
    -- stable key: prefer osm_id when present, else W<osm_way_id>
    coalesce(nullif(osm_id,''), concat('W', osm_way_id)) as feature_id,
    osm_id,
    osm_way_id,
    name,

    -- canonical region fields
    lower(country) as region_code,
    region,

    -- pull types: prefer dedicated columns, fallback to tags
    coalesce(amenity_col,  element_at(tags,'amenity'))  as amenity,
    coalesce(shop_col,     element_at(tags,'shop'))     as shop,
    coalesce(tourism_col,  element_at(tags,'tourism'))  as tourism,
    coalesce(office_col,   element_at(tags,'office'))   as office,
    coalesce(leisure_col,  element_at(tags,'leisure'))  as leisure,
    coalesce(sport_col,    element_at(tags,'sport'))    as sport,
    coalesce(building_col, element_at(tags,'building')) as building,
    coalesce(landuse_col,  element_at(tags,'landuse'))  as landuse,

    -- poi classification
    case
      when coalesce(amenity_col,  element_at(tags,'amenity'))  is not null then 'amenity'
      when coalesce(shop_col,     element_at(tags,'shop'))     is not null then 'shop'
      when coalesce(tourism_col,  element_at(tags,'tourism'))  is not null then 'tourism'
      when coalesce(office_col,   element_at(tags,'office'))   is not null then 'office'
      when coalesce(leisure_col,  element_at(tags,'leisure'))  is not null then 'leisure'
      when coalesce(sport_col,    element_at(tags,'sport'))    is not null then 'sport'
      when coalesce(building_col, element_at(tags,'building')) is not null then 'building'
      when coalesce(landuse_col,  element_at(tags,'landuse'))  is not null then 'landuse'
      else null
    end as poi_class,

    coalesce(
      coalesce(amenity_col,  element_at(tags,'amenity')),
      coalesce(shop_col,     element_at(tags,'shop')),
      coalesce(tourism_col,  element_at(tags,'tourism')),
      coalesce(office_col,   element_at(tags,'office')),
      coalesce(leisure_col,  element_at(tags,'leisure')),
      coalesce(sport_col,    element_at(tags,'sport')),
      coalesce(building_col, element_at(tags,'building')),
      coalesce(landuse_col,  element_at(tags,'landuse'))
    ) as poi_type,

    -- geometry canonical
    st_setsrid(st_geomfromwkb(geom_wkb), 4326) as geom,
    st_astext(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as geom_wkt_4326,

    -- useful derived fields for polygons
    st_centroid(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as centroid_geom,
    {{ area_m2('st_setsrid(st_geomfromwkb(geom_wkb), 4326)') }} as area_m2,

    tags,
    other_tags_raw,

    dt,
    source_file,
    load_ts
  from src
),

filtered as (
  select *
  from base
  where feature_id is not null
    and poi_class is not null
    and poi_type  is not null
    and geom is not null
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