{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with src as (
  select
    cast(osm_id as string)           as osm_id,
    cast(nullif(name,'') as string)  as name,
    cast(other_tags as string)       as other_tags_raw,
    cast(geom_wkb as binary)         as geom_wkb,

    cast(country as string)          as country,
    cast(region  as string)          as region,

    cast(dt as date)                 as dt,
    cast(source_file as string)      as source_file,
    cast(load_ts as timestamp)       as load_ts,

    {{ osm_tags_json('cast(other_tags as string)') }} as tags
  from {{ source('bronze','osm_poi_points') }}
  where osm_id is not null
    and geom_wkb is not null
),

typed as (
  select
    concat('N', osm_id) as feature_id,
    osm_id,
    name,

    -- canonical region fields
    lower(country) as region_code,
    region,

    -- address
    element_at(tags,'addr:housenumber') as addr_housenumber,
    element_at(tags,'addr:street')      as addr_street,
    element_at(tags,'addr:postcode')    as addr_postcode,
    coalesce(element_at(tags,'addr:city'), element_at(tags,'addr:place')) as addr_city_or_place,

    -- include emergency like in Snowflake model
    coalesce(
      element_at(tags,'amenity'),
      element_at(tags,'shop'),
      element_at(tags,'tourism'),
      element_at(tags,'leisure'),
      element_at(tags,'office'),
      element_at(tags,'craft'),
      element_at(tags,'man_made'),
      element_at(tags,'emergency'),
      element_at(tags,'public_transport'),
      element_at(tags,'railway'),
      element_at(tags,'highway'),
      element_at(tags,'place')
    ) as poi_type,

    case
      when element_at(tags,'amenity') is not null then 'amenity'
      when element_at(tags,'shop') is not null then 'shop'
      when element_at(tags,'tourism') is not null then 'tourism'
      when element_at(tags,'leisure') is not null then 'leisure'
      when element_at(tags,'office') is not null then 'office'
      when element_at(tags,'craft') is not null then 'craft'
      when element_at(tags,'man_made') is not null then 'man_made'
      when element_at(tags,'emergency') is not null then 'emergency'
      when element_at(tags,'public_transport') is not null then 'public_transport'
      when element_at(tags,'railway') is not null then 'railway'
      when element_at(tags,'highway') is not null then 'highway'
      when element_at(tags,'place') is not null then 'place'
      else null
    end as poi_class,

    -- geometry
    st_setsrid(st_geomfromwkb(geom_wkb), 4326) as geom,
    st_astext(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as geom_wkt_4326,

    tags,
    other_tags_raw,

    dt,
    source_file,
    load_ts
  from src
),

dedup as (
  select *
  from typed
  where poi_class is not null
    and poi_type  is not null
    and geom is not null
  qualify row_number() over (
    partition by region_code, region, osm_id
    order by dt desc, load_ts desc, source_file desc
  ) = 1
)

select *
from dedup
where geom_wkt_4326 is not null