{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with src as (
  select
    cast(osm_id as string) as osm_id,
    cast(nullif(name,'') as string) as name,
    cast(other_tags as string) as other_tags_raw,
    cast(geom_wkb as binary) as geom_wkb,

    cast(lower(country) as string) as region_code,
    cast(region as string) as region,

    cast(dt as date) as dt,
    cast(source_file as string) as source_file,
    cast(load_ts as timestamp) as load_ts,

    {{ osm_tags_json('other_tags') }} as tags
  from {{ source('bronze','osm_pt_points') }}
  where osm_id is not null
    and geom_wkb is not null
),

typed as (
  select
    concat('N', osm_id) as feature_id,
    osm_id,
    name,

    -- class
    case
      when element_at(tags,'public_transport') is not null
        or element_at(tags,'railway') is not null
        or lower(coalesce(element_at(tags,'highway'), '')) in ('bus_stop','platform')
      then 'transport'
      when element_at(tags,'amenity') is not null then 'amenity'
      when element_at(tags,'emergency') is not null then 'emergency'
      else null
    end as poi_class,

    -- type
    case
      when element_at(tags,'public_transport') is not null then element_at(tags,'public_transport')
      when element_at(tags,'railway') is not null then element_at(tags,'railway')
      when element_at(tags,'highway') is not null then element_at(tags,'highway')
      when element_at(tags,'amenity') is not null then element_at(tags,'amenity')
      when element_at(tags,'emergency') is not null then element_at(tags,'emergency')
      else null
    end as poi_type,

    st_setsrid(st_geomfromwkb(geom_wkb), 4326) as geom,
    st_astext(st_setsrid(st_geomfromwkb(geom_wkb), 4326)) as geom_wkt_4326,

    tags,
    other_tags_raw,

    region_code,
    region,
    dt,
    source_file,
    load_ts
  from src
),

dedup as (
  select *
  from typed
  where poi_class is not null
    and poi_type is not null
    and geom is not null
    and geom_wkt_4326 is not null
  qualify row_number() over (
    partition by region_code, region, osm_id
    order by dt desc, load_ts desc, source_file desc
  ) = 1
)

select * from dedup