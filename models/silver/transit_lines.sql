{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with src as (
  select
    cast(osm_id as string) as osm_id,
    cast(other_tags as string) as other_tags_raw,

    cast(railway as string) as railway,
    cast(waterway as string) as waterway,
    cast(aerialway as string) as aerialway,

    cast(geom_wkb as binary) as geom_wkb,

    cast(lower(country) as string) as region_code,
    cast(region as string) as region,

    cast(dt as date) as dt,
    cast(source_file as string) as source_file,
    cast(load_ts as timestamp) as load_ts,

    {{ osm_tags_json('other_tags') }} as tags
  from {{ source('bronze','osm_pt_lines') }}
  where osm_id is not null
    and geom_wkb is not null
),

typed as (
  select
    concat('W', osm_id) as feature_id,
    osm_id,

    case
      when railway is not null or element_at(tags,'railway') is not null then 'railway'
      when waterway is not null or element_at(tags,'waterway') is not null then 'waterway'
      when aerialway is not null or element_at(tags,'aerialway') is not null then 'aerialway'
      else null
    end as line_class,

    coalesce(
      railway, element_at(tags,'railway'),
      waterway, element_at(tags,'waterway'),
      aerialway, element_at(tags,'aerialway')
    ) as line_type,

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
  where line_class is not null
    and line_type is not null
    and geom is not null
    and geom_wkt_4326 is not null
  qualify row_number() over (
    partition by region_code, region, osm_id
    order by dt desc, load_ts desc, source_file desc
  ) = 1
)

select * from dedup