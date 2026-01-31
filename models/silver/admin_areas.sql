{{ config(materialized='table', file_format='delta', partition_by=['region_code','region']) }}

with base as (
  select
    cast(osm_id as string) as osm_id,
    cast(admin_level as int) as admin_level,
    cast(boundary as string) as boundary,
    cast(region as string) as region,
    cast(country as string) as region_code,
    geom_wkb,
    cast(dt as date) as dt,
    cast(source_file as string) as source_file,
    cast(load_ts as timestamp) as load_ts,
    cast(other_tags as string) as other_tags_raw,
    cast(nullif(name,'') as string) as name
  from {{ source('bronze','osm_admin') }}
  where osm_id is not null and geom_wkb is not null and boundary='administrative'
),

typed as (
  select
    concat('A', osm_id) as feature_id,
    osm_id,
    name,
    admin_level,
    boundary,
    {{ osm_tags_json('other_tags_raw') }} as tags,

    region_code,
    region,

    geom_wkb,
    st_setsrid(st_geomfromwkb(geom_wkb), 4326) as geom_raw,

    dt, source_file, load_ts
  from base
),

geom_fixed as (
  select
    *,
    case
      when geom_raw is null or st_isempty(geom_raw) then null
      when st_isvalid(geom_raw) then geom_raw
      else st_buffer(geom_raw, 0.0)
    end as geom
  from typed
),

final as (
  select
    feature_id, osm_id, name, admin_level, boundary,
    tags,
    geom_wkb,
    geom,
    st_astext(geom) as geom_wkt_4326,
    region_code, region,
    dt, source_file, load_ts
  from geom_fixed
),

dedup as (
  select *
  from final
  {{ dedup_qualify(
      partition_by=['region_code','region','osm_id'],
      order_by=['dt desc','load_ts desc','source_file desc']
  ) }}
)

select *
from dedup
where geom is not null