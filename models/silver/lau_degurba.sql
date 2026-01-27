{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['cntr_code','region_code','year']
) }}

with src as (
  select
    cast(gisco_id as string)   as gisco_id,
    cast(cntr_code as string)  as cntr_code,
    cast(lau_id as string)     as lau_id,
    cast(lau_name as string)   as lau_name,
    cast(dgurba as int)        as degurba,
    cast(fid as bigint)        as fid,
    cast(year as string)       as year,
    cast(geom_wkt as string)   as geom_wkt_raw,
    cast(source_file as string) as source_file,
    cast(load_ts as timestamp)  as load_ts
  from {{ source('bronze','eurostat_lau_degurba') }}
  where cntr_code is not null
    and lau_id   is not null
    and year     is not null
    and geom_wkt is not null
),

dedup as (
  select *
  from src
  qualify row_number() over (
    partition by cntr_code, lau_id, year
    order by load_ts desc, source_file desc
  ) = 1
),

geo0 as (
  select
    concat('LAU:', cntr_code, ':', lau_id, ':', year) as feature_id,
    d.*,
    st_setsrid(st_geomfromwkt(geom_wkt_raw), 4326) as geom0
  from dedup d
  where geom_wkt_raw is not null
),

fixed as (
  select
    *,
    st_isvalid(geom0) as is_valid0,
    case
      when st_isvalid(geom0) then geom0
      else st_buffer(geom0, 0.0)
    end as geom_fixed
  from geo0
  where geom0 is not null
),

flag as (
  select
    *,
    st_isvalid(geom_fixed) as is_valid_fixed,
    st_astext(geom_fixed)  as geom_wkt_4326,
    st_centroid(geom_fixed) as lau_centroid_geom
  from fixed
  where geom_fixed is not null
),

admin4 as (
  select
    region_code,
    region,
    st_setsrid(st_geomfromwkb(geom_wkb), 4326) as admin4_geom
  from {{ ref('admin_areas') }}
  where admin_level = 4
    and boundary = 'administrative'
    and geom_wkb is not null
)

select
  f.feature_id,

  f.gisco_id,
  f.cntr_code,
  f.lau_id,
  f.lau_name,
  f.degurba,
  f.fid,
  f.year,

  a.region_code,
  a.region,

  f.geom_wkt_raw,
  f.geom_wkt_4326,
  f.geom_fixed as geom,

  f.is_valid0,
  f.is_valid_fixed,

  f.source_file,
  f.load_ts
from flag f
left join admin4 a
  on st_contains(a.admin4_geom, f.lau_centroid_geom)
where f.geom_wkt_4326 is not null