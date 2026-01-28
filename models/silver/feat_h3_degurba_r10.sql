{{ config(materialized='table', file_format='delta', partition_by=['region_code']) }}

with h as (
  select
    region_code,
    h3_r10,
    st_setsrid(st_geomfromwkt(cell_center_wkt_4326), 4326) as cell_center_geom
  from {{ ref('dim_h3_r10_cells') }}
  where cell_center_wkt_4326 is not null
),

lau as (
  select
    region_code, lau_id, lau_name, degurba, year, geom as lau_geom, load_ts
  from {{ ref('lau_degurba') }}
  where geom is not null
)

select
  h.region_code,
  h.h3_r10,
  l.year,
  l.degurba,
  l.lau_id,
  l.lau_name,
  l.load_ts as last_load_ts
from h
join lau l
  on h.region_code = l.region_code
 and st_contains(l.lau_geom, h.cell_center_geom)