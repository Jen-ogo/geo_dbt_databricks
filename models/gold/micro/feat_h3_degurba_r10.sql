{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code']
) }}

with cells as (
  select
    cast(region_code as string)          as region_code,
    cast(h3_r10 as string)               as h3_r10,
    cast(cell_area_m2 as double)         as cell_area_m2,
    cast(cell_wkt_4326 as string)        as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326,
    st_setsrid(st_geomfromwkt(cell_center_wkt_4326), 4326) as cell_center_geom
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
    and h3_r10 is not null
    and cell_center_wkt_4326 is not null
),

lau as (
  select
    cast(region_code as string) as region_code,
    cast(lau_id as string)      as lau_id,
    cast(lau_name as string)    as lau_name,
    try_cast(degurba as int)    as degurba,
    try_cast(year as int)       as year,
    geom as lau_geom,
    cast(load_ts as timestamp)  as load_ts
  from {{ ref('lau_degurba') }}
  where geom is not null
    and region_code is not null
),

matched as (
  select
    c.region_code,
    c.h3_r10,

    l.year,
    l.degurba,
    l.lau_id,
    l.lau_name,
    l.load_ts as last_load_ts,

    row_number() over (
      partition by c.region_code, c.h3_r10
      order by l.year desc nulls last, l.load_ts desc nulls last, l.lau_id
    ) as rn
  from cells c
  join lau l
    on c.region_code = l.region_code
   and st_contains(l.lau_geom, c.cell_center_geom)
),

best as (
  select
    region_code,
    h3_r10,
    year,
    degurba,
    lau_id,
    lau_name,
    last_load_ts
  from matched
  where rn = 1
)

select
  c.region_code,
  c.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  b.year,
  b.lau_id,
  b.lau_name,
  b.degurba,

  case when b.degurba = 1 then 1 else 0 end as degurba_1_city,
  case when b.degurba = 2 then 1 else 0 end as degurba_2_towns_suburbs,
  case when b.degurba = 3 then 1 else 0 end as degurba_3_rural,

  b.last_load_ts
from cells c
left join best b
  on b.region_code = c.region_code
 and b.h3_r10      = c.h3_r10