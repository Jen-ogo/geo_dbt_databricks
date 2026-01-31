{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code']
) }}

-- =============================================================================
-- GOLD.FEAT_H3_DEGURBA_R10_FINAL (NO BIGINT)
-- Source:
--   - SILVER.FEAT_H3_DEGURBA_R10  (region_code, h3_r10 [STRING], year, degurba, lau_id, lau_name, last_load_ts)
--   - SILVER.DIM_H3_R10_CELLS     (region_code, h3_r10 [STRING], cell_area_m2, cell_wkt_4326, cell_center_wkt_4326)
-- Logic:
--   - 1 record per (region_code, h3_r10): max(year), then max(last_load_ts)
--   - LEFT JOIN to full set of cells
-- =============================================================================

with s0 as (
  select
    cast(region_code as string)     as region_code,
    cast(h3_r10 as string)          as h3_r10,
    try_cast(year as int)           as year,
    try_cast(degurba as int)        as degurba,
    cast(lau_id as string)          as lau_id,
    cast(lau_name as string)        as lau_name,
    cast(last_load_ts as timestamp) as last_load_ts
  from {{ ref('feat_h3_degurba_r10') }}
  where region_code is not null
    and h3_r10 is not null
),

s as (
  select region_code, h3_r10, year, degurba, lau_id, lau_name, last_load_ts
  from (
    select
      s0.*,
      row_number() over (
        partition by region_code, h3_r10
        order by year desc nulls last, last_load_ts desc nulls last
      ) as rn
    from s0
  ) t
  where rn = 1
),

cells as (
  select
    cast(region_code as string)          as region_code,
    cast(h3_r10 as string)               as h3_r10,
    cast(cell_area_m2 as double)         as cell_area_m2,
    cast(cell_wkt_4326 as string)        as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
    and h3_r10 is not null
)

select
  c.region_code,
  c.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  s.year,
  s.lau_id,
  s.lau_name,
  s.degurba,

  case when s.degurba = 1 then 1 else 0 end as degurba_1_city,
  case when s.degurba = 2 then 1 else 0 end as degurba_2_towns_suburbs,
  case when s.degurba = 3 then 1 else 0 end as degurba_3_rural,

  s.last_load_ts
from cells c
left join s
  on s.region_code = c.region_code
 and s.h3_r10      = c.h3_r10