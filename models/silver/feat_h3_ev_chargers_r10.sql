{{ config(materialized='table', file_format='delta', partition_by=['region_code']) }}

with src as (
  select
    region_code,
    {{ h3_r10_from_geom_point("st_geomfromwkt(geom_wkt_4326)") }} as h3_r10,
    cast(total_sockets_cnt as int) as total_sockets_cnt,
    cast(has_dc as boolean) as has_dc,
    cast(has_ac as boolean) as has_ac,
    load_ts
  from {{ ref('ev_chargers') }}
  where geom_wkt_4326 is not null
),

agg as (
  select
    region_code,
    h3_r10,
    count(*) as chargers_cnt,
    sum(coalesce(total_sockets_cnt,0)) as sockets_cnt_sum,
    sum(case when has_dc then 1 else 0 end) as chargers_dc_cnt,
    sum(case when has_ac then 1 else 0 end) as chargers_ac_cnt,
    max(load_ts) as last_load_ts
  from src
  where h3_r10 is not null
  group by 1,2
)

select
  c.region_code,
  c.region,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  coalesce(a.chargers_cnt, 0) as chargers_cnt,
  coalesce(a.sockets_cnt_sum, 0) as sockets_cnt_sum,
  coalesce(a.chargers_dc_cnt, 0) as chargers_dc_cnt,
  coalesce(a.chargers_ac_cnt, 0) as chargers_ac_cnt,

  a.last_load_ts
from {{ ref('dim_h3_r10_cells') }} c
left join agg a
  on a.region_code = c.region_code
 and a.h3_r10 = c.h3_r10