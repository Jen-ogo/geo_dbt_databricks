{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold']
) }}

with pop as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    cast(h3_r7       as string) as h3_r7,

    cast(cell_area_m2 as double)         as cell_area_m2,
    cast(cell_wkt_4326 as string)        as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326,

    cast(pop_total as double) as pop_total,
    cast(last_load_ts as timestamp) as pop_last_load_ts
  from {{ ref('feat_h3_pop_r7') }}
  where region_code is not null
    and region      is not null
    and h3_r7       is not null
),

ch_src as (
  select
    cast(region_code as string) as region_code,
    cast({{ h3_r7_from_geom_point("st_geomfromwkt(geom_wkt_4326)") }} as string) as h3_r7,

    cast(total_sockets_cnt as int) as total_sockets_cnt,
    cast(has_dc as boolean) as has_dc,
    cast(has_ac as boolean) as has_ac,
    cast(load_ts as timestamp) as load_ts
  from {{ ref('ev_chargers') }}
  where region_code is not null
    and geom_wkt_4326 is not null
),

ch_agg as (
  select
    region_code,
    h3_r7,

    count(*) as chargers_cnt,
    sum(coalesce(total_sockets_cnt, 0)) as sockets_cnt_sum,
    sum(case when has_dc then 1 else 0 end) as chargers_dc_cnt,
    sum(case when has_ac then 1 else 0 end) as chargers_ac_cnt,

    max(load_ts) as chargers_last_load_ts
  from ch_src
  where h3_r7 is not null
  group by 1,2
)

select
  p.region_code,
  p.region,
  p.h3_r7,

  p.cell_area_m2,
  p.cell_wkt_4326,
  p.cell_center_wkt_4326,

  /* chargers (0 for empty cells) */
  coalesce(c.chargers_cnt, 0) as chargers_cnt,
  coalesce(c.sockets_cnt_sum, 0) as sockets_cnt_sum,
  coalesce(c.chargers_dc_cnt, 0) as chargers_dc_cnt,
  coalesce(c.chargers_ac_cnt, 0) as chargers_ac_cnt,

  /* population */
  p.pop_total,

  /* derived */
  case when p.pop_total > 0
    then coalesce(c.chargers_cnt, 0) * 10000.0 / p.pop_total
  end as chargers_per_10k_pop,

  case when p.pop_total > 0
    then coalesce(c.sockets_cnt_sum, 0) * 10000.0 / p.pop_total
  end as sockets_per_10k_pop,

  coalesce(c.chargers_cnt, 0) / nullif(p.cell_area_m2 / 1e6, 0.0) as chargers_per_km2,
  coalesce(c.sockets_cnt_sum, 0) / nullif(p.cell_area_m2 / 1e6, 0.0) as sockets_per_km2,

  /* recency */
  greatest(c.chargers_last_load_ts, p.pop_last_load_ts) as last_load_ts

from pop p
left join ch_agg c
  on c.region_code = p.region_code
 and c.h3_r7       = p.h3_r7