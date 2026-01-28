{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with s as (
  select
    cast(region_code as string) as region_code,
    cast(region as string)      as region,
    cast(h3_r10 as string)      as h3_r10,

    cast(cell_area_m2 as double)         as cell_area_m2,
    cast(cell_wkt_4326 as string)        as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326,

    cast(roads_len_m_sum as double)       as roads_len_m_sum,
    cast(roads_major_len_m_sum as double) as roads_major_len_m_sum,
    cast(road_segments_cnt as bigint)     as road_segments_cnt,

    cast(maxspeed_avg_kph as double) as maxspeed_avg_kph,
    cast(maxspeed_p50_kph as double) as maxspeed_p50_kph,

    cast(lanes_avg as double) as lanes_avg,
    cast(lanes_p50 as double) as lanes_p50,

    cast(oneway_share as double) as oneway_share,
    cast(last_load_ts as timestamp) as last_load_ts
  from {{ ref('feat_h3_roads_r10') }}
  where region_code is not null
    and region is not null
    and h3_r10 is not null
)

select
  region_code,
  region,
  h3_r10,

  cell_area_m2,
  cell_wkt_4326,
  cell_center_wkt_4326,

  roads_len_m_sum,
  roads_major_len_m_sum,
  road_segments_cnt,

  maxspeed_avg_kph,
  maxspeed_p50_kph,

  lanes_avg,
  lanes_p50,

  oneway_share,

  -- densities per km2 (derived only)
  cast(road_segments_cnt as double) / nullif(cell_area_m2 / 1e6, 0.0) as road_segments_per_km2,
  roads_len_m_sum             / nullif(cell_area_m2 / 1e6, 0.0) as roads_len_m_per_km2,
  roads_major_len_m_sum       / nullif(cell_area_m2 / 1e6, 0.0) as roads_major_len_m_per_km2,

  last_load_ts
from s