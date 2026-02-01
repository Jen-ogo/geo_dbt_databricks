{{ config(materialized='table', file_format='delta', partition_by=['region_code','region']) }}

with seg as (
  select
    cast(region_code as string) as region_code,
    cast(region as string)      as region,

    -- H3 R7 directly from road segments (NOT from R10)
    {{ h3_r7_from_geom_centroid("st_geomfromwkt(geom_wkt_4326)") }} as h3_r7,

    lower(cast(highway as string)) as highway_lc,

    cast(st_length(st_geogfromwkt(geom_wkt_4326)) as double) as len_m,

    cast(oneway as boolean)        as oneway,
    cast(lanes as int)             as lanes,
    cast(maxspeed_kph as double)   as maxspeed_kph,

    cast(load_ts as timestamp) as load_ts
  from {{ ref('road_segments') }}
  where geom_wkt_4326 is not null
),

agg as (
  select
    region_code, region, h3_r7,

    sum(len_m) as roads_len_m_sum,
    sum(case when highway_lc in ('motorway','trunk','primary','secondary') then len_m else 0 end) as roads_major_len_m_sum,

    count(*) as road_segments_cnt,

    avg(maxspeed_kph) as maxspeed_avg_kph,
    percentile_approx(maxspeed_kph, 0.5) as maxspeed_p50_kph,

    avg(cast(lanes as double)) as lanes_avg,
    percentile_approx(lanes, 0.5) as lanes_p50,

    avg(case when oneway is null then null when oneway then 1.0 else 0.0 end) as oneway_share,

    max(load_ts) as last_load_ts
  from seg
  where h3_r7 is not null
  group by 1,2,3
)

select
  c.region_code,
  c.region,
  c.h3_r7,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.roads_len_m_sum,
  a.roads_major_len_m_sum,
  a.road_segments_cnt,

  a.maxspeed_avg_kph,
  a.maxspeed_p50_kph,

  a.lanes_avg,
  a.lanes_p50,

  a.oneway_share,

  -- densities per km2 (derived only)
  cast(a.road_segments_cnt as double) / nullif(c.cell_area_m2 / 1e6, 0.0) as road_segments_per_km2,
  a.roads_len_m_sum             / nullif(c.cell_area_m2 / 1e6, 0.0) as roads_len_m_per_km2,
  a.roads_major_len_m_sum       / nullif(c.cell_area_m2 / 1e6, 0.0) as roads_major_len_m_per_km2,

  cast('road_segments_to_h3_r7' as string) as road_method,

  a.last_load_ts
from {{ ref('dim_h3_r7_cells') }} c
left join agg a
  on a.region_code = c.region_code
 and a.region      = c.region
 and a.h3_r7       = c.h3_r7