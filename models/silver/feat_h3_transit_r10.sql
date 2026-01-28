{{ config(materialized='table', file_format='delta', partition_by=['region_code']) }}

with tp as (
  select
    region_code,
    {{ h3_r10_from_geom_point("st_geomfromwkt(geom_wkt_4326)") }} as h3_r10,
    load_ts
  from {{ ref('transit_points') }}
  where geom_wkt_4326 is not null
),

tl as (
  select
    region_code,
    {{ h3_r10_from_geom_centroid("st_geomfromwkt(geom_wkt_4326)") }} as h3_r10,
    cast(
      st_length(
        st_transform(st_setsrid(st_geomfromwkt(geom_wkt_4326), 4326), 3035)
      ) as double
    ) as len_m,
    load_ts
  from {{ ref('transit_lines') }}
  where geom_wkt_4326 is not null
),

agg_points as (
  select region_code, h3_r10, count(*) as transit_points_cnt, max(load_ts) as last_load_ts
  from tp
  where h3_r10 is not null
  group by 1,2
),

agg_lines as (
  select region_code, h3_r10, sum(len_m) as transit_lines_len_m_sum, max(load_ts) as last_load_ts
  from tl
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
  coalesce(p.transit_points_cnt, 0) as transit_points_cnt,
  coalesce(l.transit_lines_len_m_sum, 0) as transit_lines_len_m_sum,
  greatest(
    coalesce(p.last_load_ts, timestamp('1970-01-01')),
    coalesce(l.last_load_ts, timestamp('1970-01-01'))
  ) as last_load_ts
from {{ ref('dim_h3_r10_cells') }} c
left join agg_points p on p.region_code = c.region_code and p.h3_r10 = c.h3_r10
left join agg_lines  l on l.region_code = c.region_code and l.h3_r10 = c.h3_r10