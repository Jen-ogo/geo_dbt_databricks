{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold','heavy']
) }}

with tp as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    {{ h3_r10_from_geom_point('geom') }} as h3_r10,
    cast(load_ts as timestamp) as load_ts
  from {{ ref('transit_points') }}
  where geom is not null
),

tl as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    {{ h3_r10_from_geom_centroid('geom') }} as h3_r10,

    cast(
      st_length(st_transform(geom, 3035))
      as double
    ) as len_m,

    cast(load_ts as timestamp) as load_ts
  from {{ ref('transit_lines') }}
  where geom is not null
),

agg_points as (
  select
    region_code,
    region,
    h3_r10,
    count(*) as transit_points_cnt,
    max(load_ts) as last_points_load_ts
  from tp
  where h3_r10 is not null
  group by 1,2,3
),

agg_lines as (
  select
    region_code,
    region,
    h3_r10,
    sum(len_m) as transit_lines_len_m_sum,
    max(load_ts) as last_lines_load_ts
  from tl
  where h3_r10 is not null
  group by 1,2,3
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

  /* densities per km² */
  coalesce(p.transit_points_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0) as transit_points_per_km2,
  (coalesce(l.transit_lines_len_m_sum, 0) * 1e6 / nullif(c.cell_area_m2, 0.0)) as transit_lines_m_per_km2,

  /* flags */
  (coalesce(p.transit_points_cnt, 0) > 0 or coalesce(l.transit_lines_len_m_sum, 0) > 0) as has_transit,

  /* NULL if empty cell */
  case
    when p.last_points_load_ts is null and l.last_lines_load_ts is null then null
    else greatest(p.last_points_load_ts, l.last_lines_load_ts)
  end as last_load_ts

from {{ ref('dim_h3_r10_cells') }} c
left join agg_points p
  on p.region_code = c.region_code
 and p.region      = c.region
 and p.h3_r10      = c.h3_r10
left join agg_lines l
  on l.region_code = c.region_code
 and l.region      = c.region
 and l.h3_r10      = c.h3_r10