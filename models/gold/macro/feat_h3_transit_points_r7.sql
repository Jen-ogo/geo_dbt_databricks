{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold']
) }}

with pts as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,

    {{ h3_r7_from_geom_point('geom') }} as h3_r7,

    lower(cast(poi_class as string)) as poi_class,
    lower(cast(poi_type  as string)) as poi_type,
    cast(load_ts as timestamp) as load_ts
  from {{ ref('transit_points') }}
  where region_code is not null
    and region      is not null
    and geom is not null
),

agg as (
  select
    region_code,
    region,
    h3_r7,

    count(*) as transit_points_cnt,

    sum(case when poi_class='transport' then 1 else 0 end) as transport_points_cnt,
    sum(case when poi_class='amenity'   then 1 else 0 end) as amenity_points_cnt,
    sum(case when poi_class='emergency' then 1 else 0 end) as emergency_points_cnt,

    sum(case when poi_type in ('station','halt','tram_stop','subway_entrance')
             then 1 else 0 end) as station_like_cnt,
    sum(case when poi_type in ('bus_stop','platform')
             then 1 else 0 end) as stop_like_cnt,

    max(load_ts) as last_load_ts
  from pts
  where h3_r7 is not null
  group by 1,2,3
),

dim as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    cast(h3_r7       as string) as h3_r7,

    cast(cell_area_m2 as double) as cell_area_m2,
    cast(cell_wkt_4326 as string) as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326
  from {{ ref('dim_h3_r7_cells') }}
  where region_code is not null
    and region      is not null
    and h3_r7       is not null
    and cell_area_m2 is not null
    and cell_wkt_4326 is not null
    and cell_center_wkt_4326 is not null
)

select
  d.region_code,
  d.region,
  d.h3_r7,

  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

  coalesce(a.transit_points_cnt, 0)   as transit_points_cnt,
  coalesce(a.transport_points_cnt, 0) as transport_points_cnt,
  coalesce(a.amenity_points_cnt, 0)   as amenity_points_cnt,
  coalesce(a.emergency_points_cnt, 0) as emergency_points_cnt,
  coalesce(a.station_like_cnt, 0)     as station_like_cnt,
  coalesce(a.stop_like_cnt, 0)        as stop_like_cnt,

  case when coalesce(a.transit_points_cnt,0)=0 then null
       else a.transport_points_cnt / nullif(a.transit_points_cnt,0) end as transport_points_share,

  case when coalesce(a.transit_points_cnt,0)=0 then null
       else a.emergency_points_cnt / nullif(a.transit_points_cnt,0) end as emergency_points_share,

  (coalesce(a.transit_points_cnt,0)   * 1e6 / nullif(d.cell_area_m2,0)) as transit_points_per_km2,
  (coalesce(a.transport_points_cnt,0) * 1e6 / nullif(d.cell_area_m2,0)) as transport_points_per_km2,

  a.last_load_ts,
  (coalesce(a.transit_points_cnt,0) > 0) as has_transit_points
from dim d
left join agg a
  on a.region_code = d.region_code
 and a.region      = d.region
 and a.h3_r7       = d.h3_r7