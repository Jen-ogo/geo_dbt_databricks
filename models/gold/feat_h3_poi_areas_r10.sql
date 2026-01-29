{{ config(materialized='table', file_format='delta', partition_by=['region_code','region']) }}

with a as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,

    cast({{ h3_r10_from_geom_centroid('geom') }} as string) as h3_r10,

    lower(cast(poi_class as string)) as poi_class,
    cast(load_ts as timestamp) as load_ts
  from {{ ref('poi_areas') }}
  where region_code is not null
    and region      is not null
    and geom        is not null      
    and poi_class   is not null
),

agg as (
  select
    region_code,
    region,
    h3_r10,

    count(*) as poi_areas_cnt,

    -- buckets
    sum(case when poi_class='amenity'  then 1 else 0 end) as amenity_areas_cnt,
    sum(case when poi_class='shop'     then 1 else 0 end) as shop_areas_cnt,
    sum(case when poi_class='tourism'  then 1 else 0 end) as tourism_areas_cnt,
    sum(case when poi_class='office'   then 1 else 0 end) as office_areas_cnt,
    sum(case when poi_class='leisure'  then 1 else 0 end) as leisure_areas_cnt,
    sum(case when poi_class='sport'    then 1 else 0 end) as sport_areas_cnt,
    sum(case when poi_class='building' then 1 else 0 end) as building_areas_cnt,
    sum(case when poi_class='landuse'  then 1 else 0 end) as landuse_areas_cnt,

    max(load_ts) as last_load_ts
  from a
  where h3_r10 is not null
  group by 1,2,3
),

cells as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    cast(h3_r10       as string) as h3_r10,
    cast(cell_area_m2 as double) as cell_area_m2,
    cast(cell_wkt_4326 as string) as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
    and region      is not null
    and h3_r10      is not null
)

select
  c.region_code,
  c.region,
  c.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  -- counts: 0 for empty cells
  coalesce(a.poi_areas_cnt, 0)        as poi_areas_cnt,
  coalesce(a.amenity_areas_cnt, 0)    as amenity_areas_cnt,
  coalesce(a.shop_areas_cnt, 0)       as shop_areas_cnt,
  coalesce(a.tourism_areas_cnt, 0)    as tourism_areas_cnt,
  coalesce(a.office_areas_cnt, 0)     as office_areas_cnt,
  coalesce(a.leisure_areas_cnt, 0)    as leisure_areas_cnt,
  coalesce(a.sport_areas_cnt, 0)      as sport_areas_cnt,
  coalesce(a.building_areas_cnt, 0)   as building_areas_cnt,
  coalesce(a.landuse_areas_cnt, 0)    as landuse_areas_cnt,

  -- densities per km² (cell_area_m2 already computed in SILVER dim)
  coalesce(a.poi_areas_cnt, 0)     / nullif(c.cell_area_m2 / 1e6, 0.0) as poi_areas_per_km2,
  coalesce(a.amenity_areas_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0) as amenity_areas_per_km2,
  coalesce(a.shop_areas_cnt, 0)    / nullif(c.cell_area_m2 / 1e6, 0.0) as shop_areas_per_km2,
  coalesce(a.tourism_areas_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0) as tourism_areas_per_km2,
  coalesce(a.office_areas_cnt, 0)  / nullif(c.cell_area_m2 / 1e6, 0.0) as office_areas_per_km2,
  coalesce(a.leisure_areas_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0) as leisure_areas_per_km2,
  coalesce(a.sport_areas_cnt, 0)   / nullif(c.cell_area_m2 / 1e6, 0.0) as sport_areas_per_km2,

  -- NULL for empty cells is expected
  a.last_load_ts
from cells c
left join agg a
  on a.region_code = c.region_code
 and a.region      = c.region
 and a.h3_r10      = c.h3_r10