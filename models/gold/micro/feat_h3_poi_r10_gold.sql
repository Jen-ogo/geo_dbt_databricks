{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}


with s as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    cast(h3_r10       as string) as h3_r10,

    cast(cell_area_m2        as double) as cell_area_m2,
    cast(cell_wkt_4326       as string) as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326,

    cast(coalesce(poi_cnt, 0)           as bigint) as poi_cnt,
    cast(coalesce(poi_amenity_cnt, 0)   as bigint) as poi_amenity_cnt,
    cast(coalesce(poi_shop_cnt, 0)      as bigint) as poi_shop_cnt,
    cast(coalesce(poi_tourism_cnt, 0)   as bigint) as poi_tourism_cnt,
    cast(coalesce(poi_building_cnt, 0)  as bigint) as poi_building_cnt,
    cast(coalesce(poi_landuse_cnt, 0)   as bigint) as poi_landuse_cnt,

    cast(last_load_ts as timestamp) as last_load_ts
  from {{ ref('feat_h3_poi_r10') }}
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

  poi_cnt,
  poi_amenity_cnt,
  poi_shop_cnt,
  poi_tourism_cnt,
  poi_building_cnt,
  poi_landuse_cnt,

  /* densities per km2 (hex area) */
  cast(poi_cnt          as double) / nullif(cell_area_m2 / 1e6, 0.0) as poi_per_km2,
  cast(poi_amenity_cnt  as double) / nullif(cell_area_m2 / 1e6, 0.0) as poi_amenity_per_km2,
  cast(poi_shop_cnt     as double) / nullif(cell_area_m2 / 1e6, 0.0) as poi_shop_per_km2,
  cast(poi_tourism_cnt  as double) / nullif(cell_area_m2 / 1e6, 0.0) as poi_tourism_per_km2,

  last_load_ts
from s