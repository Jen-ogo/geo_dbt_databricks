{{ config(materialized='table', file_format='delta', partition_by=['region_code']) }}

with pts as (
  select
    region_code,
    {{ h3_r10_from_geom_point("st_geomfromwkt(geom_wkt_4326)") }} as h3_r10,
    poi_class,
    load_ts
  from {{ ref('poi_points') }}
  where geom_wkt_4326 is not null and poi_class is not null
),

areas as (
  select
    region_code,
    {{ h3_r10_from_geom_centroid("st_geomfromwkt(geom_wkt_4326)") }} as h3_r10,
    poi_class,
    load_ts
  from {{ ref('poi_areas') }}
  where geom_wkt_4326 is not null and poi_class is not null
),

all_poi as (
  select * from pts
  union all
  select * from areas
),

agg as (
  select
    region_code,
    h3_r10,
    count(*) as poi_cnt,
    sum(case when poi_class='amenity' then 1 else 0 end) as poi_amenity_cnt,
    sum(case when poi_class='shop' then 1 else 0 end) as poi_shop_cnt,
    sum(case when poi_class='tourism' then 1 else 0 end) as poi_tourism_cnt,
    sum(case when poi_class='building' then 1 else 0 end) as poi_building_cnt,
    sum(case when poi_class='landuse' then 1 else 0 end) as poi_landuse_cnt,
    max(load_ts) as last_load_ts
  from all_poi
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
  a.poi_cnt,
  a.poi_amenity_cnt,
  a.poi_shop_cnt,
  a.poi_tourism_cnt,
  a.poi_building_cnt,
  a.poi_landuse_cnt,
  a.last_load_ts
from {{ ref('dim_h3_r10_cells') }} c
left join agg a
  on a.region_code = c.region_code
 and a.h3_r10 = c.h3_r10