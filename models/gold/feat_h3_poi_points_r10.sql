{{ config(materialized='table', file_format='delta', partition_by=['region_code','region']) }}

with p as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,

    cast({{ h3_r10_from_geom_point('geom') }} as string) as h3_r10,

    lower(cast(poi_class as string)) as poi_class,
    lower(cast(poi_type  as string)) as poi_type,
    cast(load_ts as timestamp) as load_ts
  from {{ ref('poi_points') }}
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

    /* totals */
    count(*) as poi_points_cnt,
    count(distinct poi_class) as poi_classes_cnt,
    count(distinct poi_type)  as poi_types_cnt,

    /* class buckets */
    sum(case when poi_class = 'amenity'          then 1 else 0 end) as amenity_cnt,
    sum(case when poi_class = 'shop'             then 1 else 0 end) as shop_cnt,
    sum(case when poi_class = 'tourism'          then 1 else 0 end) as tourism_cnt,
    sum(case when poi_class = 'leisure'          then 1 else 0 end) as leisure_cnt,
    sum(case when poi_class = 'office'           then 1 else 0 end) as office_cnt,
    sum(case when poi_class = 'craft'            then 1 else 0 end) as craft_cnt,
    sum(case when poi_class = 'man_made'         then 1 else 0 end) as man_made_cnt,
    sum(case when poi_class = 'emergency'        then 1 else 0 end) as emergency_cnt,
    sum(case when poi_class = 'public_transport' then 1 else 0 end) as public_transport_cnt,
    sum(case when poi_class = 'railway'          then 1 else 0 end) as railway_cnt,
    sum(case when poi_class = 'highway'          then 1 else 0 end) as highway_cnt,
    sum(case when poi_class = 'place'            then 1 else 0 end) as place_cnt,

    /* EV-relevant type buckets */
    sum(case when poi_type in ('parking','parking_entrance','bicycle_parking')
             then 1 else 0 end) as parking_cnt,

    sum(case when poi_type in ('fuel','charging_station','car_wash','car_rental','car_sharing','parking_space')
             then 1 else 0 end) as mobility_services_cnt,

    sum(case when poi_type in ('supermarket','convenience','mall','department_store','hardware','doityourself')
             then 1 else 0 end) as retail_core_cnt,

    sum(case when poi_type in ('restaurant','fast_food','cafe','bar','pub')
             then 1 else 0 end) as food_cnt,

    sum(case when poi_type in ('hotel','motel','hostel','guest_house','apartments')
             then 1 else 0 end) as lodging_cnt,

    sum(case when poi_type in ('hospital','clinic','doctors','pharmacy')
             then 1 else 0 end) as health_cnt,

    max(load_ts) as last_load_ts
  from p
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

  /* counts: 0 for empty cells */
  coalesce(a.poi_points_cnt, 0) as poi_points_cnt,
  coalesce(a.poi_classes_cnt, 0) as poi_classes_cnt,
  coalesce(a.poi_types_cnt, 0) as poi_types_cnt,

  coalesce(a.amenity_cnt, 0) as amenity_cnt,
  coalesce(a.shop_cnt, 0) as shop_cnt,
  coalesce(a.tourism_cnt, 0) as tourism_cnt,
  coalesce(a.leisure_cnt, 0) as leisure_cnt,
  coalesce(a.office_cnt, 0) as office_cnt,
  coalesce(a.craft_cnt, 0) as craft_cnt,
  coalesce(a.man_made_cnt, 0) as man_made_cnt,
  coalesce(a.emergency_cnt, 0) as emergency_cnt,
  coalesce(a.public_transport_cnt, 0) as public_transport_cnt,
  coalesce(a.railway_cnt, 0) as railway_cnt,
  coalesce(a.highway_cnt, 0) as highway_cnt,
  coalesce(a.place_cnt, 0) as place_cnt,

  coalesce(a.parking_cnt, 0) as parking_cnt,
  coalesce(a.mobility_services_cnt, 0) as mobility_services_cnt,
  coalesce(a.retail_core_cnt, 0) as retail_core_cnt,
  coalesce(a.food_cnt, 0) as food_cnt,
  coalesce(a.lodging_cnt, 0) as lodging_cnt,
  coalesce(a.health_cnt, 0) as health_cnt,

  /* densities per km² */
  coalesce(a.poi_points_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0) as poi_points_per_km2,
  coalesce(a.parking_cnt, 0)    / nullif(c.cell_area_m2 / 1e6, 0.0) as parking_per_km2,
  coalesce(a.mobility_services_cnt, 0) / nullif(c.cell_area_m2 / 1e6, 0.0) as mobility_services_per_km2,

  /* NULL for empty cells is expected */
  a.last_load_ts
from cells c
left join agg a
  on a.region_code = c.region_code
 and a.region      = c.region
 and a.h3_r10      = c.h3_r10