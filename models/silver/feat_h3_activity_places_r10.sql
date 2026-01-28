{{ config(materialized='table', file_format='delta', partition_by=['region_code']) }}

with src as (
  select
    region_code,
    {{ h3_r10_from_geom_centroid("coalesce(geom, st_geomfromwkt(geom_wkt_4326))") }} as h3_r10,
    lower(activity_class) as activity_class,
    activity_type_lc as activity_type,
    load_ts
  from {{ ref('activity_places') }}
  where (geom is not null or geom_wkt_4326 is not null)
    and activity_type_lc is not null
),
agg as (
  select
    region_code,
    h3_r10,
    count(*) as places_cnt,
    sum(case when activity_class='amenity' then 1 else 0 end) as places_amenity_cnt,
    sum(case when activity_class='shop' then 1 else 0 end) as places_shop_cnt,
    sum(case when activity_class='tourism' then 1 else 0 end) as places_tourism_cnt,
    sum(case when activity_class='office' then 1 else 0 end) as places_office_cnt,
    sum(case when activity_class='leisure' then 1 else 0 end) as places_leisure_cnt,
    sum(case when activity_class='sport' then 1 else 0 end) as places_sport_cnt,
    sum(case when activity_type in ('parking','fuel','charging_station') then 1 else 0 end) as places_mobility_cnt,
    sum(case when activity_type in ('supermarket','mall') then 1 else 0 end) as places_retail_big_cnt,
    sum(case when activity_type in ('restaurant','fast_food','cafe') then 1 else 0 end) as places_food_cnt,
    sum(case when activity_type in ('hotel','guest_house','hostel') then 1 else 0 end) as places_stay_cnt,
    max(load_ts) as last_load_ts
  from src
  where h3_r10 is not null
  group by 1,2
)

select
  c.region_code,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  a.places_cnt,
  a.places_amenity_cnt,
  a.places_shop_cnt,
  a.places_tourism_cnt,
  a.places_office_cnt,
  a.places_leisure_cnt,
  a.places_sport_cnt,
  a.places_mobility_cnt,
  a.places_retail_big_cnt,
  a.places_food_cnt,
  a.places_stay_cnt,
  a.last_load_ts
from {{ ref('dim_h3_r10_cells') }} c
left join agg a
  on a.region_code = c.region_code and a.h3_r10 = c.h3_r10