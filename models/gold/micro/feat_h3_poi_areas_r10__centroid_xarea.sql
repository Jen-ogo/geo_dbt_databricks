{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold','heavy']
) }}

with poi as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    cast(feature_id  as string) as feature_id,
    lower(cast(poi_class as string)) as poi_class,
    lower(cast(poi_type  as string)) as poi_type,
    cast(load_ts as timestamp) as load_ts,

    geom as poi_geom,

    centroid_geom as poi_centroid_geom
  from {{ ref('poi_areas') }}
  where region_code is not null
    and region      is not null
    and feature_id  is not null
    and poi_class   is not null
    and poi_type    is not null
    and geom is not null
),

poi_fix as (
  select
    region_code,
    region,
    feature_id,
    poi_class,
    poi_type,
    load_ts,

    -- keep SRID 4326 even after fix
    case
      when poi_geom is null or st_isempty(poi_geom) then null
      when st_isvalid(poi_geom) then poi_geom
      else st_setsrid(st_buffer(poi_geom, 0.0), 4326)
    end as poi_geom_fixed,

    -- reuse centroid from silver if available, else compute
    coalesce(
      poi_centroid_geom,
      st_centroid(
        case
          when poi_geom is null or st_isempty(poi_geom) then null
          when st_isvalid(poi_geom) then poi_geom
          else st_setsrid(st_buffer(poi_geom, 0.0), 4326)
        end
      )
    ) as poi_centroid
  from poi
),

poi_h3 as (
  select
    region_code,
    region,
    feature_id,
    poi_class,
    poi_type,
    load_ts,
    poi_geom_fixed as poi_geom,

    cast({{ h3_r10_from_geom_point('poi_centroid') }} as string) as h3_r10
  from poi_fix
  where poi_geom_fixed is not null
    and st_isvalid(poi_geom_fixed)
    and not st_isempty(poi_geom_fixed)
    and poi_centroid is not null
),

cells as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    cast(h3_r10      as string) as h3_r10,

    cell_geom,
    cast(cell_area_m2 as double) as cell_area_m2,
    cast(cell_wkt_4326 as string) as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
    and region      is not null
    and h3_r10      is not null
    and cell_geom is not null
    and cell_area_m2 is not null
    and cell_area_m2 > 0
),

joined as (
  select
    c.region_code,
    c.region,
    c.h3_r10,

    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,
    c.cell_geom,

    p.feature_id,
    p.poi_class,
    p.poi_type,
    p.poi_geom,
    p.load_ts
  from poi_h3 p
  join cells c
    on c.region_code = p.region_code
   and c.region      = p.region
   and c.h3_r10      = p.h3_r10
  where st_intersects(p.poi_geom, c.cell_geom)
),

xarea as (
  select
    region_code,
    region,
    h3_r10,
    feature_id,
    poi_class,
    poi_type,
    load_ts,

    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,

    -- project standard: equal-area EPSG:3035
    {{ area_m2('st_intersection(poi_geom, cell_geom)') }} as poi_area_m2
  from joined
),

agg as (
  select
    region_code,
    region,
    h3_r10,

    any_value(cell_area_m2)         as cell_area_m2,
    any_value(cell_wkt_4326)        as cell_wkt_4326,
    any_value(cell_center_wkt_4326) as cell_center_wkt_4326,

    count(distinct feature_id) as poi_areas_cnt,

    sum(poi_area_m2) as poi_area_m2_sum,
    sum(poi_area_m2) / nullif(any_value(cell_area_m2), 0) as poi_area_share,

    -- counts by class (distinct)
    count(distinct case when poi_class='amenity'  then feature_id end) as amenity_areas_cnt,
    count(distinct case when poi_class='shop'     then feature_id end) as shop_areas_cnt,
    count(distinct case when poi_class='tourism'  then feature_id end) as tourism_areas_cnt,
    count(distinct case when poi_class='office'   then feature_id end) as office_areas_cnt,
    count(distinct case when poi_class='leisure'  then feature_id end) as leisure_areas_cnt,
    count(distinct case when poi_class='sport'    then feature_id end) as sport_areas_cnt,
    count(distinct case when poi_class='building' then feature_id end) as building_areas_cnt,
    count(distinct case when poi_class='landuse'  then feature_id end) as landuse_areas_cnt,

    -- area sums by class
    sum(case when poi_class='amenity'  then poi_area_m2 else 0 end) as amenity_area_m2_sum,
    sum(case when poi_class='shop'     then poi_area_m2 else 0 end) as shop_area_m2_sum,
    sum(case when poi_class='tourism'  then poi_area_m2 else 0 end) as tourism_area_m2_sum,
    sum(case when poi_class='office'   then poi_area_m2 else 0 end) as office_area_m2_sum,
    sum(case when poi_class='leisure'  then poi_area_m2 else 0 end) as leisure_area_m2_sum,
    sum(case when poi_class='sport'    then poi_area_m2 else 0 end) as sport_area_m2_sum,
    sum(case when poi_class='building' then poi_area_m2 else 0 end) as building_area_m2_sum,
    sum(case when poi_class='landuse'  then poi_area_m2 else 0 end) as landuse_area_m2_sum,

    -- densities
    count(distinct feature_id) / nullif(any_value(cell_area_m2) / 1e6, 0.0) as poi_areas_per_km2,
    sum(poi_area_m2)           / nullif(any_value(cell_area_m2) / 1e6, 0.0) as poi_area_m2_per_km2,

    max(load_ts) as last_load_ts
  from xarea
  where poi_area_m2 is not null
    and poi_area_m2 > 0
  group by 1,2,3
)

select * from agg