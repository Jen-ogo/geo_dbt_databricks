{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region']
) }}

with ap as (
  select
    region_code,
    region,
    feature_id,
    activity_class,
    activity_type_lc as building_type,
    coalesce(cast(building_levels as int), 1) as building_levels,
    geom,
    geom_wkt_4326,
    load_ts
  from {{ ref('activity_places') }}
  where activity_class = 'building'
    and geom is not null
),

-- footprints_model filter (твоя логика)
ap_model as (
  select *
  from ap
  where lower(building_type) not in (
    'yes',
    'outbuilding','farm_auxiliary','shed','barn','sty','stable',
    'garage','garages','roof','greenhouse',
    'allotment_house',
    'hut','cabin'
  )
),

footprints as (
  select
    region_code,
    region,
    feature_id,
    lower(building_type) as building_type,
    building_levels,
    geom as geom_4326,
    load_ts,

    st_centroid(geom) as centroid_geom,
    {{ h3_r10_from_geog_point('st_centroid(geom)') }} as h3_r10,

    cast(st_area(st_transform(geom, 3035)) as double) as footprint_area_m2
  from ap_model
  -- только полигоны
  where geom_wkt_4326 is not null
    and (
      upper(substr(geom_wkt_4326, 1, 7)) = 'POLYGON'
      or upper(substr(geom_wkt_4326, 1, 12)) = 'MULTIPOLYGON'
    )
),

b2 as (
  select
    region_code,
    region,
    h3_r10,
    building_levels,
    footprint_area_m2,
    case
      when building_type in (
        'house','detached','apartments','residential','semidetached_house','terrace',
        'bungalow','dormitory'
      ) then 'residential'
      when building_type in (
        'retail','commercial','office','industrial','manufacture','warehouse','service',
        'school','kindergarten','university','hospital','fire_station','government',
        'supermarket','hotel','train_station','church','chapel'
      ) then 'nonresidential'
      when building_type = 'yes' then 'unknown'
      else 'other'
    end as building_group,
    load_ts
  from footprints
  where h3_r10 is not null
),

agg as (
  select
    region_code,
    region,
    h3_r10,

    count(1) as buildings_cnt,
    sum(case when building_group='residential' then 1 else 0 end) as res_buildings_cnt,
    sum(case when building_group='nonresidential' then 1 else 0 end) as nonres_buildings_cnt,
    sum(case when building_group='unknown' then 1 else 0 end) as unknown_buildings_cnt,

    sum(footprint_area_m2) as footprint_area_m2_sum,
    sum(footprint_area_m2 * cast(building_levels as double)) as floor_area_m2_est_sum,

    avg(cast(building_levels as double)) as levels_avg,
    percentile_approx(building_levels, 0.5) as levels_p50,

    percentile_approx(footprint_area_m2, 0.5) as footprint_area_p50_m2,
    percentile_approx(footprint_area_m2, 0.9) as footprint_area_p90_m2,

    max(load_ts) as last_load_ts
  from b2
  group by 1,2,3
),

cell as (
  select
    region_code, region, h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('dim_h3_r10_cells') }}
)

select
  c.region_code,
  c.region,
  c.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.buildings_cnt,
  a.res_buildings_cnt,
  a.nonres_buildings_cnt,
  a.unknown_buildings_cnt,

  a.footprint_area_m2_sum,
  a.floor_area_m2_est_sum,

  a.levels_avg,
  a.levels_p50,
  a.footprint_area_p50_m2,
  a.footprint_area_p90_m2,

  cast(a.buildings_cnt as double) / nullif(c.cell_area_m2 / 1e6, 0) as buildings_per_km2,
  a.footprint_area_m2_sum / nullif(c.cell_area_m2 / 1e6, 0)         as footprint_m2_per_km2,
  a.floor_area_m2_est_sum / nullif(c.cell_area_m2 / 1e6, 0)         as floor_area_m2_per_km2,

  a.footprint_area_m2_sum / nullif(c.cell_area_m2, 0)               as built_up_share,

  cast('activity_places_polygon_centroid' as string) as building_method,

  a.last_load_ts
from agg a
join cell c
  on c.region_code=a.region_code and c.region=a.region and c.h3_r10=a.h3_r10
