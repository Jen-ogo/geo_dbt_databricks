{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region']
) }}

with ap as (
  select
    cast(region_code as string) as region_code,
    cast(region as string)      as region,
    cast(feature_id as string)  as feature_id,

    cast(activity_class as string)    as activity_class,
    lower(cast(activity_type_lc as string)) as building_type_lc,

    cast(building_levels as int) as building_levels,

    geom as geom_src,
    cast(geom_wkt_4326 as string) as geom_wkt_4326,

    cast(load_ts as timestamp) as load_ts
  from {{ ref('activity_places') }}
  where geom is not null or geom_wkt_4326 is not null
),

typed as (
  select
    region_code,
    region,
    feature_id,

    activity_class,
    building_type_lc,
    coalesce(building_levels, 1) as building_levels,

    case
      when geom_src is not null then st_setsrid(geom_src, 4326)
      when geom_wkt_4326 is not null then st_setsrid(st_geomfromwkt(geom_wkt_4326), 4326)
      else null
    end as geom_4326,

    load_ts
  from ap
),

filtered as (
  select *
  from typed
  where geom_4326 is not null
    and activity_class = 'building'
    and building_type_lc is not null
    and building_type_lc <> 'no'

    and building_type_lc not in (
      'yes',
      'outbuilding','farm_auxiliary','shed','barn','sty','stable',
      'garage','garages','roof','greenhouse',
      'allotment_house',
      'hut','cabin'
    )
),

dedup as (
  select *
  from filtered
  {{ dedup_qualify(
      partition_by=['region_code','region','feature_id'],
      order_by=['load_ts desc']
  ) }}
),

enriched as (
  select
    region_code,
    region,

    {{ h3_r7_from_geom_centroid('geom_4326') }} as h3_r7,

    building_type_lc,
    building_levels,

    cast({{ area_m2('geom_4326') }} as double) as footprint_area_m2,

    load_ts
  from dedup
),

poly_only as (
  select *
  from enriched
  where h3_r7 is not null
    and footprint_area_m2 is not null
    and footprint_area_m2 > 0
),

classified as (
  select
    region_code,
    region,
    h3_r7,
    building_levels,
    footprint_area_m2,

    case
      when building_type_lc in (
        'house','detached','apartments','residential','semidetached_house','terrace',
        'bungalow','dormitory'
      ) then 'residential'

      when building_type_lc in (
        'retail','commercial','office','industrial','manufacture','warehouse','service',
        'school','kindergarten','university','hospital','fire_station','government',
        'supermarket','hotel','train_station','church','chapel'
      ) then 'nonresidential'

      when building_type_lc = 'yes' then 'unknown'
      else 'other'
    end as building_group,

    load_ts
  from poly_only
),

agg as (
  select
    region_code,
    region,
    h3_r7,

    count(1) as buildings_cnt,
    sum(case when building_group = 'residential'    then 1 else 0 end) as res_buildings_cnt,
    sum(case when building_group = 'nonresidential' then 1 else 0 end) as nonres_buildings_cnt,
    sum(case when building_group = 'unknown'        then 1 else 0 end) as unknown_buildings_cnt,

    sum(footprint_area_m2) as footprint_area_m2_sum,
    sum(footprint_area_m2 * cast(building_levels as double)) as floor_area_m2_est_sum,

    avg(cast(building_levels as double)) as levels_avg,
    percentile_approx(building_levels, 0.5) as levels_p50,

    percentile_approx(footprint_area_m2, 0.5) as footprint_area_p50_m2,
    percentile_approx(footprint_area_m2, 0.9) as footprint_area_p90_m2,

    max(load_ts) as last_load_ts
  from classified
  group by 1,2,3
),

cell as (
  select
    region_code,
    region,
    h3_r7,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  from {{ ref('dim_h3_r7_cells') }}
)

select
  c.region_code,
  c.region,
  c.h3_r7,

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

  cast('activity_places_to_h3_r7' as string) as building_method,

  a.last_load_ts
from agg a
join cell c
  on c.region_code = a.region_code
 and c.region      = a.region
 and c.h3_r7       = a.h3_r7