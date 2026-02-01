{{ config(materialized='table', file_format='delta', partition_by=['region_code','region'], tags=['gold','heavy']) }}

with base as (
  select
    cast(c.region_code as string) as region_code,
    cast(c.region      as string) as region,
    cast(c.h3_r7       as string) as h3_r7,
    cast(c.cell_area_m2 as double)         as cell_area_m2,
    cast(c.cell_wkt_4326 as string)        as cell_wkt_4326,
    cast(c.cell_center_wkt_4326 as string) as cell_center_wkt_4326
  from {{ ref('dim_h3_r7_cells') }} c
  where c.region_code is not null
    and c.region      is not null
    and c.h3_r7       is not null
),

-- degurba is (region_code, h3_r7, year, degurba, last_load_ts)
deg0 as (
  select
    cast(region_code as string) as region_code,
    cast(h3_r7 as string)       as h3_r7,
    try_cast(year as int)       as year,
    try_cast(degurba as int)    as degurba,
    cast(last_load_ts as timestamp) as last_load_ts
  from {{ ref('feat_h3_degurba_r7') }}
  where region_code is not null
    and h3_r7 is not null
),

deg as (
  select region_code, h3_r7, year, degurba, last_load_ts
  from (
    select
      d.*,
      row_number() over (
        partition by region_code, h3_r7
        order by year desc nulls last, last_load_ts desc nulls last
      ) as rn
    from deg0 d
  ) t
  where rn = 1
),

centers as (
  select
    b.region_code,
    b.region,
    b.h3_r7,
    d.degurba,
    case
      when d.degurba = 1 then {{ var('k_urban', 1) }}
      when d.degurba = 2 then {{ var('k_suburban', 2) }}
      when d.degurba = 3 then {{ var('k_rural', 4) }}
      else {{ var('k_suburban', 2) }}
    end as k
  from base b
  left join deg d
    on d.region_code = b.region_code
   and d.h3_r7      = b.h3_r7
),

/* STEP 1: precompute BIGINT center (avoid alias inside explode) */
kring_base as (
  select
    region_code,
    region,
    h3_r7 as h3_center,
    degurba,
    k,
    h3_stringtoh3(h3_r7) as h3_center_h3
  from centers
),

/* STEP 2: explode kring using precomputed h3_center_h3 */
kring as (
  select
    region_code,
    region,
    h3_center,
    degurba,
    k,
    explode(
      transform(
        h3_kring(h3_center_h3, k),
        x -> h3_h3tostring(x)
      )
    ) as h3_neighbor
  from kring_base
),

nb as (
  select
    k.region_code,
    k.region,
    k.h3_center,
    k.degurba,
    k.k,
    k.h3_neighbor,

    d.cell_area_m2 as nb_area_m2,

    pop.pop_total,
    pop.emp_total,

    ch.chargers_cnt,
    poi.poi_points_cnt,
    bld.buildings_cnt,
    roads.roads_len_m_sum,
    roads.roads_major_len_m_sum,
    tr.transit_points_cnt,
    tr.transit_lines_len_m_sum

  from kring k
  join {{ ref('dim_h3_r7_cells') }} d
    on d.region_code = k.region_code
   and d.region      = k.region
   and d.h3_r7       = k.h3_neighbor

  left join {{ ref('feat_h3_pop_r7') }} pop
    on pop.region_code = k.region_code
   and pop.region      = k.region
   and pop.h3_r7       = k.h3_neighbor

  left join {{ ref('feat_h3_charging_r7') }} ch
    on ch.region_code = k.region_code
   and ch.region      = k.region
   and ch.h3_r7       = k.h3_neighbor

  left join {{ ref('feat_h3_poi_points_r7') }} poi
    on poi.region_code = k.region_code
   and poi.region      = k.region
   and poi.h3_r7       = k.h3_neighbor

  left join {{ ref('feat_h3_buildings_r7') }} bld
    on bld.region_code = k.region_code
   and bld.region      = k.region
   and bld.h3_r7       = k.h3_neighbor

  left join {{ ref('feat_h3_roads_r7') }} roads
    on roads.region_code = k.region_code
   and roads.region      = k.region
   and roads.h3_r7       = k.h3_neighbor

  left join {{ ref('feat_h3_transit_point_lines_r7') }} tr
    on tr.region_code = k.region_code
   and tr.region      = k.region
   and tr.h3_r7       = k.h3_neighbor
),

agg as (
  select
    region_code,
    region,
    h3_center as h3_r7,
    degurba,
    k,

    sum(nb_area_m2) as kring_area_m2,

    sum(coalesce(pop_total,0)) as pop_total_kring,
    sum(coalesce(emp_total,0)) as emp_total_kring,

    sum(coalesce(chargers_cnt,0))   as chargers_cnt_kring,
    sum(coalesce(poi_points_cnt,0)) as poi_points_cnt_kring,
    sum(coalesce(buildings_cnt,0))  as buildings_cnt_kring,

    sum(coalesce(roads_len_m_sum,0))       as roads_len_m_sum_kring,
    sum(coalesce(roads_major_len_m_sum,0)) as roads_major_len_m_sum_kring,

    sum(coalesce(transit_points_cnt,0))      as transit_points_cnt_kring,
    sum(coalesce(transit_lines_len_m_sum,0)) as transit_lines_len_m_sum_kring,

    (sum(coalesce(chargers_cnt,0))   * 1e6 / nullif(sum(nb_area_m2),0)) as chargers_per_km2_kring,
    (sum(coalesce(poi_points_cnt,0)) * 1e6 / nullif(sum(nb_area_m2),0)) as poi_points_per_km2_kring,
    (sum(coalesce(buildings_cnt,0))  * 1e6 / nullif(sum(nb_area_m2),0)) as buildings_per_km2_kring,
    (sum(coalesce(roads_len_m_sum,0)) * 1e6 / nullif(sum(nb_area_m2),0)) as roads_len_m_per_km2_kring,

    case when sum(coalesce(pop_total,0)) >= {{ var('min_pop_for_per10k', 500) }}
      then sum(coalesce(chargers_cnt,0)) * 10000.0 / sum(coalesce(pop_total,0))
    end as chargers_per_10k_pop_kring

  from nb
  group by 1,2,3,4,5
)

select * from agg