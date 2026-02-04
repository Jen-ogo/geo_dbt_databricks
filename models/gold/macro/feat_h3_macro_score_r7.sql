{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold','heavy']
) }}

with src as (
  select
    region_code,
    region,
    h3_r7,
    degurba,
    k,

    kring_area_m2,
    pop_total_kring,
    emp_total_kring,
    chargers_cnt_kring,
    poi_points_cnt_kring,
    buildings_cnt_kring,
    roads_len_m_sum_kring,
    roads_major_len_m_sum_kring,
    transit_points_cnt_kring,
    transit_lines_len_m_sum_kring,

    chargers_per_km2_kring,
    poi_points_per_km2_kring,
    buildings_per_km2_kring,
    roads_len_m_per_km2_kring,
    chargers_per_10k_pop_kring

  from {{ ref('feat_h3_macro_kring_r7') }}
  where region_code is not null and region is not null and h3_r7 is not null
),

dim as (
  select
    cast(region_code as string) as region_code,
    cast(region      as string) as region,
    cast(h3_r7       as string) as h3_r7,
    cast(cell_area_m2 as double)         as cell_area_m2,
    cast(cell_wkt_4326 as string)        as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326
  from {{ ref('dim_h3_r7_cells') }}
  where region_code is not null and region is not null and h3_r7 is not null
),

enrich as (
  select
    s.*,

    d.cell_area_m2 as center_cell_area_m2,
    d.cell_wkt_4326,
    d.cell_center_wkt_4326,

    (1 + 3*s.k*(s.k+1)) as expected_cells,

    (s.kring_area_m2 / nullif(d.cell_area_m2 * (1 + 3*s.k*(s.k+1)), 0.0)) as kring_coverage_area_ratio,

    case
      when (s.kring_area_m2 / nullif(d.cell_area_m2 * (1 + 3*s.k*(s.k+1)), 0.0)) is null then null
      else least(
        1.0,
        greatest(
          {{ var('coverage_floor', 0.7) }},
          (s.kring_area_m2 / nullif(d.cell_area_m2 * (1 + 3*s.k*(s.k+1)), 0.0))
        )
      )
    end as coverage_penalty,

    case when coalesce(s.pop_total_kring,0) >= {{ var('pop_gate_min', 1000) }}
      then s.chargers_per_10k_pop_kring
    end as chargers_per_10k_pop_kring_gated

  from src s
  join dim d
    on d.region_code = s.region_code
   and d.region      = s.region
   and d.h3_r7       = s.h3_r7
),

norm as (
  select
    e.*,

    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(pop_total_kring,0))             as pr_pop,
    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(emp_total_kring,0))             as pr_emp,
    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(buildings_per_km2_kring,0))     as pr_bld_dens,
    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(poi_points_per_km2_kring,0))    as pr_poi_dens,
    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(transit_points_cnt_kring,0) * 1.0) as pr_transit_pts,
    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(transit_lines_len_m_sum_kring,0)) as pr_transit_len,
    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(roads_len_m_per_km2_kring,0))    as pr_roads,

    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(chargers_per_km2_kring,0))       as pr_ch_km2,
    percent_rank() over (partition by region_code, region, degurba, k order by coalesce(chargers_per_10k_pop_kring_gated,0)) as pr_ch_10k

  from enrich e
),

score as (
  select
    region_code,
    region,
    h3_r7,
    degurba,
    k,

    -- FIX: правильное имя
    center_cell_area_m2 as cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,

    expected_cells,
    kring_area_m2,
    kring_coverage_area_ratio,
    coverage_penalty,

    pop_total_kring,
    emp_total_kring,
    chargers_cnt_kring,
    chargers_per_km2_kring,
    chargers_per_10k_pop_kring,
    chargers_per_10k_pop_kring_gated,

    poi_points_per_km2_kring,
    buildings_per_km2_kring,
    roads_len_m_per_km2_kring,
    transit_points_cnt_kring,
    transit_lines_len_m_sum_kring,

    -- ranks (нужны для дебага/анализа, и чтобы не было UNRESOLVED)
    pr_pop,
    pr_emp,
    pr_bld_dens,
    pr_poi_dens,
    pr_transit_pts,
    pr_transit_len,
    pr_roads,
    pr_ch_km2,
    pr_ch_10k,

    -- demand
    (
      {{ var('w_pop', 0.30) }} * pr_pop +
      {{ var('w_emp', 0.15) }} * pr_emp +
      {{ var('w_bld', 0.15) }} * pr_bld_dens +
      {{ var('w_poi', 0.10) }} * pr_poi_dens +
      {{ var('w_transit_pts', 0.10) }} * pr_transit_pts +
      {{ var('w_transit_len', 0.05) }} * pr_transit_len +
      {{ var('w_roads', 0.15) }} * pr_roads
    ) as demand_score,

    -- supply (gating: если pop<min -> убираем компоненту per10k)
    case
      when chargers_per_10k_pop_kring_gated is null then
        ({{ var('w_supply_km2', 0.70) }} * pr_ch_km2)
      else
        ({{ var('w_supply_km2', 0.70) }} * pr_ch_km2 +
         {{ var('w_supply_10k', 0.30) }} * pr_ch_10k)
    end as supply_score

  from norm
)

select
  s.*,
  (demand_score - supply_score) as gap_score_raw,
  (demand_score - supply_score) * coalesce(coverage_penalty, 1.0) as macro_score
from score s