{{ config(materialized='table', file_format='delta', partition_by=['region_code','region']) }}

with admin4 as (
  select
    region_code,
    region,
    geom as admin_geom
  from {{ ref('admin_areas') }}
  where admin_level = 4
    and geom is not null
),

h3 as (
  select
    region_code,
    region,
    h3_r10,
    cell_wkt_4326,
    cell_center_wkt_4326,
    cell_area_m2,

    st_geomfromwkt(cell_center_wkt_4326) as cell_center_geom
  from {{ ref('dim_h3_r10_cells') }}
  where cell_center_wkt_4326 is not null
),
grid_scoped as (
  select
    a.region_code,
    a.region,

    cast(g.grd_id as string) as grid_id,

    {{ nullif_neg9999('cast(g.t as double)') }}       as pop_total,
    {{ nullif_neg9999('cast(g.m as double)') }}       as pop_male,
    {{ nullif_neg9999('cast(g.f as double)') }}       as pop_female,
    {{ nullif_neg9999('cast(g.y_lt15 as double)') }}  as pop_age_lt15,
    {{ nullif_neg9999('cast(g.y_1564 as double)') }}  as pop_age_1564,
    {{ nullif_neg9999('cast(g.y_ge65 as double)') }}  as pop_age_ge65,
    {{ nullif_neg9999('cast(g.emp as double)') }}     as emp_total,

    {{ nullif_neg9999('cast(g.nat as double)') }}     as nat,
    {{ nullif_neg9999('cast(g.eu_oth as double)') }}  as eu_oth,
    {{ nullif_neg9999('cast(g.oth as double)') }}     as oth,
    {{ nullif_neg9999('cast(g.same as double)') }}    as same,
    {{ nullif_neg9999('cast(g.chg_in as double)') }}  as chg_in,
    {{ nullif_neg9999('cast(g.chg_out as double)') }} as chg_out,

    cast(g.land_surface as double)                    as land_surface,
    {{ nullif_neg9999('cast(g.populated as double)') }} as populated,

    g.geom as cell_geom,
    cast(g.load_ts as timestamp) as load_ts
  from {{ ref('census_grid_2021_europe') }} g
  join admin4 a
    on st_contains(a.admin_geom, st_setsrid(st_centroid(g.geom), 4326))
  where g.geom is not null
    and {{ nullif_neg9999('cast(g.t as double)') }} is not null
),

grid_scoped_dedup as (
  select *
  from grid_scoped
  qualify row_number() over (
    partition by region_code, region, grid_id
    order by load_ts desc, grid_id
  ) = 1
),
h3_grid as (
  select
    h.region_code,
    h.region,
    h.h3_r10,
    g.grid_id,

    g.pop_total,
    g.pop_male,
    g.pop_female,
    g.pop_age_lt15,
    g.pop_age_1564,
    g.pop_age_ge65,
    g.emp_total,

    g.nat,
    g.eu_oth,
    g.oth,
    g.same,
    g.chg_in,
    g.chg_out,

    g.land_surface,
    g.populated,

    g.load_ts
  from h3 h
  left join grid_scoped_dedup g
    on h.region_code = g.region_code
   and h.region      = g.region
   and st_contains(g.cell_geom, h.cell_center_geom)
),
grid_cnt as (
  select
    region_code,
    region,
    grid_id,
    count(*) as h3_cnt_in_grid
  from h3_grid
  where grid_id is not null
  group by 1,2,3
),

alloc as (
  select
    hg.region_code,
    hg.region,
    hg.h3_r10,
    hg.grid_id,
    gc.h3_cnt_in_grid,

    hg.pop_total    / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as pop_total,
    hg.pop_male     / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as pop_male,
    hg.pop_female   / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as pop_female,
    hg.pop_age_lt15 / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as pop_age_lt15,
    hg.pop_age_1564 / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as pop_age_1564,
    hg.pop_age_ge65 / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as pop_age_ge65,
    hg.emp_total    / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as emp_total,

    hg.nat     / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as nat,
    hg.eu_oth  / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as eu_oth,
    hg.oth     / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as oth,
    hg.same    / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as same,
    hg.chg_in  / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as chg_in,
    hg.chg_out / nullif(cast(gc.h3_cnt_in_grid as double), 0.0) as chg_out,

    hg.land_surface,
    hg.populated,

    case when (hg.pop_total / nullif(cast(gc.h3_cnt_in_grid as double), 0.0)) > 0
      then (hg.pop_age_ge65 / nullif(cast(gc.h3_cnt_in_grid as double), 0.0))
         / (hg.pop_total    / nullif(cast(gc.h3_cnt_in_grid as double), 0.0))
    end as share_age_ge65,

    case when (hg.pop_total / nullif(cast(gc.h3_cnt_in_grid as double), 0.0)) > 0
      then (hg.pop_age_lt15 / nullif(cast(gc.h3_cnt_in_grid as double), 0.0))
         / (hg.pop_total    / nullif(cast(gc.h3_cnt_in_grid as double), 0.0))
    end as share_age_lt15,

    case when (hg.pop_total / nullif(cast(gc.h3_cnt_in_grid as double), 0.0)) > 0
      then (hg.emp_total / nullif(cast(gc.h3_cnt_in_grid as double), 0.0))
         / (hg.pop_total / nullif(cast(gc.h3_cnt_in_grid as double), 0.0))
    end as share_emp,

    case when hg.grid_id is not null then 1 end as grid_cells_cnt,
    hg.load_ts as last_load_ts
  from h3_grid hg
  left join grid_cnt gc
    on gc.region_code = hg.region_code
   and gc.region      = hg.region
   and gc.grid_id     = hg.grid_id
)

select
  a.region_code,
  a.region,
  a.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.grid_id,
  a.h3_cnt_in_grid,

  a.pop_total, a.pop_male, a.pop_female,
  a.pop_age_lt15, a.pop_age_1564, a.pop_age_ge65,
  a.emp_total,

  a.nat, a.eu_oth, a.oth, a.same, a.chg_in, a.chg_out,
  a.land_surface,
  a.populated,

  a.share_age_ge65, a.share_age_lt15, a.share_emp,

  a.grid_cells_cnt,
  cast(a.grid_cells_cnt * 1000000 as double) as support_area_m2,

  cast('census_grid_1km_to_h3_allocated' as string) as pop_method,

  a.pop_total / nullif(cast(a.grid_cells_cnt as double), 0.0) as pop_per_km2_support,
  a.emp_total / nullif(cast(a.grid_cells_cnt as double), 0.0) as emp_per_km2_support,

  a.pop_total / nullif(c.cell_area_m2 / 1e6, 0.0) as pop_per_km2_hex,
  a.emp_total / nullif(c.cell_area_m2 / 1e6, 0.0) as emp_per_km2_hex,

  a.last_load_ts
from alloc a
join {{ ref('dim_h3_r10_cells') }} c
  on c.region_code = a.region_code
 and c.region      = a.region
 and c.h3_r10      = a.h3_r10