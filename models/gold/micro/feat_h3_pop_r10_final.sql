{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code']
) }}

with s as (
  select
    cast(region_code as string) as region_code,
    cast(h3_r10 as string)      as h3_r10,

    cast(pop_total as double)    as pop_total,
    cast(pop_male as double)     as pop_male,
    cast(pop_female as double)   as pop_female,
    cast(pop_age_lt15 as double) as pop_age_lt15,
    cast(pop_age_1564 as double) as pop_age_1564,
    cast(pop_age_ge65 as double) as pop_age_ge65,
    cast(emp_total as double)    as emp_total,

    cast(share_age_ge65 as double) as share_age_ge65,
    cast(share_age_lt15 as double) as share_age_lt15,
    cast(share_emp as double)      as share_emp,

    cast(grid_cells_cnt as bigint)  as grid_cells_cnt,
    cast(last_load_ts as timestamp) as last_load_ts
  from {{ ref('feat_h3_pop_r10') }}
  where region_code is not null
    and h3_r10 is not null
),

geo as (
  select
    region_code,
    h3_r10,

    -- H3 item (engine-native) только внутри вычислений
    h3_boundaryaswkt(h3_stringtoh3(h3_r10)) as cell_wkt_4326,
    h3_centeraswkt(h3_stringtoh3(h3_r10))   as cell_center_wkt_4326,

    cast(
      st_area(
        st_transform(
          st_setsrid(st_geomfromwkt(h3_boundaryaswkt(h3_stringtoh3(h3_r10))), 4326),
          3035
        )
      ) as double
    ) as cell_area_m2,

    pop_total, pop_male, pop_female,
    pop_age_lt15, pop_age_1564, pop_age_ge65,
    emp_total,

    share_age_ge65, share_age_lt15, share_emp,
    grid_cells_cnt,
    last_load_ts
  from s
)

select
  region_code,
  h3_r10,

  cell_wkt_4326,
  cell_center_wkt_4326,
  cell_area_m2,

  pop_total, pop_male, pop_female,
  pop_age_lt15, pop_age_1564, pop_age_ge65,
  emp_total,

  share_age_ge65,
  share_age_lt15,
  share_emp,

  grid_cells_cnt,

  cast(grid_cells_cnt * 1000000 as double) as support_area_m2,
  cast('census_grid_1km_to_h3' as string) as pop_method,

  pop_total / nullif(cast(grid_cells_cnt as double), 0.0) as pop_per_km2_support,
  emp_total / nullif(cast(grid_cells_cnt as double), 0.0) as emp_per_km2_support,

  pop_total / nullif(cell_area_m2 / 1e6, 0.0) as pop_per_km2_hex,
  emp_total / nullif(cell_area_m2 / 1e6, 0.0) as emp_per_km2_hex,

  last_load_ts
from geo