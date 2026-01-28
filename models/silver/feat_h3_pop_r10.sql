{{ config(materialized='table', file_format='delta', partition_by=['region_code']) }}

with h3 as (
  select
    region_code,
    h3_r10,
    cell_center_wkt_4326,
    st_geomfromwkt(cell_center_wkt_4326) as cell_center_geom
  from {{ ref('dim_h3_r10_cells') }}
  where cell_center_wkt_4326 is not null
),

grid as (
  select
    cast(grd_id as string) as grid_id,
    nullif(t, -9999)      as pop_total,
    nullif(m, -9999)      as pop_male,
    nullif(f, -9999)      as pop_female,
    nullif(y_lt15, -9999) as pop_age_lt15,
    nullif(y_1564, -9999) as pop_age_1564,
    nullif(y_ge65, -9999) as pop_age_ge65,
    nullif(emp, -9999)    as emp_total,

    st_geomfromwkt(geom_wkt) as cell_geom,
    cast(load_ts as timestamp)  as load_ts
  from {{ source('bronze','census_grid_2021_europe') }}
  where geom_wkt is not null
    and nullif(t, -9999) is not null
),

h3_grid as (
  select
    h.region_code,
    h.h3_r10,

    g.pop_total,
    g.pop_male,
    g.pop_female,
    g.pop_age_lt15,
    g.pop_age_1564,
    g.pop_age_ge65,
    g.emp_total,

    g.grid_id,
    g.load_ts
  from h3 h
  join grid g
    on st_contains(g.cell_geom, h.cell_center_geom)
  qualify row_number() over (
    partition by h.region_code, h.h3_r10
    order by g.load_ts desc, g.grid_id
  ) = 1
),

agg as (
  select
    region_code,
    h3_r10,

    pop_total,
    pop_male,
    pop_female,
    pop_age_lt15,
    pop_age_1564,
    pop_age_ge65,
    emp_total,

    case when pop_total > 0 then pop_age_ge65/pop_total end as share_age_ge65,
    case when pop_total > 0 then pop_age_lt15/pop_total end as share_age_lt15,
    case when pop_total > 0 then emp_total/pop_total end     as share_emp,

    1 as grid_cells_cnt,
    load_ts as last_load_ts
  from h3_grid
)

select
  a.region_code,
  a.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,


  a.pop_total, a.pop_male, a.pop_female,
  a.pop_age_lt15, a.pop_age_1564, a.pop_age_ge65,
  a.emp_total,

  a.share_age_ge65, a.share_age_lt15, a.share_emp,
  a.grid_cells_cnt,
  a.last_load_ts
from agg a
join {{ ref('dim_h3_r10_cells') }} c
  on c.region_code = a.region_code and c.h3_r10 = a.h3_r10