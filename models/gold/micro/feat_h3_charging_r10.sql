{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code']
) }}

with pop as (
  select
    cast(region_code as string) as region_code,
    cast(h3_r10 as string)      as h3_r10,

    cast(cell_area_m2 as double)        as cell_area_m2,
    cast(cell_wkt_4326 as string)       as cell_wkt_4326,
    cast(cell_center_wkt_4326 as string) as cell_center_wkt_4326,

    cast(pop_total as double) as pop_total,
    cast(last_load_ts as timestamp) as last_load_ts
  from {{ ref('feat_h3_pop_r10_final') }}
  where region_code is not null
    and h3_r10 is not null
),

ch as (

  select
    cast(region_code as string) as region_code,
    cast(h3_r10 as string)      as h3_r10,

    cast(chargers_cnt as bigint) as chargers_cnt,
    cast(last_load_ts as timestamp) as last_load_ts
  from {{ ref('feat_h3_ev_chargers_r10') }}
  where region_code is not null
    and h3_r10 is not null
),

final as (
  select
    p.region_code,
    p.h3_r10,

    p.cell_area_m2,
    p.cell_wkt_4326,
    p.cell_center_wkt_4326,

    coalesce(c.chargers_cnt, 0) as chargers_cnt,
    p.pop_total,

    case when p.pop_total > 0
      then coalesce(c.chargers_cnt, 0) * 10000.0 / p.pop_total
    end as chargers_per_10k_pop,

    coalesce(c.chargers_cnt, 0) / nullif(p.cell_area_m2 / 1e6, 0.0) as chargers_per_km2,

    coalesce(c.last_load_ts, p.last_load_ts) as last_load_ts
  from pop p
  left join ch c
    on c.region_code = p.region_code
   and c.h3_r10      = p.h3_r10
)

select * from final