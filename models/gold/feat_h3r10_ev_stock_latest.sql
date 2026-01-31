{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold','heavy']
) }}

with
admin4 as (
  select
    region_code,
    region,
    cast(osm_id as string) as admin_osm_id,
    name as admin_name,
    geom as admin_geom
  from {{ ref('admin_areas') }}
  where admin_level = 4
    and geom is not null
),

nuts2 as (
  select
    cntr_code,
    nuts_id,
    cast(level as int) as nuts_level,
    name_latn as nuts_name,

    st_setsrid(geom, 4326) as nuts_geom
  from {{ ref('gisco_nuts') }}
  where year  = '2024'
    and scale = '01m'
    and crs   = '4326'
    and cast(level as int) = 2
    and geom is not null
),

project_nuts2_scope as (
  select distinct
    a.region_code,
    a.region,
    a.admin_osm_id,
    a.admin_name,
    n.cntr_code,
    n.nuts_id,
    n.nuts_level,
    n.nuts_name
  from admin4 a
  join nuts2 n
    on st_within(st_centroid(n.nuts_geom), a.admin_geom)
),

cells as (
  select
    region_code,
    region,
    h3_r10,

    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,

    cell_geom,
    cell_center_geom
  from {{ ref('dim_h3_r10_cells') }}
  where region_code is not null
    and region is not null
    and h3_r10 is not null
    and cell_center_geom is not null
),

h3_to_nuts2 as (
  select
    c.region_code,
    c.region,
    c.h3_r10,

    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,
    c.cell_geom,
    c.cell_center_geom,

    s.cntr_code,
    s.nuts_id,
    s.nuts_level,
    s.nuts_name,

    n.nuts_geom,
    st_astext(n.nuts_geom) as nuts_wkt_4326
  from cells c
  join project_nuts2_scope s
    on s.region_code = c.region_code
   and s.region      = c.region
  join nuts2 n
    on n.cntr_code = s.cntr_code
   and n.nuts_id   = s.nuts_id
  where st_within(c.cell_center_geom, n.nuts_geom)
),

eu as (
  select
    geo,
    cast(year as int) as year,
    vehicle,
    unit,
    freq,
    cast(value as double) as value
  from {{ ref('eurostat_tran_r_elvehst') }}
  where freq = 'A'
    and vehicle in ('CAR','VG_LE3P5','BUS_MCO_TRO')
    and unit in ('NR','PC')
),

latest_year as (
  select geo, max(year) as car_year_latest
  from eu
  group by 1
),

eu_latest as (
  select e.geo, e.year, e.vehicle, e.unit, e.value
  from eu e
  join latest_year y
    on y.geo = e.geo and y.car_year_latest = e.year
),

pvt as (
  select
    geo,
    max(case when vehicle='CAR'         and unit='NR' then value end) as car_ev_nr_latest,
    max(case when vehicle='CAR'         and unit='PC' then value end) as car_ev_pc_latest,
    max(case when vehicle='VG_LE3P5'    and unit='NR' then value end) as vg_le3p5_ev_nr_latest,
    max(case when vehicle='VG_LE3P5'    and unit='PC' then value end) as vg_le3p5_ev_pc_latest,
    max(case when vehicle='BUS_MCO_TRO' and unit='NR' then value end) as bus_ev_nr_latest,
    max(case when vehicle='BUS_MCO_TRO' and unit='PC' then value end) as bus_ev_pc_latest
  from eu_latest
  group by 1
)

select
  m.region_code,
  m.region,
  m.h3_r10,

  m.cell_area_m2,
  m.cell_wkt_4326,
  m.cell_center_wkt_4326,
  m.cell_geom,
  m.cell_center_geom,

  m.cntr_code,
  m.nuts_id,
  m.nuts_level,
  m.nuts_name,
  m.nuts_geom,
  m.nuts_wkt_4326,

  y.car_year_latest,
  p.car_ev_nr_latest,
  p.car_ev_pc_latest,
  p.vg_le3p5_ev_nr_latest,
  p.vg_le3p5_ev_pc_latest,
  p.bus_ev_nr_latest,
  p.bus_ev_pc_latest

from h3_to_nuts2 m
join latest_year y on y.geo = m.nuts_id
join pvt p        on p.geo = m.nuts_id

where y.car_year_latest is not null
  and p.car_ev_nr_latest is not null
  and p.car_ev_pc_latest is not null
  and p.vg_le3p5_ev_nr_latest is not null
  and p.vg_le3p5_ev_pc_latest is not null
  and p.bus_ev_nr_latest is not null
  and p.bus_ev_pc_latest is not null