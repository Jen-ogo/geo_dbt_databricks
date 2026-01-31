{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

with all_h3 as (

  select region_code, region, h3_pointash3string(geom_wkt_4326, 10) as h3_r10
  from {{ ref('ev_chargers') }}
  where geom_wkt_4326 is not null

  union all
  select region_code, region, h3_pointash3string(geom_wkt_4326, 10) as h3_r10
  from {{ ref('poi_points') }}
  where geom_wkt_4326 is not null

  union all
  select region_code, region, h3_pointash3string(geom_wkt_4326, 10) as h3_r10
  from {{ ref('transit_points') }}
  where geom_wkt_4326 is not null

  union all
  select region_code, region,
         h3_pointash3string(st_astext(st_centroid(geom)), 10) as h3_r10
  from {{ ref('road_segments') }}
  where geom is not null

  union all
  select region_code, region,
         h3_pointash3string(st_astext(st_centroid(geom)), 10) as h3_r10
  from {{ ref('poi_areas') }}
  where geom is not null

  union all
  select region_code, region,
         h3_pointash3string(st_astext(st_centroid(geom)), 10) as h3_r10
  from {{ ref('transit_lines') }}
  where geom is not null

  union all
  select region_code, region,
         h3_pointash3string(st_astext(st_centroid(geom)), 10) as h3_r10
  from {{ ref('activity_places') }}
  where geom is not null
),

distinct_h3 as (
  select distinct region_code, region, h3_r10
  from all_h3
  where h3_r10 is not null
),

cells as (
  select
    region_code,
    region,
    h3_r10,

    h3_boundaryaswkt(h3_stringtoh3(h3_r10)) as cell_wkt_4326,
    h3_centeraswkt(h3_stringtoh3(h3_r10))   as cell_center_wkt_4326

  from distinct_h3
),

geo as (
  select
    *,

    st_setsrid(st_geomfromwkt(cell_wkt_4326), 4326) as cell_geom,
    st_setsrid(st_geomfromwkt(cell_center_wkt_4326), 4326) as cell_center_geom
  from cells
)

select
  region_code,
  region,
  h3_r10,

  cell_geom,
  cell_wkt_4326,

  st_area(st_transform(cell_geom, 3035)) as cell_area_m2,

  cell_center_geom,
  cell_center_wkt_4326

from geo
where cell_wkt_4326 is not null
  and cell_center_wkt_4326 is not null