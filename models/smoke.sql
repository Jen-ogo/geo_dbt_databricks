{{ config(materialized='view', schema='dbt_dev') }}

with counts as (

    select 'eurostat_tran_r_elvehst' as table_name, count(*) as rows_cnt
    from {{ source('bronze','eurostat_tran_r_elvehst') }}

    union all
    select 'gisco_nuts', count(*) from {{ source('bronze','gisco_nuts') }}

    union all
    select 'eurostat_lau_degurba', count(*) from {{ source('bronze','eurostat_lau_degurba') }}

    union all
    select 'census_grid_2021_europe', count(*) from {{ source('bronze','census_grid_2021_europe') }}

    union all
    select 'osm_admin', count(*) from {{ source('bronze','osm_admin') }}

    union all
    select 'osm_roads', count(*) from {{ source('bronze','osm_roads') }}

    union all
    select 'osm_charging', count(*) from {{ source('bronze','osm_charging') }}

    union all
    select 'osm_poi_points', count(*) from {{ source('bronze','osm_poi_points') }}

    union all
    select 'osm_poi_polygons', count(*) from {{ source('bronze','osm_poi_polygons') }}

    union all
    select 'osm_pt_points', count(*) from {{ source('bronze','osm_pt_points') }}

    union all
    select 'osm_pt_lines', count(*) from {{ source('bronze','osm_pt_lines') }}

    union all
    select 'osm_buildings_activity', count(*) from {{ source('bronze','osm_buildings_activity') }}

)
select *
from counts
order by table_name