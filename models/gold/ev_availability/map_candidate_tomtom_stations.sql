{{ config(materialized='table') }}

select
  cast(null as string)    as REGION_CODE,
  cast(null as string)    as REGION,
  cast(null as string)    as H3_R7,
  cast(null as double)    as QUERY_LAT,
  cast(null as double)    as QUERY_LON,
  cast(null as int)       as RADIUS_M,
  cast(null as string)    as TOMTOM_POI_ID,
  cast(null as double)    as DIST_M,
  cast(null as double)    as SCORE,
  cast(null as int)       as RANK_BY_DIST,
  cast(null as timestamp) as LOAD_TS,
  cast(null as string)    as RAW_SEARCH_JSON
where 1=0