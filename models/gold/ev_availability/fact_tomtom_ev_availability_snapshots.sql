{{ config(materialized='table') }}

select
  cast(null as string)    as CHARGING_AVAILABILITY_ID,
  cast(null as timestamp) as SNAPSHOT_TS,
  cast(null as string)    as CONNECTOR_TYPE,
  cast(null as double)    as POWER_KW,
  cast(null as int)       as TOTAL,
  cast(null as int)       as AVAILABLE,
  cast(null as int)       as OCCUPIED,
  cast(null as int)       as RESERVED,
  cast(null as int)       as UNKNOWN,
  cast(null as int)       as OUT_OF_SERVICE,
  cast(null as string)    as RAW_AVAIL_JSON
where 1=0