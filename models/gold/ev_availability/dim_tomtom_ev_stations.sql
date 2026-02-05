{{ config(materialized='table') }}
select
  cast(null as string)    as tomtom_poi_id,
  cast(null as string)    as charging_availability_id,
  cast(null as string)    as name,
  cast(null as string)    as brand,
  cast(null as string)    as category,
  cast(null as string)    as country_code,
  cast(null as string)    as country_subdivision,
  cast(null as string)    as municipality,
  cast(null as string)    as street,
  cast(null as string)    as street_number,
  cast(null as string)    as postal_code,
  cast(null as string)    as freeform_address,
  cast(null as double)    as lat,
  cast(null as double)    as lon,
  cast(null as string)    as connectors_static_json,
  cast(null as string)    as raw_poi_json,
  cast(null as timestamp) as updated_at
where 1=0