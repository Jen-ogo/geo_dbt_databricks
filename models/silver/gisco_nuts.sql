{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['year','scale','crs','level']
) }}

with base as (
  select
    cast(nuts_id   as string) as nuts_id,
    cast(cntr_code as string) as cntr_code,
    cast(name_latn as string) as name_latn,
    cast(levl_code as string) as levl_code,
    cast(level     as int)    as level,

    cast(year  as string) as year,
    cast(scale as string) as scale,
    cast(crs   as string) as crs,

    -- raw WKT from bronze
    cast(geom_wkt as string) as geom_wkt_4326_raw,

    -- canonical geometry objects
    st_geomfromwkt(cast(geom_wkt as string))        as geom,
    st_asbinary(st_geomfromwkt(cast(geom_wkt as string))) as geom_wkb,
    st_astext(st_geomfromwkt(cast(geom_wkt as string)))   as geom_wkt_4326,

    cast(source_file as string)    as source_file,
    cast(load_ts     as timestamp) as load_ts
  from {{ source('bronze','gisco_nuts') }}
  where nuts_id   is not null
    and cntr_code is not null
    and level     is not null
    and geom_wkt  is not null
),

dedup as (
  select *
  from base
  {{ dedup_qualify(
      partition_by=['level','cntr_code','nuts_id','year','scale','crs'],
      order_by=['load_ts desc','source_file desc']
  ) }}
),

final as (
  select
    concat('NUTS:', year, ':', scale, ':', crs, ':', cast(level as string), ':', nuts_id) as feature_id,
    nuts_id,
    cntr_code,
    name_latn,
    levl_code,
    level,
    year,
    scale,
    crs,
    geom_wkb,
    geom_wkt_4326,
    geom,
    source_file,
    load_ts
  from dedup
)

select *
from final
where geom_wkb is not null