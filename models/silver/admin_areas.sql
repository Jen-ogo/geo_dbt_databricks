{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region']
) }}

with base as (
  select
    concat('A', cast(osm_id as string))                 as feature_id,
    cast(osm_id as string)                             as osm_id,

    cast(nullif(name,'') as string)                    as name,
    cast(admin_level as int)                           as admin_level,
    cast(boundary as string)                           as boundary,
    cast(type as string)                               as type,
    cast(other_tags as string)                         as other_tags_raw,

    -- tags extracted from raw other_tags (Spark regexp)
    try_cast(
      nullif(trim(regexp_extract(other_tags, '\\"population\\"=>\\"([^\\"]*)\\"', 1)), '')
      as bigint
    )                                                  as population,

    try_cast(
      nullif(trim(regexp_extract(other_tags, '\\"population:date\\"=>\\"([^\\"]*)\\"', 1)), '')
      as date
    )                                                  as population_date,

    cast(
      nullif(trim(regexp_extract(other_tags, '\\"name:en\\"=>\\"([^\\"]*)\\"', 1)), '')
      as string
    )                                                  as name_en,

    -- geometry
    geom_wkb                                            as geom_wkb,
    ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326)          as geom,
    ST_AsText(ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326)) as geom_wkt_4326,

    -- canonical naming: country -> region_code
    cast(country as string)                            as region_code,
    cast(region as string)                             as region,

    cast(dt as date)                                   as dt,
    cast(source_file as string)                        as source_file,
    cast(load_ts as timestamp)                         as load_ts
  from {{ source('bronze','osm_admin') }}
  where osm_id is not null
    and geom_wkb is not null
),

ranked as (
  select
    *,
    row_number() over (
      partition by region_code, region, osm_id
      order by dt desc, load_ts desc, source_file desc
    ) as rn
  from base
)

select
  *
from ranked
where rn = 1
  and geom_wkt_4326 is not null