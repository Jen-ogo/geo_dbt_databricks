{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['source_file']
) }}

with base as (
  select
    cast(grd_id as string) as grd_id,

    {{ nullif_neg9999('cast(t as bigint)') }}          as t,
    {{ nullif_neg9999('cast(m as bigint)') }}          as m,
    {{ nullif_neg9999('cast(f as bigint)') }}          as f,
    {{ nullif_neg9999('cast(y_lt15 as bigint)') }}     as y_lt15,
    {{ nullif_neg9999('cast(y_1564 as bigint)') }}     as y_1564,
    {{ nullif_neg9999('cast(y_ge65 as bigint)') }}     as y_ge65,
    {{ nullif_neg9999('cast(emp as bigint)') }}        as emp,

    {{ nullif_neg9999('cast(nat as bigint)') }}        as nat,
    {{ nullif_neg9999('cast(eu_oth as bigint)') }}     as eu_oth,
    {{ nullif_neg9999('cast(oth as bigint)') }}        as oth,
    {{ nullif_neg9999('cast(same as bigint)') }}       as same,
    {{ nullif_neg9999('cast(chg_in as bigint)') }}     as chg_in,
    {{ nullif_neg9999('cast(chg_out as bigint)') }}    as chg_out,

    cast(land_surface as double)                       as land_surface,
    {{ nullif_neg9999('cast(populated as bigint)') }}  as populated,

    -- geometry (Databricks)
    ST_GeomFromWKT(geom_wkt)                            as geom,
    ST_AsBinary(ST_GeomFromWKT(geom_wkt))               as geom_wkb,
    ST_AsText(ST_GeomFromWKT(geom_wkt))                 as geom_wkt_4326,

    cast(source_file as string)                         as source_file,
    cast(load_ts as timestamp)                          as load_ts

  from {{ source('bronze','census_grid_2021_europe') }}
  where geom_wkt is not null
    and {{ nullif_neg9999('cast(t as bigint)') }} is not null
)

select
  md5(concat(coalesce(grd_id,''), '|', coalesce(source_file,''))) as record_id,
  *
from base