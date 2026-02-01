{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['snapshot'],
    meta = {'zorder_by': ['geo', 'year', 'vehicle']}
) }}

with base as (

    select
        cast(source_file as string) as source_file,
        cast(snapshot    as string) as snapshot,
        cast(dataset     as string) as dataset,
        cast(freq        as string) as freq,
        cast(vehicle     as string) as vehicle,
        cast(unit        as string) as unit,
        cast(geo         as string) as geo,

        cast(year  as int)    as year,
        cast(value as double) as value,

        cast(ingest_ts as timestamp) as ingest_ts,
        cast(load_ts   as timestamp) as load_ts

    from {{ source('bronze', 'eurostat_tran_r_elvehst') }}
    where geo     is not null
      and year    is not null
      and vehicle is not null

),

dedup as (

    select *
    from base
    {{ dedup_qualify(
        partition_by = ['geo', 'year', 'vehicle', 'unit', 'freq'],
        order_by     = ['ingest_ts desc', 'load_ts desc', 'snapshot desc', 'source_file desc']
    ) }}

)

select
    md5(
        concat_ws(
            '|',
            coalesce(geo, ''),
            coalesce(cast(year as string), ''),
            coalesce(vehicle, ''),
            coalesce(unit, ''),
            coalesce(freq, '')
        )
    ) as record_id,
    *
from dedup