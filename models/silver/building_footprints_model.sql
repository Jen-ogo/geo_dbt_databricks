{{ config(
    materialized = 'table',
    file_format  = 'delta',
    partition_by = ['region_code','region']
) }}

select *
from {{ ref('building_footprints') }}
where lower(building_type) not in (
  'yes',
  'outbuilding','farm_auxiliary','shed','barn','sty','stable',
  'garage','garages','roof','greenhouse',
  'allotment_house',
  'hut','cabin'
)