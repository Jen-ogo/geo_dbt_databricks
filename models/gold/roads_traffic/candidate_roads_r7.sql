{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold','roads_traffic','heavy']
) }}

with params as (
  select
    cast({{ var('top_candidates', 50) }} as int) as top_candidates,
    cast(ceil({{ var('top_candidates', 50) }} / 3.0) as int) as per_group,

    cast({{ var('roads_per_candidate_area', 30) }} as int) as roads_per_candidate_area,

    cast({{ var('roads_per_station', 30) }} as int) as roads_per_station,
    cast({{ var('ev_station_radius_m', 300) }} as int) as ev_station_radius_m
),

base as (
  select
    region_code,
    region,
    h3_r7,
    degurba,
    macro_score
  from {{ ref('feat_h3_macro_score_r7') }}
  where region_code is not null
    and region is not null
    and h3_r7 is not null
    and degurba in (1,2,3)
),

picked as (
  select
    region_code,
    region,
    h3_r7,
    degurba,
    macro_score
  from (
    select
      b.*,
      row_number() over (partition by degurba order by macro_score desc) as rn_deg
    from base b
  ) t
  where rn_deg <= (select per_group from params)
),

candidates as (
  select
    region_code,
    region,
    h3_r7,
    degurba,
    macro_score
  from (
    select
      p.*,
      row_number() over (order by macro_score desc) as rn_top
    from picked p
  ) t
  where rn_top <= (select top_candidates from params)
),

cand_cells as (
  select
    c.region_code,
    c.region,
    c.h3_r7,
    c.degurba,
    c.macro_score,
    d.cell_geom,
    d.cell_center_geom
  from candidates c
  join {{ ref('dim_h3_r7_cells') }} d
    on d.region_code = c.region_code
   and d.region      = c.region
   and d.h3_r7       = c.h3_r7
  where d.cell_geom is not null
),

/* ---- Major roads only ---- */
roads as (
  select
    r.feature_id,
    r.osm_id,
    r.region_code,
    r.region,
    r.highway,
    r.maxspeed_kph,
    r.lanes,
    r.geom as road_geom,

    st_centroid(r.geom) as road_centroid_geom,

    /* meters via equal-area projection */
    st_length(st_transform(r.geom, 3035)) as road_len_m
  from {{ ref('road_segments') }} r
  where r.geom is not null
    and lower(r.highway) in (
      'motorway','motorway_link',
      'trunk','trunk_link',
      'primary','primary_link',
      'secondary','secondary_link',
      'tertiary','tertiary_link'
    )
),

/* ---------- 1) candidate_area scope ---------- */
candidate_area_roads as (
  select
    cc.region_code,
    cc.region,
    cc.h3_r7,
    cc.degurba,
    cc.macro_score,

    cast('candidate_area' as string) as traffic_scope,
    cast(false as boolean)           as near_ev_station,
    cast(null as string)             as ev_station_id,

    rd.feature_id as road_feature_id,
    rd.osm_id     as road_osm_id,
    rd.highway,
    rd.maxspeed_kph,
    rd.lanes,
    rd.road_len_m,
    st_astext(rd.road_centroid_geom) as road_centroid_wkt_4326
  from cand_cells cc
  join roads rd
    on rd.region_code = cc.region_code
   and rd.region      = cc.region
   and st_contains(cc.cell_geom, rd.road_centroid_geom)
),

candidate_area_limited as (
  select
    region_code,
    region,
    h3_r7,
    degurba,
    macro_score,
    traffic_scope,
    near_ev_station,
    ev_station_id,
    road_feature_id,
    road_osm_id,
    highway,
    maxspeed_kph,
    lanes,
    road_len_m,
    road_centroid_wkt_4326
  from (
    select
      car.*,
      row_number() over (
        partition by region_code, region, h3_r7
        order by road_len_m desc, road_feature_id
      ) as rn_cell
    from candidate_area_roads car
  ) t
  where rn_cell <= (select roads_per_candidate_area from params)
),

/* TomTom EV stations inside candidate cell (from enrichment tables, NOT ev_chargers) */
tomtom_stations_in_candidate as (
  select
    cc.region_code,
    cc.region,
    cc.h3_r7,
    cc.degurba,
    cc.macro_score,

    cast(d.tomtom_poi_id as string) as ev_station_id,

    -- build POINT with SRID=4326 (fix SRID=0)
    st_setsrid(
      st_geomfromwkt(concat('POINT(', cast(d.lon as string), ' ', cast(d.lat as string), ')')),
      4326
    ) as ev_geom,

    m.dist_m,
    m.rank_by_dist
  from cand_cells cc
  join {{ ref('map_candidate_tomtom_stations') }} m
    on  m.region_code = cc.region_code
    and m.region      = cc.region
    and m.h3_r7       = cc.h3_r7
  join {{ ref('dim_tomtom_ev_stations') }} d
    on d.tomtom_poi_id = m.tomtom_poi_id
  where d.lat is not null
    and d.lon is not null
    and m.rank_by_dist <= 10
    and st_contains(
      cc.cell_geom,
      st_setsrid(
        st_geomfromwkt(concat('POINT(', cast(d.lon as string), ' ', cast(d.lat as string), ')')),
        4326
      )
    )
),

/* ---------- 2) station_centric scope ---------- */
station_centric_roads as (
  select
    s.region_code,
    s.region,
    s.h3_r7,
    s.degurba,
    s.macro_score,

    cast('station_centric' as string) as traffic_scope,
    cast(true as boolean)             as near_ev_station,
    s.ev_station_id                   as ev_station_id,

    rd.feature_id as road_feature_id,
    rd.osm_id     as road_osm_id,
    rd.highway,
    rd.maxspeed_kph,
    rd.lanes,
    rd.road_len_m,
    st_astext(rd.road_centroid_geom) as road_centroid_wkt_4326
  from tomtom_stations_in_candidate s
  join roads rd
    on rd.region_code = s.region_code
   and rd.region      = s.region
   and st_distance(s.ev_geom, rd.road_centroid_geom) <= (select ev_station_radius_m from params)
),

station_centric_limited as (
  select
    region_code,
    region,
    h3_r7,
    degurba,
    macro_score,
    traffic_scope,
    near_ev_station,
    ev_station_id,
    road_feature_id,
    road_osm_id,
    highway,
    maxspeed_kph,
    lanes,
    road_len_m,
    road_centroid_wkt_4326
  from (
    select
      scr.*,
      row_number() over (
        partition by region_code, region, h3_r7, ev_station_id
        order by road_len_m desc, road_feature_id
      ) as rn_station
    from station_centric_roads scr
  ) t
  where rn_station <= (select roads_per_station from params)
),

/* ---------- UNION + DEDUP ---------- */
unioned as (
  select * from candidate_area_limited
  union all
  select * from station_centric_limited
),

dedup as (
  select
    region_code,
    region,
    h3_r7,
    degurba,
    macro_score,
    traffic_scope,
    near_ev_station,
    ev_station_id,
    road_feature_id,
    road_osm_id,
    highway,
    maxspeed_kph,
    lanes,
    road_len_m,
    road_centroid_wkt_4326
  from (
    select
      u.*,
      row_number() over (
        partition by region_code, region, h3_r7, road_feature_id
        order by
          case when traffic_scope = 'station_centric' then 1 else 0 end desc,
          road_len_m desc,
          coalesce(ev_station_id, '') desc,
          road_feature_id
      ) as rn_dedup
    from unioned u
  ) t
  where rn_dedup = 1
)

select
  region_code,
  region,
  h3_r7,
  degurba,
  macro_score,
  traffic_scope,
  near_ev_station,
  ev_station_id,
  road_feature_id,
  road_osm_id,
  highway,
  maxspeed_kph,
  lanes,
  road_len_m,
  road_centroid_wkt_4326
from dedup