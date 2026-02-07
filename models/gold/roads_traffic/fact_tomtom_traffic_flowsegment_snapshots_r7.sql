{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold','roads_traffic','tomtom','traffic','r7','heavy']
) }}

-- FACT: TomTom Traffic flowSegmentData snapshots for candidate roads (R7)
-- One row = one candidate-road (from GOLD.CANDIDATE_ROADS_R7) + one TomTom response at road centroid
-- DDL-by-select: empty table with stable schema for DAC landing.

select
  /* run metadata */
  cast(null as string)                 as run_id,
  cast(null as timestamp)              as snapshot_ts,

  /* request point */
  cast(null as string)                 as request_point_key,
  cast(null as double)                 as request_point_lat,
  cast(null as double)                 as request_point_lon,
  cast(null as int)                    as tomtom_zoom,
  cast(null as string)                 as tomtom_version,

  /* candidate context */
  cast(null as string)                 as region_code,
  cast(null as string)                 as region,
  cast(null as string)                 as h3_r7,
  cast(null as int)                    as degurba,
  cast(null as double)                 as macro_score,

  cast(null as string)                 as traffic_scope,      -- candidate_area | station_centric
  cast(null as boolean)                as near_ev_station,
  cast(null as string)                 as ev_station_id,

  cast(null as string)                 as road_feature_id,
  cast(null as string)                 as road_osm_id,
  cast(null as string)                 as highway,
  cast(null as double)                 as maxspeed_kph,
  cast(null as double)                 as lanes,
  cast(null as double)                 as road_len_m,
  cast(null as string)                 as road_centroid_wkt_4326,   -- POINT(lon lat)

  /* TomTom parsed metrics */
  cast(null as string)                 as frc,
  cast(null as int)                    as current_speed,
  cast(null as int)                    as free_flow_speed,
  cast(null as int)                    as current_travel_time,
  cast(null as int)                    as free_flow_travel_time,
  cast(null as double)                 as confidence,
  cast(null as boolean)                as road_closure,

  /* geometry payload */
  cast(null as string)                 as segment_linestring_wkt_4326, -- LINESTRING(lon lat,...)
  cast(null as string)                 as segment_coords_json,         -- JSON string (keep simple for Delta)

  /* derived */
  cast(null as double)                 as speed_ratio,
  cast(null as int)                    as delay_sec,
  cast(null as double)                 as delay_ratio,

  /* diagnostics */
  cast(null as int)                    as http_status,
  cast(null as string)                 as error_message,

  /* raw (optional) */
  cast(null as string)                 as raw_json

where 1=0