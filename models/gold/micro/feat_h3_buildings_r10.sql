{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region']
) }}

WITH b AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region  AS STRING) AS region,

    CAST(h3_r10 AS STRING)  AS h3_r10,

    LOWER(CAST(building_type AS STRING))      AS building_type,
    COALESCE(CAST(building_levels AS INT), 1) AS building_levels,


    COALESCE(
      geom,
      ST_SetSRID(ST_GeomFromWKT(geom_wkt_4326), 4326)
    ) AS geom_4326,

    CAST(load_ts AS TIMESTAMP) AS load_ts
  FROM {{ ref('building_footprints_model') }}
  WHERE h3_r10 IS NOT NULL
    AND (geom IS NOT NULL OR geom_wkt_4326 IS NOT NULL)
),


cell AS (
  SELECT
    region_code,
    region,
    h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  FROM {{ ref('dim_h3_r10_cells') }}
),

b2 AS (
  SELECT
    region_code,
    region,
    h3_r10,
    building_levels,


    CAST(ST_Area(ST_Transform(geom_4326, 3035)) AS DOUBLE) AS footprint_area_m2,

    CASE
      WHEN building_type IN (
        'house','detached','apartments','residential','semidetached_house','terrace',
        'bungalow','dormitory'
      ) THEN 'residential'

      WHEN building_type IN (
        'retail','commercial','office','industrial','manufacture','warehouse','service',
        'school','kindergarten','university','hospital','fire_station','government',
        'supermarket','hotel','train_station','church','chapel'
      ) THEN 'nonresidential'

      WHEN building_type = 'yes' THEN 'unknown'
      ELSE 'other'
    END AS building_group,

    load_ts
  FROM b
  WHERE geom_4326 IS NOT NULL
),

agg AS (
  SELECT
    region_code,
    region,
    h3_r10,

    COUNT(1) AS buildings_cnt,
    SUM(CASE WHEN building_group = 'residential'    THEN 1 ELSE 0 END) AS res_buildings_cnt,
    SUM(CASE WHEN building_group = 'nonresidential' THEN 1 ELSE 0 END) AS nonres_buildings_cnt,
    SUM(CASE WHEN building_group = 'unknown'        THEN 1 ELSE 0 END) AS unknown_buildings_cnt,

    SUM(footprint_area_m2) AS footprint_area_m2_sum,
    SUM(footprint_area_m2 * CAST(building_levels AS DOUBLE)) AS floor_area_m2_est_sum,

    AVG(CAST(building_levels AS DOUBLE)) AS levels_avg,
    percentile_approx(building_levels, 0.5) AS levels_p50,

    percentile_approx(footprint_area_m2, 0.5) AS footprint_area_p50_m2,
    percentile_approx(footprint_area_m2, 0.9) AS footprint_area_p90_m2,

    MAX(load_ts) AS last_load_ts
  FROM b2
  GROUP BY 1,2,3
)

SELECT
  c.region_code,
  c.region,
  c.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.buildings_cnt,
  a.res_buildings_cnt,
  a.nonres_buildings_cnt,
  a.unknown_buildings_cnt,

  a.footprint_area_m2_sum,
  a.floor_area_m2_est_sum,

  a.levels_avg,
  a.levels_p50,
  a.footprint_area_p50_m2,
  a.footprint_area_p90_m2,

  /* densities per km2 */
  CAST(a.buildings_cnt AS DOUBLE) / NULLIF(c.cell_area_m2 / 1e6, 0)         AS buildings_per_km2,
  a.footprint_area_m2_sum          / NULLIF(c.cell_area_m2 / 1e6, 0)         AS footprint_m2_per_km2,
  a.floor_area_m2_est_sum          / NULLIF(c.cell_area_m2 / 1e6, 0)         AS floor_area_m2_per_km2,

  /* built-up share */
  a.footprint_area_m2_sum          / NULLIF(c.cell_area_m2, 0)               AS built_up_share,

  a.last_load_ts
FROM agg a
JOIN cell c
  ON c.region_code = a.region_code
 AND c.region      = a.region
 AND c.h3_r10       = a.h3_r10
