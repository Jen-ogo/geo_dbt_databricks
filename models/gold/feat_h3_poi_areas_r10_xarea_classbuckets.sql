{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold','heavy']
) }}

WITH poi AS (
  SELECT
    CAST(region_code AS STRING)         AS region_code,
    CAST(region      AS STRING)         AS region,
    CAST(osm_id      AS STRING)         AS feature_id,

    LOWER(CAST(poi_class AS STRING))    AS poi_class,
    LOWER(CAST(poi_type  AS STRING))    AS poi_type,
    CAST(load_ts AS TIMESTAMP)          AS load_ts,

    geom                                AS poi_geom_4326,
    centroid_geom                       AS poi_centroid_4326
  FROM {{ ref('poi_areas') }}
  WHERE region_code IS NOT NULL
    AND region      IS NOT NULL
    AND osm_id      IS NOT NULL
    AND poi_class   IS NOT NULL
    AND geom        IS NOT NULL
    AND centroid_geom IS NOT NULL
),

poi_fix AS (
  SELECT
    region_code, region, feature_id, poi_class, poi_type, load_ts,

    -- keep SRID=4326 even after buffer(0)
    CASE
      WHEN ST_IsValid(poi_geom_4326) AND NOT ST_IsEmpty(poi_geom_4326)
        THEN poi_geom_4326
      ELSE ST_SetSRID(ST_Buffer(poi_geom_4326, 0.0), 4326)
    END AS poi_geom_4326,

    CASE
      WHEN poi_centroid_4326 IS NOT NULL AND NOT ST_IsEmpty(poi_centroid_4326)
        THEN poi_centroid_4326
      ELSE ST_Centroid(poi_geom_4326)
    END AS poi_centroid_4326
  FROM poi
  WHERE poi_geom_4326 IS NOT NULL
),

poi_h3 AS (
  SELECT
    region_code,
    region,
    feature_id,
    poi_class,
    poi_type,
    load_ts,
    poi_geom_4326,

    -- use existing helper (centroid point -> H3 r10)
    CAST({{ h3_r10_from_geom_point('poi_centroid_4326') }} AS STRING) AS h3_r10
  FROM poi_fix
  WHERE poi_geom_4326 IS NOT NULL
    AND NOT ST_IsEmpty(poi_geom_4326)
    AND ST_IsValid(poi_geom_4326)
),

cells AS (
  SELECT
    CAST(region_code AS STRING)         AS region_code,
    CAST(region      AS STRING)         AS region,
    CAST(h3_r10       AS STRING)        AS h3_r10,

    cell_geom                           AS cell_geom_4326,
    CAST(cell_area_m2 AS DOUBLE)        AS cell_area_m2,

    CAST(cell_wkt_4326 AS STRING)       AS cell_wkt_4326,
    CAST(cell_center_wkt_4326 AS STRING) AS cell_center_wkt_4326
  FROM {{ ref('dim_h3_r10_cells') }}
  WHERE region_code IS NOT NULL
    AND region      IS NOT NULL
    AND h3_r10      IS NOT NULL
    AND cell_geom   IS NOT NULL
    AND cell_area_m2 IS NOT NULL
    AND cell_area_m2 > 0
),

joined AS (
  SELECT
    c.region_code,
    c.region,
    c.h3_r10,

    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,
    c.cell_geom_4326,

    p.feature_id,
    p.poi_class,
    p.poi_type,
    p.load_ts,
    p.poi_geom_4326
  FROM poi_h3 p
  JOIN cells c
    ON c.region_code = p.region_code
   AND c.region      = p.region
   AND c.h3_r10      = p.h3_r10
  WHERE ST_Intersects(p.poi_geom_4326, c.cell_geom_4326)
),

x AS (
  SELECT
    region_code,
    region,
    h3_r10,

    feature_id,
    poi_class,
    poi_type,
    load_ts,

    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,

    -- intersection geometry (SRID stays 4326 if inputs are 4326)
    ST_Intersection(poi_geom_4326, cell_geom_4326) AS inter_geom
  FROM joined
),

metrics AS (
  SELECT
    region_code,
    region,
    h3_r10,
    feature_id,
    poi_class,
    poi_type,
    load_ts,

    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,

    -- IMPORTANT: area in equal-area EPSG:3035
    {{ area_m2('inter_geom') }} AS poi_xarea_m2
  FROM x
  WHERE inter_geom IS NOT NULL
    AND NOT ST_IsEmpty(inter_geom)
),

agg AS (
  SELECT
    region_code,
    region,
    h3_r10,

    ANY_VALUE(cell_area_m2)          AS cell_area_m2,
    ANY_VALUE(cell_wkt_4326)         AS cell_wkt_4326,
    ANY_VALUE(cell_center_wkt_4326)  AS cell_center_wkt_4326,

    TRUE AS has_poi_areas,

    COUNT(DISTINCT feature_id)       AS poi_areas_cnt,
    SUM(poi_xarea_m2)                AS poi_xarea_m2_sum,

    -- 8 class buckets: counts
    COUNT(DISTINCT CASE WHEN poi_class='amenity'  THEN feature_id END) AS poi_amenity_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='shop'     THEN feature_id END) AS poi_shop_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='tourism'  THEN feature_id END) AS poi_tourism_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='office'   THEN feature_id END) AS poi_office_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='leisure'  THEN feature_id END) AS poi_leisure_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='sport'    THEN feature_id END) AS poi_sport_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='building' THEN feature_id END) AS poi_building_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='landuse'  THEN feature_id END) AS poi_landuse_cnt,

    -- 8 class buckets: xarea sums (m²)
    SUM(CASE WHEN poi_class='amenity'  THEN poi_xarea_m2 ELSE 0 END) AS poi_amenity_xarea_m2_sum,
    SUM(CASE WHEN poi_class='shop'     THEN poi_xarea_m2 ELSE 0 END) AS poi_shop_xarea_m2_sum,
    SUM(CASE WHEN poi_class='tourism'  THEN poi_xarea_m2 ELSE 0 END) AS poi_tourism_xarea_m2_sum,
    SUM(CASE WHEN poi_class='office'   THEN poi_xarea_m2 ELSE 0 END) AS poi_office_xarea_m2_sum,
    SUM(CASE WHEN poi_class='leisure'  THEN poi_xarea_m2 ELSE 0 END) AS poi_leisure_xarea_m2_sum,
    SUM(CASE WHEN poi_class='sport'    THEN poi_xarea_m2 ELSE 0 END) AS poi_sport_xarea_m2_sum,
    SUM(CASE WHEN poi_class='building' THEN poi_xarea_m2 ELSE 0 END) AS poi_building_xarea_m2_sum,
    SUM(CASE WHEN poi_class='landuse'  THEN poi_xarea_m2 ELSE 0 END) AS poi_landuse_xarea_m2_sum,

    MAX(load_ts) AS last_load_ts
  FROM metrics
  WHERE poi_xarea_m2 IS NOT NULL
    AND poi_xarea_m2 > 0
  GROUP BY 1,2,3
)

SELECT
  region_code,
  region,
  h3_r10,

  cell_area_m2,
  cell_wkt_4326,
  cell_center_wkt_4326,

  has_poi_areas,

  poi_areas_cnt,
  poi_xarea_m2_sum,
  poi_xarea_m2_sum / NULLIF(cell_area_m2, 0) AS poi_xarea_share,

  -- densities per km²
  poi_areas_cnt     / NULLIF(cell_area_m2 / 1e6, 0) AS poi_areas_per_km2,
  poi_xarea_m2_sum  / NULLIF(cell_area_m2 / 1e6, 0) AS poi_xarea_m2_per_km2,

  -- counts by class
  poi_amenity_cnt, poi_shop_cnt, poi_tourism_cnt, poi_office_cnt,
  poi_leisure_cnt, poi_sport_cnt, poi_building_cnt, poi_landuse_cnt,

  -- shares by count (within POI count)
  poi_amenity_cnt  / NULLIF(poi_areas_cnt, 0) AS poi_amenity_share,
  poi_shop_cnt     / NULLIF(poi_areas_cnt, 0) AS poi_shop_share,
  poi_tourism_cnt  / NULLIF(poi_areas_cnt, 0) AS poi_tourism_share,
  poi_office_cnt   / NULLIF(poi_areas_cnt, 0) AS poi_office_share,
  poi_leisure_cnt  / NULLIF(poi_areas_cnt, 0) AS poi_leisure_share,
  poi_sport_cnt    / NULLIF(poi_areas_cnt, 0) AS poi_sport_share,
  poi_building_cnt / NULLIF(poi_areas_cnt, 0) AS poi_building_share,
  poi_landuse_cnt  / NULLIF(poi_areas_cnt, 0) AS poi_landuse_share,

  -- xarea sums by class
  poi_amenity_xarea_m2_sum, poi_shop_xarea_m2_sum, poi_tourism_xarea_m2_sum, poi_office_xarea_m2_sum,
  poi_leisure_xarea_m2_sum, poi_sport_xarea_m2_sum, poi_building_xarea_m2_sum, poi_landuse_xarea_m2_sum,

  -- shares by cell area (coverage share)
  poi_amenity_xarea_m2_sum  / NULLIF(cell_area_m2, 0) AS poi_amenity_xarea_share,
  poi_shop_xarea_m2_sum     / NULLIF(cell_area_m2, 0) AS poi_shop_xarea_share,
  poi_tourism_xarea_m2_sum  / NULLIF(cell_area_m2, 0) AS poi_tourism_xarea_share,
  poi_office_xarea_m2_sum   / NULLIF(cell_area_m2, 0) AS poi_office_xarea_share,
  poi_leisure_xarea_m2_sum  / NULLIF(cell_area_m2, 0) AS poi_leisure_xarea_share,
  poi_sport_xarea_m2_sum    / NULLIF(cell_area_m2, 0) AS poi_sport_xarea_share,
  poi_building_xarea_m2_sum / NULLIF(cell_area_m2, 0) AS poi_building_xarea_share,
  poi_landuse_xarea_m2_sum  / NULLIF(cell_area_m2, 0) AS poi_landuse_xarea_share,

  last_load_ts
FROM agg