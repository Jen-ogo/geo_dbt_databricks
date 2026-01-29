{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['region_code','region'],
    tags=['gold']
) }}

SELECT
  d.region_code,
  d.region,
  d.h3_r10,
  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

  CASE WHEN s.region_code IS NULL THEN FALSE ELSE TRUE END AS has_poi_areas,

  COALESCE(s.poi_areas_cnt, 0)        AS poi_areas_cnt,
  COALESCE(s.poi_xarea_m2_sum, 0)     AS poi_xarea_m2_sum,
  COALESCE(s.poi_xarea_share, 0)      AS poi_xarea_share,
  COALESCE(s.poi_areas_per_km2, 0)    AS poi_areas_per_km2,
  COALESCE(s.poi_xarea_m2_per_km2, 0) AS poi_xarea_m2_per_km2,

  COALESCE(s.poi_amenity_cnt, 0)  AS poi_amenity_cnt,
  COALESCE(s.poi_shop_cnt, 0)     AS poi_shop_cnt,
  COALESCE(s.poi_tourism_cnt, 0)  AS poi_tourism_cnt,
  COALESCE(s.poi_office_cnt, 0)   AS poi_office_cnt,
  COALESCE(s.poi_leisure_cnt, 0)  AS poi_leisure_cnt,
  COALESCE(s.poi_sport_cnt, 0)    AS poi_sport_cnt,
  COALESCE(s.poi_building_cnt, 0) AS poi_building_cnt,
  COALESCE(s.poi_landuse_cnt, 0)  AS poi_landuse_cnt,

  COALESCE(s.poi_amenity_share, 0)  AS poi_amenity_share,
  COALESCE(s.poi_shop_share, 0)     AS poi_shop_share,
  COALESCE(s.poi_tourism_share, 0)  AS poi_tourism_share,
  COALESCE(s.poi_office_share, 0)   AS poi_office_share,
  COALESCE(s.poi_leisure_share, 0)  AS poi_leisure_share,
  COALESCE(s.poi_sport_share, 0)    AS poi_sport_share,
  COALESCE(s.poi_building_share, 0) AS poi_building_share,
  COALESCE(s.poi_landuse_share, 0)  AS poi_landuse_share,

  COALESCE(s.poi_amenity_xarea_m2_sum, 0)  AS poi_amenity_xarea_m2_sum,
  COALESCE(s.poi_shop_xarea_m2_sum, 0)     AS poi_shop_xarea_m2_sum,
  COALESCE(s.poi_tourism_xarea_m2_sum, 0)  AS poi_tourism_xarea_m2_sum,
  COALESCE(s.poi_office_xarea_m2_sum, 0)   AS poi_office_xarea_m2_sum,
  COALESCE(s.poi_leisure_xarea_m2_sum, 0)  AS poi_leisure_xarea_m2_sum,
  COALESCE(s.poi_sport_xarea_m2_sum, 0)    AS poi_sport_xarea_m2_sum,
  COALESCE(s.poi_building_xarea_m2_sum, 0) AS poi_building_xarea_m2_sum,
  COALESCE(s.poi_landuse_xarea_m2_sum, 0)  AS poi_landuse_xarea_m2_sum,

  COALESCE(s.poi_amenity_xarea_share, 0)  AS poi_amenity_xarea_share,
  COALESCE(s.poi_shop_xarea_share, 0)     AS poi_shop_xarea_share,
  COALESCE(s.poi_tourism_xarea_share, 0)  AS poi_tourism_xarea_share,
  COALESCE(s.poi_office_xarea_share, 0)   AS poi_office_xarea_share,
  COALESCE(s.poi_leisure_xarea_share, 0)  AS poi_leisure_xarea_share,
  COALESCE(s.poi_sport_xarea_share, 0)    AS poi_sport_xarea_share,
  COALESCE(s.poi_building_xarea_share, 0) AS poi_building_xarea_share,
  COALESCE(s.poi_landuse_xarea_share, 0)  AS poi_landuse_xarea_share,

  s.last_load_ts
FROM {{ ref('dim_h3_r10_cells') }} d
LEFT JOIN {{ ref('feat_h3_poi_areas_r10_xarea_classbuckets') }} s
  ON s.region_code = d.region_code
 AND s.region      = d.region
 AND s.h3_r10       = d.h3_r10