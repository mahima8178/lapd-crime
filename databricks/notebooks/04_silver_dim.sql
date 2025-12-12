-- 04_silver_dim.sql
-- Goal: Silver dimension table for location/premise.
-- This is your 2nd silver layer.

USE CATALOG public_crime;
USE SCHEMA silver;

CREATE TABLE IF NOT EXISTS dim_location AS
SELECT
  SHA2(CONCAT_WS('|',
    COALESCE(CAST(area AS STRING), ''),
    COALESCE(area_name, ''),
    COALESCE(rpt_dist_no, ''),
    COALESCE(CAST(premis_cd AS STRING), ''),
    COALESCE(premis_desc, '')
  ), 256) AS loc_hash,
  area,
  area_name,
  rpt_dist_no,
  premis_cd,
  premis_desc,
  lat,
  lon
FROM public_crime.silver.crime_event_clean
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY area, area_name, rpt_dist_no, premis_cd, premis_desc, lat, lon
  ORDER BY updated_at_ts DESC NULLS LAST
) = 1;

-- Sanity
SELECT COUNT(*) AS dim_rows FROM dim_location;
SELECT * FROM dim_location LIMIT 5;
