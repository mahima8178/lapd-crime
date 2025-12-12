-- 05_gold_agg_1.sql
-- Gold #1: daily counts by area + crime type

USE CATALOG public_crime;
USE SCHEMA gold;

CREATE TABLE IF NOT EXISTS crime_daily_area_summary AS
SELECT
  occurrence_date,
  area,
  area_name,
  crm_cd,
  crm_cd_desc,
  COUNT(*) AS incidents,
  AVG(vict_age) AS avg_vict_age
FROM public_crime.silver.crime_event_clean
GROUP BY
  occurrence_date, area, area_name, crm_cd, crm_cd_desc;

-- Sanity
SELECT * FROM crime_daily_area_summary ORDER BY occurrence_date DESC LIMIT 20;
