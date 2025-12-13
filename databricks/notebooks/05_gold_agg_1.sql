USE CATALOG public_crime;
USE SCHEMA gold;

CREATE TABLE IF NOT EXISTS crime_daily_area_summary
USING DELTA
AS
SELECT
  occurrence_date,
  area,
  area_name,
  crm_cd,
  crm_cd_desc,
  COUNT(*) AS incidents,
  AVG(vict_age) AS avg_vict_age
FROM silver.crime_event_clean
GROUP BY
  occurrence_date, area, area_name, crm_cd, crm_cd_desc;

SELECT * FROM crime_daily_area_summary

