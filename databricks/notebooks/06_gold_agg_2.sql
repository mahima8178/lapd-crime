-- 06_gold_agg_2.sql
-- Gold #2: victim profile x crime type x month

USE CATALOG public_crime;
USE SCHEMA gold;

CREATE TABLE IF NOT EXISTS crime_type_victim_summary AS
SELECT
  DATE_TRUNC('month', occurrence_date) AS month,
  crm_cd,
  crm_cd_desc,
  CASE
    WHEN vict_age IS NULL THEN 'UNKNOWN'
    WHEN vict_age < 18 THEN '0-17'
    WHEN vict_age < 30 THEN '18-29'
    WHEN vict_age < 45 THEN '30-44'
    WHEN vict_age < 60 THEN '45-59'
    ELSE '60+'
  END AS age_band,
  vict_sex,
  vict_descent,
  COUNT(*) AS incidents
FROM public_crime.silver.crime_event_clean
GROUP BY
  month, crm_cd, crm_cd_desc, age_band, vict_sex, vict_descent;

-- Sanity
SELECT * FROM crime_type_victim_summary ORDER BY month DESC, incidents DESC LIMIT 50;
