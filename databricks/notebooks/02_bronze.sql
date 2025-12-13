-- ============================================================
-- BRONZE: LAPD Crime raw JSON → Delta
-- Environment: Databricks SQL Warehouse + Unity Catalog (2025)
-- ============================================================

USE CATALOG public_crime;
USE SCHEMA bronze;

-- 1. Drop and recreate to ensure clean schema (safe for first run)
DROP TABLE IF EXISTS public_crime.bronze.lapd_crime_bronze;

-- 2. Create Bronze Delta table (NO partitions, NO defaults)
CREATE TABLE public_crime.bronze.lapd_crime_bronze (
  dr_no            STRING,
  date_rptd        STRING,
  date_occ         STRING,
  time_occ         STRING,
  area             STRING,
  area_name        STRING,
  rpt_dist_no      STRING,
  part_1_2         STRING,
  crm_cd           STRING,
  crm_cd_desc      STRING,
  mocodes          STRING,
  vict_age         STRING,
  vict_sex         STRING,
  vict_descent     STRING,
  premis_cd        STRING,
  premis_desc      STRING,
  weapon_used_cd   STRING,
  weapon_desc      STRING,
  status           STRING,
  status_desc      STRING,
  crm_cd_1         STRING,
  crm_cd_2         STRING,
  crm_cd_3         STRING,
  crm_cd_4         STRING,
  location         STRING,
  cross_street     STRING,
  lat              STRING,
  lon              STRING
)
USING DELTA;

-- 3. Incremental load from S3 JSON (idempotent)
--    Requires External Location on bucket or parent prefix
COPY INTO public_crime.bronze.lapd_crime_bronze
FROM 's3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/'
FILEFORMAT = JSON
FORMAT_OPTIONS (
  'multiLine' = 'true'
)
COPY_OPTIONS (
  'mergeSchema' = 'true',
  'on_error' = 'continue'
);

-- 4. Sanity checks
SELECT COUNT(*) AS bronze_rows
FROM public_crime.bronze.lapd_crime_bronze;

SELECT *
FROM public_crime.bronze.lapd_crime_bronze
LIMIT 10;
