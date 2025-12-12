-- 03_silver_fact.sql
-- Goal: Silver fact table (cleaned + dedup by DR_NO)
-- Uses Socrata JSON field names (snake_case) commonly returned by the API.
-- If your keys differ, update column references here only.

USE CATALOG public_crime;
USE SCHEMA silver;

CREATE TABLE IF NOT EXISTS crime_event_clean
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
AS
WITH base AS (
  SELECT
    CAST(dr_no AS STRING)                     AS dr_no,
    TRY_TO_TIMESTAMP(date_rptd)               AS date_rptd_ts,
    TRY_TO_TIMESTAMP(date_occ)                AS date_occ_ts,
    CAST(time_occ AS STRING)                  AS time_occ,
    CAST(area AS INT)                         AS area,
    CAST(area_name AS STRING)                 AS area_name,
    CAST(rpt_dist_no AS STRING)               AS rpt_dist_no,
    CAST(part_1_2 AS STRING)                  AS part_1_2,
    CAST(crm_cd AS INT)                       AS crm_cd,
    CAST(crm_cd_desc AS STRING)               AS crm_cd_desc,
    CAST(mocodes AS STRING)                   AS mocodes,
    CAST(vict_age AS INT)                     AS vict_age,
    CAST(vict_sex AS STRING)                  AS vict_sex,
    CAST(vict_descent AS STRING)              AS vict_descent,
    CAST(premis_cd AS INT)                    AS premis_cd,
    CAST(premis_desc AS STRING)               AS premis_desc,
    CAST(weapon_used_cd AS INT)               AS weapon_used_cd,
    CAST(weapon_desc AS STRING)               AS weapon_desc,
    CAST(status AS STRING)                    AS status,
    CAST(status_desc AS STRING)               AS status_desc,
    CAST(crm_cd_1 AS INT)                     AS crm_cd_1,
    CAST(crm_cd_2 AS INT)                     AS crm_cd_2,
    CAST(crm_cd_3 AS INT)                     AS crm_cd_3,
    CAST(crm_cd_4 AS INT)                     AS crm_cd_4,
    CAST(location AS STRING)                  AS location,
    CAST(cross_street AS STRING)              AS cross_street,
    CAST(lat AS DOUBLE)                       AS lat,
    CAST(lon AS DOUBLE)                       AS lon,

    -- Prefer Socrata system timestamp if present; otherwise fall back to reported timestamp.
    COALESCE(TRY_TO_TIMESTAMP(`:updated_at`), TRY_TO_TIMESTAMP(date_rptd)) AS updated_at_ts,

    -- Stable hash for joins/dedup
    SHA2(CONCAT_WS('|',
      COALESCE(CAST(dr_no AS STRING), ''),
      COALESCE(CAST(crm_cd AS STRING), ''),
      COALESCE(CAST(date_occ AS STRING), ''),
      COALESCE(CAST(time_occ AS STRING), '')
    ), 256) AS event_hash
  FROM public_crime.bronze.lapd_crime_raw_json
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY dr_no
      ORDER BY updated_at_ts DESC NULLS LAST, date_rptd_ts DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  dr_no,
  event_hash,
  DATE(date_occ_ts)     AS occurrence_date,
  DATE(date_rptd_ts)    AS reported_date,
  updated_at_ts,
  time_occ,
  area,
  area_name,
  rpt_dist_no,
  part_1_2,
  crm_cd,
  crm_cd_desc,
  mocodes,
  vict_age,
  vict_sex,
  vict_descent,
  premis_cd,
  premis_desc,
  weapon_used_cd,
  weapon_desc,
  status,
  status_desc,
  crm_cd_1,
  crm_cd_2,
  crm_cd_3,
  crm_cd_4,
  location,
  cross_street,
  lat,
  lon
FROM dedup
WHERE rn = 1;

-- Sanity
SELECT COUNT(*) AS silver_rows FROM crime_event_clean;
SELECT * FROM crime_event_clean LIMIT 5;
