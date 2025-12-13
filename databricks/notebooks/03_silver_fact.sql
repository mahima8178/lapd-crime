USE CATALOG public_crime;
USE SCHEMA silver;

DROP TABLE IF EXISTS crime_event_clean;

CREATE TABLE crime_event_clean (
  dr_no STRING,
  event_hash STRING,
  date_rptd_ts TIMESTAMP,
  date_occ_ts TIMESTAMP,
  occurrence_date DATE,
  reported_date DATE,
  time_occ STRING,
  area INT,
  area_name STRING,
  rpt_dist_no STRING,
  part_1_2 STRING,
  crm_cd INT,
  crm_cd_desc STRING,
  mocodes STRING,
  vict_age INT,
  vict_sex STRING,
  vict_descent STRING,
  premis_cd INT,
  premis_desc STRING,
  weapon_used_cd INT,
  weapon_desc STRING,
  status STRING,
  status_desc STRING,
  crm_cd_1 INT,
  crm_cd_2 INT,
  crm_cd_3 INT,
  crm_cd_4 INT,
  location STRING,
  cross_street STRING,
  lat DOUBLE,
  lon DOUBLE,
  load_date DATE
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO crime_event_clean
WITH base AS (
  SELECT
    CAST(dr_no AS STRING) AS dr_no,
    TRY_TO_TIMESTAMP(date_rptd) AS date_rptd_ts,
    TRY_TO_TIMESTAMP(date_occ)  AS date_occ_ts,
    CAST(time_occ AS STRING)    AS time_occ,
    CAST(area AS INT)           AS area,
    area_name,
    rpt_dist_no,
    part_1_2,
    CAST(crm_cd AS INT)         AS crm_cd,
    crm_cd_desc,
    mocodes,
    CAST(vict_age AS INT)       AS vict_age,
    vict_sex,
    vict_descent,
    CAST(premis_cd AS INT)      AS premis_cd,
    premis_desc,
    CAST(weapon_used_cd AS INT) AS weapon_used_cd,
    weapon_desc,
    status,
    status_desc,
    CAST(crm_cd_1 AS INT)       AS crm_cd_1,
    CAST(crm_cd_2 AS INT)       AS crm_cd_2,
    CAST(crm_cd_3 AS INT)       AS crm_cd_3,
    CAST(crm_cd_4 AS INT)       AS crm_cd_4,
    location,
    cross_street,
    CAST(lat AS DOUBLE)         AS lat,
    CAST(lon AS DOUBLE)         AS lon,
    CURRENT_DATE()              AS load_date,
    SHA2(CONCAT_WS('|',
      dr_no, crm_cd, date_occ, time_occ
    ), 256) AS event_hash
  FROM public_crime.bronze.lapd_crime_bronze
  WHERE dr_no IS NOT NULL
),
dedup AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY dr_no
           ORDER BY date_rptd_ts DESC NULLS LAST
         ) AS rn
  FROM base
)
SELECT
  dr_no,
  event_hash,
  date_rptd_ts,
  date_occ_ts,
  DATE(date_occ_ts)  AS occurrence_date,
  DATE(date_rptd_ts) AS reported_date,
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
  lon,
  load_date
FROM dedup
WHERE rn = 1;

SELECT COUNT(*) AS silver_rows FROM crime_event_clean;
