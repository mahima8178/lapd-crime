USE CATALOG public_crime;
USE SCHEMA silver;

DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location
USING DELTA
AS
SELECT
  SHA2(CONCAT_WS('|',
    CAST(area AS STRING),
    area_name,
    rpt_dist_no,
    CAST(premis_cd AS STRING),
    premis_desc
  ), 256) AS loc_hash,
  area,
  area_name,
  rpt_dist_no,
  premis_cd,
  premis_desc,
  lat,
  lon
FROM crime_event_clean
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY area, area_name, rpt_dist_no, premis_cd, premis_desc, lat, lon
  ORDER BY load_date DESC
) = 1;

SELECT COUNT(*) FROM dim_location;

