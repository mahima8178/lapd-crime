-- 02_bronze.sql
-- Goal: Bronze external table over raw Socrata JSON already present in S3.
-- Assumption: your raw JSON files exist under:
-- s3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/

USE CATALOG public_crime;
USE SCHEMA bronze;

-- External table on JSON files (schema inferred from JSON keys written by your ingestion)
CREATE TABLE IF NOT EXISTS lapd_crime_raw_json
USING JSON
LOCATION 's3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/';

-- Sanity
SELECT COUNT(*) AS bronze_rows FROM lapd_crime_raw_json;
SELECT * FROM lapd_crime_raw_json LIMIT 5;
