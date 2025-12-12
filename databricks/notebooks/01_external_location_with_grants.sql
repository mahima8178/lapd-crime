-- 01_external_location.sql
-- Goal: External Location setup with existing storage credential
-- This fixes: NO_PARENT_EXTERNAL_LOCATION_FOR_PATH
-- Uses existing storage credential: db_s3_credentials_databricks-s3-ingest-5d3d2

-- Grant permission to create external locations (metastore level)
GRANT CREATE EXTERNAL LOCATION ON METASTORE TO `mahima.thakur@sigmoidanalytics.com`;

-- Grant permission to use existing storage credential

-- Create external location using existing credential
CREATE EXTERNAL LOCATION IF NOT EXISTS `db_s3_external_databricks-s3-ingest`
URL 's3://public-crime-raw-dev-eu-west-1/'
WITH (CREDENTIAL `db_s3_credentials_databricks-s3-ingest-5d3d2`)
COMMENT 'Parent external location for raw/bronze/silver/gold prefixes';

-- Grant permissions on external location
GRANT USE LOCATION ON EXTERNAL LOCATION `db_s3_external_databricks-s3-ingest` TO `mahima.thakur@sigmoidanalytics.com`;
GRANT READ FILES ON EXTERNAL LOCATION `db_s3_external_databricks-s3-ingest` TO `mahima.thakur@sigmoidanalytics.com`;
GRANT WRITE FILES ON EXTERNAL LOCATION `db_s3_external_databricks-s3-ingest` TO `mahima.thakur@sigmoidanalytics.com`;

-- Sanity checks
SHOW STORAGE CREDENTIALS;
SHOW EXTERNAL LOCATIONS;
DESCRIBE EXTERNAL LOCATION `db_s3_external_databricks-s3-ingest`;

-- Verify S3 path is accessible
LIST 's3://public-crime-raw-dev-eu-west-1/';

