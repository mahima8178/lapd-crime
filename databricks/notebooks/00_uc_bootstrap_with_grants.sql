-- 00_uc_bootstrap.sql
-- Goal: create catalog + schemas (bronze/silver/gold) for UC with all necessary grants

-- Grant permission to create external locations (metastore level)
GRANT CREATE EXTERNAL LOCATION ON METASTORE TO `mahima.thakur@sigmoidanalytics.com`;

CREATE CATALOG IF NOT EXISTS public_crime;

-- Grant catalog-level permissions
GRANT USE CATALOG ON CATALOG public_crime TO `mahima.thakur@sigmoidanalytics.com`;
GRANT CREATE SCHEMA ON CATALOG public_crime TO `mahima.thakur@sigmoidanalytics.com`;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS public_crime.bronze;
CREATE SCHEMA IF NOT EXISTS public_crime.silver;
CREATE SCHEMA IF NOT EXISTS public_crime.gold;

-- Grant schema-level permissions for bronze
GRANT USE SCHEMA ON SCHEMA public_crime.bronze TO `mahima.thakur@sigmoidanalytics.com`;
GRANT CREATE TABLE ON SCHEMA public_crime.bronze TO `mahima.thakur@sigmoidanalytics.com`;
GRANT MODIFY ON SCHEMA public_crime.bronze TO `mahima.thakur@sigmoidanalytics.com`;
GRANT SELECT ON SCHEMA public_crime.bronze TO `mahima.thakur@sigmoidanalytics.com`;
GRANT CREATE FUNCTION ON SCHEMA public_crime.bronze TO `mahima.thakur@sigmoidanalytics.com`;

-- Grant schema-level permissions for silver
GRANT USE SCHEMA ON SCHEMA public_crime.silver TO `mahima.thakur@sigmoidanalytics.com`;
GRANT CREATE TABLE ON SCHEMA public_crime.silver TO `mahima.thakur@sigmoidanalytics.com`;
GRANT MODIFY ON SCHEMA public_crime.silver TO `mahima.thakur@sigmoidanalytics.com`;
GRANT SELECT ON SCHEMA public_crime.silver TO `mahima.thakur@sigmoidanalytics.com`;
GRANT CREATE FUNCTION ON SCHEMA public_crime.silver TO `mahima.thakur@sigmoidanalytics.com`;

-- Grant schema-level permissions for gold
GRANT USE SCHEMA ON SCHEMA public_crime.gold TO `mahima.thakur@sigmoidanalytics.com`;
GRANT CREATE TABLE ON SCHEMA public_crime.gold TO `mahima.thakur@sigmoidanalytics.com`;
GRANT MODIFY ON SCHEMA public_crime.gold TO `mahima.thakur@sigmoidanalytics.com`;
GRANT SELECT ON SCHEMA public_crime.gold TO `mahima.thakur@sigmoidanalytics.com`;
GRANT CREATE FUNCTION ON SCHEMA public_crime.gold TO `mahima.thakur@sigmoidanalytics.com`;

-- Quick sanity
SHOW CATALOGS;
SHOW SCHEMAS IN public_crime;

