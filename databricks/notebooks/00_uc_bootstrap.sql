-- 00_uc_bootstrap.sql
-- Goal: create catalog + schemas (bronze/silver/gold) for UC.

CREATE CATALOG IF NOT EXISTS public_crime;

CREATE SCHEMA IF NOT EXISTS public_crime.bronze;
CREATE SCHEMA IF NOT EXISTS public_crime.silver;
CREATE SCHEMA IF NOT EXISTS public_crime.gold;

-- Quick sanity
SHOW CATALOGS;
SHOW SCHEMAS IN public_crime;
