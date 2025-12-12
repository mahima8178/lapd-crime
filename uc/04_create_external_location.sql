CREATE EXTERNAL LOCATION IF NOT EXISTS el_public_crime_root
URL 's3://public-crime-raw-dev-eu-west-1/'
WITH (STORAGE CREDENTIAL sc_public_crime);

SHOW EXTERNAL LOCATIONS;
DESCRIBE EXTERNAL LOCATION el_public_crime_root;
