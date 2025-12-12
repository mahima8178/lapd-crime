CREATE STORAGE CREDENTIAL IF NOT EXISTS sc_public_crime
WITH IAM_ROLE = 'arn:aws:iam::018015347364:role/databricks-storage-config-role-euw1';

SHOW STORAGE CREDENTIALS;
